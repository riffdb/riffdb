//! One owned durable-local or HTTP/HTTPS webhook outbox dispatcher, outside command acknowledgement.

use std::io::{Read as _, Write as _};
use std::net::{TcpStream, ToSocketAddrs};
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use riffdb_outbox::{
    ConnectorDeclaration, ConnectorDestinationKind, ConnectorDisposition, ConnectorIdempotency,
    DeliveryAttempt, DeliveryPolicy, DurableLocalArchive, DurableLocalConnector, LocalArchiveError,
    NoOutboxFailpoints, NoOutboxTelemetry, OutboxConnector, OutboxDispatcher, OutboxRecoveryResult,
    RecoveringOutbox,
};
use riffdb_storage_api::{OutboxDestinationIdV1, OutboxPageLimit, OutboxSafeErrorV1};
use riffdb_types::{CanonicalValue, DatabaseId, encode_canonical_record};
use rustls_pki_types::ServerName;
use tokio_rustls::rustls::{ClientConnection, StreamOwned};

use crate::clocks::ServerOutboxClock;
use crate::config::{ConfiguredOutbox, ConfiguredWebhookOutbox, TelemetryPushEndpoint};
use crate::outbox_adapter::{NoDestinationOutboxHealth, OutboxWorkerWake};
use crate::storage::SharedRedbOperationalPorts;
use crate::telemetry_exporter::{build_tls_client_config, load_bearer_token};

const MAX_WEBHOOK_RESPONSE_BYTES: u64 = 65_536;
const WEBHOOK_JSON_CONTENT_TYPE: &str = "application/json";

/// First-party verified HTTPS and loopback HTTP webhook/broker push connector (`ADR-0332`).
pub(crate) struct VerifiedWebhookConnector {
    config: ConfiguredWebhookOutbox,
    last_failed: bool,
}

impl VerifiedWebhookConnector {
    pub(crate) const fn new(config: ConfiguredWebhookOutbox) -> Self {
        Self {
            config,
            last_failed: false,
        }
    }

    pub(crate) const fn last_failed(&self) -> bool {
        self.last_failed
    }
}

impl std::fmt::Debug for VerifiedWebhookConnector {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("VerifiedWebhookConnector([REDACTED])")
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn canonical_value_to_json(val: &CanonicalValue) -> serde_json::Value {
    match val {
        CanonicalValue::Null => serde_json::Value::Null,
        CanonicalValue::Bool(b) => serde_json::Value::Bool(*b),
        CanonicalValue::U64(n) => serde_json::Value::Number((*n).into()),
        CanonicalValue::I64(n) => serde_json::Value::Number((*n).into()),
        CanonicalValue::Decimal(d) => serde_json::json!({
            "coefficient": d.coefficient().to_string(),
            "precision": d.spec().precision(),
            "scale": d.spec().scale(),
        }),
        CanonicalValue::Money(m) => serde_json::json!({
            "currency": m.currency().to_string(),
            "coefficient": m.amount().coefficient().to_string(),
            "precision": m.amount().spec().precision(),
            "scale": m.amount().spec().scale(),
        }),
        CanonicalValue::String(s) => serde_json::Value::String(s.as_str().to_owned()),
        CanonicalValue::Bytes(b) => serde_json::Value::String(hex_encode(b.as_bytes())),
        CanonicalValue::Timestamp(ts) => serde_json::json!({
            "seconds": ts.seconds(),
            "nanos": ts.nanos(),
        }),
        CanonicalValue::Date(d) => serde_json::json!({
            "days_since_unix_epoch": d.days_since_unix_epoch(),
        }),
        CanonicalValue::Uuid(u) => serde_json::Value::String(hex_encode(u)),
        CanonicalValue::Vector(v) => serde_json::Value::Array(
            v.components()
                .iter()
                .filter_map(|elem| serde_json::Number::from_f64(f64::from(*elem)))
                .map(serde_json::Value::Number)
                .collect(),
        ),
        CanonicalValue::Enum {
            type_id,
            variant_id,
        } => serde_json::json!({
            "type_id": type_id.get(),
            "variant_id": variant_id.get(),
        }),
        CanonicalValue::List(list) => {
            serde_json::Value::Array(list.values().iter().map(canonical_value_to_json).collect())
        }
        CanonicalValue::Record(rec) => serde_json::Value::Array(
            rec.fields()
                .iter()
                .map(|(fid, fval)| {
                    serde_json::json!({
                        "field_id": fid.get(),
                        "value": canonical_value_to_json(fval),
                    })
                })
                .collect(),
        ),
    }
}

impl OutboxConnector for VerifiedWebhookConnector {
    fn declaration(&self) -> ConnectorDeclaration {
        ConnectorDeclaration::new(
            ConnectorDestinationKind::HttpWebhook,
            ConnectorIdempotency::EventId,
        )
    }

    fn deliver(&mut self, attempt: DeliveryAttempt<'_>) -> ConnectorDisposition {
        let payload_bytes = match encode_canonical_record(attempt.event().payload()) {
            Ok(bytes) => bytes,
            Err(_) => {
                self.last_failed = true;
                return ConnectorDisposition::PermanentFailure {
                    safe_error: OutboxSafeErrorV1::new("webhook event payload encoding failed")
                        .ok(),
                };
            }
        };

        let idempotency_key_hex = hex_encode(&attempt.idempotency_key());
        let event_hash_hex = hex_encode(attempt.event().event_hash().as_bytes());
        let event_type_id = attempt.event().event_type_id().get();
        let commit_sequence = attempt.event_id().commit_sequence().get();
        let event_ordinal = attempt.event_id().event_ordinal();
        let attempt_num = attempt.attempt().get();
        let destination_id = attempt.destination_id().as_str();

        let fields_json: Vec<serde_json::Value> = attempt
            .event()
            .payload()
            .fields()
            .iter()
            .map(|(field_id, val)| {
                serde_json::json!({
                    "field_id": field_id.get(),
                    "value": canonical_value_to_json(val),
                })
            })
            .collect();

        let body_json = serde_json::json!({
            "schema": "riffdb.outbox.webhook.v1",
            "event_id": idempotency_key_hex,
            "commit_sequence": commit_sequence,
            "event_ordinal": event_ordinal,
            "event_type_id": event_type_id,
            "event_hash": event_hash_hex,
            "destination_id": destination_id,
            "attempt": attempt_num,
            "payload_canonical_hex": hex_encode(&payload_bytes),
            "fields": fields_json,
        });
        let body = body_json.to_string();

        let bearer_token = match self.config.bearer_token_path() {
            Some(path) => match load_bearer_token(path.as_path()) {
                Ok(token) => Some(token),
                Err(()) => {
                    self.last_failed = true;
                    return ConnectorDisposition::Retryable {
                        safe_error: OutboxSafeErrorV1::new("webhook bearer token unavailable").ok(),
                    };
                }
            },
            None => None,
        };

        let (authority, path) = match self.config.endpoint() {
            TelemetryPushEndpoint::LoopbackHttp {
                authority, path, ..
            }
            | TelemetryPushEndpoint::Https {
                authority, path, ..
            } => (authority.as_str(), path.as_str()),
        };

        let mut request = format!(
            "POST {path} HTTP/1.1\r\n\
             Host: {authority}\r\n\
             Content-Type: {WEBHOOK_JSON_CONTENT_TYPE}\r\n\
             Idempotency-Key: {idempotency_key_hex}\r\n\
             X-RiffDB-Event-Id: {idempotency_key_hex}\r\n\
             X-RiffDB-Event-Type-Id: {event_type_id}\r\n\
             X-RiffDB-Event-Hash: {event_hash_hex}\r\n\
             X-RiffDB-Delivery-Attempt: {attempt_num}\r\n\
             X-RiffDB-Destination-Id: {destination_id}\r\n\
             Content-Length: {}\r\n\
             Connection: close\r\n",
            body.len()
        );
        if let Some(token) = bearer_token.as_ref() {
            request.push_str("Authorization: Bearer ");
            request.push_str(token.as_str());
            request.push_str("\r\n");
        }
        request.push_str("\r\n");
        request.push_str(&body);

        let timeout = Duration::from_secs(u64::from(attempt.timeout_seconds().get()));
        let response_result: Result<Vec<u8>, &'static str> = match self.config.endpoint() {
            TelemetryPushEndpoint::LoopbackHttp { address, .. } => (|| {
                let mut stream = TcpStream::connect_timeout(address, timeout)
                    .map_err(|_| "webhook TCP connect failed")?;
                let _ = stream.set_read_timeout(Some(timeout));
                let _ = stream.set_write_timeout(Some(timeout));
                stream
                    .write_all(request.as_bytes())
                    .map_err(|_| "webhook HTTP write failed")?;
                stream.flush().map_err(|_| "webhook HTTP flush failed")?;
                let mut buf = Vec::new();
                stream
                    .take(MAX_WEBHOOK_RESPONSE_BYTES)
                    .read_to_end(&mut buf)
                    .map_err(|_| "webhook HTTP read failed")?;
                Ok(buf)
            })(),
            TelemetryPushEndpoint::Https {
                host,
                port,
                ca_certificate_path,
                ..
            } => (|| {
                let client_config = build_tls_client_config(
                    ca_certificate_path
                        .as_ref()
                        .map(riffdb_config::ProtectedFilePath::as_path),
                )
                .map_err(|()| "webhook TLS trust root unavailable")?;
                let server_name = ServerName::try_from(host.clone())
                    .map_err(|_| "webhook TLS server name invalid")?;
                let addr = (host.as_str(), *port)
                    .to_socket_addrs()
                    .ok()
                    .and_then(|mut iter| iter.next())
                    .ok_or("webhook DNS or socket resolution failed")?;
                let tcp = TcpStream::connect_timeout(&addr, timeout)
                    .map_err(|_| "webhook TCP connect failed")?;
                let _ = tcp.set_read_timeout(Some(timeout));
                let _ = tcp.set_write_timeout(Some(timeout));
                let conn = ClientConnection::new(client_config, server_name)
                    .map_err(|_| "webhook TLS connection init failed")?;
                let mut tls = StreamOwned::new(conn, tcp);
                tls.write_all(request.as_bytes())
                    .map_err(|_| "webhook HTTPS write failed")?;
                tls.flush().map_err(|_| "webhook HTTPS flush failed")?;
                let mut buf = Vec::new();
                tls.take(MAX_WEBHOOK_RESPONSE_BYTES)
                    .read_to_end(&mut buf)
                    .map_err(|_| "webhook HTTPS read failed")?;
                Ok(buf)
            })(),
        };

        let response_bytes = match response_result {
            Ok(bytes) => bytes,
            Err(reason) => {
                self.last_failed = true;
                return ConnectorDisposition::Retryable {
                    safe_error: OutboxSafeErrorV1::new(reason).ok(),
                };
            }
        };

        let head_end = response_bytes
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
            .unwrap_or(response_bytes.len());
        let status_code = std::str::from_utf8(&response_bytes[..head_end])
            .ok()
            .and_then(|head| head.lines().next())
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|code| code.parse::<u16>().ok());

        match status_code {
            Some(200..=299) => {
                self.last_failed = false;
                ConnectorDisposition::Accepted
            }
            Some(code @ (408 | 429 | 500..=599)) => {
                self.last_failed = true;
                ConnectorDisposition::Retryable {
                    safe_error: OutboxSafeErrorV1::new(format!("webhook HTTP {code}")).ok(),
                }
            }
            Some(code @ 400..=499) => {
                self.last_failed = false;
                ConnectorDisposition::PermanentFailure {
                    safe_error: OutboxSafeErrorV1::new(format!("webhook permanent HTTP {code}"))
                        .ok(),
                }
            }
            _ => {
                self.last_failed = true;
                ConnectorDisposition::Retryable {
                    safe_error: OutboxSafeErrorV1::new("webhook malformed HTTP response").ok(),
                }
            }
        }
    }
}

static RETAINED_MAINTENANCE_ARCHIVE: Mutex<
    Option<(PathBuf, DatabaseId, NonZeroU64, DurableLocalArchive)>,
> = Mutex::new(None);

struct InitialPassSignal(Arc<(Mutex<bool>, Condvar)>);
impl InitialPassSignal {
    fn notify(&self) {
        let (lock, condvar) = &*self.0;
        let mut complete = lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !*complete {
            *complete = true;
            condvar.notify_all();
        }
    }
}
impl Drop for InitialPassSignal {
    fn drop(&mut self) {
        self.notify();
    }
}

/// The condition variable makes idle waits cancellable; ownership joins I/O
/// before the primary writer drains. There is exactly one dispatcher per graph.
pub(crate) struct RunningLocalOutbox {
    stop: Arc<OutboxWorkerWake>,
    task: Option<JoinHandle<Option<(PathBuf, DurableLocalArchive)>>>,
}

impl RunningLocalOutbox {
    pub(crate) fn start(
        config: ConfiguredOutbox,
        storage: SharedRedbOperationalPorts,
        clock: ServerOutboxClock,
        binding: DatabaseId,
        incarnation: NonZeroU64,
        health: NoDestinationOutboxHealth,
    ) -> Self {
        health.destination_ready(false);
        let retained = RETAINED_MAINTENANCE_ARCHIVE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .and_then(|(directory, retained_binding, retained_quota, archive)| {
                (Some(directory.as_path()) == config.directory()
                    && retained_binding == binding
                    && Some(retained_quota) == config.quota()
                    && archive.validated())
                .then_some(archive)
            });
        let has_retained = retained.is_some();
        let initial_pass = Arc::new((Mutex::new(false), Condvar::new()));
        let worker_initial_pass = InitialPassSignal(Arc::clone(&initial_pass));
        let stop = Arc::new((Mutex::new((false, false)), Condvar::new()));
        health.attach_worker_wake(Arc::clone(&stop));
        let worker_stop = Arc::clone(&stop);
        // Spawn failure leaves the destination explicitly degraded, with every
        // authoritative intent retained. It cannot acknowledge any delivery.
        let task = thread::Builder::new()
            .name("riffdb-local-outbox".into())
            .stack_size(crate::PRODUCTION_THREAD_STACK_BYTES)
            .spawn(move || {
                let guard = DestinationHealth(health);
                run(
                    config,
                    retained,
                    storage,
                    clock,
                    binding,
                    incarnation,
                    &guard.0,
                    &worker_stop,
                    &worker_initial_pass,
                )
            })
            .ok();
        if task.is_some() {
            let wait_bound = if has_retained {
                Duration::from_secs(10)
            } else {
                Duration::from_millis(50)
            };
            let (lock, condvar) = &*initial_pass;
            let guard = lock
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let _ = condvar
                .wait_timeout_while(guard, wait_bound, |complete| !*complete)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        Self { stop, task }
    }

    pub(crate) fn has_retained_maintenance_archive() -> bool {
        RETAINED_MAINTENANCE_ARCHIVE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some()
    }

    #[cfg(feature = "test-fixtures")]
    pub(crate) fn is_joined(&self) -> bool {
        self.task.is_none()
    }

    pub(crate) fn shutdown(&mut self) {
        if self.task.is_some() {
            let _ = self.join_worker();
            *RETAINED_MAINTENANCE_ARCHIVE
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        }
    }

    pub(crate) fn shutdown_for_maintenance(&mut self) {
        let retained = self.join_worker().and_then(|(directory, archive)| {
            archive
                .validated()
                .then(|| (directory, archive.binding(), archive.quota(), archive))
        });
        *RETAINED_MAINTENANCE_ARCHIVE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = retained;
    }

    fn join_worker(&mut self) -> Option<(PathBuf, DurableLocalArchive)> {
        let (lock, wake) = &*self.stop;
        lock.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .0 = true;
        wake.notify_all();
        self.task.take().and_then(|task| task.join().ok().flatten())
    }
}

impl Drop for RunningLocalOutbox {
    fn drop(&mut self) {
        self.shutdown();
    }
}

struct DestinationHealth(NoDestinationOutboxHealth);
impl Drop for DestinationHealth {
    fn drop(&mut self) {
        self.0.destination_ready(false);
    }
}

type Stop = OutboxWorkerWake;
fn stopping(stop: &Stop) -> bool {
    stop.0
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .0
}
fn wait(stop: &Stop, milliseconds: u64) -> bool {
    let guard = stop
        .0
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (guard, _) = stop
        .1
        .wait_timeout_while(
            guard,
            Duration::from_millis(milliseconds),
            |(stopped, _)| !*stopped,
        )
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    guard.0
}
fn wait_or_woken(stop: &Stop, milliseconds: u64) -> bool {
    let mut guard = stop
        .0
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if guard.0 {
        return true;
    }
    if guard.1 {
        guard.1 = false;
        return false;
    }
    let (mut guard, _) = stop
        .1
        .wait_timeout_while(
            guard,
            Duration::from_millis(milliseconds),
            |(stopped, woken)| !*stopped && !*woken,
        )
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    guard.1 = false;
    guard.0
}
fn refused(error: LocalArchiveError) -> bool {
    matches!(
        error,
        LocalArchiveError::Binding
            | LocalArchiveError::Integrity
            | LocalArchiveError::UnsupportedPlatform
    )
}

#[allow(clippy::too_many_arguments)]
fn run(
    config: ConfiguredOutbox,
    mut retained: Option<DurableLocalArchive>,
    storage: SharedRedbOperationalPorts,
    mut clock: ServerOutboxClock,
    binding: DatabaseId,
    incarnation: NonZeroU64,
    health: &NoDestinationOutboxHealth,
    stop: &Stop,
    initial_pass: &InitialPassSignal,
) -> Option<(PathBuf, DurableLocalArchive)> {
    let (directory, quota) = match config {
        ConfiguredOutbox::DurableLocal { directory, quota } => (directory, quota),
        ConfiguredOutbox::HttpWebhook(webhook_config) => {
            run_webhook(webhook_config, storage, clock, health, stop, initial_pass);
            return None;
        }
    };
    let mut diagnostics = NativeStageDiagnostics::new(storage.shared_read_ports());
    let archive = loop {
        if let Some(archive) = retained.take() {
            break archive;
        }
        if stopping(stop) {
            return None;
        }
        match DurableLocalArchive::open(&directory, binding, quota) {
            Ok(archive) => break archive,
            Err(error) if refused(error) => return None,
            Err(_) => {
                initial_pass.notify();
                if wait(stop, 1000) {
                    return None;
                }
            }
        }
    };
    diagnostics.archive = Some(archive.native_census_v1());
    let mut connector = DurableLocalConnector::new(archive, incarnation);
    loop {
        if stopping(stop) {
            return Some((directory, connector.into_archive()));
        }
        match connector.validate_next_page() {
            Ok(true) => break,
            Ok(false) => {}
            Err(error) if refused(error) => return None,
            Err(_) => {
                initial_pass.notify();
                if wait(stop, 1000) {
                    return Some((directory, connector.into_archive()));
                }
            }
        }
    }
    let policy = delivery_policy();
    // Uncertain source completion returns through the same recovery type state,
    // preserving the already committed archive and its deduplication identity.
    loop {
        if stopping(stop) {
            return Some((directory, connector.into_archive()));
        }
        let recovered = match RecoveringOutbox::after_authoritative_readiness(storage.clone())
            .recover_with_page_admission(
                &policy,
                &mut clock,
                &mut NoOutboxFailpoints,
                &mut NoOutboxTelemetry,
                || !stopping(stop),
            ) {
            OutboxRecoveryResult::Ready { recovered, .. } => recovered,
            OutboxRecoveryResult::Degraded { error, .. } => {
                health.destination_ready(false);
                let locator_warming = matches!(
                    error,
                    riffdb_outbox::OutboxWorkerError::Storage(ref s)
                        if s.kind() == riffdb_storage_api::StorageErrorKind::Unavailable
                );
                if !locator_warming {
                    initial_pass.notify();
                }
                let retry_ms = if locator_warming { 10 } else { 1000 };
                if wait(stop, retry_ms) {
                    return Some((directory, connector.into_archive()));
                }
                continue;
            }
        };
        let mut dispatcher = OutboxDispatcher::new(
            recovered,
            connector,
            clock,
            NoOutboxFailpoints,
            NoOutboxTelemetry,
            policy.clone(),
        );
        loop {
            if stopping(stop) {
                return Some((directory, dispatcher.into_parts().1.into_archive()));
            }
            match dispatcher.dispatch_once() {
                Ok(report) => {
                    diagnostics.emit_if_due();
                    let error = dispatcher.connector().last_error();
                    health.destination_ready(error.is_none());
                    if error.is_none() && report.next_after().is_some() {
                        continue;
                    }
                    if error.is_none()
                        && report.next_after().is_none()
                        && report.deferred() == 0
                        && report.retries_scheduled() == 0
                        && report.dead_lettered() == 0
                        && report.state_changes() == 0
                    {
                        health.set_known(false);
                    } else {
                        health.refresh(&storage);
                    }
                    initial_pass.notify();
                    if error.is_some_and(refused) {
                        return None;
                    }
                    if error.is_some() {
                        if wait(stop, 1000) {
                            return Some((directory, dispatcher.into_parts().1.into_archive()));
                        }
                    } else if wait_or_woken(stop, 25) {
                        return Some((directory, dispatcher.into_parts().1.into_archive()));
                    }
                }
                Err(_) => {
                    diagnostics.emit_if_due();
                    health.destination_ready(false);
                    initial_pass.notify();
                    let (_, saved_connector, saved_clock, _, _, _) = dispatcher.into_parts();
                    connector = saved_connector;
                    clock = saved_clock;
                    if wait(stop, 1000) {
                        return Some((directory, connector.into_archive()));
                    }
                    break;
                }
            }
        }
    }
}

#[expect(
    clippy::expect_used,
    reason = "fixed dispatcher policy values satisfy their checked constructors"
)]
fn delivery_policy() -> DeliveryPolicy {
    DeliveryPolicy::new(
        OutboxDestinationIdV1::new("server/durable-local").expect("static destination"),
        NonZeroU32::new(5).expect("static timeout"),
        NonZeroU32::new(10).expect("static lease"),
        NonZeroU32::new(32).expect("static attempts"),
        vec![NonZeroU32::MIN; 31],
        OutboxPageLimit::new(NonZeroU16::new(16).expect("static page bound"))
            .expect("bounded page"),
    )
    .expect("valid static delivery policy")
}

#[expect(
    clippy::expect_used,
    reason = "validated webhook configuration satisfies DeliveryPolicy bounds"
)]
fn webhook_delivery_policy(config: &ConfiguredWebhookOutbox) -> DeliveryPolicy {
    let timeout = config.timeout_seconds();
    let lease_seconds =
        NonZeroU32::new(timeout.get().saturating_mul(2).max(10)).expect("nonzero lease seconds");
    let max_attempts = config.max_attempts();
    let retry_delays = vec![NonZeroU32::MIN; (max_attempts.get() as usize).saturating_sub(1)];
    DeliveryPolicy::new(
        config.destination_id().clone(),
        timeout,
        lease_seconds,
        max_attempts,
        retry_delays,
        OutboxPageLimit::new(NonZeroU16::new(16).expect("static page bound"))
            .expect("bounded page"),
    )
    .expect("valid webhook delivery policy")
}

fn run_webhook(
    config: ConfiguredWebhookOutbox,
    storage: SharedRedbOperationalPorts,
    mut clock: ServerOutboxClock,
    health: &NoDestinationOutboxHealth,
    stop: &Stop,
    initial_pass: &InitialPassSignal,
) {
    let mut diagnostics = NativeStageDiagnostics::new(storage.shared_read_ports());
    let policy = webhook_delivery_policy(&config);
    let mut connector = VerifiedWebhookConnector::new(config);
    loop {
        if stopping(stop) {
            return;
        }
        let recovered = match RecoveringOutbox::after_authoritative_readiness(storage.clone())
            .recover_with_page_admission(
                &policy,
                &mut clock,
                &mut NoOutboxFailpoints,
                &mut NoOutboxTelemetry,
                || !stopping(stop),
            ) {
            OutboxRecoveryResult::Ready { recovered, .. } => recovered,
            OutboxRecoveryResult::Degraded { error, .. } => {
                health.destination_ready(false);
                let locator_warming = matches!(
                    error,
                    riffdb_outbox::OutboxWorkerError::Storage(ref s)
                        if s.kind() == riffdb_storage_api::StorageErrorKind::Unavailable
                );
                if !locator_warming {
                    initial_pass.notify();
                }
                let retry_ms = if locator_warming { 10 } else { 1000 };
                if wait(stop, retry_ms) {
                    return;
                }
                continue;
            }
        };
        let mut dispatcher = OutboxDispatcher::new(
            recovered,
            connector,
            clock,
            NoOutboxFailpoints,
            NoOutboxTelemetry,
            policy.clone(),
        );
        loop {
            if stopping(stop) {
                return;
            }
            match dispatcher.dispatch_once() {
                Ok(report) => {
                    diagnostics.emit_if_due();
                    let failed = dispatcher.connector().last_failed();
                    health.destination_ready(!failed);
                    if !failed && report.next_after().is_some() {
                        continue;
                    }
                    if !failed
                        && report.next_after().is_none()
                        && report.deferred() == 0
                        && report.retries_scheduled() == 0
                        && report.dead_lettered() == 0
                        && report.state_changes() == 0
                    {
                        health.set_known(false);
                    } else {
                        health.refresh(&storage);
                    }
                    initial_pass.notify();
                    if failed {
                        if wait(stop, 250) {
                            return;
                        }
                    } else if wait_or_woken(stop, 25) {
                        return;
                    }
                }
                Err(_) => {
                    diagnostics.emit_if_due();
                    health.destination_ready(false);
                    initial_pass.notify();
                    let (_, saved_connector, saved_clock, _, _, _) = dispatcher.into_parts();
                    connector = saved_connector;
                    clock = saved_clock;
                    if wait(stop, 1000) {
                        return;
                    }
                    break;
                }
            }
        }
    }
}

struct NativeStageDiagnostics {
    source: riffdb_storage_redb::RedbSharedPorts,
    archive: Option<riffdb_outbox::LocalArchiveNativeCensus>,
    began: std::time::Instant,
    next: Option<Duration>,
}
impl NativeStageDiagnostics {
    fn new(source: riffdb_storage_redb::RedbSharedPorts) -> Self {
        Self {
            source,
            archive: None,
            began: std::time::Instant::now(),
            next: Some(Duration::ZERO),
        }
    }
    fn emit(&self) {
        // Optional diagnostic I/O happens after native operations released their
        // writer leases. It never changes the delivery result or readiness.
        let _ = writeln!(
            std::io::stderr().lock(),
            "{}",
            format_native_stages(self.source.outbox_stage_census_v1())
        );
        if let Some(archive) = &self.archive {
            let _ = writeln!(
                std::io::stderr().lock(),
                "{}",
                format_archive_stages(archive.snapshot())
            );
        }
    }
    fn emit_if_due(&mut self) {
        let elapsed = self.began.elapsed();
        if self.next.is_some_and(|next| elapsed >= next) {
            self.emit();
            self.next = elapsed.checked_add(Duration::from_secs(60));
        }
    }
}
impl Drop for NativeStageDiagnostics {
    fn drop(&mut self) {
        self.emit();
    }
}
fn format_native_stages(values: [[u64; 3]; 8]) -> String {
    use std::fmt::Write as _;
    let mut line = String::from("riffdb-outbox-native-stages-v1");
    for (label, [count, sum, maximum]) in riffdb_storage_redb::OUTBOX_NATIVE_STAGE_LABELS_V1
        .into_iter()
        .zip(values)
    {
        let _ = write!(line, "\t{label}={count},{sum},{maximum}");
    }
    line
}

fn format_archive_stages(values: [[u64; 3]; 3]) -> String {
    use std::fmt::Write as _;
    let mut line = String::from("riffdb-outbox-archive-stages-v1");
    for (label, [count, sum, maximum]) in riffdb_outbox::LOCAL_ARCHIVE_NATIVE_STAGE_LABELS_V1
        .into_iter()
        .zip(values)
    {
        let _ = write!(line, "\t{label}={count},{sum},{maximum}");
    }
    line
}

#[cfg(test)]
mod native_stage_diagnostic_tests {
    // req: EFF-002, STO-020, END-008
    #[test]
    fn archive_stage_diagnostic_is_closed_bounded_numeric_and_distinct_from_primary_timings() {
        let line = super::format_archive_stages([[u64::MAX; 3]; 3]);
        assert!(line.len() <= 384);
        let fields: Vec<_> = line.split('\t').collect();
        assert_eq!(fields[0], "riffdb-outbox-archive-stages-v1");
        assert_eq!(fields.len(), 4);
        for (field, expected) in fields[1..]
            .iter()
            .zip(riffdb_outbox::LOCAL_ARCHIVE_NATIVE_STAGE_LABELS_V1)
        {
            let (label, values) = field.split_once('=').unwrap();
            assert_eq!(label, expected);
            assert_eq!(
                values
                    .split(',')
                    .map(|value| value.parse::<u64>().unwrap())
                    .collect::<Vec<_>>(),
                vec![u64::MAX; 3]
            );
        }
    }
    // req: STO-030, STO-031, END-008
    #[test]
    fn outbox_native_stage_diagnostic_is_closed_bounded_and_numeric() {
        let line = super::format_native_stages([[u64::MAX; 3]; 8]);
        assert!(line.len() <= 1024);
        let fields: Vec<_> = line.split('\t').skip(1).collect();
        assert_eq!(fields.len(), 8);
        for (field, expected) in fields
            .into_iter()
            .zip(riffdb_storage_redb::OUTBOX_NATIVE_STAGE_LABELS_V1)
        {
            let (label, values) = field.split_once('=').unwrap();
            assert_eq!(label, expected);
            assert_eq!(
                values
                    .split(',')
                    .map(|value| value.parse::<u64>().unwrap())
                    .collect::<Vec<_>>(),
                vec![u64::MAX; 3]
            );
        }
    }

    // req: EFF-001, EFF-002, EFF-003, NET-002, NET-003
    #[tokio::test]
    async fn webhook_outbox_connector_delivers_idempotent_events_and_classifies_retry_and_dead_letter()
     {
        use std::collections::VecDeque;
        use std::fs;
        use std::num::NonZeroU32;
        #[cfg(unix)]
        use std::os::unix::fs::PermissionsExt;
        use std::sync::{Arc, Mutex};

        use riffdb_config::{
            CanonicalHttpsEndpoint, DirectTlsListenerConfig, ListenerBounds, ProtectedFilePath,
            ServerTlsFiles, issue_tls_bundle, write_protected_pem_file,
        };
        use riffdb_outbox::{
            ConnectorDestinationKind, ConnectorDisposition, ConnectorIdempotency, DeliveryAttempt,
            OutboxConnector,
        };
        use riffdb_storage_api::{
            OutboxDestinationIdV1, StoredDurableEventV1, derive_event_hash_v1,
        };
        use riffdb_types::{
            CanonicalRecord, CanonicalValue, CommitSequence, EventId, EventTypeId, FieldId,
        };
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        use crate::config::{ConfiguredOutbox, ServerConfig};
        use crate::daemon::load_http1_tls_acceptor;

        let temp = tempfile::TempDir::with_prefix("riffdb-webhook-outbox-").expect("temp dir");
        #[cfg(unix)]
        fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700))
            .expect("chmod 0700 temp dir");

        // 1. Remote cleartext http:// webhook destination fails closed at config parse time (NET-002)
        let bad_config_path = temp.path().join("bad-outbox.toml");
        fs::write(
            &bad_config_path,
            format!(
                "[server]\ndatabase = '{0}/db.redb'\ncapability_keys = '{0}/cap.keys'\nidempotency_keys = '{0}/idem.keys'\n\
                 [maintenance]\nbackup_root = '{0}/backups'\nprojections_root = '{0}/projections'\n\
                 [maintenance.outbox]\nkind = 'http_webhook'\nendpoint = 'http://198.51.100.20:8080/events'\n",
                temp.path().display()
            ),
        )
        .expect("write bad outbox config");
        assert!(
            ServerConfig::parse_config_file(&bad_config_path, temp.path()).is_err(),
            "remote cleartext HTTP outbox webhook must fail closed"
        );

        // 2. Provision verified TLS materials and mode-0600 bearer token for the HTTPS webhook receiver
        let token_path = temp.path().join("webhook-token.secret");
        fs::write(&token_path, b"broker-webhook-secret-token\n").expect("write webhook token");
        #[cfg(unix)]
        fs::set_permissions(&token_path, fs::Permissions::from_mode(0o600))
            .expect("chmod 0600 webhook token");

        let ca_cert_path = temp.path().join("ca.pem");
        let server_cert_path = temp.path().join("server.pem");
        let server_key_path = temp.path().join("server.key");
        let endpoint = CanonicalHttpsEndpoint::parse("https://127.0.0.1:8443")
            .expect("parse canonical endpoint");
        let bundle = issue_tls_bundle(&endpoint, 30, false).expect("issue tls bundle");
        write_protected_pem_file(&ca_cert_path, bundle.ca().certificate_pem(), false)
            .expect("write ca.pem");
        write_protected_pem_file(&server_cert_path, bundle.server().certificate_pem(), false)
            .expect("write server.pem");
        write_protected_pem_file(&server_key_path, bundle.server().private_key_pem(), true)
            .expect("write server.key");

        let std_listener =
            std::net::TcpListener::bind("127.0.0.1:0").expect("bind mock webhook server");
        std_listener
            .set_nonblocking(true)
            .expect("nonblocking webhook listener");
        let webhook_addr = std_listener.local_addr().expect("webhook local addr");

        let tls_files = ServerTlsFiles::new(
            ProtectedFilePath::new(&server_cert_path).expect("server cert"),
            ProtectedFilePath::new(&server_key_path).expect("server key"),
        )
        .expect("server tls files");
        let listener_cfg = DirectTlsListenerConfig::new(
            webhook_addr,
            CanonicalHttpsEndpoint::parse(&format!("https://{webhook_addr}"))
                .expect("webhook https endpoint"),
            tls_files,
            ListenerBounds::alpha_default(),
        )
        .expect("direct tls listener config");
        let tls_acceptor = load_http1_tls_acceptor(&listener_cfg).expect("load tls acceptor");

        // Scripted responses: 1st -> 200 OK, 2nd -> 429 Too Many Requests, 3rd -> 422 Unprocessable Entity
        let scripted_statuses = Arc::new(Mutex::new(VecDeque::from([
            "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}",
            "HTTP/1.1 429 Too Many Requests\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}",
            "HTTP/1.1 422 Unprocessable Entity\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}",
        ])));
        let (received_tx, mut received_rx) =
            tokio::sync::mpsc::unbounded_channel::<(String, serde_json::Value)>();

        let statuses_for_task = Arc::clone(&scripted_statuses);
        let server_task = tokio::spawn(async move {
            let listener =
                tokio::net::TcpListener::from_std(std_listener).expect("tokio webhook listener");
            while let Ok((tcp, _)) = listener.accept().await {
                let acceptor = tls_acceptor.clone();
                let tx = received_tx.clone();
                let response_line = statuses_for_task
                    .lock()
                    .expect("lock statuses")
                    .pop_front()
                    .unwrap_or(
                        "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}",
                    );
                tokio::spawn(async move {
                    let Ok(mut tls) = acceptor.accept(tcp).await else {
                        return;
                    };
                    let mut buf = Vec::new();
                    let mut chunk = [0_u8; 4096];
                    loop {
                        let Ok(n) = tls.read(&mut chunk).await else {
                            return;
                        };
                        if n == 0 {
                            break;
                        }
                        buf.extend_from_slice(&chunk[..n]);
                        if let Some(sep_idx) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                            let body_start = sep_idx + 4;
                            let head_str = String::from_utf8_lossy(&buf[..sep_idx]);
                            let content_len = head_str
                                .lines()
                                .find_map(|line| {
                                    let (k, v) = line.split_once(':')?;
                                    if k.trim().eq_ignore_ascii_case("content-length") {
                                        v.trim().parse::<usize>().ok()
                                    } else {
                                        None
                                    }
                                })
                                .unwrap_or(0);
                            if buf.len() >= body_start + content_len {
                                let head = head_str.into_owned();
                                let body_slice = &buf[body_start..body_start + content_len];
                                if let Ok(json) =
                                    serde_json::from_slice::<serde_json::Value>(body_slice)
                                {
                                    let _ = tx.send((head, json));
                                }
                                let _ = tls.write_all(response_line.as_bytes()).await;
                                let _ = tls.shutdown().await;
                                break;
                            }
                        }
                    }
                });
            }
        });

        // 3. Parse valid `[maintenance.outbox]` with `kind = "http_webhook"`
        let valid_config_path = temp.path().join("valid-outbox.toml");
        fs::write(
            &valid_config_path,
            format!(
                "[server]\ndatabase = '{0}/db.redb'\ncapability_keys = '{0}/cap.keys'\nidempotency_keys = '{0}/idem.keys'\n\
                 [maintenance]\nbackup_root = '{0}/backups'\nprojections_root = '{0}/projections'\n\
                 [maintenance.outbox]\nkind = 'http_webhook'\ndestination_id = 'broker/orders'\nendpoint = 'https://{webhook_addr}/v1/events'\nca_certificate_path = '{1}'\nbearer_token_path = '{2}'\ntimeout_seconds = 5\nmax_attempts = 8\n",
                temp.path().display(),
                ca_cert_path.display(),
                token_path.display(),
            ),
        )
        .expect("write valid outbox config");
        let parsed_config =
            ServerConfig::parse_config_file(&valid_config_path, temp.path()).expect("parse config");
        let Some(ConfiguredOutbox::HttpWebhook(webhook_cfg)) =
            parsed_config.databases()[0].outbox().cloned()
        else {
            panic!("expected ConfiguredOutbox::HttpWebhook");
        };

        let connector = super::VerifiedWebhookConnector::new(webhook_cfg.clone());
        assert_eq!(
            connector.declaration().destination_kind(),
            ConnectorDestinationKind::HttpWebhook
        );
        assert_eq!(
            connector.declaration().idempotency(),
            ConnectorIdempotency::EventId
        );

        let event_id = EventId::new(CommitSequence::first(), 0);
        let event_type = EventTypeId::try_from(7).expect("event type");
        let payload = CanonicalRecord::new(vec![(
            FieldId::try_from(1).expect("field id"),
            CanonicalValue::string("order-created-42").expect("string value"),
        )])
        .expect("canonical payload");
        let event_hash = derive_event_hash_v1(event_id, event_type, &payload).expect("event hash");
        let stored_event =
            StoredDurableEventV1::new(event_id, event_type, payload, event_hash).expect("event");
        let destination_id = OutboxDestinationIdV1::new("broker/orders").expect("dest id");

        // Attempt 1: 200 OK -> Accepted
        let disp1 = tokio::task::spawn_blocking({
            let stored_event = stored_event.clone();
            let destination_id = destination_id.clone();
            let webhook_cfg = webhook_cfg.clone();
            move || {
                let mut conn = super::VerifiedWebhookConnector::new(webhook_cfg);
                let attempt = DeliveryAttempt::new(
                    &stored_event,
                    NonZeroU32::new(1).expect("attempt 1"),
                    &destination_id,
                    NonZeroU32::new(5).expect("timeout"),
                );
                conn.deliver(attempt)
            }
        })
        .await
        .expect("join attempt 1");
        assert_eq!(disp1, ConnectorDisposition::Accepted);

        let (head1, json1) = received_rx.recv().await.expect("received webhook 1");
        let expected_event_id_hex = super::hex_encode(&event_id.to_be_bytes());
        assert!(head1.starts_with("POST /v1/events HTTP/1.1"));
        assert!(
            head1
                .to_ascii_lowercase()
                .contains(&format!("idempotency-key: {expected_event_id_hex}"))
        );
        assert!(
            head1
                .to_ascii_lowercase()
                .contains("authorization: bearer broker-webhook-secret-token")
        );
        assert_eq!(json1["schema"], "riffdb.outbox.webhook.v1");
        assert_eq!(json1["event_id"], expected_event_id_hex);
        assert_eq!(json1["event_type_id"], 7);
        assert_eq!(json1["destination_id"], "broker/orders");
        assert_eq!(json1["attempt"], 1);
        assert_eq!(json1["fields"][0]["value"], "order-created-42");

        // Attempt 2: 429 Too Many Requests -> Retryable
        let disp2 = tokio::task::spawn_blocking({
            let stored_event = stored_event.clone();
            let destination_id = destination_id.clone();
            let webhook_cfg = webhook_cfg.clone();
            move || {
                let mut conn = super::VerifiedWebhookConnector::new(webhook_cfg);
                let attempt = DeliveryAttempt::new(
                    &stored_event,
                    NonZeroU32::new(2).expect("attempt 2"),
                    &destination_id,
                    NonZeroU32::new(5).expect("timeout"),
                );
                conn.deliver(attempt)
            }
        })
        .await
        .expect("join attempt 2");
        assert!(matches!(disp2, ConnectorDisposition::Retryable { .. }));

        // Attempt 3: 422 Unprocessable Entity -> PermanentFailure (DeadLetter)
        let disp3 = tokio::task::spawn_blocking({
            let stored_event = stored_event.clone();
            let destination_id = destination_id.clone();
            move || {
                let mut conn = super::VerifiedWebhookConnector::new(webhook_cfg);
                let attempt = DeliveryAttempt::new(
                    &stored_event,
                    NonZeroU32::new(3).expect("attempt 3"),
                    &destination_id,
                    NonZeroU32::new(5).expect("timeout"),
                );
                conn.deliver(attempt)
            }
        })
        .await
        .expect("join attempt 3");
        assert!(matches!(
            disp3,
            ConnectorDisposition::PermanentFailure { .. }
        ));

        server_task.abort();
    }
}
