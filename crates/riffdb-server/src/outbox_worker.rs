//! One owned durable-local dispatcher, outside command acknowledgement.

use std::io::Write as _;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use riffdb_outbox::{
    DeliveryPolicy, DurableLocalArchive, DurableLocalConnector, LocalArchiveError,
    NoOutboxFailpoints, NoOutboxTelemetry, OutboxDispatcher, OutboxRecoveryResult,
    RecoveringOutbox,
};
use riffdb_storage_api::{OutboxDestinationIdV1, OutboxPageLimit};
use riffdb_types::DatabaseId;

use crate::clocks::ServerOutboxClock;
use crate::config::ConfiguredOutbox;
use crate::outbox_adapter::NoDestinationOutboxHealth;
use crate::storage::SharedRedbOperationalPorts;

/// The condition variable makes idle waits cancellable; ownership joins I/O
/// before the primary writer drains. There is exactly one dispatcher per graph.
pub(crate) struct RunningLocalOutbox {
    stop: Arc<(Mutex<bool>, Condvar)>,
    task: Option<JoinHandle<()>>,
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
        let stop = Arc::new((Mutex::new(false), Condvar::new()));
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
                    storage,
                    clock,
                    binding,
                    incarnation,
                    &guard.0,
                    &worker_stop,
                );
            })
            .ok();
        Self { stop, task }
    }

    #[cfg(feature = "test-fixtures")]
    pub(crate) fn is_joined(&self) -> bool {
        self.task.is_none()
    }

    pub(crate) fn shutdown(&mut self) {
        let (lock, wake) = &*self.stop;
        *lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        wake.notify_all();
        if let Some(task) = self.task.take() {
            let _ = task.join();
        }
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

type Stop = (Mutex<bool>, Condvar);
fn stopping(stop: &Stop) -> bool {
    *stop
        .0
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
fn wait(stop: &Stop, milliseconds: u64) -> bool {
    let guard = stop
        .0
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (guard, _) = stop
        .1
        .wait_timeout_while(guard, Duration::from_millis(milliseconds), |stopped| {
            !*stopped
        })
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    *guard
}
fn refused(error: LocalArchiveError) -> bool {
    matches!(
        error,
        LocalArchiveError::Binding
            | LocalArchiveError::Integrity
            | LocalArchiveError::UnsupportedPlatform
    )
}

fn run(
    config: ConfiguredOutbox,
    storage: SharedRedbOperationalPorts,
    mut clock: ServerOutboxClock,
    binding: DatabaseId,
    incarnation: NonZeroU64,
    health: &NoDestinationOutboxHealth,
    stop: &Stop,
) {
    let mut diagnostics = NativeStageDiagnostics::new(storage.shared_read_ports());
    let archive = loop {
        if stopping(stop) {
            return;
        }
        match DurableLocalArchive::open(config.directory(), binding, config.quota()) {
            Ok(archive) => break archive,
            Err(error) if refused(error) => return,
            Err(_) => {
                if wait(stop, 1000) {
                    return;
                }
            }
        }
    };
    let mut connector = DurableLocalConnector::new(archive, incarnation);
    loop {
        if stopping(stop) {
            return;
        }
        match connector.validate_next_page() {
            Ok(true) => break,
            Ok(false) => {
                if wait(stop, 1) {
                    return;
                }
            }
            Err(error) if refused(error) => return,
            Err(_) => {
                if wait(stop, 1000) {
                    return;
                }
            }
        }
    }
    let policy = delivery_policy();
    // Uncertain source completion returns through the same recovery type state,
    // preserving the already committed archive and its deduplication identity.
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
            OutboxRecoveryResult::Degraded { .. } => {
                health.destination_ready(false);
                if wait(stop, 1000) {
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
                    let error = dispatcher.connector().last_error();
                    health.destination_ready(error.is_none());
                    health.refresh(&storage);
                    if error.is_some_and(refused) {
                        return;
                    }
                    let delay = if error.is_some() {
                        1000
                    } else if report.next_after().is_some() {
                        1
                    } else {
                        25
                    };
                    if wait(stop, delay) {
                        return;
                    }
                }
                Err(_) => {
                    diagnostics.emit_if_due();
                    health.destination_ready(false);
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

struct NativeStageDiagnostics {
    source: riffdb_storage_redb::RedbSharedPorts,
    began: std::time::Instant,
    next: Option<Duration>,
}
impl NativeStageDiagnostics {
    fn new(source: riffdb_storage_redb::RedbSharedPorts) -> Self {
        Self {
            source,
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

#[cfg(test)]
mod native_stage_diagnostic_tests {
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
}
