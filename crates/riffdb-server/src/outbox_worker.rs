//! One owned durable-local dispatcher, outside command acknowledgement.

use std::io::Write as _;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::path::PathBuf;
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
use crate::outbox_adapter::{NoDestinationOutboxHealth, OutboxWorkerWake};
use crate::storage::SharedRedbOperationalPorts;

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
                (directory == config.directory()
                    && retained_binding == binding
                    && retained_quota == config.quota()
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
                Duration::from_millis(500)
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
    let directory = config.directory().to_path_buf();
    let mut diagnostics = NativeStageDiagnostics::new(storage.shared_read_ports());
    let archive = loop {
        if let Some(archive) = retained.take() {
            break archive;
        }
        if stopping(stop) {
            return None;
        }
        match DurableLocalArchive::open(config.directory(), binding, config.quota()) {
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
}
