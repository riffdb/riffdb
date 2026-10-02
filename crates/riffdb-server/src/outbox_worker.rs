//! One owned durable-local dispatcher, outside command acknowledgement.

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
