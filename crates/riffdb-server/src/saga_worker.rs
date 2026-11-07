//! Production saga coordination port and background recovery worker (`ADR-0334`).

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use riffdb_policy::AuthorizationClock;
use riffdb_service::{
    PortFuture, RequestControl, RiffDbService, SagaCoordinationPort, SagaPortError,
};
use riffdb_storage_api::{SagaCoordinationRepository, StorageError, StorageErrorKind};
use riffdb_types::{
    SagaAdmissionStorageOutcomeV1, SagaCasOutcomeV1, SagaCompareExpectedV1, SagaId,
    SagaLifecycleStatusV1, StoredSagaInstanceV1, Timestamp,
};
use tokio::sync::Notify;

use crate::clocks::ServerAuthorizationClock;
use crate::port_driver::{BlockingPortDriver, BlockingPortExecutor};
use crate::storage::SharedRedbOperationalPorts;

const SAGA_WORKER_BATCH_LIMIT: usize = 8;
const SAGA_WORKER_LEASE_SECS: i64 = 30;
const SAGA_WORKER_ACTIVE_POLL: Duration = Duration::from_millis(100);
const SAGA_WORKER_IDLE_POLL: Duration = Duration::from_secs(1);

/// Notification hub waking the background saga worker when new saga transitions are persisted.
pub(crate) struct SagaWorkNotification {
    stopped: AtomicBool,
    wake: Notify,
}

impl SagaWorkNotification {
    pub(crate) fn new() -> Self {
        Self {
            stopped: AtomicBool::new(false),
            wake: Notify::new(),
        }
    }

    pub(crate) fn notify_work(&self) {
        self.wake.notify_one();
    }

    pub(crate) fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
        self.wake.notify_waiters();
    }

    pub(crate) fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }
}

impl fmt::Debug for SagaWorkNotification {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SagaWorkNotification([BOUNDED])")
    }
}

/// Bounded blocking adapter over authoritative saga coordination storage.
pub(crate) struct ServerSagaCoordinator {
    admit: BlockingPortExecutor<StoredSagaInstanceV1, SagaAdmissionStorageOutcomeV1, SagaPortError>,
    read: BlockingPortExecutor<SagaId, Option<StoredSagaInstanceV1>, SagaPortError>,
    cas: BlockingPortExecutor<
        (SagaCompareExpectedV1, StoredSagaInstanceV1),
        SagaCasOutcomeV1,
        SagaPortError,
    >,
    notify: Arc<SagaWorkNotification>,
}

impl ServerSagaCoordinator {
    pub(crate) fn new(
        storage: SharedRedbOperationalPorts,
        driver: &BlockingPortDriver,
        notify: Arc<SagaWorkNotification>,
    ) -> Self {
        let admit_storage = storage.clone();
        let read_storage = storage.clone();
        let cas_storage = storage;
        Self {
            admit: driver.executor(move |candidate| {
                admit_storage
                    .admit_or_get_saga(&candidate)
                    .map_err(map_storage_error)
            }),
            read: driver.executor(move |saga_id| {
                read_storage.read_saga(saga_id).map_err(map_storage_error)
            }),
            cas: driver.executor(move |(expected, replacement)| {
                cas_storage
                    .compare_and_swap_saga(expected, &replacement)
                    .map_err(map_storage_error)
            }),
            notify,
        }
    }
}

impl SagaCoordinationPort for ServerSagaCoordinator {
    fn admit_or_get_saga(
        &self,
        candidate: StoredSagaInstanceV1,
        control: &RequestControl,
    ) -> PortFuture<'_, SagaAdmissionStorageOutcomeV1, SagaPortError> {
        let permit = self.admit.reserve(control);
        Box::pin(async move {
            let permit = permit.map_err(|_| SagaPortError::Unavailable)?;
            permit
                .submit(candidate)
                .map_err(|_| SagaPortError::Unavailable)?
                .completion()
                .await
                .map_err(|_| SagaPortError::Unavailable)?
        })
    }

    fn read_saga(
        &self,
        saga_id: SagaId,
        control: &RequestControl,
    ) -> PortFuture<'_, Option<StoredSagaInstanceV1>, SagaPortError> {
        let permit = self.read.reserve(control);
        Box::pin(async move {
            let permit = permit.map_err(|_| SagaPortError::Unavailable)?;
            permit
                .submit(saga_id)
                .map_err(|_| SagaPortError::Unavailable)?
                .completion()
                .await
                .map_err(|_| SagaPortError::Unavailable)?
        })
    }

    fn compare_and_swap_saga(
        &self,
        expected: SagaCompareExpectedV1,
        replacement: StoredSagaInstanceV1,
        control: &RequestControl,
    ) -> PortFuture<'_, SagaCasOutcomeV1, SagaPortError> {
        let permit = self.cas.reserve(control);
        Box::pin(async move {
            let permit = permit.map_err(|_| SagaPortError::Unavailable)?;
            permit
                .submit((expected, replacement))
                .map_err(|_| SagaPortError::Unavailable)?
                .completion()
                .await
                .map_err(|_| SagaPortError::Unavailable)?
        })
    }

    fn notify_work(&self) {
        self.notify.notify_work();
    }
}

impl fmt::Debug for ServerSagaCoordinator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ServerSagaCoordinator([AUTHORITATIVE_SAGA_STATE])")
    }
}

fn map_storage_error(error: StorageError) -> SagaPortError {
    match error.kind() {
        StorageErrorKind::Unavailable => SagaPortError::Unavailable,
        StorageErrorKind::SequenceExhausted => SagaPortError::SequenceExhausted,
        _ => SagaPortError::Integrity,
    }
}

/// Graph-owned background saga worker driving leased recovery and compensation (`ADR-0334`).
pub(crate) struct RunningSagaWorker {
    notify: Arc<SagaWorkNotification>,
    thread: Option<JoinHandle<()>>,
}

impl RunningSagaWorker {
    pub(crate) fn start(
        storage: SharedRedbOperationalPorts,
        service: Arc<RiffDbService>,
        clock: ServerAuthorizationClock,
        worker_id: [u8; 16],
        notify: Arc<SagaWorkNotification>,
    ) -> Self {
        let worker_notify = Arc::clone(&notify);
        let runtime = tokio::runtime::Handle::current();
        let thread = std::thread::Builder::new()
            .name("riffdb-saga".to_owned())
            .stack_size(crate::PRODUCTION_THREAD_STACK_BYTES)
            .spawn(move || {
                runtime.block_on(async move {
                    run_saga_worker_loop(&storage, &service, &clock, worker_id, &worker_notify)
                        .await;
                });
            })
            .ok();
        Self { notify, thread }
    }

    pub(crate) fn shutdown(&mut self) {
        self.notify.stop();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for RunningSagaWorker {
    fn drop(&mut self) {
        self.shutdown();
    }
}

async fn run_saga_worker_loop(
    storage: &SharedRedbOperationalPorts,
    service: &Arc<RiffDbService>,
    clock: &ServerAuthorizationClock,
    worker_id: [u8; 16],
    notify: &SagaWorkNotification,
) {
    let mut check_pending = true;
    let mut cursor_partition_hash = 0_u64;
    loop {
        if notify.is_stopped() {
            return;
        }
        let mut keep_polling = false;
        if check_pending {
            if let Ok(now) = clock.now() {
                let lease_expires_at = Timestamp::new(
                    now.seconds().saturating_add(SAGA_WORKER_LEASE_SECS),
                    now.nanoseconds(),
                )
                .unwrap_or(now);
                let claimed = storage
                    .claim_due_sagas(
                        worker_id,
                        now,
                        lease_expires_at,
                        SAGA_WORKER_BATCH_LIMIT,
                        cursor_partition_hash,
                    )
                    .unwrap_or_default();
                if let Some(last) = claimed.last() {
                    cursor_partition_hash = last.partition_hash().wrapping_add(1);
                } else {
                    cursor_partition_hash = 0;
                }
                if claimed.len() == SAGA_WORKER_BATCH_LIMIT {
                    keep_polling = true;
                }
                for instance in claimed {
                    if notify.is_stopped() {
                        return;
                    }
                    if let Ok(updated) = service.advance_claimed_saga(instance, now).await
                        && matches!(
                            updated.lifecycle_status(),
                            SagaLifecycleStatusV1::RunningForward
                                | SagaLifecycleStatusV1::Compensating
                                | SagaLifecycleStatusV1::BlockedRetryable
                        )
                    {
                        keep_polling = true;
                    }
                }
                if !keep_polling && storage.has_claimable_saga().unwrap_or(false) {
                    keep_polling = true;
                }
            }
            check_pending = keep_polling;
        }

        if notify.is_stopped() {
            return;
        }
        if check_pending {
            tokio::select! {
                biased;
                _ = notify.wake.notified() => {
                    check_pending = true;
                }
                _ = tokio::time::sleep(SAGA_WORKER_ACTIVE_POLL) => {
                    check_pending = true;
                }
            }
        } else {
            tokio::select! {
                biased;
                _ = notify.wake.notified() => {
                    check_pending = true;
                }
                _ = tokio::time::sleep(SAGA_WORKER_IDLE_POLL) => {
                    check_pending = true;
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "saga_worker_tests.rs"]
mod tests;
