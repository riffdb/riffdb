//! Closed, redaction-safe storage diagnostics and process-test failpoints.

use std::collections::VecDeque;
#[cfg(feature = "test-fixtures")]
use std::io::Write;
#[cfg(feature = "test-fixtures")]
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use riffdb_storage_api::{StorageError, StorageErrorKind};

/// Closed operation classes exposed only to recovery-test composition.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedbTestOperation {
    Initialization,
    Admission,
    ExecutionFailure,
    CommandBatch,
    /// Complete command graph applied without a durable/publication fence.
    DeferredCommandBatch,
    /// Immediate empty tail that makes every prior deferred subgroup durable.
    CommandEpochTail,
    StorageFormatMigrationBatch,
    ContractMigrationBatch,
    ContractMigrationCutover,
    IndexMigrationBatch,
    CatalogAdministration,
    /// Immutable query-module activation.
    QueryModuleAdministration,
    /// Immutable reactive-module publication.
    ReactiveModuleAdministration,
    /// Durable exact application-installation campaign transition.
    ApplicationInstallationCampaign,
    /// Durable exact application-export operation transition.
    ApplicationExportOperation,
    /// Durable event-consumer state transition.
    EventConsumerTransition,
    CapabilityAdministration,
    CapabilityBootstrap,
    ServiceAudit,
    OutboxTransition,
    ProjectionMutation,
    /// Specialized schema-bound columnar expected-control transition.
    ColumnarProjectionControl,
    Backup,
    Restore,
    /// Validated-prefix startup checkpoint write (ADR-0085 A1).
    ValidatedPrefixCheckpoint,
    /// Final ADR-0157 graceful-close lifecycle transaction.
    CleanCloseLifecycle,
    /// Logical start/end of the durable suffix and journal-header barrier.
    GracefulCloseBarrier,
    /// Mid-barrier point after suffix materialization and before header proof.
    GracefulCloseBarrierSuffix,
    /// Logical start/end of immutable checkpoint classification.
    GracefulCheckpointClassification,
    /// Offline retention hold add/remove (ADR-0085 A2).
    RetentionHold,
    /// Offline retention prune: first transaction deletes the validated-prefix checkpoint.
    RetentionPruneCheckpointDelete,
    /// Offline retention prune: one sub-range delete+tombstone+watermark transaction.
    RetentionPruneSubrange,
}

/// Closed transaction boundary exposed by redaction-safe test diagnostics.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedbTestPhase {
    BeforeEngineCommit,
    AfterEngineCommit,
}

/// One diagnostic event containing no engine text or application data.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RedbTestEvent {
    operation: RedbTestOperation,
    phase: RedbTestPhase,
}

impl RedbTestEvent {
    #[must_use]
    pub const fn operation(self) -> RedbTestOperation {
        self.operation
    }

    #[must_use]
    pub const fn phase(self) -> RedbTestPhase {
        self.phase
    }
}

/// Closed controller for process-level recovery tests.
///
/// Production opens never install this controller. Its constructors admit only
/// fixed actions at fixed storage boundaries; arbitrary callbacks and data are
/// deliberately unsupported.
#[doc(hidden)]
#[derive(Clone)]
pub struct RedbTestController {
    inner: Arc<TestControllerInner>,
}

struct TestControllerInner {
    /// Closed failpoint sequence for one operation. Each matching phase hit
    /// consumes the front step whose phase matches; unmatched phases are ignored.
    operation: Option<RedbTestOperation>,
    steps: Mutex<VecDeque<(RedbTestPhase, FailpointAction)>>,
    events: Mutex<Vec<RedbTestEvent>>,
    index_migration: Mutex<IndexMigrationObservation>,
    audit_sequence_begin_reads: AtomicU64,
    fresh_locator_history_fallback_scans: AtomicU64,
    corrupt_fresh_locator_successor_stamp: AtomicBool,
    #[cfg(feature = "test-fixtures")]
    external_kill_barrier: Option<PathBuf>,
    #[cfg(feature = "test-fixtures")]
    external_engine_sync_armed: AtomicU64,
}

#[derive(Clone, Copy, Default)]
struct IndexMigrationObservation {
    pages: usize,
    v1_rewrites: usize,
    v2_confirms: usize,
}

#[derive(Clone, Copy)]
enum FailpointAction {
    ReturnBeforeCommit,
    ReturnUnknownAfterCommit,
    AbortProcess,
    #[cfg(feature = "test-fixtures")]
    WaitForExternalKillBeforeCommit,
    #[cfg(feature = "test-fixtures")]
    ArmExternalKillAfterEngineSync,
}

impl RedbTestController {
    /// Observes bounded migration control flow without arming a failure.
    #[must_use]
    pub fn observe_index_migration() -> Self {
        Self {
            inner: Arc::new(TestControllerInner {
                operation: None,
                steps: Mutex::new(VecDeque::new()),
                events: Mutex::new(Vec::new()),
                index_migration: Mutex::new(IndexMigrationObservation::default()),
                audit_sequence_begin_reads: AtomicU64::new(0),
                fresh_locator_history_fallback_scans: AtomicU64::new(0),
                corrupt_fresh_locator_successor_stamp: AtomicBool::new(false),
                #[cfg(feature = "test-fixtures")]
                external_kill_barrier: None,
                #[cfg(feature = "test-fixtures")]
                external_engine_sync_armed: AtomicU64::new(0),
            }),
        }
    }

    /// Installs a controller that counts service-audit sequence begin_read calls.
    #[must_use]
    pub fn count_audit_sequence_begin_reads() -> Self {
        Self::observe_index_migration()
    }

    /// Deprecated alias — prefer [`Self::count_audit_sequence_begin_reads`].
    #[must_use]
    pub fn observe_audit_sequence_reads() -> Self {
        Self::count_audit_sequence_begin_reads()
    }

    /// Returns how many begin_read calls service-audit sequence lookup performed.
    #[must_use]
    pub fn audit_sequence_begin_reads(&self) -> u64 {
        self.inner
            .audit_sequence_begin_reads
            .load(Ordering::Relaxed)
    }

    /// Returns how many operational idempotency misses used history fallback.
    #[must_use]
    pub fn fresh_locator_history_fallback_scans(&self) -> u64 {
        self.inner
            .fresh_locator_history_fallback_scans
            .load(Ordering::Relaxed)
    }

    pub(crate) fn observe_fresh_locator_history_fallback_scan(&self) {
        let _ = self
            .inner
            .fresh_locator_history_fallback_scans
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_add(1))
            });
    }

    /// Corrupts the successor allocator after coverage proof construction.
    #[must_use]
    pub fn corrupt_fresh_locator_successor_stamp_once() -> Self {
        let controller = Self::observe_index_migration();
        controller
            .inner
            .corrupt_fresh_locator_successor_stamp
            .store(true, Ordering::Release);
        controller
    }

    #[cfg(test)]
    pub(crate) fn take_fresh_locator_successor_stamp_corruption(&self) -> bool {
        self.inner
            .corrupt_fresh_locator_successor_stamp
            .swap(false, Ordering::AcqRel)
    }

    /// Injects one proven-not-committed storage failure.
    #[must_use]
    pub fn return_before_commit(operation: RedbTestOperation) -> Self {
        Self::new(
            operation,
            RedbTestPhase::BeforeEngineCommit,
            FailpointAction::ReturnBeforeCommit,
        )
    }

    /// Commits once, then returns an uncertain result and fences later writes.
    #[must_use]
    pub fn return_unknown_after_commit(operation: RedbTestOperation) -> Self {
        Self::new(
            operation,
            RedbTestPhase::AfterEngineCommit,
            FailpointAction::ReturnUnknownAfterCommit,
        )
    }

    /// Aborts the current child process before the selected engine commit.
    #[must_use]
    pub fn abort_before_commit(operation: RedbTestOperation) -> Self {
        Self::new(
            operation,
            RedbTestPhase::BeforeEngineCommit,
            FailpointAction::AbortProcess,
        )
    }

    /// Aborts the current child process after the selected engine commit.
    #[must_use]
    pub fn abort_after_commit(operation: RedbTestOperation) -> Self {
        Self::new(
            operation,
            RedbTestPhase::AfterEngineCommit,
            FailpointAction::AbortProcess,
        )
    }

    /// Arms a deterministic mid-commit barrier for a process-level kill test.
    ///
    /// The matching command arms the closed real-file backend. In a two-phase
    /// redb commit that backend delegates and synchronizes the secondary-slot
    /// phase, then blocks before delegating the final engine sync after the
    /// primary-header write. It publishes the fixed marker only at that
    /// structural boundary. The owning runner must kill that
    /// exact process. There is no injected error, self-abort, simulated repair,
    /// or success return.
    #[must_use]
    #[cfg(feature = "test-fixtures")]
    pub fn wait_before_final_engine_sync_for_external_kill(
        operation: RedbTestOperation,
        marker: impl Into<PathBuf>,
    ) -> Self {
        Self {
            inner: Arc::new(TestControllerInner {
                operation: Some(operation),
                steps: Mutex::new(VecDeque::from([(
                    RedbTestPhase::BeforeEngineCommit,
                    FailpointAction::ArmExternalKillAfterEngineSync,
                )])),
                events: Mutex::new(Vec::new()),
                index_migration: Mutex::new(IndexMigrationObservation::default()),
                audit_sequence_begin_reads: AtomicU64::new(0),
                fresh_locator_history_fallback_scans: AtomicU64::new(0),
                corrupt_fresh_locator_successor_stamp: AtomicBool::new(false),
                external_kill_barrier: Some(marker.into()),
                external_engine_sync_armed: AtomicU64::new(0),
            }),
        }
    }

    /// Blocks at the selected semantic pre-commit boundary until the owning
    /// process runner sends SIGKILL.
    ///
    /// Unlike the real-backend mid-commit fixture, this seam does not attempt
    /// to force an engine repair. It provides a deterministic real-daemon
    /// crash boundary while the command is still unresolved, so recovery may
    /// truthfully select either complete atomic outcome.
    #[must_use]
    #[cfg(feature = "test-fixtures")]
    pub fn wait_before_commit_for_external_kill(
        operation: RedbTestOperation,
        marker: impl Into<PathBuf>,
    ) -> Self {
        Self {
            inner: Arc::new(TestControllerInner {
                operation: Some(operation),
                steps: Mutex::new(VecDeque::from([(
                    RedbTestPhase::BeforeEngineCommit,
                    FailpointAction::WaitForExternalKillBeforeCommit,
                )])),
                events: Mutex::new(Vec::new()),
                index_migration: Mutex::new(IndexMigrationObservation::default()),
                audit_sequence_begin_reads: AtomicU64::new(0),
                fresh_locator_history_fallback_scans: AtomicU64::new(0),
                corrupt_fresh_locator_successor_stamp: AtomicBool::new(false),
                external_kill_barrier: Some(marker.into()),
                external_engine_sync_armed: AtomicU64::new(0),
            }),
        }
    }

    /// First matching before-commit returns Unavailable; the second aborts the process.
    ///
    /// Retained for explicit checkpoint-writer compatibility fixtures.
    #[must_use]
    pub fn return_before_then_abort_before(operation: RedbTestOperation) -> Self {
        Self::sequence(
            operation,
            &[
                (
                    RedbTestPhase::BeforeEngineCommit,
                    FailpointAction::ReturnBeforeCommit,
                ),
                (
                    RedbTestPhase::BeforeEngineCommit,
                    FailpointAction::AbortProcess,
                ),
            ],
        )
    }

    /// First matching before-commit returns Unavailable; the next after-commit aborts.
    ///
    /// Retained for explicit checkpoint-writer compatibility fixtures.
    #[must_use]
    pub fn return_before_then_abort_after(operation: RedbTestOperation) -> Self {
        Self::sequence(
            operation,
            &[
                (
                    RedbTestPhase::BeforeEngineCommit,
                    FailpointAction::ReturnBeforeCommit,
                ),
                (
                    RedbTestPhase::AfterEngineCommit,
                    FailpointAction::AbortProcess,
                ),
            ],
        )
    }

    /// Returns the bounded redaction-safe event history.
    #[must_use]
    pub fn events(&self) -> Vec<RedbTestEvent> {
        self.inner
            .events
            .lock()
            .map_or_else(|_| Vec::new(), |events| events.clone())
    }

    /// Returns page, V1-rewrite, and V2-confirm counts without row material.
    #[must_use]
    pub fn index_migration_observation(&self) -> (usize, usize, usize) {
        self.inner
            .index_migration
            .lock()
            .map_or((0, 0, 0), |value| {
                (value.pages, value.v1_rewrites, value.v2_confirms)
            })
    }

    fn new(operation: RedbTestOperation, phase: RedbTestPhase, action: FailpointAction) -> Self {
        Self::sequence(operation, &[(phase, action)])
    }

    fn sequence(operation: RedbTestOperation, steps: &[(RedbTestPhase, FailpointAction)]) -> Self {
        Self {
            inner: Arc::new(TestControllerInner {
                operation: Some(operation),
                steps: Mutex::new(steps.iter().copied().collect()),
                events: Mutex::new(Vec::new()),
                index_migration: Mutex::new(IndexMigrationObservation::default()),
                audit_sequence_begin_reads: AtomicU64::new(0),
                fresh_locator_history_fallback_scans: AtomicU64::new(0),
                corrupt_fresh_locator_successor_stamp: AtomicBool::new(false),
                #[cfg(feature = "test-fixtures")]
                external_kill_barrier: None,
                #[cfg(feature = "test-fixtures")]
                external_engine_sync_armed: AtomicU64::new(0),
            }),
        }
    }

    pub(crate) fn observe_index_migration_page(&self) {
        if let Ok(mut observation) = self.inner.index_migration.lock() {
            observation.pages = observation.pages.saturating_add(1);
        }
    }

    pub(crate) fn observe_index_migration_batch(&self, v1_rewrites: usize, v2_confirms: usize) {
        if let Ok(mut observation) = self.inner.index_migration.lock() {
            observation.v1_rewrites = observation.v1_rewrites.saturating_add(v1_rewrites);
            observation.v2_confirms = observation.v2_confirms.saturating_add(v2_confirms);
        }
    }

    pub(crate) fn observe_audit_sequence_begin_read(&self) {
        let _ = self
            .inner
            .audit_sequence_begin_reads
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn before_commit(&self, operation: RedbTestOperation) -> Result<(), StorageError> {
        self.observe(operation, RedbTestPhase::BeforeEngineCommit);
        if let Some(action) = self.take_action(operation, RedbTestPhase::BeforeEngineCommit) {
            match action {
                FailpointAction::ReturnBeforeCommit => {
                    return Err(StorageError::new(StorageErrorKind::Unavailable, None));
                }
                FailpointAction::AbortProcess => std::process::abort(),
                #[cfg(feature = "test-fixtures")]
                FailpointAction::WaitForExternalKillBeforeCommit => {
                    self.publish_external_kill_barrier()
                        .map_err(|_| StorageError::new(StorageErrorKind::Unavailable, None))?;
                }
                #[cfg(feature = "test-fixtures")]
                FailpointAction::ArmExternalKillAfterEngineSync => {
                    self.inner
                        .external_engine_sync_armed
                        .store(2, Ordering::Release);
                }
                FailpointAction::ReturnUnknownAfterCommit => {}
            }
        }
        Ok(())
    }

    pub(crate) fn after_commit(&self, operation: RedbTestOperation) -> Result<(), StorageError> {
        self.observe(operation, RedbTestPhase::AfterEngineCommit);
        if let Some(action) = self.take_action(operation, RedbTestPhase::AfterEngineCommit) {
            match action {
                FailpointAction::ReturnUnknownAfterCommit => {
                    return Err(StorageError::new(
                        StorageErrorKind::CommitStatusUnknown,
                        None,
                    ));
                }
                FailpointAction::AbortProcess => std::process::abort(),
                FailpointAction::ReturnBeforeCommit => {}
                #[cfg(feature = "test-fixtures")]
                FailpointAction::WaitForExternalKillBeforeCommit
                | FailpointAction::ArmExternalKillAfterEngineSync => {
                    return Err(StorageError::new(
                        StorageErrorKind::InvariantViolation,
                        None,
                    ));
                }
            }
        }
        Ok(())
    }

    fn observe(&self, operation: RedbTestOperation, phase: RedbTestPhase) {
        const MAX_TEST_EVENTS: usize = 256;
        if let Ok(mut events) = self.inner.events.lock()
            && events.len() < MAX_TEST_EVENTS
        {
            events.push(RedbTestEvent { operation, phase });
        }
    }

    fn take_action(
        &self,
        operation: RedbTestOperation,
        phase: RedbTestPhase,
    ) -> Option<FailpointAction> {
        if self.inner.operation != Some(operation) {
            return None;
        }
        let mut steps = self.inner.steps.lock().ok()?;
        let front = steps.front()?;
        if front.0 != phase {
            return None;
        }
        steps.pop_front().map(|(_, action)| action)
    }

    #[cfg(feature = "test-fixtures")]
    pub(crate) fn wait_before_engine_sync_if_armed(&self) -> Result<(), std::io::Error> {
        if self
            .inner
            .external_engine_sync_armed
            .load(Ordering::Acquire)
            != 1
        {
            return Ok(());
        }
        self.inner
            .external_engine_sync_armed
            .store(0, Ordering::Release);
        self.publish_external_kill_barrier()
    }

    #[cfg(feature = "test-fixtures")]
    fn publish_external_kill_barrier(&self) -> Result<(), std::io::Error> {
        let marker = self
            .inner
            .external_kill_barrier
            .as_ref()
            .ok_or_else(|| std::io::Error::other("external-kill barrier unavailable"))?;
        let staging = marker.with_extension("arming");
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&staging)?;
        file.write_all(b"armed\n").and_then(|()| file.sync_all())?;
        std::fs::rename(staging, marker)?;
        loop {
            std::thread::park();
        }
    }

    #[cfg(feature = "test-fixtures")]
    pub(crate) fn note_engine_sync_completed_if_armed(&self) {
        let _ = self.inner.external_engine_sync_armed.compare_exchange(
            2,
            1,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "test-fixtures")]
    const EXTERNAL_KILL_CHILD: &str = "RIFFDB_HOOK_EXTERNAL_KILL_CHILD";
    #[cfg(feature = "test-fixtures")]
    const EXTERNAL_KILL_MARKER: &str = "RIFFDB_HOOK_EXTERNAL_KILL_MARKER";

    #[test]
    fn fixed_failpoint_fires_once_and_records_only_closed_events() {
        let controller = RedbTestController::return_before_commit(RedbTestOperation::CommandBatch);
        assert_eq!(
            controller
                .before_commit(RedbTestOperation::CommandBatch)
                .expect_err("armed failpoint")
                .kind(),
            StorageErrorKind::Unavailable
        );
        controller
            .before_commit(RedbTestOperation::CommandBatch)
            .expect("one-shot failpoint already fired");
        assert_eq!(controller.events().len(), 2);
        assert!(controller.events().iter().all(|event| {
            event.operation() == RedbTestOperation::CommandBatch
                && event.phase() == RedbTestPhase::BeforeEngineCommit
        }));
    }

    // req: PERF-014
    #[cfg(feature = "test-fixtures")]
    #[test]
    fn before_commit_barrier_requires_an_external_process_kill() {
        if let Some(mode) = std::env::var_os(EXTERNAL_KILL_CHILD) {
            let marker =
                std::env::var_os(EXTERNAL_KILL_MARKER).expect("external-kill child marker path");
            let controller = if mode == "semantic" {
                RedbTestController::wait_before_commit_for_external_kill(
                    RedbTestOperation::DeferredCommandBatch,
                    marker,
                )
            } else {
                RedbTestController::wait_before_final_engine_sync_for_external_kill(
                    RedbTestOperation::CommandBatch,
                    marker,
                )
            };
            controller
                .before_commit(if mode == "semantic" {
                    RedbTestOperation::DeferredCommandBatch
                } else {
                    RedbTestOperation::CommandBatch
                })
                .expect("arm the external-kill barrier");
            if mode != "semantic" {
                controller.note_engine_sync_completed_if_armed();
                controller
                    .wait_before_engine_sync_if_armed()
                    .expect("publish the synchronized external-kill marker");
            }
            panic!("external-kill barrier returned");
        }

        for mode in ["engine", "semantic"] {
            let scope = crate::test_path::ScopedDirectory::new("hook-external-kill");
            let marker = scope.join(if mode == "engine" {
                "engine.ready"
            } else {
                "semantic.ready"
            });
            let mut child = std::process::Command::new(
                std::env::current_exe().expect("current hook test executable"),
            )
            .arg("--exact")
            .arg("hooks::tests::before_commit_barrier_requires_an_external_process_kill")
            .arg("--nocapture")
            .env(EXTERNAL_KILL_CHILD, mode)
            .env(EXTERNAL_KILL_MARKER, &marker)
            .spawn()
            .expect("spawn external-kill barrier child");
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            while !marker.exists() {
                assert!(
                    std::time::Instant::now() < deadline,
                    "child never reached the before-commit barrier"
                );
                std::thread::yield_now();
            }
            assert_eq!(
                std::fs::read(&marker).expect("read synchronized barrier marker"),
                b"armed\n"
            );
            child
                .kill()
                .expect("externally kill the exact child process");
            let status = child.wait().expect("wait for externally killed child");
            assert!(!status.success());
            std::fs::remove_file(marker).expect("remove external-kill marker");
        }
    }
}
