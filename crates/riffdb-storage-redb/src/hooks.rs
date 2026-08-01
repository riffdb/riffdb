//! Closed, redaction-safe storage diagnostics and process-test failpoints.

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
    StorageFormatMigrationBatch,
    IndexMigrationBatch,
    CatalogAdministration,
    /// Immutable query-module activation.
    QueryModuleAdministration,
    CapabilityAdministration,
    CapabilityBootstrap,
    ServiceAudit,
    OutboxTransition,
    ProjectionMutation,
    Backup,
    Restore,
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
    armed: Option<ArmedFailpoint>,
    fired: AtomicBool,
    events: Mutex<Vec<RedbTestEvent>>,
    index_migration: Mutex<IndexMigrationObservation>,
    audit_sequence_begin_reads: AtomicU64,
}

#[derive(Clone, Copy, Default)]
struct IndexMigrationObservation {
    pages: usize,
    v1_rewrites: usize,
    v2_confirms: usize,
}

#[derive(Clone, Copy)]
struct ArmedFailpoint {
    operation: RedbTestOperation,
    phase: RedbTestPhase,
    action: FailpointAction,
}

#[derive(Clone, Copy)]
enum FailpointAction {
    ReturnBeforeCommit,
    ReturnUnknownAfterCommit,
    AbortProcess,
}

impl RedbTestController {
    /// Observes bounded migration control flow without arming a failure.
    #[must_use]
    pub fn observe_index_migration() -> Self {
        Self {
            inner: Arc::new(TestControllerInner {
                armed: None,
                fired: AtomicBool::new(false),
                events: Mutex::new(Vec::new()),
                index_migration: Mutex::new(IndexMigrationObservation::default()),
                audit_sequence_begin_reads: AtomicU64::new(0),
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
        Self {
            inner: Arc::new(TestControllerInner {
                armed: Some(ArmedFailpoint {
                    operation,
                    phase,
                    action,
                }),
                fired: AtomicBool::new(false),
                events: Mutex::new(Vec::new()),
                index_migration: Mutex::new(IndexMigrationObservation::default()),
                audit_sequence_begin_reads: AtomicU64::new(0),
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
        let armed = self.inner.armed?;
        if armed.operation != operation || armed.phase != phase {
            return None;
        }
        self.inner
            .fired
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .ok()
            .map(|_| armed.action)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
