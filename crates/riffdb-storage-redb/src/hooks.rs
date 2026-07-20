//! Closed, redaction-safe storage diagnostics and process-test failpoints.

use std::sync::atomic::{AtomicBool, Ordering};
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
    CatalogAdministration,
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
    armed: ArmedFailpoint,
    fired: AtomicBool,
    events: Mutex<Vec<RedbTestEvent>>,
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

    fn new(operation: RedbTestOperation, phase: RedbTestPhase, action: FailpointAction) -> Self {
        Self {
            inner: Arc::new(TestControllerInner {
                armed: ArmedFailpoint {
                    operation,
                    phase,
                    action,
                },
                fired: AtomicBool::new(false),
                events: Mutex::new(Vec::new()),
            }),
        }
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
        let armed = self.inner.armed;
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
