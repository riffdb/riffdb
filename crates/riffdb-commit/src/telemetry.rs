//! Commit telemetry vocabulary owned by the observability leaf.

pub use riffdb_observability::{
    CommandPipelineStage, CommitCallTerminal, CommitCommandTerminal, CommitExecutionFailureKind,
    CommitGroupDispatchReason, CommitIdempotencyObservation, CommitTelemetry, CommitTelemetryEvent,
    CommitUncertaintyResolution, CommitUncertaintyStage, CompletionLanePhase, NoopCommitTelemetry,
    PreparedEpochRollbackReason,
};

impl From<crate::CommandExecutionErrorKind> for CommitExecutionFailureKind {
    fn from(kind: crate::CommandExecutionErrorKind) -> Self {
        match kind {
            crate::CommandExecutionErrorKind::AuthorizationDenied => Self::AuthorizationDenied,
            crate::CommandExecutionErrorKind::Cancelled => Self::Cancelled,
            crate::CommandExecutionErrorKind::DeadlineExceeded => Self::DeadlineExceeded,
            crate::CommandExecutionErrorKind::RetryBudgetExhausted => Self::RetryBudgetExhausted,
            crate::CommandExecutionErrorKind::StorageUnavailable => Self::StorageUnavailable,
            crate::CommandExecutionErrorKind::OutcomeUnknown => Self::OutcomeUnknown,
            crate::CommandExecutionErrorKind::InternalDefect => Self::InternalDefect,
            crate::CommandExecutionErrorKind::CoordinatorStopped => Self::CoordinatorStopped,
            crate::CommandExecutionErrorKind::CoordinatorFenced => Self::CoordinatorFenced,
        }
    }
}
