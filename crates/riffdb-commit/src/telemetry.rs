//! Closed, payload-free semantic telemetry owned by commit orchestration.

use std::time::Duration;

use riffdb_types::{CommandId, ServiceIngressKindV1};

use crate::CommandExecutionErrorKind;

/// Durable or process-local phase whose uncertain write required same-key recovery.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CommitUncertaintyStage {
    /// Initial idempotency admission.
    Admission,
    /// Successful command commit.
    CommandCommit,
    /// Deterministic execution-failure terminalization.
    ExecutionFailure,
}

/// Closed result of one mandatory uncertainty-recovery lookup.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CommitUncertaintyResolution {
    /// The exact expected outcome was durable.
    Outcome,
    /// A deterministic execution failure was durable.
    ExecutionFailure,
    /// The exact pending admission proved no later transition committed.
    ProvenPending,
    /// Durable state still could not distinguish commit from rollback.
    StillUnknown,
    /// Durable state contradicted the retained same-attempt evidence.
    Integrity,
}

/// Closed result of one exact authoritative command commit call.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CommitCallTerminal {
    /// The expected one-command batch committed.
    Committed,
    /// Storage proved no commit occurred.
    ProvenAbort,
    /// Storage could not prove commit or rollback.
    StatusUnknown,
    /// A successful storage response contradicted the staged graph.
    Integrity,
}

/// Closed command disposition emitted after the actor finishes one accepted item.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CommitCommandTerminal {
    /// An unjournaled read-only command completed successfully.
    ReadOnlySucceeded,
    /// A new authoritative application commit completed.
    FirstCommit,
    /// An equal-input durable outcome was replayed.
    OutcomeReplay,
    /// A dependency-validated deterministic failure completed or replayed.
    ExecutionFailed,
    /// Durable state selected another immutable historical preparation.
    PreparationChanged,
    /// The same idempotency identity retained different canonical input.
    InputMismatch,
    /// The command failed with a closed coordinator classification.
    Failed(CommandExecutionErrorKind),
}

/// Closed idempotency disposition known during normal command admission.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CommitIdempotencyObservation {
    /// Equal input selected a previously durable terminal state.
    Hit,
    /// The same idempotency identity retained different canonical input.
    Mismatch,
}

/// One closed semantic observation from the sole-writer command path.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommitTelemetryEvent {
    /// An accepted command reached the actor after waiting in the bounded queue.
    StorageQueueCompleted {
        /// Exact command identity from the checked executable-plan reference.
        command_id: CommandId,
        /// Trusted ingress copied into the process-local command preparation.
        ingress: ServiceIngressKindV1,
        /// Time from synchronous queue submission to actor execution.
        elapsed: Duration,
    },
    /// The actor completed one accepted command.
    CommandTerminal {
        /// Exact command identity from the checked executable-plan reference.
        command_id: CommandId,
        /// Trusted ingress copied into the process-local command preparation.
        ingress: ServiceIngressKindV1,
        /// Closed terminal disposition.
        terminal: CommitCommandTerminal,
        /// Time from synchronous queue submission through actor completion.
        elapsed: Duration,
    },
    /// Normal admission observed a terminal idempotency disposition.
    IdempotencyObserved {
        /// Hit or input mismatch only.
        observation: CommitIdempotencyObservation,
    },
    /// The exact storage commit call returned.
    CommitCallCompleted {
        /// Closed result of the storage call and exact graph check.
        terminal: CommitCallTerminal,
        /// Time spent in the synchronous commit call and immediate exact-result check.
        elapsed: Duration,
        /// Commands in the staged batch. The POC command path is exactly one.
        batch_size: u16,
        /// Whether synchronous durability was requested.
        synchronous: bool,
    },
    /// A mandatory same-key uncertainty lookup returned.
    UncertaintyResolved {
        /// Durable phase that reported unknown status.
        stage: CommitUncertaintyStage,
        /// Closed same-key lookup result.
        resolution: CommitUncertaintyResolution,
    },
}

/// Least-authority sink for commit-owned semantic events.
pub trait CommitTelemetry: Send + Sync {
    /// Records one closed event without command input, keys, credentials, or errors.
    fn record(&self, event: CommitTelemetryEvent);
}

/// No-op sink used by isolated tests and compositions without observability.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopCommitTelemetry;

impl CommitTelemetry for NoopCommitTelemetry {
    fn record(&self, _event: CommitTelemetryEvent) {}
}
