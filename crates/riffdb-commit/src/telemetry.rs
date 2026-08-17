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

/// Closed reason the coordinator stopped filling one command group.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CommitGroupDispatchReason {
    /// The maximum safe command count was selected.
    Full,
    /// A non-deferrable message fixed the ordering boundary.
    Barrier,
    /// Every message immediately available after the oldest command was drained.
    QueueDrained,
    /// Every sender was closed while the actor drained accepted work.
    ReceiverClosed,
}

/// Closed coordinator CPU stage used for redaction-safe decomposition.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CommandPipelineStage {
    /// Durable admission selection and transition.
    Admission,
    /// Exact FIFO compatibility partition construction.
    Compatibility,
    /// Snapshot materialization and deterministic runtime evaluation.
    Evaluation,
    /// Transaction-current validation, encoding, and backend staging.
    ValidationEncodingStaging,
    /// Post-commit first-commit publication.
    Publication,
}

/// Closed cause for rolling back one unpublished prepared-command epoch.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum PreparedEpochRollbackReason {
    /// A preparation worker stopped or panicked before returning its result.
    WorkerFailure,
    /// A returned body's identity, ordinal, frontier, or digest did not join.
    ProofMismatch,
    /// Fresh transaction-current state invalidated prepared evidence.
    CurrentStateChanged,
    /// Request cancellation or deadline closed the epoch before apply.
    Cancelled,
}

/// Closed phase of the bounded ordered-completion lane.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CompletionLanePhase {
    /// One unpublished unit entered the bounded FIFO lane.
    Submitted,
    /// The oldest unit's checked fence and publication completed.
    Published,
    /// The apply writer waited for the complete submitted prefix.
    Drained,
    /// Shutdown joined the empty completion owner.
    Shutdown,
}

/// One closed semantic observation from the sole-writer command path.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommitTelemetryEvent {
    /// One fixed-cardinality completion-lane state observation.
    CompletionLaneObserved {
        /// Closed lifecycle phase.
        phase: CompletionLanePhase,
        /// Submitted units not yet acknowledged by the apply writer.
        depth: u16,
        /// Ready units held behind an older FIFO predecessor. The selected
        /// one-owner design keeps this at zero by construction.
        reorder_occupancy: u16,
        /// Residence or drain duration for this phase.
        elapsed: Duration,
    },
    /// One bounded preparation-pool depth observation.
    PreparationPoolDepthObserved {
        /// Accepted tasks queued or executing under the fixed pool ceiling.
        depth: u16,
    },
    /// One bounded admission-ordinal reorder-buffer occupancy observation.
    ReorderBufferOccupancyObserved {
        /// Completed bodies waiting for a missing earlier ordinal.
        occupancy: u16,
    },
    /// One complete unpublished prepared epoch was rolled back.
    PreparedEpochRolledBack {
        /// Closed failure class; no command or application identity is retained.
        reason: PreparedEpochRollbackReason,
    },
    /// The applied epoch was compared with its private logical frontier.
    FrontierEquivalenceChecked {
        /// True only when every bounded authoritative component matched.
        equivalent: bool,
    },
    /// One bounded command group completed a coordinator CPU stage.
    CommandPipelineStageCompleted {
        /// Closed stage identity; no application values are retained.
        stage: CommandPipelineStage,
        /// Commands represented by this observation.
        command_count: u16,
        /// Wall duration of this coordinator stage.
        elapsed: Duration,
    },
    /// The scheduler dispatched one ordered command group.
    CommandGroupDispatched {
        /// Closed reason collection stopped.
        reason: CommitGroupDispatchReason,
        /// Commands selected into the group.
        selected: u16,
        /// Accepted messages retained in actor-local order after selection.
        deferred: u16,
        /// Time spent collecting after receiving the oldest groupable command.
        elapsed: Duration,
    },
    /// One intake group was partitioned into exact compatible durable groups.
    CommandGroupPartitioned {
        /// Commands presented to the compatibility partitioner.
        selected: u16,
        /// Non-empty compatible groups emitted in stable FIFO order.
        completion_groups: u16,
        /// Group boundaries caused by an overlapping declared conflict key.
        conflict_key_splits: u16,
        /// Group boundaries caused by exact entity read/write overlap.
        exact_access_splits: u16,
        /// Compatibility groups selected for one compiler-proved shared conflict lease.
        commutative_shared_groups: u16,
    },
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
        /// Closed result of the storage call and immediate exact-result check.
        terminal: CommitCallTerminal,
        /// Time spent in the commit call and immediate exact-result check.
        elapsed: Duration,
        /// Commands in the staged batch. The POC command path is exactly one.
        batch_size: u16,
    },
    /// A deferred command group completed final authoritative apply.
    CommitApplicationCompleted {
        /// Time spent applying the already staged group to writer-private state.
        elapsed: Duration,
        /// Commands represented by the applied group.
        batch_size: u16,
    },
    /// A deferred command group completed final apply, encoding, and journal submission.
    CommitSubmissionCompleted {
        /// Time from final commit entry through receipt creation.
        elapsed: Duration,
        /// Commands represented by the submitted journal frame.
        batch_size: u16,
    },
    /// A mandatory same-key uncertainty lookup returned.
    UncertaintyResolved {
        /// Durable phase that reported unknown status.
        stage: CommitUncertaintyStage,
        /// Closed same-key lookup result.
        resolution: CommitUncertaintyResolution,
    },
    /// One writer unit completed; reports busy and preceding idle durations.
    WriterUnitCompleted {
        /// Time spent executing the unit (storage + completion).
        busy: Duration,
        /// Time the writer waited idle before this unit.
        idle: Duration,
        /// Latest EWMA queue-delay estimate in microseconds after this unit.
        queue_delay_estimate_micros: u64,
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
