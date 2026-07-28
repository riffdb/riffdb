//! Actor-owned completion of admitted command attempts.

use std::{error::Error, fmt, time::Instant};

use riffdb_conflict::{ConflictError, ConflictManager};
use riffdb_idempotency::IdempotencyRecheckError;
use riffdb_storage_api::{
    AdmissionRepository, ApplicationCommandTransactionPort, DurabilityMode,
    ExecutionFailureTransitionPort, SnapshotReader, StorageError, StorageErrorKind,
};
use riffdb_types::ExecutionFailureCode;

use crate::{
    AdmissionClock, AdmissionClockError, CommandExecutionPreparation, CommitCallTerminal,
    CommitIdempotencyObservation, CommitTelemetry, CommitTelemetryEvent,
    CommitUncertaintyResolution, CommitUncertaintyStage, CommittedOutcome, ProvenanceIdSource,
    ProvenanceIdSourceError,
    command_admission::{
        CommandAdmissionError, CommandAdmissionResult, UncertainCommandAdmissionResolution,
        reduce_command_admission, resolve_uncertain_command_admission,
    },
    command_attempt::{
        CommandAttemptError, CommandAttemptResolution, EvaluatedCommandAttempt,
        ExecutionFaultAttempt, PendingCommandAttempts, RolledBackCandidateDisposition,
        evaluate_next_command_attempt,
    },
    command_execution_failure::{
        ExecutionFailureCurrentDecision, ExecutionFailureTerminalizeResult,
        ExecutionFailureTransitionStart, UncertainExecutionFailureResolution,
        begin_execution_failure_transition, resolve_uncertain_execution_failure,
    },
    command_index::{
        CheckedAffectedEpochDecision, CheckedAssignDecision, CheckedCommitCandidate,
        CheckedReserveDecision, derive_checked_command_indexes,
    },
    command_records::{
        CheckedCommandCommitResult, CheckedCommandStageError, UncertainCommandCommitResolution,
        build_and_stage_checked_candidate, resolve_uncertain_command_commit,
    },
    command_validation::{
        CheckedCandidateDecision, CommandCandidateChainStart, TransactionCurrentAttemptDecision,
        begin_bound_command_candidate, validate_checked_transaction_current,
    },
    read_only_execution::{ReadOnlyExecutionCoreError, ReadOnlyExecutionCoreErrorKind},
};

/// Explicit production durability selected when the coordinator is constructed.
///
/// The durable storage format also represents `Memory`, but that mode cannot be
/// supplied through this production value.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CoordinatorDurability {
    /// Acknowledgement requires the embedded engine's synchronous durability.
    Sync,
    /// Acknowledgement follows an explicitly configured group durability policy.
    Group,
}

impl CoordinatorDurability {
    pub(super) const fn storage_mode(self) -> DurabilityMode {
        match self {
            Self::Sync => DurabilityMode::Sync,
            Self::Group => DurabilityMode::Group,
        }
    }
}

/// Closed successful result of one accepted command submission.
#[derive(Clone, Eq, PartialEq)]
pub enum CommandExecutionResult {
    /// A first commit or exact equal-input replay returned a declared outcome.
    Committed(CommittedOutcome),
    /// A dependency-validated deterministic failure consumed the admission.
    ExecutionFailed(ExecutionFailureCode),
    /// Durable idempotency state selected another immutable historical plan.
    PreparationChanged,
    /// The same idempotency identity retained different canonical input.
    InputMismatch,
}

impl fmt::Debug for CommandExecutionResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Committed(_) => "CommandExecutionResult::Committed([REDACTED])",
            Self::ExecutionFailed(_) => "CommandExecutionResult::ExecutionFailed([REDACTED])",
            Self::PreparationChanged => "CommandExecutionResult::PreparationChanged",
            Self::InputMismatch => "CommandExecutionResult::InputMismatch",
        })
    }
}

/// Redacted closed failure kind for one accepted command submission.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CommandExecutionErrorKind {
    /// Cancellation was observed at an accepted pre-transaction safe point.
    Cancelled,
    /// The request's absolute monotonic deadline elapsed at a safe point.
    DeadlineExceeded,
    /// Three complete evaluation attempt slots were consumed.
    RetryBudgetExhausted,
    /// A retryable process or storage dependency was unavailable.
    StorageUnavailable,
    /// An authoritative write cannot yet be distinguished from rollback.
    OutcomeUnknown,
    /// Checked internal state was contradictory or structurally invalid.
    InternalDefect,
    /// The sole coordinator actor stopped before this work could complete.
    CoordinatorStopped,
    /// An earlier uncertain authoritative write fenced this work.
    CoordinatorFenced,
}

impl CommandExecutionErrorKind {
    const fn safe_message(self) -> &'static str {
        match self {
            Self::Cancelled => "command execution was cancelled",
            Self::DeadlineExceeded => "command execution deadline elapsed",
            Self::RetryBudgetExhausted => "command reevaluation budget was exhausted",
            Self::StorageUnavailable => "command storage is unavailable",
            Self::OutcomeUnknown => "command outcome is not yet known",
            Self::InternalDefect => "command execution encountered an internal defect",
            Self::CoordinatorStopped => "command coordinator stopped",
            Self::CoordinatorFenced => "command coordinator fenced authoritative writes",
        }
    }
}

/// Public-safe executor failure retaining private diagnostic classification.
pub struct CommandExecutionError {
    kind: CommandExecutionErrorKind,
    #[allow(dead_code)] // Retained only for trusted telemetry; never exposed by Error::source.
    detail: CommandExecutionErrorDetail,
}

#[allow(dead_code)] // Variant payloads are retained for the trusted telemetry boundary.
enum CommandExecutionErrorDetail {
    None,
    Storage(StorageError),
    UncertainStorage {
        write: StorageError,
        lookup: Option<StorageError>,
    },
    Conflict(ConflictError),
    AdmissionClock(AdmissionClockError),
    ProvenanceSource(ProvenanceIdSourceError),
    ReadOnly(ReadOnlyExecutionCoreError),
}

impl CommandExecutionError {
    /// Returns the complete public-safe executor classification.
    #[must_use]
    pub const fn kind(&self) -> CommandExecutionErrorKind {
        self.kind
    }

    pub(super) const fn coordinator_stopped() -> Self {
        Self::without_detail(CommandExecutionErrorKind::CoordinatorStopped)
    }

    pub(super) const fn coordinator_fenced() -> Self {
        Self::without_detail(CommandExecutionErrorKind::CoordinatorFenced)
    }

    pub(super) fn from_read_only(error: ReadOnlyExecutionCoreError) -> Self {
        let kind = match error.kind() {
            ReadOnlyExecutionCoreErrorKind::Cancelled => CommandExecutionErrorKind::Cancelled,
            ReadOnlyExecutionCoreErrorKind::DeadlineExceeded => {
                CommandExecutionErrorKind::DeadlineExceeded
            }
            ReadOnlyExecutionCoreErrorKind::StorageUnavailable => {
                CommandExecutionErrorKind::StorageUnavailable
            }
            ReadOnlyExecutionCoreErrorKind::InternalDefect => {
                CommandExecutionErrorKind::InternalDefect
            }
        };
        Self {
            kind,
            detail: CommandExecutionErrorDetail::ReadOnly(error),
        }
    }

    const fn without_detail(kind: CommandExecutionErrorKind) -> Self {
        Self {
            kind,
            detail: CommandExecutionErrorDetail::None,
        }
    }

    fn with_storage(kind: CommandExecutionErrorKind, error: StorageError) -> Self {
        Self {
            kind,
            detail: CommandExecutionErrorDetail::Storage(error),
        }
    }

    fn uncertain(write: StorageError, lookup: Option<StorageError>) -> Self {
        Self {
            kind: CommandExecutionErrorKind::OutcomeUnknown,
            detail: CommandExecutionErrorDetail::UncertainStorage { write, lookup },
        }
    }
}

impl fmt::Debug for CommandExecutionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CommandExecutionError")
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for CommandExecutionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.kind.safe_message())
    }
}

impl Error for CommandExecutionError {}

/// Process-local lifecycle publication used before uncertain-state resolution.
pub(super) trait CommandExecutionLifecycle {
    /// Prevents every later authoritative submission while recovery remains uncertain.
    fn fence(&self);

    /// Stops authoritative readiness after a proven internal integrity failure.
    fn stop(&self);
}

/// Private continuation returned by a complete synchronous attempt phase.
pub(super) enum CommandDriverContinuation {
    Complete(CommandExecutionResult),
    Retry(Box<PendingCommandAttempts>),
    Failed(CommandExecutionError),
}

/// Owns admission and every bounded attempt until one accepted command completes.
#[allow(clippy::too_many_arguments)]
pub(super) async fn drive_command_execution<P>(
    port: &P,
    conflicts: &dyn ConflictManager,
    admission_clock: &dyn AdmissionClock,
    provenance: &dyn ProvenanceIdSource,
    durability: CoordinatorDurability,
    lifecycle: &dyn CommandExecutionLifecycle,
    telemetry: &dyn CommitTelemetry,
    preparation: CommandExecutionPreparation,
) -> Result<CommandExecutionResult, CommandExecutionError>
where
    P: AdmissionRepository
        + SnapshotReader
        + ApplicationCommandTransactionPort
        + ExecutionFailureTransitionPort,
{
    let candidate = match reduce_command_admission(port, admission_clock, preparation) {
        Ok(CommandAdmissionResult::Execute(candidate)) => candidate,
        Ok(CommandAdmissionResult::Outcome(outcome)) => {
            telemetry.record(CommitTelemetryEvent::IdempotencyObserved {
                observation: CommitIdempotencyObservation::Hit,
            });
            return terminal_continuation(committed_replay(outcome), lifecycle);
        }
        Ok(CommandAdmissionResult::ExecutionFailed(failure)) => {
            telemetry.record(CommitTelemetryEvent::IdempotencyObserved {
                observation: CommitIdempotencyObservation::Hit,
            });
            return terminal_continuation(execution_failure(failure.code()), lifecycle);
        }
        Ok(CommandAdmissionResult::PreparationChanged) => {
            telemetry.record(CommitTelemetryEvent::IdempotencyObserved {
                observation: CommitIdempotencyObservation::Hit,
            });
            return Ok(CommandExecutionResult::PreparationChanged);
        }
        Ok(CommandAdmissionResult::InputMismatch) => {
            telemetry.record(CommitTelemetryEvent::IdempotencyObserved {
                observation: CommitIdempotencyObservation::Mismatch,
            });
            return Ok(CommandExecutionResult::InputMismatch);
        }
        Err(CommandAdmissionError::AdmissionStatusUnknown(uncertain)) => {
            let write = uncertain.cause().clone();
            lifecycle.fence();
            let resolution = resolve_uncertain_command_admission(port, *uncertain);
            telemetry.record(CommitTelemetryEvent::UncertaintyResolved {
                stage: CommitUncertaintyStage::Admission,
                resolution: admission_uncertainty_resolution(&resolution),
            });
            return match resolution {
                UncertainCommandAdmissionResolution::ProvenPending(candidate) => {
                    drop(candidate);
                    Err(CommandExecutionError::with_storage(
                        CommandExecutionErrorKind::StorageUnavailable,
                        write,
                    ))
                }
                UncertainCommandAdmissionResolution::Outcome(outcome) => {
                    terminal_continuation(committed_replay(outcome), lifecycle)
                }
                UncertainCommandAdmissionResolution::ExecutionFailed(failure) => {
                    terminal_continuation(execution_failure(failure.code()), lifecycle)
                }
                UncertainCommandAdmissionResolution::OutcomeUnknown(failure) => {
                    Err(CommandExecutionError::uncertain(
                        failure.admission_error().clone(),
                        failure.lookup_error().cloned(),
                    ))
                }
                UncertainCommandAdmissionResolution::Integrity => {
                    terminal_continuation(internal_defect(lifecycle), lifecycle)
                }
            };
        }
        Err(CommandAdmissionError::Recheck(IdempotencyRecheckError::Storage(error)))
        | Err(CommandAdmissionError::AdmissionWrite(error)) => {
            return Err(storage_error(error, lifecycle));
        }
        Err(CommandAdmissionError::Clock(error)) => {
            return Err(admission_clock_failure(error));
        }
        Err(CommandAdmissionError::Recheck(IdempotencyRecheckError::Integrity(_)))
        | Err(CommandAdmissionError::Integrity) => {
            return terminal_continuation(internal_defect(lifecycle), lifecycle);
        }
    };

    let state = PendingCommandAttempts::from_admission(candidate)
        .map_err(|error| command_attempt_failure(error, lifecycle))?;
    drive_pending_command_attempts(
        port, conflicts, provenance, durability, lifecycle, telemetry, state,
    )
    .await
}

async fn drive_pending_command_attempts<P>(
    port: &P,
    conflicts: &dyn ConflictManager,
    provenance: &dyn ProvenanceIdSource,
    durability: CoordinatorDurability,
    lifecycle: &dyn CommandExecutionLifecycle,
    telemetry: &dyn CommitTelemetry,
    mut state: PendingCommandAttempts,
) -> Result<CommandExecutionResult, CommandExecutionError>
where
    P: AdmissionRepository
        + SnapshotReader
        + ApplicationCommandTransactionPort
        + ExecutionFailureTransitionPort,
{
    loop {
        let resolution = evaluate_next_command_attempt(state, port, port, conflicts)
            .await
            .map_err(|error| command_attempt_failure(error, lifecycle))?;
        let continuation = match resolution {
            CommandAttemptResolution::Evaluated(attempt) => continue_evaluated_command(
                port, provenance, durability, lifecycle, telemetry, attempt,
            ),
            CommandAttemptResolution::ExecutionFault(attempt) => {
                continue_execution_fault(port, lifecycle, telemetry, attempt)
            }
            CommandAttemptResolution::OutcomeReplay(outcome) => {
                telemetry.record(CommitTelemetryEvent::IdempotencyObserved {
                    observation: CommitIdempotencyObservation::Hit,
                });
                committed_replay(outcome)
            }
            CommandAttemptResolution::ExecutionFailureReplay(failure) => {
                telemetry.record(CommitTelemetryEvent::IdempotencyObserved {
                    observation: CommitIdempotencyObservation::Hit,
                });
                execution_failure(failure.code())
            }
        };
        match continuation {
            CommandDriverContinuation::Complete(result) => return Ok(result),
            CommandDriverContinuation::Retry(retry) => state = *retry,
            CommandDriverContinuation::Failed(error) => return Err(error),
        }
    }
}

fn terminal_continuation(
    continuation: CommandDriverContinuation,
    lifecycle: &dyn CommandExecutionLifecycle,
) -> Result<CommandExecutionResult, CommandExecutionError> {
    match continuation {
        CommandDriverContinuation::Complete(result) => Ok(result),
        CommandDriverContinuation::Failed(error) => Err(error),
        CommandDriverContinuation::Retry(retry) => {
            drop(retry);
            lifecycle.stop();
            Err(CommandExecutionError::without_detail(
                CommandExecutionErrorKind::InternalDefect,
            ))
        }
    }
}

const fn admission_uncertainty_resolution(
    resolution: &UncertainCommandAdmissionResolution,
) -> CommitUncertaintyResolution {
    match resolution {
        UncertainCommandAdmissionResolution::ProvenPending(_) => {
            CommitUncertaintyResolution::ProvenPending
        }
        UncertainCommandAdmissionResolution::Outcome(_) => CommitUncertaintyResolution::Outcome,
        UncertainCommandAdmissionResolution::ExecutionFailed(_) => {
            CommitUncertaintyResolution::ExecutionFailure
        }
        UncertainCommandAdmissionResolution::OutcomeUnknown(_) => {
            CommitUncertaintyResolution::StillUnknown
        }
        UncertainCommandAdmissionResolution::Integrity => CommitUncertaintyResolution::Integrity,
    }
}

const fn commit_call_terminal(result: &CheckedCommandCommitResult) -> CommitCallTerminal {
    match result {
        CheckedCommandCommitResult::Committed(_) => CommitCallTerminal::Committed,
        CheckedCommandCommitResult::ProvenAbort(_) => CommitCallTerminal::ProvenAbort,
        CheckedCommandCommitResult::StatusUnknown(_) => CommitCallTerminal::StatusUnknown,
        CheckedCommandCommitResult::Integrity => CommitCallTerminal::Integrity,
    }
}

const fn command_uncertainty_resolution(
    resolution: &UncertainCommandCommitResolution,
) -> CommitUncertaintyResolution {
    match resolution {
        UncertainCommandCommitResolution::Committed(_) => CommitUncertaintyResolution::Outcome,
        UncertainCommandCommitResolution::ExecutionFailureReplay(_) => {
            CommitUncertaintyResolution::ExecutionFailure
        }
        UncertainCommandCommitResolution::ProvenNotCommitted(_) => {
            CommitUncertaintyResolution::ProvenPending
        }
        UncertainCommandCommitResolution::OutcomeUnknown { .. } => {
            CommitUncertaintyResolution::StillUnknown
        }
        UncertainCommandCommitResolution::Integrity => CommitUncertaintyResolution::Integrity,
    }
}

const fn execution_failure_uncertainty_resolution(
    resolution: &UncertainExecutionFailureResolution,
) -> CommitUncertaintyResolution {
    match resolution {
        UncertainExecutionFailureResolution::ExecutionFailureReplay(_) => {
            CommitUncertaintyResolution::ExecutionFailure
        }
        UncertainExecutionFailureResolution::OutcomeReplay(_) => {
            CommitUncertaintyResolution::Outcome
        }
        UncertainExecutionFailureResolution::Retry(_) => CommitUncertaintyResolution::ProvenPending,
        UncertainExecutionFailureResolution::OutcomeUnknown { .. } => {
            CommitUncertaintyResolution::StillUnknown
        }
        UncertainExecutionFailureResolution::Integrity => CommitUncertaintyResolution::Integrity,
    }
}

/// Carries a successfully evaluated attempt through the complete storage chain.
pub(super) fn continue_evaluated_command<P>(
    port: &P,
    provenance: &dyn ProvenanceIdSource,
    durability: CoordinatorDurability,
    lifecycle: &dyn CommandExecutionLifecycle,
    telemetry: &dyn CommitTelemetry,
    attempt: EvaluatedCommandAttempt,
) -> CommandDriverContinuation
where
    P: ApplicationCommandTransactionPort
        + ExecutionFailureTransitionPort
        + riffdb_storage_api::AdmissionRepository,
{
    if let Err(error) = attempt.recheck_request_control() {
        return CommandDriverContinuation::Failed(command_attempt_failure(error, lifecycle));
    }
    let provenance_id = match provenance.next_provenance_id() {
        Ok(provenance_id) => provenance_id,
        Err(error) => {
            return CommandDriverContinuation::Failed(provenance_source_failure(error));
        }
    };
    let attempt = match attempt.bind_provenance(provenance_id) {
        Ok(attempt) => attempt,
        Err(_) => return internal_defect(lifecycle),
    };
    let bound = match begin_bound_command_candidate(port, attempt) {
        CommandCandidateChainStart::Ready(bound) => bound,
        CommandCandidateChainStart::OutcomeReplay(outcome) => {
            return committed_replay(outcome);
        }
        CommandCandidateChainStart::ExecutionFailureReplay(failure) => {
            return execution_failure(failure.code());
        }
        CommandCandidateChainStart::InputMismatch => {
            return CommandDriverContinuation::Complete(CommandExecutionResult::InputMismatch);
        }
        CommandCandidateChainStart::StorageFailure(error) => {
            return proven_storage_failure(error, lifecycle);
        }
        CommandCandidateChainStart::Integrity => return internal_defect(lifecycle),
    };
    let current = match bound.read_transaction_current() {
        TransactionCurrentAttemptDecision::Ready(current) => current,
        TransactionCurrentAttemptDecision::DependencyChanged(changed) => {
            return after_rollback(
                port,
                lifecycle,
                telemetry,
                changed.reject_storage_and_rollback(),
            );
        }
        TransactionCurrentAttemptDecision::StorageFailure(error) => {
            return proven_storage_failure(error, lifecycle);
        }
        TransactionCurrentAttemptDecision::Integrity => return internal_defect(lifecycle),
    };
    let validated = match validate_checked_transaction_current(current) {
        Ok(CheckedCandidateDecision::Validated(validated)) => validated,
        Ok(CheckedCandidateDecision::Rejected(rejected)) => {
            return after_rollback(
                port,
                lifecycle,
                telemetry,
                rejected.reject_storage_and_rollback(),
            );
        }
        Err(_) => return internal_defect(lifecycle),
    };
    let indexed = match derive_checked_command_indexes(validated) {
        Ok(indexed) => indexed,
        Err(_) => return internal_defect(lifecycle),
    };
    let indexed = match indexed.read_affected_epoch_current() {
        CheckedAffectedEpochDecision::Ready(indexed) => indexed,
        CheckedAffectedEpochDecision::StorageFailure(error) => {
            return proven_storage_failure(error, lifecycle);
        }
    };
    let reserved = match resolve_checked_reserve_decision(indexed.reserve_capacity(), lifecycle) {
        Ok(reserved) => reserved,
        Err(result) => return result,
    };
    let assigned = match reserved.assign_sequence() {
        CheckedAssignDecision::Assigned(assigned) => assigned,
        CheckedAssignDecision::StorageFailure(error) => {
            return proven_storage_failure(error, lifecycle);
        }
        CheckedAssignDecision::Integrity => return internal_defect(lifecycle),
    };
    let staged = match build_and_stage_checked_candidate(assigned, durability.storage_mode()) {
        Ok(staged) => staged,
        Err(CheckedCommandStageError::Storage(error)) => {
            return proven_storage_failure(error, lifecycle);
        }
        Err(CheckedCommandStageError::InternalDefect(_)) => return internal_defect(lifecycle),
    };
    let commit_started_at = Instant::now();
    let commit_result = staged.commit();
    telemetry.record(CommitTelemetryEvent::CommitCallCompleted {
        terminal: commit_call_terminal(&commit_result),
        elapsed: commit_started_at.elapsed(),
        batch_size: 1,
        synchronous: durability == CoordinatorDurability::Sync,
    });
    match commit_result {
        CheckedCommandCommitResult::Committed(outcome) => {
            CommandDriverContinuation::Complete(CommandExecutionResult::Committed(*outcome))
        }
        CheckedCommandCommitResult::ProvenAbort(error) => proven_storage_failure(error, lifecycle),
        CheckedCommandCommitResult::StatusUnknown(uncertain) => {
            let write = uncertain.cause().clone();
            lifecycle.fence();
            let resolution = resolve_uncertain_command_commit(port, uncertain);
            telemetry.record(CommitTelemetryEvent::UncertaintyResolved {
                stage: CommitUncertaintyStage::CommandCommit,
                resolution: command_uncertainty_resolution(&resolution),
            });
            match resolution {
                UncertainCommandCommitResolution::Committed(outcome) => {
                    CommandDriverContinuation::Complete(CommandExecutionResult::Committed(outcome))
                }
                UncertainCommandCommitResolution::ExecutionFailureReplay(failure) => {
                    execution_failure(failure.code())
                }
                UncertainCommandCommitResolution::ProvenNotCommitted(proven) => {
                    let retry = proven.into_retry_state();
                    drop(retry);
                    CommandDriverContinuation::Failed(CommandExecutionError::with_storage(
                        CommandExecutionErrorKind::StorageUnavailable,
                        write,
                    ))
                }
                UncertainCommandCommitResolution::OutcomeUnknown {
                    uncertain,
                    lookup_error,
                } => {
                    drop(uncertain);
                    CommandDriverContinuation::Failed(CommandExecutionError::uncertain(
                        write,
                        Some(lookup_error),
                    ))
                }
                UncertainCommandCommitResolution::Integrity => internal_defect(lifecycle),
            }
        }
        CheckedCommandCommitResult::Integrity => internal_defect(lifecycle),
    }
}

fn resolve_checked_reserve_decision<C>(
    decision: CheckedReserveDecision<C>,
    lifecycle: &dyn CommandExecutionLifecycle,
) -> Result<CheckedCommitCandidate<C>, CommandDriverContinuation> {
    match decision {
        CheckedReserveDecision::Reserved(reserved) => Ok(reserved),
        CheckedReserveDecision::CapacityUnavailable => Err(capacity_unavailable()),
        CheckedReserveDecision::StorageFailure(error) => {
            Err(proven_storage_failure(error, lifecycle))
        }
        CheckedReserveDecision::BatchFull
        | CheckedReserveDecision::ProvenanceIdCollision
        | CheckedReserveDecision::EpochExhausted
        | CheckedReserveDecision::Integrity => Err(internal_defect(lifecycle)),
    }
}

/// Revalidates and terminalizes one deterministic execution fault.
pub(super) fn continue_execution_fault<P>(
    port: &P,
    lifecycle: &dyn CommandExecutionLifecycle,
    telemetry: &dyn CommitTelemetry,
    attempt: ExecutionFaultAttempt,
) -> CommandDriverContinuation
where
    P: ExecutionFailureTransitionPort + riffdb_storage_api::AdmissionRepository,
{
    if let Err(error) = attempt.recheck_request_control() {
        return CommandDriverContinuation::Failed(command_attempt_failure(error, lifecycle));
    }
    let current = match begin_execution_failure_transition(port, attempt) {
        ExecutionFailureTransitionStart::Ready(current) => current,
        ExecutionFailureTransitionStart::OutcomeReplay(outcome) => {
            return committed_replay(outcome);
        }
        ExecutionFailureTransitionStart::ExecutionFailureReplay(failure) => {
            return execution_failure(failure.code());
        }
        ExecutionFailureTransitionStart::StorageFailure(error) => {
            return proven_storage_failure(error, lifecycle);
        }
        ExecutionFailureTransitionStart::Integrity => return internal_defect(lifecycle),
    };
    let terminal = match current.read_transaction_current() {
        ExecutionFailureCurrentDecision::Ready(terminal) => terminal,
        ExecutionFailureCurrentDecision::Retry(state) => {
            return CommandDriverContinuation::Retry(state);
        }
        ExecutionFailureCurrentDecision::StorageFailure(error) => {
            return proven_storage_failure(error, lifecycle);
        }
        ExecutionFailureCurrentDecision::Integrity => return internal_defect(lifecycle),
    };
    match terminal.terminalize() {
        ExecutionFailureTerminalizeResult::Terminalized(failure) => {
            execution_failure(failure.code())
        }
        ExecutionFailureTerminalizeResult::ProvenAbort(error) => {
            proven_storage_failure(error, lifecycle)
        }
        ExecutionFailureTerminalizeResult::StatusUnknown(uncertain) => {
            let write = uncertain.cause().clone();
            lifecycle.fence();
            let resolution = resolve_uncertain_execution_failure(port, uncertain);
            telemetry.record(CommitTelemetryEvent::UncertaintyResolved {
                stage: CommitUncertaintyStage::ExecutionFailure,
                resolution: execution_failure_uncertainty_resolution(&resolution),
            });
            match resolution {
                UncertainExecutionFailureResolution::ExecutionFailureReplay(failure) => {
                    execution_failure(failure.code())
                }
                UncertainExecutionFailureResolution::OutcomeReplay(outcome) => {
                    committed_replay(outcome)
                }
                UncertainExecutionFailureResolution::Retry(state) => {
                    drop(state);
                    CommandDriverContinuation::Failed(CommandExecutionError::with_storage(
                        CommandExecutionErrorKind::StorageUnavailable,
                        write,
                    ))
                }
                UncertainExecutionFailureResolution::OutcomeUnknown {
                    uncertain,
                    lookup_error,
                } => {
                    drop(uncertain);
                    CommandDriverContinuation::Failed(CommandExecutionError::uncertain(
                        write,
                        Some(lookup_error),
                    ))
                }
                UncertainExecutionFailureResolution::Integrity => internal_defect(lifecycle),
            }
        }
        ExecutionFailureTerminalizeResult::Integrity => internal_defect(lifecycle),
    }
}

fn after_rollback<P>(
    port: &P,
    lifecycle: &dyn CommandExecutionLifecycle,
    telemetry: &dyn CommitTelemetry,
    disposition: RolledBackCandidateDisposition,
) -> CommandDriverContinuation
where
    P: ExecutionFailureTransitionPort + riffdb_storage_api::AdmissionRepository,
{
    match disposition {
        RolledBackCandidateDisposition::Retry { state, .. } => {
            CommandDriverContinuation::Retry(state)
        }
        RolledBackCandidateDisposition::ExecutionFault(fault) => {
            continue_execution_fault(port, lifecycle, telemetry, *fault)
        }
        RolledBackCandidateDisposition::Integrity => internal_defect(lifecycle),
    }
}

fn committed_replay(outcome: riffdb_storage_api::StoredOutcomeV1) -> CommandDriverContinuation {
    CommandDriverContinuation::Complete(CommandExecutionResult::Committed(
        CommittedOutcome::replay(outcome),
    ))
}

fn execution_failure(code: ExecutionFailureCode) -> CommandDriverContinuation {
    CommandDriverContinuation::Complete(CommandExecutionResult::ExecutionFailed(code))
}

fn admission_clock_failure(error: AdmissionClockError) -> CommandExecutionError {
    CommandExecutionError {
        kind: CommandExecutionErrorKind::InternalDefect,
        detail: CommandExecutionErrorDetail::AdmissionClock(error),
    }
}

fn provenance_source_failure(error: ProvenanceIdSourceError) -> CommandExecutionError {
    CommandExecutionError {
        kind: CommandExecutionErrorKind::InternalDefect,
        detail: CommandExecutionErrorDetail::ProvenanceSource(error),
    }
}

fn internal_defect(lifecycle: &dyn CommandExecutionLifecycle) -> CommandDriverContinuation {
    lifecycle.stop();
    CommandDriverContinuation::Failed(CommandExecutionError::without_detail(
        CommandExecutionErrorKind::InternalDefect,
    ))
}

fn capacity_unavailable() -> CommandDriverContinuation {
    CommandDriverContinuation::Failed(CommandExecutionError::without_detail(
        CommandExecutionErrorKind::StorageUnavailable,
    ))
}

fn proven_storage_failure(
    error: StorageError,
    lifecycle: &dyn CommandExecutionLifecycle,
) -> CommandDriverContinuation {
    if error.kind() == StorageErrorKind::Unavailable {
        CommandDriverContinuation::Failed(CommandExecutionError::with_storage(
            CommandExecutionErrorKind::StorageUnavailable,
            error,
        ))
    } else {
        lifecycle.stop();
        CommandDriverContinuation::Failed(CommandExecutionError::with_storage(
            CommandExecutionErrorKind::InternalDefect,
            error,
        ))
    }
}

pub(super) fn command_attempt_failure(
    error: CommandAttemptError,
    lifecycle: &dyn CommandExecutionLifecycle,
) -> CommandExecutionError {
    match error {
        CommandAttemptError::Cancelled => {
            CommandExecutionError::without_detail(CommandExecutionErrorKind::Cancelled)
        }
        CommandAttemptError::DeadlineExceeded => {
            CommandExecutionError::without_detail(CommandExecutionErrorKind::DeadlineExceeded)
        }
        CommandAttemptError::RetryBudgetExhausted => {
            CommandExecutionError::without_detail(CommandExecutionErrorKind::RetryBudgetExhausted)
        }
        CommandAttemptError::EvaluationPanicked => {
            CommandExecutionError::without_detail(CommandExecutionErrorKind::InternalDefect)
        }
        CommandAttemptError::PendingRecheck(error) | CommandAttemptError::SnapshotRead(error) => {
            storage_error(error, lifecycle)
        }
        CommandAttemptError::Conflict(error) => conflict_failure(error, lifecycle),
        CommandAttemptError::Integrity => {
            lifecycle.stop();
            CommandExecutionError::without_detail(CommandExecutionErrorKind::InternalDefect)
        }
    }
}

fn storage_error(
    error: StorageError,
    lifecycle: &dyn CommandExecutionLifecycle,
) -> CommandExecutionError {
    if error.kind() == StorageErrorKind::Unavailable {
        CommandExecutionError::with_storage(CommandExecutionErrorKind::StorageUnavailable, error)
    } else {
        lifecycle.stop();
        CommandExecutionError::with_storage(CommandExecutionErrorKind::InternalDefect, error)
    }
}

fn conflict_failure(
    error: ConflictError,
    lifecycle: &dyn CommandExecutionLifecycle,
) -> CommandExecutionError {
    let kind = match error {
        ConflictError::Cancelled => CommandExecutionErrorKind::Cancelled,
        ConflictError::DeadlineExceeded => CommandExecutionErrorKind::DeadlineExceeded,
        ConflictError::WaiterCapacityExceeded { .. }
        | ConflictError::KeyQueueCapacityExceeded { .. }
        | ConflictError::TableCapacityExceeded { .. }
        | ConflictError::CancellationRegistrationCapacityExceeded { .. } => {
            CommandExecutionErrorKind::StorageUnavailable
        }
        ConflictError::EmptyKeySet
        | ConflictError::InputKeyCountExceeded { .. }
        | ConflictError::TooManyKeys { .. }
        | ConflictError::IdentifierExhausted => {
            lifecycle.stop();
            CommandExecutionErrorKind::InternalDefect
        }
    };
    CommandExecutionError {
        kind,
        detail: CommandExecutionErrorDetail::Conflict(error),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[derive(Default)]
    struct RecordingLifecycle {
        fences: AtomicUsize,
        stops: AtomicUsize,
    }

    impl CommandExecutionLifecycle for RecordingLifecycle {
        fn fence(&self) {
            self.fences.fetch_add(1, Ordering::Relaxed);
        }

        fn stop(&self) {
            self.stops.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn production_durability_cannot_represent_memory() {
        assert_eq!(
            CoordinatorDurability::Sync.storage_mode(),
            DurabilityMode::Sync
        );
        assert_eq!(
            CoordinatorDurability::Group.storage_mode(),
            DurabilityMode::Group
        );
    }

    #[test]
    fn public_error_debug_never_exposes_private_detail() {
        let error = CommandExecutionError::with_storage(
            CommandExecutionErrorKind::StorageUnavailable,
            StorageError::new(StorageErrorKind::Unavailable, None),
        );

        assert_eq!(error.kind(), CommandExecutionErrorKind::StorageUnavailable);
        assert_eq!(
            format!("{error:?}"),
            "CommandExecutionError { kind: StorageUnavailable, .. }"
        );
        assert_eq!(error.to_string(), "command storage is unavailable");
        assert!(error.source().is_none());
    }

    #[test]
    fn every_executor_error_kind_has_static_redacted_text() {
        let cases = [
            (
                CommandExecutionErrorKind::Cancelled,
                "command execution was cancelled",
            ),
            (
                CommandExecutionErrorKind::DeadlineExceeded,
                "command execution deadline elapsed",
            ),
            (
                CommandExecutionErrorKind::RetryBudgetExhausted,
                "command reevaluation budget was exhausted",
            ),
            (
                CommandExecutionErrorKind::StorageUnavailable,
                "command storage is unavailable",
            ),
            (
                CommandExecutionErrorKind::OutcomeUnknown,
                "command outcome is not yet known",
            ),
            (
                CommandExecutionErrorKind::InternalDefect,
                "command execution encountered an internal defect",
            ),
            (
                CommandExecutionErrorKind::CoordinatorStopped,
                "command coordinator stopped",
            ),
            (
                CommandExecutionErrorKind::CoordinatorFenced,
                "command coordinator fenced authoritative writes",
            ),
        ];

        for (kind, message) in cases {
            let error = CommandExecutionError::without_detail(kind);
            assert_eq!(error.kind(), kind);
            assert_eq!(error.to_string(), message);
            assert_eq!(
                format!("{error:?}"),
                format!("CommandExecutionError {{ kind: {kind:?}, .. }}")
            );
        }
    }

    #[test]
    fn only_proven_storage_unavailability_keeps_the_coordinator_live() {
        let kinds = [
            StorageErrorKind::Unavailable,
            StorageErrorKind::CommitStatusUnknown,
            StorageErrorKind::CorruptData,
            StorageErrorKind::IncompatibleFormat,
            StorageErrorKind::LimitExceeded,
            StorageErrorKind::InvariantViolation,
            StorageErrorKind::SequenceExhausted,
        ];

        for kind in kinds {
            let lifecycle = RecordingLifecycle::default();
            let error = storage_error(StorageError::new(kind, None), &lifecycle);
            let expected = if kind == StorageErrorKind::Unavailable {
                CommandExecutionErrorKind::StorageUnavailable
            } else {
                CommandExecutionErrorKind::InternalDefect
            };
            assert_eq!(error.kind(), expected);
            assert_eq!(lifecycle.fences.load(Ordering::Relaxed), 0);
            assert_eq!(
                lifecycle.stops.load(Ordering::Relaxed),
                usize::from(kind != StorageErrorKind::Unavailable)
            );
        }
    }

    #[test]
    fn accepted_aggregate_capacity_refusal_is_redacted_and_keeps_readiness_live() {
        let lifecycle = RecordingLifecycle::default();
        let Err(CommandDriverContinuation::Failed(error)) = resolve_checked_reserve_decision::<()>(
            CheckedReserveDecision::CapacityUnavailable,
            &lifecycle,
        ) else {
            panic!("aggregate capacity refusal must fail the current attempt")
        };

        assert_eq!(error.kind(), CommandExecutionErrorKind::StorageUnavailable);
        assert_eq!(error.to_string(), "command storage is unavailable");
        assert_eq!(lifecycle.fences.load(Ordering::Relaxed), 0);
        assert_eq!(lifecycle.stops.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn semantic_write_shape_integrity_stops_readiness_as_an_internal_defect() {
        let lifecycle = RecordingLifecycle::default();
        let Err(CommandDriverContinuation::Failed(error)) =
            resolve_checked_reserve_decision::<()>(CheckedReserveDecision::Integrity, &lifecycle)
        else {
            panic!("semantic write-shape failure must fail the current attempt")
        };

        assert_eq!(error.kind(), CommandExecutionErrorKind::InternalDefect);
        assert_eq!(
            error.to_string(),
            "command execution encountered an internal defect"
        );
        assert_eq!(lifecycle.fences.load(Ordering::Relaxed), 0);
        assert_eq!(lifecycle.stops.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn request_control_and_contained_runtime_panics_do_not_change_lifecycle() {
        let cases = [
            (
                CommandAttemptError::Cancelled,
                CommandExecutionErrorKind::Cancelled,
            ),
            (
                CommandAttemptError::DeadlineExceeded,
                CommandExecutionErrorKind::DeadlineExceeded,
            ),
            (
                CommandAttemptError::RetryBudgetExhausted,
                CommandExecutionErrorKind::RetryBudgetExhausted,
            ),
            (
                CommandAttemptError::EvaluationPanicked,
                CommandExecutionErrorKind::InternalDefect,
            ),
        ];

        for (attempt, expected) in cases {
            let lifecycle = RecordingLifecycle::default();
            let error = command_attempt_failure(attempt, &lifecycle);
            assert_eq!(error.kind(), expected);
            assert_eq!(lifecycle.fences.load(Ordering::Relaxed), 0);
            assert_eq!(lifecycle.stops.load(Ordering::Relaxed), 0);
        }
    }

    #[test]
    fn injected_time_and_provenance_failures_are_contained_internal_defects() {
        let clock = admission_clock_failure(AdmissionClockError);
        let provenance = provenance_source_failure(ProvenanceIdSourceError);

        for error in [clock, provenance] {
            assert_eq!(error.kind(), CommandExecutionErrorKind::InternalDefect);
            assert_eq!(
                error.to_string(),
                "command execution encountered an internal defect"
            );
            assert!(error.source().is_none());
        }
    }
}
