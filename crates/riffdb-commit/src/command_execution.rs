//! Actor-owned completion of admitted command attempts.

use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::{
    error::Error,
    fmt,
    time::{Duration, Instant},
};

use riffdb_conflict::{ConflictError, ConflictManager};
use riffdb_idempotency::IdempotencyRecheckError;
use riffdb_storage_api::{
    AdmissionLookupResultV1, AdmissionRepository, ApplicationCommandTransactionPort,
    AuditedAdmissionRepository, CommandCandidateAdmission, CommandCandidateAffectedEpochRead,
    CommandCandidateAwaitingCapacity, CommandCandidateAwaitingValidation,
    CommandCandidateCapacityReserved, CommandCandidateSequenceAssigned, CommandCandidateStateRead,
    DeferredCommandEpoch, DeferredCommandEpochPort, DeferredNonEmptyCommandBatch,
    DetachedCommandGroupBatch, DurabilityMode, EmptyCommandBatch, EntityTarget,
    ExecutionFailureTransitionPort, NonEmptyCommandBatch, ReadSnapshot, SnapshotReader,
    StorageError, StorageErrorKind, TransactionLocalCommandBatch,
};
use riffdb_types::ExecutionFailureCode;

use crate::{
    AdmissionClock, AdmissionClockError, CommandExecutionPreparation, CommandPipelineStage,
    CommitCallTerminal, CommitIdempotencyObservation, CommitTelemetry, CommitTelemetryEvent,
    CommitUncertaintyResolution, CommitUncertaintyStage, CommittedOutcome,
    CommittedOutcomeDisposition, ProvenanceIdSource, ProvenanceIdSourceError, ServiceUuidV7Source,
    ServiceUuidV7SourceError,
    command_admission::{
        CommandAdmissionError, CommandAdmissionResult, UncertainCommandAdmissionResolution,
        reduce_audited_command_admission_group, reduce_command_admission,
        resolve_uncertain_command_admission,
    },
    command_attempt::{
        AcquiredCommandAttempt, CommandAttemptError, CommandAttemptResolution,
        EvaluatedCommandAttempt, ExecutionFaultAttempt, PendingCommandAttempts,
        ProvenanceBoundCommandAttempt, RolledBackCandidateDisposition, acquire_command_attempt,
        acquire_commutative_command_group, acquire_transaction_local_serial_group,
        acquire_writer_private_fifo_group, evaluate_acquired_command_attempt,
        evaluate_acquired_command_attempt_after_lookup_and_snapshot, evaluate_next_command_attempt,
        evaluate_transaction_local_acquired_command_attempt,
    },
    command_execution_failure::{
        ExecutionFailureCurrentDecision, ExecutionFailureTerminalizeResult,
        ExecutionFailureTransitionStart, UncertainExecutionFailureResolution,
        begin_execution_failure_transition, resolve_uncertain_execution_failure,
    },
    command_index::{
        CheckedAffectedEpochDecision, CheckedAssignDecision, CheckedCandidateDetach,
        CheckedCommitCandidate, CheckedReserveDecision, DetachedCheckedCommitCandidate,
        derive_checked_command_indexes,
    },
    command_preparation::PostEvaluationAuthorizationError,
    command_records::{
        CheckedCommandCommitResult, CheckedCommandGroupApplyResult,
        CheckedCommandGroupCommitResult, CheckedCommandGroupFence, CheckedCommandStageError,
        CheckedStagedCommand, CheckedStagedCommandEntry, DetachedRecordPreparation,
        PreparedDetachedRecordGraph, RetainedDetachedCommand, UncertainCommandCommitResolution,
        build_and_stage_checked_candidate, build_and_stage_checked_candidate_on_prior,
        checked_staged_detached_group, join_prepared_detached_command,
        prepare_detached_record_graph, resolve_uncertain_command_commit,
        seal_checked_deferred_group, split_detached_checked_candidate,
    },
    command_validation::{
        CheckedCandidateDecision, CheckedRowPolicyDecision, CommandCandidateChainStart,
        TransactionCurrentAttemptDecision, begin_bound_command_candidate,
        begin_bound_command_candidate_on_empty, begin_bound_command_candidate_on_prior,
        validate_checked_transaction_current,
    },
    read_only_execution::{ReadOnlyExecutionCoreError, ReadOnlyExecutionCoreErrorKind},
};

#[cfg(test)]
use crate::command_preparation::PreparedCommandCompatibility;

type BatchCandidate<B> = <B as NonEmptyCommandBatch>::Candidate;
type BatchStateRead<B> = <BatchCandidate<B> as CommandCandidateAdmission>::StateRead;
type BatchAwaitingValidation<B> =
    <BatchStateRead<B> as CommandCandidateStateRead>::AwaitingValidation;
type BatchAffectedRead<B> =
    <BatchAwaitingValidation<B> as CommandCandidateAwaitingValidation>::AffectedEpochRead;
type BatchAwaitingCapacity<B> =
    <BatchAffectedRead<B> as CommandCandidateAffectedEpochRead>::AwaitingCapacity;
type BatchCapacityReserved<B> =
    <BatchAwaitingCapacity<B> as CommandCandidateAwaitingCapacity>::CapacityReserved;
type BatchSequenceAssigned<B> =
    <BatchCapacityReserved<B> as CommandCandidateCapacityReserved>::SequenceAssigned;
type EmptyCandidate<P> =
    <<P as ApplicationCommandTransactionPort>::EmptyBatch as riffdb_storage_api::EmptyCommandBatch>::Candidate;
type EmptyStateRead<P> = <EmptyCandidate<P> as CommandCandidateAdmission>::StateRead;
type EmptyAwaitingValidation<P> =
    <EmptyStateRead<P> as CommandCandidateStateRead>::AwaitingValidation;
type EmptyAffectedRead<P> =
    <EmptyAwaitingValidation<P> as CommandCandidateAwaitingValidation>::AffectedEpochRead;
type EmptyAwaitingCapacity<P> =
    <EmptyAffectedRead<P> as CommandCandidateAffectedEpochRead>::AwaitingCapacity;
type EmptyCapacityReserved<P> =
    <EmptyAwaitingCapacity<P> as CommandCandidateAwaitingCapacity>::CapacityReserved;
type EmptySequenceAssigned<P> =
    <EmptyCapacityReserved<P> as CommandCandidateCapacityReserved>::SequenceAssigned;
type FirstStagedBatch<P> = <EmptySequenceAssigned<P> as CommandCandidateSequenceAssigned>::Staged;
type FirstStagedEpoch<P> = <FirstStagedBatch<P> as DeferredNonEmptyCommandBatch>::Epoch;
type RepeatableCommandGroupFuture<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = CommandGroupDriveResult> + 'a>>;

pub(super) enum CommandGroupDriveResult {
    Complete(Vec<Result<CommandExecutionResult, CommandExecutionError>>),
    Submitted(SubmittedCommandGroup),
}

/// Closed evaluation frontier selected by the sole FIFO writer.
///
/// `WriterPrivate` is legal only while an earlier complete command unit is
/// applied but unpublished. It forces every remaining command attempt through
/// the transaction-local snapshot protocol rooted at that exact private
/// successor; ordinary snapshot readers deliberately cannot observe it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CommandEvaluationFrontier {
    Published,
    WriterPrivate,
}

pub(super) struct SubmittedCommandGroup {
    results: Vec<Option<Result<CommandExecutionResult, CommandExecutionError>>>,
    subgroups: Vec<SubmittedCommandSubgroup>,
}

struct SubmittedCommandSubgroup {
    indices: Vec<usize>,
    fence: Box<dyn CheckedCommandGroupFence>,
    commit_started_at: Instant,
    batch_size: u16,
}

impl SubmittedCommandGroup {
    pub(super) fn requires_pipeline_drain(&self) -> bool {
        self.subgroups
            .iter()
            .any(|subgroup| subgroup.fence.requires_pipeline_drain())
    }

    pub(super) fn wait(
        mut self,
        lifecycle: &dyn CommandExecutionLifecycle,
        telemetry: &dyn CommitTelemetry,
    ) -> Vec<Result<CommandExecutionResult, CommandExecutionError>> {
        for subgroup in std::mem::take(&mut self.subgroups) {
            let committed = subgroup.fence.wait();
            telemetry.record(CommitTelemetryEvent::CommitCallCompleted {
                terminal: group_commit_call_terminal(&committed),
                elapsed: subgroup.commit_started_at.elapsed(),
                batch_size: subgroup.batch_size,
            });
            self.install_subgroup(subgroup.indices, committed, lifecycle);
        }
        self.take_results()
    }

    fn install_subgroup(
        &mut self,
        indices: Vec<usize>,
        committed: CheckedCommandGroupCommitResult,
        lifecycle: &dyn CommandExecutionLifecycle,
    ) {
        let completed = checked_group_result_without_lookup(committed, lifecycle);
        if completed.len() != indices.len() {
            lifecycle.stop();
            return;
        }
        for (index, result) in indices.into_iter().zip(completed) {
            if self.results.get(index).is_none_or(Option::is_some) {
                lifecycle.stop();
                continue;
            }
            self.results[index] = Some(result);
        }
    }

    fn take_results(&mut self) -> Vec<Result<CommandExecutionResult, CommandExecutionError>> {
        self.results
            .drain(..)
            .map(|result| result.unwrap_or_else(|| Err(internal_defect_error())))
            .collect()
    }
}

enum IndexedCommandGroupDriveResult {
    Complete(Vec<(usize, Result<CommandExecutionResult, CommandExecutionError>)>),
    Submitted {
        completed: Vec<(usize, Result<CommandExecutionResult, CommandExecutionError>)>,
        subgroup: SubmittedCommandSubgroup,
    },
}

impl From<Vec<(usize, Result<CommandExecutionResult, CommandExecutionError>)>>
    for IndexedCommandGroupDriveResult
{
    fn from(
        completed: Vec<(usize, Result<CommandExecutionResult, CommandExecutionError>)>,
    ) -> Self {
        Self::Complete(completed)
    }
}

impl FromIterator<(usize, Result<CommandExecutionResult, CommandExecutionError>)>
    for IndexedCommandGroupDriveResult
{
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = (usize, Result<CommandExecutionResult, CommandExecutionError>)>,
    {
        Self::Complete(iter.into_iter().collect())
    }
}

fn internal_defect_error() -> CommandExecutionError {
    CommandExecutionError::without_detail(CommandExecutionErrorKind::InternalDefect)
}

fn checked_group_result_without_lookup(
    committed: CheckedCommandGroupCommitResult,
    lifecycle: &dyn CommandExecutionLifecycle,
) -> Vec<Result<CommandExecutionResult, CommandExecutionError>> {
    match committed {
        CheckedCommandGroupCommitResult::Committed(outcomes) => outcomes
            .into_iter()
            .map(|outcome| Ok(CommandExecutionResult::Committed(outcome)))
            .collect(),
        CheckedCommandGroupCommitResult::ProvenAbort { cause, retries } => {
            let count = retries.len();
            drop(retries);
            (0..count)
                .map(|_| {
                    Err(CommandExecutionError::with_storage(
                        CommandExecutionErrorKind::StorageUnavailable,
                        cause.clone(),
                    ))
                })
                .collect()
        }
        CheckedCommandGroupCommitResult::StatusUnknown(uncertain) => {
            lifecycle.fence();
            uncertain
                .into_iter()
                .map(|uncertain| {
                    let cause = uncertain.cause().clone();
                    drop(uncertain);
                    Err(CommandExecutionError::uncertain(cause, None))
                })
                .collect()
        }
        CheckedCommandGroupCommitResult::Integrity => {
            lifecycle.stop();
            Vec::new()
        }
    }
}

const MAX_PARALLEL_EVALUATION_WORKERS: usize = 8;
const MAX_QUEUED_EVALUATIONS: usize = riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS;

trait CommandEvaluationReadPort: AdmissionRepository + SnapshotReader + Send + Sync + 'static {}

impl<T> CommandEvaluationReadPort for T where
    T: AdmissionRepository + SnapshotReader + Send + Sync + 'static
{
}

enum CommandEvaluationTaskInput {
    Published(Vec<(AcquiredCommandAttempt, AdmissionLookupResultV1)>),
    WriterPrivate(Vec<(AcquiredCommandAttempt, ReadSnapshot)>),
}

enum CommandEvaluationTask {
    Evaluate {
        ordinal: usize,
        input: CommandEvaluationTaskInput,
        completion: mpsc::Sender<(
            usize,
            Vec<Result<CommandAttemptResolution, CommandAttemptError>>,
        )>,
    },
    PrepareDetached {
        ordinal: usize,
        preparations: Vec<DetachedRecordPreparation>,
        durability: DurabilityMode,
        completion: mpsc::Sender<(usize, Vec<Result<PreparedDetachedRecordGraph, ()>>)>,
    },
}

/// Fixed-size read/evaluation workers with an admission-ordinal reorder buffer.
pub(super) struct CommandEvaluationPool {
    repository: Arc<dyn CommandEvaluationReadPort>,
    worker_count: usize,
    sender: Option<mpsc::SyncSender<CommandEvaluationTask>>,
    workers: Vec<thread::JoinHandle<()>>,
}

impl CommandEvaluationPool {
    /// The evaluation worker count production coordinators pass to
    /// [`Self::new`]: host parallelism clamped to the reviewed bounds. This is
    /// the only ambient read; the pool itself takes the count as an explicit
    /// input so deterministic harnesses can fix it (ADR-0113 hygiene).
    pub(super) fn production_worker_count() -> usize {
        thread::available_parallelism()
            .map_or(1, std::num::NonZeroUsize::get)
            .saturating_sub(1)
            .clamp(1, MAX_PARALLEL_EVALUATION_WORKERS)
    }

    pub(super) fn new<Repository>(repository: Repository, worker_count: usize) -> Result<Self, ()>
    where
        Repository: AdmissionRepository + SnapshotReader + Send + Sync + 'static,
    {
        let worker_count = worker_count.clamp(1, MAX_PARALLEL_EVALUATION_WORKERS);
        let repository: Arc<dyn CommandEvaluationReadPort> = Arc::new(repository);
        let (sender, receiver) =
            mpsc::sync_channel::<CommandEvaluationTask>(MAX_QUEUED_EVALUATIONS);
        let receiver = Arc::new(Mutex::new(receiver));
        let mut workers = Vec::with_capacity(worker_count);
        for index in 0..worker_count {
            let repository = Arc::clone(&repository);
            let receiver = Arc::clone(&receiver);
            let worker = thread::Builder::new()
                .name(format!("riffdb-command-prepare-{index}"))
                .spawn(move || {
                    loop {
                        let task = {
                            let Ok(receiver) = receiver.lock() else {
                                return;
                            };
                            let Ok(task) = receiver.recv() else {
                                return;
                            };
                            task
                        };
                        match task {
                            CommandEvaluationTask::PrepareDetached {
                                ordinal,
                                preparations,
                                durability,
                                completion,
                            } => {
                                let results = preparations
                                    .into_iter()
                                    .map(|preparation| {
                                        prepare_detached_record_graph(preparation, durability)
                                            .map_err(|_| ())
                                    })
                                    .collect();
                                let _coordinator_may_have_stopped =
                                    completion.send((ordinal, results));
                            }
                            CommandEvaluationTask::Evaluate {
                                ordinal,
                                input,
                                completion,
                            } => {
                                let results = match input {
                                    CommandEvaluationTaskInput::Published(attempts) => {
                                        let requests = attempts
                                            .iter()
                                            .map(|(attempt, _)| {
                                                attempt.transaction_local_snapshot_request()
                                            })
                                            .collect();
                                        match repository.read_snapshot_group(requests) {
                                            Ok(snapshots) if snapshots.len() == attempts.len() => {
                                                attempts
                                                    .into_iter()
                                                    .zip(snapshots)
                                                    .map(|((attempt, durable), snapshot)| {
                                                        evaluate_acquired_command_attempt_after_lookup_and_snapshot(
                                                            attempt,
                                                            durable,
                                                            snapshot,
                                                            repository.as_ref(),
                                                        )
                                                        .and_then(prepare_evaluated_resolution)
                                                    })
                                                    .collect()
                                            }
                                            Ok(_) => attempts
                                                .into_iter()
                                                .map(|(attempt, _)| {
                                                    drop(attempt);
                                                    Err(CommandAttemptError::Integrity)
                                                })
                                                .collect(),
                                            Err(error) => attempts
                                                .into_iter()
                                                .map(|(attempt, _)| {
                                                    drop(attempt);
                                                    Err(CommandAttemptError::SnapshotRead(
                                                        error.clone(),
                                                    ))
                                                })
                                                .collect(),
                                        }
                                    }
                                    CommandEvaluationTaskInput::WriterPrivate(attempts) => attempts
                                        .into_iter()
                                        .map(|(attempt, snapshot)| {
                                            evaluate_transaction_local_acquired_command_attempt(
                                                attempt, snapshot,
                                            )
                                            .and_then(prepare_evaluated_resolution)
                                        })
                                        .collect(),
                                };
                                let _coordinator_may_have_stopped =
                                    completion.send((ordinal, results));
                            }
                        }
                    }
                })
                .map_err(|_| ())?;
            workers.push(worker);
        }
        Ok(Self {
            repository,
            worker_count,
            sender: Some(sender),
            workers,
        })
    }

    fn evaluate(
        &self,
        acquired: Vec<AcquiredCommandAttempt>,
        telemetry: &dyn CommitTelemetry,
    ) -> Vec<Result<CommandAttemptResolution, CommandAttemptError>> {
        let count = acquired.len();
        let candidates = acquired
            .iter()
            .map(|attempt| attempt.lookup_candidates().clone())
            .collect();
        let durable = match self.repository.lookup_admission_group(candidates) {
            Ok(durable) if durable.len() == count => durable,
            Ok(_) => {
                return acquired
                    .into_iter()
                    .map(|attempt| {
                        drop(attempt);
                        Err(CommandAttemptError::Integrity)
                    })
                    .collect();
            }
            Err(error) => {
                return acquired
                    .into_iter()
                    .map(|attempt| {
                        drop(attempt);
                        Err(CommandAttemptError::PendingRecheck(error.clone()))
                    })
                    .collect();
            }
        };
        let (completion, receiver) = mpsc::channel();
        let Some(sender) = self.sender.as_ref() else {
            return (0..count)
                .map(|_| Err(CommandAttemptError::Integrity))
                .collect();
        };
        let chunk_size = count.div_ceil(self.worker_count).max(1);
        let task_count = count.div_ceil(chunk_size);
        telemetry.record(CommitTelemetryEvent::PreparationPoolDepthObserved {
            depth: u16::try_from(task_count).unwrap_or(u16::MAX),
        });
        let mut attempts = acquired.into_iter().zip(durable).collect::<Vec<_>>();
        let mut ordinal = 0usize;
        while !attempts.is_empty() {
            let tail = attempts.split_off(attempts.len().min(chunk_size));
            if sender
                .send(CommandEvaluationTask::Evaluate {
                    ordinal,
                    input: CommandEvaluationTaskInput::Published(attempts),
                    completion: completion.clone(),
                })
                .is_err()
            {
                return (0..count)
                    .map(|_| Err(CommandAttemptError::Integrity))
                    .collect();
            }
            ordinal = ordinal.saturating_add(chunk_size);
            attempts = tail;
        }
        drop(completion);
        let mut ordered = (0..count).map(|_| None).collect::<Vec<_>>();
        let mut received_items = 0usize;
        let mut contiguous_items = 0usize;
        for _ in 0..task_count {
            let Ok((ordinal, results)) = receiver.recv() else {
                break;
            };
            received_items = received_items.saturating_add(results.len());
            for (offset, result) in results.into_iter().enumerate() {
                if let Some(slot) = ordered.get_mut(ordinal.saturating_add(offset)) {
                    *slot = Some(result);
                }
            }
            while ordered.get(contiguous_items).is_some_and(Option::is_some) {
                contiguous_items = contiguous_items.saturating_add(1);
            }
            telemetry.record(CommitTelemetryEvent::ReorderBufferOccupancyObserved {
                occupancy: u16::try_from(received_items.saturating_sub(contiguous_items))
                    .unwrap_or(u16::MAX),
            });
        }
        telemetry.record(CommitTelemetryEvent::PreparationPoolDepthObserved { depth: 0 });
        ordered
            .into_iter()
            .map(|result| result.unwrap_or(Err(CommandAttemptError::Integrity)))
            .collect()
    }

    fn evaluate_writer_private(
        &self,
        attempts: Vec<(AcquiredCommandAttempt, ReadSnapshot)>,
        telemetry: &dyn CommitTelemetry,
    ) -> Vec<Result<CommandAttemptResolution, CommandAttemptError>> {
        let count = attempts.len();
        self.dispatch_tasks(
            attempts,
            telemetry,
            CommandEvaluationTaskInput::WriterPrivate,
            count,
        )
    }

    fn prepare_detached(
        &self,
        mut preparations: Vec<DetachedRecordPreparation>,
        durability: DurabilityMode,
        telemetry: &dyn CommitTelemetry,
    ) -> Vec<Result<PreparedDetachedRecordGraph, ()>> {
        let count = preparations.len();
        let (completion, receiver) = mpsc::channel();
        let Some(sender) = self.sender.as_ref() else {
            return (0..count).map(|_| Err(())).collect();
        };
        let chunk_size = count.div_ceil(self.worker_count).max(1);
        let task_count = count.div_ceil(chunk_size);
        telemetry.record(CommitTelemetryEvent::PreparationPoolDepthObserved {
            depth: u16::try_from(task_count).unwrap_or(u16::MAX),
        });
        let mut ordinal = 0usize;
        while !preparations.is_empty() {
            let tail = preparations.split_off(preparations.len().min(chunk_size));
            if sender
                .send(CommandEvaluationTask::PrepareDetached {
                    ordinal,
                    preparations,
                    durability,
                    completion: completion.clone(),
                })
                .is_err()
            {
                return (0..count).map(|_| Err(())).collect();
            }
            ordinal = ordinal.saturating_add(chunk_size);
            preparations = tail;
        }
        drop(completion);
        let mut ordered = (0..count).map(|_| None).collect::<Vec<_>>();
        let mut received_items = 0usize;
        let mut contiguous_items = 0usize;
        for _ in 0..task_count {
            let Ok((ordinal, results)) = receiver.recv() else {
                break;
            };
            received_items = received_items.saturating_add(results.len());
            for (offset, result) in results.into_iter().enumerate() {
                if let Some(slot) = ordered.get_mut(ordinal.saturating_add(offset)) {
                    *slot = Some(result);
                }
            }
            while ordered.get(contiguous_items).is_some_and(Option::is_some) {
                contiguous_items = contiguous_items.saturating_add(1);
            }
            telemetry.record(CommitTelemetryEvent::ReorderBufferOccupancyObserved {
                occupancy: u16::try_from(received_items.saturating_sub(contiguous_items))
                    .unwrap_or(u16::MAX),
            });
        }
        telemetry.record(CommitTelemetryEvent::PreparationPoolDepthObserved { depth: 0 });
        ordered
            .into_iter()
            .map(|result| result.unwrap_or(Err(())))
            .collect()
    }

    fn dispatch_tasks<T, F>(
        &self,
        mut attempts: Vec<T>,
        telemetry: &dyn CommitTelemetry,
        wrap: F,
        count: usize,
    ) -> Vec<Result<CommandAttemptResolution, CommandAttemptError>>
    where
        F: Fn(Vec<T>) -> CommandEvaluationTaskInput,
    {
        let (completion, receiver) = mpsc::channel();
        let Some(sender) = self.sender.as_ref() else {
            return (0..count)
                .map(|_| Err(CommandAttemptError::Integrity))
                .collect();
        };
        let chunk_size = count.div_ceil(self.worker_count).max(1);
        let task_count = count.div_ceil(chunk_size);
        telemetry.record(CommitTelemetryEvent::PreparationPoolDepthObserved {
            depth: u16::try_from(task_count).unwrap_or(u16::MAX),
        });
        let mut ordinal = 0usize;
        while !attempts.is_empty() {
            let tail = attempts.split_off(attempts.len().min(chunk_size));
            if sender
                .send(CommandEvaluationTask::Evaluate {
                    ordinal,
                    input: wrap(attempts),
                    completion: completion.clone(),
                })
                .is_err()
            {
                return (0..count)
                    .map(|_| Err(CommandAttemptError::Integrity))
                    .collect();
            }
            ordinal = ordinal.saturating_add(chunk_size);
            attempts = tail;
        }
        drop(completion);
        collect_ordered_evaluations(receiver, telemetry, count, task_count)
    }
}

fn prepare_evaluated_resolution(
    resolution: CommandAttemptResolution,
) -> Result<CommandAttemptResolution, CommandAttemptError> {
    match resolution {
        CommandAttemptResolution::Evaluated(attempt) => attempt
            .prepare_body()
            .map(CommandAttemptResolution::Evaluated),
        other => Ok(other),
    }
}

fn collect_ordered_evaluations(
    receiver: mpsc::Receiver<(
        usize,
        Vec<Result<CommandAttemptResolution, CommandAttemptError>>,
    )>,
    telemetry: &dyn CommitTelemetry,
    count: usize,
    task_count: usize,
) -> Vec<Result<CommandAttemptResolution, CommandAttemptError>> {
    let mut ordered = (0..count).map(|_| None).collect::<Vec<_>>();
    let mut received_items = 0usize;
    let mut contiguous_items = 0usize;
    for _ in 0..task_count {
        let Ok((ordinal, results)) = receiver.recv() else {
            break;
        };
        received_items = received_items.saturating_add(results.len());
        for (offset, result) in results.into_iter().enumerate() {
            if let Some(slot) = ordered.get_mut(ordinal.saturating_add(offset)) {
                *slot = Some(result);
            }
        }
        while ordered.get(contiguous_items).is_some_and(Option::is_some) {
            contiguous_items = contiguous_items.saturating_add(1);
        }
        telemetry.record(CommitTelemetryEvent::ReorderBufferOccupancyObserved {
            occupancy: u16::try_from(received_items.saturating_sub(contiguous_items))
                .unwrap_or(u16::MAX),
        });
    }
    telemetry.record(CommitTelemetryEvent::PreparationPoolDepthObserved { depth: 0 });
    ordered
        .into_iter()
        .map(|result| result.unwrap_or(Err(CommandAttemptError::Integrity)))
        .collect()
}

impl Drop for CommandEvaluationPool {
    fn drop(&mut self) {
        self.sender.take();
        for worker in self.workers.drain(..) {
            let _worker_may_have_panicked = worker.join();
        }
    }
}

pub(super) trait RepeatableCommandBatchPort:
    AdmissionRepository
    + AuditedAdmissionRepository
    + SnapshotReader
    + ApplicationCommandTransactionPort
    + DeferredCommandEpochPort
    + ExecutionFailureTransitionPort
{
    #[allow(clippy::too_many_arguments)]
    fn drive_repeatable_group<'a>(
        &'a self,
        conflicts: &'a dyn ConflictManager,
        admission_clock: &'a dyn AdmissionClock,
        service_uuids: &'a dyn ServiceUuidV7Source,
        administration_clock: &'a dyn crate::AdministrationClock,
        provenance: &'a dyn ProvenanceIdSource,
        durability: CoordinatorDurability,
        lifecycle: &'a dyn CommandExecutionLifecycle,
        telemetry: &'a dyn CommitTelemetry,
        evaluation_pool: Option<&'a CommandEvaluationPool>,
        evaluation_frontier: CommandEvaluationFrontier,
        preparations: Vec<CommandExecutionPreparation>,
    ) -> RepeatableCommandGroupFuture<'a>;
}

impl<P> RepeatableCommandBatchPort for P
where
    P: AdmissionRepository
        + AuditedAdmissionRepository
        + SnapshotReader
        + ApplicationCommandTransactionPort
        + DeferredCommandEpochPort
        + ExecutionFailureTransitionPort,
    <P as DeferredCommandEpochPort>::Epoch:
        DeferredCommandEpoch<EmptyBatch = <P as ApplicationCommandTransactionPort>::EmptyBatch>,
    <P as ApplicationCommandTransactionPort>::EmptyBatch:
        TransactionLocalCommandBatch + DetachedCommandGroupBatch<Staged = FirstStagedBatch<P>>,
    FirstStagedBatch<P>:
        NonEmptyCommandBatch + DeferredNonEmptyCommandBatch + TransactionLocalCommandBatch,
    BatchCandidate<FirstStagedBatch<P>>: CommandCandidateAdmission<Prior = FirstStagedBatch<P>>,
    BatchStateRead<FirstStagedBatch<P>>: CommandCandidateStateRead<Prior = FirstStagedBatch<P>>,
    BatchAwaitingValidation<FirstStagedBatch<P>>:
        CommandCandidateAwaitingValidation<Prior = FirstStagedBatch<P>>,
    BatchAffectedRead<FirstStagedBatch<P>>:
        CommandCandidateAffectedEpochRead<Prior = FirstStagedBatch<P>>,
    BatchAwaitingCapacity<FirstStagedBatch<P>>:
        CommandCandidateAwaitingCapacity<Prior = FirstStagedBatch<P>>,
    BatchCapacityReserved<FirstStagedBatch<P>>:
        CommandCandidateCapacityReserved<Prior = FirstStagedBatch<P>>,
    BatchSequenceAssigned<FirstStagedBatch<P>>:
        CommandCandidateSequenceAssigned<Prior = FirstStagedBatch<P>, Staged = FirstStagedBatch<P>>,
    <<P as DeferredCommandEpochPort>::Epoch as DeferredCommandEpoch>::Fence: 'static,
    <FirstStagedEpoch<P> as DeferredCommandEpoch>::Fence: 'static,
{
    fn drive_repeatable_group<'a>(
        &'a self,
        conflicts: &'a dyn ConflictManager,
        admission_clock: &'a dyn AdmissionClock,
        service_uuids: &'a dyn ServiceUuidV7Source,
        administration_clock: &'a dyn crate::AdministrationClock,
        provenance: &'a dyn ProvenanceIdSource,
        durability: CoordinatorDurability,
        lifecycle: &'a dyn CommandExecutionLifecycle,
        telemetry: &'a dyn CommitTelemetry,
        evaluation_pool: Option<&'a CommandEvaluationPool>,
        evaluation_frontier: CommandEvaluationFrontier,
        preparations: Vec<CommandExecutionPreparation>,
    ) -> RepeatableCommandGroupFuture<'a> {
        Box::pin(drive_command_execution_group(
            self,
            conflicts,
            admission_clock,
            service_uuids,
            administration_clock,
            provenance,
            durability,
            lifecycle,
            telemetry,
            evaluation_pool,
            evaluation_frontier,
            preparations,
        ))
    }
}

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
    ExecutionFailed(ExecutionFailedOutcome),
    /// Durable idempotency state selected another immutable historical plan.
    PreparationChanged,
    /// The same idempotency identity retained different canonical input.
    InputMismatch,
}

/// Current-invocation view of one immutable deterministic failure.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ExecutionFailedOutcome {
    code: ExecutionFailureCode,
    disposition: CommittedOutcomeDisposition,
}

impl ExecutionFailedOutcome {
    const fn first_terminal(code: ExecutionFailureCode) -> Self {
        Self {
            code,
            disposition: CommittedOutcomeDisposition::FirstCommit,
        }
    }

    const fn replay(code: ExecutionFailureCode) -> Self {
        Self {
            code,
            disposition: CommittedOutcomeDisposition::Replay,
        }
    }

    /// Returns the durable deterministic failure code.
    #[must_use]
    pub const fn code(self) -> ExecutionFailureCode {
        self.code
    }

    /// Returns whether this invocation persisted or replayed the failure.
    #[must_use]
    pub const fn disposition(self) -> CommittedOutcomeDisposition {
        self.disposition
    }
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
    /// Current policy explicitly denied the command after evaluation.
    AuthorizationDenied,
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
            Self::AuthorizationDenied => "command authorization changed before commit",
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
    ServiceUuid(ServiceUuidV7SourceError),
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
    service_uuids: &dyn ServiceUuidV7Source,
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
    let admission = reduce_command_admission(port, admission_clock, service_uuids, preparation);
    let candidate = match admission {
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
            return terminal_continuation(execution_failure_replay(failure.code()), lifecycle);
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
                    terminal_continuation(execution_failure_replay(failure.code()), lifecycle)
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
        Err(CommandAdmissionError::ServiceUuid(error)) => {
            return Err(service_uuid_failure(error));
        }
        Err(CommandAdmissionError::Recheck(IdempotencyRecheckError::Integrity(_)))
        | Err(CommandAdmissionError::Integrity) => {
            return terminal_continuation(internal_defect(lifecycle), lifecycle);
        }
    };

    let state = PendingCommandAttempts::from_admission(candidate)
        .map_err(|error| command_attempt_failure(error, lifecycle))?;
    drive_pending_command_attempts(
        port, conflicts, provenance, None, durability, lifecycle, telemetry, state,
    )
    .await
}

/// Owns a bounded FIFO group through one shared physical admission transition.
#[allow(clippy::too_many_arguments)]
pub(super) async fn drive_command_execution_group<P>(
    port: &P,
    conflicts: &dyn ConflictManager,
    admission_clock: &dyn AdmissionClock,
    service_uuids: &dyn ServiceUuidV7Source,
    administration_clock: &dyn crate::AdministrationClock,
    provenance: &dyn ProvenanceIdSource,
    durability: CoordinatorDurability,
    lifecycle: &dyn CommandExecutionLifecycle,
    telemetry: &dyn CommitTelemetry,
    evaluation_pool: Option<&CommandEvaluationPool>,
    evaluation_frontier: CommandEvaluationFrontier,
    preparations: Vec<CommandExecutionPreparation>,
) -> CommandGroupDriveResult
where
    P: AdmissionRepository
        + AuditedAdmissionRepository
        + SnapshotReader
        + ApplicationCommandTransactionPort
        + DeferredCommandEpochPort
        + ExecutionFailureTransitionPort,
    <P as DeferredCommandEpochPort>::Epoch:
        DeferredCommandEpoch<EmptyBatch = <P as ApplicationCommandTransactionPort>::EmptyBatch>,
    <<P as DeferredCommandEpochPort>::Epoch as DeferredCommandEpoch>::Fence: 'static,
    <FirstStagedEpoch<P> as DeferredCommandEpoch>::Fence: 'static,
    <P as ApplicationCommandTransactionPort>::EmptyBatch:
        TransactionLocalCommandBatch + DetachedCommandGroupBatch<Staged = FirstStagedBatch<P>>,
    FirstStagedBatch<P>:
        NonEmptyCommandBatch + DeferredNonEmptyCommandBatch + TransactionLocalCommandBatch,
    BatchCandidate<FirstStagedBatch<P>>: CommandCandidateAdmission<Prior = FirstStagedBatch<P>>,
    BatchStateRead<FirstStagedBatch<P>>: CommandCandidateStateRead<Prior = FirstStagedBatch<P>>,
    BatchAwaitingValidation<FirstStagedBatch<P>>:
        CommandCandidateAwaitingValidation<Prior = FirstStagedBatch<P>>,
    BatchAffectedRead<FirstStagedBatch<P>>:
        CommandCandidateAffectedEpochRead<Prior = FirstStagedBatch<P>>,
    BatchAwaitingCapacity<FirstStagedBatch<P>>:
        CommandCandidateAwaitingCapacity<Prior = FirstStagedBatch<P>>,
    BatchCapacityReserved<FirstStagedBatch<P>>:
        CommandCandidateCapacityReserved<Prior = FirstStagedBatch<P>>,
    BatchSequenceAssigned<FirstStagedBatch<P>>:
        CommandCandidateSequenceAssigned<Prior = FirstStagedBatch<P>, Staged = FirstStagedBatch<P>>,
{
    let admission_started = Instant::now();
    let admissions = reduce_audited_command_admission_group(
        port,
        admission_clock,
        service_uuids,
        administration_clock,
        preparations,
    );
    telemetry.record(CommitTelemetryEvent::CommandPipelineStageCompleted {
        stage: CommandPipelineStage::Admission,
        command_count: u16::try_from(admissions.len()).unwrap_or(u16::MAX),
        elapsed: admission_started.elapsed(),
    });
    let count = admissions.len();
    let mut results = (0..count).map(|_| None).collect::<Vec<_>>();
    let mut pending = Vec::new();
    for (index, admission) in admissions.into_iter().enumerate() {
        match lower_admission_result(port, lifecycle, telemetry, admission) {
            Ok(state) => pending.push((index, state)),
            Err(result) => results[index] = Some(result),
        }
    }

    let compatibility_started = Instant::now();
    let (groups, conflict_key_splits, exact_access_splits, commutative_shared_groups) =
        partition_pending_fifo_by_compatibility(pending);
    let pending_count = groups.iter().map(|group| group.items.len()).sum::<usize>();
    let mut serial_identities = Vec::new();
    let serial_members_eligible = groups.iter().all(|group| {
        group.items.iter().all(|(_, state)| {
            let identity = state.idempotency_identity();
            let unique = !serial_identities.iter().any(|prior| prior == identity);
            if unique {
                serial_identities.push(identity.clone());
            }
            let frontier_eligible = match evaluation_frontier {
                CommandEvaluationFrontier::Published => state.serial_micro_batch_eligible(),
                CommandEvaluationFrontier::WriterPrivate => state.has_audited_lifecycle(),
            };
            frontier_eligible && unique
        })
    });
    let serial_eligible = pending_count > 0
        && (evaluation_frontier == CommandEvaluationFrontier::WriterPrivate || groups.len() > 1)
        && serial_members_eligible;
    telemetry.record(CommitTelemetryEvent::CommandGroupPartitioned {
        selected: u16::try_from(groups.iter().map(|group| group.items.len()).sum::<usize>())
            .unwrap_or(u16::MAX),
        completion_groups: if serial_eligible {
            1
        } else {
            u16::try_from(groups.len()).unwrap_or(u16::MAX)
        },
        conflict_key_splits,
        exact_access_splits,
        commutative_shared_groups: if serial_eligible {
            0
        } else {
            commutative_shared_groups
        },
    });
    telemetry.record(CommitTelemetryEvent::CommandPipelineStageCompleted {
        stage: CommandPipelineStage::Compatibility,
        command_count: u16::try_from(groups.iter().map(|group| group.items.len()).sum::<usize>())
            .unwrap_or(u16::MAX),
        elapsed: compatibility_started.elapsed(),
    });
    if evaluation_frontier == CommandEvaluationFrontier::WriterPrivate
        && pending_count > 0
        && !serial_eligible
    {
        // A private successor may never fall through to ordinary snapshot
        // evaluation. The writer selected this frontier because an unpublished
        // predecessor exists; losing the closed FIFO proof is an integrity
        // failure, not a reason to read the older public root.
        lifecycle.stop();
        return CommandGroupDriveResult::Complete(
            results
                .into_iter()
                .map(|result| result.unwrap_or_else(|| Err(internal_defect_error())))
                .collect(),
        );
    }
    if serial_eligible {
        let parallel_writer_private = evaluation_frontier
            == CommandEvaluationFrontier::WriterPrivate
            && groups.len() == 1
            && evaluation_pool.is_some();
        let use_deferred_tail = durability == CoordinatorDurability::Group
            && groups.iter().all(|group| {
                group
                    .items
                    .iter()
                    .all(|(_, state)| state.has_audited_lifecycle())
            });
        let serial = groups
            .into_iter()
            .flat_map(|group| group.items)
            .collect::<Vec<_>>();
        let grouped = drive_transaction_local_serial_pending_group(
            port,
            conflicts,
            administration_clock,
            provenance,
            durability,
            lifecycle,
            telemetry,
            evaluation_pool,
            parallel_writer_private,
            use_deferred_tail,
            evaluation_frontier,
            serial,
        )
        .await;
        return match grouped {
            IndexedCommandGroupDriveResult::Complete(grouped) => {
                for (index, result) in grouped {
                    results[index] = Some(result);
                }
                CommandGroupDriveResult::Complete(finalize_group_results(results, lifecycle))
            }
            IndexedCommandGroupDriveResult::Submitted {
                completed,
                subgroup,
            } => {
                for (index, result) in completed {
                    results[index] = Some(result);
                }
                CommandGroupDriveResult::Submitted(SubmittedCommandGroup {
                    results,
                    subgroups: vec![subgroup],
                })
            }
        };
    }
    // Incompatible completion groups may overlap the conflict capabilities
    // retained by an earlier deferred subgroup. Deferring more than one here
    // would make the coordinator await the later acquisition before it can
    // return the earlier fence to the writer for publication. Keep split
    // groups on the existing immediate path; independent writer units can
    // still pipeline through the journal.
    let all_groups_deferred_eligible = groups.len() == 1
        && durability == CoordinatorDurability::Group
        && groups.iter().all(|group| {
            group
                .items
                .iter()
                .all(|(_, state)| state.has_audited_lifecycle())
        });
    let mut submitted = Vec::new();
    for group in groups {
        if group.items.len() > 1 || all_groups_deferred_eligible {
            let grouped = drive_compatible_pending_group(
                port,
                conflicts,
                administration_clock,
                provenance,
                durability,
                lifecycle,
                telemetry,
                evaluation_pool,
                all_groups_deferred_eligible,
                group.items,
                group.shared_conflict_lease,
            )
            .await;
            match grouped {
                IndexedCommandGroupDriveResult::Complete(grouped) => {
                    for (index, result) in grouped {
                        results[index] = Some(result);
                    }
                }
                IndexedCommandGroupDriveResult::Submitted {
                    completed,
                    subgroup,
                } => {
                    for (index, result) in completed {
                        results[index] = Some(result);
                    }
                    submitted.push(subgroup);
                }
            }
        } else {
            for (index, state) in group.items {
                results[index] = Some(
                    drive_pending_command_attempts(
                        port,
                        conflicts,
                        provenance,
                        Some(administration_clock),
                        durability,
                        lifecycle,
                        telemetry,
                        state,
                    )
                    .await,
                );
            }
        }
    }
    if submitted.is_empty() {
        CommandGroupDriveResult::Complete(finalize_group_results(results, lifecycle))
    } else {
        CommandGroupDriveResult::Submitted(SubmittedCommandGroup {
            results,
            subgroups: submitted,
        })
    }
}

fn finalize_group_results(
    results: Vec<Option<Result<CommandExecutionResult, CommandExecutionError>>>,
    lifecycle: &dyn CommandExecutionLifecycle,
) -> Vec<Result<CommandExecutionResult, CommandExecutionError>> {
    results
        .into_iter()
        .map(|result| {
            result.unwrap_or_else(|| terminal_continuation(internal_defect(lifecycle), lifecycle))
        })
        .collect()
}

fn lower_admission_result<P>(
    port: &P,
    lifecycle: &dyn CommandExecutionLifecycle,
    telemetry: &dyn CommitTelemetry,
    admission: Result<CommandAdmissionResult, CommandAdmissionError>,
) -> Result<PendingCommandAttempts, Result<CommandExecutionResult, CommandExecutionError>>
where
    P: AdmissionRepository,
{
    let candidate = match admission {
        Ok(CommandAdmissionResult::Execute(candidate)) => candidate,
        Ok(CommandAdmissionResult::Outcome(outcome)) => {
            telemetry.record(CommitTelemetryEvent::IdempotencyObserved {
                observation: CommitIdempotencyObservation::Hit,
            });
            return Err(terminal_continuation(committed_replay(outcome), lifecycle));
        }
        Ok(CommandAdmissionResult::ExecutionFailed(failure)) => {
            telemetry.record(CommitTelemetryEvent::IdempotencyObserved {
                observation: CommitIdempotencyObservation::Hit,
            });
            return Err(terminal_continuation(
                execution_failure_replay(failure.code()),
                lifecycle,
            ));
        }
        Ok(CommandAdmissionResult::PreparationChanged) => {
            telemetry.record(CommitTelemetryEvent::IdempotencyObserved {
                observation: CommitIdempotencyObservation::Hit,
            });
            return Err(Ok(CommandExecutionResult::PreparationChanged));
        }
        Ok(CommandAdmissionResult::InputMismatch) => {
            telemetry.record(CommitTelemetryEvent::IdempotencyObserved {
                observation: CommitIdempotencyObservation::Mismatch,
            });
            return Err(Ok(CommandExecutionResult::InputMismatch));
        }
        Err(CommandAdmissionError::AdmissionStatusUnknown(uncertain)) => {
            let write = uncertain.cause().clone();
            lifecycle.fence();
            let resolution = resolve_uncertain_command_admission(port, *uncertain);
            telemetry.record(CommitTelemetryEvent::UncertaintyResolved {
                stage: CommitUncertaintyStage::Admission,
                resolution: admission_uncertainty_resolution(&resolution),
            });
            return Err(match resolution {
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
                    terminal_continuation(execution_failure_replay(failure.code()), lifecycle)
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
            });
        }
        Err(CommandAdmissionError::Recheck(IdempotencyRecheckError::Storage(error)))
        | Err(CommandAdmissionError::AdmissionWrite(error)) => {
            return Err(Err(storage_error(error, lifecycle)));
        }
        Err(CommandAdmissionError::Clock(error)) => {
            return Err(Err(admission_clock_failure(error)));
        }
        Err(CommandAdmissionError::ServiceUuid(error)) => {
            return Err(Err(service_uuid_failure(error)));
        }
        Err(CommandAdmissionError::Recheck(IdempotencyRecheckError::Integrity(_)))
        | Err(CommandAdmissionError::Integrity) => {
            return Err(terminal_continuation(internal_defect(lifecycle), lifecycle));
        }
    };
    PendingCommandAttempts::from_admission(candidate)
        .map_err(|error| Err(command_attempt_failure(error, lifecycle)))
}

#[cfg(test)]
fn partition_fifo_by_compatibility<T>(
    items: Vec<T>,
    compatible: impl Fn(&[T]) -> bool,
) -> Vec<Vec<T>> {
    let mut groups = Vec::new();
    let mut current = Vec::new();
    for item in items {
        current.push(item);
        if compatible(&current) {
            continue;
        }
        let Some(conflicting) = current.pop() else {
            continue;
        };
        if !current.is_empty() {
            groups.push(current);
        }
        current = vec![conflicting];
    }
    if !current.is_empty() {
        groups.push(current);
    }
    groups
}

struct CompatibleCommandGroup {
    keys: std::collections::BTreeSet<riffdb_types::ConflictKey>,
    reads: std::collections::BTreeSet<EntityTarget>,
    writes: std::collections::BTreeSet<EntityTarget>,
    all_commutative_child_appends: bool,
    shared_conflict_lease: bool,
}

/// Exact command accesses retained while a deferred writer unit is unpublished.
///
/// Cross-unit pipelining is stricter than same-transaction grouping: even
/// commutative commands must publish in order when they share a conflict key,
/// because a later unit may need to re-evaluate against the predecessor's
/// newly published state.
pub(super) struct DeferredPipelineFootprint {
    keys: std::collections::BTreeSet<riffdb_types::ConflictKey>,
    reads: std::collections::BTreeSet<EntityTarget>,
    writes: std::collections::BTreeSet<EntityTarget>,
}

impl DeferredPipelineFootprint {
    #[cfg(test)]
    pub(super) fn requires_private_successor(&self, successor: &Self) -> bool {
        !self.is_disjoint_from(successor)
    }

    #[cfg(test)]
    fn is_disjoint_from(&self, other: &Self) -> bool {
        self.keys.is_disjoint(&other.keys)
            && exact_accesses_are_compatible(&self.reads, &self.writes, &other.reads, &other.writes)
    }
}

pub(super) fn command_group_deferred_pipeline_footprint<'a>(
    preparations: impl IntoIterator<Item = &'a CommandExecutionPreparation>,
) -> Option<DeferredPipelineFootprint> {
    let mut footprint = DeferredPipelineFootprint {
        keys: std::collections::BTreeSet::new(),
        reads: std::collections::BTreeSet::new(),
        writes: std::collections::BTreeSet::new(),
    };
    let mut count = 0_usize;
    for preparation in preparations {
        count += 1;
        let candidate = preparation.deferred_group_compatibility()?;
        footprint.keys.extend(candidate.conflict_keys);
        for (mode, target) in candidate.binding_accesses {
            match mode {
                riffdb_contract_ir::BindingMode::Read => {
                    footprint.reads.insert(target);
                }
                riffdb_contract_ir::BindingMode::Mutate
                | riffdb_contract_ir::BindingMode::Create
                | riffdb_contract_ir::BindingMode::Delete => {
                    footprint.writes.insert(target);
                }
            }
        }
        footprint.reads.extend(candidate.root_validation_targets);
    }
    (count > 0).then_some(footprint)
}

impl Default for CompatibleCommandGroup {
    fn default() -> Self {
        Self {
            keys: std::collections::BTreeSet::new(),
            reads: std::collections::BTreeSet::new(),
            writes: std::collections::BTreeSet::new(),
            all_commutative_child_appends: true,
            shared_conflict_lease: false,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CompatibilityFailure {
    ConflictKey,
    ExactAccess,
}

impl CompatibleCommandGroup {
    fn try_insert(&mut self, state: &PendingCommandAttempts) -> Result<(), CompatibilityFailure> {
        let mut reads = std::collections::BTreeSet::new();
        let mut writes = std::collections::BTreeSet::new();
        for (mode, target) in state.binding_accesses() {
            match mode {
                riffdb_contract_ir::BindingMode::Read => {
                    reads.insert(target.clone());
                }
                riffdb_contract_ir::BindingMode::Mutate
                | riffdb_contract_ir::BindingMode::Create
                | riffdb_contract_ir::BindingMode::Delete => {
                    writes.insert(target.clone());
                }
            }
        }
        reads.extend(state.root_validation_targets().iter().cloned());
        self.try_insert_accesses(
            state.raw_conflict_keys(),
            reads,
            writes,
            state.commutative_child_append_proof().is_some(),
        )
    }

    #[cfg(test)]
    fn try_insert_prepared_pipeline(
        &mut self,
        candidate: PreparedCommandCompatibility,
    ) -> Result<(), CompatibilityFailure> {
        let mut reads = std::collections::BTreeSet::new();
        let mut writes = std::collections::BTreeSet::new();
        for (mode, target) in candidate.binding_accesses {
            match mode {
                riffdb_contract_ir::BindingMode::Read => {
                    reads.insert(target);
                }
                riffdb_contract_ir::BindingMode::Mutate
                | riffdb_contract_ir::BindingMode::Create
                | riffdb_contract_ir::BindingMode::Delete => {
                    writes.insert(target);
                }
            }
        }
        reads.extend(candidate.root_validation_targets);
        self.try_insert_accesses(&candidate.conflict_keys, reads, writes, false)
    }

    fn try_insert_accesses(
        &mut self,
        conflict_keys: &[riffdb_types::ConflictKey],
        reads: std::collections::BTreeSet<EntityTarget>,
        writes: std::collections::BTreeSet<EntityTarget>,
        is_commutative_child_append: bool,
    ) -> Result<(), CompatibilityFailure> {
        let shares_conflict_key = conflict_keys.iter().any(|key| self.keys.contains(key));
        if !shared_conflict_membership_is_compatible(
            self.all_commutative_child_appends,
            self.shared_conflict_lease,
            shares_conflict_key,
            is_commutative_child_append,
        ) {
            return Err(CompatibilityFailure::ConflictKey);
        }
        // Exact validation must describe the final grouped transaction, not
        // merely the prefix visible when this candidate was staged. Reject
        // read/write and write/write overlap in either FIFO direction.
        if !exact_accesses_are_compatible(&self.reads, &self.writes, &reads, &writes) {
            return Err(CompatibilityFailure::ExactAccess);
        }
        self.keys.extend(conflict_keys.iter().cloned());
        self.reads.extend(reads);
        self.writes.extend(writes);
        self.all_commutative_child_appends &= is_commutative_child_append;
        self.shared_conflict_lease |= shares_conflict_key;
        Ok(())
    }
}

#[cfg(test)]
pub(super) fn command_group_is_deferred_eligible<'a>(
    preparations: impl IntoIterator<Item = &'a CommandExecutionPreparation>,
) -> bool {
    let mut compatibility = CompatibleCommandGroup::default();
    let mut count = 0_usize;
    let compatible = preparations.into_iter().all(|preparation| {
        count += 1;
        preparation
            .deferred_group_compatibility()
            .is_some_and(|candidate| {
                compatibility
                    .try_insert_prepared_pipeline(candidate)
                    .is_ok()
            })
    });
    compatible && count != 0
}

fn shared_conflict_membership_is_compatible(
    prior_all_commutative: bool,
    prior_uses_shared_lease: bool,
    candidate_shares_key: bool,
    candidate_is_commutative: bool,
) -> bool {
    (!candidate_shares_key || (prior_all_commutative && candidate_is_commutative))
        && (!prior_uses_shared_lease || candidate_is_commutative)
}

struct PendingCompatibilityGroup {
    items: Vec<(usize, PendingCommandAttempts)>,
    shared_conflict_lease: bool,
}

fn partition_pending_fifo_by_compatibility(
    pending: Vec<(usize, PendingCommandAttempts)>,
) -> (Vec<PendingCompatibilityGroup>, u16, u16, u16) {
    let mut groups = Vec::new();
    let mut current = Vec::new();
    let mut compatibility = CompatibleCommandGroup::default();
    let mut conflict_key_splits = 0_u16;
    let mut exact_access_splits = 0_u16;
    for item in pending {
        if let Err(reason) = compatibility.try_insert(&item.1) {
            match reason {
                CompatibilityFailure::ConflictKey => {
                    conflict_key_splits = conflict_key_splits.saturating_add(1);
                }
                CompatibilityFailure::ExactAccess => {
                    exact_access_splits = exact_access_splits.saturating_add(1);
                }
            }
            if !current.is_empty() {
                groups.push(PendingCompatibilityGroup {
                    items: std::mem::take(&mut current),
                    shared_conflict_lease: compatibility.shared_conflict_lease,
                });
            }
            compatibility = CompatibleCommandGroup::default();
            // A command's conflict keys and exact accesses are canonical and
            // duplicate-free internally, so a fresh group must accept it.
            if compatibility.try_insert(&item.1).is_err() {
                groups.push(PendingCompatibilityGroup {
                    items: vec![item],
                    shared_conflict_lease: false,
                });
                continue;
            }
        }
        current.push(item);
    }
    if !current.is_empty() {
        groups.push(PendingCompatibilityGroup {
            items: current,
            shared_conflict_lease: compatibility.shared_conflict_lease,
        });
    }
    let commutative_shared_groups = u16::try_from(
        groups
            .iter()
            .filter(|group| group.shared_conflict_lease)
            .count(),
    )
    .unwrap_or(u16::MAX);
    (
        groups,
        conflict_key_splits,
        exact_access_splits,
        commutative_shared_groups,
    )
}

fn exact_accesses_are_compatible(
    prior_reads: &std::collections::BTreeSet<EntityTarget>,
    prior_writes: &std::collections::BTreeSet<EntityTarget>,
    reads: &std::collections::BTreeSet<EntityTarget>,
    writes: &std::collections::BTreeSet<EntityTarget>,
) -> bool {
    !writes
        .iter()
        .any(|target| prior_reads.contains(target) || prior_writes.contains(target))
        && !reads.iter().any(|target| prior_writes.contains(target))
}

enum ParallelEvaluationPreparation {
    Ready(Vec<(usize, EvaluatedCommandAttempt)>),
    Complete(Vec<(usize, Result<CommandExecutionResult, CommandExecutionError>)>),
}

struct WriterPrivateSnapshotCaptureFailure {
    failed_index: usize,
    failed: AcquiredCommandAttempt,
    error: CommandAttemptError,
    fallback: Vec<(usize, AcquiredCommandAttempt)>,
}

fn capture_writer_private_snapshots<B>(
    batch: &B,
    mut attempts: std::collections::VecDeque<(usize, AcquiredCommandAttempt)>,
) -> Result<
    Vec<(usize, AcquiredCommandAttempt, ReadSnapshot)>,
    Box<WriterPrivateSnapshotCaptureFailure>,
>
where
    B: TransactionLocalCommandBatch,
{
    let mut captured = Vec::with_capacity(attempts.len());
    while let Some((index, mut attempt)) = attempts.pop_front() {
        let discovery = match batch
            .read_transaction_local_snapshot(attempt.transaction_local_snapshot_request())
        {
            Ok(snapshot) => snapshot,
            Err(error) => {
                return Err(Box::new(WriterPrivateSnapshotCaptureFailure {
                    failed_index: index,
                    failed: attempt,
                    error: CommandAttemptError::SnapshotRead(error),
                    fallback: captured
                        .into_iter()
                        .map(|(index, attempt, _)| (index, attempt))
                        .chain(attempts)
                        .collect(),
                }));
            }
        };
        let snapshot = match attempt.complete_transaction_local_snapshot(discovery, |request| {
            batch.read_transaction_local_snapshot(request)
        }) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                return Err(Box::new(WriterPrivateSnapshotCaptureFailure {
                    failed_index: index,
                    failed: attempt,
                    error,
                    fallback: captured
                        .into_iter()
                        .map(|(index, attempt, _)| (index, attempt))
                        .chain(attempts)
                        .collect(),
                }));
            }
        };
        captured.push((index, attempt, snapshot));
    }
    Ok(captured)
}

#[allow(clippy::too_many_arguments)]
async fn drive_detached_writer_private_group<P>(
    port: &P,
    conflicts: &dyn ConflictManager,
    administration_clock: &dyn crate::AdministrationClock,
    provenance: &dyn ProvenanceIdSource,
    durability: CoordinatorDurability,
    lifecycle: &dyn CommandExecutionLifecycle,
    telemetry: &dyn CommitTelemetry,
    evaluation_pool: &CommandEvaluationPool,
    use_deferred_tail: bool,
    mut evaluation_elapsed: Duration,
    serial_started: Instant,
    mut empty: <P as ApplicationCommandTransactionPort>::EmptyBatch,
    mut evaluated: std::collections::VecDeque<(usize, EvaluatedCommandAttempt)>,
) -> IndexedCommandGroupDriveResult
where
    P: AdmissionRepository
        + SnapshotReader
        + ApplicationCommandTransactionPort
        + DeferredCommandEpochPort
        + ExecutionFailureTransitionPort,
    <P as DeferredCommandEpochPort>::Epoch:
        DeferredCommandEpoch<EmptyBatch = <P as ApplicationCommandTransactionPort>::EmptyBatch>,
    <<P as DeferredCommandEpochPort>::Epoch as DeferredCommandEpoch>::Fence: 'static,
    <P as ApplicationCommandTransactionPort>::EmptyBatch:
        TransactionLocalCommandBatch + DetachedCommandGroupBatch<Staged = FirstStagedBatch<P>>,
    FirstStagedBatch<P>:
        NonEmptyCommandBatch + DeferredNonEmptyCommandBatch + TransactionLocalCommandBatch,
    <FirstStagedEpoch<P> as DeferredCommandEpoch>::Fence: 'static,
{
    let command_count = evaluated.len();
    let mut detached = Vec::with_capacity(command_count);
    while let Some((index, attempt)) = evaluated.pop_front() {
        match detach_evaluated_command_on_empty(
            port,
            empty,
            provenance,
            Some(administration_clock),
            lifecycle,
            telemetry,
            attempt,
        ) {
            Ok((prior, candidate)) => {
                empty = prior;
                detached.push((index, candidate));
            }
            Err(continuation) => {
                let mut completed = Vec::new();
                let mut fallback = detached
                    .into_iter()
                    .filter_map(|(index, candidate)| {
                        candidate
                            .into_pending_after_group_rollback()
                            .ok()
                            .map(|state| (index, state))
                    })
                    .collect::<Vec<_>>();
                fallback.extend(
                    evaluated
                        .into_iter()
                        .map(|(index, attempt)| (index, attempt.into_pending_without_commit())),
                );
                match continuation {
                    CommandDriverContinuation::Retry(state) => fallback.push((index, *state)),
                    continuation => {
                        completed.push((index, terminal_continuation(continuation, lifecycle)));
                    }
                }
                completed.extend(
                    drive_pending_items(
                        port,
                        conflicts,
                        administration_clock,
                        provenance,
                        durability,
                        lifecycle,
                        telemetry,
                        fallback,
                    )
                    .await,
                );
                return completed.into();
            }
        }
    }

    let (command_indices, candidates): (Vec<_>, Vec<_>) = detached.into_iter().unzip();
    let mut preparations = Vec::with_capacity(candidates.len());
    let mut retained = Vec::<RetainedDetachedCommand>::with_capacity(candidates.len());
    for candidate in candidates {
        let Ok((preparation, authority)) = split_detached_checked_candidate(candidate) else {
            empty.rollback();
            lifecycle.stop();
            return command_indices
                .into_iter()
                .enumerate()
                .map(|(position, index)| {
                    if position == 0 {
                        (index, Err(internal_defect_error()))
                    } else {
                        (
                            index,
                            Err(group_peer_error(
                                CommandExecutionErrorKind::CoordinatorStopped,
                            )),
                        )
                    }
                })
                .collect::<Vec<_>>()
                .into();
        };
        preparations.push(preparation);
        retained.push(authority);
    }
    let preparation_started = Instant::now();
    let prepared =
        evaluation_pool.prepare_detached(preparations, durability.storage_mode(), telemetry);
    evaluation_elapsed = evaluation_elapsed.saturating_add(preparation_started.elapsed());
    if prepared.iter().any(Result::is_err) {
        empty.rollback();
        lifecycle.stop();
        let failed = prepared.iter().position(Result::is_err).unwrap_or(0);
        return command_indices
            .into_iter()
            .zip(prepared)
            .enumerate()
            .map(|(position, (index, command))| {
                drop(command);
                if position == failed {
                    (index, Err(internal_defect_error()))
                } else {
                    (
                        index,
                        Err(group_peer_error(
                            CommandExecutionErrorKind::CoordinatorStopped,
                        )),
                    )
                }
            })
            .collect::<Vec<_>>()
            .into();
    }
    let joined = prepared
        .into_iter()
        .zip(retained)
        .map(|(command, authority)| {
            join_prepared_detached_command(
                command.expect("all detached preparations checked"),
                authority,
            )
        })
        .collect::<Result<Vec<_>, _>>();
    let joined = match joined {
        Ok(joined) => joined,
        Err(_) => {
            empty.rollback();
            lifecycle.stop();
            return command_indices
                .into_iter()
                .enumerate()
                .map(|(position, index)| {
                    if position == 0 {
                        (index, Err(internal_defect_error()))
                    } else {
                        (
                            index,
                            Err(group_peer_error(
                                CommandExecutionErrorKind::CoordinatorStopped,
                            )),
                        )
                    }
                })
                .collect::<Vec<_>>()
                .into();
        }
    };
    let mut indices = Vec::with_capacity(joined.len());
    let mut storage_commands = Vec::with_capacity(joined.len());
    let mut entries = Vec::with_capacity(joined.len());
    for (index, command) in command_indices.into_iter().zip(joined) {
        indices.push(index);
        let (storage, retained, expected_outcome) = command.into_parts();
        storage_commands.push(storage);
        entries.push((retained, expected_outcome));
    }
    let staged = match empty.stage_detached_group(storage_commands) {
        Ok(staged) => staged,
        Err(error) => {
            let first = indices.first().copied().unwrap_or(0);
            drop(entries);
            lifecycle.stop();
            return indices
                .into_iter()
                .map(|index| {
                    if index == first {
                        (index, Err(storage_error(error.clone(), lifecycle)))
                    } else {
                        (
                            index,
                            Err(group_peer_error(
                                CommandExecutionErrorKind::CoordinatorStopped,
                            )),
                        )
                    }
                })
                .collect::<Vec<_>>()
                .into();
        }
    };
    let staged = match checked_staged_detached_group(staged, entries, durability.storage_mode()) {
        Ok(staged) => staged,
        Err(_) => {
            lifecycle.stop();
            return indices
                .into_iter()
                .map(|index| (index, Err(internal_defect_error())))
                .collect::<Vec<_>>()
                .into();
        }
    };
    record_transaction_local_serial_pipeline_stages(
        telemetry,
        u16::try_from(indices.len()).expect("group cap fits u16"),
        evaluation_elapsed,
        serial_started.elapsed(),
    );
    commit_transaction_local_serial_group(
        port,
        administration_clock,
        lifecycle,
        telemetry,
        use_deferred_tail,
        staged,
        indices,
    )
}

#[allow(clippy::too_many_arguments)]
async fn prepare_parallel_compatible_group<P>(
    port: &P,
    conflicts: &dyn ConflictManager,
    administration_clock: &dyn crate::AdministrationClock,
    provenance: &dyn ProvenanceIdSource,
    durability: CoordinatorDurability,
    lifecycle: &dyn CommandExecutionLifecycle,
    telemetry: &dyn CommitTelemetry,
    pool: &CommandEvaluationPool,
    pending: Vec<(usize, PendingCommandAttempts)>,
    shared_conflict_lease: bool,
) -> ParallelEvaluationPreparation
where
    P: AdmissionRepository
        + SnapshotReader
        + ApplicationCommandTransactionPort
        + ExecutionFailureTransitionPort,
{
    let mut pending = std::collections::VecDeque::from(pending);
    let mut acquired = Vec::with_capacity(pending.len());
    let mut indices = Vec::with_capacity(pending.len());
    if shared_conflict_lease {
        let items = pending.drain(..).collect::<Vec<_>>();
        let (group_indices, states): (Vec<_>, Vec<_>) = items.into_iter().unzip();
        match acquire_commutative_command_group(states, conflicts).await {
            Ok(group) => {
                indices = group_indices;
                acquired = group;
            }
            Err((states, error)) => {
                let error = command_attempt_failure(error, lifecycle);
                if let Some(terminal_state) = group_peer_terminal_error(&error) {
                    let mut pairs = group_indices.into_iter().zip(states);
                    let Some((index, state)) = pairs.next() else {
                        return ParallelEvaluationPreparation::Complete(Vec::new());
                    };
                    drop(state);
                    let mut completed = vec![(index, Err(error))];
                    completed.extend(pairs.map(|(index, state)| {
                        drop(state);
                        (index, Err(group_peer_error(terminal_state)))
                    }));
                    return ParallelEvaluationPreparation::Complete(completed);
                }
                return ParallelEvaluationPreparation::Complete(
                    drive_pending_items(
                        port,
                        conflicts,
                        administration_clock,
                        provenance,
                        durability,
                        lifecycle,
                        telemetry,
                        group_indices.into_iter().zip(states).collect(),
                    )
                    .await,
                );
            }
        }
    }
    while !shared_conflict_lease && let Some((index, state)) = pending.pop_front() {
        match acquire_command_attempt(state, conflicts).await {
            Ok(attempt) => {
                indices.push(index);
                acquired.push(attempt);
            }
            Err(error) => {
                let error = command_attempt_failure(error, lifecycle);
                let terminal_state = group_peer_terminal_error(&error);
                let mut completed = vec![(index, Err(error))];
                if let Some(terminal_state) = terminal_state {
                    completed.extend(indices.into_iter().zip(acquired).map(|(index, attempt)| {
                        drop(attempt);
                        (index, Err(group_peer_error(terminal_state)))
                    }));
                    completed.extend(pending.into_iter().map(|(index, state)| {
                        drop(state);
                        (index, Err(group_peer_error(terminal_state)))
                    }));
                } else {
                    let mut fallback = indices
                        .into_iter()
                        .zip(acquired)
                        .map(|(index, attempt)| (index, attempt.into_pending_without_evaluation()))
                        .collect::<Vec<_>>();
                    fallback.extend(pending);
                    completed.extend(
                        drive_pending_items(
                            port,
                            conflicts,
                            administration_clock,
                            provenance,
                            durability,
                            lifecycle,
                            telemetry,
                            fallback,
                        )
                        .await,
                    );
                }
                return ParallelEvaluationPreparation::Complete(completed);
            }
        }
    }

    let evaluated = pool.evaluate(acquired, telemetry);
    if evaluated
        .iter()
        .all(|result| matches!(result, Ok(CommandAttemptResolution::Evaluated(_))))
    {
        return ParallelEvaluationPreparation::Ready(
            indices
                .into_iter()
                .zip(evaluated)
                .filter_map(|(index, result)| match result {
                    Ok(CommandAttemptResolution::Evaluated(attempt)) => Some((index, attempt)),
                    _ => None,
                })
                .collect(),
        );
    }

    let rollback_reason = if evaluated
        .iter()
        .any(|result| matches!(result, Err(CommandAttemptError::Integrity)))
    {
        Some(crate::PreparedEpochRollbackReason::ProofMismatch)
    } else if evaluated
        .iter()
        .any(|result| matches!(result, Err(CommandAttemptError::EvaluationPanicked)))
    {
        Some(crate::PreparedEpochRollbackReason::WorkerFailure)
    } else if evaluated.iter().any(|result| {
        matches!(
            result,
            Err(CommandAttemptError::Cancelled | CommandAttemptError::DeadlineExceeded)
        )
    }) {
        Some(crate::PreparedEpochRollbackReason::Cancelled)
    } else {
        None
    };
    if let Some(reason) = rollback_reason {
        telemetry.record(CommitTelemetryEvent::PreparedEpochRolledBack { reason });
    }

    let mut completed = Vec::new();
    let mut fallback = Vec::new();
    let mut terminal_state = None;
    for (index, result) in indices.into_iter().zip(evaluated) {
        match result {
            Ok(CommandAttemptResolution::Evaluated(attempt)) => {
                fallback.push((index, attempt.into_pending_without_commit()));
            }
            Ok(resolution) => {
                let continuation = continuation_from_attempt_resolution(
                    port,
                    Some(administration_clock),
                    lifecycle,
                    telemetry,
                    resolution,
                );
                match continuation {
                    CommandDriverContinuation::Retry(retry) => {
                        fallback.push((index, *retry));
                    }
                    continuation => {
                        terminal_state =
                            terminal_state.or_else(|| group_peer_terminal(&continuation));
                        completed.push((index, terminal_continuation(continuation, lifecycle)));
                    }
                }
            }
            Err(error) => {
                let error = command_attempt_failure(error, lifecycle);
                terminal_state = terminal_state.or_else(|| group_peer_terminal_error(&error));
                completed.push((index, Err(error)));
            }
        }
    }
    if let Some(terminal_state) = terminal_state {
        completed.extend(fallback.into_iter().map(|(index, state)| {
            drop(state);
            (index, Err(group_peer_error(terminal_state)))
        }));
    } else {
        completed.extend(
            drive_pending_items(
                port,
                conflicts,
                administration_clock,
                provenance,
                durability,
                lifecycle,
                telemetry,
                fallback,
            )
            .await,
        );
    }
    ParallelEvaluationPreparation::Complete(completed)
}

#[allow(clippy::too_many_arguments)]
async fn drive_transaction_local_serial_pending_group<P>(
    port: &P,
    conflicts: &dyn ConflictManager,
    administration_clock: &dyn crate::AdministrationClock,
    provenance: &dyn ProvenanceIdSource,
    durability: CoordinatorDurability,
    lifecycle: &dyn CommandExecutionLifecycle,
    telemetry: &dyn CommitTelemetry,
    evaluation_pool: Option<&CommandEvaluationPool>,
    parallel_writer_private: bool,
    use_deferred_tail: bool,
    evaluation_frontier: CommandEvaluationFrontier,
    pending: Vec<(usize, PendingCommandAttempts)>,
) -> IndexedCommandGroupDriveResult
where
    P: AdmissionRepository
        + SnapshotReader
        + ApplicationCommandTransactionPort
        + DeferredCommandEpochPort
        + ExecutionFailureTransitionPort,
    <P as DeferredCommandEpochPort>::Epoch:
        DeferredCommandEpoch<EmptyBatch = <P as ApplicationCommandTransactionPort>::EmptyBatch>,
    <<P as DeferredCommandEpochPort>::Epoch as DeferredCommandEpoch>::Fence: 'static,
    <FirstStagedEpoch<P> as DeferredCommandEpoch>::Fence: 'static,
    <P as ApplicationCommandTransactionPort>::EmptyBatch:
        TransactionLocalCommandBatch + DetachedCommandGroupBatch<Staged = FirstStagedBatch<P>>,
    FirstStagedBatch<P>:
        NonEmptyCommandBatch + DeferredNonEmptyCommandBatch + TransactionLocalCommandBatch,
    BatchCandidate<FirstStagedBatch<P>>: CommandCandidateAdmission<Prior = FirstStagedBatch<P>>,
    BatchStateRead<FirstStagedBatch<P>>: CommandCandidateStateRead<Prior = FirstStagedBatch<P>>,
    BatchAwaitingValidation<FirstStagedBatch<P>>:
        CommandCandidateAwaitingValidation<Prior = FirstStagedBatch<P>>,
    BatchAffectedRead<FirstStagedBatch<P>>:
        CommandCandidateAffectedEpochRead<Prior = FirstStagedBatch<P>>,
    BatchAwaitingCapacity<FirstStagedBatch<P>>:
        CommandCandidateAwaitingCapacity<Prior = FirstStagedBatch<P>>,
    BatchCapacityReserved<FirstStagedBatch<P>>:
        CommandCandidateCapacityReserved<Prior = FirstStagedBatch<P>>,
    BatchSequenceAssigned<FirstStagedBatch<P>>:
        CommandCandidateSequenceAssigned<Prior = FirstStagedBatch<P>, Staged = FirstStagedBatch<P>>,
{
    let (indices, states): (Vec<_>, Vec<_>) = pending.into_iter().unzip();
    let acquired = match evaluation_frontier {
        CommandEvaluationFrontier::Published => {
            acquire_transaction_local_serial_group(states, conflicts).await
        }
        CommandEvaluationFrontier::WriterPrivate => {
            acquire_writer_private_fifo_group(states, conflicts).await
        }
    };
    let acquired = match acquired {
        Ok(acquired) => acquired,
        Err((states, _)) => {
            return drive_pending_items(
                port,
                conflicts,
                administration_clock,
                provenance,
                durability,
                lifecycle,
                telemetry,
                indices.into_iter().zip(states).collect(),
            )
            .await
            .into();
        }
    };
    let mut acquired =
        std::collections::VecDeque::from(indices.into_iter().zip(acquired).collect::<Vec<_>>());
    let Some(first_pair) = acquired.pop_front() else {
        return Vec::new().into();
    };
    let first_index_for_open = first_pair.0;
    let mut first_slot = Some(first_pair);

    let serial_started = Instant::now();
    let mut evaluation_elapsed = Duration::ZERO;
    let empty = match if use_deferred_tail {
        port.begin_deferred_command_epoch()
            .and_then(DeferredCommandEpoch::begin_empty_batch)
    } else {
        port.begin_empty_batch()
    } {
        Ok(empty) => empty,
        Err(error) => {
            let mut completed = vec![(first_index_for_open, Err(storage_error(error, lifecycle)))];
            let fallback = acquired
                .into_iter()
                .map(|(index, attempt)| (index, attempt.into_pending_without_evaluation()))
                .collect();
            completed.extend(
                drive_pending_items(
                    port,
                    conflicts,
                    administration_clock,
                    provenance,
                    durability,
                    lifecycle,
                    telemetry,
                    fallback,
                )
                .await,
            );
            return completed.into();
        }
    };
    let mut pre_evaluated = std::collections::VecDeque::new();
    if parallel_writer_private {
        let pool = evaluation_pool.expect("parallel writer-private path requires pool");
        let mut to_capture = std::collections::VecDeque::new();
        to_capture.push_back(first_slot.take().expect("nonempty serial group"));
        to_capture.append(&mut acquired);
        let captured = match capture_writer_private_snapshots(&empty, to_capture) {
            Ok(captured) => captured,
            Err(failure) => {
                empty.rollback();
                drop(failure.failed);
                let error = command_attempt_failure(failure.error, lifecycle);
                let terminal = group_peer_terminal_error(&error);
                let mut completed = vec![(failure.failed_index, Err(error))];
                if let Some(terminal) = terminal {
                    completed.extend(failure.fallback.into_iter().map(|(index, attempt)| {
                        drop(attempt);
                        (index, Err(group_peer_error(terminal)))
                    }));
                } else {
                    let fallback = failure
                        .fallback
                        .into_iter()
                        .map(|(index, attempt)| (index, attempt.into_pending_without_evaluation()))
                        .collect();
                    completed.extend(
                        drive_pending_items(
                            port,
                            conflicts,
                            administration_clock,
                            provenance,
                            durability,
                            lifecycle,
                            telemetry,
                            fallback,
                        )
                        .await,
                    );
                }
                return completed.into();
            }
        };
        let indices = captured
            .iter()
            .map(|(index, _, _)| *index)
            .collect::<Vec<_>>();
        let inputs = captured
            .into_iter()
            .map(|(_, attempt, snapshot)| (attempt, snapshot))
            .collect();
        let evaluation_started = Instant::now();
        let evaluated = pool.evaluate_writer_private(inputs, telemetry);
        evaluation_elapsed = evaluation_elapsed.saturating_add(evaluation_started.elapsed());
        let mut fallback = Vec::new();
        let mut completed = Vec::new();
        let mut terminal = None;
        for (index, result) in indices.into_iter().zip(evaluated) {
            match result {
                Ok(CommandAttemptResolution::Evaluated(attempt)) => {
                    pre_evaluated.push_back((index, attempt));
                }
                Ok(CommandAttemptResolution::ExecutionFault(fault)) => {
                    fallback.push((index, fault.recover_pending_after_proven_rollback()));
                }
                Ok(
                    CommandAttemptResolution::OutcomeReplay(_)
                    | CommandAttemptResolution::ExecutionFailureReplay(_),
                ) => {
                    lifecycle.stop();
                    terminal = Some(CommandExecutionErrorKind::CoordinatorStopped);
                    completed.push((index, Err(internal_defect_error())));
                }
                Err(error) => {
                    let error = command_attempt_failure(error, lifecycle);
                    terminal = terminal.or_else(|| group_peer_terminal_error(&error));
                    completed.push((index, Err(error)));
                }
            }
        }
        if !fallback.is_empty() || !completed.is_empty() {
            empty.rollback();
            fallback.extend(
                pre_evaluated
                    .drain(..)
                    .map(|(index, attempt)| (index, attempt.into_pending_without_commit())),
            );
            if let Some(terminal) = terminal {
                completed.extend(fallback.into_iter().map(|(index, state)| {
                    drop(state);
                    (index, Err(group_peer_error(terminal)))
                }));
            } else {
                completed.extend(
                    drive_pending_items(
                        port,
                        conflicts,
                        administration_clock,
                        provenance,
                        durability,
                        lifecycle,
                        telemetry,
                        fallback,
                    )
                    .await,
                );
            }
            return completed.into();
        }
        return drive_detached_writer_private_group(
            port,
            conflicts,
            administration_clock,
            provenance,
            durability,
            lifecycle,
            telemetry,
            pool,
            use_deferred_tail,
            evaluation_elapsed,
            serial_started,
            empty,
            pre_evaluated,
        )
        .await;
    }

    let (first_index, first) = if let Some(first) = pre_evaluated.pop_front() {
        first
    } else {
        let (first_index, mut first) = first_slot.take().expect("nonparallel serial group");
        let evaluation_started = Instant::now();
        let first_snapshot = match empty
            .read_transaction_local_snapshot(first.transaction_local_snapshot_request())
        {
            Ok(snapshot) => snapshot,
            Err(error) => {
                empty.rollback();
                let mut completed = vec![(first_index, Err(storage_error(error, lifecycle)))];
                let fallback = acquired
                    .into_iter()
                    .map(|(index, attempt)| (index, attempt.into_pending_without_evaluation()))
                    .collect();
                completed.extend(
                    drive_pending_items(
                        port,
                        conflicts,
                        administration_clock,
                        provenance,
                        durability,
                        lifecycle,
                        telemetry,
                        fallback,
                    )
                    .await,
                );
                return completed.into();
            }
        };
        let first_snapshot = match first
            .complete_transaction_local_snapshot(first_snapshot, |request| {
                empty.read_transaction_local_snapshot(request)
            }) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                empty.rollback();
                drop(first);
                let mut completed =
                    vec![(first_index, Err(command_attempt_failure(error, lifecycle)))];
                let fallback = acquired
                    .into_iter()
                    .map(|(index, attempt)| (index, attempt.into_pending_without_evaluation()))
                    .collect();
                completed.extend(
                    drive_pending_items(
                        port,
                        conflicts,
                        administration_clock,
                        provenance,
                        durability,
                        lifecycle,
                        telemetry,
                        fallback,
                    )
                    .await,
                );
                return completed.into();
            }
        };
        let first =
            match evaluate_transaction_local_acquired_command_attempt(first, first_snapshot) {
                Ok(CommandAttemptResolution::Evaluated(attempt)) => attempt,
                Ok(CommandAttemptResolution::ExecutionFault(fault)) => {
                    empty.rollback();
                    let mut fallback =
                        vec![(first_index, fault.recover_pending_after_proven_rollback())];
                    fallback.extend(acquired.into_iter().map(|(index, attempt)| {
                        (index, attempt.into_pending_without_evaluation())
                    }));
                    return drive_pending_items(
                        port,
                        conflicts,
                        administration_clock,
                        provenance,
                        durability,
                        lifecycle,
                        telemetry,
                        fallback,
                    )
                    .await
                    .into();
                }
                Ok(
                    CommandAttemptResolution::OutcomeReplay(_)
                    | CommandAttemptResolution::ExecutionFailureReplay(_),
                ) => {
                    empty.rollback();
                    lifecycle.stop();
                    return std::iter::once((
                        first_index,
                        Err(CommandExecutionError::without_detail(
                            CommandExecutionErrorKind::InternalDefect,
                        )),
                    ))
                    .chain(acquired.into_iter().map(|(index, attempt)| {
                        drop(attempt);
                        (
                            index,
                            Err(CommandExecutionError::without_detail(
                                CommandExecutionErrorKind::CoordinatorStopped,
                            )),
                        )
                    }))
                    .collect::<Vec<_>>()
                    .into();
                }
                Err(error) => {
                    empty.rollback();
                    let mut completed =
                        vec![(first_index, Err(command_attempt_failure(error, lifecycle)))];
                    let fallback = acquired
                        .into_iter()
                        .map(|(index, attempt)| (index, attempt.into_pending_without_evaluation()))
                        .collect();
                    completed.extend(
                        drive_pending_items(
                            port,
                            conflicts,
                            administration_clock,
                            provenance,
                            durability,
                            lifecycle,
                            telemetry,
                            fallback,
                        )
                        .await,
                    );
                    return completed.into();
                }
            };
        evaluation_elapsed = evaluation_elapsed.saturating_add(evaluation_started.elapsed());
        (first_index, first)
    };

    let mut staged = match stage_first_evaluated_command_on_empty(
        port,
        empty,
        provenance,
        Some(administration_clock),
        durability,
        lifecycle,
        telemetry,
        first,
    ) {
        Ok(staged) => staged,
        Err(CommandDriverContinuation::Retry(retry)) => {
            let fallback = std::iter::once((first_index, *retry))
                .chain(
                    pre_evaluated
                        .into_iter()
                        .map(|(index, attempt)| (index, attempt.into_pending_without_commit())),
                )
                .chain(
                    acquired
                        .into_iter()
                        .map(|(index, attempt)| (index, attempt.into_pending_without_evaluation())),
                )
                .collect();
            return drive_pending_items(
                port,
                conflicts,
                administration_clock,
                provenance,
                durability,
                lifecycle,
                telemetry,
                fallback,
            )
            .await
            .into();
        }
        Err(continuation) => {
            let mut completed = vec![(first_index, terminal_continuation(continuation, lifecycle))];
            let fallback = pre_evaluated
                .into_iter()
                .map(|(index, attempt)| (index, attempt.into_pending_without_commit()))
                .chain(
                    acquired
                        .into_iter()
                        .map(|(index, attempt)| (index, attempt.into_pending_without_evaluation())),
                )
                .collect();
            completed.extend(
                drive_pending_items(
                    port,
                    conflicts,
                    administration_clock,
                    provenance,
                    durability,
                    lifecycle,
                    telemetry,
                    fallback,
                )
                .await,
            );
            return completed.into();
        }
    };
    let mut staged_indices = vec![first_index];

    loop {
        let (index, evaluated) = if let Some(evaluated) = pre_evaluated.pop_front() {
            evaluated
        } else {
            let Some((index, mut attempt)) = acquired.pop_front() else {
                break;
            };
            let evaluation_started = Instant::now();
            let snapshot = match staged
                .read_transaction_local_snapshot(attempt.transaction_local_snapshot_request())
            {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    let previous = match staged.rollback_into_retries() {
                        Ok(previous) => previous,
                        Err(()) => {
                            lifecycle.stop();
                            let _ = error;
                            drop(attempt);
                            return staged_indices
                                .into_iter()
                                .chain(std::iter::once(index))
                                .chain(acquired.into_iter().map(|(index, attempt)| {
                                    drop(attempt);
                                    index
                                }))
                                .map(|index| {
                                    (
                                        index,
                                        Err(CommandExecutionError::without_detail(
                                            CommandExecutionErrorKind::InternalDefect,
                                        )),
                                    )
                                })
                                .collect::<Vec<_>>()
                                .into();
                        }
                    };
                    let mut fallback = previous
                        .into_iter()
                        .zip(staged_indices)
                        .map(|(attempt, index)| (index, attempt))
                        .collect::<Vec<_>>();
                    drop(attempt);
                    fallback.extend(acquired.into_iter().map(|(index, attempt)| {
                        (index, attempt.into_pending_without_evaluation())
                    }));
                    let mut completed = vec![(index, Err(storage_error(error, lifecycle)))];
                    completed.extend(
                        drive_pending_items(
                            port,
                            conflicts,
                            administration_clock,
                            provenance,
                            durability,
                            lifecycle,
                            telemetry,
                            fallback,
                        )
                        .await,
                    );
                    return completed.into();
                }
            };
            let completed_snapshot = attempt
                .complete_transaction_local_snapshot(snapshot, |request| {
                    staged.read_transaction_local_snapshot(request)
                });
            let evaluated = match completed_snapshot.and_then(|snapshot| {
                evaluate_transaction_local_acquired_command_attempt(attempt, snapshot)
            }) {
                Ok(CommandAttemptResolution::Evaluated(attempt)) => attempt,
                Ok(CommandAttemptResolution::ExecutionFault(fault)) => {
                    let previous = match staged.rollback_into_retries() {
                        Ok(previous) => previous,
                        Err(()) => {
                            lifecycle.stop();
                            drop(fault);
                            return staged_indices
                                .into_iter()
                                .chain(std::iter::once(index))
                                .chain(acquired.into_iter().map(|(index, attempt)| {
                                    drop(attempt);
                                    index
                                }))
                                .map(|index| {
                                    (
                                        index,
                                        Err(CommandExecutionError::without_detail(
                                            CommandExecutionErrorKind::InternalDefect,
                                        )),
                                    )
                                })
                                .collect();
                        }
                    };
                    let mut fallback = staged_indices.into_iter().zip(previous).collect::<Vec<_>>();
                    fallback.push((index, fault.recover_pending_after_proven_rollback()));
                    fallback.extend(acquired.into_iter().map(|(index, attempt)| {
                        (index, attempt.into_pending_without_evaluation())
                    }));
                    return drive_pending_items(
                        port,
                        conflicts,
                        administration_clock,
                        provenance,
                        durability,
                        lifecycle,
                        telemetry,
                        fallback,
                    )
                    .await
                    .into();
                }
                Ok(
                    CommandAttemptResolution::OutcomeReplay(_)
                    | CommandAttemptResolution::ExecutionFailureReplay(_),
                ) => {
                    drop(staged.rollback_into_retries());
                    lifecycle.stop();
                    return staged_indices
                        .into_iter()
                        .chain(std::iter::once(index))
                        .chain(acquired.into_iter().map(|(index, attempt)| {
                            drop(attempt);
                            index
                        }))
                        .map(|index| {
                            (
                                index,
                                Err(CommandExecutionError::without_detail(
                                    CommandExecutionErrorKind::InternalDefect,
                                )),
                            )
                        })
                        .collect();
                }
                Err(error) => {
                    let previous = match staged.rollback_into_retries() {
                        Ok(previous) => previous,
                        Err(()) => {
                            lifecycle.stop();
                            let _ = error;
                            return staged_indices
                                .into_iter()
                                .chain(std::iter::once(index))
                                .chain(acquired.into_iter().map(|(index, attempt)| {
                                    drop(attempt);
                                    index
                                }))
                                .map(|index| {
                                    (
                                        index,
                                        Err(CommandExecutionError::without_detail(
                                            CommandExecutionErrorKind::InternalDefect,
                                        )),
                                    )
                                })
                                .collect();
                        }
                    };
                    let fallback = staged_indices
                        .into_iter()
                        .zip(previous)
                        .chain(acquired.into_iter().map(|(index, attempt)| {
                            (index, attempt.into_pending_without_evaluation())
                        }))
                        .collect();
                    let mut completed = drive_pending_items(
                        port,
                        conflicts,
                        administration_clock,
                        provenance,
                        durability,
                        lifecycle,
                        telemetry,
                        fallback,
                    )
                    .await;
                    completed.push((index, Err(command_attempt_failure(error, lifecycle))));
                    return completed.into();
                }
            };
            evaluation_elapsed = evaluation_elapsed.saturating_add(evaluation_started.elapsed());
            (index, evaluated)
        };

        let (prior, entries, durability_mode) = staged.into_storage_and_entries();
        match append_evaluated_command(
            port,
            prior,
            provenance,
            Some(administration_clock),
            durability,
            lifecycle,
            telemetry,
            evaluated,
        ) {
            Ok((storage, entry)) => {
                staged =
                    CheckedStagedCommand::from_appended(storage, entries, entry, durability_mode);
                staged_indices.push(index);
            }
            Err(CommandDriverContinuation::Retry(retry)) => {
                let previous = entries
                    .into_iter()
                    .zip(staged_indices)
                    .map(|(entry, index)| entry.into_retry().map(|state| (index, state)))
                    .collect::<Result<Vec<_>, _>>();
                let mut fallback = match previous {
                    Ok(previous) => previous,
                    Err(()) => {
                        lifecycle.stop();
                        drop(retry);
                        drop(acquired);
                        return Vec::new().into();
                    }
                };
                fallback.push((index, *retry));
                fallback.extend(
                    pre_evaluated
                        .into_iter()
                        .map(|(index, attempt)| (index, attempt.into_pending_without_commit())),
                );
                fallback.extend(
                    acquired
                        .into_iter()
                        .map(|(index, attempt)| (index, attempt.into_pending_without_evaluation())),
                );
                return drive_pending_items(
                    port,
                    conflicts,
                    administration_clock,
                    provenance,
                    durability,
                    lifecycle,
                    telemetry,
                    fallback,
                )
                .await
                .into();
            }
            Err(continuation) => {
                let previous = entries
                    .into_iter()
                    .zip(staged_indices.iter().copied())
                    .map(|(entry, index)| entry.into_retry().map(|state| (index, state)))
                    .collect::<Result<Vec<_>, _>>();
                let mut previous = match previous {
                    Ok(previous) => previous,
                    Err(()) => {
                        lifecycle.stop();
                        drop(continuation);
                        return staged_indices
                            .into_iter()
                            .chain(std::iter::once(index))
                            .chain(pre_evaluated.into_iter().map(|(index, attempt)| {
                                drop(attempt);
                                index
                            }))
                            .chain(acquired.into_iter().map(|(index, attempt)| {
                                drop(attempt);
                                index
                            }))
                            .map(|index| {
                                (
                                    index,
                                    Err(CommandExecutionError::without_detail(
                                        CommandExecutionErrorKind::InternalDefect,
                                    )),
                                )
                            })
                            .collect();
                    }
                };
                previous.extend(
                    pre_evaluated
                        .into_iter()
                        .map(|(index, attempt)| (index, attempt.into_pending_without_commit())),
                );
                previous.extend(
                    acquired
                        .into_iter()
                        .map(|(index, attempt)| (index, attempt.into_pending_without_evaluation())),
                );
                let mut completed = drive_pending_items(
                    port,
                    conflicts,
                    administration_clock,
                    provenance,
                    durability,
                    lifecycle,
                    telemetry,
                    previous,
                )
                .await;
                completed.push((index, terminal_continuation(continuation, lifecycle)));
                return completed.into();
            }
        }
    }

    record_transaction_local_serial_pipeline_stages(
        telemetry,
        u16::try_from(staged_indices.len()).expect("group cap fits u16"),
        evaluation_elapsed,
        serial_started.elapsed(),
    );

    commit_transaction_local_serial_group(
        port,
        administration_clock,
        lifecycle,
        telemetry,
        use_deferred_tail,
        staged,
        staged_indices,
    )
}

fn record_transaction_local_serial_pipeline_stages(
    telemetry: &dyn CommitTelemetry,
    command_count: u16,
    evaluation_elapsed: Duration,
    total_elapsed: Duration,
) {
    telemetry.record(CommitTelemetryEvent::CommandPipelineStageCompleted {
        stage: CommandPipelineStage::Evaluation,
        command_count,
        elapsed: evaluation_elapsed,
    });
    telemetry.record(CommitTelemetryEvent::CommandPipelineStageCompleted {
        stage: CommandPipelineStage::ValidationEncodingStaging,
        command_count,
        elapsed: total_elapsed.saturating_sub(evaluation_elapsed),
    });
}

fn commit_transaction_local_serial_group<P, B>(
    port: &P,
    administration_clock: &dyn crate::AdministrationClock,
    lifecycle: &dyn CommandExecutionLifecycle,
    telemetry: &dyn CommitTelemetry,
    use_deferred_tail: bool,
    staged: CheckedStagedCommand<B>,
    staged_indices: Vec<usize>,
) -> IndexedCommandGroupDriveResult
where
    P: AdmissionRepository,
    B: NonEmptyCommandBatch + DeferredNonEmptyCommandBatch,
    <B::Epoch as DeferredCommandEpoch>::Fence: 'static,
{
    let audits = {
        let audited_count = staged.audited_starts().filter(Option::is_some).count();
        if audited_count == 0 {
            Ok(None)
        } else if audited_count != staged.len() {
            Err(())
        } else {
            let mut audits = Vec::with_capacity(staged.len());
            let prepared = staged
                .audited_starts()
                .zip(staged.terminal_links())
                .zip(staged.requires_fused_starts())
                .try_for_each(|((started, link), fused)| {
                    let started = started.ok_or(())?;
                    let fused_start = if fused {
                        Some(
                            crate::audit_executor::prepare_administration_audit(
                                administration_clock,
                                started,
                            )
                            .map_err(|_| ())?,
                        )
                    } else {
                        None
                    };
                    let terminal = crate::audit_executor::prepare_command_terminal_audit(
                        administration_clock,
                        started,
                        link,
                    )
                    .map_err(|_| ())?;
                    let transition = if let Some(started) = fused_start {
                        riffdb_storage_api::CommandServiceAuditTransitionV1::started_and_terminal(
                            started, terminal,
                        )
                    } else {
                        riffdb_storage_api::CommandServiceAuditTransitionV1::terminal_only(terminal)
                    }
                    .map_err(|_| ())?;
                    audits.push(transition);
                    Ok::<(), ()>(())
                });
            prepared.map(|()| Some(audits))
        }
    };
    let audits = match audits {
        Ok(audits) => audits,
        Err(()) => {
            drop(staged.rollback_into_retries());
            lifecycle.stop();
            return staged_indices
                .into_iter()
                .map(|index| {
                    (
                        index,
                        Err(CommandExecutionError::without_detail(
                            CommandExecutionErrorKind::InternalDefect,
                        )),
                    )
                })
                .collect::<Vec<_>>()
                .into();
        }
    };

    let batch_size = staged.len();
    let commit_started_at = Instant::now();
    let commit_result = if use_deferred_tail {
        match audits {
            Some(audits) => {
                let apply_started_at = Instant::now();
                let applied = staged.apply_group_deferred(audits);
                telemetry.record(CommitTelemetryEvent::CommitApplicationCompleted {
                    elapsed: apply_started_at.elapsed(),
                    batch_size: u16::try_from(batch_size).expect("group cap fits u16"),
                });
                match applied {
                    CheckedCommandGroupApplyResult::Applied { epoch, batch } => {
                        return match seal_checked_deferred_group(epoch, batch, telemetry) {
                            Ok(fence) => {
                                let batch_size =
                                    u16::try_from(batch_size).expect("group cap fits u16");
                                telemetry.record(CommitTelemetryEvent::CommitSubmissionCompleted {
                                    elapsed: commit_started_at.elapsed(),
                                    batch_size,
                                });
                                IndexedCommandGroupDriveResult::Submitted {
                                    completed: Vec::new(),
                                    subgroup: SubmittedCommandSubgroup {
                                        indices: staged_indices,
                                        fence,
                                        commit_started_at,
                                        batch_size,
                                    },
                                }
                            }
                            Err(result) => complete_indexed_group_commit(
                                port,
                                lifecycle,
                                telemetry,
                                staged_indices,
                                result,
                            )
                            .into(),
                        };
                    }
                    CheckedCommandGroupApplyResult::Failed(result) => result,
                }
            }
            None => CheckedCommandGroupCommitResult::Integrity,
        }
    } else {
        staged.commit_group(audits)
    };
    telemetry.record(CommitTelemetryEvent::CommitCallCompleted {
        terminal: group_commit_call_terminal(&commit_result),
        elapsed: commit_started_at.elapsed(),
        batch_size: u16::try_from(batch_size).expect("group cap fits u16"),
    });
    complete_indexed_group_commit(port, lifecycle, telemetry, staged_indices, commit_result).into()
}

fn complete_indexed_group_commit<P>(
    port: &P,
    lifecycle: &dyn CommandExecutionLifecycle,
    telemetry: &dyn CommitTelemetry,
    staged_indices: Vec<usize>,
    commit_result: CheckedCommandGroupCommitResult,
) -> Vec<(usize, Result<CommandExecutionResult, CommandExecutionError>)>
where
    P: AdmissionRepository,
{
    match commit_result {
        CheckedCommandGroupCommitResult::Committed(outcomes) => staged_indices
            .into_iter()
            .zip(outcomes)
            .map(|(index, outcome)| (index, Ok(CommandExecutionResult::Committed(outcome))))
            .collect(),
        CheckedCommandGroupCommitResult::ProvenAbort { cause, retries } => {
            drop(retries);
            staged_indices
                .into_iter()
                .map(|index| {
                    (
                        index,
                        Err(CommandExecutionError::with_storage(
                            CommandExecutionErrorKind::StorageUnavailable,
                            cause.clone(),
                        )),
                    )
                })
                .collect()
        }
        CheckedCommandGroupCommitResult::StatusUnknown(uncertain) => {
            lifecycle.fence();
            #[derive(Clone, Copy, Eq, PartialEq)]
            enum SerialResolutionClass {
                Committed,
                ProvenAbsent,
                Unknown,
                Integrity,
            }
            let resolved = staged_indices
                .into_iter()
                .zip(uncertain)
                .map(|(index, uncertain)| {
                    let write = uncertain.cause().clone();
                    let resolution = resolve_uncertain_command_commit(port, Box::new(uncertain));
                    telemetry.record(CommitTelemetryEvent::UncertaintyResolved {
                        stage: CommitUncertaintyStage::CommandCommit,
                        resolution: command_uncertainty_resolution(&resolution),
                    });
                    let (class, result) = match resolution {
                        UncertainCommandCommitResolution::Committed(outcome) => (
                            SerialResolutionClass::Committed,
                            Ok(CommandExecutionResult::Committed(outcome)),
                        ),
                        UncertainCommandCommitResolution::ExecutionFailureReplay(failure) => {
                            drop(failure);
                            (
                                SerialResolutionClass::Integrity,
                                Err(CommandExecutionError::without_detail(
                                    CommandExecutionErrorKind::InternalDefect,
                                )),
                            )
                        }
                        UncertainCommandCommitResolution::ProvenNotCommitted(proven) => {
                            drop(proven.into_retry_state());
                            (
                                SerialResolutionClass::ProvenAbsent,
                                Err(CommandExecutionError::with_storage(
                                    CommandExecutionErrorKind::StorageUnavailable,
                                    write,
                                )),
                            )
                        }
                        UncertainCommandCommitResolution::OutcomeUnknown {
                            uncertain,
                            lookup_error,
                        } => {
                            drop(uncertain);
                            (
                                SerialResolutionClass::Unknown,
                                Err(CommandExecutionError::uncertain(write, Some(lookup_error))),
                            )
                        }
                        UncertainCommandCommitResolution::Integrity => (
                            SerialResolutionClass::Integrity,
                            Err(CommandExecutionError::without_detail(
                                CommandExecutionErrorKind::InternalDefect,
                            )),
                        ),
                    };
                    (index, class, result)
                })
                .collect::<Vec<_>>();
            let class = resolved.first().map(|(_, class, _)| *class);
            let contradictory = class == Some(SerialResolutionClass::Integrity)
                || resolved
                    .iter()
                    .any(|(_, candidate, _)| Some(*candidate) != class);
            if contradictory {
                lifecycle.stop();
                return resolved
                    .into_iter()
                    .map(|(index, _, _)| {
                        (
                            index,
                            Err(CommandExecutionError::without_detail(
                                CommandExecutionErrorKind::InternalDefect,
                            )),
                        )
                    })
                    .collect();
            }
            resolved
                .into_iter()
                .map(|(index, _, result)| (index, result))
                .collect()
        }
        CheckedCommandGroupCommitResult::Integrity => {
            lifecycle.stop();
            staged_indices
                .into_iter()
                .map(|index| {
                    (
                        index,
                        Err(CommandExecutionError::without_detail(
                            CommandExecutionErrorKind::InternalDefect,
                        )),
                    )
                })
                .collect()
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn drive_compatible_pending_group<P>(
    port: &P,
    conflicts: &dyn ConflictManager,
    administration_clock: &dyn crate::AdministrationClock,
    provenance: &dyn ProvenanceIdSource,
    durability: CoordinatorDurability,
    lifecycle: &dyn CommandExecutionLifecycle,
    telemetry: &dyn CommitTelemetry,
    evaluation_pool: Option<&CommandEvaluationPool>,
    use_deferred_tail: bool,
    pending: Vec<(usize, PendingCommandAttempts)>,
    shared_conflict_lease: bool,
) -> IndexedCommandGroupDriveResult
where
    P: AdmissionRepository
        + SnapshotReader
        + ApplicationCommandTransactionPort
        + DeferredCommandEpochPort
        + ExecutionFailureTransitionPort,
    <P as DeferredCommandEpochPort>::Epoch:
        DeferredCommandEpoch<EmptyBatch = <P as ApplicationCommandTransactionPort>::EmptyBatch>,
    <<P as DeferredCommandEpochPort>::Epoch as DeferredCommandEpoch>::Fence: 'static,
    <FirstStagedEpoch<P> as DeferredCommandEpoch>::Fence: 'static,
    FirstStagedBatch<P>: NonEmptyCommandBatch + DeferredNonEmptyCommandBatch,
    BatchCandidate<FirstStagedBatch<P>>: CommandCandidateAdmission<Prior = FirstStagedBatch<P>>,
    BatchStateRead<FirstStagedBatch<P>>: CommandCandidateStateRead<Prior = FirstStagedBatch<P>>,
    BatchAwaitingValidation<FirstStagedBatch<P>>:
        CommandCandidateAwaitingValidation<Prior = FirstStagedBatch<P>>,
    BatchAffectedRead<FirstStagedBatch<P>>:
        CommandCandidateAffectedEpochRead<Prior = FirstStagedBatch<P>>,
    BatchAwaitingCapacity<FirstStagedBatch<P>>:
        CommandCandidateAwaitingCapacity<Prior = FirstStagedBatch<P>>,
    BatchCapacityReserved<FirstStagedBatch<P>>:
        CommandCandidateCapacityReserved<Prior = FirstStagedBatch<P>>,
    BatchSequenceAssigned<FirstStagedBatch<P>>:
        CommandCandidateSequenceAssigned<Prior = FirstStagedBatch<P>, Staged = FirstStagedBatch<P>>,
{
    let evaluation_started = Instant::now();
    let mut pending = std::collections::VecDeque::from(pending);
    let mut retained_group = std::collections::VecDeque::new();
    let mut evaluated = Vec::with_capacity(pending.len());
    let mut completed = Vec::new();
    if let Some(pool) = evaluation_pool {
        let owned_pending = pending.into_iter().collect();
        match prepare_parallel_compatible_group(
            port,
            conflicts,
            administration_clock,
            provenance,
            durability,
            lifecycle,
            telemetry,
            pool,
            owned_pending,
            shared_conflict_lease,
        )
        .await
        {
            ParallelEvaluationPreparation::Ready(prepared) => evaluated = prepared,
            ParallelEvaluationPreparation::Complete(completed) => return completed.into(),
        }
        pending = std::collections::VecDeque::new();
    } else if shared_conflict_lease {
        let items = pending.drain(..).collect::<Vec<_>>();
        let (indices, states): (Vec<_>, Vec<_>) = items.into_iter().unzip();
        match acquire_commutative_command_group(states, conflicts).await {
            Ok(acquired) => retained_group = indices.into_iter().zip(acquired).collect(),
            Err((states, _error)) => {
                return drive_pending_items(
                    port,
                    conflicts,
                    administration_clock,
                    provenance,
                    durability,
                    lifecycle,
                    telemetry,
                    indices.into_iter().zip(states).collect(),
                )
                .await
                .into();
            }
        }
    }
    while evaluation_pool.is_none()
        && let Some((index, resolution)) = if shared_conflict_lease {
            retained_group.pop_front().map(|(index, acquired)| {
                (
                    index,
                    evaluate_acquired_command_attempt(acquired, port, port),
                )
            })
        } else {
            match pending.pop_front() {
                Some((index, state)) => Some((
                    index,
                    evaluate_next_command_attempt(state, port, port, conflicts).await,
                )),
                None => None,
            }
        }
    {
        match resolution {
            Ok(CommandAttemptResolution::Evaluated(attempt)) => evaluated.push((index, attempt)),
            Ok(resolution) => {
                let continuation = continuation_from_attempt_resolution(
                    port,
                    Some(administration_clock),
                    lifecycle,
                    telemetry,
                    resolution,
                );
                if let CommandDriverContinuation::Retry(retry) = continuation {
                    let mut fallback = evaluated
                        .into_iter()
                        .map(|(index, attempt)| (index, attempt.into_pending_without_commit()))
                        .collect::<Vec<_>>();
                    fallback.push((index, *retry));
                    fallback.extend(pending);
                    fallback.extend(retained_group.into_iter().map(|(index, attempt)| {
                        (index, attempt.into_pending_without_evaluation())
                    }));
                    completed.extend(
                        drive_pending_items(
                            port,
                            conflicts,
                            administration_clock,
                            provenance,
                            durability,
                            lifecycle,
                            telemetry,
                            fallback,
                        )
                        .await,
                    );
                    return completed.into();
                }
                let terminal_state = group_peer_terminal(&continuation);
                completed.push((index, terminal_continuation(continuation, lifecycle)));
                if let Some(terminal_state) = terminal_state {
                    completed.extend(evaluated.into_iter().map(|(index, attempt)| {
                        drop(attempt);
                        (index, Err(group_peer_error(terminal_state)))
                    }));
                    completed.extend(pending.into_iter().map(|(index, state)| {
                        drop(state);
                        (index, Err(group_peer_error(terminal_state)))
                    }));
                    completed.extend(retained_group.into_iter().map(|(index, attempt)| {
                        drop(attempt);
                        (index, Err(group_peer_error(terminal_state)))
                    }));
                    return completed.into();
                }
                let mut fallback = evaluated
                    .into_iter()
                    .map(|(index, attempt)| (index, attempt.into_pending_without_commit()))
                    .collect::<Vec<_>>();
                fallback.extend(pending);
                fallback.extend(
                    retained_group
                        .into_iter()
                        .map(|(index, attempt)| (index, attempt.into_pending_without_evaluation())),
                );
                completed.extend(
                    drive_pending_items(
                        port,
                        conflicts,
                        administration_clock,
                        provenance,
                        durability,
                        lifecycle,
                        telemetry,
                        fallback,
                    )
                    .await,
                );
                return completed.into();
            }
            Err(error) => {
                let error = command_attempt_failure(error, lifecycle);
                let terminal_state = group_peer_terminal_error(&error);
                completed.push((index, Err(error)));
                if let Some(terminal_state) = terminal_state {
                    completed.extend(evaluated.into_iter().map(|(index, attempt)| {
                        drop(attempt);
                        (index, Err(group_peer_error(terminal_state)))
                    }));
                    completed.extend(pending.into_iter().map(|(index, state)| {
                        drop(state);
                        (index, Err(group_peer_error(terminal_state)))
                    }));
                    completed.extend(retained_group.into_iter().map(|(index, attempt)| {
                        drop(attempt);
                        (index, Err(group_peer_error(terminal_state)))
                    }));
                    return completed.into();
                }
                let mut fallback = evaluated
                    .into_iter()
                    .map(|(index, attempt)| (index, attempt.into_pending_without_commit()))
                    .collect::<Vec<_>>();
                fallback.extend(pending);
                fallback.extend(
                    retained_group
                        .into_iter()
                        .map(|(index, attempt)| (index, attempt.into_pending_without_evaluation())),
                );
                completed.extend(
                    drive_pending_items(
                        port,
                        conflicts,
                        administration_clock,
                        provenance,
                        durability,
                        lifecycle,
                        telemetry,
                        fallback,
                    )
                    .await,
                );
                return completed.into();
            }
        }
    }
    telemetry.record(CommitTelemetryEvent::CommandPipelineStageCompleted {
        stage: CommandPipelineStage::Evaluation,
        command_count: u16::try_from(evaluated.len()).unwrap_or(u16::MAX),
        elapsed: evaluation_started.elapsed(),
    });

    let mut evaluated = std::collections::VecDeque::from(evaluated);
    let staging_started = Instant::now();
    let (first_index, first) = evaluated
        .pop_front()
        .expect("compatible command group is nonempty");
    let mut staged = match stage_first_evaluated_command_with_tail_policy(
        port,
        provenance,
        Some(administration_clock),
        durability,
        lifecycle,
        telemetry,
        use_deferred_tail,
        first,
    ) {
        Ok(staged) => staged,
        Err(CommandDriverContinuation::Retry(retry)) => {
            let fallback = std::iter::once((first_index, *retry))
                .chain(
                    evaluated
                        .into_iter()
                        .map(|(index, attempt)| (index, attempt.into_pending_without_commit())),
                )
                .collect();
            completed.extend(
                drive_pending_items(
                    port,
                    conflicts,
                    administration_clock,
                    provenance,
                    durability,
                    lifecycle,
                    telemetry,
                    fallback,
                )
                .await,
            );
            return completed.into();
        }
        Err(continuation) => {
            let terminal_state = group_peer_terminal(&continuation);
            completed.push((first_index, terminal_continuation(continuation, lifecycle)));
            if let Some(terminal_state) = terminal_state {
                completed.extend(evaluated.into_iter().map(|(index, attempt)| {
                    drop(attempt);
                    (index, Err(group_peer_error(terminal_state)))
                }));
                return completed.into();
            }
            let fallback = evaluated
                .into_iter()
                .map(|(index, attempt)| (index, attempt.into_pending_without_commit()))
                .collect();
            completed.extend(
                drive_pending_items(
                    port,
                    conflicts,
                    administration_clock,
                    provenance,
                    durability,
                    lifecycle,
                    telemetry,
                    fallback,
                )
                .await,
            );
            return completed.into();
        }
    };
    let mut staged_indices = vec![first_index];

    while let Some((index, attempt)) = evaluated.pop_front() {
        let (prior, entries, durability_mode) = staged.into_storage_and_entries();
        match append_evaluated_command(
            port,
            prior,
            provenance,
            Some(administration_clock),
            durability,
            lifecycle,
            telemetry,
            attempt,
        ) {
            Ok((storage, entry)) => {
                staged =
                    CheckedStagedCommand::from_appended(storage, entries, entry, durability_mode);
                staged_indices.push(index);
            }
            Err(CommandDriverContinuation::Retry(retry)) => {
                let previous = entries
                    .into_iter()
                    .zip(staged_indices.iter().copied())
                    .map(|(entry, index)| entry.into_retry().map(|state| (index, state)))
                    .collect::<Result<Vec<_>, _>>();
                let mut fallback = match previous {
                    Ok(previous) => previous,
                    Err(()) => {
                        lifecycle.stop();
                        drop(retry);
                        return completed.into();
                    }
                };
                fallback.push((index, *retry));
                fallback.extend(
                    evaluated
                        .into_iter()
                        .map(|(index, attempt)| (index, attempt.into_pending_without_commit())),
                );
                completed.extend(
                    drive_pending_items(
                        port,
                        conflicts,
                        administration_clock,
                        provenance,
                        durability,
                        lifecycle,
                        telemetry,
                        fallback,
                    )
                    .await,
                );
                return completed.into();
            }
            Err(continuation) => {
                let terminal_state = group_peer_terminal(&continuation);
                completed.push((index, terminal_continuation(continuation, lifecycle)));
                if let Some(terminal_state) = terminal_state {
                    drop(entries);
                    completed.extend(
                        staged_indices
                            .into_iter()
                            .map(|index| (index, Err(group_peer_error(terminal_state)))),
                    );
                    completed.extend(evaluated.into_iter().map(|(index, attempt)| {
                        drop(attempt);
                        (index, Err(group_peer_error(terminal_state)))
                    }));
                    return completed.into();
                }
                let previous = entries
                    .into_iter()
                    .zip(staged_indices)
                    .map(|(entry, index)| entry.into_retry().map(|state| (index, state)))
                    .collect::<Result<Vec<_>, _>>();
                let mut fallback = match previous {
                    Ok(previous) => previous,
                    Err(()) => {
                        lifecycle.stop();
                        return completed.into();
                    }
                };
                fallback.extend(
                    evaluated
                        .into_iter()
                        .map(|(index, attempt)| (index, attempt.into_pending_without_commit())),
                );
                completed.extend(
                    drive_pending_items(
                        port,
                        conflicts,
                        administration_clock,
                        provenance,
                        durability,
                        lifecycle,
                        telemetry,
                        fallback,
                    )
                    .await,
                );
                return completed.into();
            }
        }
    }

    let audits = {
        let audited_count = staged.audited_starts().filter(Option::is_some).count();
        if audited_count == 0 {
            Ok(None)
        } else if audited_count != staged.len() {
            Err(())
        } else {
            let mut audits = Vec::with_capacity(staged.len());
            let prepared = staged
                .audited_starts()
                .zip(staged.terminal_links())
                .zip(staged.requires_fused_starts())
                .try_for_each(|((started, link), fused)| {
                    let started = started.ok_or(())?;
                    let fused_start = if fused {
                        Some(
                            crate::audit_executor::prepare_administration_audit(
                                administration_clock,
                                started,
                            )
                            .map_err(|_| ())?,
                        )
                    } else {
                        None
                    };
                    let terminal = crate::audit_executor::prepare_command_terminal_audit(
                        administration_clock,
                        started,
                        link,
                    )
                    .map_err(|_| ())?;
                    let transition = if let Some(started) = fused_start {
                        riffdb_storage_api::CommandServiceAuditTransitionV1::started_and_terminal(
                            started, terminal,
                        )
                    } else {
                        riffdb_storage_api::CommandServiceAuditTransitionV1::terminal_only(terminal)
                    }
                    .map_err(|_| ())?;
                    audits.push(transition);
                    Ok::<(), ()>(())
                });
            prepared.map(|()| Some(audits))
        }
    };
    let audits = match audits {
        Ok(audits) => audits,
        Err(()) => {
            drop(staged.rollback_into_retries());
            lifecycle.stop();
            completed.extend(staged_indices.into_iter().map(|index| {
                (
                    index,
                    Err(CommandExecutionError::without_detail(
                        CommandExecutionErrorKind::InternalDefect,
                    )),
                )
            }));
            return completed.into();
        }
    };

    let batch_size = staged.len();
    telemetry.record(CommitTelemetryEvent::CommandPipelineStageCompleted {
        stage: CommandPipelineStage::ValidationEncodingStaging,
        command_count: u16::try_from(batch_size).expect("group cap fits u16"),
        elapsed: staging_started.elapsed(),
    });
    let commit_started_at = Instant::now();
    let commit_result = if use_deferred_tail {
        match audits {
            Some(audits) => {
                let apply_started_at = Instant::now();
                let applied = staged.apply_group_deferred(audits);
                telemetry.record(CommitTelemetryEvent::CommitApplicationCompleted {
                    elapsed: apply_started_at.elapsed(),
                    batch_size: u16::try_from(batch_size).expect("group cap fits u16"),
                });
                match applied {
                    CheckedCommandGroupApplyResult::Applied { epoch, batch } => {
                        return match seal_checked_deferred_group(epoch, batch, telemetry) {
                            Ok(fence) => {
                                let batch_size =
                                    u16::try_from(batch_size).expect("group cap fits u16");
                                telemetry.record(CommitTelemetryEvent::CommitSubmissionCompleted {
                                    elapsed: commit_started_at.elapsed(),
                                    batch_size,
                                });
                                IndexedCommandGroupDriveResult::Submitted {
                                    completed,
                                    subgroup: SubmittedCommandSubgroup {
                                        indices: staged_indices,
                                        fence,
                                        commit_started_at,
                                        batch_size,
                                    },
                                }
                            }
                            Err(result) => {
                                completed.extend(complete_indexed_group_commit(
                                    port,
                                    lifecycle,
                                    telemetry,
                                    staged_indices,
                                    result,
                                ));
                                completed.into()
                            }
                        };
                    }
                    CheckedCommandGroupApplyResult::Failed(result) => result,
                }
            }
            None => CheckedCommandGroupCommitResult::Integrity,
        }
    } else {
        staged.commit_group(audits)
    };
    telemetry.record(CommitTelemetryEvent::CommitCallCompleted {
        terminal: group_commit_call_terminal(&commit_result),
        elapsed: commit_started_at.elapsed(),
        batch_size: u16::try_from(batch_size).expect("group cap fits u16"),
    });
    completed.extend(complete_indexed_group_commit(
        port,
        lifecycle,
        telemetry,
        staged_indices,
        commit_result,
    ));
    completed.into()
}

fn group_peer_terminal(
    continuation: &CommandDriverContinuation,
) -> Option<CommandExecutionErrorKind> {
    match continuation {
        CommandDriverContinuation::Failed(error) => group_peer_terminal_error(error),
        CommandDriverContinuation::Complete(_) | CommandDriverContinuation::Retry(_) => None,
    }
}

fn group_peer_terminal_error(error: &CommandExecutionError) -> Option<CommandExecutionErrorKind> {
    match error.kind() {
        CommandExecutionErrorKind::OutcomeUnknown
        | CommandExecutionErrorKind::CoordinatorFenced => {
            Some(CommandExecutionErrorKind::CoordinatorFenced)
        }
        CommandExecutionErrorKind::InternalDefect
        | CommandExecutionErrorKind::CoordinatorStopped => {
            Some(CommandExecutionErrorKind::CoordinatorStopped)
        }
        CommandExecutionErrorKind::AuthorizationDenied
        | CommandExecutionErrorKind::Cancelled
        | CommandExecutionErrorKind::DeadlineExceeded
        | CommandExecutionErrorKind::RetryBudgetExhausted
        | CommandExecutionErrorKind::StorageUnavailable => None,
    }
}

fn group_peer_error(kind: CommandExecutionErrorKind) -> CommandExecutionError {
    if kind == CommandExecutionErrorKind::CoordinatorFenced {
        CommandExecutionError::coordinator_fenced()
    } else {
        CommandExecutionError::coordinator_stopped()
    }
}

fn continuation_from_attempt_resolution<P>(
    port: &P,
    administration_clock: Option<&dyn crate::AdministrationClock>,
    lifecycle: &dyn CommandExecutionLifecycle,
    telemetry: &dyn CommitTelemetry,
    resolution: CommandAttemptResolution,
) -> CommandDriverContinuation
where
    P: ExecutionFailureTransitionPort + AdmissionRepository,
{
    match resolution {
        CommandAttemptResolution::Evaluated(attempt) => {
            drop(attempt);
            internal_defect(lifecycle)
        }
        CommandAttemptResolution::ExecutionFault(attempt) => {
            continue_execution_fault(port, administration_clock, lifecycle, telemetry, attempt)
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
            execution_failure_replay(failure.code())
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn drive_pending_items<P>(
    port: &P,
    conflicts: &dyn ConflictManager,
    administration_clock: &dyn crate::AdministrationClock,
    provenance: &dyn ProvenanceIdSource,
    durability: CoordinatorDurability,
    lifecycle: &dyn CommandExecutionLifecycle,
    telemetry: &dyn CommitTelemetry,
    pending: Vec<(usize, PendingCommandAttempts)>,
) -> Vec<(usize, Result<CommandExecutionResult, CommandExecutionError>)>
where
    P: AdmissionRepository
        + SnapshotReader
        + ApplicationCommandTransactionPort
        + ExecutionFailureTransitionPort,
{
    let mut results = Vec::with_capacity(pending.len());
    for (index, state) in pending {
        results.push((
            index,
            drive_pending_command_attempts(
                port,
                conflicts,
                provenance,
                Some(administration_clock),
                durability,
                lifecycle,
                telemetry,
                state,
            )
            .await,
        ));
    }
    results
}

#[allow(clippy::too_many_arguments)]
async fn drive_pending_command_attempts<P>(
    port: &P,
    conflicts: &dyn ConflictManager,
    provenance: &dyn ProvenanceIdSource,
    administration_clock: Option<&dyn crate::AdministrationClock>,
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
                port,
                provenance,
                administration_clock,
                durability,
                lifecycle,
                telemetry,
                attempt,
            ),
            CommandAttemptResolution::ExecutionFault(attempt) => {
                continue_execution_fault(port, administration_clock, lifecycle, telemetry, attempt)
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
                execution_failure_replay(failure.code())
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

const fn group_commit_call_terminal(
    result: &CheckedCommandGroupCommitResult,
) -> CommitCallTerminal {
    match result {
        CheckedCommandGroupCommitResult::Committed(_) => CommitCallTerminal::Committed,
        CheckedCommandGroupCommitResult::ProvenAbort { .. } => CommitCallTerminal::ProvenAbort,
        CheckedCommandGroupCommitResult::StatusUnknown(_) => CommitCallTerminal::StatusUnknown,
        CheckedCommandGroupCommitResult::Integrity => CommitCallTerminal::Integrity,
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
    administration_clock: Option<&dyn crate::AdministrationClock>,
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
    let attempt = match attempt.authorize_after_evaluation() {
        Ok(attempt) => attempt,
        Err(error) => return post_evaluation_authorization_failure(error, lifecycle),
    };
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
            return execution_failure_replay(failure.code());
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
                administration_clock,
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
                administration_clock,
                lifecycle,
                telemetry,
                rejected.reject_storage_and_rollback(),
            );
        }
        Err(_) => return internal_defect(lifecycle),
    };
    let validated = match validated.recheck_row_policy() {
        CheckedRowPolicyDecision::Authorized(validated) => validated,
        CheckedRowPolicyDecision::Denied(rejected) => {
            return after_rollback(
                port,
                administration_clock,
                lifecycle,
                telemetry,
                rejected.reject_storage_and_rollback(),
            );
        }
        CheckedRowPolicyDecision::StorageFailure(error) => {
            return proven_storage_failure(error, lifecycle);
        }
        CheckedRowPolicyDecision::Integrity => return internal_defect(lifecycle),
    };
    let indexed = match derive_checked_command_indexes(validated) {
        Ok(indexed) => indexed,
        Err(_) => return internal_defect(lifecycle),
    };
    let indexed = match indexed.read_affected_epoch_current() {
        CheckedAffectedEpochDecision::Ready(indexed) => indexed,
        CheckedAffectedEpochDecision::Rejected(rejected) => {
            return after_rollback(port, administration_clock, lifecycle, telemetry, rejected);
        }
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
    let audit = match (staged.audited_start(), administration_clock) {
        (Some(started), Some(clock)) => {
            let fused_start = if staged.requires_fused_start() {
                match crate::audit_executor::prepare_administration_audit(clock, started) {
                    Ok(started) => Some(started),
                    Err(_) => return internal_defect(lifecycle),
                }
            } else {
                None
            };
            let terminal = match crate::audit_executor::prepare_command_terminal_audit(
                clock,
                started,
                staged.terminal_link(),
            ) {
                Ok(terminal) => terminal,
                Err(_) => return internal_defect(lifecycle),
            };
            let transition = if let Some(started) = fused_start {
                riffdb_storage_api::CommandServiceAuditTransitionV1::started_and_terminal(
                    started, terminal,
                )
            } else {
                riffdb_storage_api::CommandServiceAuditTransitionV1::terminal_only(terminal)
            };
            match transition {
                Ok(transition) => Some(transition),
                Err(_) => return internal_defect(lifecycle),
            }
        }
        (None, _) => None,
        (Some(_), None) => return internal_defect(lifecycle),
    };
    let commit_started_at = Instant::now();
    let commit_result = staged.commit(audit);
    telemetry.record(CommitTelemetryEvent::CommitCallCompleted {
        terminal: commit_call_terminal(&commit_result),
        elapsed: commit_started_at.elapsed(),
        batch_size: 1,
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
                    execution_failure_replay(failure.code())
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

fn stage_first_evaluated_command<P>(
    port: &P,
    provenance: &dyn ProvenanceIdSource,
    administration_clock: Option<&dyn crate::AdministrationClock>,
    durability: CoordinatorDurability,
    lifecycle: &dyn CommandExecutionLifecycle,
    telemetry: &dyn CommitTelemetry,
    attempt: EvaluatedCommandAttempt,
) -> Result<CheckedStagedCommand<FirstStagedBatch<P>>, CommandDriverContinuation>
where
    P: ApplicationCommandTransactionPort
        + ExecutionFailureTransitionPort
        + riffdb_storage_api::AdmissionRepository,
{
    let attempt = bind_evaluated_command_provenance(provenance, lifecycle, attempt)?;
    finish_first_bound_command_candidate(
        port,
        administration_clock,
        durability,
        lifecycle,
        telemetry,
        begin_bound_command_candidate(port, attempt),
    )
}

#[allow(clippy::too_many_arguments)]
fn stage_first_evaluated_command_with_tail_policy<P>(
    port: &P,
    provenance: &dyn ProvenanceIdSource,
    administration_clock: Option<&dyn crate::AdministrationClock>,
    durability: CoordinatorDurability,
    lifecycle: &dyn CommandExecutionLifecycle,
    telemetry: &dyn CommitTelemetry,
    use_deferred_tail: bool,
    attempt: EvaluatedCommandAttempt,
) -> Result<CheckedStagedCommand<FirstStagedBatch<P>>, CommandDriverContinuation>
where
    P: ApplicationCommandTransactionPort
        + DeferredCommandEpochPort
        + ExecutionFailureTransitionPort
        + riffdb_storage_api::AdmissionRepository,
    <P as DeferredCommandEpochPort>::Epoch:
        DeferredCommandEpoch<EmptyBatch = <P as ApplicationCommandTransactionPort>::EmptyBatch>,
{
    if !use_deferred_tail {
        return stage_first_evaluated_command(
            port,
            provenance,
            administration_clock,
            durability,
            lifecycle,
            telemetry,
            attempt,
        );
    }
    let empty = match port
        .begin_deferred_command_epoch()
        .and_then(DeferredCommandEpoch::begin_empty_batch)
    {
        Ok(empty) => empty,
        Err(error) => {
            drop(attempt);
            return Err(CommandDriverContinuation::Failed(storage_error(
                error, lifecycle,
            )));
        }
    };
    stage_first_evaluated_command_on_empty(
        port,
        empty,
        provenance,
        administration_clock,
        durability,
        lifecycle,
        telemetry,
        attempt,
    )
}

#[allow(clippy::too_many_arguments)]
fn stage_first_evaluated_command_on_empty<P>(
    port: &P,
    empty: <P as ApplicationCommandTransactionPort>::EmptyBatch,
    provenance: &dyn ProvenanceIdSource,
    administration_clock: Option<&dyn crate::AdministrationClock>,
    durability: CoordinatorDurability,
    lifecycle: &dyn CommandExecutionLifecycle,
    telemetry: &dyn CommitTelemetry,
    attempt: EvaluatedCommandAttempt,
) -> Result<CheckedStagedCommand<FirstStagedBatch<P>>, CommandDriverContinuation>
where
    P: ApplicationCommandTransactionPort
        + ExecutionFailureTransitionPort
        + riffdb_storage_api::AdmissionRepository,
{
    let attempt = match bind_evaluated_command_provenance(provenance, lifecycle, attempt) {
        Ok(attempt) => attempt,
        Err(error) => {
            empty.rollback();
            return Err(error);
        }
    };
    finish_first_bound_command_candidate(
        port,
        administration_clock,
        durability,
        lifecycle,
        telemetry,
        begin_bound_command_candidate_on_empty(empty, attempt),
    )
}

#[allow(clippy::too_many_arguments)]
fn detach_evaluated_command_on_empty<P>(
    port: &P,
    empty: <P as ApplicationCommandTransactionPort>::EmptyBatch,
    provenance: &dyn ProvenanceIdSource,
    administration_clock: Option<&dyn crate::AdministrationClock>,
    lifecycle: &dyn CommandExecutionLifecycle,
    telemetry: &dyn CommitTelemetry,
    attempt: EvaluatedCommandAttempt,
) -> Result<
    (
        <P as ApplicationCommandTransactionPort>::EmptyBatch,
        DetachedCheckedCommitCandidate,
    ),
    CommandDriverContinuation,
>
where
    P: ApplicationCommandTransactionPort
        + ExecutionFailureTransitionPort
        + riffdb_storage_api::AdmissionRepository,
{
    let attempt = match bind_evaluated_command_provenance(provenance, lifecycle, attempt) {
        Ok(attempt) => attempt,
        Err(error) => {
            empty.rollback();
            return Err(error);
        }
    };
    let bound = match begin_bound_command_candidate_on_empty(empty, attempt) {
        CommandCandidateChainStart::Ready(bound) => bound,
        CommandCandidateChainStart::OutcomeReplay(outcome) => {
            return Err(committed_replay(outcome));
        }
        CommandCandidateChainStart::ExecutionFailureReplay(failure) => {
            return Err(execution_failure_replay(failure.code()));
        }
        CommandCandidateChainStart::InputMismatch => {
            return Err(CommandDriverContinuation::Complete(
                CommandExecutionResult::InputMismatch,
            ));
        }
        CommandCandidateChainStart::StorageFailure(error) => {
            return Err(proven_storage_failure(error, lifecycle));
        }
        CommandCandidateChainStart::Integrity => return Err(internal_defect(lifecycle)),
    };
    let current = match bound.read_transaction_current() {
        TransactionCurrentAttemptDecision::Ready(current) => current,
        TransactionCurrentAttemptDecision::DependencyChanged(changed) => {
            return Err(after_rollback(
                port,
                administration_clock,
                lifecycle,
                telemetry,
                changed.reject_storage_and_rollback(),
            ));
        }
        TransactionCurrentAttemptDecision::StorageFailure(error) => {
            return Err(proven_storage_failure(error, lifecycle));
        }
        TransactionCurrentAttemptDecision::Integrity => return Err(internal_defect(lifecycle)),
    };
    let validated = match validate_checked_transaction_current(current) {
        Ok(CheckedCandidateDecision::Validated(validated)) => validated,
        Ok(CheckedCandidateDecision::Rejected(rejected)) => {
            return Err(after_rollback(
                port,
                administration_clock,
                lifecycle,
                telemetry,
                rejected.reject_storage_and_rollback(),
            ));
        }
        Err(_) => return Err(internal_defect(lifecycle)),
    };
    let validated = match validated.recheck_row_policy() {
        CheckedRowPolicyDecision::Authorized(validated) => validated,
        CheckedRowPolicyDecision::Denied(rejected) => {
            return Err(after_rollback(
                port,
                administration_clock,
                lifecycle,
                telemetry,
                rejected.reject_storage_and_rollback(),
            ));
        }
        CheckedRowPolicyDecision::StorageFailure(error) => {
            return Err(proven_storage_failure(error, lifecycle));
        }
        CheckedRowPolicyDecision::Integrity => return Err(internal_defect(lifecycle)),
    };
    let indexed =
        derive_checked_command_indexes(validated).map_err(|_| internal_defect(lifecycle))?;
    let indexed = match indexed.read_affected_epoch_current() {
        CheckedAffectedEpochDecision::Ready(indexed) => indexed,
        CheckedAffectedEpochDecision::Rejected(rejected) => {
            return Err(after_rollback(
                port,
                administration_clock,
                lifecycle,
                telemetry,
                rejected,
            ));
        }
        CheckedAffectedEpochDecision::StorageFailure(error) => {
            return Err(proven_storage_failure(error, lifecycle));
        }
    };
    let reserved = resolve_checked_reserve_decision(indexed.reserve_capacity(), lifecycle)?;
    let assigned = match reserved.assign_sequence() {
        CheckedAssignDecision::Assigned(assigned) => assigned,
        CheckedAssignDecision::StorageFailure(error) => {
            return Err(proven_storage_failure(error, lifecycle));
        }
        CheckedAssignDecision::Integrity => return Err(internal_defect(lifecycle)),
    };
    match assigned.detach() {
        CheckedCandidateDetach::Detached { prior, candidate } => Ok((prior, *candidate)),
        CheckedCandidateDetach::StorageFailure(error) => {
            Err(proven_storage_failure(error, lifecycle))
        }
        CheckedCandidateDetach::Integrity => Err(internal_defect(lifecycle)),
    }
}

fn bind_evaluated_command_provenance(
    provenance: &dyn ProvenanceIdSource,
    lifecycle: &dyn CommandExecutionLifecycle,
    attempt: EvaluatedCommandAttempt,
) -> Result<ProvenanceBoundCommandAttempt, CommandDriverContinuation> {
    let attempt = attempt
        .authorize_after_evaluation()
        .map_err(|error| post_evaluation_authorization_failure(error, lifecycle))?;
    if let Err(error) = attempt.recheck_request_control() {
        return Err(CommandDriverContinuation::Failed(command_attempt_failure(
            error, lifecycle,
        )));
    }
    let provenance_id = provenance
        .next_provenance_id()
        .map_err(|error| CommandDriverContinuation::Failed(provenance_source_failure(error)))?;
    let attempt = attempt
        .bind_provenance(provenance_id)
        .map_err(|_| internal_defect(lifecycle))?;
    Ok(attempt)
}

#[allow(clippy::too_many_arguments)]
fn finish_first_bound_command_candidate<P>(
    port: &P,
    administration_clock: Option<&dyn crate::AdministrationClock>,
    durability: CoordinatorDurability,
    lifecycle: &dyn CommandExecutionLifecycle,
    telemetry: &dyn CommitTelemetry,
    start: CommandCandidateChainStart<EmptyStateRead<P>>,
) -> Result<CheckedStagedCommand<FirstStagedBatch<P>>, CommandDriverContinuation>
where
    P: ApplicationCommandTransactionPort
        + ExecutionFailureTransitionPort
        + riffdb_storage_api::AdmissionRepository,
{
    let bound = match start {
        CommandCandidateChainStart::Ready(bound) => bound,
        CommandCandidateChainStart::OutcomeReplay(outcome) => {
            return Err(committed_replay(outcome));
        }
        CommandCandidateChainStart::ExecutionFailureReplay(failure) => {
            return Err(execution_failure_replay(failure.code()));
        }
        CommandCandidateChainStart::InputMismatch => {
            return Err(CommandDriverContinuation::Complete(
                CommandExecutionResult::InputMismatch,
            ));
        }
        CommandCandidateChainStart::StorageFailure(error) => {
            return Err(proven_storage_failure(error, lifecycle));
        }
        CommandCandidateChainStart::Integrity => return Err(internal_defect(lifecycle)),
    };
    let current = match bound.read_transaction_current() {
        TransactionCurrentAttemptDecision::Ready(current) => current,
        TransactionCurrentAttemptDecision::DependencyChanged(changed) => {
            return Err(after_rollback(
                port,
                administration_clock,
                lifecycle,
                telemetry,
                changed.reject_storage_and_rollback(),
            ));
        }
        TransactionCurrentAttemptDecision::StorageFailure(error) => {
            return Err(proven_storage_failure(error, lifecycle));
        }
        TransactionCurrentAttemptDecision::Integrity => return Err(internal_defect(lifecycle)),
    };
    let validated = match validate_checked_transaction_current(current) {
        Ok(CheckedCandidateDecision::Validated(validated)) => validated,
        Ok(CheckedCandidateDecision::Rejected(rejected)) => {
            return Err(after_rollback(
                port,
                administration_clock,
                lifecycle,
                telemetry,
                rejected.reject_storage_and_rollback(),
            ));
        }
        Err(_) => return Err(internal_defect(lifecycle)),
    };
    let validated = match validated.recheck_row_policy() {
        CheckedRowPolicyDecision::Authorized(validated) => validated,
        CheckedRowPolicyDecision::Denied(rejected) => {
            return Err(after_rollback(
                port,
                administration_clock,
                lifecycle,
                telemetry,
                rejected.reject_storage_and_rollback(),
            ));
        }
        CheckedRowPolicyDecision::StorageFailure(error) => {
            return Err(proven_storage_failure(error, lifecycle));
        }
        CheckedRowPolicyDecision::Integrity => return Err(internal_defect(lifecycle)),
    };
    let indexed =
        derive_checked_command_indexes(validated).map_err(|_| internal_defect(lifecycle))?;
    let indexed = match indexed.read_affected_epoch_current() {
        CheckedAffectedEpochDecision::Ready(indexed) => indexed,
        CheckedAffectedEpochDecision::Rejected(rejected) => {
            return Err(after_rollback(
                port,
                administration_clock,
                lifecycle,
                telemetry,
                rejected,
            ));
        }
        CheckedAffectedEpochDecision::StorageFailure(error) => {
            return Err(proven_storage_failure(error, lifecycle));
        }
    };
    let reserved = resolve_checked_reserve_decision(indexed.reserve_capacity(), lifecycle)?;
    let assigned = match reserved.assign_sequence() {
        CheckedAssignDecision::Assigned(assigned) => assigned,
        CheckedAssignDecision::StorageFailure(error) => {
            return Err(proven_storage_failure(error, lifecycle));
        }
        CheckedAssignDecision::Integrity => return Err(internal_defect(lifecycle)),
    };
    build_and_stage_checked_candidate(assigned, durability.storage_mode()).map_err(|error| {
        match error {
            CheckedCommandStageError::Storage(error) => proven_storage_failure(error, lifecycle),
            CheckedCommandStageError::InternalDefect(_) => internal_defect(lifecycle),
        }
    })
}

#[allow(clippy::too_many_arguments)]
fn append_evaluated_command<P, B>(
    port: &P,
    prior: B,
    provenance: &dyn ProvenanceIdSource,
    administration_clock: Option<&dyn crate::AdministrationClock>,
    durability: CoordinatorDurability,
    lifecycle: &dyn CommandExecutionLifecycle,
    telemetry: &dyn CommitTelemetry,
    attempt: EvaluatedCommandAttempt,
) -> Result<(B, CheckedStagedCommandEntry), CommandDriverContinuation>
where
    P: ExecutionFailureTransitionPort + riffdb_storage_api::AdmissionRepository,
    B: NonEmptyCommandBatch,
    BatchCandidate<B>: CommandCandidateAdmission<Prior = B>,
    BatchStateRead<B>: CommandCandidateStateRead<Prior = B>,
    BatchAwaitingValidation<B>: CommandCandidateAwaitingValidation<Prior = B>,
    BatchAffectedRead<B>: CommandCandidateAffectedEpochRead<Prior = B>,
    BatchAwaitingCapacity<B>: CommandCandidateAwaitingCapacity<Prior = B>,
    BatchCapacityReserved<B>: CommandCandidateCapacityReserved<Prior = B>,
    BatchSequenceAssigned<B>: CommandCandidateSequenceAssigned<Prior = B, Staged = B>,
{
    let attempt = match attempt.authorize_after_evaluation() {
        Ok(attempt) => attempt,
        Err(error) => {
            drop(prior);
            return Err(post_evaluation_authorization_failure(error, lifecycle));
        }
    };
    if let Err(error) = attempt.recheck_request_control() {
        drop(prior);
        return Err(CommandDriverContinuation::Failed(command_attempt_failure(
            error, lifecycle,
        )));
    }
    let provenance_id = match provenance.next_provenance_id() {
        Ok(provenance_id) => provenance_id,
        Err(error) => {
            drop(prior);
            return Err(CommandDriverContinuation::Failed(
                provenance_source_failure(error),
            ));
        }
    };
    let attempt = match attempt.bind_provenance(provenance_id) {
        Ok(attempt) => attempt,
        Err(_) => {
            drop(prior);
            return Err(internal_defect(lifecycle));
        }
    };
    let bound = match begin_bound_command_candidate_on_prior(prior, attempt) {
        CommandCandidateChainStart::Ready(bound) => bound,
        CommandCandidateChainStart::OutcomeReplay(outcome) => {
            return Err(committed_replay(outcome));
        }
        CommandCandidateChainStart::ExecutionFailureReplay(failure) => {
            return Err(execution_failure_replay(failure.code()));
        }
        CommandCandidateChainStart::InputMismatch => {
            return Err(CommandDriverContinuation::Complete(
                CommandExecutionResult::InputMismatch,
            ));
        }
        CommandCandidateChainStart::StorageFailure(error) => {
            return Err(proven_storage_failure(error, lifecycle));
        }
        CommandCandidateChainStart::Integrity => return Err(internal_defect(lifecycle)),
    };
    let current = match bound.read_transaction_current() {
        TransactionCurrentAttemptDecision::Ready(current) => current,
        TransactionCurrentAttemptDecision::DependencyChanged(changed) => {
            return Err(after_rollback(
                port,
                administration_clock,
                lifecycle,
                telemetry,
                changed.reject_storage_and_rollback(),
            ));
        }
        TransactionCurrentAttemptDecision::StorageFailure(error) => {
            return Err(proven_storage_failure(error, lifecycle));
        }
        TransactionCurrentAttemptDecision::Integrity => return Err(internal_defect(lifecycle)),
    };
    let validated = match validate_checked_transaction_current(current) {
        Ok(CheckedCandidateDecision::Validated(validated)) => validated,
        Ok(CheckedCandidateDecision::Rejected(rejected)) => {
            return Err(after_rollback(
                port,
                administration_clock,
                lifecycle,
                telemetry,
                rejected.reject_storage_and_rollback(),
            ));
        }
        Err(_) => return Err(internal_defect(lifecycle)),
    };
    let validated = match validated.recheck_row_policy() {
        CheckedRowPolicyDecision::Authorized(validated) => validated,
        CheckedRowPolicyDecision::Denied(rejected) => {
            return Err(after_rollback(
                port,
                administration_clock,
                lifecycle,
                telemetry,
                rejected.reject_storage_and_rollback(),
            ));
        }
        CheckedRowPolicyDecision::StorageFailure(error) => {
            return Err(proven_storage_failure(error, lifecycle));
        }
        CheckedRowPolicyDecision::Integrity => return Err(internal_defect(lifecycle)),
    };
    let indexed =
        derive_checked_command_indexes(validated).map_err(|_| internal_defect(lifecycle))?;
    let indexed = match indexed.read_affected_epoch_current() {
        CheckedAffectedEpochDecision::Ready(indexed) => indexed,
        CheckedAffectedEpochDecision::Rejected(rejected) => {
            return Err(after_rollback(
                port,
                administration_clock,
                lifecycle,
                telemetry,
                rejected,
            ));
        }
        CheckedAffectedEpochDecision::StorageFailure(error) => {
            return Err(proven_storage_failure(error, lifecycle));
        }
    };
    let reserved = resolve_checked_reserve_decision(indexed.reserve_capacity(), lifecycle)?;
    let assigned = match reserved.assign_sequence() {
        CheckedAssignDecision::Assigned(assigned) => assigned,
        CheckedAssignDecision::StorageFailure(error) => {
            return Err(proven_storage_failure(error, lifecycle));
        }
        CheckedAssignDecision::Integrity => return Err(internal_defect(lifecycle)),
    };
    build_and_stage_checked_candidate_on_prior(assigned, durability.storage_mode()).map_err(
        |error| match error {
            CheckedCommandStageError::Storage(error) => proven_storage_failure(error, lifecycle),
            CheckedCommandStageError::InternalDefect(_) => internal_defect(lifecycle),
        },
    )
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

fn prepare_execution_failure_audit(
    attempt: &ExecutionFaultAttempt,
    administration_clock: Option<&dyn crate::AdministrationClock>,
) -> Result<Option<riffdb_storage_api::CommandServiceAuditTransitionV1>, ()> {
    let Some(audited) = attempt.audited_lifecycle() else {
        return Ok(None);
    };
    let clock = administration_clock.ok_or(())?;
    let started_input = audited.started.as_ref();
    if attempt.requires_fused_start() {
        let started = crate::audit_executor::prepare_administration_audit(clock, started_input)
            .map_err(|_| ())?;
        let terminal =
            crate::audit_executor::prepare_command_failure_terminal_audit(clock, started_input)
                .map_err(|_| ())?;
        riffdb_storage_api::CommandServiceAuditTransitionV1::started_and_failure(started, terminal)
            .map(Some)
            .map_err(|_| ())
    } else {
        let terminal =
            crate::audit_executor::prepare_command_failure_terminal_audit(clock, started_input)
                .map_err(|_| ())?;
        riffdb_storage_api::CommandServiceAuditTransitionV1::failure_terminal_only(terminal)
            .map(Some)
            .map_err(|_| ())
    }
}

/// Revalidates and terminalizes one deterministic execution fault.
pub(super) fn continue_execution_fault<P>(
    port: &P,
    administration_clock: Option<&dyn crate::AdministrationClock>,
    lifecycle: &dyn CommandExecutionLifecycle,
    telemetry: &dyn CommitTelemetry,
    attempt: ExecutionFaultAttempt,
) -> CommandDriverContinuation
where
    P: ExecutionFailureTransitionPort + riffdb_storage_api::AdmissionRepository,
{
    let attempt = match attempt.authorize_after_evaluation() {
        Ok(attempt) => attempt,
        Err(error) => return post_evaluation_authorization_failure(error, lifecycle),
    };
    if let Err(error) = attempt.recheck_request_control() {
        return CommandDriverContinuation::Failed(command_attempt_failure(error, lifecycle));
    }
    let audit = match prepare_execution_failure_audit(&attempt, administration_clock) {
        Ok(audit) => audit,
        Err(()) => return internal_defect(lifecycle),
    };
    let current = match begin_execution_failure_transition(port, attempt) {
        ExecutionFailureTransitionStart::Ready(current) => current,
        ExecutionFailureTransitionStart::OutcomeReplay(outcome) => {
            return committed_replay(outcome);
        }
        ExecutionFailureTransitionStart::ExecutionFailureReplay(failure) => {
            return execution_failure_replay(failure.code());
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
    match terminal.terminalize_with_audit(audit) {
        ExecutionFailureTerminalizeResult::Terminalized(failure) => {
            execution_failure_first(failure.code())
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
                    execution_failure_first(failure.code())
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
    administration_clock: Option<&dyn crate::AdministrationClock>,
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
            continue_execution_fault(port, administration_clock, lifecycle, telemetry, *fault)
        }
        RolledBackCandidateDisposition::PolicyDenied => post_evaluation_authorization_failure(
            PostEvaluationAuthorizationError::Denied,
            lifecycle,
        ),
        RolledBackCandidateDisposition::Integrity => internal_defect(lifecycle),
    }
}

fn committed_replay(outcome: riffdb_storage_api::StoredOutcomeV1) -> CommandDriverContinuation {
    CommandDriverContinuation::Complete(CommandExecutionResult::Committed(
        CommittedOutcome::replay(outcome),
    ))
}

fn execution_failure_first(code: ExecutionFailureCode) -> CommandDriverContinuation {
    CommandDriverContinuation::Complete(CommandExecutionResult::ExecutionFailed(
        ExecutionFailedOutcome::first_terminal(code),
    ))
}

fn execution_failure_replay(code: ExecutionFailureCode) -> CommandDriverContinuation {
    CommandDriverContinuation::Complete(CommandExecutionResult::ExecutionFailed(
        ExecutionFailedOutcome::replay(code),
    ))
}

fn admission_clock_failure(error: AdmissionClockError) -> CommandExecutionError {
    CommandExecutionError {
        kind: CommandExecutionErrorKind::InternalDefect,
        detail: CommandExecutionErrorDetail::AdmissionClock(error),
    }
}

fn service_uuid_failure(error: ServiceUuidV7SourceError) -> CommandExecutionError {
    CommandExecutionError {
        kind: CommandExecutionErrorKind::InternalDefect,
        detail: CommandExecutionErrorDetail::ServiceUuid(error),
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

fn post_evaluation_authorization_failure(
    error: PostEvaluationAuthorizationError,
    lifecycle: &dyn CommandExecutionLifecycle,
) -> CommandDriverContinuation {
    match error {
        PostEvaluationAuthorizationError::Denied => CommandDriverContinuation::Failed(
            CommandExecutionError::without_detail(CommandExecutionErrorKind::AuthorizationDenied),
        ),
        PostEvaluationAuthorizationError::Unavailable => CommandDriverContinuation::Failed(
            CommandExecutionError::without_detail(CommandExecutionErrorKind::StorageUnavailable),
        ),
        PostEvaluationAuthorizationError::Integrity => internal_defect(lifecycle),
    }
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

    use riffdb_contract_compiler::compile_contract_source;
    use riffdb_types::{EntityKeyBuilder, EntityTypeId};

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

    #[derive(Default)]
    struct RecordingPipelineTelemetry {
        stages: Mutex<Vec<(CommandPipelineStage, u16, Duration)>>,
    }

    impl CommitTelemetry for RecordingPipelineTelemetry {
        fn record(&self, event: CommitTelemetryEvent) {
            if let CommitTelemetryEvent::CommandPipelineStageCompleted {
                stage,
                command_count,
                elapsed,
            } = event
            {
                self.stages.lock().expect("pipeline telemetry").push((
                    stage,
                    command_count,
                    elapsed,
                ));
            }
        }
    }

    #[test]
    fn transaction_local_serial_group_emits_one_complete_stage_pair() {
        let telemetry = RecordingPipelineTelemetry::default();
        record_transaction_local_serial_pipeline_stages(
            &telemetry,
            17,
            Duration::from_micros(230),
            Duration::from_micros(800),
        );

        assert_eq!(
            *telemetry.stages.lock().expect("pipeline telemetry"),
            vec![
                (
                    CommandPipelineStage::Evaluation,
                    17,
                    Duration::from_micros(230),
                ),
                (
                    CommandPipelineStage::ValidationEncodingStaging,
                    17,
                    Duration::from_micros(570),
                ),
            ]
        );
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
    fn grouped_exact_dependencies_reject_both_read_write_orders() {
        let entity_type = EntityTypeId::new(1).expect("entity type");
        let mut key = EntityKeyBuilder::new(entity_type);
        key.push_uuid(&[7; 16]).expect("bounded UUID key");
        let target = EntityTarget::new(entity_type, key.finish().expect("entity key"))
            .expect("entity target");
        let none = std::collections::BTreeSet::new();
        let one = std::collections::BTreeSet::from([target]);

        assert!(!exact_accesses_are_compatible(&one, &none, &none, &one));
        assert!(!exact_accesses_are_compatible(&none, &one, &one, &none));
        assert!(!exact_accesses_are_compatible(&none, &one, &none, &one));
        assert!(exact_accesses_are_compatible(&one, &none, &one, &none));
    }

    #[test]
    fn deferred_pipeline_classifies_overlapping_successors_for_private_evaluation() {
        let entity_type = EntityTypeId::new(1).expect("entity type");
        let target = |seed| {
            let mut key = EntityKeyBuilder::new(entity_type);
            key.push_uuid(&[seed; 16]).expect("bounded UUID key");
            EntityTarget::new(entity_type, key.finish().expect("entity key"))
                .expect("entity target")
        };
        let first = target(7);
        let second = target(8);
        let footprint = |reads, writes| DeferredPipelineFootprint {
            keys: std::collections::BTreeSet::new(),
            reads,
            writes,
        };
        let predecessor = footprint(
            std::collections::BTreeSet::from([first.clone()]),
            std::collections::BTreeSet::new(),
        );
        let overlapping = footprint(
            std::collections::BTreeSet::new(),
            std::collections::BTreeSet::from([first]),
        );
        let disjoint = footprint(
            std::collections::BTreeSet::new(),
            std::collections::BTreeSet::from([second]),
        );

        assert!(predecessor.requires_private_successor(&overlapping));
        assert!(!predecessor.requires_private_successor(&disjoint));
        assert!(predecessor.is_disjoint_from(&disjoint));
    }

    #[test]
    fn ticketdesk_only_proves_exact_child_append_commands() {
        let bundle = compile_contract_source(include_str!(
            "../../../examples/app-baseline/contracts/ticketdesk.riff"
        ))
        .expect("TicketDesk contract compiles");
        let proof = |name: &str| {
            bundle
                .commands()
                .iter()
                .find(|command| command.name() == name)
                .expect("named command")
                .commutative_child_append_proof(bundle.schema())
        };

        assert!(proof("CreateComment").is_some());
        assert!(proof("AttachLabel").is_some());
        assert!(proof("CreateTicket").is_none(), "aggregate root creation");
        assert!(
            proof("CloseTicketWithComment").is_none(),
            "root mutation plus child append"
        );
        assert!(
            proof("SwapMemberRoles").is_none(),
            "existing child mutation"
        );
    }

    #[test]
    fn shared_lease_group_never_admits_a_later_unproved_member() {
        assert!(shared_conflict_membership_is_compatible(
            true, false, true, true
        ));
        assert!(shared_conflict_membership_is_compatible(
            true, true, false, true
        ));
        assert!(!shared_conflict_membership_is_compatible(
            true, true, false, false
        ));
        assert!(!shared_conflict_membership_is_compatible(
            false, false, true, true
        ));
    }

    #[test]
    fn fifo_partition_keeps_maximal_compatible_prefixes_without_reordering() {
        let groups = partition_fifo_by_compatibility(vec![1, 2, 1, 3, 4, 3, 5], |group| {
            group
                .iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                == group.len()
        });
        assert_eq!(groups, vec![vec![1, 2], vec![1, 3, 4], vec![3, 5]]);

        let singletons = partition_fifo_by_compatibility(vec![1, 1, 1], |group| {
            group
                .iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                == group.len()
        });
        assert_eq!(singletons, vec![vec![1], vec![1], vec![1]]);
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

    struct UnreachableEvaluationPort;

    impl AdmissionRepository for UnreachableEvaluationPort {
        fn admit_or_resolve(
            &self,
            _request: riffdb_storage_api::AdmissionRequestV1,
        ) -> Result<riffdb_storage_api::AdmissionResultV1, StorageError> {
            Err(StorageError::new(StorageErrorKind::Unavailable, None))
        }

        fn lookup_admission(
            &self,
            _candidates: riffdb_storage_api::IdempotencyLookupCandidatesV1,
        ) -> Result<AdmissionLookupResultV1, StorageError> {
            Err(StorageError::new(StorageErrorKind::Unavailable, None))
        }
    }

    impl SnapshotReader for UnreachableEvaluationPort {
        fn read_snapshot(
            &self,
            _request: riffdb_storage_api::SnapshotRequest,
        ) -> Result<riffdb_storage_api::ReadSnapshot, StorageError> {
            Err(StorageError::new(StorageErrorKind::Unavailable, None))
        }
    }

    #[test]
    fn evaluation_pool_worker_count_is_an_explicit_bounded_input() {
        let single = CommandEvaluationPool::new(UnreachableEvaluationPort, 1)
            .expect("single-worker pool starts");
        assert_eq!(single.worker_count, 1, "explicit count is honored");
        assert_eq!(single.workers.len(), 1);

        let clamped = CommandEvaluationPool::new(UnreachableEvaluationPort, usize::MAX)
            .expect("oversized request clamps");
        assert_eq!(clamped.worker_count, MAX_PARALLEL_EVALUATION_WORKERS);

        let floored = CommandEvaluationPool::new(UnreachableEvaluationPort, 0)
            .expect("zero request floors to one worker");
        assert_eq!(floored.worker_count, 1);

        let production = CommandEvaluationPool::production_worker_count();
        assert!((1..=MAX_PARALLEL_EVALUATION_WORKERS).contains(&production));
        let available = thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
        assert!(production <= available.saturating_sub(1).max(1));
    }
}
