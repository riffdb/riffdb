//! Deterministic construction of one successful command's durable record graph.

use std::{error::Error, fmt};

use riffdb_storage_api::{
    AdmissionLookupResultV1, AdmissionRepository, AffectedEntityV1, AssignedCommandSequence,
    AtomicCommandRecordSet, CommandCandidateSequenceAssigned, CommandWriteSetPlanV1,
    CommittedBatchV1, CommittedEntityMutationV1, CommittedEntityReferenceV2, DeferredCommandEpoch,
    DeferredNonEmptyCommandBatch, DurabilityMode, DurableKeySchemaBindingV1, EmptyCommandBatch,
    EntityMutation, ExpectedEntityState, IdempotencyLookupCandidatesV1, NonEmptyCommandBatch,
    StorageError, StorageErrorKind, StorageValueError, StoredAdmissionStateV1,
    StoredCommitRecordV1, StoredDurableEventV1, StoredEntityRecordV1, StoredExecutionFailedV1,
    StoredOutcomeV1, StoredProvenanceRecordV1, StoredReadDependenciesV1, derive_event_hash_v1,
};
use riffdb_types::{EntityVersion, EventId};

use crate::{
    command_attempt::PendingCommandAttempts,
    command_index::{
        CheckedCandidateStage, CheckedCommitCandidate, PostApplyCheckedCommitCandidate,
        RetainedCheckedCommitCandidate,
    },
    outcome::CommittedOutcome,
};

/// Redacted failure for an impossible assigned-plan-to-record derivation.
#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) struct CommandRecordGraphError {
    _private: (),
}

impl CommandRecordGraphError {
    const fn internal_defect() -> Self {
        Self { _private: () }
    }
}

impl fmt::Debug for CommandRecordGraphError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CommandRecordGraphError([REDACTED])")
    }
}

impl fmt::Display for CommandRecordGraphError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("assigned command record graph could not be constructed")
    }
}

impl Error for CommandRecordGraphError {}

impl From<StorageValueError> for CommandRecordGraphError {
    fn from(_: StorageValueError) -> Self {
        Self::internal_defect()
    }
}

/// The only production input accepted by durable record construction.
struct CheckedRecordGraphInput {
    assignment: AssignedCommandSequence,
    write_plan: CommandWriteSetPlanV1,
}

impl CheckedRecordGraphInput {
    fn from_assigned_candidate<S>(
        candidate: &CheckedCommitCandidate<S>,
    ) -> Result<Self, CommandRecordGraphError>
    where
        S: CommandCandidateSequenceAssigned,
    {
        let assignment = candidate.assignment();
        let write_plan = candidate.write_plan().clone();
        if !candidate.matches_intent(write_plan.intent())
            || candidate.entry_mutations() != write_plan.index_entries()
            || candidate.affected_targets() != write_plan.affected_targets()
        {
            return Err(CommandRecordGraphError::internal_defect());
        }
        Ok(Self {
            assignment,
            write_plan,
        })
    }

    #[cfg(test)]
    fn new_for_test(
        candidate: &RetainedCheckedCommitCandidate,
        assignment: AssignedCommandSequence,
        write_plan: CommandWriteSetPlanV1,
    ) -> Result<Self, CommandRecordGraphError> {
        if !candidate.matches_intent(write_plan.intent())
            || candidate.entry_mutations() != write_plan.index_entries()
            || candidate.affected_targets() != write_plan.affected_targets()
        {
            return Err(CommandRecordGraphError::internal_defect());
        }
        Ok(Self {
            assignment,
            write_plan,
        })
    }
}

/// Closed internal failure from checked graph construction and immediate staging.
pub(super) enum CheckedCommandStageError {
    InternalDefect(CommandRecordGraphError),
    Storage(StorageError),
}

/// Closed result of submitting one semantically checked command to engine commit.
pub(super) enum CheckedCommandCommitResult {
    /// The engine returned the one exact expected durable outcome.
    Committed(Box<CommittedOutcome>),
    /// Storage proved that no commit occurred and released the retained proof.
    ProvenAbort(StorageError),
    /// Storage could not prove commit or rollback; same-key lookup is now mandatory.
    StatusUnknown(Box<UncertainCommandCommit>),
    /// A successful engine response contradicted the exact staged record graph.
    Integrity,
}

pub(super) enum CheckedCommandGroupCommitResult {
    Committed(Vec<CommittedOutcome>),
    ProvenAbort {
        cause: StorageError,
        retries: Vec<PendingCommandAttempts>,
    },
    StatusUnknown(Vec<UncertainCommandCommit>),
    Integrity,
}

/// Move-only same-attempt evidence retained after an uncertain engine commit.
pub(super) struct UncertainCommandCommit {
    cause: StorageError,
    lookup_candidates: IdempotencyLookupCandidatesV1,
    expected_outcome: StoredOutcomeV1,
    candidate: UncertainCommandCandidate,
    durability_mode: DurabilityMode,
}

#[allow(clippy::large_enum_variant)] // Avoid allocating on the normal checked commit path.
enum UncertainCommandCandidate {
    Live(RetainedCheckedCommitCandidate),
    PostApply(PostApplyCheckedCommitCandidate),
}

impl UncertainCommandCandidate {
    fn exact_intent(&self) -> &riffdb_storage_api::CommitIntent {
        match self {
            Self::Live(candidate) => candidate.exact_intent(),
            Self::PostApply(candidate) => candidate.exact_intent(),
        }
    }

    fn lookup_candidates(&self) -> &IdempotencyLookupCandidatesV1 {
        match self {
            Self::Live(candidate) => candidate.lookup_candidates(),
            Self::PostApply(candidate) => candidate.lookup_candidates(),
        }
    }

    fn matches_terminal_failure(&self, failure: &StoredExecutionFailedV1) -> bool {
        match self {
            Self::Live(candidate) => failure.pending() == candidate.exact_intent().pending(),
            Self::PostApply(candidate) => candidate.matches_terminal_failure(failure),
        }
    }

    fn into_pending_after_proven_noncommit(self) -> Result<PendingCommandAttempts, ()> {
        match self {
            Self::Live(candidate) => candidate.into_pending_after_proven_noncommit(),
            Self::PostApply(candidate) => Ok(candidate.into_pending_after_proven_noncommit()),
        }
    }
}

impl UncertainCommandCommit {
    pub(super) const fn cause(&self) -> &StorageError {
        &self.cause
    }
}

/// Closed result of one consuming same-key uncertain-commit lookup.
#[allow(clippy::large_enum_variant)] // The large noncommit proof is consumed only after uncertainty.
pub(super) enum UncertainCommandCommitResolution {
    /// The exact expected outcome committed during this invocation.
    Committed(CommittedOutcome),
    /// The same pending admission became a deterministic terminal failure.
    ExecutionFailureReplay(Box<StoredExecutionFailedV1>),
    /// The exact Pending admission proves this attempt did not commit.
    ProvenNotCommitted(ProvenNonCommitCommand),
    /// Durable state cannot yet distinguish commit from rollback.
    OutcomeUnknown {
        uncertain: Box<UncertainCommandCommit>,
        lookup_error: StorageError,
    },
    /// Durable state contradicts the retained same-attempt evidence.
    Integrity,
}

/// Opaque authority to discard the old attempt and begin a fresh evaluation.
pub(super) struct ProvenNonCommitCommand {
    candidate: UncertainCommandCandidate,
}

impl ProvenNonCommitCommand {
    pub(super) fn into_retry_state(
        self,
    ) -> Result<Box<crate::command_attempt::PendingCommandAttempts>, CommandRecordGraphError> {
        self.candidate
            .into_pending_after_proven_noncommit()
            .map(Box::new)
            .map_err(|()| CommandRecordGraphError::internal_defect())
    }
}

impl fmt::Debug for ProvenNonCommitCommand {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProvenNonCommitCommand([REDACTED])")
    }
}

/// A staged storage state that still owns the exact evaluated attempt and its
/// mutation capability. Later commit orchestration must consume this wrapper.
pub(super) struct CheckedStagedCommand<S> {
    staged: S,
    entries: Vec<CheckedStagedCommandEntry>,
    durability_mode: DurabilityMode,
}

pub(super) struct CheckedStagedCommandEntry {
    candidate: RetainedCheckedCommitCandidate,
    expected_outcome: StoredOutcomeV1,
}

struct PostApplyCheckedCommandEntry {
    candidate: PostApplyCheckedCommitCandidate,
    expected_outcome: StoredOutcomeV1,
}

impl CheckedStagedCommandEntry {
    pub(super) fn into_retry(self) -> Result<PendingCommandAttempts, ()> {
        self.candidate.into_pending_after_group_rollback()
    }

    fn into_post_apply(self) -> Result<PostApplyCheckedCommandEntry, ()> {
        Ok(PostApplyCheckedCommandEntry {
            candidate: self.candidate.into_post_apply_evidence()?,
            expected_outcome: self.expected_outcome,
        })
    }
}

impl<S> CheckedStagedCommand<S>
where
    S: NonEmptyCommandBatch,
{
    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(super) fn audited_starts(
        &self,
    ) -> impl Iterator<Item = Option<&dyn crate::AdministrationAuditInputView>> {
        self.entries.iter().map(|entry| {
            entry.candidate.audited_lifecycle().map(|lifecycle| {
                let _proof = &lifecycle.release;
                lifecycle.started.as_ref() as &dyn crate::AdministrationAuditInputView
            })
        })
    }

    pub(super) fn terminal_links(
        &self,
    ) -> impl Iterator<Item = riffdb_types::ServiceAuditLinkV1> + '_ {
        self.entries
            .iter()
            .map(|entry| riffdb_types::ServiceAuditLinkV1::Command {
                commit_sequence: entry.expected_outcome.commit_sequence(),
                provenance_id: entry.expected_outcome.provenance_id(),
            })
    }

    pub(super) fn requires_fused_starts(&self) -> impl Iterator<Item = bool> + '_ {
        self.entries.iter().map(|entry| {
            matches!(
                entry.candidate.exact_intent().admission_expectation(),
                riffdb_storage_api::CommandAdmissionExpectationV1::Vacant(_)
            )
        })
    }

    pub(super) fn requires_fused_start(&self) -> bool {
        self.requires_fused_starts().next().unwrap_or(false)
    }

    pub(super) fn into_storage_and_entries(
        self,
    ) -> (S, Vec<CheckedStagedCommandEntry>, DurabilityMode) {
        (self.staged, self.entries, self.durability_mode)
    }

    pub(super) fn from_appended(
        staged: S,
        mut entries: Vec<CheckedStagedCommandEntry>,
        entry: CheckedStagedCommandEntry,
        durability_mode: DurabilityMode,
    ) -> Self {
        entries.push(entry);
        Self {
            staged,
            entries,
            durability_mode,
        }
    }

    pub(super) fn rollback_into_retries(self) -> Result<Vec<PendingCommandAttempts>, ()> {
        let Self {
            staged,
            entries,
            durability_mode: _,
        } = self;
        staged.rollback();
        entries
            .into_iter()
            .map(|entry| entry.candidate.into_pending_after_group_rollback())
            .collect()
    }

    pub(super) fn audited_start(&self) -> Option<&dyn crate::AdministrationAuditInputView> {
        self.entries
            .first()?
            .candidate
            .audited_lifecycle()
            .map(|lifecycle| {
                let _proof = &lifecycle.release;
                lifecycle.started.as_ref()
            })
    }

    pub(super) fn terminal_link(&self) -> riffdb_types::ServiceAuditLinkV1 {
        let expected_outcome = &self
            .entries
            .first()
            .expect("checked staged command is nonempty")
            .expected_outcome;
        riffdb_types::ServiceAuditLinkV1::Command {
            commit_sequence: expected_outcome.commit_sequence(),
            provenance_id: expected_outcome.provenance_id(),
        }
    }

    /// Commits with the exact mode already embedded in the staged graph while
    /// retaining the attempt capability until the storage call returns.
    pub(super) fn commit(
        self,
        audit: Option<riffdb_storage_api::CommandServiceAuditTransitionV1>,
    ) -> CheckedCommandCommitResult {
        let Self {
            staged,
            entries,
            durability_mode,
        } = self;
        if entries.len() != 1 {
            drop(staged);
            drop(entries);
            return CheckedCommandCommitResult::Integrity;
        }
        let entry = entries
            .into_iter()
            .next()
            .expect("checked staged command is nonempty");
        let result = match audit {
            Some(audit) => staged
                .commit_with_service_audit_transitions(durability_mode, vec![audit])
                .map(|audited| audited.into_parts().0),
            None => staged.commit(durability_mode),
        };
        finish_checked_commit(
            entry.candidate,
            entry.expected_outcome,
            durability_mode,
            result,
        )
    }

    pub(super) fn commit_group(
        self,
        audits: Option<Vec<riffdb_storage_api::CommandServiceAuditTransitionV1>>,
    ) -> CheckedCommandGroupCommitResult {
        let Self {
            staged,
            entries,
            durability_mode,
        } = self;
        if entries.len() < 2
            || audits
                .as_ref()
                .is_some_and(|audits| entries.len() != audits.len())
        {
            drop(staged);
            drop(entries);
            return CheckedCommandGroupCommitResult::Integrity;
        }
        let committed = match audits {
            Some(audits) => staged
                .commit_with_service_audit_transitions(durability_mode, audits)
                .map(|audited| audited.into_parts().0),
            None => staged.commit(durability_mode),
        };
        finish_checked_group_commit(entries, durability_mode, committed)
    }
}

impl<S> CheckedStagedCommand<S>
where
    S: NonEmptyCommandBatch + DeferredNonEmptyCommandBatch,
{
    pub(super) fn apply_group_deferred(
        self,
        audits: Vec<riffdb_storage_api::CommandServiceAuditTransitionV1>,
    ) -> CheckedCommandGroupApplyResult<S::Epoch> {
        let Self {
            staged,
            entries,
            durability_mode,
        } = self;
        // Standard-profile audited commands, including idle singletons, use
        // the deferred storage boundary. A singleton selected after an
        // unpublished FIFO predecessor extends that same writer-private
        // frontier and may share a later journal fence.
        if entries.is_empty() || entries.len() != audits.len() {
            drop(staged);
            drop(entries);
            return CheckedCommandGroupApplyResult::Failed(
                CheckedCommandGroupCommitResult::Integrity,
            );
        }
        match staged.apply_unpublished_with_service_audit_transitions(durability_mode, audits) {
            Ok(epoch) => {
                let entries = entries
                    .into_iter()
                    .map(CheckedStagedCommandEntry::into_post_apply)
                    .collect::<Result<Vec<_>, _>>();
                match entries {
                    Ok(entries) => CheckedCommandGroupApplyResult::Applied {
                        epoch,
                        batch: CheckedDeferredCommandBatch {
                            entries,
                            durability_mode,
                        },
                    },
                    Err(()) => {
                        drop(epoch);
                        CheckedCommandGroupApplyResult::Failed(
                            CheckedCommandGroupCommitResult::Integrity,
                        )
                    }
                }
            }
            Err(error) => CheckedCommandGroupApplyResult::Failed(finish_checked_group_commit(
                entries,
                durability_mode,
                Err(StorageError::new(
                    StorageErrorKind::CommitStatusUnknown,
                    error.incident_id(),
                )),
            )),
        }
    }
}

pub(super) struct CheckedDeferredCommandBatch {
    entries: Vec<PostApplyCheckedCommandEntry>,
    durability_mode: DurabilityMode,
}

pub(super) enum CheckedCommandGroupApplyResult<E> {
    Applied {
        epoch: E,
        batch: CheckedDeferredCommandBatch,
    },
    Failed(CheckedCommandGroupCommitResult),
}

pub(super) trait CheckedCommandGroupFence {
    fn requires_pipeline_drain(&self) -> bool;
    fn try_wait(&mut self) -> Option<CheckedCommandGroupCommitResult>;
    fn wait(self: Box<Self>) -> CheckedCommandGroupCommitResult;
}

struct TypedCheckedCommandGroupFence<F> {
    fence: Option<F>,
    batch: Option<CheckedDeferredCommandBatch>,
}

impl<F> CheckedCommandGroupFence for TypedCheckedCommandGroupFence<F>
where
    F: riffdb_storage_api::DeferredCommandFence,
{
    fn requires_pipeline_drain(&self) -> bool {
        self.fence
            .as_ref()
            .is_some_and(riffdb_storage_api::DeferredCommandFence::requires_pipeline_drain)
    }

    fn try_wait(&mut self) -> Option<CheckedCommandGroupCommitResult> {
        let fenced = match self.fence.as_mut()?.try_wait() {
            Ok(Some(mut batches)) => {
                if batches.len() != 1 {
                    Err(StorageError::new(
                        StorageErrorKind::InvariantViolation,
                        None,
                    ))
                } else {
                    Ok(batches.remove(0).into_parts().0)
                }
            }
            Ok(None) => return None,
            Err(error) => Err(error),
        }
        .map_err(|error| {
            StorageError::new(StorageErrorKind::CommitStatusUnknown, error.incident_id())
        });
        self.fence.take();
        let batch = self.batch.take()?;
        Some(finish_post_apply_group_commit(
            batch.entries,
            batch.durability_mode,
            fenced,
        ))
    }

    fn wait(self: Box<Self>) -> CheckedCommandGroupCommitResult {
        let Self { fence, batch } = *self;
        let (Some(fence), Some(batch)) = (fence, batch) else {
            return CheckedCommandGroupCommitResult::Integrity;
        };
        let fenced = fence
            .wait()
            .and_then(|mut batches| {
                if batches.len() != 1 {
                    return Err(StorageError::new(
                        StorageErrorKind::InvariantViolation,
                        None,
                    ));
                }
                Ok(batches.remove(0).into_parts().0)
            })
            .map_err(|error| {
                StorageError::new(StorageErrorKind::CommitStatusUnknown, error.incident_id())
            });
        finish_post_apply_group_commit(batch.entries, batch.durability_mode, fenced)
    }
}

pub(super) fn seal_checked_deferred_group<E>(
    epoch: E,
    batch: CheckedDeferredCommandBatch,
) -> Result<Box<dyn CheckedCommandGroupFence>, CheckedCommandGroupCommitResult>
where
    E: DeferredCommandEpoch,
    E::Fence: 'static,
{
    match epoch.seal() {
        Ok(fence) => Ok(Box::new(TypedCheckedCommandGroupFence {
            fence: Some(fence),
            batch: Some(batch),
        })),
        Err(error) => Err(finish_post_apply_group_commit(
            batch.entries,
            batch.durability_mode,
            Err(StorageError::new(
                StorageErrorKind::CommitStatusUnknown,
                error.incident_id(),
            )),
        )),
    }
}

fn finish_post_apply_group_commit(
    entries: Vec<PostApplyCheckedCommandEntry>,
    durability_mode: DurabilityMode,
    committed: Result<CommittedBatchV1, StorageError>,
) -> CheckedCommandGroupCommitResult {
    match committed {
        Ok(batch)
            if batch.durability_mode() == durability_mode
                && batch.outcomes().len() == entries.len()
                && batch
                    .outcomes()
                    .iter()
                    .zip(&entries)
                    .all(|(outcome, entry)| {
                        outcome == &entry.expected_outcome
                            && entry.candidate.matches_terminal_outcome(outcome)
                    }) =>
        {
            CheckedCommandGroupCommitResult::Committed(
                entries
                    .into_iter()
                    .map(|entry| CommittedOutcome::first_commit(entry.expected_outcome))
                    .collect(),
            )
        }
        Ok(_) => CheckedCommandGroupCommitResult::Integrity,
        Err(cause) => CheckedCommandGroupCommitResult::StatusUnknown(
            entries
                .into_iter()
                .map(|entry| UncertainCommandCommit {
                    cause: cause.clone(),
                    lookup_candidates: entry.candidate.lookup_candidates().clone(),
                    expected_outcome: entry.expected_outcome,
                    candidate: UncertainCommandCandidate::PostApply(entry.candidate),
                    durability_mode,
                })
                .collect(),
        ),
    }
}

fn finish_checked_group_commit(
    entries: Vec<CheckedStagedCommandEntry>,
    durability_mode: DurabilityMode,
    committed: Result<CommittedBatchV1, StorageError>,
) -> CheckedCommandGroupCommitResult {
    match committed {
        Ok(batch)
            if batch.durability_mode() == durability_mode
                && batch.outcomes().len() == entries.len()
                && batch
                    .outcomes()
                    .iter()
                    .zip(&entries)
                    .all(|(outcome, entry)| outcome == &entry.expected_outcome) =>
        {
            let outcomes = entries
                .into_iter()
                .map(|entry| {
                    drop(entry.candidate);
                    CommittedOutcome::first_commit(entry.expected_outcome)
                })
                .collect();
            CheckedCommandGroupCommitResult::Committed(outcomes)
        }
        Ok(_) => {
            drop(entries);
            CheckedCommandGroupCommitResult::Integrity
        }
        Err(cause) if cause.kind() == StorageErrorKind::CommitStatusUnknown => {
            let uncertain = entries
                .into_iter()
                .map(|entry| UncertainCommandCommit {
                    cause: cause.clone(),
                    lookup_candidates: entry.candidate.lookup_candidates().clone(),
                    expected_outcome: entry.expected_outcome,
                    candidate: UncertainCommandCandidate::Live(entry.candidate),
                    durability_mode,
                })
                .collect();
            CheckedCommandGroupCommitResult::StatusUnknown(uncertain)
        }
        Err(cause) => {
            let retries = entries
                .into_iter()
                .map(|entry| entry.candidate.into_pending_after_group_rollback())
                .collect::<Result<Vec<_>, _>>();
            match retries {
                Ok(retries) => CheckedCommandGroupCommitResult::ProvenAbort { cause, retries },
                Err(()) => CheckedCommandGroupCommitResult::Integrity,
            }
        }
    }
}

impl<S> CheckedStagedCommand<S>
where
    S: NonEmptyCommandBatch + riffdb_storage_api::TransactionLocalCommandBatch,
{
    pub(super) fn read_transaction_local_snapshot(
        &self,
        request: riffdb_storage_api::SnapshotRequest,
    ) -> Result<riffdb_storage_api::ReadSnapshot, StorageError> {
        self.staged.read_transaction_local_snapshot(request)
    }
}

fn finish_checked_commit(
    candidate: RetainedCheckedCommitCandidate,
    expected_outcome: StoredOutcomeV1,
    durability_mode: DurabilityMode,
    result: Result<CommittedBatchV1, StorageError>,
) -> CheckedCommandCommitResult {
    match result {
        Ok(batch)
            if batch.durability_mode() == durability_mode
                && batch.outcomes() == std::slice::from_ref(&expected_outcome) =>
        {
            drop(candidate);
            CheckedCommandCommitResult::Committed(Box::new(CommittedOutcome::first_commit(
                expected_outcome,
            )))
        }
        Ok(_) => {
            drop(candidate);
            CheckedCommandCommitResult::Integrity
        }
        Err(cause) if cause.kind() == StorageErrorKind::CommitStatusUnknown => {
            let lookup_candidates = candidate.lookup_candidates().clone();
            CheckedCommandCommitResult::StatusUnknown(Box::new(UncertainCommandCommit {
                cause,
                lookup_candidates,
                expected_outcome,
                candidate: UncertainCommandCandidate::Live(candidate),
                durability_mode,
            }))
        }
        Err(error) => {
            drop(candidate);
            CheckedCommandCommitResult::ProvenAbort(error)
        }
    }
}

/// Performs exactly one authoritative same-key lookup after further writes are fenced.
pub(super) fn resolve_uncertain_command_commit(
    repository: &dyn AdmissionRepository,
    uncertain: Box<UncertainCommandCommit>,
) -> UncertainCommandCommitResolution {
    if uncertain.cause.kind() != StorageErrorKind::CommitStatusUnknown
        || uncertain.expected_outcome.durability_mode() != uncertain.durability_mode
        || uncertain.expected_outcome.plan()
            != uncertain.candidate.exact_intent().evaluated().plan()
        || uncertain.expected_outcome.provenance_id()
            != uncertain.candidate.exact_intent().provenance_id()
        || uncertain.lookup_candidates != *uncertain.candidate.lookup_candidates()
    {
        return UncertainCommandCommitResolution::Integrity;
    }
    let durable = match repository.lookup_admission(uncertain.lookup_candidates.clone()) {
        Ok(durable) => durable,
        Err(lookup_error) => {
            return UncertainCommandCommitResolution::OutcomeUnknown {
                uncertain,
                lookup_error,
            };
        }
    };
    match durable {
        AdmissionLookupResultV1::NotFound
            if matches!(
                uncertain.candidate.exact_intent().admission_expectation(),
                riffdb_storage_api::CommandAdmissionExpectationV1::Vacant(_)
            ) =>
        {
            let UncertainCommandCommit { candidate, .. } = *uncertain;
            UncertainCommandCommitResolution::ProvenNotCommitted(ProvenNonCommitCommand {
                candidate,
            })
        }
        AdmissionLookupResultV1::NotFound => UncertainCommandCommitResolution::Integrity,
        AdmissionLookupResultV1::MultipleMatches => UncertainCommandCommitResolution::Integrity,
        AdmissionLookupResultV1::Found(state) => match *state {
            StoredAdmissionStateV1::StoredOutcome(outcome)
                if outcome == uncertain.expected_outcome =>
            {
                UncertainCommandCommitResolution::Committed(CommittedOutcome::first_commit(outcome))
            }
            StoredAdmissionStateV1::ExecutionFailed(failure)
                if uncertain.candidate.matches_terminal_failure(&failure) =>
            {
                UncertainCommandCommitResolution::ExecutionFailureReplay(Box::new(failure))
            }
            StoredAdmissionStateV1::Pending(pending)
                if &pending == uncertain.candidate.exact_intent().pending() =>
            {
                let UncertainCommandCommit { candidate, .. } = *uncertain;
                UncertainCommandCommitResolution::ProvenNotCommitted(ProvenNonCommitCommand {
                    candidate,
                })
            }
            StoredAdmissionStateV1::Pending(_)
            | StoredAdmissionStateV1::StoredOutcome(_)
            | StoredAdmissionStateV1::ExecutionFailed(_) => {
                UncertainCommandCommitResolution::Integrity
            }
        },
    }
}

impl fmt::Debug for CheckedCommandStageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CheckedCommandStageError([REDACTED])")
    }
}

impl fmt::Debug for CheckedCommandCommitResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Committed(_) => "CheckedCommandCommitResult::Committed([REDACTED])",
            Self::ProvenAbort(_) => "CheckedCommandCommitResult::ProvenAbort([REDACTED])",
            Self::StatusUnknown(_) => "CheckedCommandCommitResult::StatusUnknown([REDACTED])",
            Self::Integrity => "CheckedCommandCommitResult::Integrity",
        })
    }
}

impl fmt::Debug for UncertainCommandCommit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("UncertainCommandCommit([REDACTED])")
    }
}

impl fmt::Debug for UncertainCommandCommitResolution {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Committed(_) => "UncertainCommandCommitResolution::Committed([REDACTED])",
            Self::ExecutionFailureReplay(_) => {
                "UncertainCommandCommitResolution::ExecutionFailureReplay([REDACTED])"
            }
            Self::ProvenNotCommitted(_) => {
                "UncertainCommandCommitResolution::ProvenNotCommitted([REDACTED])"
            }
            Self::OutcomeUnknown { .. } => {
                "UncertainCommandCommitResolution::OutcomeUnknown([REDACTED])"
            }
            Self::Integrity => "UncertainCommandCommitResolution::Integrity",
        })
    }
}

/// Consumes the checked semantic candidate only after sequence assignment,
/// derives its graph, and immediately transfers that graph to storage staging.
pub(super) fn build_and_stage_checked_candidate<S>(
    candidate: CheckedCommitCandidate<S>,
    durability_mode: DurabilityMode,
) -> Result<CheckedStagedCommand<S::Staged>, CheckedCommandStageError>
where
    S: CommandCandidateSequenceAssigned,
    S::Prior: EmptyCommandBatch,
{
    let (staged, entry) = stage_checked_candidate(candidate, durability_mode)?;
    Ok(CheckedStagedCommand {
        staged,
        entries: vec![entry],
        durability_mode,
    })
}

pub(super) fn build_and_stage_checked_candidate_on_prior<S>(
    candidate: CheckedCommitCandidate<S>,
    durability_mode: DurabilityMode,
) -> Result<(S::Staged, CheckedStagedCommandEntry), CheckedCommandStageError>
where
    S: CommandCandidateSequenceAssigned,
    S::Prior: NonEmptyCommandBatch,
{
    stage_checked_candidate(candidate, durability_mode)
}

fn stage_checked_candidate<S>(
    candidate: CheckedCommitCandidate<S>,
    durability_mode: DurabilityMode,
) -> Result<(S::Staged, CheckedStagedCommandEntry), CheckedCommandStageError>
where
    S: CommandCandidateSequenceAssigned,
{
    let input = match CheckedRecordGraphInput::from_assigned_candidate(&candidate) {
        Ok(input) => input,
        Err(error) => {
            drop(candidate);
            return Err(CheckedCommandStageError::InternalDefect(error));
        }
    };
    let records = match build_atomic_command_record_set(&input, durability_mode) {
        Ok(records) => records,
        Err(error) => {
            drop(candidate);
            return Err(CheckedCommandStageError::InternalDefect(error));
        }
    };
    let expected_outcome = records.stored_outcome().clone();
    let (staged, candidate) = match candidate.stage(records) {
        CheckedCandidateStage::Staged { storage, evidence } => (storage, evidence),
        CheckedCandidateStage::StorageFailure(error)
            if error.kind() == StorageErrorKind::CommitStatusUnknown =>
        {
            return Err(CheckedCommandStageError::InternalDefect(
                CommandRecordGraphError::internal_defect(),
            ));
        }
        CheckedCandidateStage::StorageFailure(error) => {
            return Err(CheckedCommandStageError::Storage(error));
        }
        CheckedCandidateStage::Integrity => {
            return Err(CheckedCommandStageError::InternalDefect(
                CommandRecordGraphError::internal_defect(),
            ));
        }
    };
    Ok((
        staged,
        CheckedStagedCommandEntry {
            candidate,
            expected_outcome,
        },
    ))
}

/// Derives the complete successful-command graph from one reserved write plan.
///
/// The sequence and durability mode are supplied explicitly by later
/// orchestration. Every remaining value is copied or mechanically derived from
/// the immutable intent and write plan. The storage-owned final constructor
/// rechecks all reciprocal links, canonical ordering, and aggregate bounds.
fn build_atomic_command_record_set(
    input: &CheckedRecordGraphInput,
    durability_mode: DurabilityMode,
) -> Result<AtomicCommandRecordSet, CommandRecordGraphError> {
    let assignment = input.assignment;
    let write_plan = input.write_plan.clone();
    let intent = write_plan.intent();
    let evaluated = intent.evaluated();
    let pending = intent.pending();
    let sequence = assignment.assigned();
    let schema_binding = DurableKeySchemaBindingV1::from_plan(evaluated.plan());

    let entities = evaluated
        .mutations()
        .iter()
        .map(|mutation| committed_entity(mutation, &schema_binding))
        .collect::<Result<Vec<_>, _>>()?;
    let events = evaluated
        .event_intents()
        .iter()
        .enumerate()
        .map(|(ordinal, event)| {
            let ordinal =
                u32::try_from(ordinal).map_err(|_| CommandRecordGraphError::internal_defect())?;
            let event_id = EventId::new(sequence, ordinal);
            let event_hash =
                derive_event_hash_v1(event_id, event.event_type_id(), event.payload())?;
            StoredDurableEventV1::new(
                event_id,
                event.event_type_id(),
                event.payload().clone(),
                event_hash,
            )
            .map_err(CommandRecordGraphError::from)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let event_ids = events
        .iter()
        .map(StoredDurableEventV1::event_id)
        .collect::<Vec<_>>();

    let stored_outcome = StoredOutcomeV1::new_with_causation(
        pending.identity().clone(),
        sequence,
        pending.admission_request_id(),
        pending.plan().clone(),
        pending.canonical_input_hash(),
        pending.actor().clone(),
        pending.logical_time(),
        pending.partition_key().clone(),
        intent.partition_hash(),
        intent.conflict_hashes().to_vec(),
        evaluated.outcome().clone(),
        pending.provenance_claims().clone(),
        intent.provenance_id(),
        durability_mode,
        pending.causation(),
    )?;
    let affected_entities = entities
        .iter()
        .map(|mutation| AffectedEntityV1::from_record(mutation.post_image()))
        .collect();
    let provenance = StoredProvenanceRecordV1::new_with_causation(
        intent.provenance_id(),
        sequence,
        pending.identity().clone(),
        pending.admission_request_id(),
        pending.plan().clone(),
        pending.canonical_input_hash(),
        pending.actor().clone(),
        pending.logical_time(),
        intent.partition_hash(),
        intent.conflict_hashes().to_vec(),
        evaluated.outcome().outcome_id(),
        affected_entities,
        event_ids.clone(),
        pending.provenance_claims().clone(),
        pending.causation(),
    )?;
    let entity_references = entities
        .iter()
        .map(CommittedEntityReferenceV2::from_mutation)
        .collect::<Result<Vec<_>, _>>()?;
    let commit = StoredCommitRecordV1::new(
        sequence,
        pending.admission_request_id(),
        pending.plan().clone(),
        pending.canonical_input_hash(),
        pending.actor().clone(),
        pending.logical_time(),
        intent.partition_hash(),
        intent.conflict_hashes().to_vec(),
        StoredReadDependenciesV1::from_live(evaluated.read_dependencies())?,
        entity_references,
        events,
        evaluated.outcome().clone(),
        intent.provenance_id(),
        event_ids,
        durability_mode,
    )?;
    AtomicCommandRecordSet::new(
        assignment,
        entities,
        write_plan,
        stored_outcome,
        provenance,
        commit,
    )
    .map_err(CommandRecordGraphError::from)
}

fn committed_entity(
    mutation: &EntityMutation,
    schema_binding: &DurableKeySchemaBindingV1,
) -> Result<CommittedEntityMutationV1, CommandRecordGraphError> {
    let (expected, entity_version) = match mutation {
        EntityMutation::Create(_) => (ExpectedEntityState::Absent, EntityVersion::first()),
        EntityMutation::Replace {
            expected_version, ..
        } => (
            ExpectedEntityState::Present(*expected_version),
            expected_version
                .checked_next()
                .ok_or_else(CommandRecordGraphError::internal_defect)?,
        ),
    };
    let post_image = mutation.post_image();
    let stored = StoredEntityRecordV1::from_checked_post_image(
        post_image,
        entity_version,
        schema_binding.clone(),
    )?;
    CommittedEntityMutationV1::new(expected, stored).map_err(CommandRecordGraphError::from)
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use riffdb_storage_api::{
        AffectedEpochCurrentState, AffectedIndexEpochTargets, CommandWriteSetPlanV1,
        CurrentIndexGenerationObservation, DeclaredOutcome, EncodedWriteSetUpperBoundResultV1,
        EntityObservation, EntityPostImage, EvaluatedCommand, EvaluationBudget, EventIntent,
        ExecutablePlanRef, IdempotencyIdentity, IdempotencyKeyDigest, IndexEntryMutationV1,
        IndexEpochAdvanceV1, IndexEpochPosition, PartitionIndexTarget, PreEvaluationCommitContext,
        ReadSnapshot, SnapshotRequest, StoredAdmittedProvenanceClaimsV1, StoredCommandCausationV1,
        StoredIndexEntryV2, StoredPendingAdmissionV1, command_write_set_upper_bound_v1,
        encode_atomic_command_record_set_v1,
    };
    use riffdb_types::{
        ActorId, ActorKind, AdmittedActorContext, AggregateTypeId, CanonicalInputHash,
        CanonicalRecord, CanonicalValue, CommandId, CommitSequence, ConflictKeyHash,
        ContractBundleHash, ContractLineage, ContractVersion, DatabaseId, DigestKeyId,
        EntityKeyBuilder, EntityTypeId, EntityVersion, Environment, EventTypeId,
        ExecutionFailureCode, FieldId, IndexEntryKeyBuilder, IndexEpoch, IndexId, LogicalTime,
        OutcomeId, PartitionKeyBuilder, PlanHash, ProvenanceId, RequestId, SourceCommit,
        SourceRepository, TenantId, TenantScope, Timestamp,
    };

    use super::*;

    struct RecordGraphFixture {
        assignment: AssignedCommandSequence,
        write_plan: CommandWriteSetPlanV1,
        claims: StoredAdmittedProvenanceClaimsV1,
    }

    fn build_record_graph_for_test(
        assignment: AssignedCommandSequence,
        write_plan: CommandWriteSetPlanV1,
        durability_mode: DurabilityMode,
    ) -> Result<AtomicCommandRecordSet, CommandRecordGraphError> {
        let candidate = RetainedCheckedCommitCandidate::for_record_graph_test(
            write_plan.intent().clone(),
            write_plan.index_entries().to_vec(),
            write_plan.affected_targets().clone(),
        );
        let input = CheckedRecordGraphInput::new_for_test(&candidate, assignment, write_plan)?;
        build_atomic_command_record_set(&input, durability_mode)
    }

    fn uuid_bytes(fill: u8) -> [u8; 16] {
        let mut bytes = [fill; 16];
        bytes[6] = 0x70 | (fill & 0x0f);
        bytes[8] = 0x80 | (fill & 0x3f);
        bytes
    }

    fn record(field: u32, value: i64) -> CanonicalRecord {
        CanonicalRecord::new(vec![(
            FieldId::new(field).expect("field ID"),
            CanonicalValue::I64(value),
        )])
        .expect("canonical record")
    }

    fn plan() -> ExecutablePlanRef {
        ExecutablePlanRef::new(
            ContractLineage::new("record-graph").expect("lineage"),
            ContractVersion::new(7).expect("contract version"),
            ContractBundleHash::from_bytes([0x21; 32]),
            CommandId::new(3).expect("command ID"),
            PlanHash::from_bytes([0x22; 32]),
        )
    }

    fn target(id: u64) -> riffdb_storage_api::EntityTarget {
        let entity_type = EntityTypeId::new(5).expect("entity type");
        let mut key = EntityKeyBuilder::new(entity_type);
        key.push_u64(id).expect("entity key component");
        riffdb_storage_api::EntityTarget::new(entity_type, key.finish().expect("entity key"))
            .expect("entity target")
    }

    fn fixture_with_causation(
        observations: Vec<EntityObservation>,
        mutations: Vec<EntityMutation>,
        events: Vec<EventIntent>,
        include_index_change: bool,
        causation: Option<StoredCommandCausationV1>,
    ) -> RecordGraphFixture {
        let plan = plan();
        let targets = observations
            .iter()
            .map(|observation| observation.target().clone())
            .collect::<Vec<_>>();
        let request = SnapshotRequest::new(plan.clone(), targets, Vec::new(), Vec::new())
            .expect("snapshot request");
        let snapshot = ReadSnapshot::new(&request, None, observations, Vec::new(), Vec::new())
            .expect("snapshot");
        let outcome = DeclaredOutcome::new(OutcomeId::new(9).expect("outcome ID"), record(11, 91))
            .expect("outcome");
        let evaluated = EvaluatedCommand::new(
            &snapshot,
            mutations,
            events,
            outcome,
            EvaluationBudget::v1(),
        )
        .expect("evaluated command");

        let tenant_scope = TenantScope::Tenant(TenantId::new("tenant-a").expect("tenant"));
        let principal = ActorId::new("principal-a").expect("principal");
        let actor = AdmittedActorContext::new(
            principal.clone(),
            ActorKind::Service,
            tenant_scope.clone(),
            None,
        );
        let identity = IdempotencyIdentity::new(
            DatabaseId::from_bytes(uuid_bytes(0x31)).expect("database ID"),
            Environment::new("test").expect("environment"),
            tenant_scope,
            principal,
            plan.contract_lineage().clone(),
            plan.command_id(),
            IdempotencyKeyDigest::from_hmac_bytes(
                DigestKeyId::new(2).expect("digest key ID"),
                [0x32; 32],
            ),
        );
        let claims = StoredAdmittedProvenanceClaimsV1::new(
            Some(SourceRepository::new("acme/budget").expect("source repository")),
            Some(SourceCommit::new("0123456789abcdef").expect("source commit")),
            None,
            None,
        )
        .expect("provenance claims");
        let mut partition =
            PartitionKeyBuilder::new(AggregateTypeId::new(4).expect("aggregate type"));
        partition.push_u64(17).expect("partition component");
        let partition = partition.finish().expect("partition key");
        let mut pending = StoredPendingAdmissionV1::new(
            identity,
            CanonicalInputHash::from_bytes([0x33; 32]),
            RequestId::from_bytes(uuid_bytes(0x34)).expect("request ID"),
            plan,
            LogicalTime::new(Timestamp::new(1_234, 567).expect("logical timestamp")),
            actor,
            partition.clone(),
            claims.clone(),
        )
        .expect("pending admission");
        if let Some(causation) = causation {
            pending = pending
                .with_causation(causation)
                .expect("causal pending admission");
        }
        let context = PreEvaluationCommitContext::new(
            pending,
            riffdb_types::hash_partition_key(partition.as_bytes()),
            vec![ConflictKeyHash::from_bytes([0x35; 32])],
        )
        .expect("pre-evaluation context");
        let intent = riffdb_storage_api::CommitIntent::new(
            context,
            evaluated,
            ProvenanceId::from_bytes(uuid_bytes(0x36)).expect("provenance ID"),
        )
        .expect("commit intent");

        let (index_entries, affected_targets, affected_current, index_epochs) =
            if include_index_change {
                let entity_target = intent
                    .evaluated()
                    .mutations()
                    .first()
                    .expect("indexed fixture mutation")
                    .target();
                let index_id = IndexId::new(6).expect("index ID");
                let mut key = IndexEntryKeyBuilder::new(index_id);
                key.push_i64(41).expect("index component");
                let entry_key = key
                    .finish(entity_target.key().clone())
                    .expect("index entry key");
                let entry = StoredIndexEntryV2::new(
                    entry_key,
                    DurableKeySchemaBindingV1::from_plan(intent.evaluated().plan()),
                    CanonicalRecord::new(Vec::new()).expect("empty covered values"),
                    intent.pending().partition_key().clone(),
                )
                .expect("index entry");
                let index_entries = vec![IndexEntryMutationV1::Put(entry)];
                let affected_target =
                    PartitionIndexTarget::new(intent.pending().partition_key().clone(), index_id);
                let affected_targets =
                    AffectedIndexEpochTargets::new(vec![affected_target.clone()])
                        .expect("affected targets");
                let affected_current = AffectedEpochCurrentState::new(
                    &affected_targets,
                    vec![CurrentIndexGenerationObservation::new(
                        affected_target.clone(),
                        IndexEpochPosition::BeforeFirst,
                    )],
                )
                .expect("affected current state");
                let index_epochs = vec![
                    IndexEpochAdvanceV1::new(
                        affected_target,
                        DurableKeySchemaBindingV1::from_plan(intent.evaluated().plan()),
                        IndexEpochPosition::BeforeFirst,
                    )
                    .expect("epoch advance"),
                ];
                (
                    index_entries,
                    affected_targets,
                    affected_current,
                    index_epochs,
                )
            } else {
                let affected_targets =
                    AffectedIndexEpochTargets::new(Vec::new()).expect("empty affected targets");
                let affected_current =
                    AffectedEpochCurrentState::new(&affected_targets, Vec::new())
                        .expect("empty affected current state");
                (Vec::new(), affected_targets, affected_current, Vec::new())
            };
        let encoded_upper_bound =
            match command_write_set_upper_bound_v1(&intent, &index_entries, &index_epochs)
                .expect("encoded upper bound")
            {
                EncodedWriteSetUpperBoundResultV1::Fits(bound) => bound,
                EncodedWriteSetUpperBoundResultV1::ExceedsAcceptedAggregateCap(_) => {
                    panic!("fixture write set must fit the accepted aggregate cap")
                }
            };
        let write_plan = CommandWriteSetPlanV1::new(
            &intent,
            affected_targets,
            affected_current,
            index_entries,
            index_epochs,
            encoded_upper_bound,
        )
        .expect("write plan");

        RecordGraphFixture {
            assignment: AssignedCommandSequence::from_assigned(
                CommitSequence::new(23).expect("commit sequence"),
            ),
            write_plan,
            claims,
        }
    }

    fn fixture(
        observations: Vec<EntityObservation>,
        mutations: Vec<EntityMutation>,
        events: Vec<EventIntent>,
        include_index_change: bool,
    ) -> RecordGraphFixture {
        fixture_with_causation(observations, mutations, events, include_index_change, None)
    }

    #[test]
    fn causal_command_charge_matches_its_complete_atomic_record_graph() {
        let causation = StoredCommandCausationV1::new(
            EventId::new(CommitSequence::new(4).expect("causing sequence"), 0),
            RequestId::from_bytes(uuid_bytes(0x40)).expect("root request ID"),
        );
        let fixture =
            fixture_with_causation(Vec::new(), Vec::new(), Vec::new(), false, Some(causation));

        let records = build_record_graph_for_test(
            fixture.assignment,
            fixture.write_plan,
            DurabilityMode::Group,
        )
        .expect("causal record graph");

        assert_eq!(records.stored_outcome().causation(), Some(causation));
        assert_eq!(records.provenance().causation(), Some(causation));
    }

    #[test]
    fn assigned_graph_derives_every_reciprocal_record_and_canonical_event_hash() {
        let entity_target = target(17);
        let post_image = EntityPostImage::new(
            entity_target.clone(),
            plan().contract_version(),
            record(12, 55),
        )
        .expect("post-image");
        let fixture = fixture(
            vec![EntityObservation::Absent(entity_target.clone())],
            vec![EntityMutation::Create(post_image)],
            vec![
                EventIntent::new(EventTypeId::new(7).expect("event type"), record(20, 1))
                    .expect("first event"),
                EventIntent::new(EventTypeId::new(8).expect("event type"), record(20, 2))
                    .expect("second event"),
            ],
            true,
        );
        let expected_plan = fixture.write_plan.clone();
        let expected_intent = expected_plan.intent().clone();
        let expected_claims = fixture.claims.clone();
        let assignment = fixture.assignment;

        let records =
            build_record_graph_for_test(assignment, fixture.write_plan, DurabilityMode::Sync)
                .expect("complete graph");

        assert!(records.write_plan() == &expected_plan);
        assert!(records.intent() == &expected_intent);
        assert_eq!(records.assignment(), assignment);
        assert_eq!(records.entities().len(), 1);
        let entity = &records.entities()[0];
        assert_eq!(entity.expected(), ExpectedEntityState::Absent);
        assert_eq!(entity.post_image().entity_version(), EntityVersion::first());
        assert!(entity.post_image().target() == &entity_target);
        assert!(
            entity.post_image().schema_binding()
                == &DurableKeySchemaBindingV1::from_plan(expected_intent.evaluated().plan())
        );
        assert!(
            entity.post_image().fields()
                == expected_intent.evaluated().mutations()[0]
                    .post_image()
                    .fields()
        );

        assert_eq!(records.events().len(), 2);
        for (ordinal, event) in records.events().iter().enumerate() {
            let expected_id = EventId::new(assignment.assigned(), ordinal as u32);
            assert_eq!(event.event_id(), expected_id);
            assert_eq!(
                event.event_hash(),
                derive_event_hash_v1(expected_id, event.event_type_id(), event.payload())
                    .expect("event hash")
            );
        }
        assert_eq!(records.outbox_intents().len(), records.events().len());
        assert!(
            records
                .outbox_intents()
                .iter()
                .zip(records.events())
                .all(|(intent, event)| intent.event() == event)
        );

        let outcome = records.stored_outcome();
        assert_eq!(outcome.commit_sequence(), assignment.assigned());
        assert_eq!(outcome.durability_mode(), DurabilityMode::Sync);
        assert!(outcome.declared_outcome() == expected_intent.evaluated().outcome());
        assert!(outcome.admitted_claims() == &expected_claims);
        assert_eq!(outcome.provenance_id(), expected_intent.provenance_id());

        let provenance = records.provenance();
        assert_eq!(provenance.commit_sequence(), assignment.assigned());
        assert_eq!(provenance.provenance_id(), expected_intent.provenance_id());
        assert_eq!(provenance.affected_entities().len(), 1);
        assert!(provenance.affected_entities()[0].target() == &entity_target);
        assert_eq!(provenance.event_ids(), records.commit().outbox_event_ids());
        assert!(provenance.admitted_claims() == &expected_claims);

        let commit = records.commit();
        assert_eq!(commit.commit_sequence(), assignment.assigned());
        assert_eq!(commit.durability_mode(), DurabilityMode::Sync);
        assert_eq!(
            commit.entity_references(),
            records
                .entities()
                .iter()
                .map(CommittedEntityReferenceV2::from_mutation)
                .collect::<Result<Vec<_>, _>>()
                .expect("entity references")
                .as_slice()
        );
        assert!(commit.events() == records.events());
        assert!(commit.declared_outcome() == outcome.declared_outcome());
        assert_eq!(commit.provenance_id(), provenance.provenance_id());
        assert!(
            commit.read_dependencies()
                == &riffdb_storage_api::StoredReadDependenciesV1::from_live(
                    expected_intent.evaluated().read_dependencies()
                )
                .expect("stored dependencies")
        );

        assert_eq!(records.index_entries().len(), 1);
        assert_eq!(records.index_epochs().len(), 1);
        assert_eq!(
            records.index_epochs()[0].prior(),
            IndexEpochPosition::BeforeFirst
        );
        assert_eq!(records.index_epochs()[0].next(), IndexEpoch::first());
        encode_atomic_command_record_set_v1(&records).expect("canonical graph encoding");
    }

    #[test]
    fn zero_mutation_outcome_still_builds_sequence_outcome_provenance_and_commit() {
        let fixture = fixture(Vec::new(), Vec::new(), Vec::new(), false);
        let assignment = fixture.assignment;
        let records =
            build_record_graph_for_test(assignment, fixture.write_plan, DurabilityMode::Sync)
                .expect("zero-mutation graph");

        assert!(records.entities().is_empty());
        assert!(records.index_entries().is_empty());
        assert!(records.index_epochs().is_empty());
        assert!(records.events().is_empty());
        assert!(records.outbox_intents().is_empty());
        assert_eq!(
            records.stored_outcome().commit_sequence(),
            assignment.assigned()
        );
        assert_eq!(
            records.provenance().commit_sequence(),
            assignment.assigned()
        );
        assert_eq!(records.commit().commit_sequence(), assignment.assigned());
        assert!(records.provenance().affected_entities().is_empty());
        assert!(records.provenance().event_ids().is_empty());
        encode_atomic_command_record_set_v1(&records).expect("canonical zero-mutation encoding");
    }

    #[test]
    fn every_storage_durability_mode_is_propagated_without_substitution() {
        for mode in [
            DurabilityMode::Sync,
            DurabilityMode::Group,
            DurabilityMode::Memory,
        ] {
            let fixture = fixture(Vec::new(), Vec::new(), Vec::new(), false);
            let records = build_record_graph_for_test(fixture.assignment, fixture.write_plan, mode)
                .expect("durability-specific graph");
            assert_eq!(records.stored_outcome().durability_mode(), mode);
            assert_eq!(records.commit().durability_mode(), mode);
            assert!(records.matches_durability_mode(mode));
        }
    }

    #[test]
    fn checked_record_input_rejects_index_values_not_carried_by_the_candidate() {
        let entity_target = target(70);
        let post_image = EntityPostImage::new(
            entity_target.clone(),
            plan().contract_version(),
            record(12, 55),
        )
        .expect("post-image");
        let fixture = fixture(
            vec![EntityObservation::Absent(entity_target)],
            vec![EntityMutation::Create(post_image)],
            Vec::new(),
            true,
        );
        let candidate = RetainedCheckedCommitCandidate::for_record_graph_test(
            fixture.write_plan.intent().clone(),
            Vec::new(),
            fixture.write_plan.affected_targets().clone(),
        );
        assert!(matches!(
            CheckedRecordGraphInput::new_for_test(
                &candidate,
                fixture.assignment,
                fixture.write_plan,
            ),
            Err(error) if error == CommandRecordGraphError::internal_defect()
        ));
    }

    #[test]
    fn replacement_derives_the_exact_expected_and_next_entity_versions() {
        let entity_target = target(44);
        let current_version = EntityVersion::new(40).expect("current entity version");
        let current = StoredEntityRecordV1::new(
            entity_target.clone(),
            current_version,
            plan().contract_version(),
            DurableKeySchemaBindingV1::from_plan(&plan()),
            record(12, 1),
        )
        .expect("current record");
        let replacement_fields = record(12, 2);
        let replacement = EntityMutation::Replace {
            expected_version: current_version,
            post_image: EntityPostImage::new(
                entity_target.clone(),
                plan().contract_version(),
                replacement_fields.clone(),
            )
            .expect("replacement"),
        };
        let fixture = fixture(
            vec![EntityObservation::Present(current)],
            vec![replacement],
            Vec::new(),
            false,
        );

        let records = build_record_graph_for_test(
            fixture.assignment,
            fixture.write_plan,
            DurabilityMode::Sync,
        )
        .expect("replacement graph");
        let mutation = &records.entities()[0];
        assert_eq!(
            mutation.expected(),
            ExpectedEntityState::Present(current_version)
        );
        assert_eq!(
            mutation.post_image().entity_version(),
            current_version.checked_next().expect("next entity version")
        );
        assert!(mutation.post_image().target() == &entity_target);
        assert!(mutation.post_image().fields() == &replacement_fields);
    }

    #[test]
    fn exhausted_entity_version_fails_closed_without_constructing_a_graph() {
        let entity_target = target(99);
        let maximum = EntityVersion::new(u64::MAX).expect("maximum entity version");
        let current = StoredEntityRecordV1::new(
            entity_target.clone(),
            maximum,
            plan().contract_version(),
            DurableKeySchemaBindingV1::from_plan(&plan()),
            record(12, 1),
        )
        .expect("current record");
        let replacement = EntityMutation::Replace {
            expected_version: maximum,
            post_image: EntityPostImage::new(
                entity_target,
                plan().contract_version(),
                record(12, 2),
            )
            .expect("replacement"),
        };
        let fixture = fixture(
            vec![EntityObservation::Present(current)],
            vec![replacement],
            Vec::new(),
            false,
        );

        let error = build_record_graph_for_test(
            fixture.assignment,
            fixture.write_plan,
            DurabilityMode::Sync,
        )
        .expect_err("entity versions cannot wrap");
        assert_eq!(format!("{error:?}"), "CommandRecordGraphError([REDACTED])");
        assert!(!format!("{error:?} {error}").contains("record-graph"));
    }

    #[test]
    fn checked_candidate_requires_the_exact_provenance_bound_intent() {
        let fixture = fixture(Vec::new(), Vec::new(), Vec::new(), false);
        let exact = fixture.write_plan.intent().clone();
        let candidate = RetainedCheckedCommitCandidate::for_record_graph_test(
            exact.clone(),
            Vec::new(),
            fixture.write_plan.affected_targets().clone(),
        );
        assert!(candidate.matches_intent(&exact));

        let context = PreEvaluationCommitContext::new(
            exact.pending().clone(),
            exact.partition_hash(),
            exact.conflict_hashes().to_vec(),
        )
        .expect("exact pre-evaluation context");
        let substituted = riffdb_storage_api::CommitIntent::new(
            context,
            exact.evaluated().clone(),
            ProvenanceId::from_bytes(uuid_bytes(0x55)).expect("second provenance ID"),
        )
        .expect("otherwise equal intent");
        assert_ne!(exact.provenance_id(), substituted.provenance_id());
        assert!(!candidate.matches_intent(&substituted));
    }

    struct ScriptedLookupRepository {
        expected: IdempotencyLookupCandidatesV1,
        result: Result<AdmissionLookupResultV1, StorageError>,
        lookup_calls: Cell<usize>,
        admission_calls: Cell<usize>,
    }

    impl AdmissionRepository for ScriptedLookupRepository {
        fn admit_or_resolve(
            &self,
            _request: riffdb_storage_api::AdmissionRequestV1,
        ) -> Result<riffdb_storage_api::AdmissionResultV1, StorageError> {
            self.admission_calls.set(self.admission_calls.get() + 1);
            panic!("uncertain commit recovery must not write admission state")
        }

        fn lookup_admission(
            &self,
            candidates: IdempotencyLookupCandidatesV1,
        ) -> Result<AdmissionLookupResultV1, StorageError> {
            assert_eq!(candidates, self.expected);
            self.lookup_calls.set(self.lookup_calls.get() + 1);
            self.result.clone()
        }
    }

    fn uncertain_commit_fixture() -> (
        Box<UncertainCommandCommit>,
        StoredOutcomeV1,
        riffdb_storage_api::StoredPendingAdmissionV1,
    ) {
        let fixture = fixture(Vec::new(), Vec::new(), Vec::new(), false);
        let expected = build_record_graph_for_test(
            fixture.assignment,
            fixture.write_plan.clone(),
            DurabilityMode::Sync,
        )
        .expect("checked graph")
        .stored_outcome()
        .clone();
        let pending = fixture.write_plan.intent().pending().clone();
        let candidate = RetainedCheckedCommitCandidate::for_record_graph_test(
            fixture.write_plan.intent().clone(),
            fixture.write_plan.index_entries().to_vec(),
            fixture.write_plan.affected_targets().clone(),
        );
        let result = finish_checked_commit(
            candidate,
            expected.clone(),
            DurabilityMode::Sync,
            Err(StorageError::new(
                StorageErrorKind::CommitStatusUnknown,
                None,
            )),
        );
        let CheckedCommandCommitResult::StatusUnknown(uncertain) = result else {
            panic!("unknown commit must retain its checked evidence")
        };
        (uncertain, expected, pending)
    }

    fn scripted_repository(
        uncertain: &UncertainCommandCommit,
        result: Result<AdmissionLookupResultV1, StorageError>,
    ) -> ScriptedLookupRepository {
        ScriptedLookupRepository {
            expected: uncertain.lookup_candidates.clone(),
            result,
            lookup_calls: Cell::new(0),
            admission_calls: Cell::new(0),
        }
    }

    #[test]
    fn uncertain_commit_resolution_is_one_lookup_and_never_reexecutes_or_resources() {
        let (uncertain, expected, _) = uncertain_commit_fixture();
        assert_eq!(
            uncertain.cause.kind(),
            StorageErrorKind::CommitStatusUnknown
        );
        assert_eq!(uncertain.expected_outcome, expected);
        assert_eq!(
            uncertain.candidate.exact_intent().provenance_id(),
            expected.provenance_id()
        );
        let repository = scripted_repository(
            &uncertain,
            Ok(AdmissionLookupResultV1::Found(Box::new(
                StoredAdmissionStateV1::StoredOutcome(expected.clone()),
            ))),
        );
        let resolution = resolve_uncertain_command_commit(&repository, uncertain);
        let UncertainCommandCommitResolution::Committed(committed) = resolution else {
            panic!("exact expected outcome must resolve the first commit")
        };
        assert_eq!(
            committed.disposition(),
            crate::CommittedOutcomeDisposition::FirstCommit
        );
        assert_eq!(committed.stored_outcome(), &expected);
        assert_eq!(repository.lookup_calls.get(), 1);
        assert_eq!(repository.admission_calls.get(), 0);
    }

    #[test]
    fn uncertain_commit_maps_only_exact_same_key_terminal_state() {
        let (uncertain, _, pending) = uncertain_commit_fixture();
        let failure =
            StoredExecutionFailedV1::new(pending.clone(), ExecutionFailureCode::ArithmeticFault);
        let repository = scripted_repository(
            &uncertain,
            Ok(AdmissionLookupResultV1::Found(Box::new(
                StoredAdmissionStateV1::ExecutionFailed(failure.clone()),
            ))),
        );
        assert!(matches!(
            resolve_uncertain_command_commit(&repository, uncertain),
            UncertainCommandCommitResolution::ExecutionFailureReplay(actual)
                if *actual == failure
        ));
        assert_eq!(repository.lookup_calls.get(), 1);

        let (uncertain, _, pending) = uncertain_commit_fixture();
        let repository = scripted_repository(
            &uncertain,
            Ok(AdmissionLookupResultV1::Found(Box::new(
                StoredAdmissionStateV1::Pending(pending),
            ))),
        );
        let UncertainCommandCommitResolution::ProvenNotCommitted(proven) =
            resolve_uncertain_command_commit(&repository, uncertain)
        else {
            panic!("exact pending must prove this attempt did not commit");
        };
        assert_eq!(format!("{proven:?}"), "ProvenNonCommitCommand([REDACTED])");
        assert_eq!(repository.lookup_calls.get(), 1);

        let (uncertain, _, _) = uncertain_commit_fixture();
        let repository = scripted_repository(&uncertain, Ok(AdmissionLookupResultV1::NotFound));
        assert!(matches!(
            resolve_uncertain_command_commit(&repository, uncertain),
            UncertainCommandCommitResolution::Integrity
        ));
        assert_eq!(repository.lookup_calls.get(), 1);

        let (uncertain, expected, _) = uncertain_commit_fixture();
        let repository = scripted_repository(
            &uncertain,
            Err(StorageError::new(StorageErrorKind::Unavailable, None)),
        );
        let UncertainCommandCommitResolution::OutcomeUnknown {
            uncertain,
            lookup_error,
        } = resolve_uncertain_command_commit(&repository, uncertain)
        else {
            panic!("failed recovery lookup remains outcome-uncertain");
        };
        assert_eq!(
            uncertain.cause.kind(),
            StorageErrorKind::CommitStatusUnknown
        );
        assert_eq!(lookup_error.kind(), StorageErrorKind::Unavailable);
        assert_eq!(uncertain.expected_outcome, expected);
        assert_eq!(
            uncertain.candidate.exact_intent().provenance_id(),
            expected.provenance_id()
        );
        assert_eq!(repository.lookup_calls.get(), 1);

        let recovered = scripted_repository(
            &uncertain,
            Ok(AdmissionLookupResultV1::Found(Box::new(
                StoredAdmissionStateV1::StoredOutcome(expected.clone()),
            ))),
        );
        assert!(matches!(
            resolve_uncertain_command_commit(&recovered, uncertain),
            UncertainCommandCommitResolution::Committed(committed)
                if committed.stored_outcome() == &expected
        ));
        assert_eq!(recovered.lookup_calls.get(), 1);
        assert_eq!(recovered.admission_calls.get(), 0);

        let (uncertain, _, _) = uncertain_commit_fixture();
        let repository =
            scripted_repository(&uncertain, Ok(AdmissionLookupResultV1::MultipleMatches));
        assert!(matches!(
            resolve_uncertain_command_commit(&repository, uncertain),
            UncertainCommandCommitResolution::Integrity
        ));
        assert_eq!(repository.lookup_calls.get(), 1);
    }
}
