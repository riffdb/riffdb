//! Pure grammar/IR-v1 secondary-index derivation.
//!
//! This module is deliberately sealed until the coordinator carries the exact
//! validated command attempt into derivation by construction.

use std::collections::BTreeSet;

use riffdb_catalog::ResolvedExecutablePlan;
use riffdb_contract_ir::{
    BindingMode, DeleteCheckModeV1, EXECUTABLE_IR_VERSION_V1, EXECUTABLE_IR_VERSION_V2,
    EXECUTABLE_IR_VERSION_V3, EXECUTABLE_IR_VERSION_V4, EXECUTABLE_IR_VERSION_V5,
    EXECUTABLE_IR_VERSION_V6, EXECUTABLE_IR_VERSION_V7, EXECUTABLE_IR_VERSION_V8,
    EXECUTABLE_IR_VERSION_V9, EXECUTABLE_IR_VERSION_V10, ExecutionClass, GRAMMAR_VERSION_V1,
    GRAMMAR_VERSION_V2, GRAMMAR_VERSION_V3, GRAMMAR_VERSION_V4, GRAMMAR_VERSION_V5,
    GRAMMAR_VERSION_V6, GRAMMAR_VERSION_V7, GRAMMAR_VERSION_V8, GRAMMAR_VERSION_V9,
    GRAMMAR_VERSION_V10, IndexSchema,
};
use riffdb_invariant::{InputDerivedCommandFacts, derive_input_command_facts};
use riffdb_storage_api::{
    AffectedEpochCurrentState, AffectedIndexEpochTargets, CommandCandidateAffectedEpochRead,
    CommandCandidateAwaitingCapacity, CommandCandidateAwaitingValidation,
    CommandCandidateCapacityReserved, CommandCandidateSequenceAssigned, CommandWriteSetPlanV1,
    CommitIntent, DurableCodecError, DurableKeySchemaBindingV1, EncodedWriteSetUpperBound,
    EncodedWriteSetUpperBoundResultV1, EntityObservation, EvaluatedCommand,
    IdempotencyLookupCandidatesV1, IndexEntryMutationV1, IndexEpochAdvanceError,
    IndexEpochAdvanceV1, IndexRangePrefixBuilder, IndexRangeTarget,
    MAX_AFFECTED_INDEX_EPOCH_TARGETS, MAX_INDEX_DELTAS, MAX_READ_SNAPSHOT_BYTES,
    MAX_VALIDATION_TARGETS, PartitionIndexTarget, StorageError, StoredIndexEntryV2,
    TransactionCurrentState, UniqueIndexTarget, UniqueOccupancyKind,
    ValidatedCommandWriteSetShapeV1, command_write_set_upper_bound_v1,
};
use riffdb_types::{CanonicalRecord, CanonicalValue, IndexEntryKey, PartitionKey};

use crate::command_attempt::{
    PendingCommandAttempts, PostApplyCommandEvidence, RolledBackCandidateDisposition,
};
#[cfg(test)]
use crate::command_validation::CheckedCandidateSeal;
use crate::command_validation::{
    CheckedAffectedEpochRead, CheckedCapacityReservation, CheckedSequenceAssignment,
    CheckedStorageStage, CheckedValidatedCommand, StagedValidatedCommand,
};

const AFFECTED_CURRENT_STATE_FIXED_BYTES_V1: usize = 4;
const INDEX_RANGE_TARGET_FIXED_BYTES_V1: usize = 8;
const MAX_INDEX_EPOCH_POSITION_BYTES_V1: usize = 9;

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) struct CommandIndexError {
    _private: (),
}

impl CommandIndexError {
    const fn internal_defect() -> Self {
        Self { _private: () }
    }
}

impl std::fmt::Debug for CommandIndexError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("CommandIndexError([REDACTED])")
    }
}

/// Derives every compiler-sealed reverse-index emptiness dependency.
///
/// The caller supplies only the move-only input proof. Index identities,
/// component codecs, partition routing, and prefixes all come from the exact
/// resolved plan and contract schema; no application range bytes cross this
/// boundary.
pub(super) fn derive_delete_restrict_ranges(
    resolved: &ResolvedExecutablePlan,
    facts: &InputDerivedCommandFacts,
) -> Result<Vec<IndexRangeTarget>, CommandIndexError> {
    let plan = resolved.plan();
    if facts.binding_plan_indices().len() != facts.binding_entity_keys().len() {
        return Err(CommandIndexError::internal_defect());
    }
    let schema = resolved.bundle().bundle().schema();
    let mut ranges = Vec::new();
    for (plan_index, key) in facts
        .binding_plan_indices()
        .iter()
        .zip(facts.binding_entity_keys())
    {
        let binding = plan
            .bindings()
            .get(*plan_index as usize)
            .ok_or_else(CommandIndexError::internal_defect)?;
        if binding.mode() != BindingMode::Delete {
            continue;
        }
        let check = plan
            .delete_checks()
            .iter()
            .find(|check| check.binding() == binding.id())
            .ok_or_else(CommandIndexError::internal_defect)?;
        let DeleteCheckModeV1::Restrict {
            source_entity,
            index_id,
        } = check.mode()
        else {
            continue;
        };
        let source = schema
            .entity(source_entity)
            .ok_or_else(CommandIndexError::internal_defect)?;
        let index = source
            .indexes()
            .iter()
            .find(|index| index.id() == index_id)
            .ok_or_else(CommandIndexError::internal_defect)?;
        let values = binding
            .key_schema()
            .decode_entity(key)
            .map_err(|_| CommandIndexError::internal_defect())?;
        if values.is_empty() || values.len() > index.key_schema().components().len() {
            return Err(CommandIndexError::internal_defect());
        }
        let mut storage_prefix = IndexRangePrefixBuilder::new(index_id);
        for (component, value) in index.key_schema().components().iter().zip(&values) {
            push_storage_prefix_component(&mut storage_prefix, component.codec(), value)?;
        }
        let storage_prefix = storage_prefix.finish();
        let ir_prefix = index
            .key_schema()
            .encode_index_prefix(&values)
            .map_err(|_| CommandIndexError::internal_defect())?;
        if storage_prefix.as_bytes() != ir_prefix.as_bytes() {
            return Err(CommandIndexError::internal_defect());
        }
        ranges.push(IndexRangeTarget::new(
            facts.partition_key().clone(),
            storage_prefix,
        ));
    }
    ranges.sort_unstable();
    if ranges.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(CommandIndexError::internal_defect());
    }
    Ok(ranges)
}

struct DerivedCommandIndexes {
    entry_mutations: Vec<IndexEntryMutationV1>,
    affected_targets: AffectedIndexEpochTargets,
}

/// Exact semantically checked candidate retained after index derivation.
pub(super) struct CheckedCommitCandidate<S = ()> {
    authority: CheckedAttemptAuthority<S>,
    entry_mutations: Vec<IndexEntryMutationV1>,
    affected_targets: AffectedIndexEpochTargets,
}

struct CheckedAttemptAuthority<S>(Box<CheckedValidatedCommand<S>>);

impl<S> CheckedCommitCandidate<S> {
    pub(super) fn entry_mutations(&self) -> &[IndexEntryMutationV1] {
        &self.entry_mutations
    }

    pub(super) const fn affected_targets(&self) -> &AffectedIndexEpochTargets {
        &self.affected_targets
    }

    pub(super) fn matches_intent(&self, intent: &CommitIntent) -> bool {
        let attempt = self.authority.0.attempt();
        attempt.has_exact_semantic_join() && intent == attempt.commit_intent()
    }
}

/// Consumes the sole post-validation authority and freezes the exact derived
/// index values that later write-plan construction must preserve.
pub(super) fn derive_checked_command_indexes<C>(
    checked: CheckedValidatedCommand<C>,
) -> Result<CheckedCommitCandidate<C::AffectedEpochRead>, CommandIndexError>
where
    C: CommandCandidateAwaitingValidation,
{
    let derived = if checked.evaluated().mutations().is_empty() {
        DerivedCommandIndexes {
            entry_mutations: Vec::new(),
            affected_targets: AffectedIndexEpochTargets::new(Vec::new())
                .map_err(|_| CommandIndexError::internal_defect())?,
        }
    } else {
        derive_grammar_v1_indexes(
            checked.resolved(),
            checked.attempt().normalized_input(),
            checked.evaluated(),
            checked.current(),
            checked.mutation_positions(),
            checked.attempt().commit_intent().pending().partition_key(),
        )?
    };
    let checked = checked.plan_validated(derived.affected_targets.clone());
    Ok(CheckedCommitCandidate {
        authority: CheckedAttemptAuthority(Box::new(checked)),
        entry_mutations: derived.entry_mutations,
        affected_targets: derived.affected_targets,
    })
}

/// Closed result of reading exact mutation-affected epoch positions.
pub(super) enum CheckedAffectedEpochDecision<C> {
    Ready(CheckedCommitCandidate<C>),
    Rejected(RolledBackCandidateDisposition),
    StorageFailure(StorageError),
}

impl<S> CheckedCommitCandidate<S>
where
    S: CommandCandidateAffectedEpochRead,
{
    pub(super) fn read_affected_epoch_current(
        self,
    ) -> CheckedAffectedEpochDecision<S::AwaitingCapacity> {
        let Self {
            authority,
            entry_mutations,
            affected_targets,
        } = self;
        let CheckedAttemptAuthority(checked) = authority;
        match checked.read_affected_epoch_current() {
            CheckedAffectedEpochRead::Ready(checked) => {
                if checked
                    .awaiting_capacity()
                    .affected_current()
                    .unique_occupancies()
                    .iter()
                    .any(|occupancy| occupancy.kind() == UniqueOccupancyKind::Conflict)
                {
                    return CheckedAffectedEpochDecision::Rejected(
                        checked.reject_unique_conflict(),
                    );
                }
                CheckedAffectedEpochDecision::Ready(CheckedCommitCandidate {
                    authority: CheckedAttemptAuthority(checked),
                    entry_mutations,
                    affected_targets,
                })
            }
            CheckedAffectedEpochRead::StorageFailure(error) => {
                CheckedAffectedEpochDecision::StorageFailure(error)
            }
        }
    }
}

/// Closed result of deriving and reserving the exact sequence-free write plan.
pub(super) enum CheckedReserveDecision<C> {
    Reserved(CheckedCommitCandidate<C>),
    CapacityUnavailable,
    BatchFull,
    ProvenanceIdCollision,
    EpochExhausted,
    StorageFailure(StorageError),
    Integrity,
}

enum SequenceFreeWriteSetSizing {
    Fits(EncodedWriteSetUpperBound),
    CapacityUnavailable,
    Integrity,
}

enum SequenceFreeWriteSetPreparation {
    Ready(Box<CommandWriteSetPlanV1>),
    CapacityUnavailable,
    Integrity,
}

fn classify_sequence_free_write_set_sizing(
    result: Result<EncodedWriteSetUpperBoundResultV1, DurableCodecError>,
) -> SequenceFreeWriteSetSizing {
    match result {
        Ok(EncodedWriteSetUpperBoundResultV1::Fits(bound)) => {
            SequenceFreeWriteSetSizing::Fits(bound)
        }
        Ok(EncodedWriteSetUpperBoundResultV1::ExceedsAcceptedAggregateCap(_codec_origin)) => {
            SequenceFreeWriteSetSizing::CapacityUnavailable
        }
        Err(_) => SequenceFreeWriteSetSizing::Integrity,
    }
}

fn prepare_sequence_free_write_set(
    intent: &CommitIntent,
    affected_targets: AffectedIndexEpochTargets,
    affected_current: AffectedEpochCurrentState,
    index_entries: Vec<IndexEntryMutationV1>,
    index_epochs: Vec<IndexEpochAdvanceV1>,
) -> SequenceFreeWriteSetPreparation {
    let shape = match ValidatedCommandWriteSetShapeV1::new(
        intent,
        affected_targets,
        affected_current,
        index_entries,
        index_epochs,
    ) {
        Ok(shape) => shape,
        Err(_) => return SequenceFreeWriteSetPreparation::Integrity,
    };
    match classify_sequence_free_write_set_sizing(command_write_set_upper_bound_v1(
        shape.intent(),
        shape.index_entries(),
        shape.index_epochs(),
    )) {
        SequenceFreeWriteSetSizing::Fits(bound) => SequenceFreeWriteSetPreparation::Ready(
            Box::new(CommandWriteSetPlanV1::from_validated_shape(shape, bound)),
        ),
        SequenceFreeWriteSetSizing::CapacityUnavailable => {
            SequenceFreeWriteSetPreparation::CapacityUnavailable
        }
        SequenceFreeWriteSetSizing::Integrity => SequenceFreeWriteSetPreparation::Integrity,
    }
}

impl<S> CheckedCommitCandidate<S>
where
    S: CommandCandidateAwaitingCapacity,
{
    pub(super) fn reserve_capacity(self) -> CheckedReserveDecision<S::CapacityReserved> {
        let Self {
            authority,
            entry_mutations,
            affected_targets,
        } = self;
        let CheckedAttemptAuthority(checked) = authority;
        let retained = checked.awaiting_capacity();
        if retained.intent() != checked.attempt().commit_intent()
            || retained.affected_targets() != &affected_targets
            || retained.affected_current().observations().len() != affected_targets.as_slice().len()
            || retained.affected_current().unique_occupancies().len()
                != affected_targets.unique_targets().len()
        {
            drop(checked);
            return CheckedReserveDecision::Integrity;
        }
        let schema_binding = DurableKeySchemaBindingV1::from_plan(
            checked.attempt().commit_intent().evaluated().plan(),
        );
        let mut epoch_advances = Vec::with_capacity(affected_targets.as_slice().len());
        for (target, observation) in affected_targets
            .as_slice()
            .iter()
            .zip(retained.affected_current().observations())
        {
            if observation.target() != target {
                drop(checked);
                return CheckedReserveDecision::Integrity;
            }
            let advance = match IndexEpochAdvanceV1::new(
                target.clone(),
                schema_binding.clone(),
                observation.epoch(),
            ) {
                Ok(advance) => advance,
                Err(IndexEpochAdvanceError::Exhausted) => {
                    drop(checked);
                    return CheckedReserveDecision::EpochExhausted;
                }
            };
            epoch_advances.push(advance);
        }
        let write_plan = match prepare_sequence_free_write_set(
            checked.attempt().commit_intent(),
            affected_targets.clone(),
            retained.affected_current().clone(),
            entry_mutations.clone(),
            epoch_advances,
        ) {
            SequenceFreeWriteSetPreparation::Ready(write_plan) => *write_plan,
            SequenceFreeWriteSetPreparation::CapacityUnavailable => {
                drop(checked);
                return CheckedReserveDecision::CapacityUnavailable;
            }
            SequenceFreeWriteSetPreparation::Integrity => {
                drop(checked);
                return CheckedReserveDecision::Integrity;
            }
        };
        let expected_write_plan = write_plan.clone();
        match checked.reserve_capacity(write_plan) {
            CheckedCapacityReservation::Reserved(checked)
                if checked.capacity_reserved().write_plan() == &expected_write_plan =>
            {
                CheckedReserveDecision::Reserved(CheckedCommitCandidate {
                    authority: CheckedAttemptAuthority(checked),
                    entry_mutations,
                    affected_targets,
                })
            }
            CheckedCapacityReservation::Reserved(checked) => {
                drop(checked);
                CheckedReserveDecision::Integrity
            }
            CheckedCapacityReservation::BatchFull => CheckedReserveDecision::BatchFull,
            CheckedCapacityReservation::ProvenanceIdCollision => {
                CheckedReserveDecision::ProvenanceIdCollision
            }
            CheckedCapacityReservation::StorageFailure(error) => {
                CheckedReserveDecision::StorageFailure(error)
            }
            CheckedCapacityReservation::Integrity => CheckedReserveDecision::Integrity,
        }
    }
}

/// Closed result of assigning an invisible transaction-local sequence.
pub(super) enum CheckedAssignDecision<C> {
    Assigned(CheckedCommitCandidate<C>),
    StorageFailure(StorageError),
    Integrity,
}

impl<S> CheckedCommitCandidate<S>
where
    S: CommandCandidateCapacityReserved,
{
    pub(super) fn assign_sequence(self) -> CheckedAssignDecision<S::SequenceAssigned> {
        let Self {
            authority,
            entry_mutations,
            affected_targets,
        } = self;
        let CheckedAttemptAuthority(checked) = authority;
        let retained = checked.capacity_reserved();
        if retained.intent() != checked.attempt().commit_intent()
            || retained.write_plan().index_entries() != entry_mutations
            || retained.write_plan().affected_targets() != &affected_targets
        {
            drop(checked);
            return CheckedAssignDecision::Integrity;
        }
        let expected_write_plan = retained.write_plan().clone();
        match checked.assign_sequence() {
            CheckedSequenceAssignment::Assigned(checked)
                if checked.sequence_assigned().write_plan() == &expected_write_plan =>
            {
                CheckedAssignDecision::Assigned(CheckedCommitCandidate {
                    authority: CheckedAttemptAuthority(checked),
                    entry_mutations,
                    affected_targets,
                })
            }
            CheckedSequenceAssignment::Assigned(checked) => {
                drop(checked);
                CheckedAssignDecision::Integrity
            }
            CheckedSequenceAssignment::StorageFailure(error) => {
                CheckedAssignDecision::StorageFailure(error)
            }
            CheckedSequenceAssignment::Integrity => CheckedAssignDecision::Integrity,
        }
    }
}

/// Non-generic semantic evidence retained after staging and across engine commit.
pub(super) struct RetainedCheckedCommitCandidate {
    authority: RetainedCheckedAttemptAuthority,
    entry_mutations: Vec<IndexEntryMutationV1>,
    affected_targets: AffectedIndexEpochTargets,
}

/// Checked same-attempt evidence after private storage apply has consumed the
/// conflict capability.
pub(super) struct PostApplyCheckedCommitCandidate {
    evidence: PostApplyCommandEvidence,
}

impl PostApplyCheckedCommitCandidate {
    pub(super) const fn exact_intent(&self) -> &CommitIntent {
        self.evidence.exact_intent()
    }

    pub(super) const fn lookup_candidates(&self) -> &IdempotencyLookupCandidatesV1 {
        self.evidence.lookup_candidates()
    }

    pub(super) fn matches_terminal_outcome(
        &self,
        outcome: &riffdb_storage_api::StoredOutcomeV1,
    ) -> bool {
        self.evidence.matches_terminal_outcome(outcome)
    }

    pub(super) fn matches_terminal_failure(
        &self,
        failure: &riffdb_storage_api::StoredExecutionFailedV1,
    ) -> bool {
        self.evidence.matches_terminal_failure(failure)
    }

    pub(super) fn into_pending_after_proven_noncommit(self) -> PendingCommandAttempts {
        self.evidence.into_pending_after_proven_noncommit()
    }
}

enum RetainedCheckedAttemptAuthority {
    Validated(Box<StagedValidatedCommand>),
    #[cfg(test)]
    Fixture {
        _seal: CheckedCandidateSeal,
        intent: Box<CommitIntent>,
        lookup_candidates: IdempotencyLookupCandidatesV1,
    },
}

impl RetainedCheckedCommitCandidate {
    pub(super) fn audited_lifecycle(
        &self,
    ) -> Option<&crate::command_preparation::AuditedCommandLifecycle> {
        match &self.authority {
            RetainedCheckedAttemptAuthority::Validated(checked) => {
                checked.attempt().audited_lifecycle()
            }
            #[cfg(test)]
            RetainedCheckedAttemptAuthority::Fixture { .. } => None,
        }
    }

    #[cfg(test)]
    pub(super) fn entry_mutations(&self) -> &[IndexEntryMutationV1] {
        &self.entry_mutations
    }

    #[cfg(test)]
    pub(super) const fn affected_targets(&self) -> &AffectedIndexEpochTargets {
        &self.affected_targets
    }

    #[cfg(test)]
    pub(super) fn matches_intent(&self, intent: &CommitIntent) -> bool {
        match &self.authority {
            RetainedCheckedAttemptAuthority::Validated(checked) => {
                let attempt = checked.attempt();
                attempt.has_exact_semantic_join() && intent == attempt.commit_intent()
            }
            #[cfg(test)]
            RetainedCheckedAttemptAuthority::Fixture {
                intent: expected, ..
            } => expected.as_ref() == intent,
        }
    }

    pub(super) const fn exact_intent(&self) -> &CommitIntent {
        match &self.authority {
            RetainedCheckedAttemptAuthority::Validated(checked) => {
                checked.attempt().commit_intent()
            }
            #[cfg(test)]
            RetainedCheckedAttemptAuthority::Fixture { intent, .. } => intent,
        }
    }

    pub(super) const fn lookup_candidates(&self) -> &IdempotencyLookupCandidatesV1 {
        match &self.authority {
            RetainedCheckedAttemptAuthority::Validated(checked) => {
                checked.attempt().lookup_candidates()
            }
            #[cfg(test)]
            RetainedCheckedAttemptAuthority::Fixture {
                lookup_candidates, ..
            } => lookup_candidates,
        }
    }

    pub(super) fn into_pending_after_proven_noncommit(self) -> Result<PendingCommandAttempts, ()> {
        let Self {
            authority,
            entry_mutations,
            affected_targets,
        } = self;
        drop(entry_mutations);
        drop(affected_targets);
        match authority {
            RetainedCheckedAttemptAuthority::Validated(checked) => {
                Ok(checked.into_pending_after_proven_noncommit())
            }
            #[cfg(test)]
            RetainedCheckedAttemptAuthority::Fixture { .. } => Err(()),
        }
    }

    /// Releases retained semantic evidence after the enclosing uncommitted
    /// storage batch has been rolled back.
    pub(super) fn into_pending_after_group_rollback(self) -> Result<PendingCommandAttempts, ()> {
        self.into_pending_after_proven_noncommit()
    }

    /// Consumes the live attempt only after storage returned a complete private
    /// applied epoch, releasing its conflict lease while retaining uncertainty
    /// resolution evidence.
    pub(super) fn into_post_apply_evidence(self) -> Result<PostApplyCheckedCommitCandidate, ()> {
        let Self {
            authority,
            entry_mutations,
            affected_targets,
        } = self;
        drop(entry_mutations);
        drop(affected_targets);
        match authority {
            RetainedCheckedAttemptAuthority::Validated(checked) => checked
                .into_post_apply_evidence()
                .map(|evidence| PostApplyCheckedCommitCandidate { evidence }),
            #[cfg(test)]
            RetainedCheckedAttemptAuthority::Fixture { .. } => Err(()),
        }
    }

    #[cfg(test)]
    pub(super) fn for_record_graph_test(
        intent: CommitIntent,
        entry_mutations: Vec<IndexEntryMutationV1>,
        affected_targets: AffectedIndexEpochTargets,
    ) -> Self {
        let lookup_candidates =
            IdempotencyLookupCandidatesV1::new(vec![intent.pending().identity().clone()])
                .expect("one fixture lookup identity");
        Self {
            authority: RetainedCheckedAttemptAuthority::Fixture {
                _seal: CheckedCandidateSeal::for_record_graph_test(),
                intent: Box::new(intent),
                lookup_candidates,
            },
            entry_mutations,
            affected_targets,
        }
    }
}

/// Closed stage result retaining exact checked evidence on success only.
pub(super) enum CheckedCandidateStage<S> {
    Staged {
        storage: S,
        evidence: RetainedCheckedCommitCandidate,
    },
    StorageFailure(StorageError),
    Integrity,
}

impl<S> CheckedCommitCandidate<S>
where
    S: CommandCandidateSequenceAssigned,
{
    pub(super) fn assignment(&self) -> riffdb_storage_api::AssignedCommandSequence {
        self.authority.0.sequence_assigned().assignment()
    }

    pub(super) fn write_plan(&self) -> &CommandWriteSetPlanV1 {
        self.authority.0.sequence_assigned().write_plan()
    }

    pub(super) fn stage(
        self,
        records: riffdb_storage_api::AtomicCommandRecordSet,
    ) -> CheckedCandidateStage<S::Staged> {
        let Self {
            authority,
            entry_mutations,
            affected_targets,
        } = self;
        let CheckedAttemptAuthority(checked) = authority;
        match checked.stage(records) {
            CheckedStorageStage::Staged { storage, evidence } => CheckedCandidateStage::Staged {
                storage,
                evidence: RetainedCheckedCommitCandidate {
                    authority: RetainedCheckedAttemptAuthority::Validated(evidence),
                    entry_mutations,
                    affected_targets,
                },
            },
            CheckedStorageStage::StorageFailure(error) => {
                CheckedCandidateStage::StorageFailure(error)
            }
            CheckedStorageStage::Integrity => CheckedCandidateStage::Integrity,
        }
    }
}

struct IndexDerivationBuilder {
    entry_mutations: Vec<IndexEntryMutationV1>,
    entry_keys: BTreeSet<IndexEntryKey>,
    affected_targets: BTreeSet<PartitionIndexTarget>,
    unique_targets: BTreeSet<UniqueIndexTarget>,
    validation_positions: usize,
    affected_current_semantic_bytes: usize,
}

impl IndexDerivationBuilder {
    fn new(binding_count: usize, root_count: usize) -> Result<Self, CommandIndexError> {
        let validation_positions = binding_count
            .checked_add(root_count)
            .ok_or_else(CommandIndexError::internal_defect)?;
        if validation_positions > MAX_VALIDATION_TARGETS {
            return Err(CommandIndexError::internal_defect());
        }
        Ok(Self {
            entry_mutations: Vec::new(),
            entry_keys: BTreeSet::new(),
            affected_targets: BTreeSet::new(),
            unique_targets: BTreeSet::new(),
            validation_positions,
            affected_current_semantic_bytes: AFFECTED_CURRENT_STATE_FIXED_BYTES_V1,
        })
    }

    fn push_entry(&mut self, mutation: IndexEntryMutationV1) -> Result<(), CommandIndexError> {
        if self.entry_mutations.len() >= MAX_INDEX_DELTAS
            || !self.entry_keys.insert(mutation.key().clone())
        {
            return Err(CommandIndexError::internal_defect());
        }
        self.entry_mutations.push(mutation);
        Ok(())
    }

    fn insert_target(&mut self, target: PartitionIndexTarget) -> Result<(), CommandIndexError> {
        if self.affected_targets.contains(&target) {
            return Ok(());
        }
        if self.affected_targets.len() >= MAX_AFFECTED_INDEX_EPOCH_TARGETS {
            return Err(CommandIndexError::internal_defect());
        }
        let next_positions = self
            .validation_positions
            .checked_add(1)
            .ok_or_else(CommandIndexError::internal_defect)?;
        if next_positions > MAX_VALIDATION_TARGETS {
            return Err(CommandIndexError::internal_defect());
        }
        let observation_bytes = target
            .partition_key()
            .as_bytes()
            .len()
            .checked_add(INDEX_RANGE_TARGET_FIXED_BYTES_V1)
            .and_then(|bytes| bytes.checked_add(MAX_INDEX_EPOCH_POSITION_BYTES_V1))
            .ok_or_else(CommandIndexError::internal_defect)?;
        let next_bytes = self
            .affected_current_semantic_bytes
            .checked_add(observation_bytes)
            .ok_or_else(CommandIndexError::internal_defect)?;
        if next_bytes > MAX_READ_SNAPSHOT_BYTES {
            return Err(CommandIndexError::internal_defect());
        }
        self.affected_targets.insert(target);
        self.validation_positions = next_positions;
        self.affected_current_semantic_bytes = next_bytes;
        Ok(())
    }

    fn insert_unique(&mut self, target: UniqueIndexTarget) -> Result<(), CommandIndexError> {
        if self.unique_targets.len() >= MAX_AFFECTED_INDEX_EPOCH_TARGETS
            || self.unique_targets.contains(&target)
        {
            return Err(CommandIndexError::internal_defect());
        }
        let next_positions = self
            .validation_positions
            .checked_add(1)
            .ok_or_else(CommandIndexError::internal_defect)?;
        if next_positions > MAX_VALIDATION_TARGETS {
            return Err(CommandIndexError::internal_defect());
        }
        let observation_bytes = target
            .prefix()
            .prefix()
            .as_bytes()
            .len()
            .checked_add(target.expected_entry().as_bytes().len())
            .and_then(|bytes| bytes.checked_add(13))
            .ok_or_else(CommandIndexError::internal_defect)?;
        let next_bytes = self
            .affected_current_semantic_bytes
            .checked_add(observation_bytes)
            .ok_or_else(CommandIndexError::internal_defect)?;
        if next_bytes > MAX_READ_SNAPSHOT_BYTES {
            return Err(CommandIndexError::internal_defect());
        }
        self.unique_targets.insert(target);
        self.validation_positions = next_positions;
        self.affected_current_semantic_bytes = next_bytes;
        Ok(())
    }

    fn finish(mut self) -> Result<DerivedCommandIndexes, CommandIndexError> {
        self.entry_mutations
            .sort_unstable_by(|left, right| left.key().as_bytes().cmp(right.key().as_bytes()));
        if self
            .entry_mutations
            .windows(2)
            .any(|pair| pair[0].key().as_bytes() >= pair[1].key().as_bytes())
        {
            return Err(CommandIndexError::internal_defect());
        }
        let affected_targets = AffectedIndexEpochTargets::with_unique(
            self.affected_targets.into_iter().collect(),
            self.unique_targets.into_iter().collect(),
        )
        .map_err(|_| CommandIndexError::internal_defect())?;
        Ok(DerivedCommandIndexes {
            entry_mutations: self.entry_mutations,
            affected_targets,
        })
    }
}

// WP-606 per-version index-derivation audit gate.
//
// This whitelist is a fail-closed audit boundary in the commit coordinator:
// a grammar/IR pair is admitted only after a recorded audit of what the era
// changed and why `derive_grammar_v1_indexes` remains exact for it. Every
// admission below names its audit note and the evidence test that proves it.
// A NEW contract-ir era must NOT be added here without repeating this
// ceremony; the exhaustive boundary pin
// (`index_derivation_admits_exactly_the_audited_identity_pairs`) refuses
// anything beyond the audited set.
//
// Provenance (WP-606 git archaeology): V1..V6 were admitted incrementally
// with their version bumps (V5 `c2f78f77`, V6 `2ecea31c`). V7..V10 were
// introduced in contract-ir WITHOUT extending this gate — a silent gap, no
// deferred record existed — which sealed every mutating command on a V7+
// bundle (WP-598 escalation). Commit `44e4afa0` then extended the gate
// V7..V10 in one step with no per-version audit, outside its package's
// declared paths. WP-606 is the retroactive discharge of both: the notes and
// evidence tests below are the audit that earns each admission.
//
// Per-version audit notes (each references its evidence test in this file's
// test module unless stated otherwise):
//
// (V1..V6): the originally audited eras — base grammar, workflows/service
//   values (V2), fenced leases (V3), row policies (V4; evidence
//   `row_policy_ir_v4_preserves_ordinary_index_derivation`), bounded
//   collection commands and checked deletes (V5), the distinct
//   indexed-restrict delete outcome (V6). Vector-field specs require IR V6
//   (`SchemaIr::requires_ir_v6`) and therefore sit inside this audited set;
//   vector-typed fields are not index-key components, so they never enter
//   `index_values`. Delete-capable grammar starts at V5
//   (`CommandPlan::requires_ir_v5` — delete bindings), so ADR-0107's
//   delete-aware entry derivation (the `BindingMode::Delete` arm below,
//   emitting `IndexEntryMutationV1::Delete` per index) applies to every
//   admitted era from V5 on; V7+ deletes are pinned by
//   `secret_classified_v8_delete_derives_the_exact_old_index_keys`.
//
// (V7, V7) — current-row event policy anchors (ADR-0116). The era adds an
//   optional per-event `policy_anchor` in the EVENT schema and nothing else.
//   Derivation is correct because this function consumes only entity
//   schemas: `entity.indexes()`, `schema.unique_keys()`, and mutation
//   post-images; event schemas (and their anchors) have no path into
//   `index_values`, entry encoding, or affected-target derivation. Anchors
//   are consumed by the event policy machinery, never at index-derivation
//   time. Evidence: `event_policy_anchor_v7_leaves_index_derivation_exact`
//   (an anchored-event V7 bundle whose mutating command with an anchored
//   emit derives exactly the entity-index entries and nothing more).
//
// (V8, V8) — contextual secret field classification (ADR-0118). The era adds
//   schema-level secret classification and display-surface redaction.
//   ADR-0118 §3 explicitly keeps index participation working without read
//   visibility, and §4 keeps durable storage full-fidelity: stored index
//   entries are at-rest artifacts, not display surfaces, so derivation MUST
//   carry the exact classified bytes — and does, because classification is
//   schema metadata, not a value wrapper; `CanonicalValue` has no secret
//   variant for derivation to mishandle. Failure paths cannot echo entry
//   bytes (`CommandIndexError` debug-renders as `[REDACTED]`). Evidence:
//   `secret_classified_v8_index_entries_carry_exact_bytes_without_wrappers`
//   (byte-identical entry keys against the unclassified twin contract) and
//   `secret_classified_v8_delete_derives_the_exact_old_index_keys` (ADR-0107
//   delete format on a V8 bundle).
//
// (V9, V9) — compiler-owned workflow initialization (ADR-0119 prerequisites;
//   workflow initial states and self-transitions,
//   `WorkflowSchema::requires_ir_v9`). The era changes workflow schemas and
//   creation initialization; entity/index schemas and key codecs are
//   untouched. Compiler-initialized state values arrive in mutation
//   post-images exactly like `set` fields, so derivation reads them through
//   the same `index_values` path. Evidence:
//   `workflow_initialized_v9_create_derives_entries_including_initial_state`
//   (unit level, initial state flowing into an index key) and — coordinator
//   level, the WP-598 acceptance contract — the two un-ignored race
//   schedules in tests/command_semantics/command_concurrency.rs plus the
//   live zero-mutation pin
//   `framework_profile_declared_refusal_commits_terminally_on_the_v9_bundle`.
//
// (V10, V10) — the operator-only reimport invocation class (ADR-0119). The
//   era adds `CommandPlan::invocation_class`; entity/index schemas are
//   untouched, so ordinary application commands on a V10 bundle derive
//   exactly as on V9 (evidence:
//   `reimport_era_v10_application_commands_derive_ordinarily`). Reimport
//   commands deliberately COMMIT through ordinary commit semantics
//   (ADR-0119: the coordinator remains the only sequencer), but they cannot
//   REACH ordinary derivation through the application path: IR validation
//   forces server-derived idempotency and a create-only collection shape
//   (`validate_reimport_shape`), `CommandExecutionPreparation::new` refuses
//   any plan without caller-declared idempotency
//   (tests/command_preparation.rs
//   `reimport_authority_and_server_identity_cannot_enter_the_ordinary_constructor`,
//   `ordinary_command_authority_cannot_invoke_a_hidden_reimport_plan`), and
//   the service layer never resolves reimport plans for application calls
//   (riffdb-service architecture pin on the `!plan.is_reimport()` filter).
//   The V10 evidence test additionally pins the reimport plan's structural
//   guarantees (Reimport class, no idempotency input, create-only bindings)
//   at this crate's boundary.
const fn index_derivation_version_supported(grammar: u32, ir: u32) -> bool {
    matches!(
        (grammar, ir),
        (GRAMMAR_VERSION_V1, EXECUTABLE_IR_VERSION_V1)
            | (GRAMMAR_VERSION_V2, EXECUTABLE_IR_VERSION_V2)
            | (GRAMMAR_VERSION_V3, EXECUTABLE_IR_VERSION_V3)
            | (GRAMMAR_VERSION_V4, EXECUTABLE_IR_VERSION_V4)
            | (GRAMMAR_VERSION_V5, EXECUTABLE_IR_VERSION_V5)
            | (GRAMMAR_VERSION_V6, EXECUTABLE_IR_VERSION_V6)
            | (GRAMMAR_VERSION_V7, EXECUTABLE_IR_VERSION_V7)
            | (GRAMMAR_VERSION_V8, EXECUTABLE_IR_VERSION_V8)
            | (GRAMMAR_VERSION_V9, EXECUTABLE_IR_VERSION_V9)
            | (GRAMMAR_VERSION_V10, EXECUTABLE_IR_VERSION_V10)
    )
}

fn derive_grammar_v1_indexes(
    resolved: &ResolvedExecutablePlan,
    normalized_input: &CanonicalRecord,
    evaluated: &EvaluatedCommand,
    current: &TransactionCurrentState,
    mutation_positions: &[Option<usize>],
    command_partition: &PartitionKey,
) -> Result<DerivedCommandIndexes, CommandIndexError> {
    let bundle = resolved.bundle().bundle();
    let plan = resolved.plan();
    let request = evaluated.validation_request();
    let facts = derive_input_command_facts(plan, normalized_input.clone())
        .map_err(|_| CommandIndexError::internal_defect())?;
    if !index_derivation_version_supported(bundle.grammar_version(), bundle.ir_version())
        || plan.execution_class() != ExecutionClass::IdempotentMutation
        || resolved.reference() != evaluated.plan()
        || request.plan() != evaluated.plan()
        || evaluated.mutations().is_empty()
        || facts.binding_entity_keys().len() != request.binding_targets().len()
        || facts.binding_entity_keys().len() != current.bindings().len()
        || facts.binding_entity_keys().len() != mutation_positions.len()
        || facts.root_validation_entity_keys().len() != request.root_validation_targets().len()
        || facts.root_validation_entity_keys().len() != current.root_validations().len()
    {
        return Err(CommandIndexError::internal_defect());
    }
    for (slot, (((plan_index, key), target), observation)) in facts
        .root_validation_plan_indices()
        .iter()
        .zip(facts.root_validation_entity_keys())
        .zip(request.root_validation_targets())
        .zip(current.root_validations())
        .enumerate()
    {
        let read = plan
            .root_validation_reads()
            .get(*plan_index as usize)
            .ok_or_else(CommandIndexError::internal_defect)?;
        if facts.root_validation_element_ordinals().get(slot).is_none()
            || read.entity_type() != target.entity_type_id()
            || target.key() != key
            || observation.target() != target
        {
            return Err(CommandIndexError::internal_defect());
        }
    }

    let mut builder = IndexDerivationBuilder::new(
        request.binding_targets().len(),
        request.root_validation_targets().len(),
    )?;
    let schema_binding = DurableKeySchemaBindingV1::from_plan(resolved.reference());
    let empty_covered =
        CanonicalRecord::new(Vec::new()).map_err(|_| CommandIndexError::internal_defect())?;

    for (binding_position, plan_index) in facts.binding_plan_indices().iter().enumerate() {
        let Some(mutation_position) = mutation_positions[binding_position] else {
            continue;
        };
        let binding = plan
            .bindings()
            .get(*plan_index as usize)
            .ok_or_else(CommandIndexError::internal_defect)?;
        let mutation = &evaluated.mutations()[mutation_position];
        let entity = bundle
            .schema()
            .entity(binding.entity_type())
            .ok_or_else(CommandIndexError::internal_defect)?;
        if binding.key_schema() != entity.primary_key() {
            return Err(CommandIndexError::internal_defect());
        }

        let current_record = match (binding.mode(), &current.bindings()[binding_position]) {
            (BindingMode::Create, EntityObservation::Absent(_)) => None,
            (BindingMode::Mutate | BindingMode::Delete, EntityObservation::Present(record)) => {
                Some(record.fields())
            }
            (
                BindingMode::Read | BindingMode::Create | BindingMode::Mutate | BindingMode::Delete,
                _,
            ) => {
                return Err(CommandIndexError::internal_defect());
            }
        };
        for index in entity.indexes() {
            if binding.mode() == BindingMode::Delete {
                let old_values = index_values(
                    index,
                    current_record.ok_or_else(CommandIndexError::internal_defect)?,
                )?;
                let old_key = index
                    .key_schema()
                    .encode_index(&old_values, mutation.target().key().clone())
                    .map_err(|_| CommandIndexError::internal_defect())?;
                builder.push_entry(IndexEntryMutationV1::Delete(old_key))?;
                insert_generation(index, &old_values, command_partition, &mut builder)?;
                continue;
            }
            let is_unique = bundle
                .schema()
                .unique_keys()
                .iter()
                .any(|unique| unique.index_id() == index.id());
            let new_values = index_values(index, mutation.post_image().fields())?;
            let new_key = index
                .key_schema()
                .encode_index(&new_values, mutation.target().key().clone())
                .map_err(|_| CommandIndexError::internal_defect())?;
            match current_record {
                None => {
                    if is_unique {
                        insert_unique_target(
                            index,
                            &new_values,
                            command_partition,
                            new_key.clone(),
                            &mut builder,
                        )?;
                    }
                    builder.push_entry(IndexEntryMutationV1::Put(
                        StoredIndexEntryV2::new(
                            new_key,
                            schema_binding.clone(),
                            empty_covered.clone(),
                            command_partition.clone(),
                        )
                        .map_err(|_| CommandIndexError::internal_defect())?,
                    ))?;
                    insert_generation(index, &new_values, command_partition, &mut builder)?;
                }
                Some(record) => {
                    let old_values = index_values(index, record)?;
                    let old_key = index
                        .key_schema()
                        .encode_index(&old_values, mutation.target().key().clone())
                        .map_err(|_| CommandIndexError::internal_defect())?;
                    if old_key == new_key {
                        continue;
                    }
                    if is_unique {
                        insert_unique_target(
                            index,
                            &new_values,
                            command_partition,
                            new_key.clone(),
                            &mut builder,
                        )?;
                    }
                    builder.push_entry(IndexEntryMutationV1::Delete(old_key))?;
                    builder.push_entry(IndexEntryMutationV1::Put(
                        StoredIndexEntryV2::new(
                            new_key,
                            schema_binding.clone(),
                            empty_covered.clone(),
                            command_partition.clone(),
                        )
                        .map_err(|_| CommandIndexError::internal_defect())?,
                    ))?;
                    insert_generation(index, &old_values, command_partition, &mut builder)?;
                    insert_generation(index, &new_values, command_partition, &mut builder)?;
                }
            }
        }
    }
    builder.finish()
}

fn insert_unique_target(
    index: &IndexSchema,
    values: &[CanonicalValue],
    command_partition: &PartitionKey,
    expected_entry: IndexEntryKey,
    builder: &mut IndexDerivationBuilder,
) -> Result<(), CommandIndexError> {
    let prefix = index
        .key_schema()
        .encode_index_prefix(values)
        .map_err(|_| CommandIndexError::internal_defect())?;
    let mut storage_prefix = IndexRangePrefixBuilder::new(index.id());
    for (component, value) in index.key_schema().components().iter().zip(values) {
        push_storage_prefix_component(&mut storage_prefix, component.codec(), value)?;
    }
    let storage = storage_prefix.finish();
    if storage.as_bytes() != prefix.as_bytes() {
        return Err(CommandIndexError::internal_defect());
    }
    builder.insert_unique(
        UniqueIndexTarget::new(
            IndexRangeTarget::new(command_partition.clone(), storage),
            expected_entry,
        )
        .map_err(|_| CommandIndexError::internal_defect())?,
    )
}

fn index_values(
    index: &IndexSchema,
    record: &CanonicalRecord,
) -> Result<Vec<CanonicalValue>, CommandIndexError> {
    riffdb_contract_ir::encode_operational_index_values_v1(index, record)
        .map_err(|_| CommandIndexError::internal_defect())
}

fn insert_generation(
    index: &IndexSchema,
    values: &[CanonicalValue],
    partition: &PartitionKey,
    builder: &mut IndexDerivationBuilder,
) -> Result<(), CommandIndexError> {
    if values.len() != index.key_schema().components().len() {
        return Err(CommandIndexError::internal_defect());
    }
    let mut storage_prefix = IndexRangePrefixBuilder::new(index.id());
    for (component, value) in index.key_schema().components().iter().zip(values) {
        push_storage_prefix_component(&mut storage_prefix, component.codec(), value)?;
    }
    let ir_prefix = index
        .key_schema()
        .encode_index_prefix(values)
        .map_err(|_| CommandIndexError::internal_defect())?;
    let storage = storage_prefix.finish();
    if storage.index_id() != ir_prefix.index_id() || storage.as_bytes() != ir_prefix.as_bytes() {
        return Err(CommandIndexError::internal_defect());
    }
    builder.insert_target(PartitionIndexTarget::new(partition.clone(), index.id()))
}

fn push_storage_prefix_component(
    builder: &mut IndexRangePrefixBuilder,
    codec: riffdb_contract_ir::KeyComponentCodecV1,
    value: &CanonicalValue,
) -> Result<(), CommandIndexError> {
    if codec == riffdb_contract_ir::KeyComponentCodecV1::OrderedBytes {
        let CanonicalValue::Bytes(value) = value else {
            return Err(CommandIndexError::internal_defect());
        };
        return builder
            .push_ordered_bytes(value.as_bytes())
            .map(|_| ())
            .map_err(|_| CommandIndexError::internal_defect());
    }
    let result = match value {
        CanonicalValue::Bool(value) => builder.push_bool(*value),
        CanonicalValue::I64(value) => builder.push_i64(*value),
        CanonicalValue::U64(value) => builder.push_u64(*value),
        CanonicalValue::String(value) => builder.push_str(value.as_str()),
        CanonicalValue::Bytes(value) => builder.push_bytes(value.as_bytes()),
        CanonicalValue::Timestamp(value) => builder.push_timestamp(*value),
        CanonicalValue::Date(value) => builder.push_date(*value),
        CanonicalValue::Uuid(value) => builder.push_uuid(value),
        CanonicalValue::Enum { variant_id, .. } => builder.push_enum_variant(*variant_id),
        CanonicalValue::Null
        | CanonicalValue::Decimal(_)
        | CanonicalValue::Money(_)
        | CanonicalValue::List(_)
        | CanonicalValue::Record(_)
        | CanonicalValue::Vector(_) => return Err(CommandIndexError::internal_defect()),
    };
    result
        .map(|_| ())
        .map_err(|_| CommandIndexError::internal_defect())
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use riffdb_catalog::ValidatedContractBundle;
    use riffdb_contract_compiler::compile_contract_source;
    use riffdb_contract_ir::{EntitySchema, RecordSchema};
    use riffdb_invariant::derive_input_command_facts;
    use riffdb_runtime::{ExecutionResult, TransactionContext, execute_command};
    use riffdb_storage_api::{
        CommandWriteClassBreakdownV1, DurableCodecErrorKind, EntityTarget, EvaluationBudget,
        ExecutablePlanRef, IdempotencyIdentity, IdempotencyKeyDigest, MAX_STAGED_WRITE_BYTES,
        PreEvaluationCommitContext, ReadSnapshot, SnapshotRequest,
        StoredAdmittedProvenanceClaimsV1, StoredEntityRecordV1, StoredPendingAdmissionV1,
    };
    use riffdb_types::{
        ActorId, ActorKind, AdmittedActorContext, AggregateTypeId, CanonicalInputHash,
        CanonicalList, CanonicalValue, DatabaseId, Date, DigestKeyId, EntityVersion, Environment,
        FieldId, IndexEntryKeyBuilder, IndexId, LogicalTime, MAX_CANONICAL_DOCUMENT_BYTES,
        PartitionKeyBuilder, ProvenanceId, RequestId, TenantScope, Timestamp, hash_partition_key,
    };

    use super::*;

    // WP-606 boundary pin. A synthetic (V11, V11) bundle is not constructible
    // through any public path — `ContractIrBundle` construction validates the
    // version tuple against the same closed list — so the boundary is pinned
    // exhaustively on the audit gate itself: the supported set is EXACTLY the
    // identity pairs of the audited eras. Admitting a new era must flip this
    // test deliberately, alongside a fresh per-version audit note above the
    // gate.
    #[test]
    fn index_derivation_admits_exactly_the_audited_identity_pairs() {
        for grammar in 0..=16_u32 {
            for ir in 0..=16_u32 {
                let audited_identity_pair = grammar == ir
                    && grammar >= GRAMMAR_VERSION_V1
                    && grammar <= GRAMMAR_VERSION_V10;
                assert_eq!(
                    index_derivation_version_supported(grammar, ir),
                    audited_identity_pair,
                    "gate must admit exactly the audited identity pairs; \
                     diverged at ({grammar}, {ir})"
                );
            }
        }
    }

    const INDEXED_SOURCE: &str = r#"
contract IndexedRows version 1 {
  entity Row {
    key (id: uuid)
    field tenant: uuid
    field category: string<32>
    field score: i64
    index by_tenant_category (tenant, category)
    index by_score (score)
  }
  aggregate Rows { root Row partition_by id conflict_key (id) }
  command CreateRow {
    input request_key: string<128>
    input id: uuid
    input tenant: uuid
    input category: string<32>
    input score: i64
    idempotency_key request_key
    create Row(id) as row else AlreadyExists {}
    set row.tenant = tenant
    set row.category = category
    set row.score = score
    return Created { row: row }
  }
  command ChangeRow {
    input request_key: string<128>
    input id: uuid
    input tenant: uuid
    input category: string<32>
    input score: i64
    idempotency_key request_key
    mutate Row(id) as row else Missing {}
    set row.tenant = tenant
    set row.category = category
    set row.score = score
    return Changed { row: row }
  }
}
"#;

    const ROW_POLICY_INDEXED_SOURCE: &str = r#"
contract PolicyIndexedRows version 1 {
  entity Row {
    key (id: uuid)
    field tenant: uuid
    field category: string<32>
    field score: i64
    index by_tenant_category (tenant, category)
    index by_score (score)
  }
  aggregate Rows { root Row partition_by id conflict_key (id) }
  row policy RowAccess on Row {
    allow read when tenant == principal.id
    allow create when tenant == principal.id
    allow update when tenant == principal.id
    allow delete when tenant == principal.id
  }
  command CreateRow {
    input request_key: string<128>
    input id: uuid
    input tenant: uuid
    input category: string<32>
    input score: i64
    idempotency_key request_key
    create Row(id) as row else AlreadyExists {}
    set row.tenant = tenant
    set row.category = category
    set row.score = score
    return Created { row: row }
  }
}
"#;

    const SCALAR_SOURCE: &str = r#"
contract ScalarPrefixes version 1 {
  enum State { Ready, Stopped }
  entity Scalar {
    key (id: uuid)
    field flag: bool
    field unsigned: u64
    field signed: i64
    field at: timestamp
    field day: date
    field status: State
    field other_id: uuid
    field blob: bytes<16>
    field text: string<16>
    index by_all (flag, unsigned, signed, at, day, status, other_id, blob, text)
  }
}
"#;

    const OPERATIONAL_INDEX_SOURCE: &str = r#"
contract OperationalIndexes version 1 {
  entity Document {
    key (id: uuid)
    field deleted_at: optional<timestamp>
    field title: string<32>
    index by_deleted (deleted_at, id) presence(deleted_at)
    index by_title (title, id) text_key(title, binary_utf8_v1)
  }
}
"#;

    const RESTRICT_DELETE_SOURCE: &str = r#"
contract DeleteRestrict version 1 {
  entity Parent {
    key (tenant_id: uuid, parent_id: uuid)
    delete_policy restrict Child.by_parent
  }
  entity Child {
    key (tenant_id: uuid, parent_id: uuid, child_id: uuid)
    index by_parent (tenant_id, parent_id)
    reference parent (tenant_id, parent_id) -> Parent(tenant_id, parent_id)
  }
  aggregate Owned {
    root Parent
    child Child
    partition_by tenant_id
    conflict_key (tenant_id)
  }
  bulk command DeleteParents {
    input request_id: uuid
    input tenant_id: uuid
    input parent_ids: list<uuid, 1..8>
    idempotency_key request_id
    for parent_id in parent_ids {
      delete Parent(tenant_id, parent_id) as parent else Missing {} restrict Referenced {}
    }
    return Deleted {}
  }
}
"#;

    struct Fixture {
        resolved: ResolvedExecutablePlan,
        input: CanonicalRecord,
        evaluated: EvaluatedCommand,
        intent: CommitIntent,
        current: TransactionCurrentState,
        partition: PartitionKey,
    }

    fn uuid_bytes(fill: u8) -> [u8; 16] {
        let mut bytes = [fill; 16];
        bytes[6] = 0x70 | (fill & 0x0f);
        bytes[8] = 0x80 | (fill & 0x3f);
        bytes
    }

    fn fixture(
        command_name: &str,
        request_key: &str,
        next: ([u8; 16], &str, i64),
        old: Option<([u8; 16], &str, i64)>,
    ) -> Fixture {
        fixture_from_source(INDEXED_SOURCE, command_name, request_key, next, old)
    }

    fn fixture_from_source(
        source: &str,
        command_name: &str,
        request_key: &str,
        next: ([u8; 16], &str, i64),
        old: Option<([u8; 16], &str, i64)>,
    ) -> Fixture {
        let compiled = compile_contract_source(source).expect("indexed source compiles");
        let bundle = ValidatedContractBundle::from_compiler_bundle(compiled)
            .expect("indexed bundle validates");
        let plan = bundle
            .bundle()
            .commands()
            .iter()
            .find(|candidate| candidate.name() == command_name)
            .expect("fixture command");
        let reference = ExecutablePlanRef::new(
            bundle.lineage().clone(),
            bundle.contract_version(),
            bundle.bundle_hash(),
            plan.command_id(),
            plan.plan_hash(),
        );
        let input = named_record(
            plan.input().record(),
            &[
                ("request_key", string(request_key)),
                ("id", CanonicalValue::Uuid([0x11; 16])),
                ("tenant", CanonicalValue::Uuid(next.0)),
                ("category", string(next.1)),
                ("score", CanonicalValue::I64(next.2)),
            ],
        );
        let facts = derive_input_command_facts(plan, input.clone()).expect("input facts");
        let target = EntityTarget::new(
            plan.bindings()[0].entity_type(),
            facts.binding_entity_keys()[0].clone(),
        )
        .expect("binding target");
        let entity = bundle
            .bundle()
            .schema()
            .entity(target.entity_type_id())
            .expect("row schema");
        let observation = old.map_or_else(
            || EntityObservation::Absent(target.clone()),
            |old| {
                EntityObservation::Present(
                    StoredEntityRecordV1::new(
                        target.clone(),
                        EntityVersion::first(),
                        plan.contract_version(),
                        DurableKeySchemaBindingV1::from_plan(&reference),
                        entity_record(
                            entity,
                            &target,
                            &[
                                ("tenant", CanonicalValue::Uuid(old.0)),
                                ("category", string(old.1)),
                                ("score", CanonicalValue::I64(old.2)),
                            ],
                        ),
                    )
                    .expect("stored row"),
                )
            },
        );
        let request = SnapshotRequest::new(reference.clone(), vec![target], Vec::new(), Vec::new())
            .expect("snapshot request");
        let snapshot = ReadSnapshot::new(
            &request,
            None,
            vec![observation.clone()],
            Vec::new(),
            Vec::new(),
        )
        .expect("snapshot");
        let actor = AdmittedActorContext::new(
            ActorId::new("command-index-test").expect("actor"),
            ActorKind::Service,
            TenantScope::Global,
            None,
        );
        let logical_time = LogicalTime::new(Timestamp::new(100, 2).expect("logical time"));
        let context = TransactionContext::new(
            RequestId::from_unix_milliseconds_and_random(1, [0x31; 10]).expect("request ID"),
            actor.clone(),
            reference.clone(),
            logical_time,
            facts.partition_key().clone(),
        );
        let ExecutionResult::CommitRequired(evaluated) = execute_command(
            bundle.bundle(),
            &input,
            &snapshot,
            &context,
            EvaluationBudget::v1(),
        )
        .expect("command evaluates") else {
            panic!("fixture command must commit")
        };
        let current = TransactionCurrentState::new(
            evaluated.validation_request(),
            vec![observation],
            Vec::new(),
            Vec::new(),
        )
        .expect("transaction-current state");
        let identity = IdempotencyIdentity::new(
            DatabaseId::from_bytes(uuid_bytes(0x41)).expect("database ID"),
            Environment::new("test").expect("environment"),
            TenantScope::Global,
            actor.principal_id().clone(),
            reference.contract_lineage().clone(),
            reference.command_id(),
            IdempotencyKeyDigest::from_hmac_bytes(
                DigestKeyId::new(1).expect("digest key ID"),
                [0x42; 32],
            ),
        );
        let pending = StoredPendingAdmissionV1::new(
            identity,
            CanonicalInputHash::from_bytes([0x43; 32]),
            RequestId::from_bytes(uuid_bytes(0x44)).expect("admission request ID"),
            reference.clone(),
            logical_time,
            actor,
            facts.partition_key().clone(),
            StoredAdmittedProvenanceClaimsV1::default(),
        )
        .expect("pending admission");
        let pre_evaluation = PreEvaluationCommitContext::new(
            pending,
            hash_partition_key(facts.partition_key().as_bytes()),
            Vec::new(),
        )
        .expect("pre-evaluation context");
        let intent = CommitIntent::new(
            pre_evaluation,
            evaluated.clone(),
            ProvenanceId::from_bytes(uuid_bytes(0x45)).expect("provenance ID"),
        )
        .expect("commit intent");
        Fixture {
            resolved: crate::test_support::resolve_genesis_plan(&bundle, &reference)
                .expect("resolved plan"),
            input,
            evaluated,
            intent,
            current,
            partition: facts.partition_key().clone(),
        }
    }

    fn named_record(schema: &RecordSchema, supplied: &[(&str, CanonicalValue)]) -> CanonicalRecord {
        let supplied = supplied.iter().cloned().collect::<BTreeMap<_, _>>();
        CanonicalRecord::new(
            schema
                .fields()
                .iter()
                .map(|field| {
                    (
                        field.id(),
                        supplied
                            .get(field.name())
                            .cloned()
                            .unwrap_or_else(|| panic!("missing field {}", field.name())),
                    )
                })
                .collect(),
        )
        .expect("canonical named record")
    }

    fn entity_record(
        entity: &EntitySchema,
        target: &EntityTarget,
        supplied: &[(&str, CanonicalValue)],
    ) -> CanonicalRecord {
        let supplied = supplied.iter().cloned().collect::<BTreeMap<_, _>>();
        let key_values = entity
            .primary_key()
            .decode_entity(target.key())
            .expect("entity key");
        CanonicalRecord::new(
            entity
                .record()
                .fields()
                .iter()
                .map(|field| {
                    let value = entity
                        .primary_key_fields()
                        .iter()
                        .position(|candidate| *candidate == field.id())
                        .map(|position| key_values[position].clone())
                        .or_else(|| supplied.get(field.name()).cloned())
                        .unwrap_or_else(|| panic!("missing entity field {}", field.name()));
                    (field.id(), value)
                })
                .collect(),
        )
        .expect("canonical entity record")
    }

    fn string(value: &str) -> CanonicalValue {
        CanonicalValue::string(value).expect("bounded string")
    }

    fn index<'a>(fixture: &'a Fixture, name: &str) -> &'a IndexSchema {
        fixture.resolved.bundle().bundle().schema().entities()[0]
            .indexes()
            .iter()
            .find(|index| index.name() == name)
            .expect("named index")
    }

    fn expected_generation_ids(indexes: &[&IndexSchema]) -> BTreeSet<IndexId> {
        indexes.iter().map(|index| index.id()).collect()
    }

    fn actual_generation_ids(derived: &DerivedCommandIndexes) -> BTreeSet<IndexId> {
        derived
            .affected_targets
            .as_slice()
            .iter()
            .map(PartitionIndexTarget::index_id)
            .collect()
    }

    fn actual_entry_kinds(derived: &DerivedCommandIndexes) -> BTreeSet<(Vec<u8>, bool)> {
        derived
            .entry_mutations
            .iter()
            .map(|mutation| {
                (
                    mutation.key().as_bytes().to_vec(),
                    matches!(mutation, IndexEntryMutationV1::Put(_)),
                )
            })
            .collect()
    }

    fn synthetic_index_entries(
        fixture: &Fixture,
        count: u64,
        covered_payload_bytes: usize,
    ) -> Vec<IndexEntryMutationV1> {
        let covered_values = CanonicalRecord::new(vec![(
            FieldId::first(),
            CanonicalValue::bytes(vec![0x5a; covered_payload_bytes])
                .expect("bounded covered bytes"),
        )])
        .expect("bounded covered record");
        let entity_key = fixture.intent.evaluated().mutations()[0]
            .target()
            .key()
            .clone();
        let mut entries = (0..count)
            .map(|ordinal| {
                let mut key = IndexEntryKeyBuilder::new(IndexId::new(17).expect("index ID"));
                key.push_u64(ordinal).expect("unique index component");
                let key = key.finish(entity_key.clone()).expect("synthetic index key");
                StoredIndexEntryV2::new(
                    key,
                    DurableKeySchemaBindingV1::from_plan(fixture.intent.evaluated().plan()),
                    covered_values.clone(),
                    fixture.intent.pending().partition_key().clone(),
                )
                .map(IndexEntryMutationV1::Put)
                .expect("synthetic index entry")
            })
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| left.key().as_bytes().cmp(right.key().as_bytes()));
        entries
    }

    fn validate_synthetic_shape(
        fixture: &Fixture,
        entries: Vec<IndexEntryMutationV1>,
    ) -> Result<ValidatedCommandWriteSetShapeV1, riffdb_storage_api::StorageValueError> {
        let affected_targets =
            AffectedIndexEpochTargets::new(Vec::new()).expect("empty affected targets");
        let affected_current = AffectedEpochCurrentState::new(&affected_targets, Vec::new())
            .expect("empty affected current");
        ValidatedCommandWriteSetShapeV1::new(
            &fixture.intent,
            affected_targets,
            affected_current,
            entries,
            Vec::new(),
        )
    }

    fn prepare_synthetic_write_set(
        fixture: &Fixture,
        entries: Vec<IndexEntryMutationV1>,
    ) -> SequenceFreeWriteSetPreparation {
        let affected_targets =
            AffectedIndexEpochTargets::new(Vec::new()).expect("empty affected targets");
        let affected_current = AffectedEpochCurrentState::new(&affected_targets, Vec::new())
            .expect("empty affected current");
        prepare_sequence_free_write_set(
            &fixture.intent,
            affected_targets,
            affected_current,
            entries,
            Vec::new(),
        )
    }

    #[test]
    fn semantic_aggregate_failure_preempts_encoded_capacity_classification() {
        const CANONICAL_RECORD_OVERHEAD: usize = 16;

        let fixture = fixture("CreateRow", "semantic-over", ([0x21; 16], "new", 10), None);
        let pending_before = fixture.intent.pending().clone();
        let entries = synthetic_index_entries(
            &fixture,
            17,
            MAX_CANONICAL_DOCUMENT_BYTES - CANONICAL_RECORD_OVERHEAD,
        );

        assert_eq!(
            validate_synthetic_shape(&fixture, entries.clone())
                .expect_err("semantic aggregate must reject the complete valid shape"),
            riffdb_storage_api::StorageValueError::LimitExceeded
        );
        assert!(matches!(
            command_write_set_upper_bound_v1(&fixture.intent, &entries, &[]),
            Ok(EncodedWriteSetUpperBoundResultV1::ExceedsAcceptedAggregateCap(_))
        ));
        assert!(matches!(
            prepare_synthetic_write_set(&fixture, entries),
            SequenceFreeWriteSetPreparation::Integrity
        ));
        assert_eq!(fixture.intent.pending(), &pending_before);
    }

    #[test]
    fn event_reference_trims_keep_the_largest_semantic_fit_within_encoded_capacity() {
        const ENTRY_COUNT: u64 = 17;
        const CANONICAL_RECORD_OVERHEAD: usize = 16;

        let fixture = fixture("CreateRow", "encoded-over", ([0x21; 16], "new", 10), None);
        let maximum_payload = MAX_CANONICAL_DOCUMENT_BYTES - CANONICAL_RECORD_OVERHEAD;
        let mut valid = 0usize;
        let mut invalid = maximum_payload + 1;
        while valid + 1 < invalid {
            let candidate = valid + (invalid - valid) / 2;
            if validate_synthetic_shape(
                &fixture,
                synthetic_index_entries(&fixture, ENTRY_COUNT, candidate),
            )
            .is_ok()
            {
                valid = candidate;
            } else {
                invalid = candidate;
            }
        }

        let entries = synthetic_index_entries(&fixture, ENTRY_COUNT, valid);
        let shape = validate_synthetic_shape(&fixture, entries.clone())
            .expect("largest tuned semantic shape fits");
        assert!(shape.semantic_classes().total().expect("checked total") <= MAX_STAGED_WRITE_BYTES);
        assert!(matches!(
            command_write_set_upper_bound_v1(
                shape.intent(),
                shape.index_entries(),
                shape.index_epochs(),
            ),
            Ok(EncodedWriteSetUpperBoundResultV1::Fits(_))
        ));
        drop(shape);

        let pending_before = fixture.intent.pending().clone();
        assert!(matches!(
            prepare_synthetic_write_set(&fixture, entries),
            SequenceFreeWriteSetPreparation::Ready(_)
        ));
        assert_eq!(fixture.intent.pending(), &pending_before);
    }

    #[test]
    fn fitting_sizing_is_retained_and_codec_errors_remain_integrity() {
        let classes = CommandWriteClassBreakdownV1::new(1, 0, 0, 0, 0, 0, 0, 0, 0, 0)
            .expect("one-byte class breakdown");
        let bound = EncodedWriteSetUpperBound::new(classes).expect("nonzero bound");
        let SequenceFreeWriteSetSizing::Fits(retained) = classify_sequence_free_write_set_sizing(
            Ok(EncodedWriteSetUpperBoundResultV1::Fits(bound)),
        ) else {
            panic!("fitting bound must be retained")
        };
        assert_eq!(retained, bound);

        for kind in [
            DurableCodecErrorKind::IncompatibleFormat,
            DurableCodecErrorKind::CorruptData,
            DurableCodecErrorKind::LimitExceeded,
            DurableCodecErrorKind::InvariantViolation,
            DurableCodecErrorKind::ReservationExceeded,
            DurableCodecErrorKind::UnexpectedRecordType,
        ] {
            assert!(matches!(
                classify_sequence_free_write_set_sizing(Err(DurableCodecError::new(kind))),
                SequenceFreeWriteSetSizing::Integrity
            ));
        }
    }

    #[test]
    fn create_puts_each_index_with_empty_covered_values_and_all_new_prefixes() {
        let fixture = fixture("CreateRow", "create-1", ([0x21; 16], "new", 10), None);
        let derived = derive_grammar_v1_indexes(
            &fixture.resolved,
            &fixture.input,
            &fixture.evaluated,
            &fixture.current,
            &[Some(0)],
            &fixture.partition,
        )
        .expect("create indexes");
        assert_eq!(derived.entry_mutations.len(), 2);
        assert!(
            derived
                .entry_mutations
                .windows(2)
                .all(|pair| { pair[0].key().as_bytes() < pair[1].key().as_bytes() })
        );
        for mutation in &derived.entry_mutations {
            let IndexEntryMutationV1::Put(record) = mutation else {
                panic!("create must only put")
            };
            assert!(record.covered_values().is_empty());
            assert_eq!(record.partition_key(), &fixture.partition);
            assert!(
                record
                    .schema_binding()
                    .matches_plan(fixture.resolved.reference())
            );
        }
        let tenant = index(&fixture, "by_tenant_category");
        let score = index(&fixture, "by_score");
        let entity_key = fixture.evaluated.mutations()[0].target().key().clone();
        assert_eq!(
            actual_entry_kinds(&derived),
            BTreeSet::from([
                (
                    tenant
                        .key_schema()
                        .encode_index(
                            &[CanonicalValue::Uuid([0x21; 16]), string("new")],
                            entity_key.clone(),
                        )
                        .expect("tenant index key")
                        .as_bytes()
                        .to_vec(),
                    true,
                ),
                (
                    score
                        .key_schema()
                        .encode_index(&[CanonicalValue::I64(10)], entity_key)
                        .expect("score index key")
                        .as_bytes()
                        .to_vec(),
                    true,
                ),
            ])
        );
        assert_eq!(
            actual_generation_ids(&derived),
            expected_generation_ids(&[tenant, score])
        );
        assert_eq!(derived.affected_targets.as_slice().len(), 2);
        assert!(
            derived
                .affected_targets
                .as_slice()
                .iter()
                .all(|target| target.partition_key() == &fixture.partition)
        );
    }

    #[test]
    fn row_policy_ir_v4_preserves_ordinary_index_derivation() {
        let fixture = fixture_from_source(
            ROW_POLICY_INDEXED_SOURCE,
            "CreateRow",
            "policy-create-1",
            ([0x21; 16], "new", 10),
            None,
        );
        assert_eq!(
            fixture.resolved.bundle().bundle().grammar_version(),
            GRAMMAR_VERSION_V4
        );
        assert_eq!(
            fixture.resolved.bundle().bundle().ir_version(),
            EXECUTABLE_IR_VERSION_V4
        );
        let derived = derive_grammar_v1_indexes(
            &fixture.resolved,
            &fixture.input,
            &fixture.evaluated,
            &fixture.current,
            &[Some(0)],
            &fixture.partition,
        )
        .expect("row-policy contracts retain ordinary index semantics");
        assert_eq!(derived.entry_mutations.len(), 2);
    }

    // WP-606 V7 audit evidence: the era adds event policy anchors (ADR-0116)
    // in the EVENT schema only. Entity index derivation must be untouched by
    // an anchored emit in the same mutating command.
    const ANCHORED_EVENT_INDEXED_SOURCE: &str = r#"
contract AnchoredEventRows version 1 {
  entity Row {
    key (id: uuid)
    field tenant: uuid
    field category: string<32>
    field score: i64
    index by_tenant_category (tenant, category)
    index by_score (score)
  }
  event RowTouched {
    partition_by (id)
    policy_anchor current Row(id: id)
    id: uuid
  }
  aggregate Rows { root Row partition_by id conflict_key (id) }
  row policy RowAccess on Row {
    allow read when true
    allow create when true
  }
  command CreateRow {
    input request_key: string<128>
    input id: uuid
    input tenant: uuid
    input category: string<32>
    input score: i64
    idempotency_key request_key
    create Row(id) as row else AlreadyExists {}
    set row.tenant = tenant
    set row.category = category
    set row.score = score
    emit RowTouched { id: id }
    return Created { row: row }
  }
}
"#;

    #[test]
    fn event_policy_anchor_v7_leaves_index_derivation_exact() {
        let fixture = fixture_from_source(
            ANCHORED_EVENT_INDEXED_SOURCE,
            "CreateRow",
            "anchored-create-1",
            ([0x21; 16], "new", 10),
            None,
        );
        // Non-empty trigger: this bundle must sit exactly at the V7 era.
        assert_eq!(
            fixture.resolved.bundle().bundle().grammar_version(),
            GRAMMAR_VERSION_V7
        );
        assert_eq!(
            fixture.resolved.bundle().bundle().ir_version(),
            EXECUTABLE_IR_VERSION_V7
        );
        assert!(
            fixture
                .resolved
                .bundle()
                .bundle()
                .schema()
                .events()
                .iter()
                .any(|event| event.policy_anchor().is_some()),
            "the V7 evidence bundle must carry an anchored event"
        );
        let derived = derive_grammar_v1_indexes(
            &fixture.resolved,
            &fixture.input,
            &fixture.evaluated,
            &fixture.current,
            &[Some(0)],
            &fixture.partition,
        )
        .expect("anchored-event contracts retain ordinary index derivation");
        let tenant = index(&fixture, "by_tenant_category");
        let score = index(&fixture, "by_score");
        let entity_key = fixture.evaluated.mutations()[0].target().key().clone();
        assert_eq!(
            actual_entry_kinds(&derived),
            BTreeSet::from([
                (
                    tenant
                        .key_schema()
                        .encode_index(
                            &[CanonicalValue::Uuid([0x21; 16]), string("new")],
                            entity_key.clone(),
                        )
                        .expect("tenant index key")
                        .as_bytes()
                        .to_vec(),
                    true,
                ),
                (
                    score
                        .key_schema()
                        .encode_index(&[CanonicalValue::I64(10)], entity_key)
                        .expect("score index key")
                        .as_bytes()
                        .to_vec(),
                    true,
                ),
            ]),
            "the anchored event must contribute zero index entries"
        );
        assert_eq!(
            actual_generation_ids(&derived),
            expected_generation_ids(&[tenant, score])
        );
        assert_eq!(derived.affected_targets.as_slice().len(), 2);
    }

    #[test]
    fn mutate_skips_unchanged_indexes_and_unions_changed_old_and_new_prefixes() {
        let unchanged = fixture(
            "ChangeRow",
            "unchanged-1",
            ([0x21; 16], "old", 10),
            Some(([0x21; 16], "old", 10)),
        );
        let unchanged = derive_grammar_v1_indexes(
            &unchanged.resolved,
            &unchanged.input,
            &unchanged.evaluated,
            &unchanged.current,
            &[Some(0)],
            &unchanged.partition,
        )
        .expect("unchanged indexes");
        assert!(unchanged.entry_mutations.is_empty());
        assert!(unchanged.affected_targets.as_slice().is_empty());

        let changed = fixture(
            "ChangeRow",
            "changed-1",
            ([0x21; 16], "new", 20),
            Some(([0x21; 16], "old", 10)),
        );
        let derived = derive_grammar_v1_indexes(
            &changed.resolved,
            &changed.input,
            &changed.evaluated,
            &changed.current,
            &[Some(0)],
            &changed.partition,
        )
        .expect("changed indexes");
        assert_eq!(derived.entry_mutations.len(), 4);
        assert_eq!(
            derived
                .entry_mutations
                .iter()
                .filter(|mutation| matches!(mutation, IndexEntryMutationV1::Delete(_)))
                .count(),
            2
        );
        assert_eq!(
            derived
                .entry_mutations
                .iter()
                .filter(|mutation| matches!(mutation, IndexEntryMutationV1::Put(_)))
                .count(),
            2
        );
        let tenant = index(&changed, "by_tenant_category");
        let score = index(&changed, "by_score");
        let entity_key = changed.evaluated.mutations()[0].target().key().clone();
        assert!(
            derived
                .entry_mutations
                .windows(2)
                .all(|pair| pair[0].key().as_bytes() < pair[1].key().as_bytes())
        );
        for mutation in &derived.entry_mutations {
            if let IndexEntryMutationV1::Put(record) = mutation {
                assert!(record.covered_values().is_empty());
                assert_eq!(record.partition_key(), &changed.partition);
            }
        }
        assert_eq!(
            actual_entry_kinds(&derived),
            BTreeSet::from([
                (
                    tenant
                        .key_schema()
                        .encode_index(
                            &[CanonicalValue::Uuid([0x21; 16]), string("old")],
                            entity_key.clone(),
                        )
                        .expect("old tenant index key")
                        .as_bytes()
                        .to_vec(),
                    false,
                ),
                (
                    tenant
                        .key_schema()
                        .encode_index(
                            &[CanonicalValue::Uuid([0x21; 16]), string("new")],
                            entity_key.clone(),
                        )
                        .expect("new tenant index key")
                        .as_bytes()
                        .to_vec(),
                    true,
                ),
                (
                    score
                        .key_schema()
                        .encode_index(&[CanonicalValue::I64(10)], entity_key.clone())
                        .expect("old score index key")
                        .as_bytes()
                        .to_vec(),
                    false,
                ),
                (
                    score
                        .key_schema()
                        .encode_index(&[CanonicalValue::I64(20)], entity_key)
                        .expect("new score index key")
                        .as_bytes()
                        .to_vec(),
                    true,
                ),
            ])
        );
        assert_eq!(
            actual_generation_ids(&derived),
            expected_generation_ids(&[tenant, score])
        );
        assert_eq!(derived.affected_targets.as_slice().len(), 2);
    }

    #[test]
    fn storage_prefix_builder_matches_ir_for_every_v1_key_scalar() {
        let compiled = compile_contract_source(SCALAR_SOURCE).expect("scalar source compiles");
        let bundle = ValidatedContractBundle::from_compiler_bundle(compiled)
            .expect("scalar bundle validates");
        let schema = bundle.bundle().schema();
        let index = &schema.entities()[0].indexes()[0];
        let enumeration = &schema.enums()[0];
        let values = vec![
            CanonicalValue::Bool(true),
            CanonicalValue::U64(7),
            CanonicalValue::I64(-8),
            CanonicalValue::Timestamp(Timestamp::new(-9, 10).expect("timestamp")),
            CanonicalValue::Date(Date::new(-11)),
            CanonicalValue::Enum {
                type_id: enumeration.id(),
                variant_id: enumeration.variants()[0].id(),
            },
            CanonicalValue::Uuid([0x12; 16]),
            CanonicalValue::bytes(vec![0, 1, 2]).expect("bytes"),
            string("exact"),
        ];
        let mut builder = IndexDerivationBuilder::new(0, 0).expect("builder");
        let partition = PartitionKeyBuilder::new(AggregateTypeId::first())
            .finish()
            .expect("partition key");
        insert_generation(index, &values, &partition, &mut builder)
            .expect("cross-checked generation");
        let derived = builder.finish().expect("derived generation");
        assert_eq!(derived.affected_targets.as_slice().len(), 1);
        assert_eq!(
            actual_generation_ids(&derived),
            BTreeSet::from([index.id()])
        );
    }

    #[test]
    fn restrict_delete_ranges_are_exact_per_element_reverse_index_prefixes() {
        let compiled =
            compile_contract_source(RESTRICT_DELETE_SOURCE).expect("restrict source compiles");
        let bundle = ValidatedContractBundle::from_compiler_bundle(compiled)
            .expect("restrict bundle validates");
        let plan = bundle
            .bundle()
            .commands()
            .iter()
            .find(|command| command.name() == "DeleteParents")
            .expect("delete command");
        let reference = ExecutablePlanRef::new(
            bundle.lineage().clone(),
            bundle.contract_version(),
            bundle.bundle_hash(),
            plan.command_id(),
            plan.plan_hash(),
        );
        let tenant = CanonicalValue::Uuid(uuid_bytes(0x11));
        let parents = [
            CanonicalValue::Uuid(uuid_bytes(0x21)),
            CanonicalValue::Uuid(uuid_bytes(0x22)),
        ];
        let input = named_record(
            plan.input().record(),
            &[
                ("request_id", CanonicalValue::Uuid(uuid_bytes(0x31))),
                ("tenant_id", tenant.clone()),
                (
                    "parent_ids",
                    CanonicalValue::List(
                        CanonicalList::new(parents.to_vec()).expect("parent list"),
                    ),
                ),
            ],
        );
        let facts = derive_input_command_facts(plan, input).expect("delete input facts");
        let resolved = crate::test_support::resolve_genesis_plan(&bundle, &reference)
            .expect("resolved delete plan");

        let ranges = derive_delete_restrict_ranges(&resolved, &facts)
            .expect("compiler-sealed restrict ranges");

        assert_eq!(ranges.len(), 2);
        let child = bundle
            .bundle()
            .schema()
            .entities()
            .iter()
            .find(|entity| entity.name() == "Child")
            .expect("child entity");
        let index = child
            .indexes()
            .iter()
            .find(|index| index.name() == "by_parent")
            .expect("reverse index");
        for (range, parent) in ranges.iter().zip(parents) {
            let expected = index
                .key_schema()
                .encode_index_prefix(&[tenant.clone(), parent])
                .expect("expected reverse prefix");
            assert_eq!(range.prefix().index_id(), index.id());
            assert_eq!(range.prefix().as_bytes(), expected.as_bytes());
            assert_eq!(
                range.generation_target().partition_key(),
                facts.partition_key()
            );
        }
    }

    #[test]
    fn operational_index_values_and_storage_prefixes_share_the_sealed_codec() {
        let compiled =
            compile_contract_source(OPERATIONAL_INDEX_SOURCE).expect("operational source compiles");
        let bundle = ValidatedContractBundle::from_compiler_bundle(compiled)
            .expect("operational bundle validates");
        let entity = &bundle.bundle().schema().entities()[0];
        let deleted = entity
            .indexes()
            .iter()
            .find(|index| index.name() == "by_deleted")
            .expect("presence index");
        let title = entity
            .indexes()
            .iter()
            .find(|index| index.name() == "by_title")
            .expect("text index");
        let deleted_field = entity
            .record()
            .fields()
            .iter()
            .find(|field| field.name() == "deleted_at")
            .expect("deleted field")
            .id();
        let title_field = entity
            .record()
            .fields()
            .iter()
            .find(|field| field.name() == "title")
            .expect("title field")
            .id();
        let id_field = entity
            .record()
            .fields()
            .iter()
            .find(|field| field.name() == "id")
            .expect("id field")
            .id();
        let id = CanonicalValue::Uuid([9; 16]);
        let explicit_null = CanonicalRecord::new(vec![
            (id_field, id.clone()),
            (deleted_field, CanonicalValue::Null),
        ])
        .expect("explicit-null record");
        let missing =
            CanonicalRecord::new(vec![(id_field, id.clone())]).expect("missing-field record");
        assert_eq!(
            index_values(deleted, &missing).expect("missing discriminator")[0],
            CanonicalValue::U64(riffdb_contract_ir::PRESENCE_MISSING_V1)
        );
        assert_eq!(
            index_values(deleted, &explicit_null).expect("null discriminator")[0],
            CanonicalValue::U64(riffdb_contract_ir::PRESENCE_NULL_V1)
        );

        let record = CanonicalRecord::new(vec![(id_field, id), (title_field, string("a\0title"))])
            .expect("text record");
        let values = index_values(title, &record).expect("ordered text values");
        let mut builder = IndexDerivationBuilder::new(0, 0).expect("builder");
        let partition = PartitionKeyBuilder::new(AggregateTypeId::first())
            .finish()
            .expect("partition key");
        insert_generation(title, &values, &partition, &mut builder)
            .expect("storage prefix must match ordered IR prefix");
        assert_eq!(
            builder
                .finish()
                .expect("derived")
                .affected_targets
                .as_slice()
                .len(),
            1
        );
    }

    #[test]
    fn incremental_guards_reject_duplicate_deltas_and_each_exact_plus_one_bound() {
        let fixture = fixture("CreateRow", "bounds-1", ([0x21; 16], "new", 10), None);
        let generation_target =
            |index_id: IndexId| PartitionIndexTarget::new(fixture.partition.clone(), index_id);
        let derived = derive_grammar_v1_indexes(
            &fixture.resolved,
            &fixture.input,
            &fixture.evaluated,
            &fixture.current,
            &[Some(0)],
            &fixture.partition,
        )
        .expect("fixture indexes");
        let entry = derived.entry_mutations[0].clone();
        let mut duplicate = IndexDerivationBuilder::new(0, 0).expect("builder");
        duplicate.push_entry(entry.clone()).expect("first key");
        assert_eq!(
            duplicate.push_entry(entry.clone()),
            Err(CommandIndexError::internal_defect())
        );

        let mut delta_limit = IndexDerivationBuilder::new(0, 0).expect("builder");
        delta_limit.entry_mutations = vec![entry; MAX_INDEX_DELTAS - 1];
        delta_limit
            .push_entry(derived.entry_mutations[1].clone())
            .expect("exact delta bound");
        assert_eq!(delta_limit.entry_mutations.len(), MAX_INDEX_DELTAS);
        assert_eq!(
            delta_limit.push_entry(derived.entry_mutations[1].clone()),
            Err(CommandIndexError::internal_defect())
        );

        assert!(IndexDerivationBuilder::new(MAX_VALIDATION_TARGETS, 0).is_ok());
        assert_eq!(
            IndexDerivationBuilder::new(MAX_VALIDATION_TARGETS, 1)
                .err()
                .expect("positions over limit"),
            CommandIndexError::internal_defect()
        );
        let mut position_limit =
            IndexDerivationBuilder::new(MAX_VALIDATION_TARGETS, 0).expect("exact positions");
        assert_eq!(
            position_limit.insert_target(generation_target(IndexId::first())),
            Err(CommandIndexError::internal_defect())
        );

        let mut target_limit = IndexDerivationBuilder::new(0, 0).expect("builder");
        for raw in 1..=u32::try_from(MAX_AFFECTED_INDEX_EPOCH_TARGETS).expect("u32 bound") {
            target_limit
                .insert_target(generation_target(IndexId::new(raw).expect("index ID")))
                .expect("exact affected-target bound");
        }
        assert_eq!(
            target_limit.affected_targets.len(),
            MAX_AFFECTED_INDEX_EPOCH_TARGETS
        );
        assert_eq!(
            target_limit.insert_target(generation_target(
                IndexId::new(
                    u32::try_from(MAX_AFFECTED_INDEX_EPOCH_TARGETS + 1).expect("u32 bound"),
                )
                .expect("index ID"),
            )),
            Err(CommandIndexError::internal_defect())
        );

        let exact_byte_target = generation_target(IndexId::first());
        let observation_bytes = exact_byte_target.partition_key().as_bytes().len()
            + INDEX_RANGE_TARGET_FIXED_BYTES_V1
            + MAX_INDEX_EPOCH_POSITION_BYTES_V1;
        let mut byte_limit = IndexDerivationBuilder::new(0, 0).expect("builder");
        byte_limit.affected_current_semantic_bytes = MAX_READ_SNAPSHOT_BYTES - observation_bytes;
        byte_limit
            .insert_target(exact_byte_target)
            .expect("exact affected-current byte bound");
        assert_eq!(
            byte_limit.affected_current_semantic_bytes,
            MAX_READ_SNAPSHOT_BYTES
        );
        assert_eq!(
            byte_limit.insert_target(generation_target(IndexId::new(2).expect("index ID"))),
            Err(CommandIndexError::internal_defect())
        );
    }

    #[test]
    fn zero_mutation_and_missing_index_field_fail_closed() {
        let fixture = fixture("CreateRow", "invalid-1", ([0x21; 16], "new", 10), None);
        let empty = EvaluatedCommand::new(
            &ReadSnapshot::new(
                &SnapshotRequest::new(
                    fixture.resolved.reference().clone(),
                    fixture
                        .evaluated
                        .validation_request()
                        .binding_targets()
                        .to_vec(),
                    Vec::new(),
                    Vec::new(),
                )
                .expect("request"),
                None,
                fixture.current.bindings().to_vec(),
                Vec::new(),
                Vec::new(),
            )
            .expect("snapshot"),
            Vec::new(),
            fixture.evaluated.event_intents().to_vec(),
            fixture.evaluated.outcome().clone(),
            EvaluationBudget::v1(),
        )
        .expect("structural zero-mutation candidate");
        assert_eq!(
            derive_grammar_v1_indexes(
                &fixture.resolved,
                &fixture.input,
                &empty,
                &fixture.current,
                &[None],
                &fixture.partition,
            )
            .err(),
            Some(CommandIndexError::internal_defect())
        );

        let index = index(&fixture, "by_score");
        let missing = CanonicalRecord::new(Vec::new()).expect("empty record");
        assert_eq!(
            index_values(index, &missing),
            Err(CommandIndexError::internal_defect())
        );
    }
}
