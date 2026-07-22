//! In-memory application-state storage ports.

use std::num::NonZeroU8;

use riffdb_storage_api::{
    AbandonedCandidate, AdmissionLookupResultV1, AdmissionRepository, AdmissionRequestV1,
    AdmissionResultV1, AffectedEpochCurrentState, AffectedEpochCurrentStateBuilder,
    AffectedIndexEpochTargets, ApplicationCommandTransactionPort, AssignedCommandSequence,
    AtomicCommandRecordSet, AuthoritativeIndexScanPage, AuthoritativeIndexScanRequest,
    AuthoritativePointReader, AuthoritativeScanReader, CandidateAdmissionResult,
    CandidateCapacityResult, CandidateStartResult, CandidateValidationRejection,
    CommandCandidateAdmission, CommandCandidateAffectedEpochRead, CommandCandidateAwaitingCapacity,
    CommandCandidateAwaitingValidation, CommandCandidateCapacityReserved,
    CommandCandidateSequenceAssigned, CommandCandidateStateRead, CommandWriteSetPlanV1,
    CommitIntent, CommitScanPageV1, CommitScanRequest, CommittedBatchV1, CurrentRangeObservation,
    DurabilityMode, EmptyCommandBatch, EncodedPageItem, EntityObservation, EntityTarget,
    ExecutionFailureAdmissionRechecked, ExecutionFailureAdmissionResult,
    ExecutionFailureAwaitingDecision, ExecutionFailureTransitionPort,
    ExecutionFailureTransitionRequestV1, ExpectedEntityState, FilteredAuthoritativeIndexScanPage,
    FilteredAuthoritativeIndexScanRequest, FilteredAuthoritativeScanReader,
    HistoricalPersistedKeyEvidenceV1, IdempotencyIdentity, IdempotencyIdentityKey,
    IdempotencyLookupCandidatesV1, IndexEntryMutationV1, IndexEpochPosition, IndexRangeEntry,
    IndexRangeTarget, MAX_INDEX_SCAN_INSPECTED_BYTES, MAX_INDEX_SCAN_INSPECTED_ENTRIES,
    MAX_SCAN_PAGE_BYTES, NonEmptyCommandBatch, ProvenanceIdCollision, ReadDependencies,
    ReadDependency, ReadSnapshot, ReadSnapshotBuilder, RetainedMetadataV1, SnapshotReader,
    SnapshotRequest, StagedBatchMetrics, StorageError, StorageErrorKind, StorageValueError,
    StoredAdmissionStateV1, StoredCommitRecordV1, StoredDurableEventV1, StoredEntityRecordV1,
    StoredIndexEpochV1, StoredOutcomeV1, StoredPendingAdmissionV1, StoredProvenanceRecordV1,
    TransactionCurrentState, TransactionCurrentStateBuilder, ValidationReadRequest,
    derive_event_hash_v1,
};
use riffdb_types::{CommitSequence, EventId, FrontierPosition, ProvenanceId};

use crate::startup::persisted_evidence_order_key;
use crate::state::{
    CommitAdmissionIndexRow, CommittedAdmissionIndexRow, EntityCommitIndexRow,
    HistoricalPersistedKeyRow, HistoricalPlanReferenceRow, HistoricalPlanReferenceSource,
    MemoryIndexEntry, MemoryMetadataSlot, MemoryState, SyntheticCommandClassCharges,
    bundle_identity_evidence_order_key, memory_record_charge, unique_binary_search_by,
};
use crate::store::{MemoryAccess, MemoryOperationalPorts, storage_error};

#[derive(Clone)]
struct ApplicationOverlay {
    metadata: RetainedMetadataV1,
    admissions: Vec<StoredAdmissionStateV1>,
    entities: Vec<StoredEntityRecordV1>,
    entity_commits: Vec<EntityCommitIndexRow>,
    index_entries: Vec<MemoryIndexEntry>,
    index_epochs: Vec<StoredIndexEpochV1>,
    historical_plan_references: Vec<HistoricalPlanReferenceRow>,
    historical_persisted_keys: Vec<HistoricalPersistedKeyRow>,
    commits: Vec<StoredCommitRecordV1>,
    commit_admissions: Vec<CommitAdmissionIndexRow>,
    committed_admissions: Vec<CommittedAdmissionIndexRow>,
    provenance: Vec<StoredProvenanceRecordV1>,
    events: Vec<StoredDurableEventV1>,
    outbox_intents: Vec<riffdb_storage_api::StoredOutboxIntentV1>,
    pending_outbox_events: Vec<EventId>,
    command_charges: Vec<(CommitSequence, SyntheticCommandClassCharges)>,
}

impl ApplicationOverlay {
    fn from_state(state: &MemoryState) -> Result<Self, StorageError> {
        let MemoryMetadataSlot::Retained(metadata) = &state.metadata else {
            return Err(storage_error(StorageErrorKind::CorruptData));
        };
        Ok(Self {
            metadata: metadata.clone(),
            admissions: state.admissions.clone(),
            entities: state.entities.clone(),
            entity_commits: state.entity_commits.clone(),
            index_entries: state.index_entries.clone(),
            index_epochs: state.index_epochs.clone(),
            historical_plan_references: state.historical_plan_references.clone(),
            historical_persisted_keys: state.historical_persisted_keys.clone(),
            commits: state.commits.clone(),
            commit_admissions: state.commit_admissions.clone(),
            committed_admissions: state.committed_admissions.clone(),
            provenance: state.provenance.clone(),
            events: state.events.clone(),
            outbox_intents: state.outbox_intents.clone(),
            pending_outbox_events: state.pending_outbox_events.clone(),
            command_charges: state.synthetic_charges.command_classes.clone(),
        })
    }

    fn publish(self, state: &mut MemoryState) {
        state.metadata = MemoryMetadataSlot::Retained(self.metadata);
        state.admissions = self.admissions;
        state.entities = self.entities;
        state.entity_commits = self.entity_commits;
        state.index_entries = self.index_entries;
        state.index_epochs = self.index_epochs;
        state.historical_plan_references = self.historical_plan_references;
        state.historical_persisted_keys = self.historical_persisted_keys;
        state.commits = self.commits;
        state.commit_admissions = self.commit_admissions;
        state.committed_admissions = self.committed_admissions;
        state.provenance = self.provenance;
        state.events = self.events;
        state.outbox_intents = self.outbox_intents;
        state.pending_outbox_events = self.pending_outbox_events;
        state.synthetic_charges.command_classes = self.command_charges;
    }
}

struct BatchCore {
    access: MemoryAccess,
    overlay: ApplicationOverlay,
    staged: Vec<AtomicCommandRecordSet>,
    metrics: Option<StagedBatchMetrics>,
}

impl BatchCore {
    fn open(ports: &MemoryOperationalPorts) -> Result<Self, StorageError> {
        let access = ports.acquire()?;
        let overlay = access.read(ApplicationOverlay::from_state)?;
        Ok(Self {
            access,
            overlay,
            staged: Vec::new(),
            metrics: None,
        })
    }
}

/// Empty in-memory command batch. This state has no commit operation.
pub struct MemoryEmptyBatch {
    core: BatchCore,
}

/// Nonempty in-memory command batch holding a private uncommitted overlay.
pub struct MemoryNonEmptyBatch {
    core: BatchCore,
}

/// In-memory candidate before the exact pending admission is rechecked.
pub struct MemoryCandidateAdmission<P> {
    prior: P,
    intent: Box<CommitIntent>,
}

/// In-memory candidate permitted to read transaction-current values.
pub struct MemoryCandidateStateRead<P> {
    prior: P,
    intent: Box<CommitIntent>,
}

/// In-memory candidate waiting for the coordinator's private validation decision.
pub struct MemoryCandidateAwaitingValidation<P> {
    prior: P,
    intent: Box<CommitIntent>,
}

/// In-memory candidate permitted to read mutation-affected epoch positions.
pub struct MemoryCandidateAffectedEpochRead<P> {
    prior: P,
    intent: Box<CommitIntent>,
    affected_targets: AffectedIndexEpochTargets,
}

/// In-memory candidate holding all current reads before capacity reservation.
pub struct MemoryCandidateAwaitingCapacity<P> {
    prior: P,
    intent: Box<CommitIntent>,
    affected_targets: AffectedIndexEpochTargets,
    affected_current: AffectedEpochCurrentState,
}

/// In-memory candidate with pre-sequence capacity and provenance uniqueness reserved.
pub struct MemoryCandidateCapacityReserved<P> {
    prior: P,
    intent: Box<CommitIntent>,
    write_plan: CommandWriteSetPlanV1,
    charges: SyntheticCommandClassCharges,
}

/// In-memory candidate owning an invisible transaction-local sequence.
pub struct MemoryCandidateSequenceAssigned<P> {
    prior: P,
    intent: Box<CommitIntent>,
    write_plan: CommandWriteSetPlanV1,
    charges: SyntheticCommandClassCharges,
    assignment: AssignedCommandSequence,
}

/// Rechecked short transition from pending admission to deterministic failure.
pub struct MemoryExecutionFailureRechecked {
    access: MemoryAccess,
    request: ExecutionFailureTransitionRequestV1,
}

/// Execution-failure transition after current dependency values were read.
pub struct MemoryExecutionFailureAwaitingDecision {
    access: MemoryAccess,
    request: ExecutionFailureTransitionRequestV1,
    current: TransactionCurrentState,
}

impl ApplicationCommandTransactionPort for MemoryOperationalPorts {
    type EmptyBatch = MemoryEmptyBatch;

    fn begin_empty_batch(&self) -> Result<Self::EmptyBatch, StorageError> {
        Ok(MemoryEmptyBatch {
            core: BatchCore::open(self)?,
        })
    }
}

impl EmptyCommandBatch for MemoryEmptyBatch {
    type Candidate = MemoryCandidateAdmission<Self>;

    fn begin_candidate(self, intent: Box<CommitIntent>) -> Result<Self::Candidate, StorageError> {
        Ok(MemoryCandidateAdmission {
            prior: self,
            intent,
        })
    }

    fn rollback(self) {}
}

impl NonEmptyCommandBatch for MemoryNonEmptyBatch {
    type Candidate = MemoryCandidateAdmission<Self>;

    fn metrics(&self) -> StagedBatchMetrics {
        self.core
            .metrics
            .expect("nonempty batch construction always installs metrics")
    }

    fn begin_candidate(
        self,
        intent: Box<CommitIntent>,
    ) -> Result<CandidateStartResult<Self, Self::Candidate>, StorageError> {
        if usize::from(self.metrics().command_count().get())
            == riffdb_storage_api::MAX_STAGED_COMMANDS
        {
            return Ok(CandidateStartResult::BatchFull {
                prior: self,
                intent,
            });
        }
        Ok(CandidateStartResult::Started(MemoryCandidateAdmission {
            prior: self,
            intent,
        }))
    }

    fn commit(self, durability: DurabilityMode) -> Result<CommittedBatchV1, StorageError> {
        if self
            .core
            .staged
            .iter()
            .any(|records| !records.matches_durability_mode(durability))
        {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let outcomes = self
            .core
            .staged
            .iter()
            .map(|records| records.stored_outcome().clone())
            .collect();
        let committed = CommittedBatchV1::new(outcomes, durability).map_err(invariant_value)?;
        let BatchCore {
            access, overlay, ..
        } = self.core;
        access.write(move |state| {
            overlay.publish(state);
            Ok(())
        })?;
        Ok(committed)
    }

    fn rollback(self) {}
}

fn invariant_value(_: StorageValueError) -> StorageError {
    storage_error(StorageErrorKind::InvariantViolation)
}

fn corrupt_value(_: StorageValueError) -> StorageError {
    storage_error(StorageErrorKind::CorruptData)
}

fn materialization_value(error: StorageValueError) -> StorageError {
    match error {
        StorageValueError::LimitExceeded | StorageValueError::SizeOverflow => {
            storage_error(StorageErrorKind::LimitExceeded)
        }
        _ => corrupt_value(error),
    }
}

fn identity_key(identity: &IdempotencyIdentity) -> Result<IdempotencyIdentityKey, StorageError> {
    identity
        .storage_key()
        .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))
}

fn admission_position(
    admissions: &[StoredAdmissionStateV1],
    identity: &IdempotencyIdentity,
) -> Result<Result<usize, usize>, StorageError> {
    let key = identity_key(identity)?;
    let mut left = 0;
    let mut right = admissions.len();
    while left < right {
        let middle = left + (right - left) / 2;
        let candidate = admissions[middle]
            .identity()
            .storage_key()
            .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
        if candidate < key {
            left = middle + 1;
        } else {
            right = middle;
        }
    }
    if left == admissions.len() {
        return Ok(Err(left));
    }
    let candidate = admissions[left]
        .identity()
        .storage_key()
        .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
    if candidate != key {
        return Ok(Err(left));
    }
    if left + 1 < admissions.len()
        && admissions[left + 1]
            .identity()
            .storage_key()
            .map_err(|_| storage_error(StorageErrorKind::CorruptData))?
            == key
    {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    Ok(Ok(left))
}

fn entity_observation(
    entities: &[StoredEntityRecordV1],
    target: &EntityTarget,
) -> Result<EntityObservation, StorageError> {
    match unique_binary_search_by(entities, |row| row.target().cmp(target))? {
        Ok(index) => Ok(EntityObservation::Present(entities[index].clone())),
        Err(_) => Ok(EntityObservation::Absent(target.clone())),
    }
}

fn epoch_position(
    epochs: &[StoredIndexEpochV1],
    target: &IndexRangeTarget,
) -> Result<IndexEpochPosition, StorageError> {
    let prefix = target.prefix();
    match unique_binary_search_by(epochs, |row| {
        row.target()
            .index_id()
            .cmp(&prefix.index_id())
            .then_with(|| row.target().as_bytes().cmp(prefix.as_bytes()))
    })? {
        Ok(index) => Ok(IndexEpochPosition::Value(epochs[index].epoch())),
        Err(_) => Ok(IndexEpochPosition::BeforeFirst),
    }
}

fn current_state(
    entities: &[StoredEntityRecordV1],
    epochs: &[StoredIndexEpochV1],
    request: &ValidationReadRequest,
) -> Result<TransactionCurrentState, StorageError> {
    let mut builder = TransactionCurrentStateBuilder::new(request);
    for target in request.binding_targets() {
        builder
            .push_binding(entity_observation(entities, target)?)
            .map_err(materialization_value)?;
    }
    for target in request.root_validation_targets() {
        builder
            .push_root_validation(entity_observation(entities, target)?)
            .map_err(materialization_value)?;
    }
    for target in request.range_targets() {
        builder
            .push_range(CurrentRangeObservation::new(
                target.clone(),
                epoch_position(epochs, target)?,
            ))
            .map_err(materialization_value)?;
    }
    builder.finish().map_err(materialization_value)
}

fn affected_current_state(
    epochs: &[StoredIndexEpochV1],
    targets: &AffectedIndexEpochTargets,
) -> Result<AffectedEpochCurrentState, StorageError> {
    let mut builder = AffectedEpochCurrentStateBuilder::new(targets);
    for target in targets.as_slice() {
        builder
            .push(CurrentRangeObservation::new(
                target.clone(),
                epoch_position(epochs, target)?,
            ))
            .map_err(materialization_value)?;
    }
    builder.finish().map_err(materialization_value)
}

impl SnapshotReader for MemoryOperationalPorts {
    fn read_snapshot(&self, request: SnapshotRequest) -> Result<ReadSnapshot, StorageError> {
        self.read(|state| {
            let mut builder = ReadSnapshotBuilder::new(
                &request,
                state
                    .commits
                    .last()
                    .map(StoredCommitRecordV1::commit_sequence),
            )
            .map_err(materialization_value)?;
            for target in request.binding_targets() {
                builder
                    .push_binding(entity_observation(&state.entities, target)?)
                    .map_err(materialization_value)?;
            }
            for target in request.root_validation_targets() {
                builder
                    .push_root_validation(entity_observation(&state.entities, target)?)
                    .map_err(materialization_value)?;
            }
            for target in request.range_targets() {
                let prefix = target.prefix().as_bytes();
                let start = state
                    .index_entries
                    .partition_point(|row| row.key().as_bytes() < prefix);
                let mut range = builder
                    .begin_range(target.clone(), epoch_position(&state.index_epochs, target)?)
                    .map_err(materialization_value)?;
                for row in state.index_entries[start..]
                    .iter()
                    .take_while(|row| row.key().as_bytes().starts_with(prefix))
                {
                    range
                        .push_entry(
                            IndexRangeEntry::new(
                                row.key().index_id(),
                                row.key().clone(),
                                row.covered_values().clone(),
                            )
                            .map_err(corrupt_value)?,
                        )
                        .map_err(materialization_value)?;
                }
                range.finish().map_err(materialization_value)?;
            }
            builder.finish().map_err(materialization_value)
        })
    }
}

fn matching_admissions<'a>(
    admissions: &'a [StoredAdmissionStateV1],
    candidates: &IdempotencyLookupCandidatesV1,
) -> Result<Vec<&'a StoredAdmissionStateV1>, StorageError> {
    let mut matches = Vec::new();
    for identity in candidates.as_slice() {
        if let Ok(index) = admission_position(admissions, identity)? {
            matches.push(&admissions[index]);
        }
    }
    Ok(matches)
}

fn admission_result(
    existing: &StoredAdmissionStateV1,
    proposed: &StoredPendingAdmissionV1,
) -> AdmissionResultV1 {
    let equal_input = match existing {
        StoredAdmissionStateV1::Pending(value) => {
            value.canonical_input_hash() == proposed.canonical_input_hash()
        }
        StoredAdmissionStateV1::StoredOutcome(value) => {
            value.canonical_input_hash() == proposed.canonical_input_hash()
        }
        StoredAdmissionStateV1::ExecutionFailed(value) => {
            value.pending().canonical_input_hash() == proposed.canonical_input_hash()
        }
    };
    if !equal_input {
        return AdmissionResultV1::InputMismatch;
    }
    match existing {
        StoredAdmissionStateV1::Pending(value) => AdmissionResultV1::Resumed(value.clone()),
        StoredAdmissionStateV1::StoredOutcome(value) => {
            AdmissionResultV1::StoredOutcome(value.clone())
        }
        StoredAdmissionStateV1::ExecutionFailed(value) => {
            AdmissionResultV1::ExecutionFailed(value.clone())
        }
    }
}

fn plan_bundle_exists(state: &MemoryState, plan: &riffdb_storage_api::ExecutablePlanRef) -> bool {
    let key = bundle_identity_evidence_order_key(
        plan.contract_lineage(),
        plan.contract_version(),
        plan.contract_bundle_hash(),
    );
    state
        .catalog_bundles
        .binary_search_by(|row| row.order_key.cmp(&key))
        .ok()
        .is_some_and(|index| {
            let row = &state.catalog_bundles[index];
            row.bundle.lineage() == plan.contract_lineage()
                && row.bundle.contract_version() == plan.contract_version()
                && row.bundle.bundle_hash() == plan.contract_bundle_hash()
                && (index == 0 || state.catalog_bundles[index - 1].order_key != key)
                && (index + 1 == state.catalog_bundles.len()
                    || state.catalog_bundles[index + 1].order_key != key)
        })
}

impl AdmissionRepository for MemoryOperationalPorts {
    fn admit_or_resolve(
        &self,
        request: AdmissionRequestV1,
    ) -> Result<AdmissionResultV1, StorageError> {
        let access = self.acquire()?;
        access.write(|state| {
            let matches = matching_admissions(&state.admissions, request.lookup_candidates())?;
            if matches.len() > 1 {
                return Ok(AdmissionResultV1::MultipleMatches);
            }
            if let Some(existing) = matches.first() {
                return Ok(admission_result(existing, request.proposed_pending()));
            }
            if !plan_bundle_exists(state, request.proposed_pending().plan()) {
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
            let key = identity_key(request.proposed_pending().identity())?;
            let Err(position) =
                admission_position(&state.admissions, request.proposed_pending().identity())?
            else {
                return Err(storage_error(StorageErrorKind::CorruptData));
            };
            let pending = request.proposed_pending().clone();
            let plan_key = crate::state::plan_evidence_order_key(pending.plan());
            let plan_insert =
                match unique_binary_search_by(&state.historical_plan_references, |row| {
                    row.order_key.cmp(&plan_key)
                })? {
                    Ok(index)
                        if state.historical_plan_references[index].plan == *pending.plan() =>
                    {
                        None
                    }
                    Ok(_) => return Err(storage_error(StorageErrorKind::CorruptData)),
                    Err(index) => Some(index),
                };
            state
                .admissions
                .insert(position, StoredAdmissionStateV1::Pending(pending.clone()));
            if let Some(index) = plan_insert {
                state.historical_plan_references.insert(
                    index,
                    HistoricalPlanReferenceRow::new(
                        pending.plan().clone(),
                        HistoricalPlanReferenceSource::Admission(key),
                    ),
                );
            }
            Ok(AdmissionResultV1::Created(pending))
        })
    }

    fn lookup_admission(
        &self,
        candidates: IdempotencyLookupCandidatesV1,
    ) -> Result<AdmissionLookupResultV1, StorageError> {
        self.read(
            |state| match matching_admissions(&state.admissions, &candidates)?.as_slice() {
                [] => Ok(AdmissionLookupResultV1::NotFound),
                [value] => Ok(AdmissionLookupResultV1::Found(Box::new((*value).clone()))),
                [_, ..] => Ok(AdmissionLookupResultV1::MultipleMatches),
            },
        )
    }
}

impl ExecutionFailureTransitionPort for MemoryOperationalPorts {
    type Rechecked = MemoryExecutionFailureRechecked;

    fn begin_execution_failure(
        &self,
        request: ExecutionFailureTransitionRequestV1,
    ) -> Result<ExecutionFailureAdmissionResult<Self::Rechecked>, StorageError> {
        let access = self.acquire()?;
        let result = access.read(|state| {
            let position =
                admission_position(&state.admissions, request.expected_pending().identity())?;
            Ok(match position {
                Err(_) => ExecutionFailureAdmissionResult::Missing,
                Ok(index) => match &state.admissions[index] {
                    StoredAdmissionStateV1::Pending(value)
                        if value == request.expected_pending() =>
                    {
                        ExecutionFailureAdmissionResult::Rechecked(())
                    }
                    StoredAdmissionStateV1::Pending(_) => {
                        ExecutionFailureAdmissionResult::PendingMismatch
                    }
                    StoredAdmissionStateV1::StoredOutcome(value) => {
                        ExecutionFailureAdmissionResult::StoredOutcome(value.clone())
                    }
                    StoredAdmissionStateV1::ExecutionFailed(value) => {
                        ExecutionFailureAdmissionResult::ExecutionFailed(value.clone())
                    }
                },
            })
        })?;
        Ok(match result {
            ExecutionFailureAdmissionResult::Rechecked(()) => {
                ExecutionFailureAdmissionResult::Rechecked(MemoryExecutionFailureRechecked {
                    access,
                    request,
                })
            }
            ExecutionFailureAdmissionResult::Missing => ExecutionFailureAdmissionResult::Missing,
            ExecutionFailureAdmissionResult::PendingMismatch => {
                ExecutionFailureAdmissionResult::PendingMismatch
            }
            ExecutionFailureAdmissionResult::StoredOutcome(value) => {
                ExecutionFailureAdmissionResult::StoredOutcome(value)
            }
            ExecutionFailureAdmissionResult::ExecutionFailed(value) => {
                ExecutionFailureAdmissionResult::ExecutionFailed(value)
            }
        })
    }
}

impl ExecutionFailureAdmissionRechecked for MemoryExecutionFailureRechecked {
    type AwaitingDecision = MemoryExecutionFailureAwaitingDecision;

    fn read_transaction_current(
        self,
    ) -> Result<(Self::AwaitingDecision, TransactionCurrentState), StorageError> {
        let current = self.access.read(|state| {
            current_state(
                &state.entities,
                &state.index_epochs,
                self.request.validation_request(),
            )
        })?;
        Ok((
            MemoryExecutionFailureAwaitingDecision {
                access: self.access,
                request: self.request,
                current: current.clone(),
            },
            current,
        ))
    }
}

fn dependencies_from_current(
    current: &TransactionCurrentState,
) -> Result<ReadDependencies, StorageError> {
    ReadDependencies::new(
        current
            .bindings()
            .iter()
            .chain(current.root_validations())
            .map(ReadDependency::from_entity)
            .chain(
                current
                    .ranges()
                    .iter()
                    .map(|range| ReadDependency::IndexRangeEpoch {
                        target: range.target().clone(),
                        expected: range.epoch(),
                    }),
            ),
    )
    .map_err(invariant_value)
}

impl ExecutionFailureAwaitingDecision for MemoryExecutionFailureAwaitingDecision {
    fn terminalize(self) -> Result<riffdb_storage_api::StoredExecutionFailedV1, StorageError> {
        if dependencies_from_current(&self.current)? != *self.request.read_dependencies() {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let terminal = self.request.terminal_record();
        self.access.write(|state| {
            let Ok(index) = admission_position(
                &state.admissions,
                self.request.expected_pending().identity(),
            )?
            else {
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            };
            if state.admissions[index]
                != StoredAdmissionStateV1::Pending(self.request.expected_pending().clone())
            {
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
            state.admissions[index] = StoredAdmissionStateV1::ExecutionFailed(terminal.clone());
            Ok(terminal)
        })
    }

    fn abandon(self) {}
}

fn reserved_class_charges(
    plan: &CommandWriteSetPlanV1,
) -> Result<SyntheticCommandClassCharges, StorageError> {
    let classes = plan.charge().encoded_upper_bound().classes();
    let sufficient = classes.allocator() >= 1
        && classes.pending_resolution() >= 1
        && classes.entities() >= plan.intent().evaluated().mutations().len()
        && classes.index_entries() >= plan.index_entries().len()
        && classes.index_epochs() >= plan.index_epochs().len()
        && classes.outcome() >= 1
        && classes.events() >= plan.intent().evaluated().event_intents().len()
        && classes.outbox_intents() >= plan.intent().evaluated().event_intents().len()
        && classes.provenance() >= 1
        && classes.commit() >= 1;
    if !sufficient {
        return Err(storage_error(StorageErrorKind::InvariantViolation));
    }
    SyntheticCommandClassCharges::from_supplied_classes(classes)
}

fn metrics_after(
    current: Option<StagedBatchMetrics>,
    records: &AtomicCommandRecordSet,
) -> Result<StagedBatchMetrics, StorageError> {
    let charge = records.presequence_charge();
    let (count, semantic, encoded) = match current {
        None => (
            1,
            charge.semantic_bytes(),
            charge.encoded_upper_bound().total(),
        ),
        Some(current) => (
            current
                .command_count()
                .get()
                .checked_add(1)
                .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?,
            current
                .semantic_bytes()
                .checked_add(charge.semantic_bytes())
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?,
            current
                .reserved_encoded_bytes()
                .checked_add(charge.encoded_upper_bound().total())
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?,
        ),
    };
    StagedBatchMetrics::new(
        NonZeroU8::new(count).ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?,
        semantic,
        encoded,
    )
    .map_err(invariant_value)
}

fn assign_application_sequence(
    overlay: &mut ApplicationOverlay,
) -> Result<AssignedCommandSequence, StorageError> {
    let allocation = overlay
        .metadata
        .application_sequence()
        .allocate_one()
        .map_err(|error| match error {
            riffdb_storage_api::SequenceAllocationError::Exhausted => {
                storage_error(StorageErrorKind::SequenceExhausted)
            }
            riffdb_storage_api::SequenceAllocationError::ZeroCount
            | riffdb_storage_api::SequenceAllocationError::TooMany => {
                storage_error(StorageErrorKind::InvariantViolation)
            }
        })?;
    overlay.metadata = RetainedMetadataV1::new(
        overlay.metadata.storage_format_version(),
        overlay.metadata.database_id(),
        allocation.next(),
        overlay.metadata.administration_sequence(),
        overlay.metadata.active_catalog().cloned(),
        overlay.metadata.capability_bootstrap(),
    )
    .map_err(invariant_value)?;
    Ok(AssignedCommandSequence::from_assigned(
        allocation.assigned(),
    ))
}

macro_rules! impl_candidate_chain {
    ($prior:ty) => {
        impl CommandCandidateAdmission for MemoryCandidateAdmission<$prior> {
            type Prior = $prior;
            type StateRead = MemoryCandidateStateRead<$prior>;

            fn recheck_admission(
                self,
            ) -> Result<CandidateAdmissionResult<Self::Prior, Self::StateRead>, StorageError> {
                let identity = self.intent.pending().identity();
                let position = admission_position(&self.prior.core.overlay.admissions, identity)?;
                Ok(match position {
                    Err(_) => CandidateAdmissionResult::MissingPending(AbandonedCandidate::new(
                        self.prior,
                        self.intent,
                    )),
                    Ok(index) => match self.prior.core.overlay.admissions[index].clone() {
                        StoredAdmissionStateV1::Pending(pending)
                            if &pending == self.intent.pending() =>
                        {
                            CandidateAdmissionResult::Proceed(MemoryCandidateStateRead {
                                prior: self.prior,
                                intent: self.intent,
                            })
                        }
                        StoredAdmissionStateV1::Pending(pending)
                            if pending.canonical_input_hash()
                                != self.intent.pending().canonical_input_hash() =>
                        {
                            CandidateAdmissionResult::InputMismatch(AbandonedCandidate::new(
                                self.prior,
                                self.intent,
                            ))
                        }
                        StoredAdmissionStateV1::Pending(_) => {
                            CandidateAdmissionResult::PendingMismatch(AbandonedCandidate::new(
                                self.prior,
                                self.intent,
                            ))
                        }
                        StoredAdmissionStateV1::StoredOutcome(outcome)
                            if outcome.canonical_input_hash()
                                == self.intent.pending().canonical_input_hash() =>
                        {
                            CandidateAdmissionResult::StoredOutcome {
                                prior: self.prior,
                                outcome,
                            }
                        }
                        StoredAdmissionStateV1::ExecutionFailed(failure)
                            if failure.pending().canonical_input_hash()
                                == self.intent.pending().canonical_input_hash() =>
                        {
                            CandidateAdmissionResult::ExecutionFailed {
                                prior: self.prior,
                                failure,
                            }
                        }
                        StoredAdmissionStateV1::StoredOutcome(_)
                        | StoredAdmissionStateV1::ExecutionFailed(_) => {
                            CandidateAdmissionResult::InputMismatch(AbandonedCandidate::new(
                                self.prior,
                                self.intent,
                            ))
                        }
                    },
                })
            }
        }

        impl CommandCandidateStateRead for MemoryCandidateStateRead<$prior> {
            type Prior = $prior;
            type AwaitingValidation = MemoryCandidateAwaitingValidation<$prior>;

            fn read_transaction_current(
                self,
            ) -> Result<(Self::AwaitingValidation, TransactionCurrentState), StorageError> {
                let current = current_state(
                    &self.prior.core.overlay.entities,
                    &self.prior.core.overlay.index_epochs,
                    self.intent.evaluated().validation_request(),
                )?;
                Ok((
                    MemoryCandidateAwaitingValidation {
                        prior: self.prior,
                        intent: self.intent,
                    },
                    current,
                ))
            }
        }

        impl CommandCandidateAwaitingValidation for MemoryCandidateAwaitingValidation<$prior> {
            type Prior = $prior;
            type AffectedEpochRead = MemoryCandidateAffectedEpochRead<$prior>;

            fn plan_validated(
                self,
                affected_targets: AffectedIndexEpochTargets,
            ) -> Self::AffectedEpochRead {
                MemoryCandidateAffectedEpochRead {
                    prior: self.prior,
                    intent: self.intent,
                    affected_targets,
                }
            }

            fn reject(
                self,
                _reason: CandidateValidationRejection,
            ) -> AbandonedCandidate<Self::Prior> {
                AbandonedCandidate::new(self.prior, self.intent)
            }
        }

        impl CommandCandidateAffectedEpochRead for MemoryCandidateAffectedEpochRead<$prior> {
            type Prior = $prior;
            type AwaitingCapacity = MemoryCandidateAwaitingCapacity<$prior>;

            fn read_affected_epoch_current(self) -> Result<Self::AwaitingCapacity, StorageError> {
                let affected_current = affected_current_state(
                    &self.prior.core.overlay.index_epochs,
                    &self.affected_targets,
                )?;
                Ok(MemoryCandidateAwaitingCapacity {
                    prior: self.prior,
                    intent: self.intent,
                    affected_targets: self.affected_targets,
                    affected_current,
                })
            }
        }

        impl CommandCandidateAwaitingCapacity for MemoryCandidateAwaitingCapacity<$prior> {
            type Prior = $prior;
            type CapacityReserved = MemoryCandidateCapacityReserved<$prior>;

            fn intent(&self) -> &CommitIntent {
                &self.intent
            }

            fn affected_targets(&self) -> &AffectedIndexEpochTargets {
                &self.affected_targets
            }

            fn affected_current(&self) -> &AffectedEpochCurrentState {
                &self.affected_current
            }

            fn reserve_capacity(
                self,
                write_plan: CommandWriteSetPlanV1,
            ) -> Result<CandidateCapacityResult<Self::Prior, Self::CapacityReserved>, StorageError>
            {
                if !write_plan.matches_retained_candidate(
                    &self.intent,
                    &self.affected_targets,
                    &self.affected_current,
                ) {
                    return Err(storage_error(StorageErrorKind::InvariantViolation));
                }
                let provenance_id = self.intent.provenance_id();
                if unique_binary_search_by(&self.prior.core.overlay.provenance, |record| {
                    record.provenance_id().cmp(&provenance_id)
                })?
                .is_ok()
                {
                    return Ok(CandidateCapacityResult::ProvenanceIdCollision(
                        ProvenanceIdCollision::detected(),
                    ));
                }
                let charge = write_plan.charge();
                if self
                    .prior
                    .core
                    .metrics
                    .is_some_and(|metrics| !metrics.can_add(charge))
                {
                    return Ok(CandidateCapacityResult::BatchFull(AbandonedCandidate::new(
                        self.prior,
                        self.intent,
                    )));
                }
                let charges = reserved_class_charges(&write_plan)?;
                Ok(CandidateCapacityResult::Reserved(
                    MemoryCandidateCapacityReserved {
                        prior: self.prior,
                        intent: self.intent,
                        write_plan,
                        charges,
                    },
                ))
            }
        }

        impl CommandCandidateCapacityReserved for MemoryCandidateCapacityReserved<$prior> {
            type Prior = $prior;
            type SequenceAssigned = MemoryCandidateSequenceAssigned<$prior>;

            fn intent(&self) -> &CommitIntent {
                &self.intent
            }

            fn write_plan(&self) -> &CommandWriteSetPlanV1 {
                &self.write_plan
            }

            fn assign_sequence(mut self) -> Result<Self::SequenceAssigned, StorageError> {
                let assignment = assign_application_sequence(&mut self.prior.core.overlay)?;
                Ok(MemoryCandidateSequenceAssigned {
                    prior: self.prior,
                    intent: self.intent,
                    write_plan: self.write_plan,
                    charges: self.charges,
                    assignment,
                })
            }
        }

        impl CommandCandidateSequenceAssigned for MemoryCandidateSequenceAssigned<$prior> {
            type Prior = $prior;
            type Staged = MemoryNonEmptyBatch;

            fn assignment(&self) -> AssignedCommandSequence {
                self.assignment
            }

            fn intent(&self) -> &CommitIntent {
                &self.intent
            }

            fn write_plan(&self) -> &CommandWriteSetPlanV1 {
                &self.write_plan
            }

            fn stage(self, records: AtomicCommandRecordSet) -> Result<Self::Staged, StorageError> {
                if !records.matches_reserved_candidate(
                    self.assignment,
                    &self.intent,
                    &self.write_plan,
                ) || records.next_application_sequence()
                    != self.prior.core.overlay.metadata.application_sequence()
                {
                    return Err(storage_error(StorageErrorKind::InvariantViolation));
                }
                for event in records.events() {
                    if derive_event_hash_v1(
                        event.event_id(),
                        event.event_type_id(),
                        event.payload(),
                    )
                    .map_err(invariant_value)?
                        != event.event_hash()
                    {
                        return Err(storage_error(StorageErrorKind::InvariantViolation));
                    }
                }
                let mut core = self.prior.core;
                apply_record_set(&mut core.overlay, &records, self.charges)?;
                core.metrics = Some(metrics_after(core.metrics, &records)?);
                core.staged.push(records);
                Ok(MemoryNonEmptyBatch { core })
            }
        }
    };
}

impl_candidate_chain!(MemoryEmptyBatch);
impl_candidate_chain!(MemoryNonEmptyBatch);

fn remove_persisted_evidence(
    rows: &mut Vec<HistoricalPersistedKeyRow>,
    evidence: &HistoricalPersistedKeyEvidenceV1,
) -> Result<(), StorageError> {
    let key = persisted_evidence_order_key(evidence);
    let Ok(index) = unique_binary_search_by(rows, |row| row.order_key.cmp(&key))? else {
        return Err(storage_error(StorageErrorKind::CorruptData));
    };
    if rows[index].evidence != *evidence {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    rows.remove(index);
    Ok(())
}

fn insert_persisted_evidence(
    rows: &mut Vec<HistoricalPersistedKeyRow>,
    evidence: HistoricalPersistedKeyEvidenceV1,
) -> Result<(), StorageError> {
    let key = persisted_evidence_order_key(&evidence);
    let Err(index) = unique_binary_search_by(rows, |row| row.order_key.cmp(&key))? else {
        return Err(storage_error(StorageErrorKind::InvariantViolation));
    };
    rows.insert(
        index,
        HistoricalPersistedKeyRow {
            order_key: key,
            evidence,
        },
    );
    Ok(())
}

fn apply_entities(
    overlay: &mut ApplicationOverlay,
    records: &AtomicCommandRecordSet,
) -> Result<(), StorageError> {
    let sequence = records.assignment().assigned();
    for mutation in records.entities() {
        let target = mutation.post_image().target();
        let entity_position =
            unique_binary_search_by(&overlay.entities, |row| row.target().cmp(target))?;
        let commit_position =
            unique_binary_search_by(&overlay.entity_commits, |row| row.target.cmp(target))?;
        match (mutation.expected(), entity_position, commit_position) {
            (ExpectedEntityState::Absent, Err(entity_index), Err(commit_index)) => {
                overlay
                    .entities
                    .insert(entity_index, mutation.post_image().clone());
                overlay.entity_commits.insert(
                    commit_index,
                    EntityCommitIndexRow {
                        target: target.clone(),
                        commit_sequence: sequence,
                    },
                );
            }
            (ExpectedEntityState::Present(expected), Ok(entity_index), Ok(commit_index))
                if overlay.entities[entity_index].entity_version() == expected =>
            {
                let old =
                    HistoricalPersistedKeyEvidenceV1::from_entity(&overlay.entities[entity_index]);
                remove_persisted_evidence(&mut overlay.historical_persisted_keys, &old)?;
                overlay.entities[entity_index] = mutation.post_image().clone();
                overlay.entity_commits[commit_index].commit_sequence = sequence;
            }
            (ExpectedEntityState::Absent | ExpectedEntityState::Present(_), _, _) => {
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
        }
        insert_persisted_evidence(
            &mut overlay.historical_persisted_keys,
            HistoricalPersistedKeyEvidenceV1::from_entity(mutation.post_image()),
        )?;
    }
    Ok(())
}

fn apply_index_entries(
    overlay: &mut ApplicationOverlay,
    mutations: &[IndexEntryMutationV1],
) -> Result<(), StorageError> {
    for mutation in mutations {
        let key = mutation.key();
        let position = unique_binary_search_by(&overlay.index_entries, |row| {
            row.key().as_bytes().cmp(key.as_bytes())
        })?;
        match (mutation, position) {
            (IndexEntryMutationV1::Delete(_), Ok(index)) => {
                overlay.index_entries.remove(index);
            }
            (IndexEntryMutationV1::Delete(_), Err(_)) => {
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
            (IndexEntryMutationV1::Put(record), Ok(index)) => {
                overlay.index_entries[index] =
                    MemoryIndexEntry::current(record.clone(), memory_record_charge())?;
            }
            (IndexEntryMutationV1::Put(record), Err(index)) => {
                overlay.index_entries.insert(
                    index,
                    MemoryIndexEntry::current(record.clone(), memory_record_charge())?,
                );
            }
        }
    }
    Ok(())
}

fn apply_index_epochs(
    overlay: &mut ApplicationOverlay,
    advances: &[riffdb_storage_api::IndexEpochAdvanceV1],
) -> Result<(), StorageError> {
    for advance in advances {
        let target = advance.target();
        let position =
            unique_binary_search_by(&overlay.index_epochs, |row| row.target().cmp(target))?;
        match (advance.prior(), position) {
            (IndexEpochPosition::BeforeFirst, Err(index)) => {
                overlay
                    .index_epochs
                    .insert(index, advance.post_image().clone());
            }
            (IndexEpochPosition::Value(expected), Ok(index))
                if overlay.index_epochs[index].epoch() == expected =>
            {
                let old = HistoricalPersistedKeyEvidenceV1::from_index_epoch(
                    &overlay.index_epochs[index],
                );
                remove_persisted_evidence(&mut overlay.historical_persisted_keys, &old)?;
                overlay.index_epochs[index] = advance.post_image().clone();
            }
            (IndexEpochPosition::BeforeFirst | IndexEpochPosition::Value(_), _) => {
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
        }
        insert_persisted_evidence(
            &mut overlay.historical_persisted_keys,
            HistoricalPersistedKeyEvidenceV1::from_index_epoch(advance.post_image()),
        )?;
    }
    Ok(())
}

fn append_sequence_graph(
    overlay: &mut ApplicationOverlay,
    records: &AtomicCommandRecordSet,
    charges: SyntheticCommandClassCharges,
) -> Result<(), StorageError> {
    let sequence = records.assignment().assigned();
    let expected = overlay
        .commits
        .last()
        .map_or(Some(CommitSequence::first()), |record| {
            record.commit_sequence().checked_next()
        });
    if expected != Some(sequence) {
        return Err(storage_error(StorageErrorKind::InvariantViolation));
    }

    let identity_key = identity_key(records.stored_outcome().identity())?;
    if overlay
        .commit_admissions
        .last()
        .is_some_and(|row| row.commit_sequence >= sequence)
        || unique_binary_search_by(&overlay.committed_admissions, |row| {
            row.identity_key.cmp(&identity_key)
        })?
        .is_ok()
    {
        return Err(storage_error(StorageErrorKind::InvariantViolation));
    }
    let admission_index = overlay
        .committed_admissions
        .binary_search_by(|row| row.identity_key.cmp(&identity_key))
        .unwrap_or_else(|index| index);
    overlay.commit_admissions.push(CommitAdmissionIndexRow {
        commit_sequence: sequence,
        identity_key: identity_key.clone(),
    });
    overlay.committed_admissions.insert(
        admission_index,
        CommittedAdmissionIndexRow {
            identity_key,
            commit_sequence: sequence,
        },
    );
    overlay.commits.push(records.commit().clone());
    overlay.command_charges.push((sequence, charges));
    Ok(())
}

fn apply_events(
    overlay: &mut ApplicationOverlay,
    records: &AtomicCommandRecordSet,
) -> Result<(), StorageError> {
    for (event, intent) in records.events().iter().zip(records.outbox_intents()) {
        if intent.event() != event {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let event_position =
            unique_binary_search_by(&overlay.events, |row| row.event_id().cmp(&event.event_id()))?;
        let intent_position = unique_binary_search_by(&overlay.outbox_intents, |row| {
            row.event_id().cmp(&event.event_id())
        })?;
        let pending_position = overlay
            .pending_outbox_events
            .binary_search(&event.event_id());
        let (Err(event_index), Err(intent_index), Err(pending_index)) =
            (event_position, intent_position, pending_position)
        else {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        };
        overlay.events.insert(event_index, event.clone());
        overlay.outbox_intents.insert(intent_index, intent.clone());
        overlay
            .pending_outbox_events
            .insert(pending_index, event.event_id());
    }
    Ok(())
}

fn apply_record_set(
    overlay: &mut ApplicationOverlay,
    records: &AtomicCommandRecordSet,
    charges: SyntheticCommandClassCharges,
) -> Result<(), StorageError> {
    let expected_pending = records.expected_pending();
    let Ok(admission_index) = admission_position(&overlay.admissions, expected_pending.identity())?
    else {
        return Err(storage_error(StorageErrorKind::InvariantViolation));
    };
    if overlay.admissions[admission_index]
        != StoredAdmissionStateV1::Pending(expected_pending.clone())
    {
        return Err(storage_error(StorageErrorKind::InvariantViolation));
    }
    let plan_key = crate::state::plan_evidence_order_key(records.intent().evaluated().plan());
    let Ok(plan_index) = unique_binary_search_by(&overlay.historical_plan_references, |row| {
        row.order_key.cmp(&plan_key)
    })?
    else {
        return Err(storage_error(StorageErrorKind::CorruptData));
    };
    if overlay.historical_plan_references[plan_index].plan != *records.intent().evaluated().plan() {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }

    apply_entities(overlay, records)?;
    apply_index_entries(overlay, records.index_entries())?;
    apply_index_epochs(overlay, records.index_epochs())?;

    let provenance_id = records.provenance().provenance_id();
    let Err(provenance_index) = unique_binary_search_by(&overlay.provenance, |row| {
        row.provenance_id().cmp(&provenance_id)
    })?
    else {
        return Err(storage_error(StorageErrorKind::InvariantViolation));
    };
    overlay
        .provenance
        .insert(provenance_index, records.provenance().clone());
    apply_events(overlay, records)?;
    append_sequence_graph(overlay, records, charges)?;
    overlay.admissions[admission_index] =
        StoredAdmissionStateV1::StoredOutcome(records.stored_outcome().clone());
    Ok(())
}

impl AuthoritativePointReader for MemoryOperationalPorts {
    fn read_entity(
        &self,
        target: &EntityTarget,
    ) -> Result<Option<StoredEntityRecordV1>, StorageError> {
        self.read(|state| {
            Ok(
                match unique_binary_search_by(&state.entities, |row| row.target().cmp(target))? {
                    Ok(index) => Some(state.entities[index].clone()),
                    Err(_) => None,
                },
            )
        })
    }

    fn read_stored_outcome(
        &self,
        identity: &IdempotencyIdentity,
    ) -> Result<Option<StoredOutcomeV1>, StorageError> {
        self.read(|state| {
            let value = match admission_position(&state.admissions, identity)? {
                Ok(index) => match &state.admissions[index] {
                    StoredAdmissionStateV1::StoredOutcome(outcome) => Some(outcome.clone()),
                    StoredAdmissionStateV1::Pending(_)
                    | StoredAdmissionStateV1::ExecutionFailed(_) => None,
                },
                Err(_) => None,
            };
            Ok(value)
        })
    }

    fn read_commit(
        &self,
        sequence: CommitSequence,
    ) -> Result<Option<StoredCommitRecordV1>, StorageError> {
        self.read(|state| {
            Ok(
                match unique_binary_search_by(&state.commits, |record| {
                    record.commit_sequence().cmp(&sequence)
                })? {
                    Ok(index) => Some(state.commits[index].clone()),
                    Err(_) => None,
                },
            )
        })
    }

    fn read_provenance(
        &self,
        provenance_id: ProvenanceId,
    ) -> Result<Option<StoredProvenanceRecordV1>, StorageError> {
        self.read(|state| {
            Ok(
                match unique_binary_search_by(&state.provenance, |record| {
                    record.provenance_id().cmp(&provenance_id)
                })? {
                    Ok(index) => Some(state.provenance[index].clone()),
                    Err(_) => None,
                },
            )
        })
    }

    fn read_durable_event(
        &self,
        event_id: EventId,
    ) -> Result<Option<StoredDurableEventV1>, StorageError> {
        self.read(|state| {
            Ok(
                match unique_binary_search_by(&state.events, |event| {
                    event.event_id().cmp(&event_id)
                })? {
                    Ok(index) => Some(state.events[index].clone()),
                    Err(_) => None,
                },
            )
        })
    }
}

impl AuthoritativeScanReader for MemoryOperationalPorts {
    fn scan_index(
        &self,
        request: AuthoritativeIndexScanRequest,
    ) -> Result<AuthoritativeIndexScanPage, StorageError> {
        self.read(|state| {
            let epoch = epoch_position(&state.index_epochs, request.target())?;
            let prefix = request.target().prefix().as_bytes();
            let start = match request.after() {
                Some(after) => state
                    .index_entries
                    .partition_point(|row| row.key().as_bytes() <= after.as_bytes()),
                None => state
                    .index_entries
                    .partition_point(|row| row.key().as_bytes() < prefix),
            };
            let wanted = usize::from(request.limit().get());
            let mut rows = Vec::with_capacity(wanted.saturating_add(1));
            for row in state.index_entries[start..]
                .iter()
                .take_while(|row| row.key().as_bytes().starts_with(prefix))
                .take(wanted.saturating_add(1))
            {
                if row.current_record().is_none() {
                    return Err(storage_error(StorageErrorKind::IncompatibleFormat));
                }
                rows.push(EncodedPageItem::new(
                    IndexRangeEntry::new(
                        row.key().index_id(),
                        row.key().clone(),
                        row.covered_values().clone(),
                    )
                    .map_err(corrupt_value)?,
                    memory_record_charge(),
                ));
            }
            if rows
                .windows(2)
                .any(|pair| pair[0].value().key().as_bytes() >= pair[1].value().key().as_bytes())
            {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
            let has_more = rows.len() > wanted;
            rows.truncate(wanted);
            if has_more {
                let next_after = rows
                    .last()
                    .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?
                    .value()
                    .key()
                    .clone();
                AuthoritativeIndexScanPage::page(&request, epoch, rows, next_after)
                    .map_err(corrupt_value)
            } else {
                AuthoritativeIndexScanPage::exact_end(&request, epoch, rows).map_err(corrupt_value)
            }
        })
    }

    fn scan_commits(&self, request: CommitScanRequest) -> Result<CommitScanPageV1, StorageError> {
        self.read(|state| {
            let inclusive_upper = request.inclusive_upper().map_or_else(
                || {
                    state
                        .commits
                        .last()
                        .map_or(FrontierPosition::BeforeFirst, |record| {
                            FrontierPosition::AppliedThrough(record.commit_sequence())
                        })
                },
                FrontierPosition::AppliedThrough,
            );
            let start = request.after().map_or(0, |after| {
                state
                    .commits
                    .partition_point(|record| record.commit_sequence() <= after)
            });
            let upper = match inclusive_upper {
                FrontierPosition::BeforeFirst => None,
                FrontierPosition::AppliedThrough(sequence) => Some(sequence),
            };
            let wanted = usize::from(request.limit().get());
            let mut rows = state.commits[start..]
                .iter()
                .take_while(|record| upper.is_some_and(|upper| record.commit_sequence() <= upper))
                .take(wanted.saturating_add(1))
                .cloned()
                .map(|record| EncodedPageItem::new(record, memory_record_charge()))
                .collect::<Vec<_>>();
            let mut expected = request
                .after()
                .map_or(Some(CommitSequence::first()), CommitSequence::checked_next);
            for row in &rows {
                if expected != Some(row.value().commit_sequence()) {
                    return Err(storage_error(StorageErrorKind::CorruptData));
                }
                expected = row.value().commit_sequence().checked_next();
            }
            let has_more = rows.len() > wanted;
            rows.truncate(wanted);
            if has_more {
                let next_after = rows
                    .last()
                    .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?
                    .value()
                    .commit_sequence();
                CommitScanPageV1::page(request, inclusive_upper, rows, next_after)
                    .map_err(corrupt_value)
            } else {
                CommitScanPageV1::exact_end(request, inclusive_upper, rows).map_err(corrupt_value)
            }
        })
    }
}

impl FilteredAuthoritativeScanReader for MemoryOperationalPorts {
    fn scan_index_filtered(
        &self,
        request: FilteredAuthoritativeIndexScanRequest,
    ) -> Result<FilteredAuthoritativeIndexScanPage, StorageError> {
        self.read(|state| {
            let epoch = epoch_position(&state.index_epochs, request.target())?;
            if request.partition_filter().is_none() {
                return FilteredAuthoritativeIndexScanPage::exact_end(&request, epoch, Vec::new())
                    .map_err(corrupt_value);
            }

            let prefix = request.target().prefix().as_bytes();
            let mut position = match request.after() {
                Some(after) => state
                    .index_entries
                    .partition_point(|row| row.key().as_bytes() <= after.as_bytes()),
                None => state
                    .index_entries
                    .partition_point(|row| row.key().as_bytes() < prefix),
            };
            let returned_limit = usize::from(request.limit().get());
            let mut returned = Vec::with_capacity(returned_limit);
            let mut returned_bytes = 0usize;
            let mut inspected_entries = 0usize;
            let mut inspected_bytes = 0usize;
            let mut scanned_through = None;
            let mut exact_end = true;

            while let Some(candidate) = state.index_entries.get(position) {
                if !candidate.key().as_bytes().starts_with(prefix) {
                    break;
                }
                if returned.len() == returned_limit
                    || inspected_entries == MAX_INDEX_SCAN_INSPECTED_ENTRIES
                {
                    exact_end = false;
                    break;
                }

                let charge = candidate.encoded_content_charge();
                let next_inspected_bytes = inspected_bytes
                    .checked_add(charge.get())
                    .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
                if next_inspected_bytes > MAX_INDEX_SCAN_INSPECTED_BYTES {
                    if scanned_through.is_none() {
                        return Err(storage_error(StorageErrorKind::CorruptData));
                    }
                    exact_end = false;
                    break;
                }

                let record = candidate
                    .current_record()
                    .ok_or_else(|| storage_error(StorageErrorKind::IncompatibleFormat))?;
                let is_eligible = request
                    .partition_filter()
                    .allows(record.schema_binding(), record.partition_key());
                let next_returned_bytes = returned_bytes
                    .checked_add(charge.get())
                    .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
                if is_eligible && next_returned_bytes > MAX_SCAN_PAGE_BYTES {
                    if scanned_through.is_none() {
                        return Err(storage_error(StorageErrorKind::CorruptData));
                    }
                    exact_end = false;
                    break;
                }
                inspected_entries += 1;
                inspected_bytes = next_inspected_bytes;
                scanned_through = Some(record.key().clone());
                if is_eligible {
                    returned_bytes = next_returned_bytes;
                    returned.push(EncodedPageItem::new(record.clone(), charge));
                }
                position += 1;
            }

            if exact_end {
                FilteredAuthoritativeIndexScanPage::exact_end(&request, epoch, returned)
                    .map_err(corrupt_value)
            } else {
                let scanned_through = scanned_through
                    .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
                FilteredAuthoritativeIndexScanPage::page(&request, epoch, returned, scanned_through)
                    .map_err(corrupt_value)
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use riffdb_storage_api::{
        AffectedEntityV1, ApplicationSequenceAllocator, DatabaseInitializationPort,
        DurableKeySchemaBindingV1, EncodedWriteSetUpperBound, EntityMutation, EntityPostImage,
        EvaluationBudget, EventIntent, IdempotencyKeyDigest, IndexEpochAdvanceV1,
        IndexPartitionFilter, IndexPartitionFilterScope, IndexRangeObservation,
        IndexRangePrefixBuilder, PreEvaluationCommitContext, StorageScanLimit,
        StoredAdmittedProvenanceClaimsV1, StoredContractBundleV1, StoredIndexEntryV1,
        StoredIndexEntryV2, StoredOutboxIntentV1, StoredReadDependenciesV1,
    };
    use riffdb_testkit::model::AuthoritativeCommandModel;
    use riffdb_types::{
        ActorId, ActorKind, AggregateTypeId, CanonicalInputHash, CanonicalRecord, CanonicalValue,
        CommandId, ContractBundleHash, ContractLineage, ContractVersion, DatabaseId, DigestKeyId,
        EntityKeyBuilder, EntityTypeId, EntityVersion, Environment, EventTypeId, FieldId,
        IndexEntryKeyBuilder, IndexId, LogicalTime, OutcomeId, PartitionKeyBuilder, PlanHash,
        RequestId, TenantId, TenantScope, Timestamp,
    };

    use super::*;
    use crate::state::CatalogBundleRow;
    use crate::store::MemoryStore;

    #[derive(Clone)]
    struct CommandFixture {
        admission: AdmissionRequestV1,
        pending: StoredPendingAdmissionV1,
        intent: CommitIntent,
        affected_targets: AffectedIndexEpochTargets,
        write_plan: CommandWriteSetPlanV1,
        records: AtomicCommandRecordSet,
        target: EntityTarget,
        range: IndexRangeTarget,
        index_key: riffdb_types::IndexEntryKey,
    }

    fn uuid_bytes(fill: u8) -> [u8; 16] {
        let mut bytes = [fill; 16];
        bytes[6] = 0x70 | (fill & 0x0f);
        bytes[8] = 0x80 | (fill & 0x3f);
        bytes
    }

    fn database_id() -> DatabaseId {
        DatabaseId::from_bytes(uuid_bytes(0x11)).expect("database ID")
    }

    fn plan() -> riffdb_storage_api::ExecutablePlanRef {
        riffdb_storage_api::ExecutablePlanRef::new(
            ContractLineage::new("application-test").expect("lineage"),
            ContractVersion::new(1).expect("version"),
            ContractBundleHash::from_bytes([0x21; 32]),
            CommandId::new(1).expect("command"),
            PlanHash::from_bytes([0x22; 32]),
        )
    }

    fn bundle() -> StoredContractBundleV1 {
        let plan = plan();
        StoredContractBundleV1::new(
            plan.contract_lineage().clone(),
            plan.contract_version(),
            plan.contract_bundle_hash(),
            b"application-test-bundle".to_vec(),
        )
        .expect("bundle")
    }

    fn record(value: u64) -> CanonicalRecord {
        CanonicalRecord::new(vec![(
            FieldId::new(1).expect("field"),
            CanonicalValue::U64(value),
        )])
        .expect("record")
    }

    fn large_record(fill: u8) -> CanonicalRecord {
        CanonicalRecord::new(vec![(
            FieldId::new(1).expect("field"),
            CanonicalValue::bytes(vec![fill; 900_000]).expect("bounded bytes"),
        )])
        .expect("large record")
    }

    fn target_and_index() -> (EntityTarget, riffdb_types::IndexEntryKey, IndexRangeTarget) {
        let entity_type = EntityTypeId::new(1).expect("entity type");
        let mut entity_key = EntityKeyBuilder::new(entity_type);
        entity_key.push_u64(7).expect("entity component");
        let entity_key = entity_key.finish().expect("entity key");
        let target = EntityTarget::new(entity_type, entity_key.clone()).expect("entity target");
        let index_id = IndexId::new(1).expect("index");
        let mut index_key = IndexEntryKeyBuilder::new(index_id);
        index_key.push_u64(10).expect("index component");
        let index_key = index_key.finish(entity_key).expect("index key");
        let mut prefix = IndexRangePrefixBuilder::new(index_id);
        prefix.push_u64(10).expect("prefix component");
        let range = IndexRangeTarget::new(prefix.finish());
        (target, index_key, range)
    }

    fn operational_ports(bundle: StoredContractBundleV1) -> MemoryOperationalPorts {
        let mut store = MemoryStore::new();
        store
            .initialize_database(database_id())
            .expect("initialize database");
        store
            .acquire()
            .expect("seed access")
            .write(|state| {
                state.catalog_bundles.push(CatalogBundleRow::new(bundle));
                Ok(())
            })
            .expect("seed catalog bundle");
        crate::startup::MemoryDormantPorts { store }.into_operational()
    }

    fn filtered_range() -> IndexRangeTarget {
        let mut prefix = IndexRangePrefixBuilder::new(IndexId::new(7).expect("index"));
        prefix.push_u64(19).expect("prefix component");
        IndexRangeTarget::new(prefix.finish())
    }

    fn filtered_index_key(value: u64) -> riffdb_types::IndexEntryKey {
        let mut entity = EntityKeyBuilder::new(EntityTypeId::new(1).expect("entity type"));
        entity.push_u64(value).expect("entity component");
        let mut index = IndexEntryKeyBuilder::new(IndexId::new(7).expect("index"));
        index.push_u64(19).expect("index component");
        index
            .finish(entity.finish().expect("entity key"))
            .expect("index key")
    }

    fn filtered_partition(value: u64) -> riffdb_types::PartitionKey {
        let mut partition = PartitionKeyBuilder::new(AggregateTypeId::new(1).expect("aggregate"));
        partition.push_u64(value).expect("partition component");
        partition.finish().expect("partition")
    }

    fn filtered_row(value: u64, partition: u64, lineage: &str) -> StoredIndexEntryV2 {
        let plan = plan();
        StoredIndexEntryV2::new(
            filtered_index_key(value),
            DurableKeySchemaBindingV1::new(
                ContractLineage::new(lineage).expect("lineage"),
                plan.contract_version(),
                plan.contract_bundle_hash(),
            ),
            record(value),
            filtered_partition(partition),
        )
        .expect("V2 row")
    }

    fn seed_filtered_rows(ports: &MemoryOperationalPorts, rows: Vec<(StoredIndexEntryV2, usize)>) {
        ports
            .acquire()
            .expect("seed access")
            .write(move |state| {
                state.index_entries = rows
                    .into_iter()
                    .map(|(record, bytes)| {
                        MemoryIndexEntry::current(
                            record,
                            riffdb_storage_api::EncodedContentCharge::new(bytes)
                                .expect("synthetic charge"),
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(())
            })
            .expect("seed filtered rows");
    }

    fn filtered_request(
        scope: IndexPartitionFilterScope,
        after: Option<riffdb_types::IndexEntryKey>,
        limit: u16,
    ) -> FilteredAuthoritativeIndexScanRequest {
        let filter = IndexPartitionFilter::new(
            ContractLineage::new("application-test").expect("lineage"),
            scope,
        )
        .expect("filter");
        FilteredAuthoritativeIndexScanRequest::new(
            filtered_range(),
            filter,
            after,
            StorageScanLimit::new(limit).expect("limit"),
        )
        .expect("request")
    }

    fn command_fixture(
        sequence_value: u64,
        suffix: u8,
        prior_entity: Option<StoredEntityRecordV1>,
        prior_epoch: IndexEpochPosition,
        provenance_suffix: u8,
    ) -> CommandFixture {
        let plan = plan();
        let sequence = CommitSequence::new(sequence_value).expect("sequence");
        let (target, index_key, range) = target_and_index();
        let tenant_scope = TenantScope::Tenant(TenantId::new("tenant-a").expect("tenant"));
        let principal = ActorId::new("principal-a").expect("principal");
        let actor = riffdb_types::AdmittedActorContext::new(
            principal.clone(),
            ActorKind::Human,
            tenant_scope.clone(),
            None,
        );
        let identity = IdempotencyIdentity::new(
            database_id(),
            Environment::new("test").expect("environment"),
            tenant_scope,
            principal,
            plan.contract_lineage().clone(),
            plan.command_id(),
            IdempotencyKeyDigest::from_hmac_bytes(
                DigestKeyId::new(1).expect("digest key"),
                [suffix; 32],
            ),
        );
        let request_id = RequestId::from_bytes(uuid_bytes(0x30 + suffix)).expect("request");
        let provenance_id =
            ProvenanceId::from_bytes(uuid_bytes(provenance_suffix)).expect("provenance");
        let logical_time =
            LogicalTime::new(Timestamp::new(i64::from(suffix), 0).expect("logical timestamp"));
        let mut partition = PartitionKeyBuilder::new(AggregateTypeId::new(1).expect("aggregate"));
        partition.push_u64(7).expect("partition component");
        let partition = partition.finish().expect("partition");
        let pending = StoredPendingAdmissionV1::new(
            identity.clone(),
            CanonicalInputHash::from_bytes([0x40 + suffix; 32]),
            request_id,
            plan.clone(),
            logical_time,
            actor.clone(),
            partition.clone(),
            StoredAdmittedProvenanceClaimsV1::default(),
        )
        .expect("pending");

        let binding_observation = prior_entity.as_ref().map_or_else(
            || EntityObservation::Absent(target.clone()),
            |record| EntityObservation::Present(record.clone()),
        );
        let range_entries = prior_entity.as_ref().map_or_else(Vec::new, |_| {
            vec![
                IndexRangeEntry::new(
                    index_key.index_id(),
                    index_key.clone(),
                    record(u64::from(suffix - 1)),
                )
                .expect("prior range entry"),
            ]
        });
        let snapshot_request = SnapshotRequest::new(
            plan.clone(),
            vec![target.clone()],
            Vec::new(),
            vec![range.clone()],
        )
        .expect("snapshot request");
        let snapshot = ReadSnapshot::new(
            &snapshot_request,
            sequence_value.checked_sub(1).and_then(CommitSequence::new),
            vec![binding_observation],
            Vec::new(),
            vec![
                IndexRangeObservation::new(range.clone(), prior_epoch, range_entries)
                    .expect("range observation"),
            ],
        )
        .expect("snapshot");
        let post = EntityPostImage::new(
            target.clone(),
            plan.contract_version(),
            record(u64::from(suffix)),
        )
        .expect("post image");
        let runtime_mutation = prior_entity.as_ref().map_or_else(
            || EntityMutation::Create(post.clone()),
            |prior| EntityMutation::Replace {
                expected_version: prior.entity_version(),
                post_image: post.clone(),
            },
        );
        let event_intent = EventIntent::new(
            EventTypeId::new(1).expect("event type"),
            record(u64::from(suffix)),
        )
        .expect("event intent");
        let outcome = riffdb_storage_api::DeclaredOutcome::new(
            OutcomeId::new(1).expect("outcome"),
            record(u64::from(suffix)),
        )
        .expect("outcome");
        let evaluated = riffdb_storage_api::EvaluatedCommand::new(
            &snapshot,
            vec![runtime_mutation],
            vec![event_intent],
            outcome.clone(),
            EvaluationBudget::v1(),
        )
        .expect("evaluated command");
        let context = PreEvaluationCommitContext::new(
            pending.clone(),
            riffdb_types::hash_partition_key(partition.as_bytes()),
            Vec::new(),
        )
        .expect("commit context");
        let candidates =
            IdempotencyLookupCandidatesV1::new(vec![identity.clone()]).expect("lookup candidates");
        let admission = AdmissionRequestV1::new(candidates, &context).expect("admission request");
        let intent = CommitIntent::new(context, evaluated, provenance_id).expect("commit intent");

        let expected = prior_entity
            .as_ref()
            .map_or(ExpectedEntityState::Absent, |record| {
                ExpectedEntityState::Present(record.entity_version())
            });
        let version = prior_entity
            .as_ref()
            .map_or(EntityVersion::first(), |record| {
                record
                    .entity_version()
                    .checked_next()
                    .expect("next version")
            });
        let entity = StoredEntityRecordV1::new(
            target.clone(),
            version,
            plan.contract_version(),
            DurableKeySchemaBindingV1::from_plan(&plan),
            record(u64::from(suffix)),
        )
        .expect("entity record");
        let mutation = riffdb_storage_api::CommittedEntityMutationV1::new(expected, entity)
            .expect("entity mutation");
        let index_record = StoredIndexEntryV2::new(
            index_key.clone(),
            DurableKeySchemaBindingV1::from_plan(&plan),
            record(u64::from(suffix)),
            partition.clone(),
        )
        .expect("index record");
        let affected_targets =
            AffectedIndexEpochTargets::new(vec![range.clone()]).expect("affected targets");
        let affected_current = AffectedEpochCurrentState::new(
            &affected_targets,
            vec![CurrentRangeObservation::new(range.clone(), prior_epoch)],
        )
        .expect("affected current");
        let advance = IndexEpochAdvanceV1::new(
            range.clone(),
            DurableKeySchemaBindingV1::from_plan(&plan),
            prior_epoch,
        )
        .expect("epoch advance");
        let encoded = EncodedWriteSetUpperBound::new(
            riffdb_storage_api::CommandWriteClassBreakdownV1::new(1, 1, 1, 1, 1, 1, 1, 1, 1, 1)
                .expect("encoded classes"),
        )
        .expect("encoded bound");
        let write_plan = CommandWriteSetPlanV1::new(
            &intent,
            affected_targets.clone(),
            affected_current,
            vec![IndexEntryMutationV1::Put(index_record)],
            vec![advance],
            encoded,
        )
        .expect("write plan");
        let assignment = AssignedCommandSequence::from_assigned(sequence);
        let event_id = EventId::new(sequence, 0);
        let event_payload = record(u64::from(suffix));
        let event_type = EventTypeId::new(1).expect("event type");
        let event = StoredDurableEventV1::new(
            event_id,
            event_type,
            event_payload.clone(),
            derive_event_hash_v1(event_id, event_type, &event_payload).expect("event hash"),
        )
        .expect("stored event");
        let partition_hash = riffdb_types::hash_partition_key(partition.as_bytes());
        let stored_outcome = StoredOutcomeV1::new(
            identity.clone(),
            sequence,
            request_id,
            plan.clone(),
            pending.canonical_input_hash(),
            actor.clone(),
            logical_time,
            partition.clone(),
            partition_hash,
            Vec::new(),
            outcome.clone(),
            StoredAdmittedProvenanceClaimsV1::default(),
            provenance_id,
            DurabilityMode::Memory,
        )
        .expect("stored outcome");
        let mutations = vec![mutation];
        let provenance = StoredProvenanceRecordV1::new(
            provenance_id,
            sequence,
            identity,
            request_id,
            plan.clone(),
            pending.canonical_input_hash(),
            actor.clone(),
            logical_time,
            partition_hash,
            Vec::new(),
            outcome.outcome_id(),
            vec![AffectedEntityV1::from_record(mutations[0].post_image())],
            vec![event_id],
            StoredAdmittedProvenanceClaimsV1::default(),
        )
        .expect("provenance");
        let commit = StoredCommitRecordV1::new(
            sequence,
            request_id,
            plan,
            pending.canonical_input_hash(),
            actor,
            logical_time,
            partition_hash,
            Vec::new(),
            StoredReadDependenciesV1::from_live(snapshot.read_dependencies())
                .expect("stored dependencies"),
            mutations.clone(),
            vec![event.clone()],
            outcome,
            provenance_id,
            vec![event_id],
            DurabilityMode::Memory,
        )
        .expect("commit");
        let records = AtomicCommandRecordSet::new(
            assignment,
            mutations,
            write_plan.clone(),
            stored_outcome,
            vec![event.clone()],
            vec![StoredOutboxIntentV1::new(event)],
            provenance,
            commit,
        )
        .expect("atomic record set");
        CommandFixture {
            admission,
            pending,
            intent,
            affected_targets,
            write_plan,
            records,
            target,
            range,
            index_key,
        }
    }

    fn admit(
        ports: &MemoryOperationalPorts,
        model: &mut AuthoritativeCommandModel,
        fixture: &CommandFixture,
    ) {
        assert_eq!(
            ports
                .admit_or_resolve(fixture.admission.clone())
                .expect("admit"),
            AdmissionResultV1::Created(fixture.pending.clone())
        );
        model
            .admit_pending(fixture.pending.clone())
            .expect("model admit");
    }

    fn empty_to_capacity(
        ports: &MemoryOperationalPorts,
        fixture: &CommandFixture,
    ) -> MemoryCandidateAwaitingCapacity<MemoryEmptyBatch> {
        let candidate = ports
            .begin_empty_batch()
            .expect("empty batch")
            .begin_candidate(Box::new(fixture.intent.clone()))
            .expect("candidate");
        let CandidateAdmissionResult::Proceed(candidate) =
            candidate.recheck_admission().expect("admission recheck")
        else {
            panic!("pending candidate must proceed");
        };
        let (candidate, _) = candidate
            .read_transaction_current()
            .expect("transaction current");
        candidate
            .plan_validated(fixture.affected_targets.clone())
            .read_affected_epoch_current()
            .expect("affected current")
    }

    fn nonempty_to_capacity(
        batch: MemoryNonEmptyBatch,
        fixture: &CommandFixture,
    ) -> MemoryCandidateAwaitingCapacity<MemoryNonEmptyBatch> {
        let CandidateStartResult::Started(candidate) = batch
            .begin_candidate(Box::new(fixture.intent.clone()))
            .expect("next candidate")
        else {
            panic!("batch must have capacity");
        };
        let CandidateAdmissionResult::Proceed(candidate) =
            candidate.recheck_admission().expect("admission recheck")
        else {
            panic!("pending candidate must proceed");
        };
        let (candidate, _) = candidate
            .read_transaction_current()
            .expect("transaction current");
        candidate
            .plan_validated(fixture.affected_targets.clone())
            .read_affected_epoch_current()
            .expect("affected current")
    }

    fn stage_empty(
        ports: &MemoryOperationalPorts,
        fixture: &CommandFixture,
    ) -> MemoryNonEmptyBatch {
        let candidate = empty_to_capacity(ports, fixture);
        let CandidateCapacityResult::Reserved(candidate) = candidate
            .reserve_capacity(fixture.write_plan.clone())
            .expect("reserve")
        else {
            panic!("fixture must reserve");
        };
        candidate
            .assign_sequence()
            .expect("assign sequence")
            .stage(fixture.records.clone())
            .expect("stage fixture")
    }

    #[test]
    fn empty_authoritative_scans_report_before_first_positions() {
        let ports = operational_ports(bundle());
        let fixture = command_fixture(1, 1, None, IndexEpochPosition::BeforeFirst, 0x50);
        let limit = StorageScanLimit::new(1).expect("scan limit");
        let index = ports
            .scan_index(
                AuthoritativeIndexScanRequest::new(fixture.range, None, limit)
                    .expect("index request"),
            )
            .expect("index scan");
        assert!(matches!(
            index,
            AuthoritativeIndexScanPage::ExactEnd {
                entries,
                epoch: IndexEpochPosition::BeforeFirst,
            } if entries.is_empty()
        ));

        let commits = ports
            .scan_commits(CommitScanRequest::initial(limit))
            .expect("commit scan");
        assert!(matches!(
            commits,
            CommitScanPageV1::ExactEnd {
                records,
                inclusive_upper: FrontierPosition::BeforeFirst,
            } if records.is_empty()
        ));
    }

    #[test]
    fn commit_page_values_enforce_fences_order_and_continuations() {
        let first = command_fixture(1, 1, None, IndexEpochPosition::BeforeFirst, 0x48);
        let first_entity = first.records.entities()[0].post_image().clone();
        let first_epoch = IndexEpochPosition::Value(first.records.index_epochs()[0].next());
        let second = command_fixture(2, 2, Some(first_entity), first_epoch, 0x49);
        let sequence_one = CommitSequence::first();
        let sequence_two = sequence_one.checked_next().expect("second sequence");
        let limit = StorageScanLimit::new(2).expect("scan limit");
        let initial = CommitScanRequest::initial(limit);
        let first_row =
            || EncodedPageItem::new(first.records.commit().clone(), memory_record_charge());
        let second_row =
            || EncodedPageItem::new(second.records.commit().clone(), memory_record_charge());

        assert_eq!(
            CommitScanRequest::continuing(sequence_two, sequence_one, limit),
            Err(StorageValueError::InvalidShape)
        );
        assert_eq!(
            CommitScanPageV1::exact_end(
                CommitScanRequest::initial(StorageScanLimit::new(1).expect("one-row limit")),
                FrontierPosition::AppliedThrough(sequence_two),
                vec![first_row(), second_row()],
            ),
            Err(StorageValueError::LimitExceeded)
        );
        assert_eq!(
            CommitScanPageV1::exact_end(initial, FrontierPosition::BeforeFirst, vec![first_row()],),
            Err(StorageValueError::NonCanonicalOrder)
        );
        assert_eq!(
            CommitScanPageV1::exact_end(
                initial,
                FrontierPosition::AppliedThrough(sequence_two),
                vec![first_row()],
            ),
            Err(StorageValueError::InvalidShape)
        );
        assert_eq!(
            CommitScanPageV1::page(
                initial,
                FrontierPosition::AppliedThrough(sequence_one),
                vec![first_row()],
                sequence_one,
            ),
            Err(StorageValueError::InvalidShape)
        );
        CommitScanPageV1::page(
            initial,
            FrontierPosition::AppliedThrough(sequence_two),
            vec![first_row()],
            sequence_one,
        )
        .expect("a row below the fence requires a continuation");
        let continuing = CommitScanRequest::continuing(sequence_one, sequence_two, limit)
            .expect("continuation request");
        CommitScanPageV1::exact_end(
            continuing,
            FrontierPosition::AppliedThrough(sequence_two),
            vec![second_row()],
        )
        .expect("the final contiguous row reaches the fence");
        assert_eq!(
            CommitScanPageV1::exact_end(
                CommitScanRequest::continuing(sequence_one, sequence_one, limit)
                    .expect("closed continuation"),
                FrontierPosition::AppliedThrough(sequence_one),
                vec![second_row()],
            ),
            Err(StorageValueError::NonCanonicalOrder)
        );
        assert_eq!(
            CommitScanPageV1::exact_end(
                initial,
                FrontierPosition::AppliedThrough(sequence_two),
                vec![second_row()],
            ),
            Err(StorageValueError::NonCanonicalOrder)
        );
    }

    #[test]
    fn staged_history_is_private_atomic_contiguous_and_matches_the_reference_model() {
        let ports = operational_ports(bundle());
        let mut model = AuthoritativeCommandModel::new();
        let first = command_fixture(1, 1, None, IndexEpochPosition::BeforeFirst, 0x51);
        let first_entity = first.records.entities()[0].post_image().clone();
        let first_epoch = IndexEpochPosition::Value(first.records.index_epochs()[0].next());
        let second = command_fixture(2, 2, Some(first_entity), first_epoch, 0x52);
        admit(&ports, &mut model, &first);
        admit(&ports, &mut model, &second);

        let candidate = ports
            .begin_empty_batch()
            .expect("empty batch")
            .begin_candidate(Box::new(first.intent.clone()))
            .expect("first candidate");
        let CandidateAdmissionResult::Proceed(candidate) = candidate
            .recheck_admission()
            .expect("first admission recheck")
        else {
            panic!("first pending must proceed");
        };
        let (candidate, current) = candidate
            .read_transaction_current()
            .expect("first current state");
        assert_eq!(
            current.bindings()[0].expected_state(),
            ExpectedEntityState::Absent
        );
        let candidate = candidate.plan_validated(first.affected_targets.clone());
        let candidate = candidate
            .read_affected_epoch_current()
            .expect("first affected current");
        let CandidateCapacityResult::Reserved(candidate) = candidate
            .reserve_capacity(first.write_plan.clone())
            .expect("first reserve")
        else {
            panic!("first command must reserve");
        };
        let candidate = candidate.assign_sequence().expect("first sequence");
        assert_eq!(candidate.assignment().assigned(), CommitSequence::first());
        let batch = candidate.stage(first.records.clone()).expect("stage first");
        assert!(
            ports
                .read_entity(&first.target)
                .expect("ordinary read")
                .is_none()
        );

        let CandidateStartResult::Started(candidate) = batch
            .begin_candidate(Box::new(second.intent.clone()))
            .expect("second candidate")
        else {
            panic!("second command must start");
        };
        let CandidateAdmissionResult::Proceed(candidate) = candidate
            .recheck_admission()
            .expect("second admission recheck")
        else {
            panic!("second pending must proceed");
        };
        let (candidate, current) = candidate
            .read_transaction_current()
            .expect("second current state");
        assert_eq!(
            current.bindings()[0].expected_state(),
            ExpectedEntityState::Present(EntityVersion::first())
        );
        let candidate = candidate.plan_validated(second.affected_targets.clone());
        let candidate = candidate
            .read_affected_epoch_current()
            .expect("second affected current");
        let CandidateCapacityResult::Reserved(candidate) = candidate
            .reserve_capacity(second.write_plan.clone())
            .expect("second reserve")
        else {
            panic!("second command must reserve");
        };
        let candidate = candidate.assign_sequence().expect("second sequence");
        assert_eq!(
            candidate.assignment().assigned(),
            CommitSequence::new(2).expect("second sequence")
        );
        let batch = candidate
            .stage(second.records.clone())
            .expect("stage second");
        assert!(
            ports
                .read_commit(CommitSequence::first())
                .expect("read")
                .is_none()
        );
        let committed = batch.commit(DurabilityMode::Memory).expect("commit batch");
        assert_eq!(committed.outcomes().len(), 2);

        model.apply_command(&first.records).expect("model first");
        model.apply_command(&second.records).expect("model second");
        let current = ports.read_entity(&second.target).expect("entity read");
        assert_eq!(current.as_ref(), model.entity(&second.target));
        assert_eq!(
            ports
                .read_commit(CommitSequence::new(2).expect("second"))
                .expect("commit read")
                .as_ref(),
            model.commit(CommitSequence::new(2).expect("second"))
        );
        assert_eq!(
            ports
                .read_durable_event(EventId::new(CommitSequence::first(), 0))
                .expect("event read")
                .as_ref(),
            model.event(EventId::new(CommitSequence::first(), 0))
        );
        ports
            .read(|state| {
                assert_eq!(state.commits.len(), model.commit_count());
                assert_eq!(
                    state
                        .index_entries
                        .first()
                        .and_then(MemoryIndexEntry::current_record),
                    model.index_entry(&second.index_key)
                );
                assert_eq!(
                    state.index_epochs.first(),
                    model.index_epoch(second.records.index_epochs()[0].target())
                );
                assert_eq!(
                    state.outbox_intents.last(),
                    model.outbox_intent(EventId::new(CommitSequence::new(2).expect("second"), 0,))
                );
                assert_eq!(state.pending_outbox_events.len(), 2);
                assert_eq!(state.synthetic_charges.command_classes.len(), 2);
                assert_eq!(state.historical_plan_references.len(), 1);
                Ok(())
            })
            .expect("model comparison");

        let limit = StorageScanLimit::new(1).expect("scan limit");
        let first_page = ports
            .scan_commits(CommitScanRequest::initial(limit))
            .expect("first commit page");
        let FrontierPosition::AppliedThrough(inclusive_upper) = first_page.inclusive_upper() else {
            panic!("two commits require a nonempty upper fence");
        };
        let CommitScanPageV1::Page { next_after, .. } = first_page else {
            panic!("two commits require pagination");
        };
        let index_page = ports
            .scan_index(
                AuthoritativeIndexScanRequest::new(second.range.clone(), None, limit)
                    .expect("index request"),
            )
            .expect("index scan");
        assert_eq!(index_page.entries().len(), 1);
        assert_eq!(
            index_page.epoch(),
            IndexEpochPosition::Value(second.records.index_epochs()[0].next())
        );
        let snapshot = ports
            .read_snapshot(
                SnapshotRequest::new(
                    plan(),
                    vec![second.target.clone()],
                    Vec::new(),
                    vec![second.range.clone()],
                )
                .expect("post-commit snapshot request"),
            )
            .expect("post-commit snapshot");
        assert_eq!(
            snapshot.observed_through(),
            Some(CommitSequence::new(2).expect("second"))
        );
        assert_eq!(
            snapshot.bindings()[0].expected_state(),
            ExpectedEntityState::Present(EntityVersion::new(2).expect("version two"))
        );
        assert_eq!(
            snapshot.ranges()[0].epoch(),
            IndexEpochPosition::Value(second.records.index_epochs()[0].next())
        );

        let third = command_fixture(
            3,
            3,
            Some(second.records.entities()[0].post_image().clone()),
            IndexEpochPosition::Value(second.records.index_epochs()[0].next()),
            0x53,
        );
        admit(&ports, &mut model, &third);
        stage_empty(&ports, &third)
            .commit(DurabilityMode::Memory)
            .expect("commit later command");
        let final_page = ports
            .scan_commits(
                CommitScanRequest::continuing(next_after, inclusive_upper, limit)
                    .expect("continuation request"),
            )
            .expect("frozen continuation page");
        assert_eq!(
            final_page.inclusive_upper(),
            FrontierPosition::AppliedThrough(inclusive_upper)
        );
        assert!(matches!(
            final_page,
            CommitScanPageV1::ExactEnd { records, .. }
                if records.len() == 1
                    && records[0].value().commit_sequence()
                        == CommitSequence::new(2).expect("second sequence")
        ));
    }

    #[test]
    fn staged_and_committed_provenance_collisions_abort_before_sequence_without_gaps() {
        let ports = operational_ports(bundle());
        let mut model = AuthoritativeCommandModel::new();
        let first = command_fixture(1, 1, None, IndexEpochPosition::BeforeFirst, 0x61);
        let first_entity = first.records.entities()[0].post_image().clone();
        let first_epoch = IndexEpochPosition::Value(first.records.index_epochs()[0].next());
        let collision = command_fixture(2, 2, Some(first_entity), first_epoch, 0x61);
        admit(&ports, &mut model, &first);
        admit(&ports, &mut model, &collision);

        let batch = stage_empty(&ports, &first);
        let candidate = nonempty_to_capacity(batch, &collision);
        assert!(matches!(
            candidate
                .reserve_capacity(collision.write_plan.clone())
                .expect("staged-prefix collision"),
            CandidateCapacityResult::ProvenanceIdCollision(_)
        ));
        assert!(ports.read_entity(&first.target).expect("entity").is_none());
        assert!(
            ports
                .read_commit(CommitSequence::first())
                .expect("commit")
                .is_none()
        );
        ports
            .read(|state| {
                let MemoryMetadataSlot::Retained(metadata) = &state.metadata else {
                    panic!("retained metadata");
                };
                assert_eq!(
                    metadata.application_sequence(),
                    ApplicationSequenceAllocator::initial()
                );
                Ok(())
            })
            .expect("allocator unchanged");

        stage_empty(&ports, &first)
            .commit(DurabilityMode::Memory)
            .expect("commit first");
        model.apply_command(&first.records).expect("model first");

        let candidate = empty_to_capacity(&ports, &collision);
        assert!(matches!(
            candidate
                .reserve_capacity(collision.write_plan.clone())
                .expect("committed collision"),
            CandidateCapacityResult::ProvenanceIdCollision(_)
        ));
        assert!(
            ports
                .read_commit(CommitSequence::new(2).expect("second"))
                .expect("second commit")
                .is_none()
        );

        let valid = command_fixture(
            2,
            2,
            Some(first.records.entities()[0].post_image().clone()),
            IndexEpochPosition::Value(first.records.index_epochs()[0].next()),
            0x62,
        );
        let candidate = empty_to_capacity(&ports, &valid);
        let CandidateCapacityResult::Reserved(candidate) = candidate
            .reserve_capacity(valid.write_plan.clone())
            .expect("valid reserve")
        else {
            panic!("valid command must reserve");
        };
        let assigned = candidate.assign_sequence().expect("valid assignment");
        assert_eq!(
            assigned.assignment().assigned(),
            CommitSequence::new(2).expect("second")
        );
        assigned
            .stage(valid.records.clone())
            .expect("stage valid")
            .rollback();
        assert!(
            ports
                .read_commit(CommitSequence::new(2).expect("second"))
                .expect("rolled back commit")
                .is_none()
        );
        assert_eq!(model.commit_count(), 1);
    }

    #[test]
    fn admission_corruption_and_durability_mismatch_publish_no_partial_state() {
        let ports = operational_ports(bundle());
        let fixture = command_fixture(1, 1, None, IndexEpochPosition::BeforeFirst, 0x71);
        let alternate = riffdb_storage_api::ExecutablePlanRef::new(
            ContractLineage::new("different-plan").expect("lineage"),
            ContractVersion::new(1).expect("version"),
            ContractBundleHash::from_bytes([0x91; 32]),
            CommandId::new(1).expect("command"),
            PlanHash::from_bytes([0x92; 32]),
        );
        let source_key = fixture
            .pending
            .identity()
            .storage_key()
            .expect("identity key");
        ports
            .acquire()
            .expect("corruption access")
            .write(|state| {
                state
                    .historical_plan_references
                    .push(HistoricalPlanReferenceRow {
                        order_key: crate::state::plan_evidence_order_key(fixture.pending.plan()),
                        plan: alternate,
                        source: HistoricalPlanReferenceSource::Admission(source_key),
                    });
                Ok(())
            })
            .expect("inject corrupt plan reference");
        assert_eq!(
            ports
                .admit_or_resolve(fixture.admission.clone())
                .expect_err("corrupt reference must reject")
                .kind(),
            StorageErrorKind::CorruptData
        );
        ports
            .read(|state| {
                assert!(state.admissions.is_empty());
                Ok(())
            })
            .expect("no partial admission");

        let ports = operational_ports(bundle());
        let mut model = AuthoritativeCommandModel::new();
        admit(&ports, &mut model, &fixture);
        assert_eq!(
            stage_empty(&ports, &fixture)
                .commit(DurabilityMode::Sync)
                .expect_err("durability substitution must abort")
                .kind(),
            StorageErrorKind::InvariantViolation
        );
        ports
            .read(|state| {
                let MemoryMetadataSlot::Retained(metadata) = &state.metadata else {
                    panic!("retained metadata");
                };
                assert_eq!(
                    metadata.application_sequence(),
                    ApplicationSequenceAllocator::initial()
                );
                assert!(state.entities.is_empty());
                assert!(state.commits.is_empty());
                assert!(state.events.is_empty());
                assert!(state.outbox_intents.is_empty());
                assert_eq!(
                    state.admissions,
                    vec![StoredAdmissionStateV1::Pending(fixture.pending.clone())]
                );
                Ok(())
            })
            .expect("durability abort is atomic");

        let event_id = EventId::new(CommitSequence::first(), 0);
        assert_eq!(
            StoredDurableEventV1::new(
                event_id,
                EventTypeId::new(1).expect("event type"),
                record(1),
                riffdb_types::EventHash::from_bytes([0; 32]),
            ),
            Err(StorageValueError::IdentityMismatch)
        );
    }

    #[test]
    fn execution_failure_rechecks_current_dependencies_and_terminalizes_without_sequence() {
        let ports = operational_ports(bundle());
        let fixture = command_fixture(1, 1, None, IndexEpochPosition::BeforeFirst, 0x72);
        let mut model = AuthoritativeCommandModel::new();
        admit(&ports, &mut model, &fixture);
        assert_eq!(
            ports
                .admit_or_resolve(fixture.admission.clone())
                .expect("resume admission"),
            AdmissionResultV1::Resumed(fixture.pending.clone())
        );
        let mismatched_pending = StoredPendingAdmissionV1::new(
            fixture.pending.identity().clone(),
            CanonicalInputHash::from_bytes([0xee; 32]),
            fixture.pending.admission_request_id(),
            fixture.pending.plan().clone(),
            fixture.pending.logical_time(),
            fixture.pending.actor().clone(),
            fixture.pending.partition_key().clone(),
            fixture.pending.provenance_claims().clone(),
        )
        .expect("mismatched pending");
        let mismatched_context = PreEvaluationCommitContext::new(
            mismatched_pending,
            riffdb_types::hash_partition_key(fixture.pending.partition_key().as_bytes()),
            Vec::new(),
        )
        .expect("mismatched context");
        let mismatch_request = AdmissionRequestV1::new(
            fixture.admission.lookup_candidates().clone(),
            &mismatched_context,
        )
        .expect("mismatch request");
        assert_eq!(
            ports
                .admit_or_resolve(mismatch_request)
                .expect("mismatch result"),
            AdmissionResultV1::InputMismatch
        );
        let snapshot = ports
            .read_snapshot(
                SnapshotRequest::new(
                    plan(),
                    vec![fixture.target.clone()],
                    Vec::new(),
                    vec![fixture.range.clone()],
                )
                .expect("snapshot request"),
            )
            .expect("snapshot");
        let request = ExecutionFailureTransitionRequestV1::new(
            fixture.pending.clone(),
            &snapshot,
            riffdb_types::ExecutionFailureCode::ArithmeticFault,
        )
        .expect("failure request");
        let ExecutionFailureAdmissionResult::Rechecked(rechecked) = ports
            .begin_execution_failure(request)
            .expect("begin failure")
        else {
            panic!("pending must recheck");
        };
        let (decision, current) = rechecked
            .read_transaction_current()
            .expect("failure current state");
        assert_eq!(
            current.bindings()[0].expected_state(),
            ExpectedEntityState::Absent
        );
        let failure = decision.terminalize().expect("terminalize failure");
        model
            .terminalize_execution_failure(failure.clone())
            .expect("model failure");
        assert!(matches!(
            ports
                .lookup_admission(fixture.admission.lookup_candidates().clone())
                .expect("failure lookup"),
            AdmissionLookupResultV1::Found(value)
                if *value == StoredAdmissionStateV1::ExecutionFailed(failure.clone())
        ));
        ports
            .read(|state| {
                let MemoryMetadataSlot::Retained(metadata) = &state.metadata else {
                    panic!("retained metadata");
                };
                assert_eq!(
                    metadata.application_sequence(),
                    ApplicationSequenceAllocator::initial()
                );
                assert!(state.commits.is_empty());
                assert_eq!(
                    model
                        .admission(fixture.pending.identity())
                        .expect("model admission"),
                    state.admissions.first()
                );
                Ok(())
            })
            .expect("failure model parity");
    }

    #[test]
    fn snapshot_and_index_scans_enforce_independent_incremental_bounds() {
        let ports = operational_ports(bundle());
        let plan = plan();
        let index_id = IndexId::new(1).expect("index");
        let mut prefix = IndexRangePrefixBuilder::new(index_id);
        prefix.push_u64(10).expect("prefix component");
        let range = IndexRangeTarget::new(prefix.finish());
        let mut entries = Vec::new();
        for value in 1_u64..=5 {
            let entity_type = EntityTypeId::new(1).expect("entity type");
            let mut entity_key = EntityKeyBuilder::new(entity_type);
            entity_key.push_u64(value).expect("entity component");
            let mut key = IndexEntryKeyBuilder::new(index_id);
            key.push_u64(10).expect("index component");
            entries.push(
                StoredIndexEntryV2::new(
                    key.finish(entity_key.finish().expect("entity key"))
                        .expect("index key"),
                    DurableKeySchemaBindingV1::from_plan(&plan),
                    large_record(u8::try_from(value).expect("small value")),
                    filtered_partition(value),
                )
                .expect("index entry"),
            );
        }
        ports
            .acquire()
            .expect("seed access")
            .write(move |state| {
                state.index_entries = entries
                    .into_iter()
                    .map(|entry| MemoryIndexEntry::current(entry, memory_record_charge()))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(())
            })
            .expect("seed index entries");

        assert_eq!(
            ports
                .read_snapshot(
                    SnapshotRequest::new(plan, Vec::new(), Vec::new(), vec![range.clone()])
                        .expect("snapshot request"),
                )
                .expect_err("range materialization must stop at four MiB")
                .kind(),
            StorageErrorKind::LimitExceeded
        );

        let limit = StorageScanLimit::new(2).expect("scan limit");
        let first = ports
            .scan_index(
                AuthoritativeIndexScanRequest::new(range.clone(), None, limit)
                    .expect("first request"),
            )
            .expect("first page");
        let AuthoritativeIndexScanPage::Page { next_after, .. } = first else {
            panic!("five rows require pagination");
        };
        let second = ports
            .scan_index(
                AuthoritativeIndexScanRequest::new(range.clone(), Some(next_after), limit)
                    .expect("second request"),
            )
            .expect("second page");
        let AuthoritativeIndexScanPage::Page { next_after, .. } = second else {
            panic!("three rows remain");
        };
        let final_page = ports
            .scan_index(
                AuthoritativeIndexScanRequest::new(range, Some(next_after), limit)
                    .expect("final request"),
            )
            .expect("final page");
        assert!(matches!(
            final_page,
            AuthoritativeIndexScanPage::ExactEnd { ref entries, .. } if entries.len() == 1
        ));
    }

    #[test]
    fn filtered_scan_enforces_all_explicit_none_and_lineage() {
        let ports = operational_ports(bundle());
        seed_filtered_rows(
            &ports,
            vec![
                (filtered_row(1, 1, "application-test"), 1),
                (filtered_row(2, 2, "foreign"), 1),
                (filtered_row(3, 3, "application-test"), 1),
            ],
        );

        let unfiltered = ports
            .scan_index(
                AuthoritativeIndexScanRequest::new(
                    filtered_range(),
                    None,
                    StorageScanLimit::new(500).expect("limit"),
                )
                .expect("unfiltered internal request"),
            )
            .expect("V2 rows remain valid for policy-neutral unfiltered reads");
        assert_eq!(unfiltered.entries().len(), 3);

        let all = ports
            .scan_index_filtered(filtered_request(IndexPartitionFilterScope::All, None, 500))
            .expect("all scan");
        assert!(matches!(
            all,
            FilteredAuthoritativeIndexScanPage::ExactEnd { ref entries, .. }
                if entries.len() == 2
                    && entries[0].value().key() == &filtered_index_key(1)
                    && entries[1].value().key() == &filtered_index_key(3)
        ));

        let explicit = ports
            .scan_index_filtered(filtered_request(
                IndexPartitionFilterScope::Explicit(vec![filtered_partition(3)]),
                None,
                500,
            ))
            .expect("explicit scan");
        assert!(matches!(
            explicit,
            FilteredAuthoritativeIndexScanPage::ExactEnd { ref entries, .. }
                if entries.len() == 1 && entries[0].value().key() == &filtered_index_key(3)
        ));

        let none = ports
            .scan_index_filtered(filtered_request(IndexPartitionFilterScope::None, None, 500))
            .expect("none scan");
        assert!(matches!(
            none,
            FilteredAuthoritativeIndexScanPage::ExactEnd { ref entries, .. }
                if entries.is_empty()
        ));
    }

    #[test]
    fn none_filter_returns_exact_end_without_inspecting_legacy_rows() {
        let ports = operational_ports(bundle());
        let legacy = StoredIndexEntryV1::new(
            filtered_index_key(1),
            DurableKeySchemaBindingV1::from_plan(&plan()),
            record(1),
        )
        .expect("legacy row");
        ports
            .acquire()
            .expect("seed access")
            .write(move |state| {
                state.index_entries = vec![MemoryIndexEntry::legacy(legacy)];
                Ok(())
            })
            .expect("seed legacy row");

        assert!(matches!(
            ports
                .scan_index_filtered(filtered_request(
                    IndexPartitionFilterScope::None,
                    None,
                    500
                ))
                .expect("none scan"),
            FilteredAuthoritativeIndexScanPage::ExactEnd { ref entries, .. }
                if entries.is_empty()
        ));
        assert_eq!(
            ports
                .scan_index_filtered(filtered_request(IndexPartitionFilterScope::All, None, 500))
                .expect_err("legacy row cannot enter current scan")
                .kind(),
            StorageErrorKind::IncompatibleFormat
        );
    }

    #[test]
    fn sparse_scan_stops_after_500_physical_candidates_with_empty_progress() {
        let ports = operational_ports(bundle());
        seed_filtered_rows(
            &ports,
            (1_u64..=501)
                .map(|value| (filtered_row(value, value, "foreign"), 1))
                .collect(),
        );

        let first = ports
            .scan_index_filtered(filtered_request(IndexPartitionFilterScope::All, None, 500))
            .expect("first sparse page");
        let FilteredAuthoritativeIndexScanPage::Page {
            entries,
            scanned_through,
            ..
        } = first
        else {
            panic!("501 physical candidates require progress");
        };
        assert!(entries.is_empty());
        assert_eq!(scanned_through, filtered_index_key(500));

        assert!(matches!(
            ports
                .scan_index_filtered(filtered_request(
                    IndexPartitionFilterScope::All,
                    Some(scanned_through),
                    500
                ))
                .expect("final sparse page"),
            FilteredAuthoritativeIndexScanPage::ExactEnd { ref entries, .. }
                if entries.is_empty()
        ));
    }

    #[test]
    fn filtered_scan_enforces_inspection_ceiling_for_sparse_and_returned_rows() {
        const TWO_MIB: usize = 2 * 1024 * 1024;
        let inspected = operational_ports(bundle());
        seed_filtered_rows(
            &inspected,
            vec![
                (filtered_row(1, 1, "foreign"), TWO_MIB),
                (filtered_row(2, 2, "foreign"), TWO_MIB),
                (filtered_row(3, 3, "foreign"), TWO_MIB),
            ],
        );
        let page = inspected
            .scan_index_filtered(filtered_request(IndexPartitionFilterScope::All, None, 500))
            .expect("inspection-bound page");
        assert!(matches!(
            page,
            FilteredAuthoritativeIndexScanPage::Page {
                ref entries,
                ref scanned_through,
                ..
            } if entries.is_empty() && scanned_through == &filtered_index_key(2)
        ));

        let returned = operational_ports(bundle());
        seed_filtered_rows(
            &returned,
            vec![
                (filtered_row(1, 1, "application-test"), TWO_MIB),
                (filtered_row(2, 2, "application-test"), TWO_MIB),
                (filtered_row(3, 3, "application-test"), TWO_MIB),
            ],
        );
        let page = returned
            .scan_index_filtered(filtered_request(IndexPartitionFilterScope::All, None, 500))
            .expect("return-bound page");
        assert!(matches!(
            page,
            FilteredAuthoritativeIndexScanPage::Page {
                ref entries,
                ref scanned_through,
                ..
            } if entries.len() == 2 && scanned_through == &filtered_index_key(2)
        ));
    }
}
