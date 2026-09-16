#![expect(
    clippy::expect_used,
    reason = "validated application transactions retain their nonempty command and canonical state components"
)]

//! Redb application admission and atomic command transactions.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU16;
use std::sync::Arc;

use redb::ReadableTable;
use riffdb_storage_api::{
    AbandonedCandidate, AdmissionLookupResultV1, AdmissionRepository, AdmissionRequestV1,
    AdmissionResultV1, AffectedEpochCurrentState, AffectedEpochCurrentStateBuilder,
    AffectedIndexEpochTargets, ApplicationCommandTransactionPort, ApplicationSequenceAllocator,
    AssignedCommandSequence, AtomicCommandRecordSet, AuditedCommittedBatchV1,
    AuditedExecutionFailureV1, CandidateAdmissionResult, CandidateCapacityResult,
    CandidateStartResult, CandidateValidationRejection, CommandAdmissionExpectationV1,
    CommandCandidateAdmission, CommandCandidateAffectedEpochRead, CommandCandidateAwaitingCapacity,
    CommandCandidateAwaitingValidation, CommandCandidateCapacityReserved,
    CommandCandidateSequenceAssigned, CommandCandidateStateRead, CommandDerivedIndexKindV1,
    CommandDerivedIndexManifestEntryV1, CommandDerivedMemberV1, CommandSegmentDigestV1,
    CommandSegmentManifestV1, CommandWriteSetPlanV1, CommitIntent, CommittedBatchV1,
    CommittedEntityTransitionV1, CurrentIndexGenerationObservation, CurrentRangeObservation,
    DeferredCommandEpoch, DeferredCommandEpochPort, DeferredNonEmptyCommandBatch,
    DetachedCommandGroupBatch, DetachedCommandRecordV1, DetachedCommandReservationV1,
    DurabilityMode, EmptyCommandBatch, EntityChainHeadV1, EntityChainStateV1, EntityObservation,
    EntityTarget, ExecutionFailureAdmissionRechecked, ExecutionFailureAdmissionResult,
    ExecutionFailureAwaitingDecision, ExecutionFailureTransitionPort,
    ExecutionFailureTransitionRequestV1, IdempotencyIdentity, IdempotencyIdentityKey,
    IdempotencyLookupCandidatesV1, IndexEntryMutationV1, IndexEpochPosition, IndexRangeEntry,
    MAX_INDEX_SCAN_INSPECTED_ENTRIES, NonEmptyCommandBatch, PartitionIndexTarget,
    ProvenanceIdCollision, ReadDependencies, ReadDependency, ReadSnapshot, ReadSnapshotBuilder,
    SnapshotRequest, StagedBatchMetrics, StagedCommandAuditLinkEvidenceV1, StagedCommandEvidenceV1,
    StorageError, StorageErrorKind, StorageValueError, StoredAdmissionStateV1,
    StoredCommandCapsuleV2, StoredCommandSegmentV1, StoredEventRouteV1, StoredExecutionFailedV1,
    StoredIndexEpochV1, StoredOutboxIntentV1, StoredOutcomeV1, StoredPendingAdmissionV1,
    StoredVectorEvidenceV1, TransactionCurrentPolicyRequestV1, TransactionCurrentPolicyStateV1,
    TransactionCurrentState, TransactionCurrentStateBuilder, TransactionCurrentVectorEvidenceV1,
    TransactionLocalCommandBatch, UniqueIndexOccupancy, UniqueOccupancyKind,
    UnpublishedAuditedBatchV1, ValidationReadRequest, VectorEvidenceIndexEntryV1,
    VectorEvidenceIndexPageV1, VectorEvidenceIndexRepository, VectorEvidenceIndexScanRequestV1,
    VectorEvidenceReadRequestV1, VectorHealthObservationV1, VectorObservationRepository,
    VectorObservationTargetV1, encode_capsule_command_record_set_v1,
};
use riffdb_types::{CommitSequence, EventId, ProvenanceId};

use crate::administration::{
    StagedCommandAuditRecordsV1, stage_command_service_audit_group_in_write,
    stage_service_audit_group_in_write,
};
use crate::codec::{
    IdempotencyRecordV1, decode_application_sequence_allocator_v1, decode_capability_record_v1,
    decode_database_identity_v1, decode_entity_record_v1, decode_history_incarnation_v1,
    decode_idempotency_record_v1, decode_index_entry_v2, decode_index_epoch_v1,
    decode_pending_admission_v1, decode_vector_evidence_index_v1, decode_vector_evidence_v1,
    decode_vector_health_observation_v1, decode_vector_observation_v1, encode_commit_record_v1,
    encode_durable_event_v1, encode_event_route_v1, encode_execution_failed_v1,
    encode_outbox_intent_v1, encode_pending_admission_v1, encode_provenance_record_v1,
    encode_stored_outcome_v1, encode_vector_evidence_index_v1, encode_vector_health_observation_v1,
    encode_vector_observation_v1,
};
use crate::command_authority::{command_member_at, command_member_at_access};
use crate::error::{codec_error, precommit_storage_error, table_error};
use crate::hooks::RedbTestOperation;
use crate::journal::{JournalCodecError, JournalMutation, JournalTable};
use crate::keys::{
    decode_index_entry_key, decode_vector_evidence_index_key, encode_application_sequence_key,
    encode_audit_by_request_key, encode_audit_key, encode_contract_bundle_key, encode_entity_key,
    encode_event_key, encode_event_route_key, encode_idempotency_key, encode_index_entry_key,
    encode_partition_index_key, encode_provenance_key, encode_vector_evidence_index_key,
    encode_vector_evidence_key, encode_vector_health_observation_key,
    encode_vector_observation_key,
};
use crate::layout::{
    COMMITS, CONTRACT_BUNDLES, ENTITIES, EVENT_ROUTES, EVENTS, IDEMPOTENCY, IDEMPOTENCY_PENDING,
    INDEX_EPOCHS, META_APPLICATION_SEQUENCE, META_DATABASE_ID, META_HISTORY_INCARNATION, OUTBOX,
    PROVENANCE,
};
use crate::store::{RedbDurabilityEpoch, RedbOperationalPorts, RedbReadAccess, RedbWriteAccess};
use crate::transient::TransientIndexDelta;

struct BatchCore {
    access: RedbWriteAccess,
    allocator: ApplicationSequenceAllocator,
    staged: Vec<StagedCommandEvidenceV1>,
    metrics: Option<StagedBatchMetrics>,
    /// Exact observations already materialized inside this write transaction.
    /// Entries are updated synchronously after every staged post-image, so a
    /// cache hit is byte-for-byte equivalent to another redb transaction-local
    /// read without repeating its B-tree lookup and durable decode.
    entity_observations: BTreeMap<EntityTarget, EntityObservation>,
    entity_observation_bytes: BTreeMap<EntityTarget, Option<Vec<u8>>>,
    index_generations: BTreeMap<PartitionIndexTarget, IndexEpochPosition>,
    pending_index_generations: BTreeMap<PartitionIndexTarget, PendingIndexGenerationPostImage>,
    reserved_provenance_ids: BTreeSet<ProvenanceId>,
    capsule_extensions: Vec<Vec<riffdb_storage_api::IndexEpochAdvanceV1>>,
    capsule_entity_transitions: Vec<Vec<CommittedEntityTransitionV1>>,
    capsule_independent_mutations: Vec<Vec<riffdb_storage_api::AuthoritativeMutationV3>>,
    detached: Vec<DetachedRedbCandidate>,
    detached_index_generation_base: Option<BTreeMap<PartitionIndexTarget, IndexEpochPosition>>,
}

struct PendingIndexGenerationPostImage {
    initial: IndexEpochPosition,
    final_record: StoredIndexEpochV1,
    final_bytes: riffdb_storage_api::CanonicalStoredEnvelopeV1,
}

struct DetachedRedbCandidate {
    assignment: AssignedCommandSequence,
    intent: Box<CommitIntent>,
    write_plan: CommandWriteSetPlanV1,
}

impl BatchCore {
    fn open(ports: &RedbOperationalPorts) -> Result<Self, StorageError> {
        Self::open_with_access(ports.begin_attributed_write(
            riffdb_storage_api::ChangelogAttributionV3::DirectApplicationOrServiceAuditGroup,
        )?)
    }

    fn open_with_access(access: RedbWriteAccess) -> Result<Self, StorageError> {
        let allocator = read_application_allocator(&access)?;
        Ok(Self {
            access,
            allocator,
            staged: Vec::new(),
            metrics: None,
            entity_observations: BTreeMap::new(),
            entity_observation_bytes: BTreeMap::new(),
            index_generations: BTreeMap::new(),
            pending_index_generations: BTreeMap::new(),
            reserved_provenance_ids: BTreeSet::new(),
            capsule_extensions: Vec::new(),
            capsule_entity_transitions: Vec::new(),
            capsule_independent_mutations: Vec::new(),
            detached: Vec::new(),
            detached_index_generation_base: None,
        })
    }
}

fn consume_staged_command_evidence(
    staged: Vec<StagedCommandEvidenceV1>,
) -> (Vec<StoredOutcomeV1>, Vec<StagedCommandAuditLinkEvidenceV1>) {
    let mut outcomes = Vec::with_capacity(staged.len());
    let mut command_links = Vec::with_capacity(staged.len());
    for evidence in staged {
        let (outcome, command_link) = evidence.into_outcome_and_command_audit_link();
        outcomes.push(outcome);
        command_links.push(command_link);
    }
    (outcomes, command_links)
}

/// Replaces the duplicated successful-command views with one canonical capsule
/// and payload-free lookup locators before the owning transaction can commit.
/// The full rows staged earlier are transaction-private validation material;
/// no reader or recovery boundary can observe them.
fn capsulate_command_rows(
    core: &mut BatchCore,
    command_links: Vec<StagedCommandAuditLinkEvidenceV1>,
    audit_records: Vec<StagedCommandAuditRecordsV1>,
) -> Result<
    (
        StoredCommandSegmentV1,
        Vec<riffdb_storage_api::StoredServiceAuditRecordV1>,
    ),
    StorageError,
> {
    let mut terminals = Vec::with_capacity(command_links.len());
    if audit_records.len() != command_links.len()
        || audit_records.len() != core.capsule_extensions.len()
        || audit_records.len() != core.capsule_entity_transitions.len()
        || audit_records.len() != core.capsule_independent_mutations.len()
    {
        return Err(storage_error(StorageErrorKind::InvariantViolation));
    }
    let mut capsules = Vec::with_capacity(command_links.len());
    for ((((link, audit), index_generation_transitions), entity_transitions), mutations) in
        command_links
            .into_iter()
            .zip(audit_records)
            .zip(core.capsule_extensions.drain(..))
            .zip(core.capsule_entity_transitions.drain(..))
            .zip(core.capsule_independent_mutations.drain(..))
    {
        let (started, terminal, predecessor_administration) = audit.into_parts();
        let base = link
            .into_capsule(started, terminal.clone())
            .map_err(invariant_value)?;
        let capsule =
            riffdb_storage_api::StoredCommandCapsuleV2::from_base_with_entity_transitions(
                base,
                index_generation_transitions,
                entity_transitions,
            )
            .map_err(invariant_value)?;
        let prefix = riffdb_storage_api::CommandPrefixEvidenceV1::new(
            riffdb_types::DualFrontier::new(
                CommitSequence::new(capsule.commit_sequence().get() - 1),
                predecessor_administration,
            ),
            riffdb_types::DualFrontier::new(
                Some(capsule.commit_sequence()),
                Some(terminal.administration_sequence()),
            ),
            mutations,
        )
        .map_err(crate::changelog_v3_write::value_error)?;
        let capsule = capsule
            .with_prefix_evidence(prefix)
            .map_err(invariant_value)?;
        capsules.push(capsule);
        terminals.push(terminal);
    }
    let wire_version = riffdb_storage_api::command_segment_wire_version_v1(&capsules);
    let access = &core.access;
    let (capsules, prepared_capsules) =
        access.prepare_command_segment_capsules(capsules, wire_version)?;
    let segment = build_command_segment(access, capsules)?;
    let commit_key = encode_application_sequence_key(segment.first_commit_sequence());
    let (segment, encoded_segment, encoding_metrics) = match prepared_capsules {
        Some(prepared_capsules) => {
            riffdb_storage_api::seal_and_encode_command_segment_with_prepared_capsules_v1(
                segment,
                prepared_capsules,
            )
            .map_err(codec_error)?
        }
        None => riffdb_storage_api::seal_and_encode_command_segment_with_metrics_v1(segment)
            .map_err(codec_error)?,
    };
    if !access.put_command_segment_value(
        commit_key.to_vec(),
        encoded_segment,
        segment.commands().len(),
        encoding_metrics.raw_envelope_bytes(),
    )? {
        return Err(storage_error(StorageErrorKind::InvariantViolation));
    }
    write_durable_command_locators(access, &segment)?;
    Ok((segment, terminals))
}

/// Writes the ADR-0165 locator rows that make a segment-owned command
/// resolvable by key without the transient population index.
///
/// A command's outcome, provenance and audits live inside its command segment,
/// and `COMMITS` is keyed by commit sequence, so an idempotency identity key, a
/// provenance id and an audit request id had no durable path to their owning
/// segment. With the transient index dormant, five read paths answered absent
/// for durably committed data, including the write-path admission lookup that
/// decides whether a retry re-executes.
///
/// Written in the same transaction and journal frame as the segment, so they
/// cost bytes and B-tree inserts but no additional fsync.
fn write_durable_command_locators(
    access: &RedbWriteAccess,
    segment: &StoredCommandSegmentV1,
) -> Result<(), StorageError> {
    for command in segment.commands() {
        let base = command.base();
        let locator = crate::codec::encode_command_locator_v1(
            riffdb_storage_api::StoredCommandLocatorV1::new(base.commit_sequence()),
        )?;
        let identity_key = identity_key(base.outcome().identity())?;
        access.put_command_value_assuming_absent(
            JournalTable::IdempotencyLocators,
            encode_idempotency_key(&identity_key).to_vec(),
            locator.as_bytes().to_vec(),
        )?;
        access.put_command_value_assuming_absent(
            JournalTable::ProvenanceLocators,
            encode_provenance_key(base.provenance().provenance_id()).to_vec(),
            locator.as_bytes().to_vec(),
        )?;
        // Two per command: the Started and terminal audit members. A fresh
        // administration sequence makes each key new by construction.
        for audit in [base.started_audit(), base.terminal_audit()] {
            access.put_command_value_assuming_absent(
                JournalTable::AuditByRequestLocators,
                encode_audit_by_request_key(audit.request_id(), audit.administration_sequence())
                    .to_vec(),
                locator.as_bytes().to_vec(),
            )?;
        }
    }
    Ok(())
}

/// Newest command segment's digest, read durably through a bounded window.
///
/// `command_segment_tail` answers `None` when the transient index is dormant,
/// which a bounded clean-close start leaves it. The previous fallback opened
/// `COMMITS` through `access.transaction()`, but this path is also reached
/// after the transaction has been taken, so it failed with an invariant
/// violation that surfaced as `storage is temporarily unavailable` on the first
/// command.
///
/// The newest segment's physical key is its first commit sequence, and a segment
/// holds at most `MAX_STAGED_COMMANDS` commands, so it lies within a bounded
/// window ending at the application frontier. That keeps this a window read
/// rather than the full-table scan bounded startup exists to remove.
fn last_command_segment_digest(
    access: &RedbWriteAccess,
) -> Result<Option<riffdb_storage_api::CommandSegmentDigestV1>, StorageError> {
    let allocator = read_application_allocator(access)?;
    let last = match allocator {
        ApplicationSequenceAllocator::Next(next) => {
            CommitSequence::new(next.get().saturating_sub(1))
        }
        ApplicationSequenceAllocator::Exhausted => CommitSequence::new(u64::MAX),
    };
    let Some(last) = last else {
        return Ok(None);
    };
    let window = u64::try_from(riffdb_storage_api::MAX_STAGED_COMMANDS)
        .map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?
        .saturating_sub(1);
    let first = CommitSequence::new(last.get().saturating_sub(window).max(1))
        .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
    let start = encode_application_sequence_key(first);
    let mut end = encode_application_sequence_key(last).to_vec();
    end.push(0);
    let rows = access.read_command_range(
        JournalTable::Commits,
        start.as_slice(),
        &end,
        riffdb_storage_api::MAX_STAGED_COMMANDS.saturating_add(1),
    )?;
    let Some((_, value)) = rows.last() else {
        return Ok(None);
    };
    match riffdb_storage_api::decode_command_segment_v1(&value[..]) {
        Ok(segment) => Ok(Some(segment.value().segment_digest())),
        Err(error)
            if error.kind() == riffdb_storage_api::DurableCodecErrorKind::UnexpectedRecordType =>
        {
            Ok(None)
        }
        Err(error) => Err(codec_error(error)),
    }
}

fn build_command_segment(
    access: &RedbWriteAccess,
    capsules: Vec<StoredCommandCapsuleV2>,
) -> Result<StoredCommandSegmentV1, StorageError> {
    let first = capsules
        .first()
        .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?
        .commit_sequence();
    let database_id_row = access
        .read_command_value(JournalTable::Meta, META_DATABASE_ID.as_bytes())?
        .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
    let database_id = *decode_database_identity_v1(&database_id_row)?.value();
    let history_incarnation_row = access
        .read_command_value(JournalTable::Meta, META_HISTORY_INCARNATION.as_bytes())?
        .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
    let history_incarnation = *decode_history_incarnation_v1(&history_incarnation_row)?.value();

    let predecessor = if let Some(tail) = access.command_segment_tail()? {
        tail.map(|(_, digest)| digest)
    } else {
        last_command_segment_digest(access)?
    };
    let manifest = build_command_segment_manifest(&capsules, first)?;
    StoredCommandSegmentV1::new(
        database_id,
        history_incarnation,
        predecessor,
        capsules,
        manifest,
        CommandSegmentDigestV1::from_bytes([0; 32]),
    )
    .map_err(invariant_value)
}

pub(crate) fn build_command_segment_manifest(
    capsules: &[StoredCommandCapsuleV2],
    segment_first: riffdb_types::CommitSequence,
) -> Result<CommandSegmentManifestV1, StorageError> {
    let entry_capacity = capsules.iter().try_fold(0_usize, |count, capsule| {
        capsule
            .events()
            .len()
            .checked_mul(2)
            .and_then(|event_entries| count.checked_add(6 + event_entries))
    });
    let mut entries = Vec::with_capacity(
        entry_capacity.ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?,
    );
    for (ordinal, capsule) in capsules.iter().enumerate() {
        let command_ordinal =
            u16::try_from(ordinal).map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?;
        let base = capsule.base();
        let identity = base
            .outcome()
            .identity()
            .storage_key()
            .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
        let command_entries = [
            (
                CommandDerivedIndexKindV1::Idempotency,
                CommandDerivedMemberV1::Command,
                encode_idempotency_key(&identity).to_vec(),
            ),
            (
                CommandDerivedIndexKindV1::Provenance,
                CommandDerivedMemberV1::Command,
                encode_provenance_key(base.provenance().provenance_id()).to_vec(),
            ),
            (
                CommandDerivedIndexKindV1::AuditSequence,
                CommandDerivedMemberV1::AuditStarted,
                encode_audit_key(base.started_audit().administration_sequence()).to_vec(),
            ),
            (
                CommandDerivedIndexKindV1::AuditSequence,
                CommandDerivedMemberV1::AuditTerminal,
                encode_audit_key(base.terminal_audit().administration_sequence()).to_vec(),
            ),
            (
                CommandDerivedIndexKindV1::AuditRequest,
                CommandDerivedMemberV1::AuditStarted,
                encode_audit_by_request_key(
                    base.started_audit().request_id(),
                    base.started_audit().administration_sequence(),
                )
                .to_vec(),
            ),
            (
                CommandDerivedIndexKindV1::AuditRequest,
                CommandDerivedMemberV1::AuditTerminal,
                encode_audit_by_request_key(
                    base.terminal_audit().request_id(),
                    base.terminal_audit().administration_sequence(),
                )
                .to_vec(),
            ),
        ];
        for (kind, member, exact_key) in command_entries {
            entries.push(
                CommandDerivedIndexManifestEntryV1::new(
                    kind,
                    member,
                    exact_key,
                    command_ordinal,
                    0,
                    segment_first,
                )
                .map_err(invariant_value)?,
            );
        }
        for (event_ordinal, event) in capsule.events().iter().enumerate() {
            let member_ordinal = u16::try_from(event_ordinal)
                .map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?;
            for (kind, key) in [
                (
                    CommandDerivedIndexKindV1::EventRoute,
                    encode_event_route_key(base.commit().partition_hash(), event.event_id())
                        .to_vec(),
                ),
                (
                    CommandDerivedIndexKindV1::PendingOutbox,
                    encode_event_key(event.event_id()).to_vec(),
                ),
            ] {
                entries.push(
                    CommandDerivedIndexManifestEntryV1::new(
                        kind,
                        CommandDerivedMemberV1::Event,
                        key,
                        command_ordinal,
                        member_ordinal,
                        segment_first,
                    )
                    .map_err(invariant_value)?,
                );
            }
        }
    }
    entries.sort_unstable();
    CommandSegmentManifestV1::new(entries).map_err(invariant_value)
}

/// Empty redb command batch. This state has no commit operation.
pub struct RedbEmptyBatch {
    core: BatchCore,
}

/// Nonempty redb command batch holding one uncommitted engine transaction.
pub struct RedbNonEmptyBatch {
    core: BatchCore,
}

/// Candidate before exact pending-admission recheck.
pub struct RedbCandidateAdmission<P> {
    prior: P,
    intent: Box<CommitIntent>,
}

/// Candidate permitted to read transaction-current values.
pub struct RedbCandidateStateRead<P> {
    prior: P,
    intent: Box<CommitIntent>,
}

/// Candidate awaiting the coordinator's validation decision.
pub struct RedbCandidateAwaitingValidation<P> {
    prior: P,
    intent: Box<CommitIntent>,
}

/// Candidate permitted to read mutation-affected epochs.
pub struct RedbCandidateAffectedEpochRead<P> {
    prior: P,
    intent: Box<CommitIntent>,
    affected_targets: AffectedIndexEpochTargets,
}

/// Candidate holding complete transaction-current observations.
pub struct RedbCandidateAwaitingCapacity<P> {
    prior: P,
    intent: Box<CommitIntent>,
    affected_targets: AffectedIndexEpochTargets,
    affected_current: AffectedEpochCurrentState,
}

/// Candidate with capacity and provenance uniqueness reserved.
pub struct RedbCandidateCapacityReserved<P> {
    prior: P,
    intent: Box<CommitIntent>,
    write_plan: CommandWriteSetPlanV1,
}

/// Candidate owning a transaction-local, still-invisible sequence.
pub struct RedbCandidateSequenceAssigned<P> {
    prior: P,
    intent: Box<CommitIntent>,
    write_plan: CommandWriteSetPlanV1,
    assignment: AssignedCommandSequence,
}

/// Rechecked short pending-to-failure transition.
pub struct RedbExecutionFailureRechecked {
    access: RedbWriteAccess,
    request: ExecutionFailureTransitionRequestV1,
}

/// Pending-to-failure transition after current reads.
pub struct RedbExecutionFailureAwaitingDecision {
    access: RedbWriteAccess,
    request: ExecutionFailureTransitionRequestV1,
    current: TransactionCurrentState,
}

impl ApplicationCommandTransactionPort for RedbOperationalPorts {
    type EmptyBatch = RedbEmptyBatch;

    fn begin_empty_batch(&self) -> Result<Self::EmptyBatch, StorageError> {
        Ok(RedbEmptyBatch {
            core: BatchCore::open(self)?,
        })
    }
}

impl VectorObservationRepository for RedbOperationalPorts {
    fn read_vector_observation(
        &self,
        target: &VectorObservationTargetV1,
    ) -> Result<Option<riffdb_storage_api::VectorObservationCountsV1>, StorageError> {
        let key = encode_vector_observation_key(target)
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        self.begin_composite_read()?
            .read_value(JournalTable::VectorObservations, &key)?
            .map(|bytes| decode_vector_observation_v1(&bytes).map(decoded_value))
            .transpose()
    }

    fn read_vector_health_observation(
        &self,
        lineage: &riffdb_types::ContractLineage,
    ) -> Result<Option<VectorHealthObservationV1>, StorageError> {
        let key = encode_vector_health_observation_key(lineage)
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        self.begin_composite_read()?
            .read_value(JournalTable::VectorObservations, &key)?
            .map(|bytes| decode_vector_health_observation_v1(&bytes).map(decoded_value))
            .transpose()
    }
}

impl VectorEvidenceIndexRepository for RedbOperationalPorts {
    fn scan_vector_evidence_index(
        &self,
        request: &VectorEvidenceIndexScanRequestV1,
    ) -> Result<VectorEvidenceIndexPageV1, StorageError> {
        let prefix = crate::keys::encode_vector_evidence_index_prefix(request.target())
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let upper = exclusive_prefix_end(&prefix)
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
        let start = request
            .after()
            .map_or_else(
                || Ok(prefix.clone()),
                |after| crate::keys::encode_vector_evidence_index_key(request.target(), after),
            )
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let limit = usize::from(request.limit().get());
        let rows = self.begin_composite_read()?.read_range(
            JournalTable::VectorEvidenceIndex,
            &start,
            &upper,
            limit.saturating_add(2),
        )?;
        let mut entries = Vec::with_capacity(limit);
        let mut encoded_bytes = 0usize;
        let mut more = false;
        for (key, value) in rows {
            let (target, entity_key) = decode_vector_evidence_index_key(&key)
                .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
            if request.after().is_some_and(|after| after == &entity_key) {
                continue;
            }
            if entries.len() == limit {
                more = true;
                break;
            }
            let decoded = decode_vector_evidence_index_v1(&value)?;
            if decoded.value().target() != &target
                || decoded.value().entity_key() != &entity_key
                || &target != request.target()
            {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
            encoded_bytes = encoded_bytes
                .checked_add(decoded.encoded_content_charge().get())
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            entries.push(decoded.into_parts().0);
        }
        let continuation = more.then(|| {
            entries
                .last()
                .expect("non-exact page is nonempty")
                .entity_key()
                .clone()
        });
        VectorEvidenceIndexPageV1::new(
            request.target(),
            entries,
            continuation,
            !more,
            encoded_bytes,
        )
        .map_err(|error| match error {
            StorageValueError::LimitExceeded => storage_error(StorageErrorKind::LimitExceeded),
            _ => storage_error(StorageErrorKind::InvariantViolation),
        })
    }
}

impl DeferredCommandEpochPort for RedbOperationalPorts {
    type Epoch = RedbDurabilityEpoch;

    fn begin_deferred_command_epoch(&self) -> Result<Self::Epoch, StorageError> {
        self.begin_deferred_epoch()
    }
}

impl DeferredCommandEpoch for RedbDurabilityEpoch {
    type EmptyBatch = RedbEmptyBatch;
    type Fence = crate::store::RedbSubmittedCommandFence;

    fn begin_empty_batch(self) -> Result<Self::EmptyBatch, StorageError> {
        Ok(RedbEmptyBatch {
            core: BatchCore::open_with_access(self.begin_write()?)?,
        })
    }

    fn seal(self) -> Result<Self::Fence, StorageError> {
        self.seal()
    }
}

impl EmptyCommandBatch for RedbEmptyBatch {
    type Candidate = RedbCandidateAdmission<Self>;

    fn begin_candidate(self, intent: Box<CommitIntent>) -> Result<Self::Candidate, StorageError> {
        Ok(RedbCandidateAdmission {
            prior: self,
            intent,
        })
    }

    fn rollback(self) {}
}

impl TransactionLocalCommandBatch for RedbEmptyBatch {
    fn read_transaction_local_snapshot(
        &self,
        request: SnapshotRequest,
    ) -> Result<ReadSnapshot, StorageError> {
        read_transaction_local_snapshot(&self.core, request)
    }
}

impl NonEmptyCommandBatch for RedbNonEmptyBatch {
    type Candidate = RedbCandidateAdmission<Self>;

    fn metrics(&self) -> StagedBatchMetrics {
        self.core
            .metrics
            .expect("nonempty batch construction installs metrics")
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
        Ok(CandidateStartResult::Started(RedbCandidateAdmission {
            prior: self,
            intent,
        }))
    }

    fn commit(mut self, durability: DurabilityMode) -> Result<CommittedBatchV1, StorageError> {
        if durability == DurabilityMode::Memory
            || self
                .core
                .staged
                .iter()
                .any(|evidence| evidence.outcome().durability_mode() != durability)
            // WP-608: ENTITY_CHAIN_HEADS are valid only when the corresponding
            // transition is retained in a canonical command capsule. This
            // storage-only completion path has no service-audit lifecycle from
            // which such a capsule can be built, so every entity-bearing batch
            // refuses while its transaction is still private.
            || self
                .core
                .capsule_entity_transitions
                .iter()
                .any(|transitions| !transitions.is_empty())
        {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let (outcomes, pending_events) = materialize_uncapsulated_command_rows(
            &self.core.access,
            std::mem::take(&mut self.core.staged),
        )?;
        let committed = CommittedBatchV1::new(outcomes, durability).map_err(invariant_value)?;
        let delta = (!pending_events.is_empty())
            .then_some(TransientIndexDelta::PendingOutboxInserted(pending_events));
        flush_index_generation_post_images(&mut self.core)?;
        stage_application_allocator(&self.core.access, self.core.allocator)?;
        self.core
            .access
            .commit_for_with_delta(RedbTestOperation::CommandBatch, delta)?;
        Ok(committed)
    }

    fn commit_with_service_audit_transitions(
        mut self,
        durability: DurabilityMode,
        transitions: Vec<riffdb_storage_api::CommandServiceAuditTransitionV1>,
    ) -> Result<AuditedCommittedBatchV1, StorageError> {
        if durability == DurabilityMode::Memory
            || self.core.staged.len() != transitions.len()
            || transitions.is_empty()
            || self
                .core
                .staged
                .iter()
                .any(|evidence| evidence.outcome().durability_mode() != durability)
        {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let (outcomes, command_links) =
            consume_staged_command_evidence(std::mem::take(&mut self.core.staged));
        let committed = CommittedBatchV1::new(outcomes, durability).map_err(invariant_value)?;
        let records = stage_command_service_audit_group_in_write(
            &self.core.access,
            transitions,
            &command_links,
        )?;
        let (segment, terminal_records) =
            capsulate_command_rows(&mut self.core, command_links, records)?;
        let audited = AuditedCommittedBatchV1::new(committed, terminal_records.clone())
            .map_err(invariant_value)?;
        // Request and outbox membership are derived from the canonical segment
        // manifest. Publish their transient accelerators only after durability.
        let delta = Some(TransientIndexDelta::CommandSegmentPublished(Arc::new(
            segment,
        )));
        flush_index_generation_post_images(&mut self.core)?;
        stage_application_allocator(&self.core.access, self.core.allocator)?;
        self.core
            .access
            .commit_for_with_delta(RedbTestOperation::CommandBatch, delta)?;
        Ok(audited)
    }

    fn rollback(self) {}
}

/// Completes the storage-only non-audited batch path. Public application
/// commands always use the audited capsule path; this retained lower-level
/// conformance path restores the historical complete rows before its direct
/// durable boundary so it never commits dangling capsule locators.
fn materialize_uncapsulated_command_rows(
    access: &RedbWriteAccess,
    staged: Vec<StagedCommandEvidenceV1>,
) -> Result<(Vec<StoredOutcomeV1>, Vec<EventId>), StorageError> {
    let transaction = access.transaction()?;
    let mut outcomes = Vec::with_capacity(staged.len());
    let event_count = staged
        .iter()
        .map(|evidence| evidence.event_ids().len())
        .sum();
    let mut event_ids = Vec::with_capacity(event_count);
    let mut idempotency = transaction.open_table(IDEMPOTENCY).map_err(table_error)?;
    let mut provenances = transaction.open_table(PROVENANCE).map_err(table_error)?;
    let mut commits = transaction.open_table(COMMITS).map_err(table_error)?;
    let mut events = transaction.open_table(EVENTS).map_err(table_error)?;
    let mut event_routes = transaction.open_table(EVENT_ROUTES).map_err(table_error)?;
    let mut outbox = transaction.open_table(OUTBOX).map_err(table_error)?;
    let mut journal = access.retains_journal_mutations().then(Vec::new);
    for evidence in staged {
        let (outcome, provenance, commit) = evidence.into_authority_parts();
        let sequence = commit.commit_sequence();
        let identity_key = identity_key(outcome.identity())?;
        let idempotency_key = encode_idempotency_key(&identity_key);
        let provenance_key = encode_provenance_key(provenance.provenance_id());

        let encoded_outcome = encode_stored_outcome_v1(&outcome)?;
        if idempotency
            .insert(idempotency_key, encoded_outcome.as_bytes())
            .map_err(precommit_storage_error)?
            .is_some()
        {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        if let Some(journal) = journal.as_mut() {
            journal.push(
                JournalMutation::put(
                    JournalTable::Idempotency,
                    idempotency_key.to_vec(),
                    encoded_outcome.as_bytes().to_vec(),
                )
                .map_err(journal_codec_error)?,
            );
        }

        let encoded_provenance = encode_provenance_record_v1(&provenance)?;
        if provenances
            .insert(provenance_key.as_slice(), encoded_provenance.as_bytes())
            .map_err(precommit_storage_error)?
            .is_some()
        {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        if let Some(journal) = journal.as_mut() {
            journal.push(
                JournalMutation::put(
                    JournalTable::Provenance,
                    provenance_key.to_vec(),
                    encoded_provenance.as_bytes().to_vec(),
                )
                .map_err(journal_codec_error)?,
            );
        }

        for event in commit.events() {
            let key = encode_event_key(event.event_id());
            let route_key = encode_event_route_key(commit.partition_hash(), event.event_id());
            let route = StoredEventRouteV1::new(
                event.event_id(),
                event.event_type_id(),
                event.event_hash(),
            );
            let intent = StoredOutboxIntentV1::new(event.clone());
            let encoded_event = encode_durable_event_v1(event)?;
            let encoded_route = encode_event_route_v1(route)?;
            let encoded_intent = encode_outbox_intent_v1(&intent)?;
            if events
                .insert(key.as_slice(), encoded_event.as_bytes())
                .map_err(precommit_storage_error)?
                .is_some()
                || event_routes
                    .insert(route_key.as_slice(), encoded_route.as_bytes())
                    .map_err(precommit_storage_error)?
                    .is_some()
                || outbox
                    .insert(key.as_slice(), encoded_intent.as_bytes())
                    .map_err(precommit_storage_error)?
                    .is_some()
            {
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
            if let Some(journal) = journal.as_mut() {
                for (table, exact_key, value) in [
                    (JournalTable::Events, key.to_vec(), encoded_event.as_bytes()),
                    (
                        JournalTable::EventRoutes,
                        route_key.to_vec(),
                        encoded_route.as_bytes(),
                    ),
                    (
                        JournalTable::Outbox,
                        key.to_vec(),
                        encoded_intent.as_bytes(),
                    ),
                ] {
                    journal.push(
                        JournalMutation::put(table, exact_key, value.to_vec())
                            .map_err(journal_codec_error)?,
                    );
                }
            }
        }

        let encoded_commit = encode_commit_record_v1(&commit)?;
        let commit_key = encode_application_sequence_key(sequence);
        if commits
            .insert(commit_key.as_slice(), encoded_commit.as_bytes())
            .map_err(precommit_storage_error)?
            .is_some()
        {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        if let Some(journal) = journal.as_mut() {
            journal.push(
                JournalMutation::put(
                    JournalTable::Commits,
                    commit_key.to_vec(),
                    encoded_commit.as_bytes().to_vec(),
                )
                .map_err(journal_codec_error)?,
            );
        }
        event_ids.extend(provenance.event_ids());
        outcomes.push(outcome);
    }
    if let Some(journal) = journal {
        access.record_journal_mutations(journal)?;
    }
    Ok((outcomes, event_ids))
}

impl DeferredNonEmptyCommandBatch for RedbNonEmptyBatch {
    type Epoch = RedbDurabilityEpoch;

    fn apply_unpublished_with_service_audit_transitions(
        mut self,
        durability: DurabilityMode,
        transitions: Vec<riffdb_storage_api::CommandServiceAuditTransitionV1>,
    ) -> Result<Self::Epoch, StorageError> {
        if durability == DurabilityMode::Memory
            || self.core.staged.len() != transitions.len()
            || transitions.is_empty()
            || self
                .core
                .staged
                .iter()
                .any(|evidence| evidence.outcome().durability_mode() != durability)
        {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let metrics = self.metrics();
        let (outcomes, command_links) =
            consume_staged_command_evidence(std::mem::take(&mut self.core.staged));
        let records = stage_command_service_audit_group_in_write(
            &self.core.access,
            transitions,
            &command_links,
        )?;
        let (segment, terminals) = capsulate_command_rows(&mut self.core, command_links, records)?;
        let unpublished =
            UnpublishedAuditedBatchV1::new(outcomes, terminals).map_err(invariant_value)?;
        let delta = Some(TransientIndexDelta::CommandSegmentPublished(Arc::new(
            segment,
        )));
        flush_index_generation_post_images(&mut self.core)?;
        stage_application_allocator(&self.core.access, self.core.allocator)?;
        self.core
            .access
            .apply_unpublished(unpublished, metrics, delta)
    }
}

impl TransactionLocalCommandBatch for RedbNonEmptyBatch {
    fn read_transaction_local_snapshot(
        &self,
        request: SnapshotRequest,
    ) -> Result<ReadSnapshot, StorageError> {
        read_transaction_local_snapshot(&self.core, request)
    }
}

fn read_transaction_local_snapshot(
    core: &BatchCore,
    request: SnapshotRequest,
) -> Result<ReadSnapshot, StorageError> {
    let observed_through = core
        .staged
        .last()
        .map(|evidence| evidence.outcome().commit_sequence())
        .or_else(|| match core.allocator {
            ApplicationSequenceAllocator::Next(next)
                if next == riffdb_types::CommitSequence::first() =>
            {
                None
            }
            ApplicationSequenceAllocator::Next(next) => {
                riffdb_types::CommitSequence::new(next.get().saturating_sub(1))
            }
            ApplicationSequenceAllocator::Exhausted => riffdb_types::CommitSequence::new(u64::MAX),
        });
    let mut snapshot =
        ReadSnapshotBuilder::new(&request, observed_through).map_err(materialization_value)?;

    for target in request.binding_targets() {
        snapshot
            .push_binding(entity_observation_from_access(&core.access, target)?.0)
            .map_err(materialization_value)?;
    }
    for target in request.root_validation_targets() {
        snapshot
            .push_root_validation(entity_observation_from_access(&core.access, target)?.0)
            .map_err(materialization_value)?;
    }
    for target in request.cascade_targets() {
        snapshot
            .push_cascade_predecessor(entity_observation_from_access(&core.access, target)?.0)
            .map_err(materialization_value)?;
    }
    for (position, target) in request.range_targets().iter().enumerate() {
        let epoch = core
            .index_generations
            .get(target.generation_target())
            .copied()
            .map_or_else(
                || epoch_position_from_access(&core.access, target.generation_target()),
                Ok,
            )?;
        let mut range = snapshot
            .begin_range(target.clone(), epoch)
            .map_err(materialization_value)?;
        let prefix = target.prefix().as_bytes();
        let upper = exclusive_prefix_end(prefix)
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        let entries = core.access.read_command_range(
            JournalTable::SecondaryIndexes,
            prefix,
            upper.as_slice(),
            riffdb_storage_api::MAX_INDEX_SCAN_INSPECTED_ENTRIES,
        )?;
        let entry_limit = request
            .range_entry_limit(position)
            .ok_or_else(|| materialization_value(StorageValueError::IdentityMismatch))?;
        let mut retained = 0usize;
        for (physical_key, encoded) in entries {
            let key = decode_index_entry_key(&physical_key)
                .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
            let decoded = decode_index_entry_v2(&encoded)?;
            if decoded.value().key() != &key {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
            if decoded.value().partition_key() != target.generation_target().partition_key() {
                continue;
            }
            if retained == entry_limit {
                break;
            }
            range
                .push_entry(
                    IndexRangeEntry::new(
                        key.index_id(),
                        key,
                        decoded.value().covered_values().clone(),
                    )
                    .map_err(|_| storage_error(StorageErrorKind::CorruptData))?,
                )
                .map_err(materialization_value)?;
            retained += 1;
        }
        range.finish().map_err(materialization_value)?;
    }
    snapshot.finish().map_err(materialization_value)
}

impl AdmissionRepository for RedbOperationalPorts {
    fn admit_or_resolve(
        &self,
        request: AdmissionRequestV1,
    ) -> Result<AdmissionResultV1, StorageError> {
        let mut results = self.admit_or_resolve_group(vec![request])?;
        results
            .pop()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))
    }

    fn admit_or_resolve_group(
        &self,
        requests: Vec<AdmissionRequestV1>,
    ) -> Result<Vec<AdmissionResultV1>, StorageError> {
        if requests.is_empty() || requests.len() > riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS
        {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }
        let access = self
            .begin_attributed_write(riffdb_storage_api::ChangelogAttributionV3::CommandAdmission)?;
        access.arm_fresh_locator_coverage()?;
        let mut created_any = false;
        let staged = stage_admission_group(&access, requests.iter())?;
        let mut results = Vec::with_capacity(staged.len());
        for (result, created) in staged {
            created_any |= created;
            results.push(result);
        }
        if created_any {
            access.commit_for(RedbTestOperation::Admission)?;
        } else {
            access.abort()?;
        }
        Ok(results)
    }

    fn lookup_admission(
        &self,
        candidates: IdempotencyLookupCandidatesV1,
    ) -> Result<AdmissionLookupResultV1, StorageError> {
        let mut results = self.lookup_admission_group(vec![candidates])?;
        results
            .pop()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))
    }

    fn lookup_admission_group(
        &self,
        candidates: Vec<IdempotencyLookupCandidatesV1>,
    ) -> Result<Vec<AdmissionLookupResultV1>, StorageError> {
        if candidates.is_empty()
            || candidates.len() > riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS
        {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }
        let transaction = self.begin_composite_read()?;
        candidates
            .iter()
            .map(|candidate| {
                match matching_admissions_from_access(self, &transaction, candidate)?.as_slice() {
                    [] => Ok(AdmissionLookupResultV1::NotFound),
                    [value] => Ok(AdmissionLookupResultV1::Found(Box::new(value.clone()))),
                    [_, ..] => Ok(AdmissionLookupResultV1::MultipleMatches),
                }
            })
            .collect()
    }
}

/// Stages a bounded FIFO admission group while reusing one handle for each
/// transaction-local authority table. Every request retains its independent
/// identity lookup, plan-retirement check, canonical encoding, and insert
/// assertion.
pub(crate) fn stage_admission_group<'a, I>(
    access: &RedbWriteAccess,
    requests: I,
) -> Result<Vec<(AdmissionResultV1, bool)>, StorageError>
where
    I: IntoIterator<Item = &'a AdmissionRequestV1>,
    I::IntoIter: ExactSizeIterator,
{
    let requests = requests.into_iter();
    let request_count = requests.len();
    if request_count == 0 || request_count > riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS {
        return Err(storage_error(StorageErrorKind::LimitExceeded));
    }
    let transaction = access.transaction()?;
    let mut pending = transaction
        .open_table(IDEMPOTENCY_PENDING)
        .map_err(table_error)?;
    let terminal = transaction.open_table(IDEMPOTENCY).map_err(table_error)?;
    let commits = transaction.open_table(COMMITS).map_err(table_error)?;
    let events = transaction.open_table(EVENTS).map_err(table_error)?;
    let retirements = transaction
        .open_table(crate::layout::CONTRACT_WRITE_RETIREMENTS)
        .map_err(table_error)?;
    let bundles = transaction
        .open_table(CONTRACT_BUNDLES)
        .map_err(table_error)?;
    struct PreparedAdmission {
        result: AdmissionResultV1,
        encoded: Option<(Vec<u8>, Vec<u8>)>,
    }
    let mut prepared: Vec<PreparedAdmission> = Vec::with_capacity(request_count);
    for request in requests {
        let mut matches = matching_admissions_from_tables(
            &pending,
            &terminal,
            &commits,
            &events,
            request.lookup_candidates(),
            |identity| command_outcome_from_write_indexes(access, identity),
        )?;
        for prior in &prepared {
            if let AdmissionResultV1::Created(pending) = &prior.result
                && request
                    .lookup_candidates()
                    .as_slice()
                    .contains(pending.identity())
            {
                matches.push(StoredAdmissionStateV1::Pending(pending.clone()));
            }
        }
        let result = if matches.len() > 1 {
            PreparedAdmission {
                result: AdmissionResultV1::MultipleMatches,
                encoded: None,
            }
        } else if let Some(existing) = matches.first() {
            PreparedAdmission {
                result: admission_result(existing, request.proposed_pending()),
                encoded: None,
            }
        } else {
            if !plan_bundle_exists_from_tables(
                &retirements,
                &bundles,
                request.proposed_pending().plan(),
            )? {
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
            let key = identity_key(request.proposed_pending().identity())?;
            let encoded_key = encode_idempotency_key(&key).to_vec();
            let encoded = encode_pending_admission_v1(request.proposed_pending())?;
            PreparedAdmission {
                result: AdmissionResultV1::Created(request.proposed_pending().clone()),
                encoded: Some((encoded_key, encoded.as_bytes().to_vec())),
            }
        };
        prepared.push(result);
    }
    for admission in &prepared {
        if let Some((key, _)) = &admission.encoded {
            access.expect_fresh_locator_raw_insert(JournalTable::IdempotencyPending, key)?;
        }
    }
    if access.has_fresh_locator_mutation_expectations()? {
        access.close_fresh_locator_mutation_expectations()?;
    }
    for admission in &prepared {
        if let Some((key, encoded)) = &admission.encoded {
            access.record_actual_fresh_locator_byte_insert(IDEMPOTENCY_PENDING, key)?;
            if pending
                .insert(key.as_slice(), encoded.as_slice())
                .map_err(precommit_storage_error)?
                .is_some()
            {
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
        }
    }
    Ok(prepared
        .into_iter()
        .map(|admission| {
            let created = admission.encoded.is_some();
            (admission.result, created)
        })
        .collect())
}

impl ExecutionFailureTransitionPort for RedbOperationalPorts {
    type Rechecked = RedbExecutionFailureRechecked;

    fn begin_execution_failure(
        &self,
        request: ExecutionFailureTransitionRequestV1,
    ) -> Result<ExecutionFailureAdmissionResult<Self::Rechecked>, StorageError> {
        let access = self.begin_attributed_write(
            riffdb_storage_api::ChangelogAttributionV3::CommandExecutionFailure,
        )?;
        let state = match request.admission_expectation() {
            CommandAdmissionExpectationV1::ExistingPending => {
                read_admission(&access, request.expected_pending().identity())?
            }
            CommandAdmissionExpectationV1::Vacant(candidates) => {
                let matches = matching_admissions(&access, candidates)?;
                if matches.len() > 1 {
                    return Ok(ExecutionFailureAdmissionResult::PendingMismatch);
                }
                matches.into_iter().next()
            }
        };
        Ok(match state {
            None if matches!(
                request.admission_expectation(),
                CommandAdmissionExpectationV1::Vacant(_)
            ) =>
            {
                ExecutionFailureAdmissionResult::Rechecked(RedbExecutionFailureRechecked {
                    access,
                    request,
                })
            }
            None => ExecutionFailureAdmissionResult::Missing,
            Some(StoredAdmissionStateV1::Pending(value))
                if value == *request.expected_pending() =>
            {
                ExecutionFailureAdmissionResult::Rechecked(RedbExecutionFailureRechecked {
                    access,
                    request,
                })
            }
            Some(StoredAdmissionStateV1::Pending(_)) => {
                ExecutionFailureAdmissionResult::PendingMismatch
            }
            Some(StoredAdmissionStateV1::StoredOutcome(value)) => {
                ExecutionFailureAdmissionResult::StoredOutcome(value)
            }
            Some(StoredAdmissionStateV1::ExecutionFailed(value)) => {
                ExecutionFailureAdmissionResult::ExecutionFailed(value)
            }
        })
    }
}

impl ExecutionFailureAdmissionRechecked for RedbExecutionFailureRechecked {
    type AwaitingDecision = RedbExecutionFailureAwaitingDecision;

    fn read_transaction_current(
        self,
    ) -> Result<(Self::AwaitingDecision, TransactionCurrentState), StorageError> {
        let current = current_state_uncached(
            self.access.transaction()?,
            self.request.validation_request(),
        )?;
        Ok((
            RedbExecutionFailureAwaitingDecision {
                access: self.access,
                request: self.request,
                current: current.clone(),
            },
            current,
        ))
    }
}

impl ExecutionFailureAwaitingDecision for RedbExecutionFailureAwaitingDecision {
    fn terminalize(self) -> Result<StoredExecutionFailedV1, StorageError> {
        self.terminalize_inner(None).map(|(failure, _)| failure)
    }

    fn terminalize_with_service_audit(
        self,
        transition: riffdb_storage_api::CommandServiceAuditTransitionV1,
    ) -> Result<AuditedExecutionFailureV1, StorageError> {
        let (failure, terminal) = self.terminalize_inner(Some(transition))?;
        AuditedExecutionFailureV1::new(
            failure,
            terminal.ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?,
        )
        .map_err(invariant_value)
    }

    fn abandon(self) {}
}

impl RedbExecutionFailureAwaitingDecision {
    fn terminalize_inner(
        self,
        audit: Option<riffdb_storage_api::CommandServiceAuditTransitionV1>,
    ) -> Result<
        (
            StoredExecutionFailedV1,
            Option<riffdb_storage_api::StoredServiceAuditRecordV1>,
        ),
        StorageError,
    > {
        if dependencies_from_current(&self.current)? != *self.request.read_dependencies() {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let terminal = self.request.terminal_record();
        let transaction = self.access.transaction()?;
        if !plan_bundle_exists(&self.access, self.request.expected_pending().plan())? {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        match self.request.admission_expectation() {
            CommandAdmissionExpectationV1::ExistingPending => {
                if read_admission(&self.access, self.request.expected_pending().identity())?
                    != Some(StoredAdmissionStateV1::Pending(
                        self.request.expected_pending().clone(),
                    ))
                {
                    return Err(storage_error(StorageErrorKind::InvariantViolation));
                }
            }
            CommandAdmissionExpectationV1::Vacant(candidates) => {
                if !matching_admissions(&self.access, candidates)?.is_empty() {
                    return Err(storage_error(StorageErrorKind::InvariantViolation));
                }
            }
        }
        let key = identity_key(self.request.expected_pending().identity())?;
        let encoded_key = encode_idempotency_key(&key);
        let encoded = encode_execution_failed_v1(&terminal)?;
        if matches!(
            self.request.admission_expectation(),
            CommandAdmissionExpectationV1::ExistingPending
        ) {
            self.access
                .expect_fresh_locator_byte_delete(IDEMPOTENCY_PENDING, encoded_key)?;
        }
        self.access
            .expect_fresh_locator_byte_insert(IDEMPOTENCY, encoded_key)?;
        let terminal_audit = if let Some(transition) = audit {
            let intents = transition.into_intents();
            let records = stage_service_audit_group_in_write(&self.access, &intents)?;
            records.last().cloned()
        } else {
            self.access.close_fresh_locator_mutation_expectations()?;
            None
        };
        if matches!(
            self.request.admission_expectation(),
            CommandAdmissionExpectationV1::ExistingPending
        ) {
            let mut pending = transaction
                .open_table(IDEMPOTENCY_PENDING)
                .map_err(table_error)?;
            self.access
                .record_actual_fresh_locator_byte_delete(IDEMPOTENCY_PENDING, encoded_key)?;
            if pending
                .remove(encoded_key)
                .map_err(precommit_storage_error)?
                .is_none()
            {
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
        }
        {
            let mut outcomes = transaction.open_table(IDEMPOTENCY).map_err(table_error)?;
            self.access
                .record_actual_fresh_locator_byte_insert(IDEMPOTENCY, encoded_key)?;
            if outcomes
                .insert(encoded_key, encoded.as_bytes())
                .map_err(precommit_storage_error)?
                .is_some()
            {
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
        }
        // The one lane that writes a terminal ExecutionFailed row commits through
        // the one path that counts it, so the checkpoint's StoredOutcome-only
        // idempotency count can be derived from the IDEMPOTENCY row count.
        self.access.commit_execution_failure()?;
        Ok((terminal, terminal_audit))
    }
}

macro_rules! impl_candidate_chain {
    ($prior:ty) => {
        impl CommandCandidateAdmission for RedbCandidateAdmission<$prior> {
            type Prior = $prior;
            type StateRead = RedbCandidateStateRead<$prior>;

            fn recheck_admission(
                self,
            ) -> Result<CandidateAdmissionResult<Self::Prior, Self::StateRead>, StorageError> {
                let state = match self.intent.admission_expectation() {
                    CommandAdmissionExpectationV1::ExistingPending => read_candidate_admission(
                        &self.prior.core,
                        self.intent.pending().identity(),
                    )?,
                    CommandAdmissionExpectationV1::Vacant(candidates) => {
                        let matches = matching_candidate_admissions(&self.prior.core, candidates)?;
                        match matches.as_slice() {
                            [] => {
                                return Ok(CandidateAdmissionResult::Proceed(
                                    RedbCandidateStateRead {
                                        prior: self.prior,
                                        intent: self.intent,
                                    },
                                ));
                            }
                            [state] => Some(state.clone()),
                            [_, ..] => {
                                return Ok(CandidateAdmissionResult::PendingMismatch(
                                    AbandonedCandidate::new(self.prior, self.intent),
                                ));
                            }
                        }
                    }
                };
                Ok(match state {
                    None => CandidateAdmissionResult::MissingPending(AbandonedCandidate::new(
                        self.prior,
                        self.intent,
                    )),
                    Some(StoredAdmissionStateV1::Pending(pending))
                        if pending == *self.intent.pending() =>
                    {
                        CandidateAdmissionResult::Proceed(RedbCandidateStateRead {
                            prior: self.prior,
                            intent: self.intent,
                        })
                    }
                    Some(StoredAdmissionStateV1::Pending(pending))
                        if pending.canonical_input_hash()
                            != self.intent.pending().canonical_input_hash() =>
                    {
                        CandidateAdmissionResult::InputMismatch(AbandonedCandidate::new(
                            self.prior,
                            self.intent,
                        ))
                    }
                    Some(StoredAdmissionStateV1::Pending(_)) => {
                        CandidateAdmissionResult::PendingMismatch(AbandonedCandidate::new(
                            self.prior,
                            self.intent,
                        ))
                    }
                    Some(StoredAdmissionStateV1::StoredOutcome(outcome))
                        if outcome.canonical_input_hash()
                            == self.intent.pending().canonical_input_hash() =>
                    {
                        CandidateAdmissionResult::StoredOutcome {
                            prior: self.prior,
                            outcome,
                        }
                    }
                    Some(StoredAdmissionStateV1::ExecutionFailed(failure))
                        if failure.pending().canonical_input_hash()
                            == self.intent.pending().canonical_input_hash() =>
                    {
                        CandidateAdmissionResult::ExecutionFailed {
                            prior: self.prior,
                            failure,
                        }
                    }
                    Some(
                        StoredAdmissionStateV1::StoredOutcome(_)
                        | StoredAdmissionStateV1::ExecutionFailed(_),
                    ) => CandidateAdmissionResult::InputMismatch(AbandonedCandidate::new(
                        self.prior,
                        self.intent,
                    )),
                })
            }
        }

        impl CommandCandidateStateRead for RedbCandidateStateRead<$prior> {
            type Prior = $prior;
            type AwaitingValidation = RedbCandidateAwaitingValidation<$prior>;

            fn read_transaction_current(
                self,
            ) -> Result<(Self::AwaitingValidation, TransactionCurrentState), StorageError> {
                let mut prior = self.prior;
                let current = current_state_cached(
                    &mut prior.core,
                    self.intent.evaluated().validation_request(),
                )?;
                Ok((
                    RedbCandidateAwaitingValidation {
                        prior,
                        intent: self.intent,
                    },
                    current,
                ))
            }
        }

        impl CommandCandidateAwaitingValidation for RedbCandidateAwaitingValidation<$prior> {
            type Prior = $prior;
            type AffectedEpochRead = RedbCandidateAffectedEpochRead<$prior>;

            fn read_transaction_current_policy(
                &self,
                request: &TransactionCurrentPolicyRequestV1,
            ) -> Result<TransactionCurrentPolicyStateV1, StorageError> {
                transaction_current_policy_state(&self.prior.core, request)
            }

            fn read_transaction_current_vector_evidence(
                &self,
                request: &VectorEvidenceReadRequestV1,
            ) -> Result<TransactionCurrentVectorEvidenceV1, StorageError> {
                transaction_current_vector_evidence(&self.prior.core, request)
            }

            fn plan_validated(
                self,
                affected_targets: AffectedIndexEpochTargets,
            ) -> Self::AffectedEpochRead {
                RedbCandidateAffectedEpochRead {
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

        impl CommandCandidateAffectedEpochRead for RedbCandidateAffectedEpochRead<$prior> {
            type Prior = $prior;
            type AwaitingCapacity = RedbCandidateAwaitingCapacity<$prior>;

            fn read_affected_epoch_current(self) -> Result<Self::AwaitingCapacity, StorageError> {
                let mut prior = self.prior;
                let current =
                    affected_current_state_cached(&mut prior.core, &self.affected_targets)?;
                Ok(RedbCandidateAwaitingCapacity {
                    prior,
                    intent: self.intent,
                    affected_targets: self.affected_targets,
                    affected_current: current,
                })
            }
        }

        impl CommandCandidateAwaitingCapacity for RedbCandidateAwaitingCapacity<$prior> {
            type Prior = $prior;
            type CapacityReserved = RedbCandidateCapacityReserved<$prior>;

            fn intent(&self) -> &CommitIntent {
                &self.intent
            }

            fn affected_targets(&self) -> &AffectedIndexEpochTargets {
                &self.affected_targets
            }

            fn affected_current(&self) -> &AffectedEpochCurrentState {
                &self.affected_current
            }

            fn reject(
                self,
                _reason: CandidateValidationRejection,
            ) -> AbandonedCandidate<Self::Prior> {
                AbandonedCandidate::new(self.prior, self.intent)
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
                let mut prior = self.prior;
                if provenance_exists(&prior.core, self.intent.provenance_id())? {
                    return Ok(CandidateCapacityResult::ProvenanceIdCollision(
                        ProvenanceIdCollision::detected(),
                    ));
                }
                if prior
                    .core
                    .metrics
                    .is_some_and(|metrics| !metrics.can_add(write_plan.charge()))
                {
                    return Ok(CandidateCapacityResult::BatchFull(AbandonedCandidate::new(
                        prior,
                        self.intent,
                    )));
                }
                if !prior
                    .core
                    .reserved_provenance_ids
                    .insert(self.intent.provenance_id())
                {
                    return Err(storage_error(StorageErrorKind::InvariantViolation));
                }
                Ok(CandidateCapacityResult::Reserved(
                    RedbCandidateCapacityReserved {
                        prior,
                        intent: self.intent,
                        write_plan,
                    },
                ))
            }
        }

        impl CommandCandidateCapacityReserved for RedbCandidateCapacityReserved<$prior> {
            type Prior = $prior;
            type SequenceAssigned = RedbCandidateSequenceAssigned<$prior>;

            fn intent(&self) -> &CommitIntent {
                &self.intent
            }

            fn write_plan(&self) -> &CommandWriteSetPlanV1 {
                &self.write_plan
            }

            fn assign_sequence(mut self) -> Result<Self::SequenceAssigned, StorageError> {
                let allocation = self
                    .prior
                    .core
                    .allocator
                    .allocate_one()
                    .map_err(sequence_error)?;
                self.prior.core.allocator = allocation.next();
                Ok(RedbCandidateSequenceAssigned {
                    prior: self.prior,
                    intent: self.intent,
                    write_plan: self.write_plan,
                    assignment: AssignedCommandSequence::from_assigned(allocation.assigned()),
                })
            }
        }

        impl CommandCandidateSequenceAssigned for RedbCandidateSequenceAssigned<$prior> {
            type Prior = $prior;
            type Staged = RedbNonEmptyBatch;

            fn assignment(&self) -> AssignedCommandSequence {
                self.assignment
            }

            fn intent(&self) -> &CommitIntent {
                &self.intent
            }

            fn write_plan(&self) -> &CommandWriteSetPlanV1 {
                &self.write_plan
            }

            fn detach(
                mut self,
            ) -> Result<(Self::Prior, DetachedCommandReservationV1), StorageError> {
                let ordinal = u16::try_from(self.prior.core.detached.len())
                    .map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?;
                let reservation = DetachedCommandReservationV1::new(ordinal, self.assignment)
                    .map_err(invariant_value)?;
                for advance in self.write_plan.index_epochs() {
                    retain_detached_index_generation_base(
                        &mut self.prior.core.detached_index_generation_base,
                        &self.prior.core.index_generations,
                        advance.post_image().target(),
                        advance.prior(),
                    )?;
                    self.prior.core.index_generations.insert(
                        advance.post_image().target().clone(),
                        IndexEpochPosition::Value(advance.next()),
                    );
                }
                self.prior.core.metrics = Some(metrics_after_charge(
                    self.prior.core.metrics,
                    self.write_plan.charge(),
                )?);
                self.prior.core.detached.push(DetachedRedbCandidate {
                    assignment: self.assignment,
                    intent: self.intent,
                    write_plan: self.write_plan,
                });
                Ok((self.prior, reservation))
            }

            fn stage(
                self,
                mut records: AtomicCommandRecordSet,
            ) -> Result<Self::Staged, StorageError> {
                if !records.matches_reserved_candidate(
                    self.assignment,
                    &self.intent,
                    &self.write_plan,
                ) || records.next_application_sequence() != self.prior.core.allocator
                {
                    return Err(storage_error(StorageErrorKind::InvariantViolation));
                }
                // Canonical encoding and the complete reservation proof precede
                // every physical write for this candidate.
                let encoded =
                    encode_capsule_command_record_set_v1(&mut records).map_err(codec_error)?;
                let mut core = self.prior.core;
                let entity_transitions = apply_record_set(&mut core, &records, encoded)?;
                core.metrics = Some(metrics_after(core.metrics, &records)?);
                core.capsule_extensions
                    .push(records.index_epochs().to_vec());
                core.capsule_entity_transitions.push(entity_transitions);
                core.staged.push(records.into_staged_evidence());
                Ok(RedbNonEmptyBatch { core })
            }
        }
    };
}

fn read_candidate_admission(
    core: &BatchCore,
    identity: &IdempotencyIdentity,
) -> Result<Option<StoredAdmissionStateV1>, StorageError> {
    let staged = core
        .staged
        .iter()
        .find(|evidence| evidence.outcome().identity() == identity)
        .map(|evidence| StoredAdmissionStateV1::StoredOutcome(evidence.outcome().clone()));
    let detached = core
        .detached
        .iter()
        .find(|candidate| candidate.intent.pending().identity() == identity)
        .map(|candidate| StoredAdmissionStateV1::Pending(candidate.intent.pending().clone()));
    let staged = match (staged, detached) {
        (None, value) | (value, None) => value,
        (Some(_), Some(_)) => return Err(storage_error(StorageErrorKind::CorruptData)),
    };
    let stored = read_admission(&core.access, identity)?;
    match (staged, stored) {
        (None, value) | (value, None) => Ok(value),
        (Some(_), Some(_)) => Err(storage_error(StorageErrorKind::CorruptData)),
    }
}

fn matching_candidate_admissions(
    core: &BatchCore,
    candidates: &IdempotencyLookupCandidatesV1,
) -> Result<Vec<StoredAdmissionStateV1>, StorageError> {
    let mut matches = Vec::new();
    for identity in candidates.as_slice() {
        if let Some(value) = read_candidate_admission(core, identity)? {
            matches.push(value);
        }
    }
    Ok(matches)
}

impl_candidate_chain!(RedbEmptyBatch);
impl_candidate_chain!(RedbNonEmptyBatch);

fn retain_detached_index_generation_base(
    base: &mut Option<BTreeMap<PartitionIndexTarget, IndexEpochPosition>>,
    current: &BTreeMap<PartitionIndexTarget, IndexEpochPosition>,
    target: &PartitionIndexTarget,
    prior: IndexEpochPosition,
) -> Result<(), StorageError> {
    if current.get(target) != Some(&prior) {
        return Err(storage_error(StorageErrorKind::InvariantViolation));
    }
    base.get_or_insert_with(|| current.clone())
        .entry(target.clone())
        .or_insert(prior);
    Ok(())
}

impl DetachedCommandGroupBatch for RedbEmptyBatch {
    type Staged = RedbNonEmptyBatch;

    fn stage_detached_group(
        self,
        commands: Vec<DetachedCommandRecordV1>,
    ) -> Result<Self::Staged, StorageError> {
        let mut core = self.core;
        if commands.is_empty() || commands.len() != core.detached.len() {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let expected_generations = core.index_generations.clone();
        let has_index_generation_advance = core
            .detached
            .iter()
            .any(|candidate| !candidate.write_plan.index_epochs().is_empty());
        core.index_generations = detached_index_generation_base(
            core.detached_index_generation_base.take(),
            &expected_generations,
            has_index_generation_advance,
        )?;
        let retained = std::mem::take(&mut core.detached);
        for (ordinal, (command, retained)) in commands.into_iter().zip(retained).enumerate() {
            let (reservation, mut records) = command.into_parts();
            if usize::from(reservation.ordinal()) != ordinal
                || reservation.assignment() != retained.assignment
                || !records.matches_reserved_candidate(
                    retained.assignment,
                    &retained.intent,
                    &retained.write_plan,
                )
            {
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
            let encoded =
                encode_capsule_command_record_set_v1(&mut records).map_err(codec_error)?;
            let entity_transitions = apply_record_set(&mut core, &records, encoded)?;
            core.capsule_extensions
                .push(records.index_epochs().to_vec());
            core.capsule_entity_transitions.push(entity_transitions);
            core.staged.push(records.into_staged_evidence());
        }
        if core.index_generations != expected_generations
            || core.staged.len()
                != usize::from(
                    core.metrics
                        .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?
                        .command_count()
                        .get(),
                )
        {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        Ok(RedbNonEmptyBatch { core })
    }
}

fn detached_index_generation_base(
    retained: Option<BTreeMap<PartitionIndexTarget, IndexEpochPosition>>,
    expected_generations: &BTreeMap<PartitionIndexTarget, IndexEpochPosition>,
    has_index_generation_advance: bool,
) -> Result<BTreeMap<PartitionIndexTarget, IndexEpochPosition>, StorageError> {
    match (retained, has_index_generation_advance) {
        (Some(base), _) => Ok(base),
        (None, false) => Ok(expected_generations.clone()),
        (None, true) => Err(storage_error(StorageErrorKind::InvariantViolation)),
    }
}

fn apply_record_set(
    core: &mut BatchCore,
    records: &AtomicCommandRecordSet,
    encoded: riffdb_storage_api::EncodedCapsuleCommandRecordSetV1,
) -> Result<Vec<CommittedEntityTransitionV1>, StorageError> {
    core.access.begin_command_prefix_capture(
        records
            .presequence_charge()
            .encoded_upper_bound()
            .classes()
            .commit(),
    )?;
    let BatchCore {
        access,
        entity_observations,
        entity_observation_bytes,
        index_generations,
        ..
    } = core;
    let identity_key = identity_key(records.expected_pending().identity())?;
    let (entities, vector_evidence, index_entries, index_epochs) = encoded.into_parts();
    // `RedbCandidateAdmission::recheck_admission` established this exact
    // expectation earlier in the same exclusive write transaction. Every
    // subsequent state is consuming and backend-private, so no path can stage
    // without that transaction-current check and no other writer can alter the
    // identity before this point. The terminal insert/remove below remains the
    // authoritative duplicate/vacancy assertion. Re-reading and decoding both
    // idempotency tables here supplied no newer evidence.

    let entity_transitions = apply_entities(
        access,
        records,
        entities,
        entity_observations,
        entity_observation_bytes,
    )?;
    apply_vector_evidence(access, records, vector_evidence)?;
    apply_index_entries(access, records, index_entries)?;
    apply_index_epochs(
        access,
        records,
        index_epochs,
        index_generations,
        &mut core.pending_index_generations,
    )?;

    if !records.events().is_empty() {
        for (event, intent) in records.events().iter().zip(records.outbox_intents()) {
            if event != intent.event() {
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
        }
    }
    if matches!(
        records.intent().admission_expectation(),
        CommandAdmissionExpectationV1::ExistingPending
    ) {
        let Some(_removed) = access.delete_command_value(
            JournalTable::IdempotencyPending,
            encode_idempotency_key(&identity_key).to_vec(),
        )?
        else {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        };
    }
    let independent = access.finish_command_prefix_capture()?;
    core.capsule_independent_mutations.push(independent);
    Ok(entity_transitions)
}

fn apply_vector_evidence(
    access: &RedbWriteAccess,
    records: &AtomicCommandRecordSet,
    encoded: Vec<Option<riffdb_storage_api::CanonicalStoredEnvelopeV1>>,
) -> Result<(), StorageError> {
    if records.vector_evidence().len() != encoded.len()
        || records.vector_evidence_transitions().len() != encoded.len()
    {
        return Err(storage_error(StorageErrorKind::InvariantViolation));
    }
    let sequence = records.commit().commit_sequence();
    for ((transition, mutation), bytes) in records
        .vector_evidence_transitions()
        .iter()
        .zip(records.vector_evidence())
        .zip(encoded)
    {
        let key = encode_vector_evidence_key(mutation.target().key(), mutation.vector_field())
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let current_bytes = access.read_command_value(JournalTable::VectorEvidence, &key)?;
        let current = current_bytes
            .as_deref()
            .map(|value| decode_vector_evidence_v1(value).map(decoded_value))
            .transpose()?;
        if !transition.matches_current(current.as_ref()) {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        apply_vector_observation(access, transition, sequence)?;
        apply_vector_evidence_index(access, transition, mutation, current.as_ref())?;
        match (mutation, bytes, current_bytes) {
            (riffdb_storage_api::VectorEvidenceMutationV1::Put(_), Some(bytes), current) => {
                access.put_proven_command_value(
                    JournalTable::VectorEvidence,
                    key,
                    current,
                    bytes,
                )?;
            }
            (riffdb_storage_api::VectorEvidenceMutationV1::Delete { .. }, None, Some(current)) => {
                access.delete_proven_command_value(JournalTable::VectorEvidence, key, current)?;
            }
            _ => return Err(storage_error(StorageErrorKind::InvariantViolation)),
        }
    }
    Ok(())
}

/// Maintains the authoritative partition-ordered reciprocal index in the same
/// journal transaction as its primary evidence row and observation counters.
fn apply_vector_evidence_index(
    access: &RedbWriteAccess,
    transition: &riffdb_storage_api::VectorEvidenceTransitionPlanV1,
    mutation: &riffdb_storage_api::VectorEvidenceMutationV1,
    current: Option<&StoredVectorEvidenceV1>,
) -> Result<(), StorageError> {
    let target = transition.observation_target();
    let key = encode_vector_evidence_index_key(&target, mutation.target().key())
        .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
    let current_bytes = access.read_command_value(JournalTable::VectorEvidenceIndex, &key)?;
    let current_index = current_bytes
        .as_deref()
        .map(|value| decode_vector_evidence_index_v1(value).map(decoded_value))
        .transpose()?;
    match (current, current_index.as_ref()) {
        (Some(evidence), Some(index)) if index.matches_evidence(evidence) => {}
        (None, None) => {}
        _ => return Err(storage_error(StorageErrorKind::InvariantViolation)),
    }

    match mutation {
        riffdb_storage_api::VectorEvidenceMutationV1::Put(value) => {
            let successor = VectorEvidenceIndexEntryV1::from_evidence(value)
                .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
            let encoded = encode_vector_evidence_index_v1(&successor)?;
            access.put_proven_command_value(
                JournalTable::VectorEvidenceIndex,
                key,
                current_bytes,
                encoded,
            )?;
        }
        riffdb_storage_api::VectorEvidenceMutationV1::Delete { .. } => {
            let Some(current) = current_bytes else {
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            };
            access.delete_proven_command_value(JournalTable::VectorEvidenceIndex, key, current)?;
        }
    }
    Ok(())
}

/// Maintains one partition/field observation from the exact evidence
/// predecessor already proven in this command's exclusive write transaction.
/// The observation mutation shares the entity/evidence journal transition, so
/// neither state can become visible without the other.
fn apply_vector_observation(
    access: &RedbWriteAccess,
    transition: &riffdb_storage_api::VectorEvidenceTransitionPlanV1,
    sequence: CommitSequence,
) -> Result<(), StorageError> {
    let target = transition.observation_target();
    let key = encode_vector_observation_key(&target)
        .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
    let current_bytes = access.read_command_value(JournalTable::VectorObservations, &key)?;
    let prior_observation = current_bytes
        .as_deref()
        .map(|value| decode_vector_observation_v1(value).map(decoded_value))
        .transpose()?;
    let mut observation = prior_observation.clone().unwrap_or_else(|| {
        riffdb_storage_api::VectorObservationCountsV1::empty(target.clone(), sequence)
    });
    if observation.target() != &target {
        return Err(storage_error(StorageErrorKind::InvariantViolation));
    }
    let classification = transition
        .classification_transition(sequence)
        .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
    observation
        .apply(&classification, sequence)
        .map_err(|error| match error {
            StorageValueError::LimitExceeded => storage_error(StorageErrorKind::LimitExceeded),
            _ => storage_error(StorageErrorKind::InvariantViolation),
        })?;
    if observation.total_entities() == 0 {
        let Some(current) = current_bytes else {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        };
        access.delete_proven_command_value(JournalTable::VectorObservations, key, current)?;
    } else {
        let encoded = encode_vector_observation_v1(&observation)?;
        access.put_proven_command_value(
            JournalTable::VectorObservations,
            key,
            current_bytes,
            encoded,
        )?;
    }

    let health_key = encode_vector_health_observation_key(target.lineage())
        .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
    let health_current_bytes =
        access.read_command_value(JournalTable::VectorObservations, &health_key)?;
    let mut health = health_current_bytes
        .as_deref()
        .map(|value| decode_vector_health_observation_v1(value).map(decoded_value))
        .transpose()?
        .unwrap_or_else(|| VectorHealthObservationV1::empty(target.lineage().clone(), sequence));
    health
        .apply_partition(
            target.entity_type(),
            target.vector_field(),
            transition.stale_entity_count_threshold(),
            prior_observation.as_ref(),
            (observation.total_entities() != 0).then_some(&observation),
            sequence,
        )
        .map_err(|error| match error {
            StorageValueError::LimitExceeded => storage_error(StorageErrorKind::LimitExceeded),
            _ => storage_error(StorageErrorKind::InvariantViolation),
        })?;
    let health_encoded = encode_vector_health_observation_v1(&health)?;
    access.put_proven_command_value(
        JournalTable::VectorObservations,
        health_key,
        health_current_bytes,
        health_encoded,
    )?;
    Ok(())
}

fn journal_codec_error(error: JournalCodecError) -> StorageError {
    let kind = if error == JournalCodecError::LimitExceeded {
        StorageErrorKind::LimitExceeded
    } else {
        StorageErrorKind::InvariantViolation
    };
    storage_error(kind)
}

fn stage_application_allocator(
    access: &RedbWriteAccess,
    allocator: ApplicationSequenceAllocator,
) -> Result<(), StorageError> {
    let encoded = riffdb_storage_api::encode_application_sequence_allocator_v1(allocator)
        .map_err(codec_error)?;
    access
        .put_command_value(
            JournalTable::Meta,
            META_APPLICATION_SEQUENCE.as_bytes().to_vec(),
            encoded.into_bytes(),
        )?
        .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
    Ok(())
}

fn apply_entities(
    access: &RedbWriteAccess,
    records: &AtomicCommandRecordSet,
    encoded: Vec<Option<riffdb_storage_api::CanonicalStoredEnvelopeV1>>,
    observations: &mut BTreeMap<EntityTarget, EntityObservation>,
    observation_bytes: &mut BTreeMap<EntityTarget, Option<Vec<u8>>>,
) -> Result<Vec<CommittedEntityTransitionV1>, StorageError> {
    if records.entities().is_empty() {
        return Ok(Vec::new());
    }
    let mut transitions = Vec::with_capacity(records.entities().len());
    let mut retained_references = records.commit().entity_references().iter();
    for (ordinal, (mutation, bytes)) in records.entities().iter().zip(encoded).enumerate() {
        let target = mutation.target();
        let key = encode_entity_key(target.key());
        let observation = observations
            .get(target)
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
        if observation.expected_state() != mutation.expected() {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        if mutation.is_delete()
            && !matches!(observation, EntityObservation::Present(record) if record == mutation.checked_image())
        {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let stored_head_bytes = access.read_command_value(JournalTable::EntityChainHeads, key)?;
        let stored_head = stored_head_bytes
            .as_deref()
            .map(|bytes| {
                riffdb_storage_api::decode_entity_chain_head_v1(bytes)
                    .map(|decoded| decoded.into_parts().0)
                    .map_err(codec_error)
            })
            .transpose()?;
        let (prior_state, prior_revision, prior_hash) = match (stored_head.as_ref(), observation) {
            (None, EntityObservation::Absent(_)) => (EntityChainStateV1::NeverExisted, 0, None),
            (Some(head), EntityObservation::Absent(_))
                if head.target() == target && head.state() == EntityChainStateV1::Deleted =>
            {
                (
                    head.state(),
                    head.chain_revision(),
                    Some(head.last_transition_hash()),
                )
            }
            (Some(head), EntityObservation::Present(record)) => {
                let expected_state = EntityChainStateV1::Live {
                    version: record.entity_version(),
                    value_hash: riffdb_storage_api::derive_entity_record_hash_v1(record)
                        .map_err(invariant_value)?,
                };
                if head.target() != target || head.state() != expected_state {
                    return Err(storage_error(StorageErrorKind::CorruptData));
                }
                (
                    head.state(),
                    head.chain_revision(),
                    Some(head.last_transition_hash()),
                )
            }
            _ => return Err(storage_error(StorageErrorKind::CorruptData)),
        };
        let next_state = match mutation.live_post_image() {
            Some(post_image) => {
                let reference = retained_references
                    .next()
                    .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
                if reference.target() != target
                    || reference.entity_version() != post_image.entity_version()
                {
                    return Err(storage_error(StorageErrorKind::InvariantViolation));
                }
                EntityChainStateV1::Live {
                    version: reference.entity_version(),
                    // AtomicCommandRecordSet construction already proved this
                    // retained reference against the exact post-image. Consume
                    // that proof instead of rebuilding and hashing the same
                    // canonical preimage on the authoritative apply lane.
                    value_hash: reference.post_image_hash(),
                }
            }
            None => EntityChainStateV1::Deleted,
        };
        let transition = CommittedEntityTransitionV1::new(
            records.assignment().assigned(),
            u32::try_from(ordinal).map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?,
            target.clone(),
            prior_state,
            prior_revision,
            prior_hash,
            next_state,
        )
        .map_err(invariant_value)?;
        let next_head = match stored_head {
            Some(head) => head.apply(&transition),
            None => EntityChainHeadV1::from_genesis(&transition),
        }
        .map_err(invariant_value)?;
        let encoded_head =
            riffdb_storage_api::encode_entity_chain_head_v1(&next_head).map_err(codec_error)?;
        access.put_proven_command_value(
            JournalTable::EntityChainHeads,
            key.to_vec(),
            stored_head_bytes,
            encoded_head,
        )?;
        let proven_current = observation_bytes
            .get(target)
            .cloned()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
        if proven_current.is_some() != matches!(observation, EntityObservation::Present(_)) {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let next_bytes = bytes.as_ref().map(|value| value.as_bytes().to_vec());
        match (bytes, proven_current) {
            (Some(bytes), current) => access.put_proven_command_value(
                JournalTable::Entities,
                key.to_vec(),
                current,
                bytes,
            )?,
            (None, Some(current)) => {
                access.delete_proven_command_value(JournalTable::Entities, key.to_vec(), current)?
            }
            (None, None) => return Err(storage_error(StorageErrorKind::InvariantViolation)),
        }
        let next_observation = match mutation.live_post_image() {
            Some(post_image) => EntityObservation::Present(post_image.clone()),
            None => EntityObservation::Absent(target.clone()),
        };
        observations.insert(target.clone(), next_observation);
        observation_bytes.insert(target.clone(), next_bytes);
        transitions.push(transition);
    }
    if retained_references.next().is_some() {
        return Err(storage_error(StorageErrorKind::InvariantViolation));
    }
    Ok(transitions)
}

fn apply_index_entries(
    access: &RedbWriteAccess,
    records: &AtomicCommandRecordSet,
    encoded: Vec<Option<riffdb_storage_api::CanonicalStoredEnvelopeV1>>,
) -> Result<(), StorageError> {
    if records.index_entries().is_empty() {
        return Ok(());
    }
    for (mutation, bytes) in records.index_entries().iter().zip(encoded) {
        let key = encode_index_entry_key(mutation.key());
        match (mutation, bytes) {
            (IndexEntryMutationV1::Delete(_), None) => {
                let Some(current) =
                    access.read_command_value(JournalTable::SecondaryIndexes, key)?
                else {
                    return Err(storage_error(StorageErrorKind::InvariantViolation));
                };
                access.delete_proven_command_value(
                    JournalTable::SecondaryIndexes,
                    key.to_vec(),
                    current,
                )?;
            }
            (IndexEntryMutationV1::Put(_), Some(bytes)) => {
                let current = access.read_command_value(JournalTable::SecondaryIndexes, key)?;
                access.put_proven_command_value(
                    JournalTable::SecondaryIndexes,
                    key.to_vec(),
                    current,
                    bytes,
                )?;
            }
            _ => return Err(storage_error(StorageErrorKind::InvariantViolation)),
        }
    }
    Ok(())
}

fn apply_index_epochs(
    access: &RedbWriteAccess,
    records: &AtomicCommandRecordSet,
    encoded: Vec<riffdb_storage_api::CanonicalStoredEnvelopeV1>,
    generations: &mut BTreeMap<PartitionIndexTarget, IndexEpochPosition>,
    pending: &mut BTreeMap<PartitionIndexTarget, PendingIndexGenerationPostImage>,
) -> Result<(), StorageError> {
    if records.index_epochs().is_empty() {
        return Ok(());
    }
    for (advance, bytes) in records.index_epochs().iter().zip(encoded) {
        if generations.get(advance.post_image().target()) != Some(&advance.prior()) {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        match pending.get(advance.post_image().target()) {
            Some(prior)
                if IndexEpochPosition::Value(prior.final_record.epoch()) != advance.prior() =>
            {
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
            _ => {}
        }
        let key = encode_partition_index_key(advance.post_image().target());
        let prior_bytes = match pending.get(advance.post_image().target()) {
            Some(prior) => Some(prior.final_bytes.as_bytes().to_vec()),
            None => access.read_command_value(JournalTable::IndexEpochs, &key)?,
        };
        let physical = match prior_bytes.as_deref() {
            Some(before) => {
                let row = decoded_value(decode_index_epoch_v1(before)?);
                if row.target() != advance.post_image().target() {
                    return Err(storage_error(StorageErrorKind::CorruptData));
                }
                IndexEpochPosition::Value(row.epoch())
            }
            None => IndexEpochPosition::BeforeFirst,
        };
        if physical != advance.prior() {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let logical = match prior_bytes.as_deref() {
            Some(before) => {
                JournalMutation::replace(JournalTable::IndexEpochs, key, before, bytes.as_bytes())
            }
            None => JournalMutation::put(JournalTable::IndexEpochs, key, bytes.as_bytes()),
        }
        .map_err(journal_codec_error)?;
        access.capture_logical_command_mutation(&logical)?;
        let initial = pending
            .get(advance.post_image().target())
            .map_or(advance.prior(), |prior| prior.initial);
        pending.insert(
            advance.post_image().target().clone(),
            PendingIndexGenerationPostImage {
                initial,
                final_record: advance.post_image().clone(),
                final_bytes: bytes,
            },
        );
        generations.insert(
            advance.post_image().target().clone(),
            IndexEpochPosition::Value(advance.next()),
        );
    }
    Ok(())
}

fn flush_index_generation_post_images(core: &mut BatchCore) -> Result<(), StorageError> {
    if core.pending_index_generations.is_empty() {
        return Ok(());
    }
    let pending = std::mem::take(&mut core.pending_index_generations);
    for (target, post_image) in pending {
        if post_image.final_record.target() != &target
            || core.index_generations.get(&target)
                != Some(&IndexEpochPosition::Value(post_image.final_record.epoch()))
        {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let key = encode_partition_index_key(&target);
        let prior_bytes = core
            .access
            .read_command_value(JournalTable::IndexEpochs, key.as_slice())?;
        let physical = match prior_bytes.as_deref() {
            Some(bytes) => {
                let decoded = decoded_value(decode_index_epoch_v1(bytes)?);
                if decoded.target() != &target {
                    return Err(storage_error(StorageErrorKind::CorruptData));
                }
                IndexEpochPosition::Value(decoded.epoch())
            }
            None => IndexEpochPosition::BeforeFirst,
        };
        if physical != post_image.initial {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        core.access.put_proven_command_value(
            JournalTable::IndexEpochs,
            key.to_vec(),
            prior_bytes,
            post_image.final_bytes,
        )?;
    }
    Ok(())
}

fn read_application_allocator(
    access: &RedbWriteAccess,
) -> Result<ApplicationSequenceAllocator, StorageError> {
    let value = access
        .read_command_value(JournalTable::Meta, META_APPLICATION_SEQUENCE.as_bytes())?
        .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
    decode_application_sequence_allocator_v1(&value).map(decoded_value)
}

fn read_admission(
    access: &RedbWriteAccess,
    identity: &IdempotencyIdentity,
) -> Result<Option<StoredAdmissionStateV1>, StorageError> {
    let key = identity_key(identity)?;
    let physical_key = encode_idempotency_key(&key);
    let pending = access
        .read_command_value(JournalTable::IdempotencyPending, physical_key)?
        .map(|value| decode_pending_admission_v1(&value).map(decoded_value))
        .transpose()?;
    let physical = access
        .read_command_value(JournalTable::Idempotency, physical_key)?
        .map(|value| decode_idempotency_record_v1(&value).map(decoded_value))
        .transpose()?;
    let derived = command_outcome_from_write_indexes(access, identity)?;
    let terminal = match (physical, derived) {
        (None, None) => None,
        (None, Some(value)) => Some(IdempotencyRecordV1::StoredOutcome(value)),
        (Some(IdempotencyRecordV1::CommandLocator(locator)), Some(value))
            if locator.commit_sequence() == value.commit_sequence() =>
        {
            Some(IdempotencyRecordV1::StoredOutcome(value))
        }
        (Some(IdempotencyRecordV1::StoredOutcome(physical)), Some(derived))
            if physical == derived =>
        {
            Some(IdempotencyRecordV1::StoredOutcome(physical))
        }
        (Some(value), None) => Some(value),
        _ => return Err(storage_error(StorageErrorKind::CorruptData)),
    };
    match (pending, terminal) {
        (None, None) => Ok(None),
        (Some(value), None) if value.identity() == identity => {
            Ok(Some(StoredAdmissionStateV1::Pending(value)))
        }
        (None, Some(IdempotencyRecordV1::StoredOutcome(value))) if value.identity() == identity => {
            Ok(Some(StoredAdmissionStateV1::StoredOutcome(value)))
        }
        (None, Some(IdempotencyRecordV1::ExecutionFailed(value)))
            if value.pending().identity() == identity =>
        {
            Ok(Some(StoredAdmissionStateV1::ExecutionFailed(value)))
        }
        _ => Err(storage_error(StorageErrorKind::CorruptData)),
    }
}

fn read_admission_from_tables(
    pending: &impl ReadableTable<&'static [u8], &'static [u8]>,
    terminal: &impl ReadableTable<&'static [u8], &'static [u8]>,
    commits: &impl ReadableTable<&'static [u8], &'static [u8]>,
    events: &impl ReadableTable<&'static [u8], &'static [u8]>,
    identity: &IdempotencyIdentity,
    derived: Option<StoredOutcomeV1>,
) -> Result<Option<StoredAdmissionStateV1>, StorageError> {
    let key = identity_key(identity)?;
    let pending = pending
        .get(encode_idempotency_key(&key))
        .map_err(precommit_storage_error)?
        .map(|value| decode_pending_admission_v1(value.value()).map(decoded_value))
        .transpose()?;
    let terminal = terminal
        .get(encode_idempotency_key(&key))
        .map_err(precommit_storage_error)?
        .map(|value| decode_idempotency_record_v1(value.value()).map(decoded_value))
        .transpose()?;
    let physical = match terminal {
        Some(IdempotencyRecordV1::CommandLocator(locator)) => {
            let capsule = command_member_at(commits, events, locator.commit_sequence())?
                .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?
                .into_base();
            if capsule.commit_sequence() != locator.commit_sequence() {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
            Some(IdempotencyRecordV1::StoredOutcome(
                capsule.outcome().clone(),
            ))
        }
        other => other,
    };
    let terminal = match (physical, derived) {
        (None, None) => None,
        (None, Some(value)) => Some(IdempotencyRecordV1::StoredOutcome(value)),
        (Some(value), None) => Some(value),
        (Some(IdempotencyRecordV1::StoredOutcome(physical)), Some(derived))
            if physical == derived =>
        {
            Some(IdempotencyRecordV1::StoredOutcome(physical))
        }
        _ => return Err(storage_error(StorageErrorKind::CorruptData)),
    };
    match (pending, terminal) {
        (None, None) => Ok(None),
        (Some(value), None) if value.identity() == identity => {
            Ok(Some(StoredAdmissionStateV1::Pending(value)))
        }
        (None, Some(IdempotencyRecordV1::StoredOutcome(value))) if value.identity() == identity => {
            Ok(Some(StoredAdmissionStateV1::StoredOutcome(value)))
        }
        (None, Some(IdempotencyRecordV1::ExecutionFailed(value)))
            if value.pending().identity() == identity =>
        {
            Ok(Some(StoredAdmissionStateV1::ExecutionFailed(value)))
        }
        (None, Some(IdempotencyRecordV1::CommandLocator(_))) => {
            Err(storage_error(StorageErrorKind::CorruptData))
        }
        _ => Err(storage_error(StorageErrorKind::CorruptData)),
    }
}

fn command_outcome_from_member(
    segment: &StoredCommandSegmentV1,
    locator: crate::transient::CommandDerivedLocator,
    identity: &IdempotencyIdentity,
) -> Result<StoredOutcomeV1, StorageError> {
    if locator.member != CommandDerivedMemberV1::Command || locator.member_ordinal != 0 {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    let command = segment
        .commands()
        .get(usize::from(locator.command_ordinal))
        .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
    let outcome = command.base().outcome();
    if outcome.identity() != identity
        || command.commit_sequence() < segment.first_commit_sequence()
        || command.commit_sequence() > segment.last_commit_sequence()
    {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    Ok(outcome.clone())
}

fn command_outcome_from_write_indexes(
    access: &RedbWriteAccess,
    identity: &IdempotencyIdentity,
) -> Result<Option<StoredOutcomeV1>, StorageError> {
    let key = identity_key(identity)?;
    let exact_key = encode_idempotency_key(&key);
    if let Some((segment, locator)) =
        access.command_derived_member(CommandDerivedIndexKindV1::Idempotency, exact_key)?
    {
        return command_outcome_from_member(&segment, locator, identity).map(Some);
    }
    // This is the admission lookup, so `Ok(None)` means "never admitted" and a
    // durably committed command answering absent is executed again. With the
    // transient index dormant and no physical IDEMPOTENCY row that is exactly
    // what happened; the ADR-0165 locator closes it.
    //
    // Every failure below is closed, never absent: only a genuinely absent
    // locator is absence.
    let Some(encoded) = access.read_command_value(JournalTable::IdempotencyLocators, exact_key)?
    else {
        let _coverage_survives = access.fresh_locator_allows_miss()?;
        return Ok(None);
    };
    let locator = crate::codec::decode_command_locator_v1(&encoded)?
        .into_parts()
        .0;
    let member = crate::command_authority::command_member_at_write_access(
        access,
        locator.commit_sequence(),
    )?
    .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
    let capsule = member.into_base();
    if capsule.commit_sequence() != locator.commit_sequence()
        || capsule.outcome().identity() != identity
    {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    Ok(Some(capsule.outcome().clone()))
}

fn matching_admissions(
    access: &RedbWriteAccess,
    candidates: &IdempotencyLookupCandidatesV1,
) -> Result<Vec<StoredAdmissionStateV1>, StorageError> {
    let mut matches = Vec::new();
    for identity in candidates.as_slice() {
        if let Some(value) = read_admission(access, identity)? {
            matches.push(value);
        }
    }
    Ok(matches)
}

fn matching_admissions_from_tables(
    pending: &impl ReadableTable<&'static [u8], &'static [u8]>,
    terminal: &impl ReadableTable<&'static [u8], &'static [u8]>,
    commits: &impl ReadableTable<&'static [u8], &'static [u8]>,
    events: &impl ReadableTable<&'static [u8], &'static [u8]>,
    candidates: &IdempotencyLookupCandidatesV1,
    mut derived: impl FnMut(&IdempotencyIdentity) -> Result<Option<StoredOutcomeV1>, StorageError>,
) -> Result<Vec<StoredAdmissionStateV1>, StorageError> {
    let mut matches = Vec::new();
    for identity in candidates.as_slice() {
        if let Some(value) = read_admission_from_tables(
            pending,
            terminal,
            commits,
            events,
            identity,
            derived(identity)?,
        )? {
            matches.push(value);
        }
    }
    Ok(matches)
}

fn matching_admissions_from_access(
    ports: &RedbOperationalPorts,
    access: &RedbReadAccess,
    candidates: &IdempotencyLookupCandidatesV1,
) -> Result<Vec<StoredAdmissionStateV1>, StorageError> {
    let mut matches = Vec::new();
    for identity in candidates.as_slice() {
        let key = identity_key(identity)?;
        let physical_key = encode_idempotency_key(&key);
        let pending = access
            .read_value(JournalTable::IdempotencyPending, physical_key)?
            .map(|value| decode_pending_admission_v1(&value).map(decoded_value))
            .transpose()?;
        let terminal = access
            .read_value(JournalTable::Idempotency, physical_key)?
            .map(|value| decode_idempotency_record_v1(&value).map(decoded_value))
            .transpose()?;
        let physical = match terminal {
            Some(IdempotencyRecordV1::CommandLocator(locator)) => {
                let capsule = command_member_at_access(access, locator.commit_sequence())?
                    .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?
                    .into_base();
                if capsule.commit_sequence() != locator.commit_sequence() {
                    return Err(storage_error(StorageErrorKind::CorruptData));
                }
                Some(IdempotencyRecordV1::StoredOutcome(
                    capsule.outcome().clone(),
                ))
            }
            other => other,
        };
        let derived = command_outcome_from_operational_indexes(ports, access, identity)?;
        let terminal = match (physical, derived) {
            (None, None) => None,
            (None, Some(value)) => Some(IdempotencyRecordV1::StoredOutcome(value)),
            (Some(value), None) => Some(value),
            (Some(IdempotencyRecordV1::StoredOutcome(physical)), Some(derived))
                if physical == derived =>
            {
                Some(IdempotencyRecordV1::StoredOutcome(physical))
            }
            _ => return Err(storage_error(StorageErrorKind::CorruptData)),
        };
        let value = match (pending, terminal) {
            (None, None) => None,
            (Some(value), None) if value.identity() == identity => {
                Some(StoredAdmissionStateV1::Pending(value))
            }
            (None, Some(IdempotencyRecordV1::StoredOutcome(value)))
                if value.identity() == identity =>
            {
                Some(StoredAdmissionStateV1::StoredOutcome(value))
            }
            (None, Some(IdempotencyRecordV1::ExecutionFailed(value)))
                if value.pending().identity() == identity =>
            {
                Some(StoredAdmissionStateV1::ExecutionFailed(value))
            }
            _ => return Err(storage_error(StorageErrorKind::CorruptData)),
        };
        if let Some(value) = value {
            matches.push(value);
        }
    }
    Ok(matches)
}

fn command_outcome_from_operational_indexes(
    ports: &RedbOperationalPorts,
    access: &RedbReadAccess,
    identity: &IdempotencyIdentity,
) -> Result<Option<StoredOutcomeV1>, StorageError> {
    let key = identity_key(identity)?;
    let exact_key = encode_idempotency_key(&key);
    let frontier = access.application_frontier()?;
    if let Some((segment, locator)) =
        ports.command_derived_member(CommandDerivedIndexKindV1::Idempotency, exact_key)?
        && frontier.is_some_and(|frontier| locator.segment_first <= frontier)
    {
        return command_outcome_from_member(&segment, locator, identity).map(Some);
    }
    // ADR-0165 locator table, consulted BEFORE any absence conclusion below.
    //
    // The coverage short-circuit that follows infers absence from the validated-
    // prefix checkpoint covering the captured frontier. That inference assumes
    // the derived index was built over the checkpoint, which is false on a
    // bounded clean-close start where the index is dormant: on such a start the
    // retained checkpoint's S can equal the captured frontier, so the
    // short-circuit reported a durably committed outcome as absent.
    if let Some(encoded) = access.read_value(JournalTable::IdempotencyLocators, exact_key)? {
        let locator = crate::codec::decode_command_locator_v1(&encoded)?
            .into_parts()
            .0;
        // Fail closed from here: the locator asserted the segment exists.
        let capsule =
            crate::command_authority::command_member_at_access(access, locator.commit_sequence())?
                .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?
                .into_base();
        if capsule.commit_sequence() != locator.commit_sequence()
            || capsule.outcome().identity() != identity
        {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        return Ok(Some(capsule.outcome().clone()));
    }
    if ports.fresh_locator_proves_absence(access)? {
        return Ok(None);
    }
    let Some(frontier) = frontier else {
        return Ok(None);
    };
    if command_derived_index_covers(
        access.checkpoint_application_frontier(),
        ports.command_derived_frontier()?,
        frontier,
    ) {
        return Ok(None);
    }
    let first = access
        .checkpoint_application_frontier()
        .and_then(CommitSequence::checked_next)
        .unwrap_or(CommitSequence::first());
    if first > frontier {
        return Ok(None);
    }
    ports.note_fresh_locator_history_fallback_scan();
    let start = encode_application_sequence_key(first);
    let mut end = encode_application_sequence_key(frontier).to_vec();
    end.push(0);
    let rows = access.read_range(
        JournalTable::Commits,
        &start,
        &end,
        riffdb_storage_api::MAX_COMPOSITE_OVERLAY_TRANSITIONS,
    )?;
    let mut found = None;
    for (_, encoded) in rows {
        let segment = match riffdb_storage_api::decode_command_segment_v1(&encoded) {
            Ok(segment) => segment.into_parts().0,
            Err(error)
                if error.kind()
                    == riffdb_storage_api::DurableCodecErrorKind::UnexpectedRecordType =>
            {
                continue;
            }
            Err(error) => return Err(codec_error(error)),
        };
        let Some(entry) = segment.manifest().entries().iter().find(|entry| {
            entry.kind() == CommandDerivedIndexKindV1::Idempotency && entry.exact_key() == exact_key
        }) else {
            continue;
        };
        let locator = crate::transient::CommandDerivedLocator {
            segment_first: entry.segment_first_commit_sequence(),
            command_ordinal: entry.command_ordinal(),
            member_ordinal: entry.member_ordinal(),
            member: entry.member(),
        };
        let outcome = command_outcome_from_member(&segment, locator, identity)?;
        if found.replace(outcome).is_some() {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
    }
    Ok(found)
}

fn command_derived_index_covers(
    checkpoint: Option<CommitSequence>,
    derived: Option<CommitSequence>,
    captured: CommitSequence,
) -> bool {
    checkpoint == Some(captured) || derived.is_some_and(|frontier| frontier >= captured)
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

fn plan_bundle_exists(
    access: &RedbWriteAccess,
    plan: &riffdb_storage_api::ExecutablePlanRef,
) -> Result<bool, StorageError> {
    let retirement_key =
        crate::keys::encode_contract_write_retirement_key(plan.contract_bundle_hash());
    if let Some(value) = access.read_checkpoint_byte_value(
        crate::layout::CONTRACT_WRITE_RETIREMENTS,
        retirement_key.as_slice(),
    )? {
        let retirement =
            riffdb_storage_api::proto_codec::decode_contract_write_retirement_v1(&value)
                .map_err(crate::error::codec_error)?;
        if retirement.value().artifacts().parent() != plan.contract_bundle_hash() {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        return Ok(false);
    }
    let key = encode_contract_bundle_key(plan.contract_lineage(), plan.contract_version())
        .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
    let Some(value) = access.read_checkpoint_byte_value(CONTRACT_BUNDLES, key.as_slice())? else {
        return Ok(false);
    };
    let bundle = decoded_value(crate::codec::decode_contract_bundle_v1(&value)?);
    Ok(bundle.lineage() == plan.contract_lineage()
        && bundle.contract_version() == plan.contract_version()
        && bundle.bundle_hash() == plan.contract_bundle_hash())
}

fn plan_bundle_exists_from_tables(
    retirements: &impl ReadableTable<&'static [u8], &'static [u8]>,
    bundles: &impl ReadableTable<&'static [u8], &'static [u8]>,
    plan: &riffdb_storage_api::ExecutablePlanRef,
) -> Result<bool, StorageError> {
    let retirement_key =
        crate::keys::encode_contract_write_retirement_key(plan.contract_bundle_hash());
    if let Some(value) = retirements
        .get(retirement_key.as_slice())
        .map_err(precommit_storage_error)?
    {
        let retirement =
            riffdb_storage_api::proto_codec::decode_contract_write_retirement_v1(value.value())
                .map_err(crate::error::codec_error)?;
        if retirement.value().artifacts().parent() != plan.contract_bundle_hash() {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        return Ok(false);
    }
    let key = encode_contract_bundle_key(plan.contract_lineage(), plan.contract_version())
        .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
    let Some(value) = bundles
        .get(key.as_slice())
        .map_err(precommit_storage_error)?
    else {
        return Ok(false);
    };
    let bundle = decoded_value(crate::codec::decode_contract_bundle_v1(value.value())?);
    Ok(bundle.lineage() == plan.contract_lineage()
        && bundle.contract_version() == plan.contract_version()
        && bundle.bundle_hash() == plan.contract_bundle_hash())
}

fn current_state_uncached(
    transaction: &crate::store::OperationalWriteTransaction,
    request: &ValidationReadRequest,
) -> Result<TransactionCurrentState, StorageError> {
    let mut builder = TransactionCurrentStateBuilder::new(request);
    if !request.binding_targets().is_empty()
        || !request.root_validation_targets().is_empty()
        || !request.cascade_targets().is_empty()
    {
        let entities = transaction.open_table(ENTITIES).map_err(table_error)?;
        for target in request.binding_targets() {
            builder
                .push_binding(entity_observation_from_table(&entities, target)?)
                .map_err(materialization_value)?;
        }
        for target in request.root_validation_targets() {
            builder
                .push_root_validation(entity_observation_from_table(&entities, target)?)
                .map_err(materialization_value)?;
        }
        for target in request.cascade_targets() {
            builder
                .push_cascade_predecessor(entity_observation_from_table(&entities, target)?)
                .map_err(materialization_value)?;
        }
    }
    if !request.range_targets().is_empty() {
        let epochs = transaction.open_table(INDEX_EPOCHS).map_err(table_error)?;
        for target in request.range_targets() {
            builder
                .push_range(CurrentRangeObservation::new(
                    target.clone(),
                    epoch_position_from_table(&epochs, target.generation_target())?,
                ))
                .map_err(materialization_value)?;
        }
    }
    builder.finish().map_err(materialization_value)
}

fn current_state_cached(
    core: &mut BatchCore,
    request: &ValidationReadRequest,
) -> Result<TransactionCurrentState, StorageError> {
    let mut builder = TransactionCurrentStateBuilder::new(request);
    for target in request.binding_targets() {
        builder
            .push_binding(cached_entity_observation(core, target)?)
            .map_err(materialization_value)?;
    }
    for target in request.root_validation_targets() {
        builder
            .push_root_validation(cached_entity_observation(core, target)?)
            .map_err(materialization_value)?;
    }
    for target in request.cascade_targets() {
        builder
            .push_cascade_predecessor(cached_entity_observation(core, target)?)
            .map_err(materialization_value)?;
    }
    for target in request.range_targets() {
        builder
            .push_range(CurrentRangeObservation::new(
                target.clone(),
                cached_index_generation(core, target.generation_target())?,
            ))
            .map_err(materialization_value)?;
    }
    builder.finish().map_err(materialization_value)
}

fn transaction_current_policy_state(
    core: &BatchCore,
    request: &TransactionCurrentPolicyRequestV1,
) -> Result<TransactionCurrentPolicyStateV1, StorageError> {
    let capability = core
        .access
        .read_command_capability_bytes(request.capability_id())?
        .map(|value| {
            let record = decoded_value(decode_capability_record_v1(&value)?);
            if record.capability_id() != request.capability_id() {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
            Ok(record)
        })
        .transpose()?;
    let mut relationship_exists = Vec::with_capacity(request.lookups().len());
    for lookup in request.lookups() {
        let upper = exclusive_prefix_end(lookup.index_prefix())
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
        let mut exists = false;
        let rows = core.access.read_command_range(
            JournalTable::SecondaryIndexes,
            lookup.index_prefix(),
            &upper,
            MAX_INDEX_SCAN_INSPECTED_ENTRIES.saturating_add(1),
        )?;
        if rows.len() > MAX_INDEX_SCAN_INSPECTED_ENTRIES {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }
        for (key, value) in rows {
            let physical = decode_index_entry_key(&key)
                .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
            let record = decoded_value(decode_index_entry_v2(&value)?);
            if record.key() != &physical {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
            if record.key().index_id() == lookup.index_id()
                && record.partition_key() == lookup.partition()
            {
                exists = true;
                break;
            }
        }
        relationship_exists.push(exists);
    }
    TransactionCurrentPolicyStateV1::new(request, capability, relationship_exists)
        .map_err(materialization_value)
}

fn transaction_current_vector_evidence(
    core: &BatchCore,
    request: &VectorEvidenceReadRequestV1,
) -> Result<TransactionCurrentVectorEvidenceV1, StorageError> {
    let mut observations = Vec::with_capacity(request.targets().len());
    for target in request.targets() {
        let key = encode_vector_evidence_key(target.target().key(), target.vector_field())
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let observation = core
            .access
            .read_command_value(JournalTable::VectorEvidence, &key)?
            .map(|bytes| {
                let row: StoredVectorEvidenceV1 = decoded_value(decode_vector_evidence_v1(&bytes)?);
                if row.target() != target.target() || row.vector_field() != target.vector_field() {
                    return Err(storage_error(StorageErrorKind::CorruptData));
                }
                Ok(row)
            })
            .transpose()?;
        observations.push(observation);
    }
    TransactionCurrentVectorEvidenceV1::new(request.clone(), observations)
        .map_err(materialization_value)
}

fn cached_entity_observation(
    core: &mut BatchCore,
    target: &EntityTarget,
) -> Result<EntityObservation, StorageError> {
    if let Some(observation) = core.entity_observations.get(target) {
        if !core.entity_observation_bytes.contains_key(target) {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        return Ok(observation.clone());
    }
    let (observation, encoded) = entity_observation_from_access(&core.access, target)?;
    core.entity_observations
        .insert(target.clone(), observation.clone());
    core.entity_observation_bytes
        .insert(target.clone(), encoded);
    Ok(observation)
}

fn cached_index_generation(
    core: &mut BatchCore,
    target: &PartitionIndexTarget,
) -> Result<IndexEpochPosition, StorageError> {
    if let Some(position) = core.index_generations.get(target) {
        return Ok(*position);
    }
    let position = { epoch_position_from_access(&core.access, target)? };
    core.index_generations.insert(target.clone(), position);
    Ok(position)
}

fn affected_current_state_cached(
    core: &mut BatchCore,
    targets: &AffectedIndexEpochTargets,
) -> Result<AffectedEpochCurrentState, StorageError> {
    let mut builder = AffectedEpochCurrentStateBuilder::new(targets);
    for target in targets.as_slice() {
        builder
            .push(CurrentIndexGenerationObservation::new(
                target.clone(),
                cached_index_generation(core, target)?,
            ))
            .map_err(materialization_value)?;
    }
    if !targets.unique_targets().is_empty() {
        for target in targets.unique_targets() {
            let prefix = target.prefix().prefix().as_bytes();
            let upper = exclusive_prefix_end(prefix)
                .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
            let rows = core.access.read_command_range(
                JournalTable::SecondaryIndexes,
                prefix,
                upper.as_slice(),
                2,
            )?;
            let kind = match rows.as_slice() {
                [] => UniqueOccupancyKind::Vacant,
                [(key, _)] if key.as_ref() == target.expected_entry().as_bytes() => {
                    UniqueOccupancyKind::Owned
                }
                [(_, _)] => UniqueOccupancyKind::Conflict,
                _ => return Err(storage_error(StorageErrorKind::CorruptData)),
            };
            builder
                .push_unique(UniqueIndexOccupancy::new(target.clone(), kind))
                .map_err(materialization_value)?;
        }
    }
    builder.finish().map_err(materialization_value)
}

fn exclusive_prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    let position = end.iter().rposition(|byte| *byte != u8::MAX)?;
    end[position] = end[position].checked_add(1)?;
    end.truncate(position + 1);
    Some(end)
}

fn entity_observation_from_table(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    target: &EntityTarget,
) -> Result<EntityObservation, StorageError> {
    let Some(value) = table
        .get(encode_entity_key(target.key()))
        .map_err(precommit_storage_error)?
    else {
        return Ok(EntityObservation::Absent(target.clone()));
    };
    let record = decoded_value(decode_entity_record_v1(value.value())?);
    if record.target() != target {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    Ok(EntityObservation::Present(record))
}

fn entity_observation_from_access(
    access: &RedbWriteAccess,
    target: &EntityTarget,
) -> Result<(EntityObservation, Option<Vec<u8>>), StorageError> {
    let Some(value) =
        access.read_command_value(JournalTable::Entities, encode_entity_key(target.key()))?
    else {
        return Ok((EntityObservation::Absent(target.clone()), None));
    };
    let record = decoded_value(decode_entity_record_v1(&value)?);
    if record.target() != target {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    Ok((EntityObservation::Present(record), Some(value)))
}

fn epoch_position_from_table(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    target: &PartitionIndexTarget,
) -> Result<IndexEpochPosition, StorageError> {
    let key = encode_partition_index_key(target);
    let Some(value) = table.get(key.as_slice()).map_err(precommit_storage_error)? else {
        return Ok(IndexEpochPosition::BeforeFirst);
    };
    let record = decoded_value(decode_index_epoch_v1(value.value())?);
    if record.target() != target {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    Ok(IndexEpochPosition::Value(record.epoch()))
}

fn epoch_position_from_access(
    access: &RedbWriteAccess,
    target: &PartitionIndexTarget,
) -> Result<IndexEpochPosition, StorageError> {
    let key = encode_partition_index_key(target);
    let Some(value) = access.read_command_value(JournalTable::IndexEpochs, key.as_slice())? else {
        return Ok(IndexEpochPosition::BeforeFirst);
    };
    let record = decoded_value(decode_index_epoch_v1(&value)?);
    if record.target() != target {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    Ok(IndexEpochPosition::Value(record.epoch()))
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

fn provenance_exists(core: &BatchCore, provenance_id: ProvenanceId) -> Result<bool, StorageError> {
    if core.reserved_provenance_ids.contains(&provenance_id) {
        return Ok(true);
    }
    let key = encode_provenance_key(provenance_id);
    if core
        .access
        .command_derived_key_exists(CommandDerivedIndexKindV1::Provenance, key.as_slice())?
    {
        return Ok(true);
    }
    // ADR-0165: provenance is segment-owned, so PROVENANCE is empty and the
    // locator table is what proves the id is taken. Checking only PROVENANCE
    // reported a reserved id as free.
    if core
        .access
        .read_command_value(JournalTable::Provenance, key.as_slice())?
        .is_some()
    {
        return Ok(true);
    }
    Ok(core
        .access
        .read_command_value(JournalTable::ProvenanceLocators, key.as_slice())?
        .is_some())
}

fn metrics_after(
    current: Option<StagedBatchMetrics>,
    records: &AtomicCommandRecordSet,
) -> Result<StagedBatchMetrics, StorageError> {
    metrics_after_charge(current, records.presequence_charge())
}

fn metrics_after_charge(
    current: Option<StagedBatchMetrics>,
    charge: riffdb_storage_api::CommandWriteSetChargeV1,
) -> Result<StagedBatchMetrics, StorageError> {
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
        NonZeroU16::new(count)
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?,
        semantic,
        encoded,
    )
    .map_err(invariant_value)
}

fn identity_key(identity: &IdempotencyIdentity) -> Result<IdempotencyIdentityKey, StorageError> {
    identity
        .storage_key()
        .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))
}

fn sequence_error(error: riffdb_storage_api::SequenceAllocationError) -> StorageError {
    match error {
        riffdb_storage_api::SequenceAllocationError::Exhausted => {
            storage_error(StorageErrorKind::SequenceExhausted)
        }
        riffdb_storage_api::SequenceAllocationError::ZeroCount
        | riffdb_storage_api::SequenceAllocationError::TooMany => {
            storage_error(StorageErrorKind::InvariantViolation)
        }
    }
}

fn invariant_value(_: StorageValueError) -> StorageError {
    storage_error(StorageErrorKind::InvariantViolation)
}

fn materialization_value(error: StorageValueError) -> StorageError {
    match error {
        StorageValueError::LimitExceeded | StorageValueError::SizeOverflow => {
            storage_error(StorageErrorKind::LimitExceeded)
        }
        _ => storage_error(StorageErrorKind::CorruptData),
    }
}

fn storage_error(kind: StorageErrorKind) -> StorageError {
    StorageError::new(kind, None)
}

fn decoded_value<T>(item: riffdb_storage_api::EncodedPageItem<T>) -> T {
    item.into_parts().0
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU16;

    use riffdb_query_executor::{QueryExecutionPort, VectorInspectionTargetV1};
    use riffdb_storage_api::{
        DatabaseInitializationPort, IdempotencyKeyDigest, IdempotencyLookupCandidatesV1,
        StorageScanLimit, VectorEvidenceIndexEntryV1, VectorEvidenceIndexScanRequestV1,
        VectorHealthFieldObservationV1, VectorHealthObservationV1, VectorObservationCountsV1,
    };
    use riffdb_types::{
        ActorId, AggregateTypeId, CommandId, ContractLineage, DatabaseId, DigestKeyId,
        EntityKeyBuilder, EntityTypeId, Environment, FieldId, PartitionKeyBuilder, TenantId,
        TenantScope,
    };

    use super::*;
    use crate::layout::{VECTOR_EVIDENCE_INDEX, VECTOR_OBSERVATIONS};
    use crate::store::RedbStore;

    fn vector_observation_target(partition: u64) -> VectorObservationTargetV1 {
        let mut key = PartitionKeyBuilder::new(
            AggregateTypeId::new(9).expect("vector observation aggregate"),
        );
        key.push_u64(partition).expect("partition component");
        VectorObservationTargetV1::new(
            ContractLineage::new("vector-observation-test").expect("lineage"),
            key.finish().expect("partition key"),
            EntityTypeId::new(7).expect("entity type"),
            FieldId::new(8).expect("vector field"),
        )
    }

    // req: OUT-001, OUT-002, TXN-042, PERF-019
    #[test]
    fn cold_fresh_database_publications_complete_without_history_scans() {
        let scope = crate::test_path::ScopedDirectory::new("fresh-prefix-private-scan-count");
        let path = scope.join("db.redb");
        let database_id =
            DatabaseId::from_unix_milliseconds_and_random(1, [0x74; 10]).expect("database ID");
        let mut store = RedbStore::open(&path).expect("open store");
        store
            .initialize_database(database_id)
            .expect("initialize store");
        let ports = crate::store::RedbDormantPorts {
            pending_v3_activation: None,
            shared: Arc::clone(&store.shared),
        }
        .into_operational_after_catalog_validation()
        .expect("activate ports");
        let access = ports.begin_write().expect("first command-write entry");
        access
            .arm_fresh_locator_coverage()
            .expect("arm exact empty authority");
        access.abort().expect("abort mutation-free entry");

        let identity = IdempotencyIdentity::new(
            database_id,
            Environment::new("test").expect("environment"),
            TenantScope::Tenant(TenantId::new("tenant-a").expect("tenant")),
            ActorId::new("principal-a").expect("actor"),
            ContractLineage::new("fresh-prefix").expect("lineage"),
            CommandId::new(1).expect("command"),
            IdempotencyKeyDigest::from_hmac_bytes(
                DigestKeyId::new(1).expect("digest key"),
                [0x55; 32],
            ),
        );
        let candidates =
            IdempotencyLookupCandidatesV1::new(vec![identity]).expect("lookup candidates");
        assert_eq!(
            ports.lookup_admission(candidates).expect("covered miss"),
            AdmissionLookupResultV1::NotFound
        );
        assert_eq!(ports.shared.fresh_locator_history_fallback_scans(), 0);
    }

    fn vector_index_entry(value: u64) -> VectorEvidenceIndexEntryV1 {
        let mut key = EntityKeyBuilder::new(EntityTypeId::new(7).expect("entity type"));
        key.push_u64(value).expect("entity key");
        VectorEvidenceIndexEntryV1::from_parts(
            vector_observation_target(1),
            key.finish().expect("entity key"),
            CommitSequence::new(value).expect("sequence"),
            Some(CommitSequence::new(value).expect("source sequence")),
            None,
        )
        .expect("index entry")
    }

    #[test]
    fn vector_observation_repository_reads_one_exact_canonical_row() {
        let scope = crate::test_path::ScopedDirectory::new("vector-observation-read");
        let path = scope.join("db.redb");
        let mut store = RedbStore::open(&path).expect("open store");
        store
            .initialize_database(
                DatabaseId::from_unix_milliseconds_and_random(1, [0x71; 10]).expect("database ID"),
            )
            .expect("initialize store");
        let ports = RedbOperationalPorts {
            shared: Arc::clone(&store.shared),
        };
        let present = vector_observation_target(1);
        let absent = vector_observation_target(2);
        let expected = VectorObservationCountsV1::from_parts(
            present.clone(),
            3,
            1,
            Vec::new(),
            CommitSequence::new(11).expect("revision"),
        )
        .expect("observation");
        let key = encode_vector_observation_key(&present).expect("observation key");
        let value = encode_vector_observation_v1(&expected).expect("observation value");
        let health = VectorHealthObservationV1::from_parts(
            present.lineage().clone(),
            vec![
                VectorHealthFieldObservationV1::from_parts(
                    present.entity_type(),
                    present.vector_field(),
                    3,
                    1,
                    0,
                )
                .expect("field health"),
            ],
            CommitSequence::new(11).expect("revision"),
        )
        .expect("health");
        let health_key = encode_vector_health_observation_key(present.lineage())
            .expect("health observation key");
        let health_value =
            encode_vector_health_observation_v1(&health).expect("health observation value");
        let transaction = store
            .shared
            .database
            .begin_write()
            .expect("write transaction");
        {
            let mut table = transaction
                .open_table(VECTOR_OBSERVATIONS)
                .expect("observation table");
            table
                .insert(key.as_slice(), value.as_bytes())
                .expect("insert observation");
            table
                .insert(health_key.as_slice(), health_value.as_bytes())
                .expect("insert health observation");
        }
        transaction.commit().expect("commit observation");

        assert_eq!(
            ports
                .read_vector_observation(&present)
                .expect("read present observation"),
            Some(expected)
        );
        assert_eq!(
            ports
                .read_vector_observation(&absent)
                .expect("read absent observation"),
            None
        );
        assert_eq!(
            ports
                .read_vector_health_observation(present.lineage())
                .expect("read health observation"),
            Some(health)
        );
    }

    #[test]
    fn vector_evidence_index_repository_pages_over_the_published_composite_view() {
        let scope = crate::test_path::ScopedDirectory::new("vector-evidence-index-read");
        let path = scope.join("db.redb");
        let mut store = RedbStore::open(&path).expect("open store");
        store
            .initialize_database(
                DatabaseId::from_unix_milliseconds_and_random(1, [0x72; 10]).expect("database ID"),
            )
            .expect("initialize store");
        let ports = RedbOperationalPorts {
            shared: Arc::clone(&store.shared),
        };
        let transaction = store
            .shared
            .database
            .begin_write()
            .expect("write transaction");
        {
            let mut table = transaction
                .open_table(VECTOR_EVIDENCE_INDEX)
                .expect("index table");
            for entry in [vector_index_entry(1), vector_index_entry(2)] {
                let key = crate::keys::encode_vector_evidence_index_key(
                    entry.target(),
                    entry.entity_key(),
                )
                .expect("index key");
                let value = encode_vector_evidence_index_v1(&entry).expect("index value");
                table
                    .insert(key.as_slice(), value.as_bytes())
                    .expect("insert index row");
            }
        }
        transaction.commit().expect("commit index rows");

        let limit = StorageScanLimit::new(1).expect("limit");
        let first_request =
            VectorEvidenceIndexScanRequestV1::new(vector_observation_target(1), None, limit)
                .expect("request");
        let first = ports
            .scan_vector_evidence_index(&first_request)
            .expect("first page");
        assert_eq!(first.entries().len(), 1);
        assert!(!first.exact_end());
        let second_request = VectorEvidenceIndexScanRequestV1::new(
            vector_observation_target(1),
            first.continuation().cloned(),
            limit,
        )
        .expect("request");
        let second = ports
            .scan_vector_evidence_index(&second_request)
            .expect("second page");
        assert_eq!(second.entries().len(), 1);
        assert!(second.exact_end());
        assert!(first.entries()[0].entity_key() < second.entries()[0].entity_key());
    }

    #[test]
    fn vector_inspection_reads_counts_evidence_and_frontier_from_one_snapshot() {
        let scope = crate::test_path::ScopedDirectory::new("vector-inspection-snapshot");
        let path = scope.join("db.redb");
        let mut store = RedbStore::open(&path).expect("open store");
        store
            .initialize_database(
                DatabaseId::from_unix_milliseconds_and_random(1, [0x73; 10]).expect("database ID"),
            )
            .expect("initialize store");
        let ports = RedbOperationalPorts {
            shared: Arc::clone(&store.shared),
        };
        let target = vector_observation_target(1);
        let observation = VectorObservationCountsV1::from_parts(
            target.clone(),
            2,
            2,
            Vec::new(),
            CommitSequence::new(2).expect("revision"),
        )
        .expect("observation");
        let transaction = store
            .shared
            .database
            .begin_write()
            .expect("write transaction");
        {
            let mut observations = transaction
                .open_table(VECTOR_OBSERVATIONS)
                .expect("observation table");
            let key = encode_vector_observation_key(&target).expect("observation key");
            let value = encode_vector_observation_v1(&observation).expect("observation value");
            observations
                .insert(key.as_slice(), value.as_bytes())
                .expect("insert observation");
        }
        {
            let mut index = transaction
                .open_table(VECTOR_EVIDENCE_INDEX)
                .expect("index table");
            for entry in [vector_index_entry(1), vector_index_entry(2)] {
                let key = crate::keys::encode_vector_evidence_index_key(
                    entry.target(),
                    entry.entity_key(),
                )
                .expect("index key");
                let value = encode_vector_evidence_index_v1(&entry).expect("index value");
                index
                    .insert(key.as_slice(), value.as_bytes())
                    .expect("insert index row");
            }
        }
        transaction.commit().expect("commit snapshot facts");

        let request = VectorInspectionTargetV1::new(
            target.lineage().clone(),
            target.partition_key().clone(),
            target.entity_type(),
            target.vector_field(),
            None,
            NonZeroU16::new(1).expect("limit"),
        );
        let snapshot = QueryExecutionPort::inspect_vector_evidence(&ports, &request, None)
            .expect("inspect snapshot");
        assert_eq!(snapshot.total_entities(), 2);
        assert_eq!(snapshot.stale_entities(), 2);
        assert_eq!(
            snapshot.revision(),
            Some(CommitSequence::new(2).expect("revision"))
        );
        assert_eq!(snapshot.candidates().len(), 1);
        assert!(!snapshot.exact_end());
        assert!(snapshot.continuation().is_some());
        assert!(snapshot.admission().is_none());
    }

    fn generation_target(index: u32) -> PartitionIndexTarget {
        let aggregate = riffdb_types::AggregateTypeId::new(1).expect("aggregate");
        let mut partition = riffdb_types::PartitionKeyBuilder::new(aggregate);
        partition.push_str("tenant").expect("partition component");
        PartitionIndexTarget::new(
            partition.finish().expect("partition"),
            riffdb_types::IndexId::new(index).expect("index"),
        )
    }

    #[test]
    fn detached_generation_base_retains_targets_discovered_after_the_first_candidate() {
        let first = generation_target(1);
        let later = generation_target(2);
        let first_prior =
            IndexEpochPosition::Value(riffdb_types::IndexEpoch::new(3).expect("first prior epoch"));
        let later_prior =
            IndexEpochPosition::Value(riffdb_types::IndexEpoch::new(7).expect("later prior epoch"));
        let mut current = BTreeMap::from([(first.clone(), first_prior)]);
        let mut base = None;

        retain_detached_index_generation_base(&mut base, &current, &first, first_prior)
            .expect("capture first target");
        current.insert(
            first.clone(),
            IndexEpochPosition::Value(
                riffdb_types::IndexEpoch::new(4).expect("advanced first epoch"),
            ),
        );
        current.insert(later.clone(), later_prior);
        retain_detached_index_generation_base(&mut base, &current, &later, later_prior)
            .expect("capture later target");

        assert_eq!(
            base,
            Some(BTreeMap::from(
                [(first, first_prior), (later, later_prior),]
            ))
        );
    }

    #[test]
    fn detached_index_free_group_preserves_the_exact_generation_map() {
        let target = generation_target(1);
        let position = IndexEpochPosition::Value(riffdb_types::IndexEpoch::new(3).expect("epoch"));
        let expected = BTreeMap::from([(target, position)]);

        let restored = detached_index_generation_base(None, &expected, false)
            .expect("index-free detached group");

        assert_eq!(restored, expected);
    }

    #[test]
    fn missing_detached_generation_base_with_an_advance_is_integrity() {
        let error = detached_index_generation_base(None, &BTreeMap::new(), true)
            .expect_err("advanced group requires retained base");

        assert_eq!(error.kind(), StorageErrorKind::InvariantViolation);
    }

    #[test]
    fn derived_command_coverage_proves_absence_only_through_the_captured_frontier() {
        let captured = CommitSequence::new(8).expect("captured frontier");

        assert!(command_derived_index_covers(Some(captured), None, captured));
        assert!(command_derived_index_covers(
            CommitSequence::new(3),
            CommitSequence::new(9),
            captured
        ));
        assert!(!command_derived_index_covers(
            CommitSequence::new(3),
            CommitSequence::new(7),
            captured
        ));
        assert!(!command_derived_index_covers(None, None, captured));
    }
}
