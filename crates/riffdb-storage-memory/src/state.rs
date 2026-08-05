//! Private typed state shared by all memory-backend semantic ports.

use riffdb_storage_api::{
    AdministrationSequenceRange, CapabilityTokenLookupV1, CommandWriteClassBreakdownV1,
    DurableCodecError, DurableCodecErrorKind, EncodedContentCharge, EntityTarget,
    ExecutablePlanRef, HistoricalPersistedKeyEvidenceV1, IdempotencyIdentityKey,
    IndexMigrationSemanticRow, OutboxStatusObservationV1, OutboxTransitionResultV1,
    RetainedMetadataV1, SequenceAllocationError, StorageError, StorageErrorKind,
    StoredAdministrationAuditRecordV1, StoredAdmissionStateV1, StoredCapabilityRecordV1,
    StoredCommitRecordV1, StoredContractBundleV1, StoredDurableEventV1, StoredEntityRecordV1,
    StoredEventConsumerDeliveryV1, StoredEventConsumerV1, StoredEventRouteV1, StoredIndexEntryV1,
    StoredIndexEntryV2, StoredIndexEpochV1, StoredOutboxIntentV1, StoredOutboxStatusV1,
    StoredProjectionApplyV1, StoredProjectionControlV1, StoredProjectionStateV1,
    StoredProvenanceRecordV1, StoredQueryModuleAdministrationV1, StoredQueryModuleV1,
    StoredReactiveModuleV1,
};
use riffdb_types::{
    AdministrationSequence, CapabilityTokenDigest, CommitSequence, ContractBundleHash,
    ContractLineage, ContractVersion, EventId, PartitionKeyHash, ProjectionIdentity, ProvenanceId,
    RequestId,
};

use crate::store::storage_error;

/// Explicit memory-model content charge.
///
/// One retained semantic record has an exact synthetic charge of one byte.
/// Command-class values may hold a larger caller-supplied conservative
/// reservation. Neither value claims to be a real encoded `StoredEnvelope`;
/// WP-065 owns that codec and its byte proofs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(dead_code)]
pub(crate) struct SyntheticRecordCharge(EncodedContentCharge);

pub(crate) const MEMORY_SYNTHETIC_RECORD_BYTES: usize = 1;

/// Memory representation of the durable `(partition, event)` routing index.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct EventRouteRow {
    pub(crate) partition_hash: PartitionKeyHash,
    pub(crate) route: StoredEventRouteV1,
}

impl EventRouteRow {
    pub(crate) const fn new(partition_hash: PartitionKeyHash, route: StoredEventRouteV1) -> Self {
        Self {
            partition_hash,
            route,
        }
    }

    pub(crate) const fn order_key(self) -> (PartitionKeyHash, EventId) {
        (self.partition_hash, self.route.event_id())
    }
}

#[allow(dead_code)]
impl SyntheticRecordCharge {
    pub(crate) const fn new(charge: EncodedContentCharge) -> Self {
        Self(charge)
    }

    pub(crate) const fn encoded_content_charge(self) -> EncodedContentCharge {
        self.0
    }
}

/// Returns the exact one-record charge used by the volatile reference model.
pub(crate) fn memory_record_charge() -> EncodedContentCharge {
    EncodedContentCharge::new(MEMORY_SYNTHETIC_RECORD_BYTES)
        .expect("the fixed nonzero memory charge is within the absolute bound")
}

/// Returns the exact charge for a nonempty composite of memory-model records.
pub(crate) fn memory_composite_charge(
    record_count: usize,
) -> Result<EncodedContentCharge, StorageError> {
    EncodedContentCharge::new(record_count)
        .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))
}

/// Retained caller-supplied synthetic class charges for one command graph.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(dead_code)]
pub(crate) struct SyntheticCommandClassCharges {
    pub(crate) allocator: Option<SyntheticRecordCharge>,
    pub(crate) pending_resolution: Option<SyntheticRecordCharge>,
    pub(crate) entities: Option<SyntheticRecordCharge>,
    pub(crate) index_entries: Option<SyntheticRecordCharge>,
    pub(crate) index_epochs: Option<SyntheticRecordCharge>,
    pub(crate) outcome: Option<SyntheticRecordCharge>,
    pub(crate) events: Option<SyntheticRecordCharge>,
    pub(crate) outbox_intents: Option<SyntheticRecordCharge>,
    pub(crate) provenance: Option<SyntheticRecordCharge>,
    pub(crate) commit: Option<SyntheticRecordCharge>,
}

#[allow(dead_code)]
impl SyntheticCommandClassCharges {
    pub(crate) fn from_supplied_classes(
        classes: CommandWriteClassBreakdownV1,
    ) -> Result<Self, StorageError> {
        Ok(Self {
            allocator: synthetic_class_charge(classes.allocator())?,
            pending_resolution: synthetic_class_charge(classes.pending_resolution())?,
            entities: synthetic_class_charge(classes.entities())?,
            index_entries: synthetic_class_charge(classes.index_entries())?,
            index_epochs: synthetic_class_charge(classes.index_epochs())?,
            outcome: synthetic_class_charge(classes.outcome())?,
            events: synthetic_class_charge(classes.events())?,
            outbox_intents: synthetic_class_charge(classes.outbox_intents())?,
            provenance: synthetic_class_charge(classes.provenance())?,
            commit: synthetic_class_charge(classes.commit())?,
        })
    }
}

#[allow(dead_code)]
fn synthetic_class_charge(bytes: usize) -> Result<Option<SyntheticRecordCharge>, StorageError> {
    if bytes == 0 {
        return Ok(None);
    }
    EncodedContentCharge::new(bytes)
        .map(SyntheticRecordCharge::new)
        .map(Some)
        .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))
}

#[derive(Clone)]
#[allow(dead_code)]
pub(crate) struct KeyedSyntheticCharge<K> {
    pub(crate) key: K,
    pub(crate) charge: SyntheticRecordCharge,
}

/// Memory-only charge sidecars. They are model evidence, not durable records.
///
/// Keeping these separate avoids coupling structural record representation to a
/// codec that does not exist until WP-065. Mutation modules must update each
/// sidecar atomically with its corresponding semantic row.
#[derive(Clone, Default)]
pub(crate) struct MemorySyntheticCharges {
    pub(crate) command_classes: Vec<(CommitSequence, SyntheticCommandClassCharges)>,
    pub(crate) administration_audit: Vec<KeyedSyntheticCharge<AdministrationSequence>>,
    pub(crate) outbox_statuses: Vec<KeyedSyntheticCharge<EventId>>,
    pub(crate) projection_controls: Vec<KeyedSyntheticCharge<ProjectionIdentity>>,
}

impl MemorySyntheticCharges {
    fn is_empty(&self) -> bool {
        self.command_classes.is_empty()
            && self.administration_audit.is_empty()
            && self.outbox_statuses.is_empty()
            && self.projection_controls.is_empty()
    }
}

/// A completely prepared memory mutation whose application cannot fail.
///
/// All semantic validation precedes `apply`; it returns no recoverable error and
/// performs only the already-validated in-memory changes. A process-level
/// allocation failure remains fatal for this volatile reference model, so it
/// cannot return a partially applied semantic transition.
pub(crate) trait PreparedMemoryDelta {
    type Output;

    fn apply(self, state: &mut MemoryState) -> Self::Output;
}

/// Preflighted administration allocation plus its complete metadata post-image.
#[allow(dead_code)]
pub(crate) struct PreparedAdministrationAllocation {
    assigned: AdministrationSequenceRange,
    metadata_post_image: RetainedMetadataV1,
}

#[allow(dead_code)]
impl PreparedAdministrationAllocation {
    pub(crate) fn assigned(&self) -> &[AdministrationSequence] {
        self.assigned.assigned()
    }

    pub(crate) fn into_metadata_post_image(self) -> RetainedMetadataV1 {
        self.metadata_post_image
    }
}

/// Exact compare-and-set preparation for one projection control row.
#[allow(dead_code)]
pub(crate) enum ProjectionControlCasPreparation {
    Apply(PreparedProjectionControlCas),
    Existing(StoredProjectionControlV1),
    StateChanged,
}

#[allow(dead_code)]
pub(crate) struct PreparedProjectionControlCas {
    position: PreparedRowPosition,
    charge_position: PreparedRowPosition,
    updated: StoredProjectionControlV1,
    charge: KeyedSyntheticCharge<ProjectionIdentity>,
}

impl PreparedMemoryDelta for PreparedProjectionControlCas {
    type Output = StoredProjectionControlV1;

    fn apply(self, state: &mut MemoryState) -> Self::Output {
        apply_prepared_row(
            &mut state.projection_controls,
            self.position,
            self.updated.clone(),
        );
        apply_prepared_row(
            &mut state.synthetic_charges.projection_controls,
            self.charge_position,
            self.charge,
        );
        self.updated
    }
}

/// Exact compare-and-set preparation for one derived outbox status row.
#[allow(dead_code)]
pub(crate) enum OutboxStatusCasPreparation {
    Apply(PreparedOutboxStatusCas),
    NoChange(OutboxTransitionResultV1),
}

#[allow(dead_code)]
pub(crate) struct PreparedOutboxStatusCas {
    position: PreparedRowPosition,
    charge_position: PreparedRowPosition,
    updated: StoredOutboxStatusV1,
    charge: KeyedSyntheticCharge<EventId>,
}

impl PreparedMemoryDelta for PreparedOutboxStatusCas {
    type Output = OutboxTransitionResultV1;

    fn apply(self, state: &mut MemoryState) -> Self::Output {
        apply_prepared_row(
            &mut state.outbox_statuses,
            self.position,
            self.updated.clone(),
        );
        apply_prepared_row(
            &mut state.synthetic_charges.outbox_statuses,
            self.charge_position,
            self.charge,
        );
        OutboxTransitionResultV1::Applied(self.updated)
    }
}

#[derive(Clone, Copy)]
#[allow(dead_code)]
enum PreparedRowPosition {
    Replace(usize),
    Insert(usize),
}

#[allow(dead_code)]
fn apply_prepared_row<T>(rows: &mut Vec<T>, position: PreparedRowPosition, value: T) {
    match position {
        PreparedRowPosition::Replace(index) => rows[index] = value,
        PreparedRowPosition::Insert(index) => rows.insert(index, value),
    }
}

#[derive(Clone, Default)]
pub(crate) enum MemoryMetadataSlot {
    #[default]
    Absent,
    Retained(RetainedMetadataV1),
    #[cfg(test)]
    Corrupt,
}

#[derive(Clone)]
pub(crate) struct CapabilityLookupRow {
    pub(crate) digest: CapabilityTokenDigest,
    pub(crate) value: CapabilityTokenLookupV1,
}

#[derive(Clone)]
pub(crate) struct HistoricalPersistedKeyRow {
    pub(crate) order_key: Vec<u8>,
    pub(crate) evidence: HistoricalPersistedKeyEvidenceV1,
}

#[derive(Clone)]
pub(crate) struct CatalogBundleRow {
    pub(crate) order_key: Vec<u8>,
    pub(crate) bundle: StoredContractBundleV1,
}

/// Reverse index from a commit sequence to the terminal admission identity.
///
/// Future command writers must install this row atomically with the command
/// graph. Its sequence order permits bounded commit-to-admission validation.
#[allow(dead_code)]
#[derive(Clone)]
pub(crate) struct CommitAdmissionIndexRow {
    pub(crate) commit_sequence: CommitSequence,
    pub(crate) identity_key: IdempotencyIdentityKey,
}

/// Identity-ordered reciprocal of [`CommitAdmissionIndexRow`].
#[allow(dead_code)]
#[derive(Clone)]
pub(crate) struct CommittedAdmissionIndexRow {
    pub(crate) identity_key: IdempotencyIdentityKey,
    pub(crate) commit_sequence: CommitSequence,
}

/// Target-ordered proof of the commit owning each current entity post-image.
#[allow(dead_code)]
#[derive(Clone)]
pub(crate) struct EntityCommitIndexRow {
    pub(crate) target: EntityTarget,
    pub(crate) commit_sequence: CommitSequence,
}

/// Sequence-ordered membership index for catalog activations in the shared
/// administration stream.
#[allow(dead_code)]
#[derive(Clone)]
pub(crate) struct CatalogActivationIndexRow {
    pub(crate) administration_sequence: AdministrationSequence,
}

/// Bundle-identity-ordered reciprocal of [`CatalogActivationIndexRow`].
#[allow(dead_code)]
#[derive(Clone)]
pub(crate) struct CatalogBundleActivationIndexRow {
    pub(crate) order_key: Vec<u8>,
    pub(crate) administration_sequence: AdministrationSequence,
}

/// Canonical request lifecycle index for service-audit phase records.
#[allow(dead_code)]
#[derive(Clone)]
pub(crate) enum ServiceAuditLifecycleIndex {
    /// A denied/failed/cancelled invocation that never entered protected work.
    Standalone { sequence: AdministrationSequence },
    /// An admitted invocation, with at most one durable terminal phase.
    Started {
        started_sequence: AdministrationSequence,
        terminal_sequence: Option<AdministrationSequence>,
    },
}

#[allow(dead_code)]
#[derive(Clone)]
pub(crate) struct ServiceAuditInvocationIndexRow {
    pub(crate) request_id: RequestId,
    pub(crate) lifecycle: ServiceAuditLifecycleIndex,
}

impl CatalogBundleRow {
    #[allow(dead_code)]
    pub(crate) fn new(bundle: StoredContractBundleV1) -> Self {
        Self {
            order_key: bundle_evidence_order_key(&bundle),
            bundle,
        }
    }
}

#[allow(dead_code)]
#[derive(Clone)]
pub(crate) enum HistoricalPlanReferenceSource {
    Admission(IdempotencyIdentityKey),
    Commit(CommitSequence),
    Provenance(ProvenanceId),
}

#[allow(dead_code)]
#[derive(Clone)]
pub(crate) struct HistoricalPlanReferenceRow {
    pub(crate) order_key: Vec<u8>,
    pub(crate) plan: ExecutablePlanRef,
    pub(crate) source: HistoricalPlanReferenceSource,
}

/// One physical memory index-table row during the coordinated ADR-0038 cutover.
///
/// `LegacyV1` exists only so already-merged command writers and startup tests
/// keep compiling while their owning packages move to V2. Both variants share
/// one ordered physical table; there is no reciprocal partition side table.
#[derive(Clone)]
#[allow(dead_code)]
pub(crate) enum MemoryIndexEntry {
    LegacyV1 {
        record: StoredIndexEntryV1,
        observed_envelope: Box<[u8]>,
    },
    CurrentV2 {
        record: StoredIndexEntryV2,
        observed_envelope: Box<[u8]>,
        charge: EncodedContentCharge,
    },
}

impl MemoryIndexEntry {
    #[cfg(test)]
    pub(crate) fn legacy(record: StoredIndexEntryV1) -> Self {
        let encoded = riffdb_storage_api::encode_index_entry_v1_fixture(&record)
            .expect("checked V1 fixture must have a canonical envelope");
        Self::LegacyV1 {
            record,
            observed_envelope: encoded.as_bytes().to_vec().into_boxed_slice(),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn current(
        record: StoredIndexEntryV2,
        charge: EncodedContentCharge,
    ) -> Result<Self, StorageError> {
        let observed_envelope = riffdb_storage_api::encode_index_entry_v2(&record)
            .map_err(durable_codec_error_as_storage)?
            .into_bytes()
            .into_boxed_slice();
        Ok(Self::CurrentV2 {
            record,
            observed_envelope,
            charge,
        })
    }

    pub(crate) fn current_from_encoded(
        record: StoredIndexEntryV2,
        observed_envelope: Vec<u8>,
        charge: EncodedContentCharge,
    ) -> Self {
        Self::CurrentV2 {
            record,
            observed_envelope: observed_envelope.into_boxed_slice(),
            charge,
        }
    }

    pub(crate) fn key(&self) -> &riffdb_types::IndexEntryKey {
        match self {
            Self::LegacyV1 { record, .. } => record.key(),
            Self::CurrentV2 { record, .. } => record.key(),
        }
    }

    pub(crate) fn covered_values(&self) -> &riffdb_types::CanonicalRecord {
        match self {
            Self::LegacyV1 { record, .. } => record.covered_values(),
            Self::CurrentV2 { record, .. } => record.covered_values(),
        }
    }

    pub(crate) fn current_record(&self) -> Option<&StoredIndexEntryV2> {
        match self {
            Self::LegacyV1 { .. } => None,
            Self::CurrentV2 { record, .. } => Some(record),
        }
    }

    pub(crate) fn encoded_content_charge(&self) -> EncodedContentCharge {
        match self {
            Self::LegacyV1 {
                observed_envelope, ..
            } => EncodedContentCharge::new(observed_envelope.len())
                .expect("canonical V1 fixture envelope is nonempty and bounded"),
            Self::CurrentV2 { charge, .. } => *charge,
        }
    }

    pub(crate) fn observed_envelope(&self) -> &[u8] {
        match self {
            Self::LegacyV1 {
                observed_envelope, ..
            }
            | Self::CurrentV2 {
                observed_envelope, ..
            } => observed_envelope,
        }
    }

    pub(crate) fn matches_migration_row(&self, row: &IndexMigrationSemanticRow) -> bool {
        match (self, row) {
            (Self::LegacyV1 { record, .. }, IndexMigrationSemanticRow::V1(decoded)) => {
                record == decoded
            }
            (Self::CurrentV2 { record, .. }, IndexMigrationSemanticRow::V2(decoded)) => {
                record == decoded
            }
            (Self::LegacyV1 { .. }, IndexMigrationSemanticRow::V2(_))
            | (Self::CurrentV2 { .. }, IndexMigrationSemanticRow::V1(_)) => false,
        }
    }
}

pub(crate) const fn durable_codec_error_as_storage(error: DurableCodecError) -> StorageError {
    let kind = match error.kind() {
        DurableCodecErrorKind::IncompatibleFormat => StorageErrorKind::IncompatibleFormat,
        DurableCodecErrorKind::CorruptData | DurableCodecErrorKind::UnexpectedRecordType => {
            StorageErrorKind::CorruptData
        }
        DurableCodecErrorKind::LimitExceeded => StorageErrorKind::LimitExceeded,
        DurableCodecErrorKind::InvariantViolation | DurableCodecErrorKind::ReservationExceeded => {
            StorageErrorKind::InvariantViolation
        }
    };
    StorageError::new(kind, None)
}

impl HistoricalPlanReferenceRow {
    #[allow(dead_code)]
    pub(crate) fn new(plan: ExecutablePlanRef, source: HistoricalPlanReferenceSource) -> Self {
        Self {
            order_key: plan_evidence_order_key(&plan),
            plan,
            source,
        }
    }
}

/// Complete typed state for the POC memory engine.
///
/// Fields are intentionally crate-private. Backend modules add behavior around
/// these records; this state itself is not a second semantic API. Ordered
/// `Vec`s are deliberate for this reference backend: sequence streams append,
/// keyed tables use binary search plus compact prepared position deltas, and
/// startup evidence walks one stable canonical order without materializing a
/// second index. Mutation shifts are acceptable here because each write set is
/// bounded and this backend is conformance infrastructure, not the production
/// engine.
/// Moving a keyed table to `BTreeMap` remains a private optimization if memory
/// mutation cost becomes material; no public or durable contract depends on it.
#[allow(dead_code)]
#[derive(Clone, Default)]
pub(crate) struct MemoryState {
    pub(crate) metadata: MemoryMetadataSlot,
    pub(crate) catalog_bundles: Vec<CatalogBundleRow>,
    pub(crate) catalog_activations: Vec<CatalogActivationIndexRow>,
    pub(crate) catalog_bundle_activations: Vec<CatalogBundleActivationIndexRow>,
    pub(crate) query_modules: Vec<StoredQueryModuleV1>,
    pub(crate) active_query_modules: Vec<StoredQueryModuleAdministrationV1>,
    pub(crate) reactive_modules: Vec<StoredReactiveModuleV1>,
    pub(crate) administration_audit: Vec<StoredAdministrationAuditRecordV1>,
    pub(crate) service_audit_invocations: Vec<ServiceAuditInvocationIndexRow>,
    pub(crate) admissions: Vec<StoredAdmissionStateV1>,
    pub(crate) entities: Vec<StoredEntityRecordV1>,
    pub(crate) entity_commits: Vec<EntityCommitIndexRow>,
    pub(crate) index_entries: Vec<MemoryIndexEntry>,
    pub(crate) index_epochs: Vec<StoredIndexEpochV1>,
    // Writers maintain both historical indexes atomically with their source
    // rows while holding MemoryAccess. Plan rows retain one immutable source;
    // persisted-key rows are removed/replaced with their current post-image.
    pub(crate) historical_plan_references: Vec<HistoricalPlanReferenceRow>,
    pub(crate) historical_persisted_keys: Vec<HistoricalPersistedKeyRow>,
    pub(crate) commits: Vec<StoredCommitRecordV1>,
    pub(crate) commit_admissions: Vec<CommitAdmissionIndexRow>,
    pub(crate) committed_admissions: Vec<CommittedAdmissionIndexRow>,
    pub(crate) provenance: Vec<StoredProvenanceRecordV1>,
    pub(crate) events: Vec<StoredDurableEventV1>,
    pub(crate) event_routes: Vec<EventRouteRow>,
    pub(crate) event_consumers: Vec<StoredEventConsumerV1>,
    pub(crate) event_consumer_deliveries: Vec<StoredEventConsumerDeliveryV1>,
    pub(crate) outbox_intents: Vec<StoredOutboxIntentV1>,
    pub(crate) outbox_statuses: Vec<StoredOutboxStatusV1>,
    /// Memory-only ordered accelerator for effective pending outbox rows.
    ///
    /// Writers maintain it atomically with command events and status changes.
    /// It is neither a durable semantic record nor startup evidence.
    pub(crate) pending_outbox_events: Vec<EventId>,
    /// Memory-only ordered accelerator for every non-delivered outbox row.
    ///
    /// This includes canonical and explicit pending, delivering, and dead-letter
    /// observations. Like the pending accelerator, it is not semantic state.
    pub(crate) undelivered_outbox_events: Vec<EventId>,
    pub(crate) capabilities: Vec<StoredCapabilityRecordV1>,
    pub(crate) capability_lookups: Vec<CapabilityLookupRow>,
    pub(crate) projection_controls: Vec<StoredProjectionControlV1>,
    pub(crate) projection_states: Vec<StoredProjectionStateV1>,
    pub(crate) projection_applies: Vec<StoredProjectionApplyV1>,
    pub(crate) synthetic_charges: MemorySyntheticCharges,
    #[cfg(test)]
    pub(crate) injected_structural_findings: Vec<riffdb_storage_api::StructuralFinding>,
}

pub(crate) fn bundle_evidence_order_key(bundle: &StoredContractBundleV1) -> Vec<u8> {
    bundle_identity_evidence_order_key(
        bundle.lineage(),
        bundle.contract_version(),
        bundle.bundle_hash(),
    )
}

pub(crate) fn bundle_identity_evidence_order_key(
    lineage: &ContractLineage,
    version: ContractVersion,
    hash: ContractBundleHash,
) -> Vec<u8> {
    let mut key = vec![0x01];
    push_lineage(&mut key, lineage);
    key.extend_from_slice(&version.to_be_bytes());
    key.extend_from_slice(hash.as_bytes());
    key
}

pub(crate) fn plan_evidence_order_key(plan: &ExecutablePlanRef) -> Vec<u8> {
    let mut key = vec![0x02];
    push_lineage(&mut key, plan.contract_lineage());
    key.extend_from_slice(&plan.contract_version().to_be_bytes());
    key.extend_from_slice(plan.contract_bundle_hash().as_bytes());
    key.extend_from_slice(&plan.command_id().to_be_bytes());
    key.extend_from_slice(plan.command_plan_hash().as_bytes());
    key
}

pub(crate) fn compare_projection_identity_storage_order(
    left: &ProjectionIdentity,
    right: &ProjectionIdentity,
) -> std::cmp::Ordering {
    left.to_canonical_bytes().cmp(&right.to_canonical_bytes())
}

fn push_lineage(output: &mut Vec<u8>, lineage: &ContractLineage) {
    let length =
        u32::try_from(lineage.as_bytes().len()).expect("contract-lineage hard bound fits in u32");
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(lineage.as_bytes());
}

impl MemoryState {
    /// Preflights the complete administration range without mutating metadata.
    #[allow(dead_code)]
    pub(crate) fn prepare_administration_allocation(
        &self,
        count: u16,
    ) -> Result<PreparedAdministrationAllocation, StorageError> {
        let MemoryMetadataSlot::Retained(metadata) = &self.metadata else {
            return Err(storage_error(StorageErrorKind::CorruptData));
        };
        let assigned = metadata
            .administration_sequence()
            .allocate_consecutive(count)
            .map_err(sequence_allocation_error)?;
        let metadata_post_image = RetainedMetadataV1::new(
            metadata.storage_format_version(),
            metadata.database_id(),
            metadata.application_sequence(),
            assigned.next(),
            metadata.history_incarnation(),
            metadata.active_catalog().cloned(),
            metadata.capability_bootstrap(),
        )
        .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
        Ok(PreparedAdministrationAllocation {
            assigned,
            metadata_post_image,
        })
    }

    /// Prepares an exact projection-control CAS without deriving lifecycle policy.
    ///
    /// The caller must construct `updated` through the accepted projection API.
    /// This helper only enforces exact prior equality, canonical identity order,
    /// and an explicit per-record synthetic envelope charge.
    #[allow(dead_code)]
    pub(crate) fn prepare_projection_control_cas(
        &self,
        identity: &ProjectionIdentity,
        expected: Option<&StoredProjectionControlV1>,
        updated: StoredProjectionControlV1,
        updated_charge: EncodedContentCharge,
    ) -> Result<ProjectionControlCasPreparation, StorageError> {
        if updated.identity() != identity
            || expected.is_some_and(|prior| prior.identity() != identity)
        {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let position = unique_binary_search_by(&self.projection_controls, |row| {
            compare_projection_identity_storage_order(row.identity(), identity)
        })?;
        let charge_position =
            unique_binary_search_by(&self.synthetic_charges.projection_controls, |row| {
                compare_projection_identity_storage_order(&row.key, identity)
            })?;
        if position.is_ok() != charge_position.is_ok() {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        let current = position.ok().map(|index| &self.projection_controls[index]);
        if current != expected {
            return Ok(match (expected, current) {
                (None, Some(existing)) => {
                    ProjectionControlCasPreparation::Existing(existing.clone())
                }
                (Some(_), None | Some(_)) => ProjectionControlCasPreparation::StateChanged,
                (None, None) => unreachable!("equal absent observations handled below"),
            });
        }

        let charge = KeyedSyntheticCharge {
            key: identity.clone(),
            charge: SyntheticRecordCharge::new(updated_charge),
        };
        let (position, charge_position) = match (position, charge_position) {
            (Ok(index), Ok(charge_index)) => (
                PreparedRowPosition::Replace(index),
                PreparedRowPosition::Replace(charge_index),
            ),
            (Err(index), Err(charge_index)) => (
                PreparedRowPosition::Insert(index),
                PreparedRowPosition::Insert(charge_index),
            ),
            (Ok(_) | Err(_), Ok(_) | Err(_)) => {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
        };
        Ok(ProjectionControlCasPreparation::Apply(
            PreparedProjectionControlCas {
                position,
                charge_position,
                updated,
                charge,
            },
        ))
    }

    /// Prepares one outbox status CAS after proving reciprocal authoritative rows.
    ///
    /// Missing both authoritative rows is the API's normal missing-intent result;
    /// a partial, duplicate, or mismatched authoritative graph is corruption.
    #[allow(dead_code)]
    pub(crate) fn prepare_outbox_status_cas(
        &self,
        event_id: EventId,
        expected: &OutboxStatusObservationV1,
        updated: StoredOutboxStatusV1,
        updated_charge: EncodedContentCharge,
    ) -> Result<OutboxStatusCasPreparation, StorageError> {
        if updated.event_id() != event_id {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }

        let event_position =
            unique_binary_search_by(&self.events, |event| event.event_id().cmp(&event_id))?;
        let intent_position = unique_binary_search_by(&self.outbox_intents, |intent| {
            intent.event_id().cmp(&event_id)
        })?;
        let status_position = unique_binary_search_by(&self.outbox_statuses, |status| {
            status.event_id().cmp(&event_id)
        })?;
        let charge_position =
            unique_binary_search_by(&self.synthetic_charges.outbox_statuses, |row| {
                row.key.cmp(&event_id)
            })?;
        if status_position.is_ok() != charge_position.is_ok() {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }

        match (event_position, intent_position) {
            (Err(_), Err(_)) => {
                if status_position.is_ok() {
                    return Err(storage_error(StorageErrorKind::CorruptData));
                }
                return Ok(OutboxStatusCasPreparation::NoChange(
                    OutboxTransitionResultV1::AuthoritativeIntentMissing,
                ));
            }
            (Ok(event_index), Ok(intent_index))
                if self.outbox_intents[intent_index].event() == &self.events[event_index] => {}
            (Ok(_) | Err(_), Ok(_) | Err(_)) => {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
        }

        let current = match status_position {
            Ok(index) => OutboxStatusObservationV1::Present(self.outbox_statuses[index].clone()),
            Err(_) => OutboxStatusObservationV1::AbsentInitialPending,
        };
        if &current != expected {
            return Ok(OutboxStatusCasPreparation::NoChange(
                OutboxTransitionResultV1::StateChanged(current),
            ));
        }

        let charge = KeyedSyntheticCharge {
            key: event_id,
            charge: SyntheticRecordCharge::new(updated_charge),
        };
        let (position, charge_position) = match (status_position, charge_position) {
            (Ok(index), Ok(charge_index)) => (
                PreparedRowPosition::Replace(index),
                PreparedRowPosition::Replace(charge_index),
            ),
            (Err(index), Err(charge_index)) => (
                PreparedRowPosition::Insert(index),
                PreparedRowPosition::Insert(charge_index),
            ),
            (Ok(_) | Err(_), Ok(_) | Err(_)) => {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
        };
        Ok(OutboxStatusCasPreparation::Apply(PreparedOutboxStatusCas {
            position,
            charge_position,
            updated,
            charge,
        }))
    }

    pub(crate) fn is_truly_empty(&self) -> bool {
        matches!(self.metadata, MemoryMetadataSlot::Absent)
            && self.catalog_bundles.is_empty()
            && self.catalog_activations.is_empty()
            && self.catalog_bundle_activations.is_empty()
            && self.administration_audit.is_empty()
            && self.service_audit_invocations.is_empty()
            && self.admissions.is_empty()
            && self.entities.is_empty()
            && self.entity_commits.is_empty()
            && self.index_entries.is_empty()
            && self.index_epochs.is_empty()
            && self.historical_plan_references.is_empty()
            && self.historical_persisted_keys.is_empty()
            && self.commits.is_empty()
            && self.commit_admissions.is_empty()
            && self.committed_admissions.is_empty()
            && self.provenance.is_empty()
            && self.events.is_empty()
            && self.outbox_intents.is_empty()
            && self.outbox_statuses.is_empty()
            && self.pending_outbox_events.is_empty()
            && self.undelivered_outbox_events.is_empty()
            && self.capabilities.is_empty()
            && self.capability_lookups.is_empty()
            && self.projection_controls.is_empty()
            && self.projection_states.is_empty()
            && self.projection_applies.is_empty()
            && self.synthetic_charges.is_empty()
            && self.test_findings_are_empty()
    }

    #[cfg(test)]
    fn test_findings_are_empty(&self) -> bool {
        self.injected_structural_findings.is_empty()
    }

    #[cfg(not(test))]
    const fn test_findings_are_empty(&self) -> bool {
        true
    }
}

#[allow(dead_code)]
pub(crate) fn unique_binary_search_by<T>(
    rows: &[T],
    mut compare: impl FnMut(&T) -> std::cmp::Ordering,
) -> Result<Result<usize, usize>, StorageError> {
    let position = rows.binary_search_by(&mut compare);
    if let Ok(index) = position
        && ((index > 0 && compare(&rows[index - 1]).is_eq())
            || (index + 1 < rows.len() && compare(&rows[index + 1]).is_eq()))
    {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    Ok(position)
}

#[allow(dead_code)]
fn sequence_allocation_error(error: SequenceAllocationError) -> StorageError {
    let kind = match error {
        SequenceAllocationError::Exhausted => StorageErrorKind::SequenceExhausted,
        SequenceAllocationError::ZeroCount | SequenceAllocationError::TooMany => {
            StorageErrorKind::InvariantViolation
        }
    };
    storage_error(kind)
}

#[cfg(test)]
mod tests {
    use riffdb_storage_api::CommandWriteClassBreakdownV1;
    use riffdb_types::DatabaseId;

    use super::*;

    fn database_id() -> DatabaseId {
        DatabaseId::from_bytes([
            0x01, 0x8f, 0x00, 0x00, 0x00, 0x00, 0x70, 0x01, 0x80, 0x02, 0, 0, 0, 0, 0, 1,
        ])
        .expect("UUIDv7 database ID")
    }

    #[test]
    fn synthetic_command_charges_preserve_supplied_classes_without_inference() {
        let classes = CommandWriteClassBreakdownV1::new(1, 2, 0, 0, 0, 3, 0, 0, 4, 5)
            .expect("bounded supplied charges");
        let charges = SyntheticCommandClassCharges::from_supplied_classes(classes)
            .expect("typed synthetic charges");

        assert_eq!(
            charges
                .allocator
                .expect("allocator charge")
                .encoded_content_charge()
                .get(),
            1
        );
        assert!(charges.entities.is_none());
        assert_eq!(
            charges
                .commit
                .expect("commit charge")
                .encoded_content_charge()
                .get(),
            5
        );
    }

    #[test]
    fn administration_preflight_allocates_compound_range_without_mutation() {
        let state = MemoryState {
            metadata: MemoryMetadataSlot::Retained(RetainedMetadataV1::initial(database_id())),
            ..MemoryState::default()
        };

        let prepared = state
            .prepare_administration_allocation(3)
            .expect("three-slot bootstrap preflight");
        assert_eq!(prepared.assigned().len(), 3);
        let MemoryMetadataSlot::Retained(before) = &state.metadata else {
            panic!("test initialized metadata");
        };
        assert_eq!(
            before.administration_sequence(),
            riffdb_storage_api::AdministrationSequenceAllocator::initial()
        );
        assert_eq!(
            prepared
                .into_metadata_post_image()
                .administration_sequence(),
            riffdb_storage_api::AdministrationSequenceAllocator::next(
                AdministrationSequence::new(4).expect("sequence four")
            )
        );
    }
}
