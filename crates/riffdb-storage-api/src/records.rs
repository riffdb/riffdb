//! Engine-neutral semantic records for one atomic application command.

use std::error::Error;
use std::fmt;
use std::sync::Arc;

use riffdb_types::{
    AdmittedActorContext, CanonicalInputHash, CanonicalRecord, CommitSequence, ConflictKeyHash,
    ContractVersion, EntityRecordHash, EntityVersion, EventHash, EventId, EventTypeId,
    IndexEntryKey, IndexEpoch, LogicalTime, MAX_CANONICAL_DOCUMENT_BYTES,
    MAX_COMMIT_INTENT_SEMANTIC_BYTES, OutcomeId, PartitionKey, PartitionKeyHash, ProvenanceId,
    RequestId, encode_canonical_record, hash_entity_record, hash_event, hash_partition_key,
};

use crate::{
    AffectedEpochCurrentState, AffectedIndexEpochTargets, ApplicationSequenceAllocator,
    AssignedCommandSequence, CommitIntent, DeclaredOutcome, DurableKeySchemaBindingV1,
    EntityMutation, EntityTarget, ExecutablePlanRef, ExpectedEntityState, IdempotencyIdentity,
    IndexEpochPosition, MAX_COMMIT_CONFLICT_HASHES, MAX_ENTITY_MUTATIONS, MAX_EVENT_INTENTS,
    MAX_INDEX_DELTAS, MAX_STAGED_WRITE_BYTES, MAX_VALIDATION_TARGETS, PartitionIndexTarget,
    StorageValueError, StoredAdmittedProvenanceClaimsV1, StoredReadDependenciesV1,
    StoredServiceAuditRecordV1, StructurallyDecodedIndexRangePrefixV1, actor_semantic_bytes,
    canonical_codec_storage_error, canonical_record_bytes, framed_bytes,
};

/// The durability contract used for one completed engine commit.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum DurabilityMode {
    /// Acknowledged only after durable synchronization.
    Sync,
    /// Multiple compatible commands share one durable flush.
    Group,
    /// No durability guarantee; valid only in tests and models.
    Memory,
}

/// One authoritative canonical entity row.
#[derive(Clone, Eq, PartialEq)]
pub struct StoredEntityRecordV1 {
    target: EntityTarget,
    entity_version: EntityVersion,
    written_by_contract: ContractVersion,
    schema_binding: DurableKeySchemaBindingV1,
    fields: Arc<CanonicalRecord>,
    fields_encoded: Arc<[u8]>,
}

impl StoredEntityRecordV1 {
    /// Constructs a complete bounded entity row.
    pub fn new(
        target: EntityTarget,
        entity_version: EntityVersion,
        written_by_contract: ContractVersion,
        schema_binding: DurableKeySchemaBindingV1,
        fields: CanonicalRecord,
    ) -> Result<Self, StorageValueError> {
        let fields_encoded = encode_canonical_record(&fields)
            .map_err(|error| canonical_codec_storage_error(&error))?;
        if fields_encoded.len() > MAX_CANONICAL_DOCUMENT_BYTES {
            return Err(StorageValueError::LimitExceeded);
        }
        if written_by_contract != schema_binding.contract_version() {
            return Err(StorageValueError::IdentityMismatch);
        }
        Ok(Self {
            target,
            entity_version,
            written_by_contract,
            schema_binding,
            fields: Arc::new(fields),
            fields_encoded: Arc::from(fields_encoded),
        })
    }

    /// Materializes a stored record from one already checked evaluation post-image.
    ///
    /// The post-image is the only source of canonical fields and encoded bytes;
    /// callers cannot supply detached bytes. External and decoded values use
    /// [`Self::new`] and retain its complete canonical encoding checks.
    pub fn from_checked_post_image(
        post_image: &crate::EntityPostImage,
        entity_version: EntityVersion,
        schema_binding: DurableKeySchemaBindingV1,
    ) -> Result<Self, StorageValueError> {
        if post_image.fields_encoded_len() > MAX_CANONICAL_DOCUMENT_BYTES {
            return Err(StorageValueError::LimitExceeded);
        }
        if post_image.written_by_contract() != schema_binding.contract_version() {
            return Err(StorageValueError::IdentityMismatch);
        }
        let (fields, fields_encoded) = post_image.shared_fields();
        Ok(Self {
            target: post_image.target().clone(),
            entity_version,
            written_by_contract: post_image.written_by_contract(),
            schema_binding,
            fields,
            fields_encoded,
        })
    }

    /// Borrows the complete canonical entity target.
    #[must_use]
    pub const fn target(&self) -> &EntityTarget {
        &self.target
    }

    /// Returns the nonzero authoritative entity version.
    #[must_use]
    pub const fn entity_version(&self) -> EntityVersion {
        self.entity_version
    }

    /// Returns the application contract version that wrote this image.
    #[must_use]
    pub const fn written_by_contract(&self) -> ContractVersion {
        self.written_by_contract
    }

    /// Borrows the exact retained bundle owning this persisted key post-image.
    #[must_use]
    pub const fn schema_binding(&self) -> &DurableKeySchemaBindingV1 {
        &self.schema_binding
    }

    /// Borrows all canonical fields, including compatible unknown fields.
    #[must_use]
    pub fn fields(&self) -> &CanonicalRecord {
        &self.fields
    }

    /// Returns the checked canonical encoding length retained at construction.
    #[must_use]
    pub fn fields_encoded_len(&self) -> usize {
        self.fields_encoded.len()
    }

    /// Borrows the canonical bytes sealed with the semantic record.
    #[must_use]
    pub fn fields_encoded(&self) -> &[u8] {
        &self.fields_encoded
    }

    pub(crate) fn semantic_bytes(&self) -> Result<usize, StorageValueError> {
        stored_entity_semantic_bytes(
            &self.target,
            &self.schema_binding,
            self.fields_encoded.len(),
        )
    }
}

/// One complete index entry post-image.
#[derive(Clone, Eq, PartialEq)]
pub struct StoredIndexEntryV1 {
    key: IndexEntryKey,
    schema_binding: DurableKeySchemaBindingV1,
    covered_values: CanonicalRecord,
}

impl StoredIndexEntryV1 {
    /// Constructs a bounded legacy index-entry migration source.
    ///
    /// Current writes must use [`StoredIndexEntryV2`]. V1 remains public until
    /// the ADR-0038 startup migration and durable decoder are integrated.
    pub fn new(
        key: IndexEntryKey,
        schema_binding: DurableKeySchemaBindingV1,
        covered_values: CanonicalRecord,
    ) -> Result<Self, StorageValueError> {
        if canonical_record_bytes(&covered_values)? > MAX_CANONICAL_DOCUMENT_BYTES {
            return Err(StorageValueError::LimitExceeded);
        }
        Ok(Self {
            key,
            schema_binding,
            covered_values,
        })
    }

    /// Borrows the complete canonical index key.
    #[must_use]
    pub const fn key(&self) -> &IndexEntryKey {
        &self.key
    }

    /// Borrows the exact retained bundle owning this persisted key post-image.
    #[must_use]
    pub const fn schema_binding(&self) -> &DurableKeySchemaBindingV1 {
        &self.schema_binding
    }

    /// Borrows the complete canonical covered-value record.
    #[must_use]
    pub const fn covered_values(&self) -> &CanonicalRecord {
        &self.covered_values
    }
}

/// One complete current index-entry post-image with exact partition identity.
#[derive(Clone, Eq, PartialEq)]
pub struct StoredIndexEntryV2 {
    key: IndexEntryKey,
    schema_binding: DurableKeySchemaBindingV1,
    covered_values: CanonicalRecord,
    covered_values_encoded: Arc<[u8]>,
    partition_key: PartitionKey,
}

impl StoredIndexEntryV2 {
    /// Constructs a bounded canonical current index-entry record.
    pub fn new(
        key: IndexEntryKey,
        schema_binding: DurableKeySchemaBindingV1,
        covered_values: CanonicalRecord,
        partition_key: PartitionKey,
    ) -> Result<Self, StorageValueError> {
        let covered_values_encoded = encode_canonical_record(&covered_values)
            .map_err(|error| canonical_codec_storage_error(&error))?;
        if covered_values_encoded.len() > MAX_CANONICAL_DOCUMENT_BYTES {
            return Err(StorageValueError::LimitExceeded);
        }
        let record = Self {
            key,
            schema_binding,
            covered_values,
            covered_values_encoded: Arc::from(covered_values_encoded),
            partition_key,
        };
        let _ = record.semantic_bytes()?;
        Ok(record)
    }

    /// Borrows the complete canonical index key.
    #[must_use]
    pub const fn key(&self) -> &IndexEntryKey {
        &self.key
    }

    /// Borrows the exact retained bundle owning this persisted key post-image.
    #[must_use]
    pub const fn schema_binding(&self) -> &DurableKeySchemaBindingV1 {
        &self.schema_binding
    }

    /// Borrows the complete canonical covered-value record.
    #[must_use]
    pub const fn covered_values(&self) -> &CanonicalRecord {
        &self.covered_values
    }

    /// Returns the checked canonical encoding length retained at construction.
    #[must_use]
    pub fn covered_values_encoded_len(&self) -> usize {
        self.covered_values_encoded.len()
    }

    /// Borrows the canonical bytes sealed with the semantic record.
    #[must_use]
    pub fn covered_values_encoded(&self) -> &[u8] {
        &self.covered_values_encoded
    }

    /// Borrows the exact canonical logical partition stored with this row.
    #[must_use]
    pub const fn partition_key(&self) -> &PartitionKey {
        &self.partition_key
    }

    pub(crate) fn semantic_bytes(&self) -> Result<usize, StorageValueError> {
        let covered_bytes = framed_bytes(self.covered_values_encoded.len())?;
        framed_bytes(self.key.as_bytes().len())?
            .checked_add(self.schema_binding.semantic_bytes()?)
            .and_then(|value| value.checked_add(covered_bytes))
            .and_then(|value| {
                value.checked_add(framed_bytes(self.partition_key.as_bytes().len()).ok()?)
            })
            .ok_or(StorageValueError::SizeOverflow)
    }
}

/// One authoritative secondary-index change.
#[derive(Clone, Eq, PartialEq)]
pub enum IndexEntryMutationV1 {
    /// Removes the exact complete index key and its entire current schema binding.
    Delete(IndexEntryKey),
    /// Installs or replaces the complete entry post-image.
    Put(StoredIndexEntryV2),
}

impl IndexEntryMutationV1 {
    /// Borrows the canonical key ordering this change.
    #[must_use]
    pub const fn key(&self) -> &IndexEntryKey {
        match self {
            Self::Delete(key) => key,
            Self::Put(record) => record.key(),
        }
    }

    fn semantic_bytes(&self) -> Result<usize, StorageValueError> {
        match self {
            Self::Delete(key) => framed_bytes(key.as_bytes().len())?
                .checked_add(1)
                .ok_or(StorageValueError::SizeOverflow),
            Self::Put(record) => record
                .semantic_bytes()?
                .checked_add(1)
                .ok_or(StorageValueError::SizeOverflow),
        }
    }
}

/// One persisted range-epoch post-image with durable schema ownership.
#[derive(Clone, Eq, PartialEq)]
pub struct StoredIndexEpochV1 {
    target: PartitionIndexTarget,
    schema_binding: DurableKeySchemaBindingV1,
    epoch: IndexEpoch,
}

/// Decoded historical prefix-epoch row retained only for bounded V1 migration.
#[derive(Clone, Eq, PartialEq)]
pub struct LegacyStoredIndexEpochV1 {
    target: StructurallyDecodedIndexRangePrefixV1,
    schema_binding: DurableKeySchemaBindingV1,
    epoch: IndexEpoch,
}

impl LegacyStoredIndexEpochV1 {
    /// Constructs one already structurally checked historical row.
    #[must_use]
    pub const fn new(
        target: StructurallyDecodedIndexRangePrefixV1,
        schema_binding: DurableKeySchemaBindingV1,
        epoch: IndexEpoch,
    ) -> Self {
        Self {
            target,
            schema_binding,
            epoch,
        }
    }

    /// Borrows the historical prefix identity.
    #[must_use]
    pub const fn target(&self) -> &StructurallyDecodedIndexRangePrefixV1 {
        &self.target
    }

    /// Borrows the exact historical schema binding.
    #[must_use]
    pub const fn schema_binding(&self) -> &DurableKeySchemaBindingV1 {
        &self.schema_binding
    }

    /// Returns the historical epoch value.
    #[must_use]
    pub const fn epoch(&self) -> IndexEpoch {
        self.epoch
    }
}

impl fmt::Debug for LegacyStoredIndexEpochV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("LegacyStoredIndexEpochV1([REDACTED])")
    }
}

impl StoredIndexEpochV1 {
    /// Constructs a structurally decoded persisted epoch post-image.
    #[must_use]
    pub const fn new(
        target: PartitionIndexTarget,
        schema_binding: DurableKeySchemaBindingV1,
        epoch: IndexEpoch,
    ) -> Self {
        Self {
            target,
            schema_binding,
            epoch,
        }
    }

    /// Borrows the exact persisted prefix bytes.
    #[must_use]
    pub const fn target(&self) -> &PartitionIndexTarget {
        &self.target
    }

    /// Borrows the exact retained bundle owning this prefix post-image.
    #[must_use]
    pub const fn schema_binding(&self) -> &DurableKeySchemaBindingV1 {
        &self.schema_binding
    }

    /// Returns the assigned nonzero epoch.
    #[must_use]
    pub const fn epoch(&self) -> IndexEpoch {
        self.epoch
    }

    fn semantic_bytes(&self) -> Result<usize, StorageValueError> {
        self.target
            .semantic_bytes()?
            .checked_add(self.schema_binding.semantic_bytes()?)
            .and_then(|value| value.checked_add(8))
            .ok_or(StorageValueError::SizeOverflow)
    }
}

/// One exact affected range bucket and its checked epoch advance.
#[derive(Clone, Eq, PartialEq)]
pub struct IndexEpochAdvanceV1 {
    prior: IndexEpochPosition,
    post_image: StoredIndexEpochV1,
}

impl IndexEpochAdvanceV1 {
    /// Advances `BeforeFirst` to one or a nonzero epoch without wrapping.
    pub fn new(
        target: PartitionIndexTarget,
        schema_binding: DurableKeySchemaBindingV1,
        prior: IndexEpochPosition,
    ) -> Result<Self, IndexEpochAdvanceError> {
        let next = match prior {
            IndexEpochPosition::BeforeFirst => IndexEpoch::first(),
            IndexEpochPosition::Value(value) => value
                .checked_next()
                .ok_or(IndexEpochAdvanceError::Exhausted)?,
        };
        Ok(Self {
            prior,
            post_image: StoredIndexEpochV1::new(target, schema_binding, next),
        })
    }

    /// Borrows the exact affected prefix bucket.
    #[must_use]
    pub const fn target(&self) -> &PartitionIndexTarget {
        self.post_image.target()
    }

    /// Returns the required prior epoch position.
    #[must_use]
    pub const fn prior(&self) -> IndexEpochPosition {
        self.prior
    }

    /// Returns the newly assigned nonzero epoch.
    #[must_use]
    pub const fn next(&self) -> IndexEpoch {
        self.post_image.epoch()
    }

    /// Borrows the exact persisted epoch post-image.
    #[must_use]
    pub const fn post_image(&self) -> &StoredIndexEpochV1 {
        &self.post_image
    }

    fn semantic_bytes(&self) -> Result<usize, StorageValueError> {
        let prior_bytes = match self.prior {
            IndexEpochPosition::BeforeFirst => 1,
            IndexEpochPosition::Value(_) => 1 + 8,
        };
        self.post_image
            .semantic_bytes()?
            .checked_add(prior_bytes)
            .ok_or(StorageValueError::SizeOverflow)
    }
}

/// A range epoch cannot advance without wrapping.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IndexEpochAdvanceError {
    /// The prior epoch is the maximum representable value.
    Exhausted,
}

impl fmt::Display for IndexEpochAdvanceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("index epoch is exhausted")
    }
}

impl Error for IndexEpochAdvanceError {}

/// One committed entity change, including its exact expected prior state.
///
/// Deletes retain the checked predecessor only in the in-flight atomic graph.
/// The predecessor is never published as current state: durable command
/// capsules carry the resulting [`crate::CommittedEntityTransitionV1`] and
/// commit records reference only live post-images.
#[derive(Clone, Eq, PartialEq)]
pub enum CommittedEntityMutationV1 {
    /// A create or replacement with one complete materialized post-image.
    Put {
        /// Exact observation required before applying the post-image.
        expected: ExpectedEntityState,
        /// Complete authoritative post-image.
        post_image: StoredEntityRecordV1,
    },
    /// A checked removal of one exact materialized predecessor.
    Delete {
        /// Exact live version required before deletion.
        expected_version: EntityVersion,
        /// Complete checked predecessor used for reciprocity and hashing.
        prior_image: StoredEntityRecordV1,
    },
}

impl CommittedEntityMutationV1 {
    /// Checks first-version and monotonic replacement semantics.
    pub fn new(
        expected: ExpectedEntityState,
        post_image: StoredEntityRecordV1,
    ) -> Result<Self, StorageValueError> {
        let required = match expected {
            ExpectedEntityState::Absent => EntityVersion::first(),
            ExpectedEntityState::Present(version) => version
                .checked_next()
                .ok_or(StorageValueError::InvalidShape)?,
        };
        if post_image.entity_version() != required {
            return Err(StorageValueError::IdentityMismatch);
        }
        Ok(Self::Put {
            expected,
            post_image,
        })
    }

    /// Constructs one checked deletion from its exact materialized predecessor.
    pub fn delete(
        expected_version: EntityVersion,
        prior_image: StoredEntityRecordV1,
    ) -> Result<Self, StorageValueError> {
        if prior_image.entity_version() != expected_version {
            return Err(StorageValueError::IdentityMismatch);
        }
        Ok(Self::Delete {
            expected_version,
            prior_image,
        })
    }

    /// Returns the exact required prior observation.
    #[must_use]
    pub const fn expected(&self) -> ExpectedEntityState {
        match self {
            Self::Put { expected, .. } => *expected,
            Self::Delete {
                expected_version, ..
            } => ExpectedEntityState::Present(*expected_version),
        }
    }

    /// Borrows the exact affected entity target.
    #[must_use]
    pub const fn target(&self) -> &EntityTarget {
        match self {
            Self::Put { post_image, .. } => post_image.target(),
            Self::Delete { prior_image, .. } => prior_image.target(),
        }
    }

    /// Borrows the complete checked mutation image.
    ///
    /// For a delete this is the checked predecessor, not a materialized
    /// post-delete row. Callers that publish state must branch on the variant
    /// or use [`Self::live_post_image`].
    #[must_use]
    pub const fn post_image(&self) -> &StoredEntityRecordV1 {
        self.checked_image()
    }

    /// Borrows the committed post-image only when current state remains live.
    #[must_use]
    pub const fn live_post_image(&self) -> Option<&StoredEntityRecordV1> {
        match self {
            Self::Put { post_image, .. } => Some(post_image),
            Self::Delete { .. } => None,
        }
    }

    /// Borrows the complete checked image carried by the atomic graph.
    ///
    /// This is the post-image for a put and the predecessor for a delete.
    #[must_use]
    pub const fn checked_image(&self) -> &StoredEntityRecordV1 {
        match self {
            Self::Put { post_image, .. } => post_image,
            Self::Delete { prior_image, .. } => prior_image,
        }
    }

    /// Returns whether this mutation removes current materialized state.
    #[must_use]
    pub const fn is_delete(&self) -> bool {
        matches!(self, Self::Delete { .. })
    }

    fn semantic_bytes(&self) -> Result<usize, StorageValueError> {
        committed_entity_semantic_bytes_from_len(
            self.expected(),
            self.target(),
            self.checked_image().schema_binding(),
            self.checked_image().fields_encoded_len(),
        )
    }
}

/// The immutable stored result used for equal-input replay.
#[derive(Clone, Eq, PartialEq)]
pub struct StoredOutcomeV1 {
    identity: IdempotencyIdentity,
    commit_sequence: CommitSequence,
    admission_request_id: RequestId,
    plan: ExecutablePlanRef,
    canonical_input_hash: CanonicalInputHash,
    actor: AdmittedActorContext,
    logical_time: LogicalTime,
    partition_key: PartitionKey,
    partition_hash: PartitionKeyHash,
    conflict_hashes: Vec<ConflictKeyHash>,
    declared_outcome: DeclaredOutcome,
    admitted_claims: StoredAdmittedProvenanceClaimsV1,
    provenance_id: ProvenanceId,
    durability_mode: DurabilityMode,
    causation: Option<crate::StoredCommandCausationV1>,
    service_values: CanonicalRecord,
}

impl StoredOutcomeV1 {
    /// Constructs a complete terminal outcome with immutable commit context.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        identity: IdempotencyIdentity,
        commit_sequence: CommitSequence,
        admission_request_id: RequestId,
        plan: ExecutablePlanRef,
        canonical_input_hash: CanonicalInputHash,
        actor: AdmittedActorContext,
        logical_time: LogicalTime,
        partition_key: PartitionKey,
        partition_hash: PartitionKeyHash,
        conflict_hashes: Vec<ConflictKeyHash>,
        declared_outcome: DeclaredOutcome,
        admitted_claims: StoredAdmittedProvenanceClaimsV1,
        provenance_id: ProvenanceId,
        durability_mode: DurabilityMode,
    ) -> Result<Self, StorageValueError> {
        Self::new_with_causation(
            identity,
            commit_sequence,
            admission_request_id,
            plan,
            canonical_input_hash,
            actor,
            logical_time,
            partition_key,
            partition_hash,
            conflict_hashes,
            declared_outcome,
            admitted_claims,
            provenance_id,
            durability_mode,
            None,
        )
    }

    /// Constructs a terminal outcome with optional server-validated causation.
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_causation(
        identity: IdempotencyIdentity,
        commit_sequence: CommitSequence,
        admission_request_id: RequestId,
        plan: ExecutablePlanRef,
        canonical_input_hash: CanonicalInputHash,
        actor: AdmittedActorContext,
        logical_time: LogicalTime,
        partition_key: PartitionKey,
        partition_hash: PartitionKeyHash,
        conflict_hashes: Vec<ConflictKeyHash>,
        declared_outcome: DeclaredOutcome,
        admitted_claims: StoredAdmittedProvenanceClaimsV1,
        provenance_id: ProvenanceId,
        durability_mode: DurabilityMode,
        causation: Option<crate::StoredCommandCausationV1>,
    ) -> Result<Self, StorageValueError> {
        if identity.contract_lineage() != plan.contract_lineage()
            || identity.command_id() != plan.command_id()
            || identity.principal_id() != actor.principal_id()
            || identity.tenant_scope() != actor.tenant_scope()
            || hash_partition_key(partition_key.as_bytes()) != partition_hash
            || causation
                .is_some_and(|value| value.causing_event_id().commit_sequence() >= commit_sequence)
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        validate_conflict_hashes(&conflict_hashes)?;
        Ok(Self {
            identity,
            commit_sequence,
            admission_request_id,
            plan,
            canonical_input_hash,
            actor,
            logical_time,
            partition_key,
            partition_hash,
            conflict_hashes,
            declared_outcome,
            admitted_claims,
            provenance_id,
            durability_mode,
            causation,
            service_values: CanonicalRecord::new(Vec::new())
                .map_err(|_| StorageValueError::InvalidShape)?,
        })
    }

    /// Borrows the complete durable idempotency identity.
    #[must_use]
    pub const fn identity(&self) -> &IdempotencyIdentity {
        &self.identity
    }

    /// Returns the original application commit sequence.
    #[must_use]
    pub const fn commit_sequence(&self) -> CommitSequence {
        self.commit_sequence
    }

    /// Returns the original admission request identity.
    #[must_use]
    pub const fn admission_request_id(&self) -> RequestId {
        self.admission_request_id
    }

    /// Borrows the exact historical plan reference.
    #[must_use]
    pub const fn plan(&self) -> &ExecutablePlanRef {
        &self.plan
    }

    /// Returns the original canonical input hash.
    #[must_use]
    pub const fn canonical_input_hash(&self) -> CanonicalInputHash {
        self.canonical_input_hash
    }

    /// Borrows the admitted actor context.
    #[must_use]
    pub const fn actor(&self) -> &AdmittedActorContext {
        &self.actor
    }

    /// Returns the logical time frozen at admission.
    #[must_use]
    pub const fn logical_time(&self) -> LogicalTime {
        self.logical_time
    }

    /// Borrows the exact canonical partition identity retained for internal authorization.
    ///
    /// Transport and public-error layers must not expose these bytes.
    #[must_use]
    pub const fn partition_key(&self) -> &PartitionKey {
        &self.partition_key
    }

    /// Returns the canonical partition identity hash.
    #[must_use]
    pub const fn partition_hash(&self) -> PartitionKeyHash {
        self.partition_hash
    }

    /// Borrows canonical conflict identities.
    #[must_use]
    pub fn conflict_hashes(&self) -> &[ConflictKeyHash] {
        &self.conflict_hashes
    }

    /// Borrows the original declared business outcome.
    #[must_use]
    pub const fn declared_outcome(&self) -> &DeclaredOutcome {
        &self.declared_outcome
    }

    /// Borrows the immutable provenance-claim snapshot frozen at admission.
    #[must_use]
    pub const fn admitted_claims(&self) -> &StoredAdmittedProvenanceClaimsV1 {
        &self.admitted_claims
    }

    /// Returns the immutable provenance record identity.
    #[must_use]
    pub const fn provenance_id(&self) -> ProvenanceId {
        self.provenance_id
    }

    /// Returns the durability contract used for the original commit.
    #[must_use]
    pub const fn durability_mode(&self) -> DurabilityMode {
        self.durability_mode
    }

    /// Returns server-validated causation for a contextual reaction.
    #[must_use]
    pub const fn causation(&self) -> Option<crate::StoredCommandCausationV1> {
        self.causation
    }

    /// Borrows the exact service values sealed at command admission.
    #[must_use]
    pub const fn service_values(&self) -> &CanonicalRecord {
        &self.service_values
    }

    /// Upgrades one decoded V1 base into its V2 causal successor.
    pub fn with_causation(
        mut self,
        causation: crate::StoredCommandCausationV1,
    ) -> Result<Self, StorageValueError> {
        if self.causation.is_some() {
            return Err(StorageValueError::InvalidShape);
        }
        self.causation = Some(causation);
        Ok(self)
    }

    /// Upgrades terminal replay evidence with sealed service values.
    pub fn with_service_values(
        mut self,
        service_values: CanonicalRecord,
    ) -> Result<Self, StorageValueError> {
        if !self.service_values.is_empty() {
            return Err(StorageValueError::InvalidShape);
        }
        self.service_values = service_values;
        self.semantic_bytes()?;
        Ok(self)
    }

    pub(crate) fn semantic_bytes(&self) -> Result<usize, StorageValueError> {
        let base = stored_outcome_semantic_bytes(
            &self.identity,
            &self.plan,
            &self.actor,
            &self.partition_key,
            &self.conflict_hashes,
            &self.declared_outcome,
            &self.admitted_claims,
        )?;
        if self.service_values.is_empty() {
            return Ok(base);
        }
        base.checked_add(framed_bytes(
            encode_canonical_record(&self.service_values)
                .map_err(|error| canonical_codec_storage_error(&error))?
                .len(),
        )?)
        .ok_or(StorageValueError::SizeOverflow)
    }
}

/// One authoritative durable event with its stable committed identity.
#[derive(Clone, Eq, PartialEq)]
pub struct StoredDurableEventV1 {
    event_id: EventId,
    event_type_id: EventTypeId,
    payload: Arc<CanonicalRecord>,
    payload_encoded: Arc<[u8]>,
    event_hash: EventHash,
}

/// Exact payload-free link to one authoritative durable event row.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EventReferenceV2 {
    event_id: EventId,
    event_hash: EventHash,
}

impl EventReferenceV2 {
    /// Constructs the exact event identity and integrity link.
    #[must_use]
    pub const fn new(event_id: EventId, event_hash: EventHash) -> Self {
        Self {
            event_id,
            event_hash,
        }
    }

    /// Derives a reference from its authoritative event.
    #[must_use]
    pub const fn from_event(event: &StoredDurableEventV1) -> Self {
        Self::new(event.event_id(), event.event_hash())
    }

    /// Returns the referenced event identity.
    #[must_use]
    pub const fn event_id(self) -> EventId {
        self.event_id
    }

    /// Returns the exact authoritative event hash.
    #[must_use]
    pub const fn event_hash(self) -> EventHash {
        self.event_hash
    }

    /// Proves an authoritative event is the exact referenced value.
    #[must_use]
    pub fn matches(self, event: &StoredDurableEventV1) -> bool {
        self.event_id == event.event_id() && self.event_hash == event.event_hash()
    }
}

/// Exact post-image-free link to one authoritative entity mutation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommittedEntityReferenceV2 {
    target: EntityTarget,
    entity_version: EntityVersion,
    post_image_hash: EntityRecordHash,
}

impl CommittedEntityReferenceV2 {
    /// Constructs the exact entity target, version, and integrity link.
    #[must_use]
    pub const fn new(
        target: EntityTarget,
        entity_version: EntityVersion,
        post_image_hash: EntityRecordHash,
    ) -> Self {
        Self {
            target,
            entity_version,
            post_image_hash,
        }
    }

    /// Derives a reference from its authoritative post-image.
    pub fn from_post_image(post_image: &StoredEntityRecordV1) -> Result<Self, StorageValueError> {
        Ok(Self::new(
            post_image.target().clone(),
            post_image.entity_version(),
            derive_entity_record_hash_v1(post_image)?,
        ))
    }

    /// Derives a reference from one staged committed mutation.
    pub fn from_mutation(mutation: &CommittedEntityMutationV1) -> Result<Self, StorageValueError> {
        let post_image = mutation
            .live_post_image()
            .ok_or(StorageValueError::InvalidShape)?;
        Self::from_post_image(post_image)
    }

    /// Derives a reference when a mutation leaves a live post-image.
    pub fn from_live_mutation(
        mutation: &CommittedEntityMutationV1,
    ) -> Result<Option<Self>, StorageValueError> {
        mutation
            .live_post_image()
            .map(Self::from_post_image)
            .transpose()
    }

    /// Returns the exact prior state implied by this committed version.
    ///
    /// Preserves the inverse of [`CommittedEntityMutationV1::new`]: first version
    /// requires absence; every later version requires the prior version present.
    #[must_use]
    pub const fn expected_from_version(version: EntityVersion) -> ExpectedEntityState {
        if version.get() == EntityVersion::first().get() {
            ExpectedEntityState::Absent
        } else {
            match EntityVersion::new(version.get() - 1) {
                Some(prior) => ExpectedEntityState::Present(prior),
                // Unreachable: version is nonzero and greater than first.
                None => ExpectedEntityState::Absent,
            }
        }
    }

    /// Borrows the referenced entity target.
    #[must_use]
    pub const fn target(&self) -> &EntityTarget {
        &self.target
    }

    /// Returns the committed entity version.
    #[must_use]
    pub const fn entity_version(&self) -> EntityVersion {
        self.entity_version
    }

    /// Returns the exact authoritative post-image hash.
    #[must_use]
    pub const fn post_image_hash(&self) -> EntityRecordHash {
        self.post_image_hash
    }

    /// Proves an authoritative entity row is the exact referenced post-image.
    #[must_use]
    pub fn matches(&self, post_image: &StoredEntityRecordV1) -> bool {
        self.target == *post_image.target()
            && self.entity_version == post_image.entity_version()
            && derive_entity_record_hash_v1(post_image)
                .is_ok_and(|hash| hash == self.post_image_hash)
    }
}

/// Derives the accepted v1 hash of one complete entity post-image.
///
/// The payload supplied to the `riffdb.entity-record/v1` hash domain is exactly:
/// `u32 entity_type_id ‖ u32 key_len ‖ key ‖ u64 entity_version ‖
/// u64 written_by_contract ‖ u32 lineage_len ‖ lineage ‖
/// u64 binding_contract_version ‖ 32B bundle_hash ‖ u32 fields_len ‖
/// canonical_fields`.
pub fn derive_entity_record_hash_v1(
    post_image: &StoredEntityRecordV1,
) -> Result<EntityRecordHash, StorageValueError> {
    Ok(hash_entity_record(&canonical_entity_record_preimage_v1(
        post_image,
    )?))
}

fn canonical_entity_record_preimage_v1(
    post_image: &StoredEntityRecordV1,
) -> Result<Vec<u8>, StorageValueError> {
    let key_bytes = post_image.target().key().as_bytes();
    let key_len = u32::try_from(key_bytes.len()).map_err(|_| StorageValueError::LimitExceeded)?;
    let lineage_bytes = post_image.schema_binding().lineage().as_bytes();
    let lineage_len =
        u32::try_from(lineage_bytes.len()).map_err(|_| StorageValueError::LimitExceeded)?;
    let fields_bytes = post_image.fields_encoded();
    let fields_len =
        u32::try_from(fields_bytes.len()).map_err(|_| StorageValueError::LimitExceeded)?;
    let preimage_capacity = 4usize
        .checked_add(4)
        .and_then(|value| value.checked_add(key_bytes.len()))
        .and_then(|value| value.checked_add(8))
        .and_then(|value| value.checked_add(8))
        .and_then(|value| value.checked_add(4))
        .and_then(|value| value.checked_add(lineage_bytes.len()))
        .and_then(|value| value.checked_add(8))
        .and_then(|value| value.checked_add(32))
        .and_then(|value| value.checked_add(4))
        .and_then(|value| value.checked_add(fields_bytes.len()))
        .ok_or(StorageValueError::SizeOverflow)?;
    let mut preimage = Vec::with_capacity(preimage_capacity);
    preimage.extend_from_slice(&post_image.target().entity_type_id().to_be_bytes());
    preimage.extend_from_slice(&key_len.to_be_bytes());
    preimage.extend_from_slice(key_bytes);
    preimage.extend_from_slice(&post_image.entity_version().get().to_be_bytes());
    preimage.extend_from_slice(&post_image.written_by_contract().get().to_be_bytes());
    preimage.extend_from_slice(&lineage_len.to_be_bytes());
    preimage.extend_from_slice(lineage_bytes);
    preimage.extend_from_slice(
        &post_image
            .schema_binding()
            .contract_version()
            .get()
            .to_be_bytes(),
    );
    preimage.extend_from_slice(post_image.schema_binding().bundle_hash().as_bytes());
    preimage.extend_from_slice(&fields_len.to_be_bytes());
    preimage.extend_from_slice(fields_bytes);
    Ok(preimage)
}

impl StoredDurableEventV1 {
    /// Constructs a bounded durable event from coordinator-checked values.
    pub fn new(
        event_id: EventId,
        event_type_id: EventTypeId,
        payload: CanonicalRecord,
        event_hash: EventHash,
    ) -> Result<Self, StorageValueError> {
        let payload_encoded = encode_canonical_record(&payload)
            .map_err(|error| canonical_codec_storage_error(&error))?;
        if derive_event_hash_v1(event_id, event_type_id, &payload)? != event_hash {
            return Err(StorageValueError::IdentityMismatch);
        }
        Ok(Self {
            event_id,
            event_type_id,
            payload: Arc::new(payload),
            payload_encoded: Arc::from(payload_encoded),
            event_hash,
        })
    }

    /// Returns the stable commit-sequence and ordinal identity.
    #[must_use]
    pub const fn event_id(&self) -> EventId {
        self.event_id
    }

    /// Returns the stable event type identity.
    #[must_use]
    pub const fn event_type_id(&self) -> EventTypeId {
        self.event_type_id
    }

    /// Borrows the complete canonical event payload.
    #[must_use]
    pub fn payload(&self) -> &CanonicalRecord {
        &self.payload
    }

    /// Returns the checked canonical encoding length retained at construction.
    #[must_use]
    pub fn payload_encoded_len(&self) -> usize {
        self.payload_encoded.len()
    }

    /// Borrows the canonical bytes sealed with the semantic record.
    #[must_use]
    pub fn payload_encoded(&self) -> &[u8] {
        &self.payload_encoded
    }

    /// Returns the domain-separated canonical event hash.
    #[must_use]
    pub const fn event_hash(&self) -> EventHash {
        self.event_hash
    }

    fn semantic_bytes(&self) -> Result<usize, StorageValueError> {
        stored_event_semantic_bytes(self.event_type_id, self.payload_encoded.len())
    }
}

/// Derives the accepted v1 hash of one complete durable event.
///
/// The payload supplied to the `riffdb.event/v1` hash domain is exactly the
/// 12-byte canonical event ID, the four-byte event type ID, the four-byte
/// canonical-record length, and the complete canonical record bytes.
pub fn derive_event_hash_v1(
    event_id: EventId,
    event_type_id: EventTypeId,
    payload: &CanonicalRecord,
) -> Result<EventHash, StorageValueError> {
    Ok(hash_event(&canonical_event_preimage_v1(
        event_id,
        event_type_id,
        payload,
    )?))
}

fn canonical_event_preimage_v1(
    event_id: EventId,
    event_type_id: EventTypeId,
    payload: &CanonicalRecord,
) -> Result<Vec<u8>, StorageValueError> {
    let payload_bytes =
        encode_canonical_record(payload).map_err(|error| canonical_codec_storage_error(&error))?;
    let payload_length =
        u32::try_from(payload_bytes.len()).map_err(|_| StorageValueError::LimitExceeded)?;
    let preimage_capacity = 12usize
        .checked_add(4)
        .and_then(|value| value.checked_add(4))
        .and_then(|value| value.checked_add(payload_bytes.len()))
        .ok_or(StorageValueError::SizeOverflow)?;
    let mut preimage = Vec::with_capacity(preimage_capacity);
    preimage.extend_from_slice(&event_id.to_be_bytes());
    preimage.extend_from_slice(&event_type_id.to_be_bytes());
    preimage.extend_from_slice(&payload_length.to_be_bytes());
    preimage.extend_from_slice(&payload_bytes);
    Ok(preimage)
}

/// One authoritative outbox intent written atomically with its event.
#[derive(Clone, Eq, PartialEq)]
pub struct StoredOutboxIntentV1 {
    event: StoredDurableEventV1,
}

impl StoredOutboxIntentV1 {
    /// Freezes the exact event identity and payload for later at-least-once dispatch.
    #[must_use]
    pub const fn new(event: StoredDurableEventV1) -> Self {
        Self { event }
    }

    /// Returns the stable downstream deduplication identity.
    #[must_use]
    pub const fn event_id(&self) -> EventId {
        self.event.event_id()
    }

    /// Borrows the exact reciprocal durable event value.
    #[must_use]
    pub const fn event(&self) -> &StoredDurableEventV1 {
        &self.event
    }

    /// Returns the reciprocal canonical event hash.
    #[must_use]
    pub const fn event_hash(&self) -> EventHash {
        self.event.event_hash()
    }

    /// Returns the payload-free durable reference written by current storage.
    #[must_use]
    pub const fn event_reference(&self) -> EventReferenceV2 {
        EventReferenceV2::from_event(&self.event)
    }

    fn semantic_bytes(&self) -> Result<usize, StorageValueError> {
        self.event.semantic_bytes()
    }
}

/// One immutable affected entity identity and committed version.
#[derive(Clone, Eq, PartialEq)]
pub struct AffectedEntityV1 {
    target: EntityTarget,
    entity_version: EntityVersion,
}

impl AffectedEntityV1 {
    /// Reconstructs an affected-entity link from its exact durable parts.
    #[must_use]
    pub const fn from_stored_parts(target: EntityTarget, entity_version: EntityVersion) -> Self {
        Self {
            target,
            entity_version,
        }
    }

    /// Constructs an affected-entity link from a complete post-image.
    #[must_use]
    pub fn from_record(record: &StoredEntityRecordV1) -> Self {
        Self::from_stored_parts(record.target().clone(), record.entity_version())
    }

    /// Constructs an affected-entity link from one complete checked mutation.
    ///
    /// A delete records the exact predecessor version removed by the command.
    #[must_use]
    pub fn from_mutation(mutation: &CommittedEntityMutationV1) -> Self {
        Self::from_record(mutation.checked_image())
    }

    /// Borrows the affected entity target.
    #[must_use]
    pub const fn target(&self) -> &EntityTarget {
        &self.target
    }

    /// Returns the committed entity version.
    #[must_use]
    pub const fn entity_version(&self) -> EntityVersion {
        self.entity_version
    }
}

/// Immutable policy-approved command provenance.
#[derive(Clone, Eq, PartialEq)]
pub struct StoredProvenanceRecordV1 {
    provenance_id: ProvenanceId,
    commit_sequence: CommitSequence,
    identity: IdempotencyIdentity,
    admission_request_id: RequestId,
    plan: ExecutablePlanRef,
    canonical_input_hash: CanonicalInputHash,
    actor: AdmittedActorContext,
    logical_time: LogicalTime,
    partition_hash: PartitionKeyHash,
    conflict_hashes: Vec<ConflictKeyHash>,
    outcome_id: OutcomeId,
    affected_entities: Vec<AffectedEntityV1>,
    event_ids: Vec<EventId>,
    admitted_claims: StoredAdmittedProvenanceClaimsV1,
    causation: Option<crate::StoredCommandCausationV1>,
}

impl StoredProvenanceRecordV1 {
    /// Constructs a complete immutable provenance record.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        provenance_id: ProvenanceId,
        commit_sequence: CommitSequence,
        identity: IdempotencyIdentity,
        admission_request_id: RequestId,
        plan: ExecutablePlanRef,
        canonical_input_hash: CanonicalInputHash,
        actor: AdmittedActorContext,
        logical_time: LogicalTime,
        partition_hash: PartitionKeyHash,
        conflict_hashes: Vec<ConflictKeyHash>,
        outcome_id: OutcomeId,
        affected_entities: Vec<AffectedEntityV1>,
        event_ids: Vec<EventId>,
        admitted_claims: StoredAdmittedProvenanceClaimsV1,
    ) -> Result<Self, StorageValueError> {
        Self::new_with_causation(
            provenance_id,
            commit_sequence,
            identity,
            admission_request_id,
            plan,
            canonical_input_hash,
            actor,
            logical_time,
            partition_hash,
            conflict_hashes,
            outcome_id,
            affected_entities,
            event_ids,
            admitted_claims,
            None,
        )
    }

    /// Constructs the V2 causal successor while preserving the V1 base shape.
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_causation(
        provenance_id: ProvenanceId,
        commit_sequence: CommitSequence,
        identity: IdempotencyIdentity,
        admission_request_id: RequestId,
        plan: ExecutablePlanRef,
        canonical_input_hash: CanonicalInputHash,
        actor: AdmittedActorContext,
        logical_time: LogicalTime,
        partition_hash: PartitionKeyHash,
        conflict_hashes: Vec<ConflictKeyHash>,
        outcome_id: OutcomeId,
        affected_entities: Vec<AffectedEntityV1>,
        event_ids: Vec<EventId>,
        admitted_claims: StoredAdmittedProvenanceClaimsV1,
        causation: Option<crate::StoredCommandCausationV1>,
    ) -> Result<Self, StorageValueError> {
        if affected_entities.len() > MAX_ENTITY_MUTATIONS || event_ids.len() > MAX_EVENT_INTENTS {
            return Err(StorageValueError::LimitExceeded);
        }
        if identity.contract_lineage() != plan.contract_lineage()
            || identity.command_id() != plan.command_id()
            || identity.principal_id() != actor.principal_id()
            || identity.tenant_scope() != actor.tenant_scope()
            || causation
                .is_some_and(|value| value.causing_event_id().commit_sequence() >= commit_sequence)
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        validate_conflict_hashes(&conflict_hashes)?;
        if affected_entities.windows(2).any(|pair| {
            pair[0].target().canonical_target_key() >= pair[1].target().canonical_target_key()
        }) || event_ids.windows(2).any(|pair| pair[0] >= pair[1])
            || event_ids
                .iter()
                .any(|event_id| event_id.commit_sequence() != commit_sequence)
        {
            return Err(StorageValueError::NonCanonicalOrder);
        }
        Ok(Self {
            provenance_id,
            commit_sequence,
            identity,
            admission_request_id,
            plan,
            canonical_input_hash,
            actor,
            logical_time,
            partition_hash,
            conflict_hashes,
            outcome_id,
            affected_entities,
            event_ids,
            admitted_claims,
            causation,
        })
    }

    /// Returns this immutable provenance identity.
    #[must_use]
    pub const fn provenance_id(&self) -> ProvenanceId {
        self.provenance_id
    }

    /// Returns the linked application sequence.
    #[must_use]
    pub const fn commit_sequence(&self) -> CommitSequence {
        self.commit_sequence
    }

    /// Borrows the complete idempotency identity.
    #[must_use]
    pub const fn identity(&self) -> &IdempotencyIdentity {
        &self.identity
    }

    /// Returns the original admission request identity.
    #[must_use]
    pub const fn admission_request_id(&self) -> RequestId {
        self.admission_request_id
    }

    /// Borrows the exact historical plan reference.
    #[must_use]
    pub const fn plan(&self) -> &ExecutablePlanRef {
        &self.plan
    }

    /// Returns the canonical input hash.
    #[must_use]
    pub const fn canonical_input_hash(&self) -> CanonicalInputHash {
        self.canonical_input_hash
    }

    /// Borrows the admitted actor context.
    #[must_use]
    pub const fn actor(&self) -> &AdmittedActorContext {
        &self.actor
    }

    /// Returns the deterministic logical time.
    #[must_use]
    pub const fn logical_time(&self) -> LogicalTime {
        self.logical_time
    }

    /// Returns the partition identity hash.
    #[must_use]
    pub const fn partition_hash(&self) -> PartitionKeyHash {
        self.partition_hash
    }

    /// Borrows canonical conflict identity hashes.
    #[must_use]
    pub fn conflict_hashes(&self) -> &[ConflictKeyHash] {
        &self.conflict_hashes
    }

    /// Returns the declared business outcome identity.
    #[must_use]
    pub const fn outcome_id(&self) -> OutcomeId {
        self.outcome_id
    }

    /// Borrows canonical affected entity/version links.
    #[must_use]
    pub fn affected_entities(&self) -> &[AffectedEntityV1] {
        &self.affected_entities
    }

    /// Borrows event links in ordinal order.
    #[must_use]
    pub fn event_ids(&self) -> &[EventId] {
        &self.event_ids
    }

    /// Borrows the immutable admitted provenance-claim snapshot.
    #[must_use]
    pub const fn admitted_claims(&self) -> &StoredAdmittedProvenanceClaimsV1 {
        &self.admitted_claims
    }

    /// Returns causal context only for a V2 contextual-reaction record.
    #[must_use]
    pub const fn causation(&self) -> Option<crate::StoredCommandCausationV1> {
        self.causation
    }

    /// Upgrades one decoded V1 base into its V2 causal successor.
    pub fn with_causation(
        mut self,
        causation: crate::StoredCommandCausationV1,
    ) -> Result<Self, StorageValueError> {
        if self.causation.is_some()
            || causation.causing_event_id().commit_sequence() >= self.commit_sequence
        {
            return Err(StorageValueError::InvalidShape);
        }
        self.causation = Some(causation);
        Ok(self)
    }

    pub(crate) fn semantic_bytes(&self) -> Result<usize, StorageValueError> {
        stored_provenance_semantic_bytes(
            &self.identity,
            &self.plan,
            &self.actor,
            &self.conflict_hashes,
            self.affected_entities.iter().map(AffectedEntityV1::target),
            self.event_ids.len(),
            &self.admitted_claims,
        )?
        .checked_add(if self.causation.is_some() {
            crate::command::STORED_COMMAND_CAUSATION_SEMANTIC_BYTES
        } else {
            0
        })
        .ok_or(StorageValueError::SizeOverflow)
    }
}

/// The complete authoritative command-log record.
#[derive(Clone, Eq, PartialEq)]
pub struct StoredCommitRecordV1 {
    commit_sequence: CommitSequence,
    admission_request_id: RequestId,
    plan: ExecutablePlanRef,
    canonical_input_hash: CanonicalInputHash,
    actor: AdmittedActorContext,
    logical_time: LogicalTime,
    partition_hash: PartitionKeyHash,
    conflict_hashes: Vec<ConflictKeyHash>,
    read_dependencies: StoredReadDependenciesV1,
    entity_references: Vec<CommittedEntityReferenceV2>,
    events: Vec<StoredDurableEventV1>,
    declared_outcome: DeclaredOutcome,
    provenance_id: ProvenanceId,
    outbox_event_ids: Vec<EventId>,
    durability_mode: DurabilityMode,
}

impl StoredCommitRecordV1 {
    /// Constructs a complete canonical commit record.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        commit_sequence: CommitSequence,
        admission_request_id: RequestId,
        plan: ExecutablePlanRef,
        canonical_input_hash: CanonicalInputHash,
        actor: AdmittedActorContext,
        logical_time: LogicalTime,
        partition_hash: PartitionKeyHash,
        conflict_hashes: Vec<ConflictKeyHash>,
        read_dependencies: StoredReadDependenciesV1,
        entity_references: Vec<CommittedEntityReferenceV2>,
        events: Vec<StoredDurableEventV1>,
        declared_outcome: DeclaredOutcome,
        provenance_id: ProvenanceId,
        outbox_event_ids: Vec<EventId>,
        durability_mode: DurabilityMode,
    ) -> Result<Self, StorageValueError> {
        validate_conflict_hashes(&conflict_hashes)?;
        validate_entity_references(&entity_references)?;
        if entity_references.iter().any(|reference| {
            read_dependencies.expected_entity_state(reference.target())
                != Some(CommittedEntityReferenceV2::expected_from_version(
                    reference.entity_version(),
                ))
        }) {
            return Err(StorageValueError::IdentityMismatch);
        }
        validate_events(commit_sequence, &events)?;
        let expected_event_ids: Vec<_> =
            events.iter().map(StoredDurableEventV1::event_id).collect();
        if outbox_event_ids != expected_event_ids {
            return Err(StorageValueError::IdentityMismatch);
        }
        let value = Self {
            commit_sequence,
            admission_request_id,
            plan,
            canonical_input_hash,
            actor,
            logical_time,
            partition_hash,
            conflict_hashes,
            read_dependencies,
            entity_references,
            events,
            declared_outcome,
            provenance_id,
            outbox_event_ids,
            durability_mode,
        };
        if value.semantic_bytes()? > MAX_COMMIT_INTENT_SEMANTIC_BYTES {
            return Err(StorageValueError::LimitExceeded);
        }
        Ok(value)
    }

    /// Returns this authoritative application sequence.
    #[must_use]
    pub const fn commit_sequence(&self) -> CommitSequence {
        self.commit_sequence
    }

    /// Returns the original admission request identity.
    #[must_use]
    pub const fn admission_request_id(&self) -> RequestId {
        self.admission_request_id
    }

    /// Borrows the exact historical executable-plan identity.
    #[must_use]
    pub const fn plan(&self) -> &ExecutablePlanRef {
        &self.plan
    }

    /// Returns the canonical input hash.
    #[must_use]
    pub const fn canonical_input_hash(&self) -> CanonicalInputHash {
        self.canonical_input_hash
    }

    /// Borrows the admitted actor context.
    #[must_use]
    pub const fn actor(&self) -> &AdmittedActorContext {
        &self.actor
    }

    /// Returns the deterministic logical time.
    #[must_use]
    pub const fn logical_time(&self) -> LogicalTime {
        self.logical_time
    }

    /// Returns the partition identity hash.
    #[must_use]
    pub const fn partition_hash(&self) -> PartitionKeyHash {
        self.partition_hash
    }

    /// Borrows canonical conflict identity hashes.
    #[must_use]
    pub fn conflict_hashes(&self) -> &[ConflictKeyHash] {
        &self.conflict_hashes
    }

    /// Borrows complete canonical dependency evidence.
    #[must_use]
    pub const fn read_dependencies(&self) -> &StoredReadDependenciesV1 {
        &self.read_dependencies
    }

    /// Borrows exact entity post-image references in ordinal order.
    #[must_use]
    pub fn entity_references(&self) -> &[CommittedEntityReferenceV2] {
        &self.entity_references
    }

    /// Borrows complete durable events in ordinal order.
    #[must_use]
    pub fn events(&self) -> &[StoredDurableEventV1] {
        &self.events
    }

    /// Returns stable event links in ordinal order.
    #[must_use]
    pub fn event_ids(&self) -> Vec<EventId> {
        self.events
            .iter()
            .map(StoredDurableEventV1::event_id)
            .collect()
    }

    /// Returns payload-free durable references in ordinal order.
    #[must_use]
    pub fn event_references(&self) -> Vec<EventReferenceV2> {
        self.events
            .iter()
            .map(EventReferenceV2::from_event)
            .collect()
    }

    /// Borrows the original declared business outcome.
    #[must_use]
    pub const fn declared_outcome(&self) -> &DeclaredOutcome {
        &self.declared_outcome
    }

    /// Returns the immutable provenance link.
    #[must_use]
    pub const fn provenance_id(&self) -> ProvenanceId {
        self.provenance_id
    }

    /// Borrows reciprocal outbox intent links.
    #[must_use]
    pub fn outbox_event_ids(&self) -> &[EventId] {
        &self.outbox_event_ids
    }

    /// Returns the durability contract used for the engine commit.
    #[must_use]
    pub const fn durability_mode(&self) -> DurabilityMode {
        self.durability_mode
    }

    pub(crate) fn semantic_bytes(&self) -> Result<usize, StorageValueError> {
        stored_commit_semantic_bytes(
            &self.plan,
            &self.actor,
            &self.conflict_hashes,
            &self.read_dependencies,
            self.entity_references
                .iter()
                .map(CommittedEntityReferenceV2::target),
            self.events
                .iter()
                .map(|event| (event.event_type_id(), event.payload_encoded_len())),
            &self.declared_outcome,
            self.outbox_event_ids.len(),
        )
    }
}

/// Per-record-class charge for one complete command write set.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct CommandWriteClassBreakdownV1 {
    allocator: usize,
    pending_resolution: usize,
    entities: usize,
    index_entries: usize,
    index_epochs: usize,
    outcome: usize,
    events: usize,
    outbox_intents: usize,
    provenance: usize,
    commit: usize,
}

impl CommandWriteClassBreakdownV1 {
    /// Checks all class values and their aggregate against the staged ceiling.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        allocator: usize,
        pending_resolution: usize,
        entities: usize,
        index_entries: usize,
        index_epochs: usize,
        outcome: usize,
        events: usize,
        outbox_intents: usize,
        provenance: usize,
        commit: usize,
    ) -> Result<Self, StorageValueError> {
        let value = Self {
            allocator,
            pending_resolution,
            entities,
            index_entries,
            index_epochs,
            outcome,
            events,
            outbox_intents,
            provenance,
            commit,
        };
        if value.total()? > MAX_STAGED_WRITE_BYTES {
            return Err(StorageValueError::LimitExceeded);
        }
        Ok(value)
    }

    /// Returns the allocator metadata contribution.
    #[must_use]
    pub const fn allocator(self) -> usize {
        self.allocator
    }

    /// Returns the pending-resolution contribution.
    #[must_use]
    pub const fn pending_resolution(self) -> usize {
        self.pending_resolution
    }

    /// Returns the framed entity mutation contribution.
    #[must_use]
    pub const fn entities(self) -> usize {
        self.entities
    }

    /// Returns the framed index mutation contribution.
    #[must_use]
    pub const fn index_entries(self) -> usize {
        self.index_entries
    }

    /// Returns the framed epoch post-image contribution.
    #[must_use]
    pub const fn index_epochs(self) -> usize {
        self.index_epochs
    }

    /// Returns the terminal outcome contribution.
    #[must_use]
    pub const fn outcome(self) -> usize {
        self.outcome
    }

    /// Returns the framed durable-event contribution.
    #[must_use]
    pub const fn events(self) -> usize {
        self.events
    }

    /// Returns the framed outbox-intent contribution.
    #[must_use]
    pub const fn outbox_intents(self) -> usize {
        self.outbox_intents
    }

    /// Returns the immutable provenance contribution.
    #[must_use]
    pub const fn provenance(self) -> usize {
        self.provenance
    }

    /// Returns the complete commit-record contribution.
    #[must_use]
    pub const fn commit(self) -> usize {
        self.commit
    }

    /// Returns the checked aggregate of every record class.
    pub fn total(self) -> Result<usize, StorageValueError> {
        [
            self.allocator,
            self.pending_resolution,
            self.entities,
            self.index_entries,
            self.index_epochs,
            self.outcome,
            self.events,
            self.outbox_intents,
            self.provenance,
            self.commit,
        ]
        .into_iter()
        .try_fold(0usize, |total, value| {
            total
                .checked_add(value)
                .ok_or(StorageValueError::SizeOverflow)
        })
    }
}

/// Conservative backend/codec upper bounds for every staged record class.
///
/// This is intentionally distinct from semantic byte accounting and exact read
/// page charges. The WP-060 memory model uses explicit per-class synthetic
/// complete-envelope charges for conformance and makes no durable-codec claim.
/// Once WP-065 supplies the canonical codec, a durable backend must recompute the
/// actual complete `StoredEnvelope` bytes for every record before staging and
/// prove each class is at most its corresponding reservation.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct EncodedWriteSetUpperBound {
    classes: CommandWriteClassBreakdownV1,
    total: usize,
}

impl EncodedWriteSetUpperBound {
    /// Checks a complete conservative per-class encoded reservation.
    pub fn new(classes: CommandWriteClassBreakdownV1) -> Result<Self, StorageValueError> {
        let total = classes.total()?;
        if total == 0 || total > MAX_STAGED_WRITE_BYTES {
            return Err(StorageValueError::LimitExceeded);
        }
        Ok(Self { classes, total })
    }

    /// Returns every conservative encoded class reservation.
    #[must_use]
    pub const fn classes(self) -> CommandWriteClassBreakdownV1 {
        self.classes
    }

    /// Returns the aggregate conservative encoded write-set bound.
    #[must_use]
    pub const fn total(self) -> usize {
        self.total
    }
}

/// Checked pre-sequence capacity charge for one privately validated command.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct CommandWriteSetChargeV1 {
    semantic_classes: CommandWriteClassBreakdownV1,
    encoded_upper_bound: EncodedWriteSetUpperBound,
}

impl CommandWriteSetChargeV1 {
    const fn from_validated_shape(
        shape: &ValidatedCommandWriteSetShapeV1,
        encoded_upper_bound: EncodedWriteSetUpperBound,
    ) -> Self {
        Self {
            semantic_classes: shape.semantic_classes,
            encoded_upper_bound,
        }
    }

    /// Returns the exact future semantic record-class breakdown.
    #[must_use]
    pub const fn semantic_classes(self) -> CommandWriteClassBreakdownV1 {
        self.semantic_classes
    }

    /// Returns the exact aggregate semantic staged-record charge.
    #[must_use]
    pub fn semantic_bytes(self) -> usize {
        self.semantic_classes
            .total()
            .expect("constructor checked semantic class arithmetic")
    }

    /// Returns the conservative codec/backend encoded write-set bound.
    #[must_use]
    pub const fn encoded_upper_bound(self) -> EncodedWriteSetUpperBound {
        self.encoded_upper_bound
    }
}

/// Complete sequence-free command write shape after semantic validation.
///
/// Construction checks target cardinality, index ordering and bindings,
/// affected-epoch coverage, every projected semantic record charge, checked
/// aggregate arithmetic, and the accepted semantic staged-write ceiling. No
/// codec sizing or application sequence is performed here.
#[derive(Eq, PartialEq)]
pub struct ValidatedCommandWriteSetShapeV1 {
    intent: CommitIntent,
    affected_targets: AffectedIndexEpochTargets,
    affected_current: AffectedEpochCurrentState,
    index_entries: Vec<IndexEntryMutationV1>,
    index_epochs: Vec<IndexEpochAdvanceV1>,
    semantic_classes: CommandWriteClassBreakdownV1,
}

impl ValidatedCommandWriteSetShapeV1 {
    /// Validates and freezes the complete sequence-free semantic write shape.
    pub fn new(
        intent: &CommitIntent,
        affected_targets: AffectedIndexEpochTargets,
        affected_current: AffectedEpochCurrentState,
        index_entries: Vec<IndexEntryMutationV1>,
        index_epochs: Vec<IndexEpochAdvanceV1>,
    ) -> Result<Self, StorageValueError> {
        let validation = intent.evaluated().validation_request();
        let validation_target_count = validation
            .binding_targets()
            .len()
            .checked_add(validation.root_validation_targets().len())
            .and_then(|value| value.checked_add(validation.range_targets().len()))
            .and_then(|value| value.checked_add(affected_targets.as_slice().len()))
            .ok_or(StorageValueError::SizeOverflow)?;
        if validation_target_count > MAX_VALIDATION_TARGETS {
            return Err(StorageValueError::LimitExceeded);
        }
        validate_index_entries(&index_entries)?;
        validate_index_epochs(&index_epochs)?;
        validate_affected_epoch_coverage(&affected_targets, &affected_current, &index_epochs)?;
        validate_post_image_bindings(
            intent.evaluated().plan(),
            intent.pending().partition_key(),
            &index_entries,
            &index_epochs,
        )?;
        let semantic_classes =
            projected_atomic_semantic_breakdown(intent, &index_entries, &index_epochs)?;
        Ok(Self {
            intent: intent.clone(),
            affected_targets,
            affected_current,
            index_entries,
            index_epochs,
            semantic_classes,
        })
    }

    /// Borrows the exact retained candidate intent.
    #[must_use]
    pub const fn intent(&self) -> &CommitIntent {
        &self.intent
    }

    /// Borrows the canonical affected prefix set.
    #[must_use]
    pub const fn affected_targets(&self) -> &AffectedIndexEpochTargets {
        &self.affected_targets
    }

    /// Borrows exact transaction-current affected epoch observations.
    #[must_use]
    pub const fn affected_current(&self) -> &AffectedEpochCurrentState {
        &self.affected_current
    }

    /// Borrows exact canonical index mutations.
    #[must_use]
    pub fn index_entries(&self) -> &[IndexEntryMutationV1] {
        &self.index_entries
    }

    /// Borrows exact canonical affected epoch advances.
    #[must_use]
    pub fn index_epochs(&self) -> &[IndexEpochAdvanceV1] {
        &self.index_epochs
    }

    /// Returns the checked semantic record-class breakdown.
    #[must_use]
    pub const fn semantic_classes(&self) -> CommandWriteClassBreakdownV1 {
        self.semantic_classes
    }
}

/// Exact sequence-free write plan retained across capacity reservation.
///
/// It owns every coordinator-derived index mutation and epoch advance, preventing
/// an equal-size shape from being substituted after preflight.
#[derive(Clone)]
pub struct CommandWriteSetPlanV1(Arc<CommandWriteSetPlanInnerV1>);

#[derive(Eq, PartialEq)]
struct CommandWriteSetPlanInnerV1 {
    intent: CommitIntent,
    affected_targets: AffectedIndexEpochTargets,
    affected_current: AffectedEpochCurrentState,
    index_entries: Vec<IndexEntryMutationV1>,
    index_epochs: Vec<IndexEpochAdvanceV1>,
    charge: CommandWriteSetChargeV1,
}

impl PartialEq for CommandWriteSetPlanV1 {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0) || self.0 == other.0
    }
}

impl Eq for CommandWriteSetPlanV1 {}

impl CommandWriteSetPlanV1 {
    /// Validates exact affected coverage and freezes the complete pre-sequence plan.
    pub fn new(
        intent: &CommitIntent,
        affected_targets: AffectedIndexEpochTargets,
        affected_current: AffectedEpochCurrentState,
        index_entries: Vec<IndexEntryMutationV1>,
        index_epochs: Vec<IndexEpochAdvanceV1>,
        encoded_upper_bound: EncodedWriteSetUpperBound,
    ) -> Result<Self, StorageValueError> {
        let shape = ValidatedCommandWriteSetShapeV1::new(
            intent,
            affected_targets,
            affected_current,
            index_entries,
            index_epochs,
        )?;
        Ok(Self::from_validated_shape(shape, encoded_upper_bound))
    }

    /// Binds a codec-proved fitting upper bound to one validated semantic shape.
    #[must_use]
    pub fn from_validated_shape(
        shape: ValidatedCommandWriteSetShapeV1,
        encoded_upper_bound: EncodedWriteSetUpperBound,
    ) -> Self {
        let charge = CommandWriteSetChargeV1::from_validated_shape(&shape, encoded_upper_bound);
        let ValidatedCommandWriteSetShapeV1 {
            intent,
            affected_targets,
            affected_current,
            index_entries,
            index_epochs,
            semantic_classes: _,
        } = shape;
        Self(Arc::new(CommandWriteSetPlanInnerV1 {
            intent,
            affected_targets,
            affected_current,
            index_entries,
            index_epochs,
            charge,
        }))
    }

    /// Borrows the exact retained candidate intent used for every derivation.
    #[must_use]
    pub fn intent(&self) -> &CommitIntent {
        &self.0.intent
    }

    /// Borrows the canonical affected prefix set.
    #[must_use]
    pub fn affected_targets(&self) -> &AffectedIndexEpochTargets {
        &self.0.affected_targets
    }

    /// Borrows exact transaction-current epoch positions used by the advances.
    #[must_use]
    pub fn affected_current(&self) -> &AffectedEpochCurrentState {
        &self.0.affected_current
    }

    /// Borrows exact canonical index mutations.
    #[must_use]
    pub fn index_entries(&self) -> &[IndexEntryMutationV1] {
        &self.0.index_entries
    }

    /// Borrows exact canonical affected epoch advances.
    #[must_use]
    pub fn index_epochs(&self) -> &[IndexEpochAdvanceV1] {
        &self.0.index_epochs
    }

    /// Returns the checked semantic and encoded capacity charge.
    #[must_use]
    pub fn charge(&self) -> CommandWriteSetChargeV1 {
        self.0.charge
    }

    /// Proves this plan is the exact candidate retained before capacity reservation.
    #[must_use]
    pub fn matches_retained_candidate(
        &self,
        intent: &CommitIntent,
        affected_targets: &AffectedIndexEpochTargets,
        affected_current: &AffectedEpochCurrentState,
    ) -> bool {
        self.0.intent == *intent
            && self.0.affected_targets == *affected_targets
            && self.0.affected_current == *affected_current
    }
}

/// The exact authoritative record graph staged for one command sequence.
#[derive(Clone, Eq, PartialEq)]
pub struct AtomicCommandRecordSet {
    assignment: AssignedCommandSequence,
    entities: Vec<CommittedEntityMutationV1>,
    write_plan: CommandWriteSetPlanV1,
    stored_outcome: StoredOutcomeV1,
    outbox_intents: Vec<StoredOutboxIntentV1>,
    provenance: StoredProvenanceRecordV1,
    commit: StoredCommitRecordV1,
    semantic_bytes: usize,
}

/// Minimal immutable evidence retained after one complete graph is physically staged.
///
/// Construction is only available by consuming an [`AtomicCommandRecordSet`], so
/// the outcome and event identities cannot diverge from the graph that passed the
/// complete reciprocal validation boundary.
pub struct StagedCommandEvidenceV1 {
    outcome: StoredOutcomeV1,
    provenance: StoredProvenanceRecordV1,
    commit: StoredCommitRecordV1,
}

/// Move-only exact command-link proof derived while consuming staged evidence.
pub struct StagedCommandAuditLinkEvidenceV1 {
    outcome: StoredOutcomeV1,
    provenance: StoredProvenanceRecordV1,
    commit: StoredCommitRecordV1,
}

impl StagedCommandAuditLinkEvidenceV1 {
    /// Proves one terminal audit link names the consumed staged graph.
    #[must_use]
    pub fn matches(&self, commit_sequence: CommitSequence, provenance_id: ProvenanceId) -> bool {
        self.commit.commit_sequence() == commit_sequence
            && self.provenance.provenance_id() == provenance_id
    }

    /// Consumes the exact staged command views and joins them to the two audit
    /// members allocated in the same authoritative transaction.
    pub fn into_capsule(
        self,
        started: StoredServiceAuditRecordV1,
        terminal: StoredServiceAuditRecordV1,
    ) -> Result<crate::StoredCommandCapsuleV1, StorageValueError> {
        crate::StoredCommandCapsuleV1::new(
            self.outcome,
            self.provenance,
            self.commit,
            started,
            terminal,
        )
    }
}

impl StagedCommandEvidenceV1 {
    /// Borrows the exact checked terminal outcome.
    #[must_use]
    pub const fn outcome(&self) -> &StoredOutcomeV1 {
        &self.outcome
    }

    /// Borrows authoritative event identities in ordinal order.
    #[must_use]
    pub fn event_ids(&self) -> &[EventId] {
        self.provenance.event_ids()
    }

    /// Consumes the post-staging evidence into its exact checked parts.
    #[must_use]
    pub fn into_parts(self) -> (StoredOutcomeV1, Vec<EventId>) {
        let event_ids = self.provenance.event_ids().to_vec();
        (self.outcome, event_ids)
    }

    /// Consumes the complete checked authority retained for a direct
    /// non-audited storage commit. Normal application writes consume the same
    /// values through `into_parts_with_command_audit_link` and a capsule.
    #[must_use]
    pub fn into_authority_parts(
        self,
    ) -> (
        StoredOutcomeV1,
        StoredProvenanceRecordV1,
        StoredCommitRecordV1,
    ) {
        (self.outcome, self.provenance, self.commit)
    }

    /// Consumes the evidence into commit publication values plus the exact
    /// move-only terminal-audit link proof derived from the same outcome.
    #[must_use]
    pub fn into_parts_with_command_audit_link(
        self,
    ) -> (
        StoredOutcomeV1,
        Vec<EventId>,
        StagedCommandAuditLinkEvidenceV1,
    ) {
        let event_ids = self.provenance.event_ids().to_vec();
        let outcome = self.outcome.clone();
        let link = StagedCommandAuditLinkEvidenceV1 {
            outcome: self.outcome,
            provenance: self.provenance,
            commit: self.commit,
        };
        (outcome, event_ids, link)
    }

    /// Consumes successful-command evidence without copying the event-ID
    /// collection already owned by the command graph.
    #[must_use]
    pub fn into_outcome_and_command_audit_link(
        self,
    ) -> (StoredOutcomeV1, StagedCommandAuditLinkEvidenceV1) {
        let outcome = self.outcome.clone();
        let link = StagedCommandAuditLinkEvidenceV1 {
            outcome: self.outcome,
            provenance: self.provenance,
            commit: self.commit,
        };
        (outcome, link)
    }
}

impl AtomicCommandRecordSet {
    /// Validates complete membership, canonical order, reciprocal links, and bounds.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        assignment: AssignedCommandSequence,
        entities: Vec<CommittedEntityMutationV1>,
        write_plan: CommandWriteSetPlanV1,
        stored_outcome: StoredOutcomeV1,
        provenance: StoredProvenanceRecordV1,
        commit: StoredCommitRecordV1,
    ) -> Result<Self, StorageValueError> {
        let expected_pending = write_plan.intent().pending();
        let evaluated = write_plan.intent().evaluated();
        let index_entries = write_plan.index_entries();
        let index_epochs = write_plan.index_epochs();
        let presequence_charge = write_plan.charge();
        let sequence = commit.commit_sequence();
        let events = commit.events();
        let mut retained_references = commit.entity_references().iter();
        let mut entity_references_match = true;
        for mutation in &entities {
            match CommittedEntityReferenceV2::from_live_mutation(mutation)? {
                Some(reference) if retained_references.next() == Some(&reference) => {}
                Some(_) => entity_references_match = false,
                None => {}
            }
        }
        entity_references_match &= retained_references.next().is_none();
        if sequence != assignment.assigned()
            || assignment.next_allocator() != expected_next_allocator(sequence)
            || expected_pending.identity() != stored_outcome.identity()
            || expected_pending.admission_request_id() != stored_outcome.admission_request_id()
            || expected_pending.plan() != stored_outcome.plan()
            || expected_pending.canonical_input_hash() != stored_outcome.canonical_input_hash()
            || expected_pending.actor() != stored_outcome.actor()
            || expected_pending.logical_time() != stored_outcome.logical_time()
            || expected_pending.provenance_claims() != stored_outcome.admitted_claims()
            || expected_pending.causation() != stored_outcome.causation()
            || expected_pending.service_values() != stored_outcome.service_values()
            || expected_pending.partition_key() != stored_outcome.partition_key()
            || hash_partition_key(expected_pending.partition_key().as_bytes())
                != stored_outcome.partition_hash()
            || !entity_references_match
            || entities.iter().any(|mutation| {
                mutation.checked_image().written_by_contract() != commit.plan().contract_version()
                    || !mutation
                        .checked_image()
                        .schema_binding()
                        .matches_plan(commit.plan())
                    || commit
                        .read_dependencies()
                        .expected_entity_state(mutation.target())
                        != Some(mutation.expected())
            })
            || stored_outcome.commit_sequence() != sequence
            || stored_outcome.plan() != commit.plan()
            || stored_outcome.admission_request_id() != commit.admission_request_id()
            || stored_outcome.canonical_input_hash() != commit.canonical_input_hash()
            || stored_outcome.actor() != commit.actor()
            || stored_outcome.logical_time() != commit.logical_time()
            || stored_outcome.partition_hash() != commit.partition_hash()
            || stored_outcome.conflict_hashes() != commit.conflict_hashes()
            || stored_outcome.declared_outcome() != commit.declared_outcome()
            || stored_outcome.provenance_id() != commit.provenance_id()
            || stored_outcome.durability_mode() != commit.durability_mode()
            || provenance.commit_sequence() != sequence
            || provenance.provenance_id() != commit.provenance_id()
            || provenance.identity() != stored_outcome.identity()
            || provenance.admission_request_id() != commit.admission_request_id()
            || provenance.plan() != commit.plan()
            || provenance.canonical_input_hash() != commit.canonical_input_hash()
            || provenance.actor() != commit.actor()
            || provenance.logical_time() != commit.logical_time()
            || provenance.partition_hash() != commit.partition_hash()
            || provenance.conflict_hashes() != commit.conflict_hashes()
            || provenance.outcome_id() != commit.declared_outcome().outcome_id()
            || provenance.admitted_claims() != stored_outcome.admitted_claims()
            || provenance.causation() != stored_outcome.causation()
            || stored_outcome.provenance_id() != write_plan.intent().provenance_id()
            || stored_outcome.partition_hash() != write_plan.intent().partition_hash()
            || stored_outcome.conflict_hashes() != write_plan.intent().conflict_hashes()
            || stored_outcome.declared_outcome() != evaluated.outcome()
        {
            return Err(StorageValueError::IdentityMismatch);
        }

        validate_committed_mutations(&entities)?;
        validate_intent_entity_derivation(evaluated, &entities)?;
        validate_index_entries(index_entries)?;
        validate_index_epochs(index_epochs)?;
        validate_affected_target_coverage(write_plan.affected_targets(), index_epochs)?;
        validate_post_image_bindings(
            commit.plan(),
            expected_pending.partition_key(),
            index_entries,
            index_epochs,
        )?;
        validate_events(sequence, events)?;
        validate_intent_event_derivation(evaluated, sequence, events)?;
        if !commit
            .read_dependencies()
            .matches_live(evaluated.read_dependencies())
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        if !commit
            .outbox_event_ids()
            .iter()
            .copied()
            .eq(events.iter().map(StoredDurableEventV1::event_id))
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        let mut outbox_intents = Vec::with_capacity(events.len());
        for event in events {
            outbox_intents.push(StoredOutboxIntentV1::new(event.clone()));
        }

        if provenance.affected_entities().len() != entities.len()
            || provenance
                .affected_entities()
                .iter()
                .zip(&entities)
                .any(|(affected, mutation)| affected != &AffectedEntityV1::from_mutation(mutation))
            || provenance.event_ids().len() != events.len()
            || provenance
                .event_ids()
                .iter()
                .copied()
                .zip(events.iter().map(StoredDurableEventV1::event_id))
                .any(|(provenance, event)| provenance != event)
        {
            return Err(StorageValueError::IdentityMismatch);
        }

        let semantic_classes = atomic_semantic_breakdown(
            expected_pending,
            &entities,
            index_entries,
            index_epochs,
            events,
            &outbox_intents,
            &stored_outcome,
            &provenance,
            &commit,
        )?;
        if semantic_classes != presequence_charge.semantic_classes() {
            return Err(StorageValueError::IdentityMismatch);
        }
        let semantic_bytes = semantic_classes.total()?;
        Ok(Self {
            assignment,
            entities,
            write_plan,
            stored_outcome,
            outbox_intents,
            provenance,
            commit,
            semantic_bytes,
        })
    }

    /// Returns allocator metadata after assigning this record set's sequence.
    #[must_use]
    pub const fn next_application_sequence(&self) -> ApplicationSequenceAllocator {
        self.assignment.next_allocator()
    }

    /// Borrows the exact pending admission this record set atomically resolves.
    #[must_use]
    pub fn expected_pending(&self) -> &crate::StoredPendingAdmissionV1 {
        self.write_plan.intent().pending()
    }

    /// Returns the exact invisible sequence assignment used for every final ID.
    #[must_use]
    pub const fn assignment(&self) -> AssignedCommandSequence {
        self.assignment
    }

    /// Borrows the exact retained intent from which the graph was derived.
    #[must_use]
    pub fn intent(&self) -> &CommitIntent {
        self.write_plan.intent()
    }

    /// Borrows canonical committed entity changes.
    #[must_use]
    pub fn entities(&self) -> &[CommittedEntityMutationV1] {
        &self.entities
    }

    /// Borrows canonical secondary-index changes.
    #[must_use]
    pub fn index_entries(&self) -> &[IndexEntryMutationV1] {
        self.write_plan.index_entries()
    }

    /// Borrows canonical partition/index generation advances.
    #[must_use]
    pub fn index_epochs(&self) -> &[IndexEpochAdvanceV1] {
        self.write_plan.index_epochs()
    }

    /// Borrows the terminal stored outcome.
    #[must_use]
    pub const fn stored_outcome(&self) -> &StoredOutcomeV1 {
        &self.stored_outcome
    }

    /// Borrows authoritative events in ordinal order.
    #[must_use]
    pub fn events(&self) -> &[StoredDurableEventV1] {
        self.commit.events()
    }

    /// Borrows reciprocal authoritative outbox intents.
    #[must_use]
    pub fn outbox_intents(&self) -> &[StoredOutboxIntentV1] {
        &self.outbox_intents
    }

    /// Borrows immutable command provenance.
    #[must_use]
    pub const fn provenance(&self) -> &StoredProvenanceRecordV1 {
        &self.provenance
    }

    /// Borrows the complete authoritative commit record.
    #[must_use]
    pub const fn commit(&self) -> &StoredCommitRecordV1 {
        &self.commit
    }

    /// Returns checked aggregate semantic write-set bytes.
    #[must_use]
    pub const fn semantic_bytes(&self) -> usize {
        self.semantic_bytes
    }

    /// Returns the pre-sequence charge cross-checked against this complete graph.
    #[must_use]
    pub fn presequence_charge(&self) -> CommandWriteSetChargeV1 {
        self.write_plan.charge()
    }

    /// Borrows the exact sequence-free plan retained across capacity reservation.
    #[must_use]
    pub const fn write_plan(&self) -> &CommandWriteSetPlanV1 {
        &self.write_plan
    }

    /// Releases the complete graph after physical staging and retains only the
    /// immutable evidence required to finish or recover the engine commit.
    #[must_use]
    pub fn into_staged_evidence(self) -> StagedCommandEvidenceV1 {
        let Self {
            stored_outcome,
            provenance,
            commit,
            ..
        } = self;
        StagedCommandEvidenceV1 {
            outcome: stored_outcome,
            provenance,
            commit,
        }
    }

    /// Proves this graph exactly matches the sequence-assigned candidate being staged.
    #[must_use]
    pub fn matches_reserved_candidate(
        &self,
        assignment: AssignedCommandSequence,
        intent: &CommitIntent,
        write_plan: &CommandWriteSetPlanV1,
    ) -> bool {
        self.assignment == assignment
            && self.intent() == intent
            && self.write_plan() == write_plan
            && self.commit.commit_sequence() == assignment.assigned()
    }

    /// Proves both durable command records use the requested engine commit mode.
    #[must_use]
    pub fn matches_durability_mode(&self, durability: DurabilityMode) -> bool {
        self.stored_outcome.durability_mode() == durability
            && self.commit.durability_mode() == durability
    }
}

fn validate_conflict_hashes(hashes: &[ConflictKeyHash]) -> Result<(), StorageValueError> {
    if hashes.len() > MAX_COMMIT_CONFLICT_HASHES {
        return Err(StorageValueError::LimitExceeded);
    }
    if hashes.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(StorageValueError::NonCanonicalOrder);
    }
    Ok(())
}

fn validate_committed_mutations(
    mutations: &[CommittedEntityMutationV1],
) -> Result<(), StorageValueError> {
    if mutations.len() > MAX_ENTITY_MUTATIONS {
        return Err(StorageValueError::LimitExceeded);
    }
    if mutations.windows(2).any(|pair| {
        pair[0].target().canonical_target_key() >= pair[1].target().canonical_target_key()
    }) {
        return Err(StorageValueError::NonCanonicalOrder);
    }
    Ok(())
}

fn validate_entity_references(
    references: &[CommittedEntityReferenceV2],
) -> Result<(), StorageValueError> {
    if references.len() > MAX_ENTITY_MUTATIONS {
        return Err(StorageValueError::LimitExceeded);
    }
    if references.windows(2).any(|pair| {
        pair[0].target().canonical_target_key() >= pair[1].target().canonical_target_key()
    }) {
        return Err(StorageValueError::NonCanonicalOrder);
    }
    Ok(())
}

fn validate_intent_entity_derivation(
    evaluated: &crate::EvaluatedCommand,
    committed: &[CommittedEntityMutationV1],
) -> Result<(), StorageValueError> {
    if evaluated.mutations().len() != committed.len() {
        return Err(StorageValueError::IdentityMismatch);
    }

    for (intent_mutation, committed_mutation) in evaluated.mutations().iter().zip(committed) {
        let expected = match intent_mutation {
            EntityMutation::Create(_) => ExpectedEntityState::Absent,
            EntityMutation::Replace {
                expected_version, ..
            }
            | EntityMutation::Delete {
                expected_version, ..
            } => ExpectedEntityState::Present(*expected_version),
        };
        let intent_post_image = intent_mutation.post_image();
        let committed_image = committed_mutation.checked_image();
        let kinds_match = matches!(
            (intent_mutation, committed_mutation),
            (
                EntityMutation::Create(_) | EntityMutation::Replace { .. },
                CommittedEntityMutationV1::Put { .. }
            ) | (
                EntityMutation::Delete { .. },
                CommittedEntityMutationV1::Delete { .. }
            )
        );
        if committed_mutation.expected() != expected
            || !kinds_match
            || committed_image.target() != intent_post_image.target()
            || committed_image.written_by_contract() != intent_post_image.written_by_contract()
            || committed_image.fields() != intent_post_image.fields()
            || !committed_image
                .schema_binding()
                .matches_plan(evaluated.plan())
        {
            return Err(StorageValueError::IdentityMismatch);
        }
    }
    Ok(())
}

fn validate_index_entries(entries: &[IndexEntryMutationV1]) -> Result<(), StorageValueError> {
    if entries.len() > MAX_INDEX_DELTAS {
        return Err(StorageValueError::LimitExceeded);
    }
    if entries
        .windows(2)
        .any(|pair| pair[0].key().as_bytes() >= pair[1].key().as_bytes())
    {
        return Err(StorageValueError::NonCanonicalOrder);
    }
    Ok(())
}

fn validate_index_epochs(epochs: &[IndexEpochAdvanceV1]) -> Result<(), StorageValueError> {
    if epochs.len() > MAX_INDEX_DELTAS {
        return Err(StorageValueError::LimitExceeded);
    }
    if epochs
        .windows(2)
        .any(|pair| pair[0].target() >= pair[1].target())
    {
        return Err(StorageValueError::NonCanonicalOrder);
    }
    Ok(())
}

fn validate_post_image_bindings(
    plan: &ExecutablePlanRef,
    command_partition: &PartitionKey,
    entries: &[IndexEntryMutationV1],
    epochs: &[IndexEpochAdvanceV1],
) -> Result<(), StorageValueError> {
    if entries.iter().any(|entry| match entry {
        IndexEntryMutationV1::Delete(_) => false,
        IndexEntryMutationV1::Put(record) => {
            !record.schema_binding().matches_plan(plan)
                || record.partition_key() != command_partition
        }
    }) || epochs
        .iter()
        .any(|epoch| !epoch.post_image().schema_binding().matches_plan(plan))
    {
        return Err(StorageValueError::IdentityMismatch);
    }
    Ok(())
}

fn validate_affected_target_coverage(
    targets: &AffectedIndexEpochTargets,
    epochs: &[IndexEpochAdvanceV1],
) -> Result<(), StorageValueError> {
    if targets.as_slice().len() != epochs.len()
        || targets
            .as_slice()
            .iter()
            .zip(epochs)
            .any(|(target, epoch)| target != epoch.target())
    {
        return Err(StorageValueError::IdentityMismatch);
    }
    Ok(())
}

fn validate_affected_epoch_coverage(
    targets: &AffectedIndexEpochTargets,
    current: &AffectedEpochCurrentState,
    epochs: &[IndexEpochAdvanceV1],
) -> Result<(), StorageValueError> {
    validate_affected_target_coverage(targets, epochs)?;
    if current.observations().len() != epochs.len()
        || current.unique_occupancies().len() != targets.unique_targets().len()
        || current
            .observations()
            .iter()
            .zip(epochs)
            .any(|(observation, epoch)| {
                observation.target() != epoch.target() || observation.epoch() != epoch.prior()
            })
    {
        return Err(StorageValueError::IdentityMismatch);
    }
    Ok(())
}

fn stored_entity_semantic_bytes(
    target: &EntityTarget,
    schema_binding: &DurableKeySchemaBindingV1,
    fields_encoded_len: usize,
) -> Result<usize, StorageValueError> {
    target
        .semantic_bytes()?
        .checked_add(8 + 8)
        .and_then(|value| value.checked_add(schema_binding.semantic_bytes().ok()?))
        .and_then(|value| value.checked_add(framed_bytes(fields_encoded_len).ok()?))
        .ok_or(StorageValueError::SizeOverflow)
}

fn committed_entity_semantic_bytes_from_len(
    expected: ExpectedEntityState,
    target: &EntityTarget,
    schema_binding: &DurableKeySchemaBindingV1,
    fields_encoded_len: usize,
) -> Result<usize, StorageValueError> {
    let expected_bytes: usize = match expected {
        ExpectedEntityState::Absent => 1,
        ExpectedEntityState::Present(_) => 1 + 8,
    };
    expected_bytes
        .checked_add(stored_entity_semantic_bytes(
            target,
            schema_binding,
            fields_encoded_len,
        )?)
        .ok_or(StorageValueError::SizeOverflow)
}

fn stored_event_semantic_bytes(
    _event_type_id: EventTypeId,
    payload_encoded_len: usize,
) -> Result<usize, StorageValueError> {
    framed_bytes(payload_encoded_len)?
        .checked_add(12 + 4 + 32)
        .ok_or(StorageValueError::SizeOverflow)
}

fn stored_outcome_semantic_bytes(
    identity: &IdempotencyIdentity,
    plan: &ExecutablePlanRef,
    actor: &AdmittedActorContext,
    partition_key: &PartitionKey,
    conflict_hashes: &[ConflictKeyHash],
    outcome: &DeclaredOutcome,
    admitted_claims: &StoredAdmittedProvenanceClaimsV1,
) -> Result<usize, StorageValueError> {
    let identity = identity
        .storage_key()
        .map_err(|_| StorageValueError::InvalidShape)?;
    let conflict_bytes = conflict_hashes
        .len()
        .checked_mul(32)
        .ok_or(StorageValueError::SizeOverflow)?;
    framed_bytes(identity.as_bytes().len())?
        .checked_add(8 + 16)
        .and_then(|value| value.checked_add(plan.semantic_bytes()?))
        .and_then(|value| value.checked_add(32))
        .and_then(|value| value.checked_add(actor_semantic_bytes(actor).ok()?))
        .and_then(|value| value.checked_add(12 + 32 + 4))
        .and_then(|value| value.checked_add(framed_bytes(partition_key.as_bytes().len()).ok()?))
        .and_then(|value| value.checked_add(conflict_bytes))
        .and_then(|value| value.checked_add(outcome.semantic_bytes().ok()?))
        .and_then(|value| value.checked_add(admitted_claims.semantic_bytes().ok()?))
        .and_then(|value| value.checked_add(16 + 1))
        .ok_or(StorageValueError::SizeOverflow)
}

fn stored_provenance_semantic_bytes<'a>(
    identity: &IdempotencyIdentity,
    plan: &ExecutablePlanRef,
    actor: &AdmittedActorContext,
    conflict_hashes: &[ConflictKeyHash],
    affected_targets: impl IntoIterator<Item = &'a EntityTarget>,
    event_count: usize,
    admitted_claims: &StoredAdmittedProvenanceClaimsV1,
) -> Result<usize, StorageValueError> {
    let identity = identity
        .storage_key()
        .map_err(|_| StorageValueError::InvalidShape)?;
    let conflict_bytes = conflict_hashes
        .len()
        .checked_mul(32)
        .ok_or(StorageValueError::SizeOverflow)?;
    let mut total = 16usize
        .checked_add(8)
        .and_then(|value| value.checked_add(framed_bytes(identity.as_bytes().len()).ok()?))
        .and_then(|value| value.checked_add(16))
        .and_then(|value| value.checked_add(plan.semantic_bytes()?))
        .and_then(|value| value.checked_add(32))
        .and_then(|value| value.checked_add(actor_semantic_bytes(actor).ok()?))
        .and_then(|value| value.checked_add(12 + 32 + 4))
        .and_then(|value| value.checked_add(conflict_bytes))
        .and_then(|value| value.checked_add(4 + 4 + 4))
        .ok_or(StorageValueError::SizeOverflow)?;
    for target in affected_targets {
        total = total
            .checked_add(target.semantic_bytes()?)
            .and_then(|value| value.checked_add(8))
            .ok_or(StorageValueError::SizeOverflow)?;
    }
    total
        .checked_add(
            event_count
                .checked_mul(12)
                .ok_or(StorageValueError::SizeOverflow)?,
        )
        .and_then(|value| value.checked_add(admitted_claims.semantic_bytes().ok()?))
        .ok_or(StorageValueError::SizeOverflow)
}

#[allow(clippy::too_many_arguments)]
fn stored_commit_semantic_bytes<'a, R, E>(
    plan: &ExecutablePlanRef,
    actor: &AdmittedActorContext,
    conflict_hashes: &[ConflictKeyHash],
    read_dependencies: &StoredReadDependenciesV1,
    entity_references: R,
    events: E,
    outcome: &DeclaredOutcome,
    event_count: usize,
) -> Result<usize, StorageValueError>
where
    R: IntoIterator<Item = &'a EntityTarget>,
    E: IntoIterator<Item = (EventTypeId, usize)>,
{
    let conflict_bytes = conflict_hashes
        .len()
        .checked_mul(32)
        .ok_or(StorageValueError::SizeOverflow)?;
    let mut total = 8usize
        .checked_add(16)
        .and_then(|value| value.checked_add(plan.semantic_bytes()?))
        .and_then(|value| value.checked_add(32))
        .and_then(|value| value.checked_add(actor_semantic_bytes(actor).ok()?))
        .and_then(|value| value.checked_add(12 + 32 + 4))
        .and_then(|value| value.checked_add(conflict_bytes))
        .and_then(|value| value.checked_add(read_dependencies.semantic_bytes().ok()?))
        .and_then(|value| value.checked_add(4 + 4))
        .ok_or(StorageValueError::SizeOverflow)?;
    for target in entity_references {
        // Reference charge: target + entity_version (8) + post_image_hash (32).
        total = total
            .checked_add(target.semantic_bytes()?)
            .and_then(|value| value.checked_add(8 + 32))
            .ok_or(StorageValueError::SizeOverflow)?;
    }
    for (event_type_id, payload_encoded_len) in events {
        total = total
            .checked_add(stored_event_semantic_bytes(
                event_type_id,
                payload_encoded_len,
            )?)
            .ok_or(StorageValueError::SizeOverflow)?;
    }
    total
        .checked_add(outcome.semantic_bytes()?)
        .and_then(|value| value.checked_add(16 + 4))
        .and_then(|value| value.checked_add(event_count.checked_mul(12)?))
        .and_then(|value| value.checked_add(1))
        .ok_or(StorageValueError::SizeOverflow)
}

fn validate_events(
    sequence: CommitSequence,
    events: &[StoredDurableEventV1],
) -> Result<(), StorageValueError> {
    if events.len() > MAX_EVENT_INTENTS {
        return Err(StorageValueError::LimitExceeded);
    }
    for (ordinal, event) in events.iter().enumerate() {
        let ordinal = u32::try_from(ordinal).map_err(|_| StorageValueError::LimitExceeded)?;
        if event.event_id() != EventId::new(sequence, ordinal) {
            return Err(StorageValueError::IdentityMismatch);
        }
    }
    Ok(())
}

fn validate_intent_event_derivation(
    evaluated: &crate::EvaluatedCommand,
    sequence: CommitSequence,
    events: &[StoredDurableEventV1],
) -> Result<(), StorageValueError> {
    if evaluated.event_intents().len() != events.len() {
        return Err(StorageValueError::IdentityMismatch);
    }

    for (ordinal, (intent_event, committed_event)) in
        evaluated.event_intents().iter().zip(events).enumerate()
    {
        let ordinal = u32::try_from(ordinal).map_err(|_| StorageValueError::LimitExceeded)?;
        if committed_event.event_id() != EventId::new(sequence, ordinal)
            || committed_event.event_type_id() != intent_event.event_type_id()
            || committed_event.payload() != intent_event.payload()
        {
            return Err(StorageValueError::IdentityMismatch);
        }
    }
    Ok(())
}

fn expected_next_allocator(sequence: CommitSequence) -> ApplicationSequenceAllocator {
    sequence.checked_next().map_or(
        ApplicationSequenceAllocator::Exhausted,
        ApplicationSequenceAllocator::Next,
    )
}

fn projected_atomic_semantic_breakdown(
    intent: &CommitIntent,
    index_entries: &[IndexEntryMutationV1],
    index_epochs: &[IndexEpochAdvanceV1],
) -> Result<CommandWriteClassBreakdownV1, StorageValueError> {
    let pending = intent.pending();
    let evaluated = intent.evaluated();
    let binding = DurableKeySchemaBindingV1::from_plan(evaluated.plan());
    let expected_for = |mutation: &EntityMutation| match mutation {
        EntityMutation::Create(_) => ExpectedEntityState::Absent,
        EntityMutation::Replace {
            expected_version, ..
        }
        | EntityMutation::Delete {
            expected_version, ..
        } => ExpectedEntityState::Present(*expected_version),
    };

    let entity_bytes = evaluated.mutations().iter().try_fold(
        0usize,
        |total, mutation| -> Result<usize, StorageValueError> {
            total
                .checked_add(committed_entity_semantic_bytes_from_len(
                    expected_for(mutation),
                    mutation.target(),
                    &binding,
                    mutation.post_image().fields_encoded_len(),
                )?)
                .ok_or(StorageValueError::SizeOverflow)
        },
    )?;
    let index_bytes = index_entries.iter().try_fold(0usize, |total, entry| {
        total
            .checked_add(entry.semantic_bytes()?)
            .ok_or(StorageValueError::SizeOverflow)
    })?;
    let epoch_bytes = index_epochs.iter().try_fold(0usize, |total, epoch| {
        total
            .checked_add(epoch.semantic_bytes()?)
            .ok_or(StorageValueError::SizeOverflow)
    })?;
    let event_bytes = evaluated
        .event_intents()
        .iter()
        .try_fold(0usize, |total, event| {
            total
                .checked_add(stored_event_semantic_bytes(
                    event.event_type_id(),
                    event.payload_encoded_len(),
                )?)
                .ok_or(StorageValueError::SizeOverflow)
        })?;
    let stored_dependencies = StoredReadDependenciesV1::from_live(evaluated.read_dependencies())?;
    let mut outcome_bytes = stored_outcome_semantic_bytes(
        pending.identity(),
        evaluated.plan(),
        pending.actor(),
        pending.partition_key(),
        intent.conflict_hashes(),
        evaluated.outcome(),
        pending.provenance_claims(),
    )?;
    if !pending.service_values().is_empty() {
        outcome_bytes = outcome_bytes
            .checked_add(framed_bytes(
                encode_canonical_record(pending.service_values())
                    .map_err(|error| canonical_codec_storage_error(&error))?
                    .len(),
            )?)
            .ok_or(StorageValueError::SizeOverflow)?;
    }
    let provenance_bytes = stored_provenance_semantic_bytes(
        pending.identity(),
        evaluated.plan(),
        pending.actor(),
        intent.conflict_hashes(),
        evaluated.mutations().iter().map(EntityMutation::target),
        evaluated.event_intents().len(),
        pending.provenance_claims(),
    )?
    .checked_add(if pending.causation().is_some() {
        crate::command::STORED_COMMAND_CAUSATION_SEMANTIC_BYTES
    } else {
        0
    })
    .ok_or(StorageValueError::SizeOverflow)?;
    let commit_bytes = stored_commit_semantic_bytes(
        evaluated.plan(),
        pending.actor(),
        intent.conflict_hashes(),
        &stored_dependencies,
        evaluated
            .mutations()
            .iter()
            .filter(|mutation| !mutation.is_delete())
            .map(EntityMutation::target),
        evaluated
            .event_intents()
            .iter()
            .map(|event| (event.event_type_id(), event.payload_encoded_len())),
        evaluated.outcome(),
        evaluated.event_intents().len(),
    )?;

    CommandWriteClassBreakdownV1::new(
        // Reserve the complete fixed-width future allocator field even when
        // the maximum assigned sequence transitions the state to Exhausted.
        9,
        pending.semantic_bytes()?,
        entity_bytes
            .checked_add(4)
            .ok_or(StorageValueError::SizeOverflow)?,
        index_bytes
            .checked_add(4)
            .ok_or(StorageValueError::SizeOverflow)?,
        epoch_bytes
            .checked_add(4)
            .ok_or(StorageValueError::SizeOverflow)?,
        outcome_bytes,
        event_bytes
            .checked_add(4)
            .ok_or(StorageValueError::SizeOverflow)?,
        event_bytes
            .checked_add(4)
            .ok_or(StorageValueError::SizeOverflow)?,
        provenance_bytes,
        commit_bytes,
    )
}

#[allow(clippy::too_many_arguments)]
fn atomic_semantic_breakdown(
    expected_pending: &crate::StoredPendingAdmissionV1,
    entities: &[CommittedEntityMutationV1],
    index_entries: &[IndexEntryMutationV1],
    index_epochs: &[IndexEpochAdvanceV1],
    events: &[StoredDurableEventV1],
    outbox_intents: &[StoredOutboxIntentV1],
    outcome: &StoredOutcomeV1,
    provenance: &StoredProvenanceRecordV1,
    commit: &StoredCommitRecordV1,
) -> Result<CommandWriteClassBreakdownV1, StorageValueError> {
    let entity_bytes = entities.iter().try_fold(4usize, |total, mutation| {
        total
            .checked_add(mutation.semantic_bytes()?)
            .ok_or(StorageValueError::SizeOverflow)
    })?;
    let index_bytes = index_entries.iter().try_fold(4usize, |total, entry| {
        total
            .checked_add(entry.semantic_bytes()?)
            .ok_or(StorageValueError::SizeOverflow)
    })?;
    let epoch_bytes = index_epochs.iter().try_fold(4usize, |total, epoch| {
        total
            .checked_add(epoch.semantic_bytes()?)
            .ok_or(StorageValueError::SizeOverflow)
    })?;
    let event_bytes = events.iter().try_fold(4usize, |total, event| {
        total
            .checked_add(event.semantic_bytes()?)
            .ok_or(StorageValueError::SizeOverflow)
    })?;
    let outbox_bytes = outbox_intents.iter().try_fold(4usize, |total, intent| {
        total
            .checked_add(intent.semantic_bytes()?)
            .ok_or(StorageValueError::SizeOverflow)
    })?;
    CommandWriteClassBreakdownV1::new(
        9,
        expected_pending.semantic_bytes()?,
        entity_bytes,
        index_bytes,
        epoch_bytes,
        outcome.semantic_bytes()?,
        event_bytes,
        outbox_bytes,
        provenance.semantic_bytes()?,
        commit.semantic_bytes()?,
    )
}

macro_rules! redacted_debug {
    ($($type:ty),+ $(,)?) => {
        $(
            impl fmt::Debug for $type {
                fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                    formatter.write_str(concat!(stringify!($type), "([REDACTED])"))
                }
            }
        )+
    };
}

redacted_debug!(
    StoredEntityRecordV1,
    StoredIndexEntryV1,
    StoredIndexEntryV2,
    StoredIndexEpochV1,
    IndexEntryMutationV1,
    IndexEpochAdvanceV1,
    CommittedEntityMutationV1,
    StoredOutcomeV1,
    StoredDurableEventV1,
    StoredOutboxIntentV1,
    AffectedEntityV1,
    StoredProvenanceRecordV1,
    StoredCommitRecordV1,
    ValidatedCommandWriteSetShapeV1,
    CommandWriteSetPlanV1,
    AtomicCommandRecordSet,
    StagedCommandAuditLinkEvidenceV1,
    StagedCommandEvidenceV1,
);

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_types::{
        ActorId, ActorKind, AggregateTypeId, ApprovalId, CanonicalInputHash, CanonicalValue,
        CommandId, ContractBundleHash, ContractLineage, DatabaseId, DigestKeyId, EntityKeyBuilder,
        EntityTypeId, Environment, FieldId, PartitionKeyBuilder, PlanHash, TenantId, TenantScope,
        Timestamp,
    };

    use crate::{
        AffectedEpochCurrentState, AffectedIndexEpochTargets, EntityMutation, EntityObservation,
        EntityPostImage, EvaluatedCommand, EvaluationBudget, EventIntent, IdempotencyKeyDigest,
        PreEvaluationCommitContext, ReadDependencies, ReadDependency, ReadSnapshot,
        SnapshotRequest, StoredPendingAdmissionV1,
    };

    fn uuid_bytes(fill: u8) -> [u8; 16] {
        let mut bytes = [fill; 16];
        bytes[6] = 0x70 | (fill & 0x0f);
        bytes[8] = 0x80 | (fill & 0x3f);
        bytes
    }

    fn plan() -> ExecutablePlanRef {
        ExecutablePlanRef::new(
            ContractLineage::new("bounded-records").expect("lineage"),
            ContractVersion::new(1).expect("version"),
            ContractBundleHash::from_bytes([0x21; 32]),
            CommandId::new(1).expect("command"),
            PlanHash::from_bytes([0x22; 32]),
        )
    }

    fn entity_target() -> EntityTarget {
        let entity_type = EntityTypeId::new(1).expect("entity type");
        let mut key = EntityKeyBuilder::new(entity_type);
        key.push_u64(1).expect("key component");
        EntityTarget::new(entity_type, key.finish().expect("entity key")).expect("target")
    }

    fn payload_record(length: usize) -> CanonicalRecord {
        CanonicalRecord::new(vec![(
            FieldId::new(1).expect("field"),
            CanonicalValue::bytes(vec![0xa5; length]).expect("bounded payload"),
        )])
        .expect("record")
    }

    #[test]
    fn affected_entity_reconstructs_from_exact_stored_parts() {
        let target = entity_target();
        let entity_version = EntityVersion::first();
        let affected = AffectedEntityV1::from_stored_parts(target.clone(), entity_version);

        assert_eq!(affected.target(), &target);
        assert_eq!(affected.entity_version(), entity_version);
    }

    #[test]
    fn staged_record_graph_consumption_preserves_only_outcome_and_event_identity() {
        let records = atomic_record_set(8, &[5, 7]).expect("valid record graph");
        let expected_outcome = records.stored_outcome().clone();
        let expected_event_ids = records
            .events()
            .iter()
            .map(StoredDurableEventV1::event_id)
            .collect::<Vec<_>>();

        let evidence = records.into_staged_evidence();
        assert_eq!(evidence.event_ids(), expected_event_ids);
        let (outcome, event_ids) = evidence.into_parts();

        assert_eq!(outcome, expected_outcome);
        assert_eq!(event_ids, expected_event_ids);
    }

    #[test]
    fn staged_command_evidence_matches_only_its_exact_command_audit_link() {
        let records = atomic_record_set(8, &[5, 7]).expect("valid record graph");
        let sequence = records.commit().commit_sequence();
        let provenance_id = records.provenance().provenance_id();
        let (_, _, link) = records
            .into_staged_evidence()
            .into_parts_with_command_audit_link();
        assert!(link.matches(sequence, provenance_id));
        assert!(!link.matches(
            sequence.checked_next().expect("next sequence"),
            provenance_id,
        ));
        assert!(!link.matches(
            sequence,
            ProvenanceId::from_bytes(uuid_bytes(0x66)).expect("other provenance"),
        ));
        assert_eq!(
            format!("{link:?}"),
            "StagedCommandAuditLinkEvidenceV1([REDACTED])"
        );
    }

    #[test]
    fn staged_command_evidence_moves_directly_into_segment_link_authority() {
        let records = atomic_record_set(8, &[5, 7]).expect("valid record graph");
        let expected_outcome = records.stored_outcome().clone();
        let sequence = records.commit().commit_sequence();
        let provenance_id = records.provenance().provenance_id();

        let (outcome, link) = records
            .into_staged_evidence()
            .into_outcome_and_command_audit_link();

        assert_eq!(outcome, expected_outcome);
        assert!(link.matches(sequence, provenance_id));
    }

    #[test]
    fn atomic_record_graph_and_commit_share_one_event_collection() {
        let records = atomic_record_set(8, &[5, 7]).expect("valid record graph");

        assert_eq!(
            records.events().as_ptr(),
            records.commit().events().as_ptr()
        );
        assert_eq!(
            records.outbox_intents()[0].event(),
            &records.commit().events()[0]
        );
    }

    fn atomic_record_set(
        entity_payload_bytes: usize,
        event_payload_bytes: &[usize],
    ) -> Result<AtomicCommandRecordSet, StorageValueError> {
        let plan = plan();
        let sequence = CommitSequence::first();
        let tenant_scope = TenantScope::Tenant(TenantId::new("tenant-a").expect("tenant"));
        let principal = ActorId::new("principal-a").expect("principal");
        let actor = AdmittedActorContext::new(
            principal.clone(),
            ActorKind::Human,
            tenant_scope.clone(),
            None,
        );
        let identity = IdempotencyIdentity::new(
            DatabaseId::from_bytes(uuid_bytes(0x11)).expect("database"),
            Environment::new("test").expect("environment"),
            tenant_scope,
            principal,
            plan.contract_lineage().clone(),
            plan.command_id(),
            IdempotencyKeyDigest::from_hmac_bytes(
                DigestKeyId::new(1).expect("digest key"),
                [0x31; 32],
            ),
        );
        let request_id = RequestId::from_bytes(uuid_bytes(0x12)).expect("request");
        let provenance_id = ProvenanceId::from_bytes(uuid_bytes(0x13)).expect("provenance");
        let logical_time = LogicalTime::new(Timestamp::new(42, 7).expect("timestamp"));
        let mut partition = PartitionKeyBuilder::new(AggregateTypeId::new(1).expect("aggregate"));
        partition.push_u64(1).expect("partition component");
        let partition = partition.finish().expect("partition");
        let pending = StoredPendingAdmissionV1::new(
            identity.clone(),
            CanonicalInputHash::from_bytes([0x32; 32]),
            request_id,
            plan.clone(),
            logical_time,
            actor.clone(),
            partition.clone(),
            StoredAdmittedProvenanceClaimsV1::default(),
        )?;

        let target = entity_target();
        let snapshot_request =
            SnapshotRequest::new(plan.clone(), vec![target.clone()], Vec::new(), Vec::new())?;
        let snapshot = ReadSnapshot::new(
            &snapshot_request,
            None,
            vec![EntityObservation::Absent(target.clone())],
            Vec::new(),
            Vec::new(),
        )?;
        let runtime_mutation = EntityMutation::Create(EntityPostImage::new(
            target.clone(),
            plan.contract_version(),
            payload_record(entity_payload_bytes),
        )?);
        let runtime_events = event_payload_bytes
            .iter()
            .map(|length| {
                EventIntent::new(
                    EventTypeId::new(1).expect("event type"),
                    payload_record(*length),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let declared_outcome =
            DeclaredOutcome::new(OutcomeId::new(1).expect("outcome"), payload_record(0))?;
        let evaluated = EvaluatedCommand::new(
            &snapshot,
            vec![runtime_mutation],
            runtime_events,
            declared_outcome.clone(),
            EvaluationBudget::v1(),
        )?;
        let partition_hash = hash_partition_key(partition.as_bytes());
        let context = PreEvaluationCommitContext::new(pending.clone(), partition_hash, Vec::new())?;
        let intent = CommitIntent::new(context, evaluated, provenance_id)?;

        let read_dependencies = ReadDependencies::new([ReadDependency::EntityObservation {
            target: target.clone(),
            expected: ExpectedEntityState::Absent,
        }])?;
        let stored_read_dependencies = StoredReadDependenciesV1::from_live(&read_dependencies)?;
        let entity = StoredEntityRecordV1::new(
            target,
            EntityVersion::first(),
            plan.contract_version(),
            DurableKeySchemaBindingV1::from_plan(&plan),
            payload_record(entity_payload_bytes),
        )?;
        let mutation = CommittedEntityMutationV1::new(ExpectedEntityState::Absent, entity)?;
        let mutations = vec![mutation];

        let events = event_payload_bytes
            .iter()
            .enumerate()
            .map(|(ordinal, length)| {
                let ordinal = u32::try_from(ordinal).expect("bounded event count");
                let event_id = EventId::new(sequence, ordinal);
                let event_type_id = EventTypeId::new(1).expect("event type");
                let payload = payload_record(*length);
                let event_hash = derive_event_hash_v1(event_id, event_type_id, &payload)
                    .expect("canonical event hash");
                StoredDurableEventV1::new(event_id, event_type_id, payload, event_hash)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let event_ids = events
            .iter()
            .map(StoredDurableEventV1::event_id)
            .collect::<Vec<_>>();
        let stored_outcome = StoredOutcomeV1::new(
            identity.clone(),
            sequence,
            request_id,
            plan.clone(),
            CanonicalInputHash::from_bytes([0x32; 32]),
            actor.clone(),
            logical_time,
            partition.clone(),
            partition_hash,
            Vec::new(),
            declared_outcome.clone(),
            StoredAdmittedProvenanceClaimsV1::default(),
            provenance_id,
            DurabilityMode::Memory,
        )?;
        let affected = mutations
            .iter()
            .map(AffectedEntityV1::from_mutation)
            .collect();
        let provenance = StoredProvenanceRecordV1::new(
            provenance_id,
            sequence,
            identity,
            request_id,
            plan.clone(),
            CanonicalInputHash::from_bytes([0x32; 32]),
            actor.clone(),
            logical_time,
            partition_hash,
            Vec::new(),
            declared_outcome.outcome_id(),
            affected,
            event_ids.clone(),
            StoredAdmittedProvenanceClaimsV1::default(),
        )?;
        let entity_references = mutations
            .iter()
            .map(CommittedEntityReferenceV2::from_mutation)
            .collect::<Result<Vec<_>, _>>()?;
        let commit = StoredCommitRecordV1::new(
            sequence,
            request_id,
            plan.clone(),
            CanonicalInputHash::from_bytes([0x32; 32]),
            actor,
            logical_time,
            partition_hash,
            Vec::new(),
            stored_read_dependencies,
            entity_references,
            events,
            declared_outcome,
            provenance_id,
            event_ids,
            DurabilityMode::Memory,
        )?;
        let affected_targets = AffectedIndexEpochTargets::new(Vec::new())?;
        let affected_current = AffectedEpochCurrentState::new(&affected_targets, Vec::new())?;
        let shape = ValidatedCommandWriteSetShapeV1::new(
            &intent,
            affected_targets,
            affected_current,
            Vec::new(),
            Vec::new(),
        )?;
        let encoded_upper_bound = EncodedWriteSetUpperBound::new(shape.semantic_classes())?;
        let write_plan = CommandWriteSetPlanV1::from_validated_shape(shape, encoded_upper_bound);
        AtomicCommandRecordSet::new(
            AssignedCommandSequence::from_assigned(sequence),
            mutations,
            write_plan,
            stored_outcome,
            provenance,
            commit,
        )
    }

    #[test]
    fn event_hash_v1_freezes_the_complete_identity_type_and_payload_preimage() {
        let event_id = EventId::new(CommitSequence::first(), 0);
        let event_type_id = EventTypeId::new(1).expect("event type");
        let payload = CanonicalRecord::new(Vec::new()).expect("empty record");
        assert_eq!(
            canonical_event_preimage_v1(event_id, event_type_id, &payload)
                .expect("canonical preimage"),
            vec![
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // commit sequence
                0x00, 0x00, 0x00, 0x00, // event ordinal
                0x00, 0x00, 0x00, 0x01, // event type
                0x00, 0x00, 0x00, 0x06, // complete canonical payload length
                0x01, 0x0d, 0x00, 0x00, 0x00, 0x00, // empty Value::Record
            ]
        );
        let expected = EventHash::from_bytes([
            0xe5, 0xd1, 0x5c, 0x17, 0xd9, 0x67, 0xed, 0x14, 0xa4, 0xd0, 0xa9, 0xc4, 0x6e, 0x42,
            0xc3, 0x64, 0xf5, 0xbb, 0x9f, 0xce, 0x20, 0x73, 0xff, 0x06, 0x0f, 0x48, 0xcd, 0xba,
            0x27, 0xc8, 0x56, 0x3d,
        ]);

        assert_eq!(
            derive_event_hash_v1(event_id, event_type_id, &payload),
            Ok(expected)
        );
        assert!(
            StoredDurableEventV1::new(event_id, event_type_id, payload.clone(), expected).is_ok()
        );
        assert_eq!(
            StoredDurableEventV1::new(
                event_id,
                event_type_id,
                payload.clone(),
                EventHash::from_bytes([0; 32]),
            ),
            Err(StorageValueError::IdentityMismatch)
        );
        assert_ne!(
            derive_event_hash_v1(
                EventId::new(CommitSequence::first(), 1),
                event_type_id,
                &payload
            )
            .expect("ordinal hash"),
            expected
        );
        assert_ne!(
            derive_event_hash_v1(
                EventId::new(CommitSequence::new(2).expect("second sequence"), 0),
                event_type_id,
                &payload
            )
            .expect("commit-sequence hash"),
            expected
        );
        assert_ne!(
            derive_event_hash_v1(
                event_id,
                EventTypeId::new(2).expect("second event type"),
                &payload,
            )
            .expect("type hash"),
            expected
        );
        assert_ne!(
            derive_event_hash_v1(event_id, event_type_id, &payload_record(0))
                .expect("payload hash"),
            expected
        );
    }

    #[test]
    fn event_hash_v1_has_a_nonempty_record_golden() {
        let event_id = EventId::new(CommitSequence::first(), 0);
        let event_type_id = EventTypeId::new(1).expect("event type");
        let payload = payload_record(1);
        let expected = EventHash::from_bytes([
            0xa9, 0x77, 0xa3, 0xe8, 0xa4, 0x33, 0xcf, 0x89, 0xda, 0xf5, 0x69, 0x58, 0xfd, 0xc7,
            0x1c, 0xbb, 0xe2, 0xa5, 0x5d, 0xf8, 0xee, 0x08, 0x2f, 0x7e, 0xe8, 0x94, 0xbd, 0xbf,
            0x65, 0x47, 0x32, 0x14,
        ]);

        assert_eq!(
            derive_event_hash_v1(event_id, event_type_id, &payload),
            Ok(expected)
        );
    }

    #[test]
    fn event_hash_v1_accepts_exact_payload_maximum_and_classifies_one_over_as_limit() {
        const RECORD_OVERHEAD: usize = 16;
        let event_id = EventId::new(CommitSequence::first(), 0);
        let event_type_id = EventTypeId::new(1).expect("event type");
        let exact = payload_record(MAX_CANONICAL_DOCUMENT_BYTES - RECORD_OVERHEAD);
        assert!(derive_event_hash_v1(event_id, event_type_id, &exact).is_ok());

        let over = payload_record(MAX_CANONICAL_DOCUMENT_BYTES - RECORD_OVERHEAD + 1);
        assert_eq!(
            derive_event_hash_v1(event_id, event_type_id, &over),
            Err(StorageValueError::LimitExceeded)
        );
        assert_eq!(
            StoredDurableEventV1::new(
                event_id,
                event_type_id,
                over,
                EventHash::from_bytes([0; 32]),
            ),
            Err(StorageValueError::LimitExceeded)
        );
    }

    fn ordered_conflict_hashes(count: usize) -> Vec<ConflictKeyHash> {
        (0..count)
            .map(|value| {
                let mut bytes = [0_u8; 32];
                bytes[..4].copy_from_slice(
                    &u32::try_from(value)
                        .expect("test conflict count fits u32")
                        .to_be_bytes(),
                );
                ConflictKeyHash::from_bytes(bytes)
            })
            .collect()
    }

    fn outcome_with_conflicts(
        template: &StoredOutcomeV1,
        conflict_hashes: Vec<ConflictKeyHash>,
    ) -> Result<StoredOutcomeV1, StorageValueError> {
        StoredOutcomeV1::new(
            template.identity.clone(),
            template.commit_sequence,
            template.admission_request_id,
            template.plan.clone(),
            template.canonical_input_hash,
            template.actor.clone(),
            template.logical_time,
            template.partition_key.clone(),
            template.partition_hash,
            conflict_hashes,
            template.declared_outcome.clone(),
            template.admitted_claims.clone(),
            template.provenance_id,
            template.durability_mode,
        )
    }

    fn outcome_with_claims(
        template: &StoredOutcomeV1,
        admitted_claims: StoredAdmittedProvenanceClaimsV1,
    ) -> Result<StoredOutcomeV1, StorageValueError> {
        StoredOutcomeV1::new(
            template.identity.clone(),
            template.commit_sequence,
            template.admission_request_id,
            template.plan.clone(),
            template.canonical_input_hash,
            template.actor.clone(),
            template.logical_time,
            template.partition_key.clone(),
            template.partition_hash,
            template.conflict_hashes.clone(),
            template.declared_outcome.clone(),
            admitted_claims,
            template.provenance_id,
            template.durability_mode,
        )
    }

    #[test]
    fn terminal_outcome_claims_must_match_pending_admission_and_provenance() {
        let records = atomic_record_set(1, &[]).expect("valid record graph");
        let claims = StoredAdmittedProvenanceClaimsV1::new(
            None,
            None,
            None,
            Some(ApprovalId::new("approval-2").expect("approval ID")),
        )
        .expect("admitted claims");
        let mismatched_outcome = outcome_with_claims(records.stored_outcome(), claims.clone())
            .expect("structurally valid outcome");
        assert_eq!(mismatched_outcome.admitted_claims(), &claims);

        assert_eq!(
            AtomicCommandRecordSet::new(
                records.assignment(),
                records.entities().to_vec(),
                records.write_plan().clone(),
                mismatched_outcome,
                records.provenance().clone(),
                records.commit().clone(),
            ),
            Err(StorageValueError::IdentityMismatch)
        );

        let mismatched_provenance = provenance_with_claims(records.provenance(), claims)
            .expect("structurally valid provenance");
        assert_eq!(
            AtomicCommandRecordSet::new(
                records.assignment(),
                records.entities().to_vec(),
                records.write_plan().clone(),
                records.stored_outcome().clone(),
                mismatched_provenance,
                records.commit().clone(),
            ),
            Err(StorageValueError::IdentityMismatch)
        );
    }

    fn provenance_with_conflicts(
        template: &StoredProvenanceRecordV1,
        conflict_hashes: Vec<ConflictKeyHash>,
    ) -> Result<StoredProvenanceRecordV1, StorageValueError> {
        StoredProvenanceRecordV1::new(
            template.provenance_id,
            template.commit_sequence,
            template.identity.clone(),
            template.admission_request_id,
            template.plan.clone(),
            template.canonical_input_hash,
            template.actor.clone(),
            template.logical_time,
            template.partition_hash,
            conflict_hashes,
            template.outcome_id,
            template.affected_entities.clone(),
            template.event_ids.clone(),
            template.admitted_claims.clone(),
        )
    }

    fn provenance_with_claims(
        template: &StoredProvenanceRecordV1,
        admitted_claims: StoredAdmittedProvenanceClaimsV1,
    ) -> Result<StoredProvenanceRecordV1, StorageValueError> {
        StoredProvenanceRecordV1::new(
            template.provenance_id,
            template.commit_sequence,
            template.identity.clone(),
            template.admission_request_id,
            template.plan.clone(),
            template.canonical_input_hash,
            template.actor.clone(),
            template.logical_time,
            template.partition_hash,
            template.conflict_hashes.clone(),
            template.outcome_id,
            template.affected_entities.clone(),
            template.event_ids.clone(),
            admitted_claims,
        )
    }

    fn commit_with_conflicts(
        template: &StoredCommitRecordV1,
        conflict_hashes: Vec<ConflictKeyHash>,
    ) -> Result<StoredCommitRecordV1, StorageValueError> {
        StoredCommitRecordV1::new(
            template.commit_sequence,
            template.admission_request_id,
            template.plan.clone(),
            template.canonical_input_hash,
            template.actor.clone(),
            template.logical_time,
            template.partition_hash,
            conflict_hashes,
            template.read_dependencies.clone(),
            template.entity_references.clone(),
            template.events.to_vec(),
            template.declared_outcome.clone(),
            template.provenance_id,
            template.outbox_event_ids.clone(),
            template.durability_mode,
        )
    }

    #[test]
    fn stored_outcome_conflict_hash_count_accepts_2046_and_rejects_2047() {
        let records = atomic_record_set(1, &[]).expect("valid record graph");
        let exact = outcome_with_conflicts(
            records.stored_outcome(),
            ordered_conflict_hashes(MAX_COMMIT_CONFLICT_HASHES),
        )
        .expect("exact maximum");
        assert_eq!(exact.conflict_hashes().len(), MAX_COMMIT_CONFLICT_HASHES);
        assert_eq!(
            outcome_with_conflicts(
                records.stored_outcome(),
                ordered_conflict_hashes(MAX_COMMIT_CONFLICT_HASHES + 1),
            ),
            Err(StorageValueError::LimitExceeded)
        );
    }

    #[test]
    fn stored_provenance_conflict_hash_count_accepts_2046_and_rejects_2047() {
        let records = atomic_record_set(1, &[]).expect("valid record graph");
        let exact = provenance_with_conflicts(
            records.provenance(),
            ordered_conflict_hashes(MAX_COMMIT_CONFLICT_HASHES),
        )
        .expect("exact maximum");
        assert_eq!(exact.conflict_hashes().len(), MAX_COMMIT_CONFLICT_HASHES);
        assert_eq!(
            provenance_with_conflicts(
                records.provenance(),
                ordered_conflict_hashes(MAX_COMMIT_CONFLICT_HASHES + 1),
            ),
            Err(StorageValueError::LimitExceeded)
        );
    }

    #[test]
    fn stored_commit_conflict_hash_count_accepts_2046_and_rejects_2047() {
        let records = atomic_record_set(1, &[]).expect("valid record graph");
        let exact = commit_with_conflicts(
            records.commit(),
            ordered_conflict_hashes(MAX_COMMIT_CONFLICT_HASHES),
        )
        .expect("exact maximum");
        assert_eq!(exact.conflict_hashes().len(), MAX_COMMIT_CONFLICT_HASHES);
        assert_eq!(
            commit_with_conflicts(
                records.commit(),
                ordered_conflict_hashes(MAX_COMMIT_CONFLICT_HASHES + 1),
            ),
            Err(StorageValueError::LimitExceeded)
        );
    }

    #[test]
    fn staged_semantic_graph_counts_each_bounded_materialized_copy() {
        let baseline = atomic_record_set(10, &[10]).expect("baseline");
        let larger_entity = atomic_record_set(11, &[10]).expect("larger entity");
        let larger_event = atomic_record_set(10, &[11]).expect("larger event");
        // Entity field growth is charged once on the ENTITIES row; the commit
        // stores only a fixed-size post-image hash reference.
        assert_eq!(
            larger_entity.semantic_bytes() - baseline.semantic_bytes(),
            1
        );
        // Event payload growth is charged on the event row, outbox intent, and
        // commit event materialization path (three copies).
        assert_eq!(larger_event.semantic_bytes() - baseline.semantic_bytes(), 3);
    }

    #[test]
    fn cloned_write_plan_shares_the_sealed_immutable_graph() {
        let records = atomic_record_set(10, &[10]).expect("record graph");
        let cloned = records.write_plan.clone();

        assert!(Arc::ptr_eq(&records.write_plan.0, &cloned.0));
        assert_eq!(records.write_plan, cloned);
    }

    #[test]
    fn aggregate_staged_limit_rejects_exactly_one_charged_byte_over() {
        const BULK: usize = 900_000;
        let mut base_events = vec![BULK; 5];
        base_events.push(0);
        let base = atomic_record_set(0, &base_events).expect("base fits");
        let remaining = MAX_STAGED_WRITE_BYTES
            .checked_sub(base.semantic_bytes())
            .expect("five bulk events leave tuning room");
        drop(base);

        // Events still amplify three ways; entity fields amplify once (commit
        // references no longer embed post-image field bytes).
        let (event_bytes, entity_bytes) = (0..=BULK)
            .rev()
            .find_map(|event_bytes| {
                let event_charge = 3 * event_bytes;
                let entity_charge = remaining.checked_sub(event_charge)?;
                let entity_bytes = entity_charge;
                (entity_bytes <= BULK && entity_bytes > 0 && event_bytes < BULK)
                    .then_some((event_bytes, entity_bytes))
            })
            .expect("one- and three-copy payloads can tune the exact boundary");

        let mut exact_events = vec![BULK; 5];
        exact_events.push(event_bytes);
        let exact = atomic_record_set(entity_bytes, &exact_events).expect("exact limit fits");
        assert_eq!(exact.semantic_bytes(), MAX_STAGED_WRITE_BYTES);
        drop(exact);

        let mut over_events = vec![BULK; 5];
        over_events.push(event_bytes + 1);
        assert_eq!(
            atomic_record_set(entity_bytes.saturating_sub(1), &over_events),
            Err(StorageValueError::LimitExceeded)
        );
    }
}
