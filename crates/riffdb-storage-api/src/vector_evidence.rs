//! Authoritative, nonduplicating evidence for production vector fields.

use std::{collections::BTreeMap, fmt};

use riffdb_types::{
    CommitSequence, ContractLineage, EmbeddingMetadata, EntityKey, EntityTypeId, EntityVersion,
    FieldId, FrontierPosition, PartitionKey, ProjectionGeneration, ProvenanceId,
};

use crate::{
    DurableKeySchemaBindingV1, EntityTarget, ExecutablePlanRef, MAX_SCAN_PAGE_BYTES,
    MAX_SCAN_PAGE_ENTRIES, StorageError, StorageScanLimit, StorageValueError,
};

/// Maximum distinct live embedding model revisions retained in one logical
/// partition/field observation row.
///
/// Contract successors can introduce new versions, but a malformed workload
/// cannot grow one authoritative counter record without bound.
pub const MAX_VECTOR_MODELS_PER_OBSERVATION: usize = 256;

/// Maximum vector fields represented in one lineage-wide health observation.
///
/// The contract compiler is already bounded below this ceiling. Keeping the
/// durable summary independently bounded prevents a corrupt record from
/// turning an authenticated health probe into unbounded allocation.
pub const MAX_VECTOR_FIELDS_PER_HEALTH_OBSERVATION: usize = 256;

/// Durable lifecycle for one compiler-declared vector projection source.
///
/// Unlike the generic aggregate projection identity, this identity is the
/// stable `(lineage, entity, vector field)` tuple. A model or layout successor
/// therefore allocates a new generation under the same logical source instead
/// of inventing a colliding synthetic `ProjectionId`.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum VectorProjectionLifecycleV1 {
    /// No complete generation has been published yet.
    Building,
    /// One complete generation is queryable and owns a retention frontier.
    Ready,
    /// A replay limit was breached and the old retention frontier is detached.
    RebuildRequired,
    /// A bounded authoritative snapshot replacement is being assembled.
    Rebuilding,
    /// Durable control or rebuild input failed closed.
    Invalid,
}

impl VectorProjectionLifecycleV1 {
    /// Stable durable semantic tag.
    #[must_use]
    pub const fn tag(self) -> u8 {
        match self {
            Self::Building => 0x01,
            Self::Ready => 0x02,
            Self::RebuildRequired => 0x03,
            Self::Rebuilding => 0x04,
            Self::Invalid => 0x05,
        }
    }

    /// Decodes one stable durable semantic tag.
    #[must_use]
    pub const fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            0x01 => Some(Self::Building),
            0x02 => Some(Self::Ready),
            0x03 => Some(Self::RebuildRequired),
            0x04 => Some(Self::Rebuilding),
            0x05 => Some(Self::Invalid),
            _ => None,
        }
    }
}

/// Closed reason for a vector-projection rebuild.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum VectorProjectionRebuildReasonV1 {
    /// Retained commit age exceeded the compiler-sealed limit.
    ReplayAge,
    /// Retained encoded commit bytes exceeded the compiler-sealed limit.
    ReplayBytes,
    /// Head-to-durable sequence distance exceeded the compiler-sealed limit.
    ReplayBacklog,
    /// The exact model/layout definition changed under a successor contract.
    DefinitionChanged,
}

impl VectorProjectionRebuildReasonV1 {
    /// Stable durable semantic tag.
    #[must_use]
    pub const fn tag(self) -> u8 {
        match self {
            Self::ReplayAge => 0x01,
            Self::ReplayBytes => 0x02,
            Self::ReplayBacklog => 0x03,
            Self::DefinitionChanged => 0x04,
        }
    }

    /// Decodes one stable durable semantic tag.
    #[must_use]
    pub const fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            0x01 => Some(Self::ReplayAge),
            0x02 => Some(Self::ReplayBytes),
            0x03 => Some(Self::ReplayBacklog),
            0x04 => Some(Self::DefinitionChanged),
            _ => None,
        }
    }
}

/// Compiler-sealed positive replay limits persisted with vector control.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VectorProjectionReplayLimitsV1 {
    age_seconds: u64,
    bytes: u64,
    backlog: u64,
}

impl VectorProjectionReplayLimitsV1 {
    /// Constructs positive limits. Compiler maxima remain enforced by IR;
    /// storage independently refuses the unsafe zero sentinel.
    pub const fn new(age_seconds: u64, bytes: u64, backlog: u64) -> Option<Self> {
        if age_seconds == 0 || bytes == 0 || backlog == 0 {
            return None;
        }
        Some(Self {
            age_seconds,
            bytes,
            backlog,
        })
    }

    /// Maximum retained replay age.
    #[must_use]
    pub const fn age_seconds(self) -> u64 {
        self.age_seconds
    }

    /// Maximum retained encoded bytes.
    #[must_use]
    pub const fn bytes(self) -> u64 {
        self.bytes
    }

    /// Maximum retained sequence backlog.
    #[must_use]
    pub const fn backlog(self) -> u64 {
        self.backlog
    }
}

/// Pure-read port for one exact authoritative vector-observation row.
///
/// Callers supply a compiler-derived partition/field identity; there is no
/// unscoped scan or numeric application-facing selector on this boundary.
pub trait VectorObservationRepository {
    /// Reads the current published counts for one exact partitioned field.
    fn read_vector_observation(
        &self,
        target: &VectorObservationTargetV1,
    ) -> Result<Option<VectorObservationCountsV1>, crate::StorageError>;

    /// Reads the maintained lineage-wide health summary in one exact lookup.
    ///
    /// This is deliberately not expressible as a scan: operational health must
    /// remain bounded independently of entity and partition cardinality.
    fn read_vector_health_observation(
        &self,
        lineage: &ContractLineage,
    ) -> Result<Option<VectorHealthObservationV1>, crate::StorageError>;
}

/// Pure-read port for one exact partition-ordered authoritative evidence index.
///
/// This lower boundary is deliberately policy-neutral. The application service
/// must apply the compiler-derived row policy before releasing any entry or
/// minting a public continuation.
pub trait VectorEvidenceIndexRepository {
    /// Reads one bounded ascending page from the exact declared partition/field.
    fn scan_vector_evidence_index(
        &self,
        request: &VectorEvidenceIndexScanRequestV1,
    ) -> Result<VectorEvidenceIndexPageV1, StorageError>;
}

/// One bounded ascending scan request over a symbolic service-resolved target.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VectorEvidenceIndexScanRequestV1 {
    target: VectorObservationTargetV1,
    after: Option<EntityKey>,
    limit: StorageScanLimit,
}

impl VectorEvidenceIndexScanRequestV1 {
    /// Checks continuation identity and retains the fixed storage scan bound.
    pub fn new(
        target: VectorObservationTargetV1,
        after: Option<EntityKey>,
        limit: StorageScanLimit,
    ) -> Result<Self, StorageValueError> {
        if after
            .as_ref()
            .is_some_and(|key| key.entity_type_id() != target.entity_type())
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        Ok(Self {
            target,
            after,
            limit,
        })
    }

    /// Exact partitioned vector-field identity.
    #[must_use]
    pub const fn target(&self) -> &VectorObservationTargetV1 {
        &self.target
    }

    /// Exclusive entity-key continuation.
    #[must_use]
    pub const fn after(&self) -> Option<&EntityKey> {
        self.after.as_ref()
    }

    /// Checked maximum returned rows.
    #[must_use]
    pub const fn limit(&self) -> StorageScanLimit {
        self.limit
    }
}

/// One exact bounded page from the authoritative reciprocal evidence index.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VectorEvidenceIndexPageV1 {
    entries: Vec<VectorEvidenceIndexEntryV1>,
    continuation: Option<EntityKey>,
    exact_end: bool,
    encoded_bytes: usize,
}

impl VectorEvidenceIndexPageV1 {
    /// Checks target reciprocity, canonical order, row/byte bounds, and exact-end shape.
    pub fn new(
        target: &VectorObservationTargetV1,
        entries: Vec<VectorEvidenceIndexEntryV1>,
        continuation: Option<EntityKey>,
        exact_end: bool,
        encoded_bytes: usize,
    ) -> Result<Self, StorageValueError> {
        if entries.len() > MAX_SCAN_PAGE_ENTRIES || encoded_bytes > MAX_SCAN_PAGE_BYTES {
            return Err(StorageValueError::LimitExceeded);
        }
        let continuation_valid = match (continuation.as_ref(), entries.last()) {
            (None, _) => exact_end,
            (Some(continuation), Some(last)) => !exact_end && continuation == last.entity_key(),
            (Some(_), None) => false,
        };
        if entries.iter().any(|entry| entry.target() != target)
            || entries
                .windows(2)
                .any(|pair| pair[0].entity_key() >= pair[1].entity_key())
            || !continuation_valid
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        Ok(Self {
            entries,
            continuation,
            exact_end,
            encoded_bytes,
        })
    }

    /// Canonically ordered index rows.
    #[must_use]
    pub fn entries(&self) -> &[VectorEvidenceIndexEntryV1] {
        &self.entries
    }

    /// Exclusive continuation for the next lower page.
    #[must_use]
    pub const fn continuation(&self) -> Option<&EntityKey> {
        self.continuation.as_ref()
    }

    /// Whether this page proved exact end of the target prefix.
    #[must_use]
    pub const fn exact_end(&self) -> bool {
        self.exact_end
    }

    /// Exact retained envelope bytes decoded for returned rows.
    #[must_use]
    pub const fn encoded_bytes(&self) -> usize {
        self.encoded_bytes
    }
}

/// Stable logical identity for maintained vector counts and ordered indexes.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct VectorObservationTargetV1 {
    lineage: ContractLineage,
    partition_key: PartitionKey,
    entity_type: EntityTypeId,
    vector_field: FieldId,
}

/// Stable logical identity of one compiler-declared vector projection.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct VectorProjectionSourceV1 {
    lineage: ContractLineage,
    entity_type: EntityTypeId,
    vector_field: FieldId,
}

impl VectorProjectionSourceV1 {
    /// Constructs a source identity from compiler-owned stable IDs.
    #[must_use]
    pub const fn new(
        lineage: ContractLineage,
        entity_type: EntityTypeId,
        vector_field: FieldId,
    ) -> Self {
        Self {
            lineage,
            entity_type,
            vector_field,
        }
    }

    /// Contract lineage owning this projection.
    #[must_use]
    pub const fn lineage(&self) -> &ContractLineage {
        &self.lineage
    }

    /// Compiler-assigned entity identity.
    #[must_use]
    pub const fn entity_type(&self) -> EntityTypeId {
        self.entity_type
    }

    /// Compiler-assigned vector field identity.
    #[must_use]
    pub const fn vector_field(&self) -> FieldId {
        self.vector_field
    }
}

/// Durable control for one vector projection and its current generation.
///
/// `Ready` is the only lifecycle whose frontier participates in retention.
/// Every rebuild state is therefore detached by construction rather than by a
/// second fallible administration write.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredVectorProjectionControlV1 {
    source: VectorProjectionSourceV1,
    generation: ProjectionGeneration,
    definition_fingerprint: [u8; 32],
    lifecycle: VectorProjectionLifecycleV1,
    published_frontier: FrontierPosition,
    rebuild_snapshot_frontier: Option<FrontierPosition>,
    rebuild_reason: Option<VectorProjectionRebuildReasonV1>,
    limits: VectorProjectionReplayLimitsV1,
}

impl StoredVectorProjectionControlV1 {
    /// Constructs and validates one complete durable control record.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        source: VectorProjectionSourceV1,
        generation: ProjectionGeneration,
        definition_fingerprint: [u8; 32],
        lifecycle: VectorProjectionLifecycleV1,
        published_frontier: FrontierPosition,
        rebuild_snapshot_frontier: Option<FrontierPosition>,
        rebuild_reason: Option<VectorProjectionRebuildReasonV1>,
        limits: VectorProjectionReplayLimitsV1,
    ) -> Result<Self, StorageValueError> {
        let rebuild = matches!(
            lifecycle,
            VectorProjectionLifecycleV1::RebuildRequired | VectorProjectionLifecycleV1::Rebuilding
        );
        if rebuild != rebuild_reason.is_some()
            || (lifecycle == VectorProjectionLifecycleV1::Rebuilding)
                != rebuild_snapshot_frontier.is_some()
            || lifecycle == VectorProjectionLifecycleV1::Building
                && published_frontier != FrontierPosition::BeforeFirst
        {
            return Err(StorageValueError::InvalidShape);
        }
        Ok(Self {
            source,
            generation,
            definition_fingerprint,
            lifecycle,
            published_frontier,
            rebuild_snapshot_frontier,
            rebuild_reason,
            limits,
        })
    }

    /// Creates the first detached, unpublished generation.
    #[must_use]
    pub fn initial(
        source: VectorProjectionSourceV1,
        definition_fingerprint: [u8; 32],
        limits: VectorProjectionReplayLimitsV1,
    ) -> Self {
        Self {
            source,
            generation: ProjectionGeneration::first(),
            definition_fingerprint,
            lifecycle: VectorProjectionLifecycleV1::Building,
            published_frontier: FrontierPosition::BeforeFirst,
            rebuild_snapshot_frontier: None,
            rebuild_reason: None,
            limits,
        }
    }

    /// Stable logical source.
    #[must_use]
    pub const fn source(&self) -> &VectorProjectionSourceV1 {
        &self.source
    }

    /// Never-reused generation.
    #[must_use]
    pub const fn generation(&self) -> ProjectionGeneration {
        self.generation
    }

    /// Exact derived-layout/model fingerprint.
    #[must_use]
    pub const fn definition_fingerprint(&self) -> &[u8; 32] {
        &self.definition_fingerprint
    }

    /// Current durable lifecycle.
    #[must_use]
    pub const fn lifecycle(&self) -> VectorProjectionLifecycleV1 {
        self.lifecycle
    }

    /// Last completely published frontier. It never advances during rebuild.
    #[must_use]
    pub const fn published_frontier(&self) -> FrontierPosition {
        self.published_frontier
    }

    /// Stable snapshot frontier selected for a rebuilding generation.
    #[must_use]
    pub const fn rebuild_snapshot_frontier(&self) -> Option<FrontierPosition> {
        self.rebuild_snapshot_frontier
    }

    /// Closed rebuild reason, present only in rebuild states.
    #[must_use]
    pub const fn rebuild_reason(&self) -> Option<VectorProjectionRebuildReasonV1> {
        self.rebuild_reason
    }

    /// Compiler-sealed replay limits.
    #[must_use]
    pub const fn limits(&self) -> VectorProjectionReplayLimitsV1 {
        self.limits
    }

    /// Whether this control contributes its frontier to retention.
    #[must_use]
    pub const fn retention_attached(&self) -> bool {
        matches!(self.lifecycle, VectorProjectionLifecycleV1::Ready)
    }
}

/// Compare-and-set result for one durable vector-control transition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VectorProjectionControlWriteResultV1 {
    /// Replacement was durably applied.
    Applied,
    /// Current state differed from the caller's exact expected state.
    CompareMismatch,
    /// Current state already equals the requested replacement.
    Unchanged,
}

/// Durable control repository. Implementations must compare and replace in one
/// transaction so detachment and `RebuildRequired` are one fact.
pub trait VectorProjectionControlRepository {
    /// Reads one exact logical source.
    fn read_vector_projection_control(
        &self,
        source: &VectorProjectionSourceV1,
    ) -> Result<Option<StoredVectorProjectionControlV1>, StorageError>;

    /// Atomically compares and replaces one exact logical source.
    fn compare_and_set_vector_projection_control(
        &self,
        expected: Option<&StoredVectorProjectionControlV1>,
        replacement: &StoredVectorProjectionControlV1,
    ) -> Result<VectorProjectionControlWriteResultV1, StorageError>;

    /// Returns every attached control frontier for retention calculation.
    fn attached_vector_projection_frontiers(&self) -> Result<Vec<FrontierPosition>, StorageError>;
}

impl VectorObservationTargetV1 {
    /// Constructs the stable cross-version identity for one partitioned vector
    /// field.
    #[must_use]
    pub const fn new(
        lineage: ContractLineage,
        partition_key: PartitionKey,
        entity_type: EntityTypeId,
        vector_field: FieldId,
    ) -> Self {
        Self {
            lineage,
            partition_key,
            entity_type,
            vector_field,
        }
    }

    /// Borrows the application contract lineage.
    #[must_use]
    pub const fn lineage(&self) -> &ContractLineage {
        &self.lineage
    }

    /// Borrows the exact logical partition.
    #[must_use]
    pub const fn partition_key(&self) -> &PartitionKey {
        &self.partition_key
    }

    /// Returns the stable entity type.
    #[must_use]
    pub const fn entity_type(&self) -> EntityTypeId {
        self.entity_type
    }

    /// Returns the stable vector field.
    #[must_use]
    pub const fn vector_field(&self) -> FieldId {
        self.vector_field
    }
}

/// Canonical authoritative counters for one partitioned vector field.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VectorObservationCountsV1 {
    target: VectorObservationTargetV1,
    total_entities: u64,
    source_stale_entities: u64,
    model_counts: BTreeMap<EmbeddingMetadata, u64>,
    revision: CommitSequence,
}

impl VectorObservationCountsV1 {
    /// Reconstructs one persisted canonical observation row.
    pub fn from_parts(
        target: VectorObservationTargetV1,
        total_entities: u64,
        source_stale_entities: u64,
        model_counts: Vec<(EmbeddingMetadata, u64)>,
        revision: CommitSequence,
    ) -> Result<Self, StorageValueError> {
        if source_stale_entities > total_entities
            || model_counts.len() > MAX_VECTOR_MODELS_PER_OBSERVATION
            || model_counts.iter().any(|(_, count)| *count == 0)
            || model_counts.windows(2).any(|pair| pair[0].0 >= pair[1].0)
        {
            return Err(StorageValueError::InvalidShape);
        }
        let model_total = model_counts.iter().try_fold(0_u64, |total, (_, count)| {
            total
                .checked_add(*count)
                .ok_or(StorageValueError::SizeOverflow)
        })?;
        if model_total > total_entities {
            return Err(StorageValueError::InvalidShape);
        }
        Ok(Self {
            target,
            total_entities,
            source_stale_entities,
            model_counts: model_counts.into_iter().collect(),
            revision,
        })
    }

    /// Creates the first empty observation state immediately before applying a
    /// transition at `revision`.
    #[must_use]
    pub const fn empty(target: VectorObservationTargetV1, revision: CommitSequence) -> Self {
        Self {
            target,
            total_entities: 0,
            source_stale_entities: 0,
            model_counts: BTreeMap::new(),
            revision,
        }
    }

    /// Applies one exact before/after evidence classification.
    pub fn apply(
        &mut self,
        transition: &VectorEvidenceClassificationTransitionV1,
        revision: CommitSequence,
    ) -> Result<(), StorageValueError> {
        if revision < self.revision {
            return Err(StorageValueError::InvalidShape);
        }
        if let Some(prior) = transition.prior() {
            self.remove(prior)?;
        }
        if let Some(successor) = transition.successor() {
            self.insert(successor)?;
        }
        if self.source_stale_entities > self.total_entities
            || self.model_counts.values().any(|count| *count == 0)
            || self.model_counts.len() > MAX_VECTOR_MODELS_PER_OBSERVATION
        {
            return Err(StorageValueError::InvalidShape);
        }
        self.revision = revision;
        Ok(())
    }

    fn remove(
        &mut self,
        classification: &VectorEvidenceClassificationV1,
    ) -> Result<(), StorageValueError> {
        self.total_entities = self
            .total_entities
            .checked_sub(1)
            .ok_or(StorageValueError::InvalidShape)?;
        if classification.source_stale() {
            self.source_stale_entities = self
                .source_stale_entities
                .checked_sub(1)
                .ok_or(StorageValueError::InvalidShape)?;
        }
        if let Some(embedding) = classification.embedding() {
            let metadata = embedding.metadata();
            let count = self
                .model_counts
                .get_mut(metadata)
                .ok_or(StorageValueError::InvalidShape)?;
            *count = count
                .checked_sub(1)
                .ok_or(StorageValueError::InvalidShape)?;
            if *count == 0 {
                self.model_counts.remove(metadata);
            }
        }
        Ok(())
    }

    fn insert(
        &mut self,
        classification: &VectorEvidenceClassificationV1,
    ) -> Result<(), StorageValueError> {
        self.total_entities = self
            .total_entities
            .checked_add(1)
            .ok_or(StorageValueError::SizeOverflow)?;
        if classification.source_stale() {
            self.source_stale_entities = self
                .source_stale_entities
                .checked_add(1)
                .ok_or(StorageValueError::SizeOverflow)?;
        }
        if let Some(embedding) = classification.embedding() {
            if !self.model_counts.contains_key(embedding.metadata())
                && self.model_counts.len() == MAX_VECTOR_MODELS_PER_OBSERVATION
            {
                return Err(StorageValueError::LimitExceeded);
            }
            let count = self
                .model_counts
                .entry(embedding.metadata().clone())
                .or_default();
            *count = count
                .checked_add(1)
                .ok_or(StorageValueError::SizeOverflow)?;
        }
        Ok(())
    }

    /// Borrows the stable partition/field identity.
    #[must_use]
    pub const fn target(&self) -> &VectorObservationTargetV1 {
        &self.target
    }

    /// Returns the total live evidence rows.
    #[must_use]
    pub const fn total_entities(&self) -> u64 {
        self.total_entities
    }

    /// Returns the source-stale evidence rows, including missing embeddings.
    #[must_use]
    pub const fn source_stale_entities(&self) -> u64 {
        self.source_stale_entities
    }

    /// Returns the count carrying one exact model identity/version.
    #[must_use]
    pub fn model_count(&self, metadata: &EmbeddingMetadata) -> u64 {
        self.model_counts.get(metadata).copied().unwrap_or(0)
    }

    /// Iterates exact model counts in canonical metadata order.
    pub fn model_counts(&self) -> impl ExactSizeIterator<Item = (&EmbeddingMetadata, u64)> {
        self.model_counts
            .iter()
            .map(|(metadata, count)| (metadata, *count))
    }

    /// Returns the last command sequence incorporated into these counts.
    #[must_use]
    pub const fn revision(&self) -> CommitSequence {
        self.revision
    }
}

/// Canonical lineage-wide partition/SLO counts for one vector field.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VectorHealthFieldObservationV1 {
    entity_type: EntityTypeId,
    vector_field: FieldId,
    stale_entity_count_threshold: u64,
    partition_count: u64,
    breached_partition_count: u64,
}

impl VectorHealthFieldObservationV1 {
    /// Reconstructs one checked durable field summary.
    pub fn from_parts(
        entity_type: EntityTypeId,
        vector_field: FieldId,
        stale_entity_count_threshold: u64,
        partition_count: u64,
        breached_partition_count: u64,
    ) -> Result<Self, StorageValueError> {
        if stale_entity_count_threshold == 0 || breached_partition_count > partition_count {
            return Err(StorageValueError::InvalidShape);
        }
        Ok(Self {
            entity_type,
            vector_field,
            stale_entity_count_threshold,
            partition_count,
            breached_partition_count,
        })
    }

    /// Stable entity identity.
    #[must_use]
    pub const fn entity_type(&self) -> EntityTypeId {
        self.entity_type
    }

    /// Stable vector-field identity.
    #[must_use]
    pub const fn vector_field(&self) -> FieldId {
        self.vector_field
    }

    /// Contract-owned strict stale-entity threshold.
    #[must_use]
    pub const fn stale_entity_count_threshold(&self) -> u64 {
        self.stale_entity_count_threshold
    }

    /// Number of nonempty logical partitions for this field.
    #[must_use]
    pub const fn partition_count(&self) -> u64 {
        self.partition_count
    }

    /// Number of partitions strictly over the declared threshold.
    #[must_use]
    pub const fn breached_partition_count(&self) -> u64 {
        self.breached_partition_count
    }

    const fn is_breached(&self) -> bool {
        self.breached_partition_count != 0
    }
}

/// Authoritative bounded health observation for every populated vector field
/// in one contract lineage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VectorHealthObservationV1 {
    lineage: ContractLineage,
    fields: BTreeMap<(EntityTypeId, FieldId), VectorHealthFieldObservationV1>,
    revision: CommitSequence,
}

impl VectorHealthObservationV1 {
    /// Reconstructs a canonical persisted health observation.
    pub fn from_parts(
        lineage: ContractLineage,
        fields: Vec<VectorHealthFieldObservationV1>,
        revision: CommitSequence,
    ) -> Result<Self, StorageValueError> {
        if fields.len() > MAX_VECTOR_FIELDS_PER_HEALTH_OBSERVATION
            || fields.windows(2).any(|pair| {
                (pair[0].entity_type(), pair[0].vector_field())
                    >= (pair[1].entity_type(), pair[1].vector_field())
            })
        {
            return Err(StorageValueError::InvalidShape);
        }
        Ok(Self {
            lineage,
            fields: fields
                .into_iter()
                .map(|field| ((field.entity_type(), field.vector_field()), field))
                .collect(),
            revision,
        })
    }

    /// Creates the first empty summary immediately before one vector transition.
    #[must_use]
    pub const fn empty(lineage: ContractLineage, revision: CommitSequence) -> Self {
        Self {
            lineage,
            fields: BTreeMap::new(),
            revision,
        }
    }

    /// Applies one partition observation transition under the compiler-owned threshold.
    pub fn apply_partition(
        &mut self,
        entity_type: EntityTypeId,
        vector_field: FieldId,
        stale_entity_count_threshold: u64,
        prior: Option<&VectorObservationCountsV1>,
        successor: Option<&VectorObservationCountsV1>,
        revision: CommitSequence,
    ) -> Result<(), StorageValueError> {
        if stale_entity_count_threshold == 0 || revision < self.revision {
            return Err(StorageValueError::InvalidShape);
        }
        let key = (entity_type, vector_field);
        if prior.is_some_and(|observation| {
            observation.target().lineage() != &self.lineage
                || observation.target().entity_type() != entity_type
                || observation.target().vector_field() != vector_field
        }) || successor.is_some_and(|observation| {
            observation.target().lineage() != &self.lineage
                || observation.target().entity_type() != entity_type
                || observation.target().vector_field() != vector_field
        }) {
            return Err(StorageValueError::IdentityMismatch);
        }
        if !self.fields.contains_key(&key) {
            if prior.is_some_and(|observation| observation.total_entities() != 0)
                || self.fields.len() == MAX_VECTOR_FIELDS_PER_HEALTH_OBSERVATION
            {
                return Err(StorageValueError::InvalidShape);
            }
            self.fields.insert(
                key,
                VectorHealthFieldObservationV1::from_parts(
                    entity_type,
                    vector_field,
                    stale_entity_count_threshold,
                    0,
                    0,
                )?,
            );
        }
        let field = self
            .fields
            .get_mut(&key)
            .ok_or(StorageValueError::InvalidShape)?;
        if field.stale_entity_count_threshold != stale_entity_count_threshold {
            return Err(StorageValueError::IdentityMismatch);
        }

        let prior_present = prior.is_some_and(|value| value.total_entities() != 0);
        let prior_breached = prior.is_some_and(|value| {
            value.total_entities() != 0
                && value.source_stale_entities() > stale_entity_count_threshold
        });
        let successor_present = successor.is_some_and(|value| value.total_entities() != 0);
        let successor_breached = successor.is_some_and(|value| {
            value.total_entities() != 0
                && value.source_stale_entities() > stale_entity_count_threshold
        });

        field.partition_count =
            apply_boolean_delta(field.partition_count, prior_present, successor_present)?;
        field.breached_partition_count = apply_boolean_delta(
            field.breached_partition_count,
            prior_breached,
            successor_breached,
        )?;
        if field.breached_partition_count > field.partition_count {
            return Err(StorageValueError::InvalidShape);
        }
        self.revision = revision;
        Ok(())
    }

    /// Contract lineage covered by this observation.
    #[must_use]
    pub const fn lineage(&self) -> &ContractLineage {
        &self.lineage
    }

    /// Canonically ordered field summaries.
    pub fn fields(&self) -> impl ExactSizeIterator<Item = &VectorHealthFieldObservationV1> {
        self.fields.values()
    }

    /// Last vector-affecting command incorporated into the summary.
    #[must_use]
    pub const fn revision(&self) -> CommitSequence {
        self.revision
    }

    /// Whether any populated partition is strictly over its field threshold.
    #[must_use]
    pub fn any_partition_breached(&self) -> bool {
        self.fields
            .values()
            .any(VectorHealthFieldObservationV1::is_breached)
    }
}

fn apply_boolean_delta(
    current: u64,
    prior: bool,
    successor: bool,
) -> Result<u64, StorageValueError> {
    match (prior, successor) {
        (false, true) => current
            .checked_add(1)
            .ok_or(StorageValueError::SizeOverflow),
        (true, false) => current
            .checked_sub(1)
            .ok_or(StorageValueError::InvalidShape),
        _ => Ok(current),
    }
}

/// Canonical partition-ordered index row for one authoritative vector-evidence
/// record.
///
/// This is an authoritative reciprocal index, not a derived cache. It is
/// mutated atomically with the primary evidence row and contains only the
/// bounded classification needed for stale/outdated pages. Startup proves its
/// identity and classification against the primary row.
#[derive(Clone, Eq, PartialEq)]
pub struct VectorEvidenceIndexEntryV1 {
    target: VectorObservationTargetV1,
    entity_key: EntityKey,
    evidence_sequence: CommitSequence,
    newest_source_write: Option<CommitSequence>,
    embedding_write: Option<StoredVectorEmbeddingWriteV1>,
}

impl VectorEvidenceIndexEntryV1 {
    /// Reconstructs one checked durable index row.
    pub fn from_parts(
        target: VectorObservationTargetV1,
        entity_key: EntityKey,
        evidence_sequence: CommitSequence,
        newest_source_write: Option<CommitSequence>,
        embedding_write: Option<StoredVectorEmbeddingWriteV1>,
    ) -> Result<Self, StorageValueError> {
        if entity_key.entity_type_id() != target.entity_type()
            || newest_source_write.is_some_and(|sequence| sequence > evidence_sequence)
            || embedding_write
                .as_ref()
                .is_some_and(|embedding| embedding.sequence() > evidence_sequence)
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        Ok(Self {
            target,
            entity_key,
            evidence_sequence,
            newest_source_write,
            embedding_write,
        })
    }

    /// Constructs the exact index row owned by one primary evidence record.
    pub fn from_evidence(value: &StoredVectorEvidenceV1) -> Result<Self, StorageValueError> {
        Self::from_parts(
            VectorObservationTargetV1::new(
                value.schema_binding().lineage().clone(),
                value.partition_key().clone(),
                value.target().entity_type_id(),
                value.vector_field(),
            ),
            value.target().key().clone(),
            value.evidence_sequence(),
            value.newest_source_write(),
            value.embedding_write().cloned(),
        )
    }

    /// Stable partitioned field identity used as the physical key prefix.
    #[must_use]
    pub const fn target(&self) -> &VectorObservationTargetV1 {
        &self.target
    }

    /// Canonical entity key used as the final ordered key component.
    #[must_use]
    pub const fn entity_key(&self) -> &EntityKey {
        &self.entity_key
    }

    /// Primary evidence revision this index row mirrors.
    #[must_use]
    pub const fn evidence_sequence(&self) -> CommitSequence {
        self.evidence_sequence
    }

    /// Newest declared source-field write, when one exists.
    #[must_use]
    pub const fn newest_source_write(&self) -> Option<CommitSequence> {
        self.newest_source_write
    }

    /// Exact stored embedding write and model, when present.
    #[must_use]
    pub const fn embedding_write(&self) -> Option<&StoredVectorEmbeddingWriteV1> {
        self.embedding_write.as_ref()
    }

    /// Whether this row is source-stale, including a missing embedding after a
    /// source write.
    #[must_use]
    pub fn source_stale(&self) -> bool {
        match (self.newest_source_write, self.embedding_write.as_ref()) {
            (Some(_), None) => true,
            (Some(source), Some(embedding)) => source > embedding.sequence(),
            _ => false,
        }
    }

    /// Proves complete classification reciprocity with a primary evidence row.
    #[must_use]
    pub fn matches_evidence(&self, value: &StoredVectorEvidenceV1) -> bool {
        Self::from_evidence(value).is_ok_and(|expected| expected == *self)
    }
}

impl fmt::Debug for VectorEvidenceIndexEntryV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("VectorEvidenceIndexEntryV1")
            .field("target", &"[REDACTED]")
            .field("entity_key", &"[REDACTED]")
            .field("evidence_sequence", &self.evidence_sequence)
            .field("source_stale", &self.source_stale())
            .finish_non_exhaustive()
    }
}

/// Canonical count/index classification for one current vector-evidence row.
///
/// This is the single definition consumed by authoritative observation
/// counters, ordered inspection indexes, projected candidate admission, and
/// health. A missing embedding is stale once source state exists; model
/// identity is absent exactly when the embedding is absent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VectorEvidenceClassificationV1 {
    source_stale: bool,
    embedding: Option<StoredVectorEmbeddingWriteV1>,
}

impl VectorEvidenceClassificationV1 {
    fn from_parts(
        newest_source_write: Option<CommitSequence>,
        embedding: Option<StoredVectorEmbeddingWriteV1>,
    ) -> Self {
        let source_stale = newest_source_write.is_some_and(|source| {
            embedding
                .as_ref()
                .is_none_or(|embedding| source > embedding.sequence())
        });
        Self {
            source_stale,
            embedding,
        }
    }

    /// Returns whether source state is newer than, or exists without, an
    /// embedding revision.
    #[must_use]
    pub const fn source_stale(&self) -> bool {
        self.source_stale
    }

    /// Borrows the exact model identity/version and embedding sequence, when
    /// an embedding exists.
    #[must_use]
    pub const fn embedding(&self) -> Option<&StoredVectorEmbeddingWriteV1> {
        self.embedding.as_ref()
    }
}

/// Exact before/after classification for one authoritative evidence mutation.
///
/// Count and ordered-index maintenance consume this value inside the same
/// command transition as the entity and evidence mutation. `None` represents
/// absence, including the checked deletion successor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VectorEvidenceClassificationTransitionV1 {
    prior: Option<VectorEvidenceClassificationV1>,
    successor: Option<VectorEvidenceClassificationV1>,
}

impl VectorEvidenceClassificationTransitionV1 {
    /// Borrows the transaction-current predecessor classification.
    #[must_use]
    pub const fn prior(&self) -> Option<&VectorEvidenceClassificationV1> {
        self.prior.as_ref()
    }

    /// Borrows the sequence-assigned successor classification.
    #[must_use]
    pub const fn successor(&self) -> Option<&VectorEvidenceClassificationV1> {
        self.successor.as_ref()
    }
}

/// One compiler-derived authoritative evidence key read during command finalization.
#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
pub struct VectorEvidenceReadTargetV1 {
    target: EntityTarget,
    vector_field: FieldId,
}

impl VectorEvidenceReadTargetV1 {
    /// Constructs one exact production vector evidence key.
    #[must_use]
    pub const fn new(target: EntityTarget, vector_field: FieldId) -> Self {
        Self {
            target,
            vector_field,
        }
    }

    /// Borrows the exact entity target.
    #[must_use]
    pub const fn target(&self) -> &EntityTarget {
        &self.target
    }

    /// Returns the stable vector field identity.
    #[must_use]
    pub const fn vector_field(&self) -> FieldId {
        self.vector_field
    }
}

impl fmt::Debug for VectorEvidenceReadTargetV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("VectorEvidenceReadTargetV1([REDACTED])")
    }
}

/// Bounded canonical current-evidence request derived from mutated vector specs.
#[derive(Clone, Eq, PartialEq)]
pub struct VectorEvidenceReadRequestV1 {
    targets: Vec<VectorEvidenceReadTargetV1>,
}

impl VectorEvidenceReadRequestV1 {
    /// Retains exact keys only after proving count, uniqueness, and order.
    pub fn new(targets: Vec<VectorEvidenceReadTargetV1>) -> Result<Self, StorageValueError> {
        if targets.len() > crate::MAX_VALIDATION_TARGETS {
            return Err(StorageValueError::LimitExceeded);
        }
        if targets.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(StorageValueError::NonCanonicalOrder);
        }
        Ok(Self { targets })
    }

    /// Borrows keys in canonical entity-target/field order.
    #[must_use]
    pub fn targets(&self) -> &[VectorEvidenceReadTargetV1] {
        &self.targets
    }
}

impl fmt::Debug for VectorEvidenceReadRequestV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("VectorEvidenceReadRequestV1")
            .field("target_count", &self.targets.len())
            .finish()
    }
}

/// Complete transaction-current evidence observations for one canonical request.
#[derive(Clone, Eq, PartialEq)]
pub struct TransactionCurrentVectorEvidenceV1 {
    request: VectorEvidenceReadRequestV1,
    observations: Vec<Option<StoredVectorEvidenceV1>>,
}

impl TransactionCurrentVectorEvidenceV1 {
    /// Joins every present row to its exact requested physical identity.
    pub fn new(
        request: VectorEvidenceReadRequestV1,
        observations: Vec<Option<StoredVectorEvidenceV1>>,
    ) -> Result<Self, StorageValueError> {
        if request.targets().len() != observations.len()
            || request
                .targets()
                .iter()
                .zip(&observations)
                .any(|(target, observation)| {
                    observation.as_ref().is_some_and(|row| {
                        row.target() != target.target()
                            || row.vector_field() != target.vector_field()
                    })
                })
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        Ok(Self {
            request,
            observations,
        })
    }

    /// Returns the exact observed predecessor for a requested key.
    #[must_use]
    pub fn get(&self, target: &VectorEvidenceReadTargetV1) -> Option<&StoredVectorEvidenceV1> {
        self.request
            .targets()
            .binary_search(target)
            .ok()
            .and_then(|position| self.observations[position].as_ref())
    }

    /// Borrows the complete canonical request.
    #[must_use]
    pub const fn request(&self) -> &VectorEvidenceReadRequestV1 {
        &self.request
    }
}

impl fmt::Debug for TransactionCurrentVectorEvidenceV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TransactionCurrentVectorEvidenceV1")
            .field("target_count", &self.request.targets().len())
            .field(
                "present_count",
                &self
                    .observations
                    .iter()
                    .filter(|value| value.is_some())
                    .count(),
            )
            .finish()
    }
}

/// Sequence-free authoritative transition for one production vector field.
///
/// This is retained in the pre-sequence write plan. `source_changed` and
/// `embedding_changed` select the newly assigned command sequence; absent
/// changes preserve the exact prior evidence supplied by the transaction-
/// current reader.
#[derive(Clone, Eq, PartialEq)]
pub struct VectorEvidenceTransitionPlanV1 {
    target: EntityTarget,
    partition_key: PartitionKey,
    vector_field: FieldId,
    stale_entity_count_threshold: u64,
    entity_version: EntityVersion,
    prior_source_write: Option<CommitSequence>,
    prior_embedding_write: Option<StoredVectorEmbeddingWriteV1>,
    prior_exists: bool,
    source_changed: bool,
    embedding_changed: Option<EmbeddingMetadata>,
    delete: bool,
    schema_binding: DurableKeySchemaBindingV1,
    provenance_id: ProvenanceId,
    plan: ExecutablePlanRef,
}

impl VectorEvidenceTransitionPlanV1 {
    /// Constructs one live source/embedding transition from exact prior evidence.
    #[allow(clippy::too_many_arguments)]
    pub fn live(
        target: EntityTarget,
        partition_key: PartitionKey,
        vector_field: FieldId,
        stale_entity_count_threshold: u64,
        entity_version: EntityVersion,
        prior: Option<&StoredVectorEvidenceV1>,
        source_changed: bool,
        embedding_changed: Option<EmbeddingMetadata>,
        schema_binding: DurableKeySchemaBindingV1,
        provenance_id: ProvenanceId,
        plan: ExecutablePlanRef,
    ) -> Result<Self, StorageValueError> {
        if (!source_changed && embedding_changed.is_none()) || stale_entity_count_threshold == 0 {
            return Err(StorageValueError::InvalidShape);
        }
        if !schema_binding.matches_plan(&plan)
            || prior.is_some_and(|prior| {
                prior.target() != &target
                    || prior.partition_key() != &partition_key
                    || prior.vector_field() != vector_field
            })
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        Ok(Self {
            target,
            partition_key,
            vector_field,
            stale_entity_count_threshold,
            entity_version,
            prior_source_write: prior.and_then(StoredVectorEvidenceV1::newest_source_write),
            prior_embedding_write: prior
                .and_then(StoredVectorEvidenceV1::embedding_write)
                .cloned(),
            prior_exists: prior.is_some(),
            source_changed,
            embedding_changed,
            delete: false,
            schema_binding,
            provenance_id,
            plan,
        })
    }

    /// Constructs the checked removal paired with one entity deletion.
    pub fn delete(
        prior: &StoredVectorEvidenceV1,
        stale_entity_count_threshold: u64,
        provenance_id: ProvenanceId,
        plan: ExecutablePlanRef,
    ) -> Result<Self, StorageValueError> {
        if stale_entity_count_threshold == 0 {
            return Err(StorageValueError::InvalidShape);
        }
        Ok(Self {
            target: prior.target().clone(),
            partition_key: prior.partition_key().clone(),
            vector_field: prior.vector_field(),
            stale_entity_count_threshold,
            entity_version: prior.entity_version(),
            prior_source_write: prior.newest_source_write(),
            prior_embedding_write: prior.embedding_write().cloned(),
            prior_exists: true,
            source_changed: false,
            embedding_changed: None,
            delete: true,
            schema_binding: prior.schema_binding().clone(),
            provenance_id,
            plan,
        })
    }

    /// Borrows the entity target used for canonical ordering and graph checks.
    #[must_use]
    pub const fn target(&self) -> &EntityTarget {
        &self.target
    }

    /// Returns the stable vector field identity.
    #[must_use]
    pub const fn vector_field(&self) -> FieldId {
        self.vector_field
    }

    /// Compiler-owned strict stale-entity threshold for health maintenance.
    #[must_use]
    pub const fn stale_entity_count_threshold(&self) -> u64 {
        self.stale_entity_count_threshold
    }

    /// Returns the stable partitioned observation identity maintained with
    /// this transition.
    #[must_use]
    pub fn observation_target(&self) -> VectorObservationTargetV1 {
        VectorObservationTargetV1::new(
            self.schema_binding.lineage().clone(),
            self.partition_key.clone(),
            self.target.entity_type_id(),
            self.vector_field,
        )
    }

    pub(crate) const fn provenance_id(&self) -> ProvenanceId {
        self.provenance_id
    }

    pub(crate) const fn plan(&self) -> &ExecutablePlanRef {
        &self.plan
    }

    /// Returns whether this transition deletes current evidence.
    #[must_use]
    pub const fn is_delete(&self) -> bool {
        self.delete
    }

    /// Proves an observed current row is the exact predecessor summarized by this plan.
    #[must_use]
    pub fn matches_current(&self, current: Option<&StoredVectorEvidenceV1>) -> bool {
        match current {
            Some(current) => {
                current.target() == &self.target
                    && current.partition_key() == &self.partition_key
                    && current.vector_field() == self.vector_field
                    && current.newest_source_write() == self.prior_source_write
                    && current.embedding_write() == self.prior_embedding_write.as_ref()
            }
            None => {
                !self.delete
                    && self.prior_source_write.is_none()
                    && self.prior_embedding_write.is_none()
            }
        }
    }

    /// Materializes the exact sequence-assigned current-row mutation.
    pub fn materialize(
        &self,
        sequence: CommitSequence,
    ) -> Result<VectorEvidenceMutationV1, StorageValueError> {
        if self.delete {
            return Ok(VectorEvidenceMutationV1::Delete {
                target: self.target.clone(),
                vector_field: self.vector_field,
            });
        }
        let newest_source_write = if self.source_changed {
            Some(sequence)
        } else {
            self.prior_source_write
        };
        let embedding_write = match &self.embedding_changed {
            Some(metadata) => Some(StoredVectorEmbeddingWriteV1::new(
                sequence,
                metadata.clone(),
            )),
            None => self.prior_embedding_write.clone(),
        };
        StoredVectorEvidenceV1::new(
            self.target.clone(),
            self.partition_key.clone(),
            self.vector_field,
            self.entity_version,
            sequence,
            newest_source_write,
            embedding_write,
            self.schema_binding.clone(),
            self.provenance_id,
            self.plan.clone(),
        )
        .map(Box::new)
        .map(VectorEvidenceMutationV1::Put)
    }

    /// Produces the one canonical classification transition used by all
    /// maintained observation structures.
    pub fn classification_transition(
        &self,
        sequence: CommitSequence,
    ) -> Result<VectorEvidenceClassificationTransitionV1, StorageValueError> {
        let prior = self.prior_exists.then(|| {
            VectorEvidenceClassificationV1::from_parts(
                self.prior_source_write,
                self.prior_embedding_write.clone(),
            )
        });
        let successor = match self.materialize(sequence)? {
            VectorEvidenceMutationV1::Put(value) => Some(value.classification()),
            VectorEvidenceMutationV1::Delete { .. } => None,
        };
        Ok(VectorEvidenceClassificationTransitionV1 { prior, successor })
    }

    pub(crate) fn semantic_bytes(&self) -> Result<usize, StorageValueError> {
        self.materialize(
            CommitSequence::new(u64::MAX).expect("maximum nonzero commit sequence is valid"),
        )?
        .semantic_bytes()
    }
}

impl fmt::Debug for VectorEvidenceTransitionPlanV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("VectorEvidenceTransitionPlanV1([REDACTED])")
    }
}

/// One current-row vector evidence insertion/replacement or checked deletion.
#[derive(Clone, Eq, PartialEq)]
pub enum VectorEvidenceMutationV1 {
    /// Insert or replace the complete authoritative evidence row.
    Put(Box<StoredVectorEvidenceV1>),
    /// Remove evidence paired with an entity deletion.
    Delete {
        /// Deleted entity target.
        target: EntityTarget,
        /// Stable vector field identity.
        vector_field: FieldId,
    },
}

impl VectorEvidenceMutationV1 {
    /// Borrows the target used by the physical key.
    #[must_use]
    pub const fn target(&self) -> &EntityTarget {
        match self {
            Self::Put(value) => value.target(),
            Self::Delete { target, .. } => target,
        }
    }

    /// Returns the vector field used by the physical key.
    #[must_use]
    pub const fn vector_field(&self) -> FieldId {
        match self {
            Self::Put(value) => value.vector_field(),
            Self::Delete { vector_field, .. } => *vector_field,
        }
    }

    pub(crate) fn semantic_bytes(&self) -> Result<usize, StorageValueError> {
        match self {
            Self::Put(value) => value.semantic_bytes(),
            Self::Delete { target, .. } => target
                .semantic_bytes()?
                .checked_add(4 + 1)
                .ok_or(StorageValueError::SizeOverflow),
        }
    }
}

impl fmt::Debug for VectorEvidenceMutationV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("VectorEvidenceMutationV1([REDACTED])")
    }
}

/// The last authoritative embedding write for one vector field.
///
/// Model metadata cannot exist independently of an embedding revision because
/// both are carried by this one semantic value.
#[derive(Clone, Eq, PartialEq)]
pub struct StoredVectorEmbeddingWriteV1 {
    sequence: CommitSequence,
    metadata: EmbeddingMetadata,
}

impl StoredVectorEmbeddingWriteV1 {
    /// Binds validated model metadata to the exact commit that wrote the vector.
    #[must_use]
    pub const fn new(sequence: CommitSequence, metadata: EmbeddingMetadata) -> Self {
        Self { sequence, metadata }
    }

    /// Returns the commit that wrote the current vector value.
    #[must_use]
    pub const fn sequence(&self) -> CommitSequence {
        self.sequence
    }

    /// Borrows the exact submitted model identity and version.
    #[must_use]
    pub const fn metadata(&self) -> &EmbeddingMetadata {
        &self.metadata
    }
}

impl fmt::Debug for StoredVectorEmbeddingWriteV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StoredVectorEmbeddingWriteV1")
            .field("sequence", &self.sequence)
            .field("metadata", &"[REDACTED]")
            .finish()
    }
}

/// Authoritative side evidence for one live entity's declared vector field.
///
/// The exact vector-spec identity is the tuple of the schema binding, entity
/// type in `target`, and stable `vector_field`. The record deliberately carries
/// neither vector components nor an entity post-image.
#[derive(Clone, Eq, PartialEq)]
pub struct StoredVectorEvidenceV1 {
    target: EntityTarget,
    partition_key: PartitionKey,
    vector_field: FieldId,
    entity_version: EntityVersion,
    evidence_sequence: CommitSequence,
    newest_source_write: Option<CommitSequence>,
    embedding_write: Option<StoredVectorEmbeddingWriteV1>,
    schema_binding: DurableKeySchemaBindingV1,
    provenance_id: ProvenanceId,
    plan: ExecutablePlanRef,
}

impl StoredVectorEvidenceV1 {
    /// Constructs one closed source-only, embedding-only, or combined revision.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        target: EntityTarget,
        partition_key: PartitionKey,
        vector_field: FieldId,
        entity_version: EntityVersion,
        evidence_sequence: CommitSequence,
        newest_source_write: Option<CommitSequence>,
        embedding_write: Option<StoredVectorEmbeddingWriteV1>,
        schema_binding: DurableKeySchemaBindingV1,
        provenance_id: ProvenanceId,
        plan: ExecutablePlanRef,
    ) -> Result<Self, StorageValueError> {
        if !schema_binding.matches_plan(&plan) {
            return Err(StorageValueError::IdentityMismatch);
        }

        if newest_source_write.is_some_and(|sequence| sequence > evidence_sequence)
            || embedding_write
                .as_ref()
                .is_some_and(|write| write.sequence() > evidence_sequence)
        {
            return Err(StorageValueError::InvalidShape);
        }

        let source_changed = newest_source_write == Some(evidence_sequence);
        let embedding_changed = embedding_write
            .as_ref()
            .is_some_and(|write| write.sequence() == evidence_sequence);
        if !source_changed && !embedding_changed {
            return Err(StorageValueError::InvalidShape);
        }

        Ok(Self {
            target,
            partition_key,
            vector_field,
            entity_version,
            evidence_sequence,
            newest_source_write,
            embedding_write,
            schema_binding,
            provenance_id,
            plan,
        })
    }

    /// Borrows the live entity target carrying the vector value.
    #[must_use]
    pub const fn target(&self) -> &EntityTarget {
        &self.target
    }

    /// Borrows the exact command partition.
    #[must_use]
    pub const fn partition_key(&self) -> &PartitionKey {
        &self.partition_key
    }

    /// Returns the stable field identity within the exact bound bundle.
    #[must_use]
    pub const fn vector_field(&self) -> FieldId {
        self.vector_field
    }

    /// Returns the entity post-image version written by the same transition.
    #[must_use]
    pub const fn entity_version(&self) -> EntityVersion {
        self.entity_version
    }

    /// Returns the commit sequence of this evidence revision.
    #[must_use]
    pub const fn evidence_sequence(&self) -> CommitSequence {
        self.evidence_sequence
    }

    /// Returns the newest commit that changed a declared source field.
    #[must_use]
    pub const fn newest_source_write(&self) -> Option<CommitSequence> {
        self.newest_source_write
    }

    /// Borrows the current embedding revision and its exact model metadata.
    #[must_use]
    pub const fn embedding_write(&self) -> Option<&StoredVectorEmbeddingWriteV1> {
        self.embedding_write.as_ref()
    }

    /// Borrows the exact contract bundle owning the vector spec.
    #[must_use]
    pub const fn schema_binding(&self) -> &DurableKeySchemaBindingV1 {
        &self.schema_binding
    }

    /// Returns the provenance row written by the same command transition.
    #[must_use]
    pub const fn provenance_id(&self) -> ProvenanceId {
        self.provenance_id
    }

    /// Borrows the exact command plan that produced this evidence revision.
    #[must_use]
    pub const fn plan(&self) -> &ExecutablePlanRef {
        &self.plan
    }

    /// Returns the canonical maintained-count and ordered-index
    /// classification for this row.
    #[must_use]
    pub fn classification(&self) -> VectorEvidenceClassificationV1 {
        VectorEvidenceClassificationV1::from_parts(
            self.newest_source_write,
            self.embedding_write.clone(),
        )
    }

    pub(crate) fn semantic_bytes(&self) -> Result<usize, StorageValueError> {
        let embedding_bytes = self.embedding_write.as_ref().map_or(1, |write| {
            1 + 8
                + 4
                + write.metadata().model_identity().len()
                + 4
                + write.metadata().model_version().len()
        });
        self.target
            .semantic_bytes()?
            .checked_add(4 + self.partition_key.as_bytes().len())
            .and_then(|value| value.checked_add(4 + 8 + 8 + 1 + embedding_bytes))
            .and_then(|value| {
                self.schema_binding
                    .semantic_bytes()
                    .ok()?
                    .checked_add(value)
            })
            .and_then(|value| self.plan.semantic_bytes()?.checked_add(value))
            .and_then(|value| value.checked_add(16))
            .ok_or(StorageValueError::SizeOverflow)
    }
}

impl fmt::Debug for StoredVectorEvidenceV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StoredVectorEvidenceV1")
            .field("target", &self.target)
            .field("partition_key", &"[REDACTED]")
            .field("vector_field", &self.vector_field)
            .field("entity_version", &self.entity_version)
            .field("evidence_sequence", &self.evidence_sequence)
            .field("newest_source_write", &self.newest_source_write)
            .field("embedding_write", &self.embedding_write)
            .field("schema_binding", &self.schema_binding)
            .field("provenance_id", &self.provenance_id)
            .field("plan", &self.plan)
            .finish()
    }
}

#[cfg(test)]
mod index_page_tests {
    use super::*;
    use riffdb_types::{AggregateTypeId, EntityKeyBuilder, PartitionKeyBuilder};

    fn target() -> VectorObservationTargetV1 {
        let mut partition = PartitionKeyBuilder::new(AggregateTypeId::first());
        partition.push_str("org-a").expect("partition");
        VectorObservationTargetV1::new(
            ContractLineage::new("vectors").expect("lineage"),
            partition.finish().expect("partition"),
            EntityTypeId::new(7).expect("entity type"),
            FieldId::new(9).expect("field"),
        )
    }

    fn entry(value: u64) -> VectorEvidenceIndexEntryV1 {
        let mut key = EntityKeyBuilder::new(EntityTypeId::new(7).expect("entity type"));
        key.push_u64(value).expect("key");
        VectorEvidenceIndexEntryV1::from_parts(
            target(),
            key.finish().expect("key"),
            CommitSequence::new(value).expect("sequence"),
            Some(CommitSequence::new(value).expect("sequence")),
            None,
        )
        .expect("index entry")
    }

    #[test]
    fn page_requires_canonical_order_and_exact_continuation() {
        let first = entry(1);
        let second = entry(2);
        let continuation = second.entity_key().clone();
        let page = VectorEvidenceIndexPageV1::new(
            &target(),
            vec![first.clone(), second.clone()],
            Some(continuation.clone()),
            false,
            10,
        )
        .expect("bounded page");
        assert_eq!(page.continuation(), Some(&continuation));
        assert!(!page.exact_end());
        assert!(
            VectorEvidenceIndexPageV1::new(
                &target(),
                vec![second, first],
                Some(continuation),
                false,
                10,
            )
            .is_err()
        );
        assert!(VectorEvidenceIndexPageV1::new(&target(), Vec::new(), None, true, 0).is_ok());
    }
}

#[cfg(test)]
mod projection_control_tests {
    use super::*;

    fn source() -> VectorProjectionSourceV1 {
        VectorProjectionSourceV1::new(
            ContractLineage::new("vectors").expect("lineage"),
            EntityTypeId::new(2).expect("entity"),
            FieldId::new(4).expect("field"),
        )
    }

    fn limits() -> VectorProjectionReplayLimitsV1 {
        VectorProjectionReplayLimitsV1::new(60, 1_024, 10).expect("limits")
    }

    #[test]
    fn only_ready_control_contributes_to_retention() {
        let initial = StoredVectorProjectionControlV1::initial(source(), [0x11; 32], limits());
        assert!(!initial.retention_attached());
        crate::encode_vector_projection_control_v1(&initial).expect("control codec");

        let ready = StoredVectorProjectionControlV1::new(
            source(),
            ProjectionGeneration::first(),
            [0x11; 32],
            VectorProjectionLifecycleV1::Ready,
            FrontierPosition::AppliedThrough(CommitSequence::new(7).expect("sequence")),
            None,
            None,
            limits(),
        )
        .expect("ready");
        assert!(ready.retention_attached());

        let detached = StoredVectorProjectionControlV1::new(
            source(),
            ProjectionGeneration::new(2).expect("generation"),
            [0x22; 32],
            VectorProjectionLifecycleV1::RebuildRequired,
            ready.published_frontier(),
            None,
            Some(VectorProjectionRebuildReasonV1::ReplayBacklog),
            limits(),
        )
        .expect("detached");
        assert!(!detached.retention_attached());
        assert_eq!(detached.published_frontier(), ready.published_frontier());
    }

    #[test]
    fn rebuild_shape_is_closed_and_limits_are_positive() {
        assert!(VectorProjectionReplayLimitsV1::new(0, 1, 1).is_none());
        assert!(
            StoredVectorProjectionControlV1::new(
                source(),
                ProjectionGeneration::first(),
                [0; 32],
                VectorProjectionLifecycleV1::Ready,
                FrontierPosition::BeforeFirst,
                None,
                Some(VectorProjectionRebuildReasonV1::ReplayAge),
                limits(),
            )
            .is_err()
        );
        assert!(
            StoredVectorProjectionControlV1::new(
                source(),
                ProjectionGeneration::new(2).expect("generation"),
                [0; 32],
                VectorProjectionLifecycleV1::Rebuilding,
                FrontierPosition::BeforeFirst,
                Some(FrontierPosition::AppliedThrough(
                    CommitSequence::new(9).expect("sequence")
                )),
                Some(VectorProjectionRebuildReasonV1::DefinitionChanged),
                limits(),
            )
            .is_ok()
        );
    }
}
