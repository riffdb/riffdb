//! Authoritative, nonduplicating evidence for production vector fields.

use std::{collections::BTreeMap, fmt};

use riffdb_types::{
    CommitSequence, ContractLineage, EmbeddingMetadata, EntityTypeId, EntityVersion, FieldId,
    PartitionKey, ProvenanceId,
};

use crate::{DurableKeySchemaBindingV1, EntityTarget, ExecutablePlanRef, StorageValueError};

/// Maximum distinct live embedding model revisions retained in one logical
/// partition/field observation row.
///
/// Contract successors can introduce new versions, but a malformed workload
/// cannot grow one authoritative counter record without bound.
pub const MAX_VECTOR_MODELS_PER_OBSERVATION: usize = 256;

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
}

/// Stable logical identity for maintained vector counts and ordered indexes.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct VectorObservationTargetV1 {
    lineage: ContractLineage,
    partition_key: PartitionKey,
    entity_type: EntityTypeId,
    vector_field: FieldId,
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
        entity_version: EntityVersion,
        prior: Option<&StoredVectorEvidenceV1>,
        source_changed: bool,
        embedding_changed: Option<EmbeddingMetadata>,
        schema_binding: DurableKeySchemaBindingV1,
        provenance_id: ProvenanceId,
        plan: ExecutablePlanRef,
    ) -> Result<Self, StorageValueError> {
        if !source_changed && embedding_changed.is_none() {
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
        provenance_id: ProvenanceId,
        plan: ExecutablePlanRef,
    ) -> Result<Self, StorageValueError> {
        Ok(Self {
            target: prior.target().clone(),
            partition_key: prior.partition_key().clone(),
            vector_field: prior.vector_field(),
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
