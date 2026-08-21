//! Authoritative, nonduplicating evidence for production vector fields.

use std::fmt;

use riffdb_types::{
    CommitSequence, EmbeddingMetadata, EntityVersion, FieldId, PartitionKey, ProvenanceId,
};

use crate::{DurableKeySchemaBindingV1, EntityTarget, ExecutablePlanRef, StorageValueError};

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
