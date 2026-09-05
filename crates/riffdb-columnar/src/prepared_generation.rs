//! Process-generation-bound evidence for validated immutable columnar artifacts.

use riffdb_storage_api::{
    ColumnarControlError, ColumnarProjectionControlWriteResultV1,
    ColumnarProjectionGenerationRoleV1, ColumnarProjectionLayoutV1, StorageError,
    StoredColumnarProjectionControlV1, StoredColumnarProjectionGenerationV1,
};
use riffdb_types::{ColumnarDefinitionSemanticsHashV1, ColumnarProjectionSourceV1};

use crate::ColumnarProjectionSpecV1;

/// Nonserializable, fields-private proof that one immutable generation was
/// reopened and completely validated by one exact process generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedColumnarGenerationV1 {
    source: ColumnarProjectionSourceV1,
    definition_semantics_hash: ColumnarDefinitionSemanticsHashV1,
    generation: StoredColumnarProjectionGenerationV1,
    process_generation: [u8; 16],
}

impl PreparedColumnarGenerationV1 {
    pub(crate) fn from_validated(
        spec: &ColumnarProjectionSpecV1,
        generation: StoredColumnarProjectionGenerationV1,
        process_generation: [u8; 16],
    ) -> Result<Self, ColumnarControlError> {
        if generation.artifact().is_none()
            || !matches!(
                generation.role(),
                ColumnarProjectionGenerationRoleV1::Candidate
                    | ColumnarProjectionGenerationRoleV1::Published
            )
            || generation.definition_fingerprint() != spec.definition_fingerprint()
            || generation.spec_hash() != spec.hash()
        {
            return Err(ColumnarControlError);
        }
        Ok(Self {
            source: spec.source().clone(),
            definition_semantics_hash: spec.definition_semantics_hash(),
            generation,
            process_generation,
        })
    }

    /// Schema-bound source validated with this artifact.
    #[must_use]
    pub const fn source(&self) -> &ColumnarProjectionSourceV1 {
        &self.source
    }

    /// Complete definition-semantics digest validated with this artifact.
    #[must_use]
    pub const fn definition_semantics_hash(&self) -> ColumnarDefinitionSemanticsHashV1 {
        self.definition_semantics_hash
    }

    /// Exact prepared durable pointer.
    #[must_use]
    pub const fn generation(&self) -> &StoredColumnarProjectionGenerationV1 {
        &self.generation
    }

    /// Validates the witness against its control and current process before any
    /// repository transaction begins.
    #[doc(hidden)]
    pub fn replacement_for(
        &self,
        expected: &StoredColumnarProjectionControlV1,
        process_generation: [u8; 16],
    ) -> Result<StoredColumnarProjectionGenerationV1, ColumnarControlError> {
        if self.process_generation != process_generation
            || &self.source != expected.source()
            || self.generation.definition_fingerprint() != expected.target_definition_fingerprint()
            || self.generation.spec_hash() != expected.target_spec_hash()
        {
            return Err(ColumnarControlError);
        }
        Ok(self.generation.clone())
    }
}

/// Columnar-owned durable operations that require a validated generation witness.
pub trait PreparedColumnarGenerationRepository {
    /// Atomically installs the first complete Candidate artifact evidence.
    fn record_durable_snapshot(
        &self,
        expected: &StoredColumnarProjectionControlV1,
        prepared: &PreparedColumnarGenerationV1,
        process_generation: [u8; 16],
    ) -> Result<ColumnarProjectionControlWriteResultV1, StorageError>;

    /// Advances a Prepared V1 Candidate without passing the current head.
    fn record_candidate_frontier(
        &self,
        expected: &StoredColumnarProjectionControlV1,
        replacement: &PreparedColumnarGenerationV1,
        process_generation: [u8; 16],
    ) -> Result<ColumnarProjectionControlWriteResultV1, StorageError>;

    /// Advances only Published V1 while preserving any Candidate byte-exact.
    fn advance_published_v1(
        &self,
        expected: &StoredColumnarProjectionControlV1,
        replacement: &PreparedColumnarGenerationV1,
        process_generation: [u8; 16],
    ) -> Result<ColumnarProjectionControlWriteResultV1, StorageError>;

    /// Publishes a Prepared Candidate only at the transaction-current head.
    fn publish_prepared_generation(
        &self,
        expected: &StoredColumnarProjectionControlV1,
        prepared: &PreparedColumnarGenerationV1,
        process_generation: [u8; 16],
    ) -> Result<ColumnarProjectionControlWriteResultV1, StorageError>;
}

pub(crate) fn validate_v1_pointer_shape(
    generation: &StoredColumnarProjectionGenerationV1,
) -> Result<(), ColumnarControlError> {
    if generation.layout() != ColumnarProjectionLayoutV1::V1
        || generation.physical_generation_fingerprint().is_some()
    {
        Err(ColumnarControlError)
    } else {
        Ok(())
    }
}

pub(crate) fn validate_v2_pointer_shape(
    generation: &StoredColumnarProjectionGenerationV1,
) -> Result<(), ColumnarControlError> {
    if generation.layout() != ColumnarProjectionLayoutV1::V2
        || generation.physical_generation_fingerprint().is_none()
    {
        Err(ColumnarControlError)
    } else {
        Ok(())
    }
}
