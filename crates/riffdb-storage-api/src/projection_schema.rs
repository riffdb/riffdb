//! Narrow bridge to the compiler-owned immutable projection group schema.

use riffdb_contract_ir::BoundProjectionGroupSchema;
use riffdb_types::{
    CanonicalRecord, CanonicalValue, ProjectionGeneration, ProjectionGroupKey,
    ProjectionGroupPrefix, ProjectionIdentity, encode_canonical_record,
};

use crate::{StorageValueError, canonical_codec_storage_error};

/// Storage's checked view of one compiler-owned projection group schema.
///
/// This wrapper is the only semantic storage module permitted to name a
/// contract-IR type. It exposes value validation, not executable plan access.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckedProjectionSchema {
    bound: BoundProjectionGroupSchema,
}

impl CheckedProjectionSchema {
    /// Wraps an immutable compiler-checked schema without weakening it.
    #[must_use]
    pub const fn new(bound: BoundProjectionGroupSchema) -> Self {
        Self { bound }
    }

    /// Returns the exact lineage, projection ID, and projection-plan hash.
    #[must_use]
    pub const fn identity(&self) -> &ProjectionIdentity {
        self.bound.identity()
    }

    /// Returns the declared number of complete group components.
    #[must_use]
    pub fn group_component_count(&self) -> usize {
        self.bound.schema().group_components().len()
    }

    /// Returns the statically checked maximum complete group-key size.
    #[must_use]
    pub const fn maximum_complete_key_bytes(&self) -> usize {
        self.bound.schema().maximum_complete_key_bytes()
    }

    /// Returns the statically checked maximum stored state size.
    #[must_use]
    pub const fn maximum_stored_state_bytes(&self) -> usize {
        self.bound.schema().maximum_stored_state_bytes()
    }

    /// Constructs a complete key under the exact checked schema.
    pub fn group_key(
        &self,
        generation: ProjectionGeneration,
        values: &[CanonicalValue],
    ) -> Result<ProjectionGroupKey, StorageValueError> {
        self.bound
            .group_key(generation, values)
            .map_err(|_| StorageValueError::InvalidShape)
    }

    /// Constructs a zero-or-more-component leading prefix under the schema.
    pub fn group_prefix(
        &self,
        generation: ProjectionGeneration,
        values: &[CanonicalValue],
    ) -> Result<ProjectionGroupPrefix, StorageValueError> {
        self.bound
            .group_prefix(generation, values)
            .map_err(|_| StorageValueError::InvalidShape)
    }

    /// Revalidates a syntactically decoded complete key under this schema.
    pub fn validate_group_key(&self, key: &ProjectionGroupKey) -> Result<(), StorageValueError> {
        self.bound
            .validate_group_key(key)
            .map_err(|_| StorageValueError::InvalidShape)
    }

    /// Validates an exact canonical measure record under this projection.
    pub fn validate_measure_record(
        &self,
        measures: &CanonicalRecord,
    ) -> Result<(), StorageValueError> {
        let schema_fields = self.bound.schema().measures().fields();
        let values = measures.fields();
        if schema_fields.len() != values.len() {
            return Err(StorageValueError::InvalidShape);
        }
        for (schema, (field_id, value)) in schema_fields.iter().zip(values) {
            if schema.id() != *field_id || schema.value_type().validate_value(value).is_err() {
                return Err(StorageValueError::InvalidShape);
            }
        }
        let encoded = encode_canonical_record(measures)
            .map_err(|error| canonical_codec_storage_error(&error))?;
        if encoded.len() > self.maximum_stored_state_bytes() {
            return Err(StorageValueError::LimitExceeded);
        }
        Ok(())
    }
}
