//! Generation-neutral row values. Representation and schema are not authority.

use crate::{CheckedProjectionSchema, StorageValueError, canonical_codec_storage_error};
use riffdb_types::{
    CanonicalRecord, CanonicalValue, CommitSequence, MAX_KEY_BYTES, MAX_NESTING_DEPTH,
    ProjectionGeneration, ProjectionGroupKey, ProjectionGroupKeyBuilder, ProjectionIdentity,
    ProjectionSourceLocatorV1, canonical_record_encoded_len,
};
use std::{fmt, mem::size_of};

/// One bounded generation-neutral authoritative source row representation.
/// Only independent evaluation and coordinator publication establish authority.
#[derive(Clone, Eq, PartialEq)]
pub struct ProjectionSourceRowV1 {
    identity: ProjectionIdentity,
    locator: ProjectionSourceLocatorV1,
    group_values: Vec<CanonicalValue>,
    measures: CanonicalRecord,
    last_changed_sequence: CommitSequence,
}

impl ProjectionSourceRowV1 {
    /// Checks exact schema and prefix context, then transfers the owned values.
    /// The input key/measure ownership and the returned preflight work allowance
    /// must already be charged to the worker before this operation allocates.
    pub fn new(
        schema: &CheckedProjectionSchema,
        locator: ProjectionSourceLocatorV1,
        applied_through: CommitSequence,
        key: ProjectionGroupKey,
        measures: CanonicalRecord,
        last_changed_sequence: CommitSequence,
    ) -> Result<Self, StorageValueError> {
        if key.identity() != schema.identity() {
            return Err(StorageValueError::IdentityMismatch);
        }
        if last_changed_sequence > applied_through {
            return Err(StorageValueError::InvalidShape);
        }
        schema.validate_group_key(&key)?;
        schema.validate_measure_record(&measures)?;
        Self::from_key(locator, key, measures, last_changed_sequence)
    }

    /// Reconstructs bounded canonical syntax without claiming schema or source
    /// closure. Decode admission must precede input allocation; this operation's
    /// separate key/validation work must also be reserved before calling it.
    pub fn from_stored_parts(
        identity: ProjectionIdentity,
        locator: ProjectionSourceLocatorV1,
        group_values: Vec<CanonicalValue>,
        measures: CanonicalRecord,
        last_changed_sequence: CommitSequence,
    ) -> Result<Self, StorageValueError> {
        let mut key = ProjectionGroupKeyBuilder::new(identity, ProjectionGeneration::first());
        for value in group_values {
            key.push_component(value)
                .map_err(crate::projection::projection_key_storage_error)?;
        }
        let key = key
            .finish()
            .map_err(crate::projection::projection_key_storage_error)?;
        Self::from_key(locator, key, measures, last_changed_sequence)
    }

    fn from_key(
        locator: ProjectionSourceLocatorV1,
        key: ProjectionGroupKey,
        measures: CanonicalRecord,
        last_changed_sequence: CommitSequence,
    ) -> Result<Self, StorageValueError> {
        crate::projection::validate_projection_state_structure(&key, &measures)?;
        let (identity, _discarded_generation, group_values) = key.into_parts();
        Ok(Self {
            identity,
            locator,
            group_values,
            measures,
            last_changed_sequence,
        })
    }

    /// Consumes a decoded row and validates the exact intended source context.
    /// Context validation never grants independent-evaluation or publication.
    pub fn into_checked(
        self,
        schema: &CheckedProjectionSchema,
        expected_locator: ProjectionSourceLocatorV1,
        applied_through: CommitSequence,
    ) -> Result<Self, StorageValueError> {
        if self.locator != expected_locator || &self.identity != schema.identity() {
            return Err(StorageValueError::IdentityMismatch);
        }
        let mut key = ProjectionGroupKeyBuilder::new(self.identity, ProjectionGeneration::first());
        for value in self.group_values {
            key.push_component(value)
                .map_err(crate::projection::projection_key_storage_error)?;
        }
        Self::new(
            schema,
            self.locator,
            applied_through,
            key.finish()
                .map_err(crate::projection::projection_key_storage_error)?,
            self.measures,
            self.last_changed_sequence,
        )
    }

    /// Exact projection identity, including its semantic plan hash.
    #[must_use]
    pub const fn identity(&self) -> &ProjectionIdentity {
        &self.identity
    }
    /// Original full source allocation locator; equality alone grants nothing.
    #[must_use]
    pub const fn locator(&self) -> ProjectionSourceLocatorV1 {
        self.locator
    }
    /// Canonical scalar grouping values in declared order.
    #[must_use]
    pub fn group_values(&self) -> &[CanonicalValue] {
        &self.group_values
    }
    /// Canonical measure values; no cache or generation field is retained.
    #[must_use]
    pub const fn measures(&self) -> &CanonicalRecord {
        &self.measures
    }
    /// Exact last authoritative command that changed this evaluated group.
    #[must_use]
    pub const fn last_changed_sequence(&self) -> CommitSequence {
        self.last_changed_sequence
    }
    /// Transfers all row ownership without introducing a derived generation.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        ProjectionIdentity,
        ProjectionSourceLocatorV1,
        Vec<CanonicalValue>,
        CanonicalRecord,
        CommitSequence,
    ) {
        (
            self.identity,
            self.locator,
            self.group_values,
            self.measures,
            self.last_changed_sequence,
        )
    }

    /// Allocation-free conservative additional constructor scratch bound.
    /// Existing owned inputs are separately charged; this includes transient
    /// encoded measures and bounded key/identity buffering. It grants no budget.
    pub fn construction_work_bound_v1(
        key: &ProjectionGroupKey,
        measures: &CanonicalRecord,
    ) -> Result<u64, StorageValueError> {
        Self::stored_parts_work_bound_v1(key.components(), measures)
    }

    /// Allocation-free preflight before rebuilding a transient key from already
    /// owned decoded parts. Decode/input ownership is a separate prior charge.
    pub fn stored_parts_work_bound_v1(
        group_values: &[CanonicalValue],
        measures: &CanonicalRecord,
    ) -> Result<u64, StorageValueError> {
        let bytes = canonical_record_encoded_len(measures)
            .map_err(|e| canonical_codec_storage_error(&e))?;
        let typed = group_values
            .len()
            .checked_mul(size_of::<CanonicalValue>())
            .ok_or(StorageValueError::SizeOverflow)?;
        allowance(
            bytes
                .checked_add(typed)
                .and_then(|b| b.checked_add(2 * MAX_KEY_BYTES + size_of::<Self>()))
                .ok_or(StorageValueError::SizeOverflow)?,
        )
    }

    /// Allocation-free conservative bound before cloning this row. Every typed
    /// collection/string/vector allocation is included, rather than treating
    /// encoded bytes as retained heap size. Original ownership stays charged.
    pub fn clone_work_bound_v1(&self) -> Result<u64, StorageValueError> {
        let mut total = allowance(size_of::<Self>() + MAX_KEY_BYTES)?;
        add(
            &mut total,
            allowance(
                self.group_values
                    .len()
                    .checked_mul(size_of::<CanonicalValue>())
                    .ok_or(StorageValueError::SizeOverflow)?,
            )?,
        )?;
        for value in &self.group_values {
            value_work_bound(value, 0, &mut total)?;
        }
        record_work_bound(&self.measures, 0, &mut total)?;
        Ok(total)
    }
}

impl fmt::Debug for ProjectionSourceRowV1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProjectionSourceRowV1")
            .field("identity", &self.identity)
            .field("group_count", &self.group_values.len())
            .field("measures", &"[REDACTED]")
            .field("last_changed_sequence", &self.last_changed_sequence)
            .finish()
    }
}

// Eight times requested capacity plus allocation slack conservatively covers
// clone growth and the existing owned-allocation inspector's allocator bound.
fn allowance(bytes: usize) -> Result<u64, StorageValueError> {
    u64::try_from(bytes)
        .ok()
        .and_then(|b| b.checked_add(512))
        .and_then(|b| b.checked_mul(8))
        .ok_or(StorageValueError::SizeOverflow)
}
fn add(total: &mut u64, bytes: u64) -> Result<(), StorageValueError> {
    *total = total
        .checked_add(bytes)
        .ok_or(StorageValueError::SizeOverflow)?;
    Ok(())
}
fn record_work_bound(
    record: &CanonicalRecord,
    depth: usize,
    total: &mut u64,
) -> Result<(), StorageValueError> {
    if depth > MAX_NESTING_DEPTH {
        return Err(StorageValueError::LimitExceeded);
    }
    add(
        total,
        allowance(
            record
                .len()
                .checked_mul(size_of::<(riffdb_types::FieldId, CanonicalValue)>())
                .ok_or(StorageValueError::SizeOverflow)?,
        )?,
    )?;
    for (_, value) in record.fields() {
        value_work_bound(value, depth + 1, total)?;
    }
    Ok(())
}
fn value_work_bound(
    value: &CanonicalValue,
    depth: usize,
    total: &mut u64,
) -> Result<(), StorageValueError> {
    if depth > MAX_NESTING_DEPTH {
        return Err(StorageValueError::LimitExceeded);
    }
    match value {
        CanonicalValue::String(v) => add(total, allowance(v.len())?),
        CanonicalValue::Bytes(v) => add(total, allowance(v.len())?),
        CanonicalValue::Vector(v) => add(
            total,
            allowance(
                v.components()
                    .len()
                    .checked_mul(size_of::<f32>())
                    .ok_or(StorageValueError::SizeOverflow)?,
            )?,
        ),
        CanonicalValue::List(v) => {
            add(
                total,
                allowance(
                    v.len()
                        .checked_mul(size_of::<CanonicalValue>())
                        .ok_or(StorageValueError::SizeOverflow)?,
                )?,
            )?;
            for item in v.values() {
                value_work_bound(item, depth + 1, total)?;
            }
            Ok(())
        }
        CanonicalValue::Record(v) => record_work_bound(v, depth, total),
        CanonicalValue::Null
        | CanonicalValue::Bool(_)
        | CanonicalValue::I64(_)
        | CanonicalValue::U64(_)
        | CanonicalValue::Decimal(_)
        | CanonicalValue::Money(_)
        | CanonicalValue::Timestamp(_)
        | CanonicalValue::Date(_)
        | CanonicalValue::Uuid(_)
        | CanonicalValue::Enum { .. } => Ok(()),
    }
}

riffdb_types::inspect_owned_capacity_struct_v1!(ProjectionSourceRowV1 {
    owned: [identity, group_values, measures],
    inline: [locator, last_changed_sequence]
});
