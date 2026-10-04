//! Admitted private derived-record normalization with exact lineage checks.
use super::*;
use crate::ActiveCatalogSnapshot;
use riffdb_storage_api::{AdmittedProviderEntityRecordV1, DurableKeySchemaBindingV1};
use riffdb_types::{
    FieldId, OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1, OwnedCapacityInspectionV1,
    exact_provider_budget_v1::{BudgetUnavailable, ProviderBudget},
};
use std::{
    mem::{align_of, size_of},
    sync::Arc,
};

const METADATA_BYTES: u64 = 32 * 1024;

#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderDerivedEntityMaterializationErrorV1 {
    Unavailable,
    Integrity,
}
impl From<BudgetUnavailable> for ProviderDerivedEntityMaterializationErrorV1 {
    fn from(_: BudgetUnavailable) -> Self {
        Self::Unavailable
    }
}
impl fmt::Display for ProviderDerivedEntityMaterializationErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Unavailable => "derived record materialization unavailable",
            Self::Integrity => "derived record materialization integrity failure",
        })
    }
}
impl Error for ProviderDerivedEntityMaterializationErrorV1 {}

fn field_heap(
    record: &StoredEntityRecordV1,
) -> Result<u64, ProviderDerivedEntityMaterializationErrorV1> {
    let report =
        OwnedCapacityInspectionV1::inspect(record.fields()).map_err(|_| BudgetUnavailable)?;
    if !report.shared_allocations().is_empty() {
        return Err(BudgetUnavailable.into());
    }
    Ok(report.unshared_bytes())
}

pub(super) fn maximum_work(
    source: &StoredEntityRecordV1,
    executing_fields: usize,
    heap: u64,
) -> Result<u64, BudgetUnavailable> {
    let count = source
        .fields()
        .len()
        .checked_add(executing_fields)
        .ok_or(BudgetUnavailable)?;
    // Null insertion clones the existing values once. Its top-level field vector
    // may grow geometrically; key validation also constructs one scalar-value
    // vector. Four native field slots per possible output field cover those
    // overlapping buffers. Existing nested value capacities are bounded by the
    // inspected source heap, independently of their encoded lengths.
    let slots = count
        .checked_mul(4)
        .and_then(|count| count.checked_mul(size_of::<(FieldId, CanonicalValue)>()))
        .and_then(|bytes| bytes.checked_mul(2))
        .and_then(|bytes| bytes.checked_add(2 * align_of::<(FieldId, CanonicalValue)>() + 256))
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or(BudgetUnavailable)?;
    // Analysis builds missing-position and inserted-null masks. Each insertion
    // adds exactly six canonical bytes; input and normalized encodings each have
    // at most twice their encoded length in a geometrically grown Vec. The
    // normalized Arc byte copy can overlap its encoding buffer.
    let positions = executing_fields
        .checked_mul(4)
        .and_then(|count| count.checked_mul(size_of::<usize>()))
        .and_then(|bytes| bytes.checked_mul(2))
        .and_then(|bytes| bytes.checked_add(4096))
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or(BudgetUnavailable)?;
    let encoded = executing_fields
        .checked_mul(CANONICAL_NULL_FIELD_BYTES_V1)
        .and_then(|bytes| bytes.checked_add(source.fields_encoded_len()))
        .and_then(|bytes| bytes.checked_mul(8))
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or(BudgetUnavailable)?;
    heap.checked_mul(4)
        .and_then(|bytes| bytes.checked_add(slots))
        .and_then(|bytes| bytes.checked_add(positions))
        .and_then(|bytes| bytes.checked_add(encoded))
        .and_then(|bytes| bytes.checked_add(4 * METADATA_BYTES))
        .and_then(|bytes| bytes.checked_add(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1))
        .ok_or(BudgetUnavailable)
}

impl ActiveCatalogSnapshot {
    /// First-party worker normalization. The input must already belong to this
    /// accounting graph. All ordinary writer, lineage, key and null-fill checks
    /// remain mandatory; admission grants no observation or mutation authority.
    #[doc(hidden)]
    pub fn materialize_derived_entity_admitted_v1(
        &self,
        executing: &DurableKeySchemaBindingV1,
        source: &AdmittedProviderEntityRecordV1,
        budget: &ProviderBudget,
    ) -> Result<AdmittedProviderEntityRecordV1, ProviderDerivedEntityMaterializationErrorV1> {
        materialize_derived_record_admitted_v1(self.lineage_proof(), executing, source, budget)
    }
}

pub(super) fn materialize_derived_record_admitted_v1(
    lineage: &LineageMaterializationProof,
    executing: &DurableKeySchemaBindingV1,
    source: &AdmittedProviderEntityRecordV1,
    budget: &ProviderBudget,
) -> Result<AdmittedProviderEntityRecordV1, ProviderDerivedEntityMaterializationErrorV1> {
    if !source.belongs_to(budget) {
        return Err(BudgetUnavailable.into());
    }
    let sizing = budget.reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1 + METADATA_BYTES)?;
    let (_, bundle) = lineage
        .exact_binding_member(executing)
        .ok_or(ProviderDerivedEntityMaterializationErrorV1::Integrity)?;
    let entity = bundle
        .bundle()
        .schema()
        .entity(source.target().entity_type_id())
        .ok_or(ProviderDerivedEntityMaterializationErrorV1::Integrity)?;
    let heap = field_heap(source)?;
    let mut work = budget.reserve(maximum_work(source, entity.record().fields().len(), heap)?)?;
    drop(sizing);
    let normalized = materialize_derived_record(lineage, executing, (***source).clone())
        .map_err(|_| ProviderDerivedEntityMaterializationErrorV1::Integrity)?;
    if normalized == ***source {
        // No new field owner: reuse the complete admitted original, rather than
        // charge another wrapper for the same shared field allocation.
        return Ok(Arc::clone(source));
    }
    // A semantic change in this closed materializer consists solely of null
    // insertion. It constructs fresh private field/encoding Arcs. Seal their
    // original before any semantic clone can escape, just like admitted decode.
    let original_bytes = field_heap(&normalized)?
        .checked_add(
            u64::try_from(normalized.fields_encoded_len())
                .map_err(|_| BudgetUnavailable)?
                .checked_mul(2)
                .ok_or(BudgetUnavailable)?,
        )
        .and_then(|bytes| bytes.checked_add(METADATA_BYTES))
        .ok_or(BudgetUnavailable)?;
    let root_work = work.split(METADATA_BYTES)?;
    let original = work.split(original_bytes)?.commit_value(normalized)?;
    let record = StoredEntityRecordV1::from_admitted_provider_original_v1(original);
    drop(work);
    root_work.commit_value(record).map_err(Into::into)
}
