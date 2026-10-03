//! Borrowed admission for the existing workflow section and its constructors.
// req: OQ-040, PRJ-004
use super::*;
use riffdb_types::{
    OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1,
    exact_provider_budget_v1::{BudgetUnavailable, ProviderAllocation, ProviderBudget},
};
use std::{
    mem::{align_of, size_of},
    sync::Arc,
};

fn allocation<T>(count: usize) -> Result<u64, BudgetUnavailable> {
    if count == 0 {
        return Ok(0);
    }
    count
        .checked_mul(size_of::<T>())
        .and_then(|bytes| bytes.checked_mul(2))
        .and_then(|bytes| bytes.checked_add(2 * align_of::<T>() + 256))
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or(BudgetUnavailable)
}
fn add(total: &mut u64, bytes: u64) -> Result<(), BudgetUnavailable> {
    *total = total.checked_add(bytes).ok_or(BudgetUnavailable)?;
    Ok(())
}
fn name(reader: &mut Reader<'_>, heap: &mut u64) -> Result<(), ProviderBundleEncodingErrorV1> {
    let bytes = reader.bytes(256)?;
    std::str::from_utf8(bytes).map_err(|_| IrValidationError::InvalidText { kind: "IR" })?;
    add(heap, allocation::<u8>(bytes.len())?)?;
    Ok(())
}

pub(super) struct WorkflowSectionBound {
    pub(super) heap: u64,
    pub(super) constructors: u64,
    pub(super) canonical: u64,
}
impl WorkflowSectionBound {
    pub(super) fn inspect(
        bytes: &[u8],
        schema: &SchemaIr,
        ir_version: u32,
    ) -> Result<Self, ProviderBundleEncodingErrorV1> {
        let mut reader = Reader::new(bytes);
        let count = decode_len_with_minimum(
            &mut reader,
            "workflows",
            crate::MAX_DECLARATIONS_PER_KIND,
            21,
        )?;
        let mut heap = allocation::<WorkflowSchema>(count)?;
        let mut largest_edges = 0;
        let mut largest_variants = 0;
        for _ in 0..count {
            name(&mut reader, &mut heap)?;
            reader.u32()?; // entity, semantically checked by ordinary decoding
            reader.u32()?; // state field
            let state_enum = EnumTypeId::new(reader.u32()?);
            let variants = state_enum
                .and_then(|id| schema.enumeration(id))
                .map_or(0, |value| value.variants().len());
            largest_variants =
                largest_variants.max(plan_hash_admission::tree_bound::<EnumVariantId>(variants)?);
            if ir_version >= EXECUTABLE_IR_VERSION_V9 && reader.bool()? {
                reader.u32()?;
            }
            let transitions = decode_len_with_minimum(
                &mut reader,
                "workflow transitions",
                crate::MAX_DECLARATIONS_PER_KIND,
                17,
            )?;
            add(
                &mut heap,
                allocation::<WorkflowTransitionSchema>(transitions)?,
            )?;
            let mut edges = 0usize;
            for _ in 0..transitions {
                name(&mut reader, &mut heap)?;
                let states = decode_len_with_minimum(
                    &mut reader,
                    "workflow transition source states",
                    crate::MAX_DECLARATIONS_PER_KIND,
                    4,
                )?;
                add(&mut heap, allocation::<EnumVariantId>(states)?)?;
                edges = edges.checked_add(states).ok_or(BudgetUnavailable)?;
                let length = states.checked_mul(4).ok_or(BudgetUnavailable)?;
                reader.read(length)?;
                reader.u32()?; // destination
            }
            largest_edges = largest_edges.max(plan_hash_admission::tree_bound::<(
                EnumVariantId,
                EnumVariantId,
            )>(edges)?);
            if reader.bool()? {
                name(&mut reader, &mut heap)?;
                reader.read(12)?; // owner/expiry/fencing fields
                if reader.bool()? {
                    reader.u32()?;
                }
                reader.read(16)?; // lease duration bounds
            }
        }
        reader.finish()?;
        let catalog = plan_hash_admission::tree_bound::<EntityTypeId>(count)?
            .checked_add(largest_variants)
            .ok_or(BudgetUnavailable)?;
        // Per-workflow edge sets are destroyed before catalog validation. The
        // catalog's entity set overlaps only one enum-variant set at a time.
        Ok(Self {
            heap,
            constructors: largest_edges.max(catalog),
            canonical: allocation::<u8>(bytes.len())?,
        })
    }
    pub(super) fn maximum(&self) -> Result<u64, BudgetUnavailable> {
        // The decoded catalog survives constructor validation and canonical
        // verification. Those temporary phases do not overlap. Fixed work
        // covers lease-field vector growth, roots and immutable owner metadata.
        self.heap
            .checked_add(self.constructors.max(self.canonical))
            .and_then(|bytes| bytes.checked_add(32 * 1024))
            .and_then(|bytes| bytes.checked_add(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1))
            .ok_or(BudgetUnavailable)
    }
}

/// Decode the existing workflow section with admitted parser/constructor work.
/// This is a first-party fragment reader, not whole-bundle admission or a new
/// durable format. Structural schema and complete bundle identity remain separate.
#[doc(hidden)]
pub fn decode_provider_workflow_section_admitted_v1(
    bytes: &[u8],
    schema: &SchemaIr,
    ir_version: u32,
    budget: &ProviderBudget,
) -> Result<Arc<ProviderAllocation<WorkflowCatalog>>, ProviderBundleEncodingErrorV1> {
    checked_len("canonical contract bundle", bytes.len(), MAX_BUNDLE_BYTES)?;
    if !(EXECUTABLE_IR_VERSION_V2..=EXECUTABLE_IR_VERSION_V26).contains(&ir_version) {
        return Err(IrValidationError::UnsupportedVersion {
            kind: "workflow executable IR",
            value: ir_version,
        }
        .into());
    }
    let sizing = budget.reserve(32 * 1024)?;
    let bound = WorkflowSectionBound::inspect(bytes, schema, ir_version)?;
    let work = budget.reserve(bound.maximum()?)?;
    drop(sizing);
    let mut reader = Reader::new(bytes);
    let workflows = decode_workflows(&mut reader, schema, ir_version)?;
    reader.finish()?;
    {
        let mut writer = Writer::with_capacity(bytes.len(), bytes.len());
        encode_workflows(&mut writer, &workflows, ir_version)?;
        if writer.finish() != bytes {
            return Err(IrValidationError::NonCanonicalOrder {
                kind: "canonical contract bundle bytes",
            }
            .into());
        }
    }
    work.settle_owned_value(workflows).map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn workflow_admission_bounds_refuse_overflow() {
        assert_eq!(
            allocation::<WorkflowSchema>(usize::MAX),
            Err(BudgetUnavailable)
        );
        let bound = WorkflowSectionBound {
            heap: u64::MAX,
            constructors: 1,
            canonical: 0,
        };
        assert_eq!(bound.maximum(), Err(BudgetUnavailable));
    }
}
