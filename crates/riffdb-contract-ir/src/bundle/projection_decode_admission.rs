//! Borrowed framing bounds for complete projection parsing and construction.
// req: OQ-040, PRJ-004
use super::*;
use riffdb_types::{
    OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1,
    exact_provider_budget_v1::{BudgetUnavailable, ProviderAllocation, ProviderBudget},
};
use std::sync::Arc;
use workflow_admission::allocation;

fn add(total: &mut u64, bytes: u64) -> Result<(), BudgetUnavailable> {
    *total = total.checked_add(bytes).ok_or(BudgetUnavailable)?;
    Ok(())
}

#[derive(Default)]
struct Walk {
    heap: u64,
    constructors: u64,
}
impl Walk {
    fn name(&mut self, reader: &mut Reader<'_>) -> Result<(), ProviderBundleEncodingErrorV1> {
        let bytes = reader.bytes(256)?;
        std::str::from_utf8(bytes).map_err(|_| IrValidationError::InvalidText { kind: "IR" })?;
        add(&mut self.heap, allocation::<u8>(bytes.len())?)?;
        Ok(())
    }
    fn field(&mut self, reader: &mut Reader<'_>) -> Result<(), ProviderBundleEncodingErrorV1> {
        reader.u32()?;
        self.name(reader)?;
        add(&mut self.heap, row_policy_admission::type_heap(reader, 0)?)?;
        Ok(())
    }
    fn record(&mut self, reader: &mut Reader<'_>) -> Result<usize, ProviderBundleEncodingErrorV1> {
        decode_record_ref(reader)?;
        let count = decode_len(reader, "record fields", crate::MAX_DECLARATIONS_PER_KIND)?;
        add(&mut self.heap, allocation::<FieldSchema>(count)?)?;
        add(
            &mut self.constructors,
            plan_hash_admission::tree_bound::<&str>(count)?,
        )?;
        for _ in 0..count {
            self.field(reader)?;
        }
        Ok(count)
    }
    fn projection(
        &mut self,
        reader: &mut Reader<'_>,
        schema: &SchemaIr,
    ) -> Result<(), ProviderBundleEncodingErrorV1> {
        reader.u32()?;
        self.name(reader)?;
        reader.read(36)?; // stored hash and source event
        let nodes = reader.peek_u32()? as usize;
        let arena = term_admission::expression_bound(reader, EXECUTABLE_IR_VERSION_V1)?;
        add(&mut self.heap, arena.heap)?;
        add(&mut self.constructors, arena.constructors)?;
        if reader.bool()? {
            reader.u32()?;
        }
        let keys = decode_len(
            reader,
            "projection key expressions",
            crate::MAX_COMMAND_ITEMS,
        )?;
        add(&mut self.heap, allocation::<ExprId>(keys)?)?;
        reader.read(keys.checked_mul(4).ok_or(BudgetUnavailable)?)?;
        let measures = decode_len(reader, "projection measures", crate::MAX_COMMAND_ITEMS)?;
        add(
            &mut self.heap,
            allocation::<ProjectionMeasurePlan>(measures)?,
        )?;
        for _ in 0..measures {
            self.field(reader)?;
            decode_projection_aggregation(reader.u8()?)?;
            if reader.bool()? {
                reader.u32()?;
            }
        }
        decode_projection_frontier(reader.u8()?)?;
        reader.u32()?;
        require_version(
            reader.u32()?,
            crate::PROJECTION_GROUP_CODEC_VERSION_V1,
            "projection group codec",
        )?;
        let components = decode_len(
            reader,
            "projection group components",
            riffdb_types::MAX_PROJECTION_GROUP_COMPONENTS,
        )?;
        add(
            &mut self.heap,
            allocation::<ProjectionGroupComponentSchema>(components)?,
        )?;
        for _ in 0..components {
            add(&mut self.heap, row_policy_admission::type_heap(reader, 0)?)?;
            let variants = decode_len(
                reader,
                "projection enum variants",
                crate::MAX_DECLARATIONS_PER_KIND,
            )?;
            add(&mut self.heap, allocation::<EnumVariantId>(variants)?)?;
            reader.read(variants.checked_mul(4).ok_or(BudgetUnavailable)?)?;
            reader.u32()?;
        }
        let fields = self.record(reader)?;
        reader.read(8)?;
        // Reachability keeps one boolean per node. Every node adds at most two
        // children once. Root collection and subsequent geometric vector growth
        // use at most twice the complete insertion count (or the native minimum).
        add(&mut self.constructors, allocation::<bool>(nodes)?)?;
        let pending = nodes
            .checked_mul(2)
            .and_then(|count| count.checked_add(keys))
            .and_then(|count| count.checked_add(measures))
            .and_then(|count| count.checked_add(1))
            .and_then(|count| count.checked_mul(2))
            .ok_or(BudgetUnavailable)?
            .max(4);
        add(&mut self.constructors, allocation::<ExprId>(pending)?)?;
        // Variant validation copies one declared registry at a time.
        let variants = schema
            .enums()
            .iter()
            .map(|enumeration| enumeration.variants().len())
            .max()
            .unwrap_or(0);
        add(
            &mut self.constructors,
            allocation::<EnumVariantId>(variants)?,
        )?;
        // Hash closure can encounter unvalidated result-type enum IDs. Charge
        // those independently of declared enum count; one type contains at most
        // one enum leaf. Referenced record expansion visits only declared owners.
        let enums = schema
            .enums()
            .len()
            .checked_add(nodes)
            .and_then(|count| count.checked_add(measures))
            .and_then(|count| count.checked_add(components))
            .and_then(|count| count.checked_add(fields))
            .ok_or(BudgetUnavailable)?;
        let records = schema
            .entities()
            .len()
            .checked_add(schema.events().len())
            .and_then(|count| count.checked_add(1))
            .ok_or(BudgetUnavailable)?;
        add(
            &mut self.constructors,
            plan_hash_admission::tree_bound::<EnumTypeId>(enums)?,
        )?;
        add(
            &mut self.constructors,
            plan_hash_admission::tree_bound::<RecordTypeRef>(records)?,
        )?;
        Ok(())
    }
}

pub(super) struct ProjectionSectionBound {
    pub(super) heap: u64,
    pub(super) constructors: u64,
    canonical: u64,
}
impl ProjectionSectionBound {
    pub(super) fn inspect(
        bytes: &[u8],
        schema: &SchemaIr,
    ) -> Result<Self, ProviderBundleEncodingErrorV1> {
        let mut reader = Reader::new(bytes);
        let count = decode_len_with_minimum(
            &mut reader,
            "projections",
            crate::MAX_DECLARATIONS_PER_KIND,
            40,
        )?;
        let mut walk = Walk {
            heap: allocation::<ProjectionPlan>(count)?,
            ..Walk::default()
        };
        for _ in 0..count {
            walk.projection(&mut reader, schema)?;
        }
        reader.finish()?;
        // Ordinary constructor hashing uses a geometrically grown Writer bounded
        // by MAX_BUNDLE_BYTES. At most one preimage survives at a time; its Vec
        // capacity is at most twice the encoded maximum. This is a format bound,
        // not a payload multiplier guessed from encoded plan size.
        if count > 0 {
            add(
                &mut walk.constructors,
                allocation::<u8>(MAX_BUNDLE_BYTES.checked_mul(2).ok_or(BudgetUnavailable)?)?,
            )?;
        }
        Ok(Self {
            heap: walk.heap,
            constructors: walk.constructors,
            canonical: allocation::<u8>(bytes.len())?,
        })
    }
    pub(super) fn maximum(&self) -> Result<u64, BudgetUnavailable> {
        self.heap
            .checked_add(self.constructors.max(self.canonical))
            .and_then(|bytes| bytes.checked_add(32 * 1024))
            .and_then(|bytes| bytes.checked_add(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1))
            .ok_or(BudgetUnavailable)
    }
}

/// First-party admission for the existing projection section. Complete bundle
/// identity, structural schema and borrowed input ownership remain separate.
#[doc(hidden)]
pub fn decode_provider_projection_section_admitted_v1(
    bytes: &[u8],
    schema: &SchemaIr,
    budget: &ProviderBudget,
) -> Result<Arc<ProviderAllocation<Vec<ProjectionPlan>>>, ProviderBundleEncodingErrorV1> {
    checked_len("canonical contract bundle", bytes.len(), MAX_BUNDLE_BYTES)?;
    let sizing = budget.reserve(32 * 1024)?;
    let bound = ProjectionSectionBound::inspect(bytes, schema)?;
    let work = budget.reserve(bound.maximum()?)?;
    drop(sizing);
    let mut reader = Reader::new(bytes);
    let projections = decode_projections(&mut reader, schema)?;
    reader.finish()?;
    {
        let mut writer = Writer::with_capacity(bytes.len(), bytes.len());
        writer.u32(projections.len() as u32)?;
        for projection in &projections {
            encode_projection_bundle_entry(&mut writer, projection)?;
        }
        if writer.finish() != bytes {
            return Err(IrValidationError::NonCanonicalOrder {
                kind: "canonical contract bundle bytes",
            }
            .into());
        }
    }
    work.settle_owned_value(projections).map_err(Into::into)
}
