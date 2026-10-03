//! Admitted enum-dependency forests and exact command/projection hash buffers.
// req: OQ-040, PRJ-004
use super::*;
use riffdb_types::exact_provider_budget_v1::{BudgetUnavailable, ProviderBudget};
use std::mem::{align_of, size_of};

pub(super) fn tree_bound<K>(count: usize) -> Result<u64, BudgetUnavailable> {
    tree_layout_bound::<K, ()>(count)
}

pub(super) fn tree_layout_bound<K, V>(count: usize) -> Result<u64, BudgetUnavailable> {
    // Match the existing ownership inspector's full eleven-slot internal node
    // per entry plus an empty root, including edges, alignment and allocation
    // overhead. Any heap owned by keys and values needs separate admission.
    let node = size_of::<K>()
        .checked_add(size_of::<V>())
        .and_then(|bytes| bytes.checked_mul(11))
        .and_then(|bytes| bytes.checked_add(256 + align_of::<K>() + align_of::<V>()))
        .ok_or(BudgetUnavailable)?;
    count
        .checked_add(1)
        .and_then(|count| count.checked_mul(node))
        .and_then(|bytes| bytes.checked_mul(2))
        .and_then(|bytes| {
            bytes.checked_add(
                2 * align_of::<K>()
                    .max(align_of::<V>())
                    .max(align_of::<usize>())
                    + 256,
            )
        })
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or(BudgetUnavailable)
}

pub(super) fn closure_bound(
    schema: &SchemaIr,
    plan: Option<&CommandPlan>,
) -> Result<u64, BudgetUnavailable> {
    // This path borrows an already validated ContractBundle: enum references
    // are members of the schema's enum registry. Referenced record expansion
    // only follows entity/event owners; command input/outcomes contribute their
    // own roots, and a projection can additionally visit its result record.
    let records = schema
        .entities()
        .len()
        .checked_add(schema.events().len())
        .and_then(|count| count.checked_add(1))
        .and_then(|count| count.checked_add(plan.map_or(0, |plan| plan.outcomes().len())))
        .ok_or(BudgetUnavailable)?;
    let mut bytes = tree_bound::<EnumTypeId>(schema.enums().len())?
        .checked_add(tree_bound::<RecordTypeRef>(records)?)
        .ok_or(BudgetUnavailable)?;
    let (entities, events) = if let Some(plan) = plan {
        (
            plan.bindings()
                .len()
                .checked_add(plan.root_validation_reads().len())
                .ok_or(BudgetUnavailable)?,
            plan.instructions().len(),
        )
    } else {
        (0, 0)
    };
    bytes = bytes
        .checked_add(tree_bound::<EntityTypeId>(entities)?)
        .and_then(|bytes| bytes.checked_add(tree_bound::<EventTypeId>(events).ok()?))
        .ok_or(BudgetUnavailable)?;
    // Traversal and the borrowed canonical length pass allocate no other heap
    // data. Keep bounded root/formatting overhead through preimage encoding.
    bytes.checked_add(32 * 1024).ok_or(BudgetUnavailable)
}

fn encoding_bound(length: usize) -> Result<u64, BudgetUnavailable> {
    length
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(4096))
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or(BudgetUnavailable)
}

fn command_hash(
    plan: &CommandPlan,
    schema: &SchemaIr,
    budget: &ProviderBudget,
) -> Result<PlanHash, ProviderBundleEncodingErrorV1> {
    let _closure = budget.reserve(closure_bound(schema, Some(plan))?)?;
    let forest = collect_command_enum_forest(plan, schema)?;
    let mut counter = Writer::counting(MAX_BUNDLE_BYTES);
    encode_command_plan_prefix(&mut counter, plan, schema)?;
    encode_enum_closure(&mut counter, &forest.enums, schema)?;
    let length = counter.encoded_len();
    let _encoding = budget.reserve(encoding_bound(length)?)?;
    let mut writer = Writer::with_capacity(length, length);
    encode_command_plan_prefix(&mut writer, plan, schema)?;
    encode_enum_closure(&mut writer, &forest.enums, schema)?;
    Ok(hash_plan(&writer.finish()))
}

fn projection_hash(
    plan: &ProjectionPlan,
    schema: &SchemaIr,
    budget: &ProviderBudget,
) -> Result<ProjectionPlanHash, ProviderBundleEncodingErrorV1> {
    let source = schema
        .event(plan.source_event())
        .ok_or(IrValidationError::InvalidReference {
            kind: "projection source event",
        })?;
    let _closure = budget.reserve(closure_bound(schema, None)?)?;
    let forest = collect_projection_enum_forest(plan, source, schema)?;
    let mut counter = Writer::counting(MAX_BUNDLE_BYTES);
    encode_projection_plan_prefix(&mut counter, plan, source)?;
    encode_enum_closure(&mut counter, &forest.enums, schema)?;
    let length = counter.encoded_len();
    let _encoding = budget.reserve(encoding_bound(length)?)?;
    let mut writer = Writer::with_capacity(length, length);
    encode_projection_plan_prefix(&mut writer, plan, source)?;
    encode_enum_closure(&mut writer, &forest.enums, schema)?;
    Ok(hash_projection_plan(&writer.finish()))
}

impl ContractBundle {
    /// Revalidate existing individual plan hashes with admitted temporary work.
    /// Borrowed IR must already have passed bundle construction; this does not
    /// admit parsing, write-evidence validation or schema-artifact validation.
    #[doc(hidden)]
    pub fn validate_provider_plan_hashes_admitted_v1(
        &self,
        budget: &ProviderBudget,
    ) -> Result<(), ProviderBundleEncodingErrorV1> {
        for command in &self.commands {
            if command_hash(command, &self.schema, budget)? != command.plan_hash() {
                return Err(IrValidationError::HashMismatch {
                    kind: "command plan",
                }
                .into());
            }
        }
        for projection in &self.projections {
            if projection_hash(projection, &self.schema, budget)? != projection.plan_hash() {
                return Err(IrValidationError::HashMismatch {
                    kind: "projection plan",
                }
                .into());
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn provider_hash_tree_and_encoding_bounds_refuse_overflow() {
        assert_eq!(
            tree_bound::<RecordTypeRef>(usize::MAX),
            Err(BudgetUnavailable)
        );
        assert_eq!(encoding_bound(usize::MAX), Err(BudgetUnavailable));
    }
}
