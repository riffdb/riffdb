//! Admitted enum-dependency forests and exact command/projection hash buffers.
// req: OQ-040, PRJ-004
use super::*;
use riffdb_types::exact_provider_budget_v1::{
    BudgetUnavailable, ProjectionSourceWorkReservationV1, ProjectionSourceWorkingBudgetV1,
    ProviderBudget, Reservation,
};
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
        let mut events = plan.instructions().len();
        for decision in plan.decisions() {
            for action in decision
                .when_arms()
                .iter()
                .map(CommandDecisionArmV1::action)
                .chain(std::iter::once(decision.else_action()))
            {
                events = events
                    .checked_add(action.instructions().len())
                    .ok_or(BudgetUnavailable)?;
            }
        }
        (
            plan.bindings()
                .len()
                .checked_add(plan.root_validation_reads().len())
                .ok_or(BudgetUnavailable)?,
            events,
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

// The two closed caller families share the existing exact counting encoder,
// enum forest and hash functions. This enum cannot accept an application
// callback, create a budget, or switch a public Source decode to ordinary work.
enum PlanHashAdmissionV1<'a> {
    Provider(&'a ProviderBudget),
    Source(&'a ProjectionSourceWorkingBudgetV1),
}
// Every alternative owns the actual family's reservation through the last
// temporary forest/encoding/error destruction. Numeric admission is not a plan
// validation, storage capability or Source publication proof.
enum PlanHashWorkV1 {
    Provider {
        _work: Reservation,
    },
    Source {
        _work: ProjectionSourceWorkReservationV1,
    },
}
impl PlanHashAdmissionV1<'_> {
    fn reserve(&self, bytes: u64) -> Result<PlanHashWorkV1, BudgetUnavailable> {
        match self {
            Self::Provider(budget) => Ok(PlanHashWorkV1::Provider {
                _work: budget.reserve(bytes)?,
            }),
            Self::Source(budget) => Ok(PlanHashWorkV1::Source {
                _work: budget.reserve(bytes)?,
            }),
        }
    }
}

// The existing semantic encoder materializes two schema artifacts, then
// retains both canonical Strings while visiting its schema-closure vectors.
// JSON trees are sequential: never price two whole trees as simultaneous.
// The caller admits the existing sizing workspace BEFORE calling this method.
pub(super) fn command_encoding_work_bound(
    plan: &CommandPlan,
    schema: &SchemaIr,
) -> Result<u64, ProviderBundleEncodingErrorV1> {
    use super::source_decode_sink::source_growing_vector_bound;
    use crate::ProviderSchemaArtifactSourceV1 as Source;
    let input = Source::CommandInput {
        id: plan.command_id(),
        record: plan.input().record(),
        idempotency_input: plan.idempotency_input(),
    }
    .expected_size(schema)?;
    let output = Source::CommandOutcomes {
        id: plan.command_id(),
        outcomes: plan.outcomes(),
    }
    .expected_size(schema)?;
    checked_len(
        "generated JSON Schema",
        input,
        crate::MAX_JSON_SCHEMA_ARTIFACT_BYTES,
    )?;
    checked_len(
        "generated JSON Schema",
        output,
        crate::MAX_JSON_SCHEMA_ARTIFACT_BYTES,
    )?;
    let input_retained = super::workflow_admission::allocation::<u8>(input)?;
    let output_retained = super::workflow_admission::allocation::<u8>(output)?;
    let input_generation = crate::json_schema::json_workspace_bound(input)?;
    let output_generation = crate::json_schema::json_workspace_bound(output)?;
    let entities = plan
        .bindings()
        .len()
        .checked_add(plan.root_validation_reads().len())
        .ok_or(BudgetUnavailable)?;
    let mut events = plan.instructions().len();
    for decision in plan.decisions() {
        for action in decision
            .when_arms()
            .iter()
            .map(CommandDecisionArmV1::action)
            .chain(std::iter::once(decision.else_action()))
        {
            events = events
                .checked_add(action.instructions().len())
                .ok_or(BudgetUnavailable)?;
        }
    }
    let closure = source_growing_vector_bound::<EntityTypeId>(entities)?
        .max(source_growing_vector_bound::<EventTypeId>(events)?);
    let both_outputs_and_closure = input_retained
        .checked_add(output_retained)
        .and_then(|bytes| bytes.checked_add(closure))
        .ok_or(BudgetUnavailable)?;
    Ok(input_generation
        .max(
            input_retained
                .checked_add(output_generation)
                .ok_or(BudgetUnavailable)?,
        )
        .max(both_outputs_and_closure))
}

// Schema and delete-check encoding borrow their modes; the only semantic heap
// temporaries of the canonical bundle encoder are the sequential command work.
pub(super) fn bundle_encoding_work_bound(
    bundle: &ContractBundle,
) -> Result<u64, ProviderBundleEncodingErrorV1> {
    let mut maximum = 0u64;
    for command in &bundle.commands {
        maximum = maximum.max(command_encoding_work_bound(command, &bundle.schema)?);
    }
    Ok(maximum)
}

fn command_hash(
    plan: &CommandPlan,
    schema: &SchemaIr,
    admission: &PlanHashAdmissionV1<'_>,
) -> Result<PlanHash, ProviderBundleEncodingErrorV1> {
    let sizing = admission.reserve(32 * 1024)?;
    let json_workspace = command_encoding_work_bound(plan, schema)?;
    let _semantic_encoding = admission.reserve(json_workspace)?;
    drop(sizing);
    let _closure = admission.reserve(closure_bound(schema, Some(plan))?)?;
    let forest = collect_command_enum_forest(plan, schema)?;
    let mut counter = Writer::counting(MAX_BUNDLE_BYTES);
    encode_command_plan_prefix(&mut counter, plan, schema)?;
    encode_enum_closure(&mut counter, &forest.enums, schema)?;
    let length = counter.encoded_len();
    let _encoding = admission.reserve(encoding_bound(length)?)?;
    let mut writer = Writer::with_capacity(length, length);
    encode_command_plan_prefix(&mut writer, plan, schema)?;
    encode_enum_closure(&mut writer, &forest.enums, schema)?;
    Ok(hash_plan(&writer.finish()))
}

fn projection_hash(
    plan: &ProjectionPlan,
    schema: &SchemaIr,
    admission: &PlanHashAdmissionV1<'_>,
) -> Result<ProjectionPlanHash, ProviderBundleEncodingErrorV1> {
    let source = schema
        .event(plan.source_event())
        .ok_or(IrValidationError::InvalidReference {
            kind: "projection source event",
        })?;
    let _closure = admission.reserve(closure_bound(schema, None)?)?;
    let forest = collect_projection_enum_forest(plan, source, schema)?;
    let mut counter = Writer::counting(MAX_BUNDLE_BYTES);
    encode_projection_plan_prefix(&mut counter, plan, source)?;
    encode_enum_closure(&mut counter, &forest.enums, schema)?;
    let length = counter.encoded_len();
    let _encoding = admission.reserve(encoding_bound(length)?)?;
    let mut writer = Writer::with_capacity(length, length);
    encode_projection_plan_prefix(&mut writer, plan, source)?;
    encode_enum_closure(&mut writer, &forest.enums, schema)?;
    Ok(hash_projection_plan(&writer.finish()))
}

// Initial Source constructors and the final Source bundle validator call the
// same exact kernel. The containing owning decoder must already have prepaid
// the plan/schema graph and its structural constructor workspace.
pub(super) fn source_projection_hash(
    plan: &ProjectionPlan,
    schema: &SchemaIr,
    budget: &ProjectionSourceWorkingBudgetV1,
) -> Result<ProjectionPlanHash, ProviderBundleEncodingErrorV1> {
    projection_hash(plan, schema, &PlanHashAdmissionV1::Source(budget))
}

pub(super) fn source_command_hash(
    plan: &CommandPlan,
    schema: &SchemaIr,
    budget: &ProjectionSourceWorkingBudgetV1,
) -> Result<PlanHash, ProviderBundleEncodingErrorV1> {
    command_hash(plan, schema, &PlanHashAdmissionV1::Source(budget))
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
            if command_hash(
                command,
                &self.schema,
                &PlanHashAdmissionV1::Provider(budget),
            )? != command.plan_hash()
            {
                return Err(IrValidationError::HashMismatch {
                    kind: "command plan",
                }
                .into());
            }
        }
        for projection in &self.projections {
            if projection_hash(
                projection,
                &self.schema,
                &PlanHashAdmissionV1::Provider(budget),
            )? != projection.plan_hash()
            {
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
