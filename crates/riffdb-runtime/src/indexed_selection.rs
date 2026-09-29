//! Pure validation and empty-result execution for indexed selectors.
use super::*;

pub(super) fn validate_snapshot(
    schema: &SchemaIr,
    plan: &CommandPlan,
    input: &InputDerivedCommandFacts,
    snapshot: &ReadSnapshot,
    context: &TransactionContext,
) -> Result<(), ExecutionFault> {
    let selection = riffdb_storage_api::IndexedSelectionRequest::from_plan(
        plan,
        schema,
        input.partition_key().clone(),
        input
            .selection_prefix_values()
            .ok_or(ExecutionFault::Integrity)?,
    )
    .map_err(|_| ExecutionFault::Integrity)?;
    // Storage and catalog reconstruct the entry from the raw stored row before
    // normalization. Runtime must retain that physical range evidence unchanged:
    // a historical missing optional value may now be materialized as null.
    let range = snapshot.ranges().first().ok_or(ExecutionFault::Integrity)?;
    if range.target() != selection.range() || range.entries().len() > 1 {
        return Err(ExecutionFault::Integrity);
    }
    let key = if let Some(entry) = range.entries().first() {
        let target = selection
            .selected_target(entry)
            .map_err(|_| ExecutionFault::Integrity)?;
        let (binding, _) = plan.indexed_selection().ok_or(ExecutionFault::Integrity)?;
        let row = snapshot
            .bindings()
            .get(binding.id().get() as usize)
            .ok_or(ExecutionFault::Integrity)?;
        if row.target() != &target
            || !matches!(row, riffdb_storage_api::EntityObservation::Present(_))
        {
            return Err(ExecutionFault::Integrity);
        }
        Some(row.target().key())
    } else {
        None
    };
    let facts = riffdb_invariant::resolve_selected_command_targets(plan, schema, input, key)
        .map_err(|_| ExecutionFault::Integrity)?;
    if context.partition_key() != facts.partition_key()
        || snapshot.ranges().len() != 1
        || !snapshot.cascade_predecessors().is_empty()
        || snapshot.bindings().len() != facts.binding_entity_keys().len()
        || snapshot.root_validations().len() != facts.root_validation_entity_keys().len()
        || snapshot
            .bindings()
            .iter()
            .zip(facts.binding_entity_keys())
            .any(|(row, key)| row.target().key() != key)
        || snapshot
            .root_validations()
            .iter()
            .zip(facts.root_validation_entity_keys())
            .any(|(row, key)| row.target().key() != key)
    {
        return Err(ExecutionFault::Integrity);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn empty_outcome(
    bundle: &ContractBundle,
    plan: &CommandPlan,
    input: &CanonicalRecord,
    snapshot: &ReadSnapshot,
    context: &TransactionContext,
    budget: EvaluationBudget,
    evaluator: &mut ExpressionEvaluator<'_>,
) -> Result<Option<ExecutionResult>, ExecutionFault> {
    if snapshot
        .ranges()
        .first()
        .is_none_or(|range| !range.entries().is_empty())
    {
        return Ok(None);
    }
    let (selected, _) = plan.indexed_selection().ok_or(ExecutionFault::Integrity)?;
    let mut records = vec![None; plan.bindings().len()];
    let branches = decision_branch_bindings(plan);
    let mut failed = selected;
    for (index, binding) in plan
        .bindings()
        .iter()
        .take(selected.id().get() as usize)
        .enumerate()
    {
        if branches.contains(&binding.id()) {
            continue;
        }
        let observation = snapshot
            .bindings()
            .get(index)
            .ok_or(ExecutionFault::Integrity)?;
        if !materialize_ordinary_decision_binding(
            bundle,
            plan,
            input,
            context,
            evaluator,
            &mut records,
            index,
            binding,
            observation,
        )? {
            failed = binding;
            break;
        }
    }
    let values = RuntimeValues {
        schema: bundle.schema(),
        plan,
        input,
        records: &records,
        roots: &[],
        tx_time: context.tx_time(),
        service_values: context.service_values(),
    };
    let outcome = construct_outcome(
        failed.failure().ok_or(ExecutionFault::Integrity)?,
        &mut evaluator.batch(&values),
    )?;
    finish_declared(plan, snapshot, budget, outcome, vec![], vec![], vec![]).map(Some)
}
