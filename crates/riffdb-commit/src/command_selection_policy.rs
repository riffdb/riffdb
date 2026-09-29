//! Current selected-row policy evidence for deterministic no-mutation failures.
use riffdb_catalog::ResolvedExecutablePlan;
use riffdb_contract_ir::RowPolicyOperationV1;
use riffdb_policy::AuthorizedCommandRowPolicyContextV1;
use riffdb_storage_api::{
    EntityObservation, ExecutionFailureAwaitingDecision, StorageError, StorageErrorKind,
    TransactionCurrentPolicyLookupV1, TransactionCurrentPolicyRequestV1, TransactionCurrentState,
};

pub(super) fn check_fault<A: ExecutionFailureAwaitingDecision>(
    resolved: &ResolvedExecutablePlan,
    context: Option<&AuthorizedCommandRowPolicyContextV1>,
    awaiting: &A,
    current: &TransactionCurrentState,
) -> Result<bool, StorageError> {
    let Some((binding, _)) = resolved.plan().indexed_selection() else {
        return Ok(true);
    };
    let Some(row) = current
        .bindings()
        .iter()
        .find(|row| row.target().entity_type_id() == binding.entity_type())
    else {
        return Ok(true);
    };
    if !super::command_validation::entity_is_policy_protected(resolved, binding.entity_type()) {
        return Ok(true);
    }
    let EntityObservation::Present(row) = row else {
        return Ok(false);
    };
    let Some(context) = context else {
        return Ok(false);
    };
    let operation = RowPolicyOperationV1::Update;
    let lookups = match context.relationship_lookups(
        binding.entity_type(),
        operation,
        Some(row.fields()),
        Some(row.fields()),
    ) {
        Ok(lookups) => lookups,
        Err(_) => return Ok(false),
    };
    let lookups = lookups
        .iter()
        .map(|lookup| {
            TransactionCurrentPolicyLookupV1::new(
                lookup.target_entity(),
                lookup.index_id(),
                lookup.partition().clone(),
                lookup.index_prefix().to_vec(),
            )
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| StorageError::new(StorageErrorKind::InvariantViolation, None))?;
    let request = TransactionCurrentPolicyRequestV1::new(
        context
            .internal_authority()
            .internal_principal()
            .capability_id(),
        lookups,
    )
    .map_err(|_| StorageError::new(StorageErrorKind::InvariantViolation, None))?;
    let evidence = awaiting.read_transaction_current_policy(&request)?;
    Ok(evidence.capability().is_some_and(|capability| {
        super::command_validation::capability_matches_policy_context(context, capability)
    }) && context.allows_transition(
        binding.entity_type(),
        operation,
        Some(row.fields()),
        Some(row.fields()),
        evidence.relationship_exists(),
    ))
}
