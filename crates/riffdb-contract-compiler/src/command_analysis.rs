//! Grammar-v1 command semantic validation over resolved typed HIR.

use std::collections::{BTreeMap, BTreeSet};

use riffdb_contract_ir::{BindingId, BindingMode, ExpressionKind, ValueTypeTag};
use riffdb_types::FieldId;

use crate::diagnostic::{CompilerDiagnostic, CompilerDiagnosticCode, CompilerDiagnostics};
use crate::hir::{
    HirCommand, HirEffect, HirExpressionRoot, HirObjectField, HirOutcome,
    HirWorkflowLeaseOperation, TypedContractHir,
};
use crate::locality::command_expression_fingerprint;

fn lease_roots(operation: &HirWorkflowLeaseOperation) -> Vec<&HirExpressionRoot> {
    match operation {
        HirWorkflowLeaseOperation::Claim {
            owner,
            duration_seconds,
            expected_revision,
            ..
        } => vec![owner, duration_seconds, expected_revision],
        HirWorkflowLeaseOperation::Renew {
            owner,
            fencing_token,
            duration_seconds,
            expected_revision,
            ..
        } => vec![owner, fencing_token, duration_seconds, expected_revision],
        HirWorkflowLeaseOperation::Release {
            owner,
            fencing_token,
            expected_revision,
            ..
        }
        | HirWorkflowLeaseOperation::Fence {
            owner,
            fencing_token,
            expected_revision,
            ..
        } => vec![owner, fencing_token, expected_revision],
        HirWorkflowLeaseOperation::Expire {
            expected_revision, ..
        } => vec![expected_revision],
    }
}

fn lease_outcomes(operation: &HirWorkflowLeaseOperation) -> Vec<&HirOutcome> {
    match operation {
        HirWorkflowLeaseOperation::Claim {
            stale,
            unavailable,
            invalid,
            exhausted,
            ..
        } => vec![stale, unavailable, invalid, exhausted],
        HirWorkflowLeaseOperation::Renew {
            stale,
            invalid,
            expired,
            exhausted,
            ..
        } => vec![stale, invalid, expired, exhausted],
        HirWorkflowLeaseOperation::Release { stale, invalid, .. } => vec![stale, invalid],
        HirWorkflowLeaseOperation::Expire { stale, active, .. } => vec![stale, active],
        HirWorkflowLeaseOperation::Fence {
            stale,
            invalid,
            expired,
            ..
        } => vec![stale, invalid, expired],
    }
}

#[derive(Clone, Debug)]
struct BindingState {
    mode: BindingMode,
    key_fields: BTreeSet<FieldId>,
    initialized_fields: BTreeSet<FieldId>,
    required_create_fields: BTreeSet<FieldId>,
}

/// Validates all command-local semantics and dependency visibility.
pub(crate) fn validate_commands(hir: &TypedContractHir) -> Result<(), CompilerDiagnostics> {
    let mut diagnostics = Vec::new();
    for command in &hir.commands {
        validate_command(hir, command, &mut diagnostics);
    }
    if diagnostics.is_empty() {
        Ok(())
    } else {
        Err(CompilerDiagnostics::new(diagnostics).expect("nonempty diagnostics"))
    }
}

fn validate_command(
    hir: &TypedContractHir,
    command: &HirCommand,
    diagnostics: &mut Vec<CompilerDiagnostic>,
) {
    for binding in command
        .bindings
        .iter()
        .filter(|binding| binding.mode == BindingMode::Delete)
    {
        if hir
            .entity(binding.entity_id)
            .is_some_and(|entity| entity.delete_policy.is_none())
        {
            diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::InvalidDeletePolicy,
                binding.entity_span,
            ));
        }
    }
    let mut restrict_failures = command
        .bindings
        .iter()
        .filter_map(|binding| binding.restriction_failure.as_ref());
    let _first_restrict_failure = restrict_failures.next();
    for failure in restrict_failures {
        diagnostics.push(CompilerDiagnostic::new(
            CompilerDiagnosticCode::InvalidDeletePolicy,
            failure.span,
        ));
    }
    validate_binding_ownership(command, diagnostics);
    validate_relationship_reads(hir, command, diagnostics);
    validate_unique_conflicts(hir, command, diagnostics);
    let secret_input = validate_idempotency(command, diagnostics);
    let mut states = command
        .bindings
        .iter()
        .filter_map(|binding| {
            let entity = hir.entity(binding.entity_id)?;
            let key_fields = entity.key_field_set();
            let initialized_fields = if binding.mode == BindingMode::Create {
                entity
                    .fields
                    .iter()
                    .filter_map(|field| {
                        (key_fields.contains(&field.id) || field.value_type.is_optional())
                            .then_some(field.id)
                    })
                    .collect()
            } else {
                entity.fields.iter().map(|field| field.id).collect()
            };
            let required_create_fields = if binding.mode == BindingMode::Create {
                entity
                    .fields
                    .iter()
                    .filter_map(|field| {
                        (!key_fields.contains(&field.id) && !field.value_type.is_optional())
                            .then_some(field.id)
                    })
                    .collect()
            } else {
                BTreeSet::new()
            };
            Some((
                binding.id,
                BindingState {
                    mode: binding.mode,
                    key_fields,
                    initialized_fields,
                    required_create_fields,
                },
            ))
        })
        .collect::<BTreeMap<_, _>>();

    let mut influential_roots = Vec::new();
    for binding in &command.bindings {
        influential_roots.extend(binding.arguments.iter());
        influential_roots.extend(binding.failure.fields.iter().map(|field| &field.value));
        if let Some(failure) = &binding.restriction_failure {
            influential_roots.extend(failure.fields.iter().map(|field| &field.value));
        }
    }
    let mut outcomes = command
        .bindings
        .iter()
        .flat_map(|binding| {
            std::iter::once(&binding.failure).chain(binding.restriction_failure.iter())
        })
        .collect::<Vec<_>>();
    for requirement in &command.requirements {
        validate_create_reads(command, &states, &requirement.condition, diagnostics);
        validate_create_object_reads(command, &states, &requirement.rejection.fields, diagnostics);
        influential_roots.push(&requirement.condition);
        influential_roots.extend(
            requirement
                .rejection
                .fields
                .iter()
                .map(|field| &field.value),
        );
        outcomes.push(&requirement.rejection);
    }

    let mut written = BTreeSet::new();
    for effect in &command.effects {
        match effect {
            HirEffect::Set {
                target_span,
                binding,
                field,
                value,
                ..
            } => {
                let Some(state) = states.get(binding) else {
                    diagnostics.push(CompilerDiagnostic::new(
                        CompilerDiagnosticCode::UnknownName,
                        *target_span,
                    ));
                    continue;
                };
                if state.mode == BindingMode::Read
                    || state.key_fields.contains(field)
                    || !written.insert((*binding, *field))
                {
                    diagnostics.push(CompilerDiagnostic::new(
                        CompilerDiagnosticCode::InvalidMutation,
                        *target_span,
                    ));
                    continue;
                }
                validate_create_reads(command, &states, value, diagnostics);
                influential_roots.push(value);
                states
                    .get_mut(binding)
                    .expect("binding state exists")
                    .initialized_fields
                    .insert(*field);
            }
            HirEffect::Emit { fields, .. } => {
                validate_create_object_reads(command, &states, fields, diagnostics);
                influential_roots.extend(fields.iter().map(|field| &field.value));
            }
            HirEffect::WorkflowTransition {
                transition_span,
                binding,
                state_field,
                expected_revision,
                stale,
                illegal,
                ..
            } => {
                let Some(state) = states.get(binding) else {
                    diagnostics.push(CompilerDiagnostic::new(
                        CompilerDiagnosticCode::UnknownName,
                        *transition_span,
                    ));
                    continue;
                };
                if state.mode != BindingMode::Mutate
                    || state.key_fields.contains(state_field)
                    || !written.insert((*binding, *state_field))
                {
                    diagnostics.push(CompilerDiagnostic::new(
                        CompilerDiagnosticCode::InvalidWorkflowTransition,
                        *transition_span,
                    ));
                    continue;
                }
                validate_create_reads(command, &states, expected_revision, diagnostics);
                validate_create_object_reads(command, &states, &stale.fields, diagnostics);
                validate_create_object_reads(command, &states, &illegal.fields, diagnostics);
                influential_roots.push(expected_revision);
                influential_roots.extend(stale.fields.iter().map(|field| &field.value));
                influential_roots.extend(illegal.fields.iter().map(|field| &field.value));
                outcomes.push(stale);
                outcomes.push(illegal);
            }
            HirEffect::WorkflowLease {
                lease_span,
                binding,
                owner_field,
                expiry_field,
                fencing_token_field,
                attempt_field,
                operation,
                ..
            } => {
                let Some(state) = states.get(binding) else {
                    diagnostics.push(CompilerDiagnostic::new(
                        CompilerDiagnosticCode::UnknownName,
                        *lease_span,
                    ));
                    continue;
                };
                let written_fields = match operation.as_ref() {
                    HirWorkflowLeaseOperation::Claim { .. } => [
                        Some(*owner_field),
                        Some(*expiry_field),
                        Some(*fencing_token_field),
                        *attempt_field,
                    ],
                    HirWorkflowLeaseOperation::Renew { .. } => {
                        [Some(*expiry_field), None, None, None]
                    }
                    HirWorkflowLeaseOperation::Release { .. }
                    | HirWorkflowLeaseOperation::Expire { .. } => {
                        [Some(*owner_field), Some(*expiry_field), None, None]
                    }
                    HirWorkflowLeaseOperation::Fence { .. } => [None, None, None, None],
                };
                if state.mode != BindingMode::Mutate
                    || written_fields.into_iter().flatten().any(|field| {
                        state.key_fields.contains(&field) || !written.insert((*binding, field))
                    })
                {
                    diagnostics.push(CompilerDiagnostic::new(
                        CompilerDiagnosticCode::InvalidWorkflowLease,
                        *lease_span,
                    ));
                    continue;
                }
                for root in lease_roots(operation) {
                    validate_create_reads(command, &states, root, diagnostics);
                    influential_roots.push(root);
                }
                for outcome in lease_outcomes(operation) {
                    validate_create_object_reads(command, &states, &outcome.fields, diagnostics);
                    influential_roots.extend(outcome.fields.iter().map(|field| &field.value));
                    outcomes.push(outcome);
                }
            }
        }
    }
    for binding in command
        .bindings
        .iter()
        .filter(|binding| binding.mode == BindingMode::Mutate)
    {
        let lease_protected = hir
            .workflows
            .iter()
            .any(|workflow| workflow.entity_id == binding.entity_id && workflow.lease.is_some());
        let has_fence = command.effects.iter().any(|effect| {
            matches!(
                effect,
                HirEffect::WorkflowLease {
                    binding: lease_binding,
                    ..
                } if *lease_binding == binding.id
            )
        });
        if lease_protected && !has_fence {
            diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::InvalidWorkflowLease,
                binding.span,
            ));
        }
    }
    validate_create_object_reads(command, &states, &command.success.fields, diagnostics);
    influential_roots.extend(command.success.fields.iter().map(|field| &field.value));
    outcomes.push(&command.success);

    for state in states.values() {
        if state.mode == BindingMode::Create
            && !state
                .required_create_fields
                .is_subset(&state.initialized_fields)
        {
            diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::InvalidCreation,
                command.span,
            ));
        }
    }
    validate_outcome_shapes(&outcomes, diagnostics);
    validate_secret_taint(secret_input, command, &influential_roots, diagnostics);
}

fn validate_unique_conflicts(
    hir: &TypedContractHir,
    command: &HirCommand,
    diagnostics: &mut Vec<CompilerDiagnostic>,
) {
    let assignments = command
        .effects
        .iter()
        .filter_map(|effect| match effect {
            HirEffect::Set {
                binding,
                field,
                value,
                ..
            } => Some(((*binding, *field), value)),
            HirEffect::Emit { .. } => None,
            HirEffect::WorkflowTransition { .. } | HirEffect::WorkflowLease { .. } => None,
        })
        .collect::<BTreeMap<_, _>>();
    for binding in command
        .bindings
        .iter()
        .filter(|binding| matches!(binding.mode, BindingMode::Create | BindingMode::Mutate))
    {
        let Some(entity) = hir.entity(binding.entity_id) else {
            continue;
        };
        for unique in entity.indexes.iter().filter(|index| index.unique) {
            let changes = binding.mode == BindingMode::Create
                || unique
                    .fields
                    .iter()
                    .any(|field| assignments.contains_key(&(binding.id, field.0)));
            if !changes {
                continue;
            }
            let values = unique
                .fields
                .iter()
                .map(|field| {
                    assignments
                        .get(&(binding.id, field.0))
                        .copied()
                        .or_else(|| {
                            entity
                                .key_fields
                                .iter()
                                .position(|key| *key == field.0)
                                .and_then(|position| binding.arguments.get(position))
                        })
                })
                .collect::<Option<Vec<_>>>();
            let input_computable = values.is_some_and(|values| {
                values.iter().all(|value| {
                    command
                        .expressions
                        .dependencies(value.id, value.span)
                        .is_ok_and(|dependencies| dependencies.is_input_computable())
                })
            });
            if !input_computable {
                diagnostics.push(CompilerDiagnostic::new(
                    CompilerDiagnosticCode::UniqueKeyNotInputComputable,
                    unique.span,
                ));
            }
        }
    }
}

fn validate_relationship_reads(
    hir: &TypedContractHir,
    command: &HirCommand,
    diagnostics: &mut Vec<CompilerDiagnostic>,
) {
    let assignments = command
        .effects
        .iter()
        .filter_map(|effect| match effect {
            HirEffect::Set {
                binding,
                field,
                value,
                ..
            } => Some(((*binding, *field), value)),
            HirEffect::Emit { .. } => None,
            HirEffect::WorkflowTransition { .. } | HirEffect::WorkflowLease { .. } => None,
        })
        .collect::<BTreeMap<_, _>>();

    for source_binding in &command.bindings {
        if !matches!(
            source_binding.mode,
            BindingMode::Create | BindingMode::Mutate
        ) {
            continue;
        }
        let Some(source_entity) = hir.entity(source_binding.entity_id) else {
            continue;
        };
        for relationship in &source_entity.relationships {
            let changes_relationship = source_binding.mode == BindingMode::Create
                || relationship
                    .source_fields
                    .iter()
                    .any(|field| assignments.contains_key(&(source_binding.id, field.0)));
            if !changes_relationship {
                continue;
            }
            let resulting_values = relationship
                .source_fields
                .iter()
                .map(|field| {
                    assignments
                        .get(&(source_binding.id, field.0))
                        .copied()
                        .or_else(|| {
                            source_entity
                                .key_fields
                                .iter()
                                .position(|key| *key == field.0)
                                .and_then(|position| source_binding.arguments.get(position))
                        })
                })
                .collect::<Option<Vec<_>>>();
            let qualifying_target = resulting_values.as_ref().is_some_and(|values| {
                command.bindings.iter().any(|target| {
                    target.id < source_binding.id
                        && matches!(
                            target.mode,
                            BindingMode::Read | BindingMode::Mutate | BindingMode::Create
                        )
                        && target.entity_id == relationship.target_entity
                        && target.arguments.len() == values.len()
                        && target
                            .arguments
                            .iter()
                            .zip(values)
                            .all(|(actual, expected)| {
                                command_expression_fingerprint(&command.expressions, actual.id)
                                    == command_expression_fingerprint(
                                        &command.expressions,
                                        expected.id,
                                    )
                            })
                })
            });
            if !qualifying_target {
                diagnostics.push(
                    CompilerDiagnostic::new(
                        CompilerDiagnosticCode::MissingRelationshipRead,
                        relationship.name_span,
                    )
                    .with_related_span(source_binding.span),
                );
            }
        }
    }
}

fn validate_binding_ownership(command: &HirCommand, diagnostics: &mut Vec<CompilerDiagnostic>) {
    let has_mutable_binding = command.bindings.iter().any(|binding| {
        matches!(
            binding.mode,
            BindingMode::Mutate | BindingMode::Create | BindingMode::Delete
        )
    });
    if !command.bindings.is_empty() && (command.effects.is_empty() || has_mutable_binding) {
        return;
    }

    let span = command
        .effects
        .first()
        .map_or(command.span, |effect| match effect {
            HirEffect::Set { target_span, .. } => *target_span,
            HirEffect::Emit { event_span, .. } => *event_span,
            HirEffect::WorkflowTransition {
                transition_span, ..
            } => *transition_span,
            HirEffect::WorkflowLease { lease_span, .. } => *lease_span,
        });
    diagnostics.push(CompilerDiagnostic::new(
        CompilerDiagnosticCode::InvalidBinding,
        span,
    ));
}

fn validate_idempotency(
    command: &HirCommand,
    diagnostics: &mut Vec<CompilerDiagnostic>,
) -> Option<FieldId> {
    let mutating = command.bindings.iter().any(|binding| {
        matches!(
            binding.mode,
            BindingMode::Mutate | BindingMode::Create | BindingMode::Delete
        )
    }) || !command.effects.is_empty();
    if command.invocation_class == riffdb_contract_ir::CommandInvocationClass::Reimport {
        if command.idempotency.is_some() {
            diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::InvalidIdempotency,
                command.span,
            ));
        }
        return None;
    }
    let Some(idempotency) = &command.idempotency else {
        if mutating {
            diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::MissingIdempotency,
                command.span,
            ));
        }
        return None;
    };
    let Some(node) = idempotency.expressions.node(idempotency.root.id) else {
        diagnostics.push(CompilerDiagnostic::new(
            CompilerDiagnosticCode::InvalidIdempotency,
            idempotency.root.span,
        ));
        return None;
    };
    let ExpressionKind::InputField(field_id) = node.kind else {
        diagnostics.push(CompilerDiagnostic::new(
            CompilerDiagnosticCode::InvalidIdempotency,
            idempotency.root.span,
        ));
        return None;
    };
    let Some(input) = command
        .inputs
        .iter()
        .find(|input| input.field.id == field_id)
    else {
        diagnostics.push(CompilerDiagnostic::new(
            CompilerDiagnosticCode::InvalidIdempotency,
            idempotency.root.span,
        ));
        return None;
    };
    let valid = match input.field.value_type.tag() {
        ValueTypeTag::Uuid => true,
        ValueTypeTag::String => input
            .field
            .value_type
            .byte_bound()
            .is_some_and(|maximum| (1..=128).contains(&maximum)),
        _ => false,
    };
    if valid {
        Some(field_id)
    } else {
        diagnostics.push(CompilerDiagnostic::new(
            CompilerDiagnosticCode::InvalidIdempotency,
            idempotency.root.span,
        ));
        None
    }
}

fn validate_create_object_reads(
    command: &HirCommand,
    states: &BTreeMap<BindingId, BindingState>,
    fields: &[HirObjectField],
    diagnostics: &mut Vec<CompilerDiagnostic>,
) {
    for field in fields {
        validate_create_reads(command, states, &field.value, diagnostics);
    }
}

fn validate_create_reads(
    command: &HirCommand,
    states: &BTreeMap<BindingId, BindingState>,
    root: &HirExpressionRoot,
    diagnostics: &mut Vec<CompilerDiagnostic>,
) {
    let Ok(dependencies) = command.expressions.dependencies(root.id, root.span) else {
        diagnostics.push(CompilerDiagnostic::new(
            CompilerDiagnosticCode::InvalidIr,
            root.span,
        ));
        return;
    };
    for (binding, field) in dependencies.bound_fields() {
        if states.get(binding).is_some_and(|state| {
            state.mode == BindingMode::Create && !state.initialized_fields.contains(field)
        }) {
            diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::InvalidCreation,
                root.span,
            ));
        }
    }
    for binding in dependencies.complete_bindings() {
        if states.get(binding).is_some_and(|state| {
            state.mode == BindingMode::Create
                && !state
                    .required_create_fields
                    .is_subset(&state.initialized_fields)
        }) {
            diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::InvalidCreation,
                root.span,
            ));
        }
    }
}

fn validate_outcome_shapes(outcomes: &[&HirOutcome], diagnostics: &mut Vec<CompilerDiagnostic>) {
    let mut by_id = BTreeMap::<_, Vec<&HirOutcome>>::new();
    for outcome in outcomes {
        by_id.entry(outcome.id).or_default().push(outcome);
    }
    for occurrences in by_id.values() {
        let field_names = occurrences
            .iter()
            .flat_map(|outcome| outcome.fields.iter().map(|field| field.name.clone()))
            .collect::<BTreeSet<_>>();
        for name in field_names {
            let present = occurrences
                .iter()
                .filter_map(|outcome| outcome.fields.iter().find(|field| field.name == name))
                .collect::<Vec<_>>();
            let inconsistent_type = present
                .windows(2)
                .any(|pair| pair[0].value.value_type != pair[1].value.value_type);
            let missing_required = present.len() != occurrences.len()
                && present
                    .iter()
                    .any(|field| !field.value.value_type.is_optional());
            if inconsistent_type || missing_required {
                let span = present
                    .get(1)
                    .map_or(occurrences[0].span, |field| field.name_span);
                diagnostics.push(CompilerDiagnostic::new(
                    CompilerDiagnosticCode::InvalidOutcome,
                    span,
                ));
            }
        }
    }
}

fn validate_secret_taint(
    secret_input: Option<FieldId>,
    command: &HirCommand,
    roots: &[&HirExpressionRoot],
    diagnostics: &mut Vec<CompilerDiagnostic>,
) {
    let Some(secret_input) = secret_input else {
        return;
    };
    for root in roots {
        if command
            .expressions
            .dependencies(root.id, root.span)
            .is_ok_and(|dependencies| dependencies.input_fields().contains(&secret_input))
        {
            diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::InvalidIdempotency,
                root.span,
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use riffdb_contract_syntax::parse_contract;

    use super::*;
    use crate::hir::lower_contract_hir;
    use crate::symbols::allocate_genesis_symbols;
    use crate::typecheck::resolve_declared_types;

    fn validate(source: &str) -> Result<(), CompilerDiagnostics> {
        let document = parse_contract(source).expect("valid syntax");
        let symbols = allocate_genesis_symbols(&document)?;
        let types = resolve_declared_types(&document, &symbols)?;
        let hir = lower_contract_hir(&document, &symbols, &types)?;
        validate_commands(&hir)
    }

    #[test]
    fn canonical_budget_commands_validate() {
        validate(include_str!("../../../contracts/examples/budget.riff")).expect("budget commands");
    }

    #[test]
    fn direct_uuid_idempotency_input_is_valid() {
        validate(
            r#"
contract UuidIdempotency version 1 {
  entity Row { key (id: uuid) field value: i64 }
  aggregate Rows { root Row partition_by id conflict_key (id) }
  command Change {
    input request_key: uuid
    input id: uuid
    idempotency_key request_key
    mutate Row(id) as row else Missing { id: id }
    set row.value = 1
    return Changed { row: row }
  }
}
"#,
        )
        .expect("UUID idempotency command");
    }

    #[test]
    fn idempotency_secret_cannot_influence_event_or_outcome() {
        let source = r#"
contract Invalid version 1 {
  entity Row { key (id: uuid) field value: i64 }
  event Changed { leaked: string<128> }
  aggregate Rows { root Row partition_by id conflict_key (id) }
  command Change {
    input idempotency_key: string<128>
    input id: uuid
    idempotency_key idempotency_key
    mutate Row(id) as row else Missing { id: id }
    set row.value = 1
    emit Changed { leaked: idempotency_key }
    return ChangedOutcome { row: row }
  }
}
"#;
        let diagnostics = validate(source).expect_err("secret leak rejects");
        assert!(
            diagnostics.as_slice().iter().any(|diagnostic| {
                diagnostic.code() == CompilerDiagnosticCode::InvalidIdempotency
            })
        );
    }

    #[test]
    fn create_requires_every_nonoptional_nonkey_field_once() {
        let source = r#"
contract Invalid version 1 {
  entity Row { key (id: uuid) field first: i64 field second: i64 }
  aggregate Rows { root Row partition_by id conflict_key (id) }
  command Create {
    input idempotency_key: string<128>
    input id: uuid
    idempotency_key idempotency_key
    create Row(id) as row else Exists { id: id }
    set row.first = 1
    return Created { row: row }
  }
}
"#;
        let diagnostics = validate(source).expect_err("incomplete create rejects");
        assert!(
            diagnostics
                .as_slice()
                .iter()
                .any(|diagnostic| { diagnostic.code() == CompilerDiagnosticCode::InvalidCreation })
        );
    }

    #[test]
    fn binding_failure_payloads_reject_binding_and_transaction_dependencies() {
        for forbidden in ["first_row", "second_row", "tx.time"] {
            let source = format!(
                r#"
contract Invalid version 1 {{
  entity Row {{ key (id: uuid) field value: i64 }}
  aggregate Rows {{ root Row partition_by id conflict_key (id) }}
  command ReadTwo {{
    input first_id: uuid
    input second_id: uuid
    read Row(first_id) as first_row else MissingFirst {{ leaked: {forbidden} }}
    read Row(second_id) as second_row else MissingSecond {{ id: second_id }}
    return Found {{}}
  }}
}}
"#
            );
            let diagnostics = validate(&source).expect_err("failure dependency rejects");
            let forbidden_start = source
                .find(&format!("leaked: {forbidden}"))
                .expect("payload marker")
                + "leaked: ".len();
            assert!(diagnostics.as_slice().iter().any(|diagnostic| {
                diagnostic.code() == CompilerDiagnosticCode::UnknownName
                    && diagnostic.primary_span().start() as usize == forbidden_start
            }));
        }
    }
}
