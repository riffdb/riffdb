//! Role and multi-step durable saga lowering and static verification (`ADR-0334`).

use std::collections::{BTreeMap, BTreeSet};

use riffdb_contract_ir::{
    BindingId, CommandInvocationClass, CommandPlan, FieldExpression, FieldSchema,
    MAX_SAGA_ACCUMULATED_STATE_BYTES, MAX_SAGA_INPUT_FIELDS, MAX_SAGA_OUTCOME_FIELDS,
    MAX_SAGA_STEPS, OutcomeSchema, RecordTypeRef, RoleSchemaV1, SagaBranchTransitionV1,
    SagaCommandCallPlanV1, SagaOutcomeBranchPlanV1, SagaOutcomeEffectClassV1,
    SagaOutcomeFieldPlanV1, SagaPlanV1, SagaStepPlanV1, SagaTerminalOutcomePlanV1, SchemaIr,
    ValueType, ValueTypeTag, compute_saga_plan_hash, saga_step_outcome_field_id,
};
use riffdb_contract_syntax::ast::{
    Declaration, Expression, OutcomeExpression, RoleDeclaration, SagaCommandCall, SagaDeclaration,
    SagaOutcomeEffectClass, SagaStepAction,
};
use riffdb_contract_syntax::{ContractDocument, Span, Spanned};
use riffdb_types::{CanonicalValue, CommandId, EntityTypeId, FieldId, OutcomeId};

use crate::diagnostic::{
    CompilerBoundResource, CompilerDiagnostic, CompilerDiagnosticCause, CompilerDiagnosticCode,
    CompilerDiagnostics,
};
use crate::expression_lowering::{BindingExpressionScope, ExpressionLowerer, ExpressionScope};
use crate::hir::{HirBinding, HirCommand, HirDecisionAction, TypedContractHir};
use crate::symbols::GenesisSymbols;
use crate::typecheck::resolve_type;

/// Lowers and validates all `role` and `saga` declarations in a contract.
pub(crate) fn lower_roles_and_sagas(
    document: &ContractDocument,
    symbols: &GenesisSymbols,
    hir: &TypedContractHir,
    schema: &SchemaIr,
    commands: &[CommandPlan],
) -> Result<(Vec<RoleSchemaV1>, Vec<SagaPlanV1>), CompilerDiagnostics> {
    let mut diagnostics = Vec::new();

    let mut commands_by_name = BTreeMap::<&str, (&HirCommand, &CommandPlan)>::new();
    for hir_cmd in &hir.commands {
        if let Some(plan) = commands.iter().find(|plan| plan.command_id() == hir_cmd.id) {
            commands_by_name.insert(hir_cmd.name.as_str(), (hir_cmd, plan));
        }
    }

    let mut role_schemas = Vec::new();
    let mut roles_by_name = BTreeMap::<String, BTreeSet<CommandId>>::new();

    for declaration in &document.contract.value.declarations {
        if let Declaration::Role(role) = &declaration.value
            && let Some(role_schema) = lower_one_role(
                role,
                &commands_by_name,
                &mut roles_by_name,
                &mut diagnostics,
            )
        {
            role_schemas.push(role_schema);
        }
    }

    let mut saga_plans = Vec::new();
    for declaration in &document.contract.value.declarations {
        if let Declaration::Saga(saga) = &declaration.value
            && let Some(saga_plan) = lower_one_saga(
                saga,
                symbols,
                schema,
                &commands_by_name,
                &roles_by_name,
                &mut diagnostics,
            )
        {
            saga_plans.push(saga_plan);
        }
    }

    if !diagnostics.is_empty() {
        return Err(CompilerDiagnostics::new(diagnostics).expect("nonempty diagnostics"));
    }

    role_schemas.sort_unstable_by(|a, b| a.name().cmp(b.name()));
    saga_plans.sort_unstable_by(|a, b| a.name().cmp(b.name()));
    Ok((role_schemas, saga_plans))
}

fn lower_one_role(
    role: &RoleDeclaration,
    commands_by_name: &BTreeMap<&str, (&HirCommand, &CommandPlan)>,
    roles_by_name: &mut BTreeMap<String, BTreeSet<CommandId>>,
    diagnostics: &mut Vec<CompilerDiagnostic>,
) -> Option<RoleSchemaV1> {
    if role.commands.is_empty() {
        diagnostics.push(CompilerDiagnostic::new(
            CompilerDiagnosticCode::MissingDeclaration,
            role.name.span,
        ));
        return None;
    }
    let mut command_ids = Vec::with_capacity(role.commands.len());
    let mut command_set = BTreeSet::new();
    let mut had_error = false;
    for cmd_name in &role.commands {
        match commands_by_name.get(cmd_name.value.as_str()) {
            Some((hir_cmd, plan)) => {
                if hir_cmd.invocation_class != CommandInvocationClass::Application {
                    diagnostics.push(CompilerDiagnostic::new(
                        CompilerDiagnosticCode::InvalidWorkflow,
                        cmd_name.span,
                    ));
                    had_error = true;
                } else {
                    command_ids.push(plan.command_id());
                    command_set.insert(plan.command_id());
                }
            }
            None => {
                diagnostics.push(CompilerDiagnostic::new(
                    CompilerDiagnosticCode::UnknownName,
                    cmd_name.span,
                ));
                had_error = true;
            }
        }
    }
    roles_by_name.insert(role.name.value.clone(), command_set);
    if had_error {
        return None;
    }
    match RoleSchemaV1::new(role.name.value.clone(), command_ids) {
        Ok(schema) => Some(schema),
        Err(error) => {
            diagnostics.push(CompilerDiagnostic::from_ir_error(error, role.name.span));
            None
        }
    }
}

fn lower_one_saga(
    saga: &SagaDeclaration,
    symbols: &GenesisSymbols,
    schema: &SchemaIr,
    commands_by_name: &BTreeMap<&str, (&HirCommand, &CommandPlan)>,
    roles_by_name: &BTreeMap<String, BTreeSet<CommandId>>,
    diagnostics: &mut Vec<CompilerDiagnostic>,
) -> Option<SagaPlanV1> {
    let start_diag_count = diagnostics.len();

    if saga.inputs.is_empty() {
        diagnostics.push(CompilerDiagnostic::new(
            CompilerDiagnosticCode::MissingDeclaration,
            saga.name.span,
        ));
    } else if saga.inputs.len() > MAX_SAGA_INPUT_FIELDS {
        diagnostics.push(CompilerDiagnostic::bound_exceeded(
            CompilerBoundResource::DeclarationCount,
            saga.inputs.len(),
            MAX_SAGA_INPUT_FIELDS,
            saga.name.span,
        ));
    }

    if saga.steps.is_empty() {
        diagnostics.push(CompilerDiagnostic::new(
            CompilerDiagnosticCode::MissingDeclaration,
            saga.name.span,
        ));
        return None;
    }
    if saga.steps.len() > MAX_SAGA_STEPS {
        diagnostics.push(CompilerDiagnostic::bound_exceeded(
            CompilerBoundResource::DeclarationCount,
            saga.steps.len(),
            MAX_SAGA_STEPS,
            saga.name.span,
        ));
    }

    // Resolve input types and allocate deterministic per-saga FieldIds in canonical name order.
    let mut resolved_inputs_by_name = BTreeMap::<String, (Span, ValueType)>::new();
    for input in &saga.inputs {
        if let Some(ty) = resolve_type(&input.value.ty, symbols, diagnostics) {
            resolved_inputs_by_name
                .entry(input.value.name.value.clone())
                .or_insert((input.value.name.span, ty));
        }
    }

    let mut sorted_input_names = resolved_inputs_by_name.keys().cloned().collect::<Vec<_>>();
    sorted_input_names.sort_by(|left, right| canonical_name_cmp(left, right));

    let mut inputs_scope = BTreeMap::<String, (FieldId, ValueType)>::new();
    let mut input_schemas = Vec::with_capacity(sorted_input_names.len());
    for (idx, name) in sorted_input_names.into_iter().enumerate() {
        let Some(field_id) = u32::try_from(idx + 1).ok().and_then(FieldId::new) else {
            continue;
        };
        let (span, ty) = resolved_inputs_by_name[&name].clone();
        inputs_scope.insert(name.clone(), (field_id, ty.clone()));
        match FieldSchema::new(field_id, name, ty) {
            Ok(field_schema) => input_schemas.push(field_schema),
            Err(error) => diagnostics.push(CompilerDiagnostic::from_ir_error(error, span)),
        }
    }

    // Check `saga_id: uuid` input.
    let saga_id_field = match inputs_scope.get("saga_id") {
        Some((field_id, ty)) if *ty == ValueType::uuid() => Some(*field_id),
        _ => {
            diagnostics.push(
                CompilerDiagnostic::new(CompilerDiagnosticCode::InvalidWorkflow, saga.name.span)
                    .with_cause(CompilerDiagnosticCause::SagaMissingSagaIdInput),
            );
            None
        }
    };

    // Check `partition_by` input.
    let partition_field = match inputs_scope.get(&saga.partition_by.value) {
        Some((field_id, ty)) => {
            if ty.is_authoritative_key_scalar() {
                Some(*field_id)
            } else {
                diagnostics.push(
                    CompilerDiagnostic::new(
                        CompilerDiagnosticCode::InvalidWorkflow,
                        saga.partition_by.span,
                    )
                    .with_cause(CompilerDiagnosticCause::SagaPartitionFieldInvalid),
                );
                None
            }
        }
        None => {
            diagnostics.push(
                CompilerDiagnostic::new(
                    CompilerDiagnosticCode::UnknownName,
                    saga.partition_by.span,
                )
                .with_cause(CompilerDiagnosticCause::SagaPartitionFieldInvalid),
            );
            None
        }
    };

    // Check `execution_role`.
    let role_commands = match roles_by_name.get(&saga.execution_role.value) {
        Some(cmds) => Some(cmds),
        None => {
            diagnostics.push(
                CompilerDiagnostic::new(
                    CompilerDiagnosticCode::UnknownName,
                    saga.execution_role.span,
                )
                .with_cause(CompilerDiagnosticCause::SagaRoleMissingCommandCapability),
            );
            None
        }
    };

    let dummy_command_id = CommandId::first();
    let mut lowerer = ExpressionLowerer::new(
        symbols,
        ExpressionScope::Command {
            command_id: dummy_command_id,
            inputs: inputs_scope.clone(),
            service_values: BTreeMap::new(),
            bindings: BTreeMap::new(),
            collection_element: None,
        },
    );

    // Validate and lower `idempotency_key`.
    let idempotency_expression = if let Some(idempotency) = &saga.idempotency {
        let expr = &idempotency.value.expression;
        let valid_direct_input = match &expr.value {
            Expression::Path(path) if path.value.segments.len() == 1 => inputs_scope
                .get(&path.value.segments[0].value)
                .is_some_and(|(_, ty)| is_valid_idempotency_type(ty)),
            _ => false,
        };
        if !valid_direct_input {
            diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::InvalidIdempotency,
                idempotency.span,
            ));
            None
        } else {
            match lowerer.lower_input_only(expr, None) {
                Ok((expr_id, _)) => Some(expr_id),
                Err(diag) => {
                    diagnostics.push(diag);
                    None
                }
            }
        }
    } else {
        None
    };

    // Build step declaration index table and check step transition targets, reachability, and cycles.
    let step_count = saga.steps.len();
    let mut step_name_to_decl = BTreeMap::<String, usize>::new();
    for (idx, step) in saga.steps.iter().enumerate() {
        step_name_to_decl
            .entry(step.value.name.value.clone())
            .or_insert(idx);
    }

    let mut adj = vec![Vec::<(usize, Span)>::new(); step_count];
    for (idx, step) in saga.steps.iter().enumerate() {
        for branch in &step.value.branches {
            if let SagaStepAction::Next(target) = &branch.value.action.value {
                if let Some(&target_idx) = step_name_to_decl.get(&target.value) {
                    adj[idx].push((target_idx, target.span));
                } else {
                    diagnostics.push(CompilerDiagnostic::new(
                        CompilerDiagnosticCode::UnknownName,
                        target.span,
                    ));
                }
            }
        }
    }

    // Reachability from entry step 0.
    let mut reachable = vec![false; step_count];
    let mut queue = std::collections::VecDeque::new();
    reachable[0] = true;
    queue.push_back(0);
    while let Some(u) = queue.pop_front() {
        for &(v, _) in &adj[u] {
            if !reachable[v] {
                reachable[v] = true;
                queue.push_back(v);
            }
        }
    }
    let mut has_unreachable = false;
    for (idx, is_reach) in reachable.iter().copied().enumerate() {
        if !is_reach {
            has_unreachable = true;
            diagnostics.push(
                CompilerDiagnostic::new(
                    CompilerDiagnosticCode::InvalidWorkflowTransition,
                    saga.steps[idx].value.name.span,
                )
                .with_cause(CompilerDiagnosticCause::SagaStepUnreachable),
            );
        }
    }

    // Cycle detection via 3-color DFS (`0 = unvisited`, `1 = visiting`, `2 = visited`).
    let mut color = vec![0_u8; step_count];
    let mut has_cycle = false;
    for start in 0..step_count {
        if color[start] == 0 {
            detect_saga_cycles(start, &adj, &mut color, &mut has_cycle, diagnostics);
        }
    }

    // Topological ordering of steps (falls back to `0..step_count` if graph had cycles/unreachable so we can still check commands).
    let topo_order = if !has_cycle && !has_unreachable && step_count <= MAX_SAGA_STEPS {
        compute_topological_order(step_count, &adj).unwrap_or_else(|| (0..step_count).collect())
    } else {
        (0..step_count).collect()
    };

    let mut decl_to_topo = vec![0_usize; step_count];
    for (topo_idx, &decl_idx) in topo_order.iter().enumerate() {
        decl_to_topo[decl_idx] = topo_idx;
    }
    let mut step_name_to_topo = BTreeMap::<String, usize>::new();
    for (name, &decl_idx) in &step_name_to_decl {
        step_name_to_topo.insert(name.clone(), decl_to_topo[decl_idx]);
    }

    // Resolve each step's forward command and validate branch coverage and effect classifications.
    let mut resolved_step_commands = vec![None::<(&HirCommand, &CommandPlan)>; step_count];
    let mut step_branch_outcomes =
        vec![Vec::<(usize, OutcomeId, SagaOutcomeEffectClassV1)>::new(); step_count];

    for topo_idx in 0..step_count {
        let decl_idx = topo_order[topo_idx];
        let step = &saga.steps[decl_idx].value;
        let cmd_name = &step.command.value.command;
        let Some(&(hir_cmd, cmd_plan)) = commands_by_name.get(cmd_name.value.as_str()) else {
            diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::UnknownName,
                cmd_name.span,
            ));
            continue;
        };
        if hir_cmd.invocation_class != CommandInvocationClass::Application {
            diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::InvalidWorkflow,
                cmd_name.span,
            ));
        }
        if let Some(allowed) = role_commands
            && !allowed.contains(&cmd_plan.command_id())
        {
            diagnostics.push(
                CompilerDiagnostic::new(CompilerDiagnosticCode::InvalidWorkflow, cmd_name.span)
                    .with_cause(CompilerDiagnosticCause::SagaRoleMissingCommandCapability),
            );
        }
        resolved_step_commands[topo_idx] = Some((hir_cmd, cmd_plan));

        let mut seen_outcomes = BTreeMap::<String, Span>::new();
        for (branch_idx, branch) in step.branches.iter().enumerate() {
            let outcome_name = &branch.value.outcome;
            if let Some(&first_span) = seen_outcomes.get(&outcome_name.value) {
                diagnostics.push(
                    CompilerDiagnostic::new(
                        CompilerDiagnosticCode::DuplicateName,
                        outcome_name.span,
                    )
                    .with_related_span(first_span),
                );
                continue;
            }
            seen_outcomes.insert(outcome_name.value.clone(), outcome_name.span);

            let Some(outcome_schema) = cmd_plan
                .outcomes()
                .iter()
                .find(|o| o.name() == outcome_name.value)
            else {
                diagnostics.push(CompilerDiagnostic::new(
                    CompilerDiagnosticCode::UnknownName,
                    outcome_name.span,
                ));
                continue;
            };

            let expected_effect =
                command_outcome_effect_class(hir_cmd, cmd_plan, outcome_schema.id());
            let declared_effect = match branch.value.effect_class.value {
                SagaOutcomeEffectClass::Effectful => SagaOutcomeEffectClassV1::Effectful,
                SagaOutcomeEffectClass::NoEffect => SagaOutcomeEffectClassV1::NoEffect,
            };
            if declared_effect != expected_effect {
                diagnostics.push(
                    CompilerDiagnostic::new(
                        CompilerDiagnosticCode::InvalidWorkflow,
                        branch.value.effect_class.span,
                    )
                    .with_cause(CompilerDiagnosticCause::SagaOutcomeEffectMismatch),
                );
            }
            step_branch_outcomes[topo_idx].push((branch_idx, outcome_schema.id(), declared_effect));
        }

        let covered_all = cmd_plan
            .outcomes()
            .iter()
            .all(|o| seen_outcomes.contains_key(o.name()));
        if !covered_all {
            diagnostics.push(
                CompilerDiagnostic::new(CompilerDiagnosticCode::InvalidWorkflow, step.name.span)
                    .with_cause(CompilerDiagnosticCause::SagaOutcomeBranchMissing),
            );
        }
    }

    // Build topological DAG edges, reachability, and strict dominators when acyclic.
    let mut topo_edges =
        vec![Vec::<(usize, OutcomeId, SagaOutcomeEffectClassV1, Span)>::new(); step_count];
    for topo_u in 0..step_count {
        let decl_u = topo_order[topo_u];
        let step = &saga.steps[decl_u].value;
        for &(branch_idx, outcome_id, effect_class) in &step_branch_outcomes[topo_u] {
            let branch = &step.branches[branch_idx];
            if let SagaStepAction::Next(target) = &branch.value.action.value
                && let Some(&topo_v) = step_name_to_topo.get(&target.value)
            {
                topo_edges[topo_u].push((topo_v, outcome_id, effect_class, branch.span));
            }
        }
    }

    let mut can_reach = vec![vec![false; step_count]; step_count];
    for u in (0..step_count).rev() {
        can_reach[u][u] = true;
        for &(v, _, _, _) in &topo_edges[u] {
            if v > u {
                let reach_from_v = can_reach[v].clone();
                for (u_reach_w, v_reach_w) in can_reach[u].iter_mut().zip(reach_from_v) {
                    if v_reach_w {
                        *u_reach_w = true;
                    }
                }
            }
        }
    }

    // Compute strict dominators on the topological DAG.
    let mut dom_or_self = vec![BTreeSet::<usize>::new(); step_count];
    dom_or_self[0].insert(0);
    for u in 1..step_count {
        let preds = (0..u)
            .filter(|&p| topo_edges[p].iter().any(|&(v, _, _, _)| v == u))
            .collect::<Vec<_>>();
        if let Some(&first_pred) = preds.first() {
            let mut intersection = dom_or_self[first_pred].clone();
            for &pred in &preds[1..] {
                intersection.retain(|node| dom_or_self[pred].contains(node));
            }
            dom_or_self[u] = intersection;
        }
        dom_or_self[u].insert(u);
    }

    // Compensation and uncompensated-abort static verification.
    let mut can_reach_with_prior_effect = vec![false; step_count];
    let mut can_reach_compensate = vec![false; step_count];
    for u in (0..step_count).rev() {
        let decl_u = topo_order[u];
        let step = &saga.steps[decl_u].value;
        for &(branch_idx, _, _) in &step_branch_outcomes[u] {
            match &step.branches[branch_idx].value.action.value {
                SagaStepAction::Compensate(_) => {
                    can_reach_compensate[u] = true;
                }
                SagaStepAction::Next(target) => {
                    if let Some(&v) = step_name_to_topo.get(&target.value)
                        && v > u
                        && can_reach_compensate[v]
                    {
                        can_reach_compensate[u] = true;
                    }
                }
                SagaStepAction::Complete(_)
                | SagaStepAction::Abort(_)
                | SagaStepAction::AbortUncompensated(_) => {}
            }
        }
    }

    for u in 0..step_count {
        let decl_u = topo_order[u];
        let step = &saga.steps[decl_u].value;
        let mut step_needs_compensation = false;
        for &(branch_idx, _, effect_class) in &step_branch_outcomes[u] {
            let branch = &step.branches[branch_idx];
            let branch_has_effect = can_reach_with_prior_effect[u]
                || effect_class == SagaOutcomeEffectClassV1::Effectful;
            match &branch.value.action.value {
                SagaStepAction::Next(target) => {
                    if let Some(&v) = step_name_to_topo.get(&target.value)
                        && v > u
                    {
                        if branch_has_effect {
                            can_reach_with_prior_effect[v] = true;
                        }
                        if effect_class == SagaOutcomeEffectClassV1::Effectful
                            && can_reach_compensate[v]
                        {
                            step_needs_compensation = true;
                        }
                    }
                }
                SagaStepAction::Compensate(_) => {
                    if effect_class == SagaOutcomeEffectClassV1::Effectful {
                        step_needs_compensation = true;
                    }
                }
                SagaStepAction::Abort(_) => {
                    if branch_has_effect {
                        diagnostics.push(
                            CompilerDiagnostic::new(
                                CompilerDiagnosticCode::InvalidWorkflowTransition,
                                branch.span,
                            )
                            .with_cause(CompilerDiagnosticCause::SagaUncompensatedAbortAfterEffect),
                        );
                    }
                }
                SagaStepAction::Complete(_) | SagaStepAction::AbortUncompensated(_) => {}
            }
        }
        if step_needs_compensation && step.compensation.is_none() {
            diagnostics.push(
                CompilerDiagnostic::new(CompilerDiagnosticCode::InvalidWorkflow, step.name.span)
                    .with_cause(CompilerDiagnosticCause::SagaMissingCompensation),
            );
        }
    }

    // Check accumulated step-outcome + input state byte ceiling (`<= 64 KiB`).
    let mut accumulated_state_bytes = 0_usize;
    let mut state_overflow = false;
    for field in &input_schemas {
        match max_value_type_bytes(field.value_type(), schema) {
            Some(bytes) => {
                if let Some(next) = accumulated_state_bytes.checked_add(bytes) {
                    accumulated_state_bytes = next;
                } else {
                    state_overflow = true;
                }
            }
            None => state_overflow = true,
        }
    }
    for resolved_cmd in &resolved_step_commands {
        if let Some((_, cmd_plan)) = *resolved_cmd {
            let mut max_step_outcome_bytes = 0_usize;
            for outcome in cmd_plan.outcomes() {
                let mut outcome_bytes = 0_usize;
                for field in outcome.payload().fields() {
                    match max_value_type_bytes(field.value_type(), schema) {
                        Some(bytes) => {
                            if let Some(next) = outcome_bytes.checked_add(bytes) {
                                outcome_bytes = next;
                            } else {
                                state_overflow = true;
                            }
                        }
                        None => state_overflow = true,
                    }
                }
                max_step_outcome_bytes = max_step_outcome_bytes.max(outcome_bytes);
            }
            if let Some(next) = accumulated_state_bytes.checked_add(max_step_outcome_bytes) {
                accumulated_state_bytes = next;
            } else {
                state_overflow = true;
            }
        }
    }
    if state_overflow {
        diagnostics.push(CompilerDiagnostic::arithmetic_overflow(
            CompilerBoundResource::ValueBytes,
            saga.name.span,
        ));
    } else if accumulated_state_bytes > MAX_SAGA_ACCUMULATED_STATE_BYTES {
        diagnostics.push(CompilerDiagnostic::bound_exceeded(
            CompilerBoundResource::ValueBytes,
            accumulated_state_bytes,
            MAX_SAGA_ACCUMULATED_STATE_BYTES,
            saga.name.span,
        ));
    }

    // Lower each step's forward command arguments, compensation command arguments, and terminal outcomes.
    let mut terminal_outcome_shapes =
        BTreeMap::<String, (Span, BTreeMap<String, ValueType>)>::new();
    let mut lowered_steps = Vec::with_capacity(step_count);

    for topo_u in 0..step_count {
        let decl_u = topo_order[topo_u];
        let step = &saga.steps[decl_u].value;

        // Build dominating step scope for `topo_u`.
        let mut dom_bindings = BTreeMap::<String, BindingExpressionScope>::new();
        for &dom_step in &dom_or_self[topo_u] {
            if dom_step == topo_u {
                continue;
            }
            let dom_decl = topo_order[dom_step];
            let dom_name = saga.steps[dom_decl].value.name.value.clone();
            let Some((_, dom_cmd_plan)) = resolved_step_commands[dom_step] else {
                continue;
            };
            let reaching_outcomes = topo_edges[dom_step]
                .iter()
                .filter(|&&(next_v, _, _, _)| can_reach[next_v][topo_u])
                .filter_map(|&(_, outcome_id, _, _)| {
                    dom_cmd_plan
                        .outcomes()
                        .iter()
                        .find(|o| o.id() == outcome_id)
                })
                .collect::<Vec<_>>();
            let fields = intersect_outcome_fields(dom_cmd_plan, &reaching_outcomes);
            if let Some(binding_id) = u32::try_from(dom_step + 1).ok().map(BindingId::new) {
                dom_bindings.insert(
                    dom_name,
                    BindingExpressionScope {
                        id: binding_id,
                        entity_id: EntityTypeId::first(),
                        fields,
                        collection_local: false,
                    },
                );
            }
        }

        // Lower forward command call.
        let forward_call_plan = lower_saga_command_call(
            &step.command,
            commands_by_name,
            None,
            &inputs_scope,
            &dom_bindings,
            &step_name_to_topo,
            &mut lowerer,
            diagnostics,
        );

        // Lower optional compensating command call (in scope: strict dominators + `topo_u`'s effectful outcomes).
        let compensation_call_plan = if let Some(comp_call) = &step.compensation {
            let mut comp_bindings = dom_bindings.clone();
            if let Some((_, step_cmd_plan)) = resolved_step_commands[topo_u]
                && let Some(binding_id) = u32::try_from(topo_u + 1).ok().map(BindingId::new)
            {
                let effectful_outcomes = step_branch_outcomes[topo_u]
                    .iter()
                    .filter(|&&(_, _, eff)| eff == SagaOutcomeEffectClassV1::Effectful)
                    .filter_map(|&(_, outcome_id, _)| {
                        step_cmd_plan
                            .outcomes()
                            .iter()
                            .find(|o| o.id() == outcome_id)
                    })
                    .collect::<Vec<_>>();
                let fields = intersect_outcome_fields(step_cmd_plan, &effectful_outcomes);
                comp_bindings.insert(
                    step.name.value.clone(),
                    BindingExpressionScope {
                        id: binding_id,
                        entity_id: EntityTypeId::first(),
                        fields,
                        collection_local: false,
                    },
                );
            }
            lower_saga_command_call(
                comp_call,
                commands_by_name,
                role_commands,
                &inputs_scope,
                &comp_bindings,
                &step_name_to_topo,
                &mut lowerer,
                diagnostics,
            )
        } else {
            None
        };

        // Lower branches.
        let mut branch_plans = Vec::with_capacity(step_branch_outcomes[topo_u].len());
        for &(branch_idx, outcome_id, effect_class) in &step_branch_outcomes[topo_u] {
            let branch = &step.branches[branch_idx];
            let mut branch_bindings = dom_bindings.clone();
            if let Some((_, step_cmd_plan)) = resolved_step_commands[topo_u]
                && let Some(outcome_schema) = step_cmd_plan
                    .outcomes()
                    .iter()
                    .find(|o| o.id() == outcome_id)
                && let Some(binding_id) = u32::try_from(topo_u + 1).ok().map(BindingId::new)
            {
                let fields = intersect_outcome_fields(step_cmd_plan, &[outcome_schema]);
                branch_bindings.insert(
                    step.name.value.clone(),
                    BindingExpressionScope {
                        id: binding_id,
                        entity_id: EntityTypeId::first(),
                        fields,
                        collection_local: false,
                    },
                );
            }

            let transition = match &branch.value.action.value {
                SagaStepAction::Next(target) => step_name_to_topo
                    .get(&target.value)
                    .and_then(|&v| u16::try_from(v).ok())
                    .map(SagaBranchTransitionV1::Next),
                SagaStepAction::Complete(outcome) => lower_saga_terminal_outcome(
                    outcome,
                    &inputs_scope,
                    &branch_bindings,
                    &step_name_to_topo,
                    &mut terminal_outcome_shapes,
                    &mut lowerer,
                    diagnostics,
                )
                .map(SagaBranchTransitionV1::Complete),
                SagaStepAction::Compensate(outcome) => lower_saga_terminal_outcome(
                    outcome,
                    &inputs_scope,
                    &branch_bindings,
                    &step_name_to_topo,
                    &mut terminal_outcome_shapes,
                    &mut lowerer,
                    diagnostics,
                )
                .map(SagaBranchTransitionV1::Compensate),
                SagaStepAction::Abort(outcome) => lower_saga_terminal_outcome(
                    outcome,
                    &inputs_scope,
                    &branch_bindings,
                    &step_name_to_topo,
                    &mut terminal_outcome_shapes,
                    &mut lowerer,
                    diagnostics,
                )
                .map(SagaBranchTransitionV1::AbortClean),
                SagaStepAction::AbortUncompensated(outcome) => lower_saga_terminal_outcome(
                    outcome,
                    &inputs_scope,
                    &branch_bindings,
                    &step_name_to_topo,
                    &mut terminal_outcome_shapes,
                    &mut lowerer,
                    diagnostics,
                )
                .map(SagaBranchTransitionV1::AbortUncompensated),
            };
            if let Some(transition) = transition {
                branch_plans.push(SagaOutcomeBranchPlanV1::new(
                    outcome_id,
                    effect_class,
                    transition,
                ));
            }
        }

        if let (Some(step_index), Some(forward_command)) =
            (u16::try_from(topo_u).ok(), forward_call_plan)
            && !branch_plans.is_empty()
        {
            match SagaStepPlanV1::new(
                step_index,
                step.name.value.clone(),
                forward_command,
                compensation_call_plan,
                branch_plans,
            ) {
                Ok(step_plan) => lowered_steps.push(step_plan),
                Err(error) => {
                    diagnostics.push(CompilerDiagnostic::from_ir_error(error, step.name.span));
                }
            }
        }
    }

    if diagnostics.len() > start_diag_count {
        return None;
    }

    let (Some(partition_field), Some(saga_id_field), Some(idempotency_expression)) =
        (partition_field, saga_id_field, idempotency_expression)
    else {
        return None;
    };

    let hir_arena = match lowerer.finish_hir(saga.name.span) {
        Ok(arena) => arena,
        Err(diag) => {
            diagnostics.push(diag);
            return None;
        }
    };
    let expressions = match hir_arena.to_ir(saga.name.span) {
        Ok(arena) => arena,
        Err(diag) => {
            diagnostics.push(diag);
            return None;
        }
    };

    let plan_hash = match compute_saga_plan_hash(
        &saga.name.value,
        partition_field,
        saga_id_field,
        &input_schemas,
        &expressions,
        idempotency_expression,
        &saga.execution_role.value,
        &lowered_steps,
    ) {
        Ok(hash) => hash,
        Err(error) => {
            diagnostics.push(CompilerDiagnostic::from_ir_error(error, saga.name.span));
            return None;
        }
    };

    match SagaPlanV1::new(
        saga.name.value.clone(),
        plan_hash,
        partition_field,
        saga_id_field,
        input_schemas,
        expressions,
        idempotency_expression,
        saga.execution_role.value.clone(),
        lowered_steps,
    ) {
        Ok(plan) => Some(plan),
        Err(error) => {
            diagnostics.push(CompilerDiagnostic::from_ir_error(error, saga.name.span));
            None
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn lower_saga_command_call(
    call: &Spanned<SagaCommandCall>,
    commands_by_name: &BTreeMap<&str, (&HirCommand, &CommandPlan)>,
    role_commands: Option<&BTreeSet<CommandId>>,
    inputs_scope: &BTreeMap<String, (FieldId, ValueType)>,
    bindings_scope: &BTreeMap<String, BindingExpressionScope>,
    step_name_to_topo: &BTreeMap<String, usize>,
    lowerer: &mut ExpressionLowerer<'_>,
    diagnostics: &mut Vec<CompilerDiagnostic>,
) -> Option<SagaCommandCallPlanV1> {
    let cmd_name = &call.value.command;
    let Some(&(hir_cmd, cmd_plan)) = commands_by_name.get(cmd_name.value.as_str()) else {
        diagnostics.push(CompilerDiagnostic::new(
            CompilerDiagnosticCode::UnknownName,
            cmd_name.span,
        ));
        return None;
    };
    if hir_cmd.invocation_class != CommandInvocationClass::Application {
        diagnostics.push(CompilerDiagnostic::new(
            CompilerDiagnosticCode::InvalidWorkflow,
            cmd_name.span,
        ));
    }
    if let Some(allowed) = role_commands
        && !allowed.contains(&cmd_plan.command_id())
    {
        diagnostics.push(
            CompilerDiagnostic::new(CompilerDiagnosticCode::InvalidWorkflow, cmd_name.span)
                .with_cause(CompilerDiagnosticCause::SagaRoleMissingCommandCapability),
        );
    }

    lowerer.set_scope(ExpressionScope::Command {
        command_id: cmd_plan.command_id(),
        inputs: inputs_scope.clone(),
        service_values: BTreeMap::new(),
        bindings: bindings_scope.clone(),
        collection_element: None,
    });

    let mut arg_expressions = Vec::with_capacity(hir_cmd.inputs.len());
    let mut had_error = false;

    if call.value.arguments.len() > hir_cmd.inputs.len() {
        diagnostics.push(CompilerDiagnostic::new(
            CompilerDiagnosticCode::TypeMismatch,
            call.span,
        ));
        had_error = true;
    }

    for (arg_expr, target_input) in call.value.arguments.iter().zip(hir_cmd.inputs.iter()) {
        if check_non_dominating_step_refs(arg_expr, bindings_scope, step_name_to_topo, diagnostics)
        {
            had_error = true;
            continue;
        }

        match lowerer.lower(arg_expr, Some(&target_input.field.value_type)) {
            Ok((expr_id, _)) => {
                arg_expressions.push(FieldExpression::new(target_input.field.id, expr_id));
            }
            Err(diag) => {
                diagnostics.push(diag);
                had_error = true;
            }
        }
    }

    if call.value.arguments.len() < hir_cmd.inputs.len() {
        for missing_input in &hir_cmd.inputs[call.value.arguments.len()..] {
            if missing_input.field.value_type.is_optional() {
                match lowerer.push_constant(
                    CanonicalValue::Null,
                    missing_input.field.value_type.clone(),
                    call.span,
                ) {
                    Ok(expr_id) => {
                        arg_expressions.push(FieldExpression::new(missing_input.field.id, expr_id));
                    }
                    Err(diag) => {
                        diagnostics.push(diag);
                        had_error = true;
                    }
                }
            } else {
                diagnostics.push(CompilerDiagnostic::new(
                    CompilerDiagnosticCode::MissingDeclaration,
                    call.value.command.span,
                ));
                had_error = true;
            }
        }
    }

    if had_error {
        return None;
    }

    match SagaCommandCallPlanV1::new(cmd_plan.command_id(), arg_expressions) {
        Ok(plan) => Some(plan),
        Err(error) => {
            diagnostics.push(CompilerDiagnostic::from_ir_error(error, call.span));
            None
        }
    }
}

fn lower_saga_terminal_outcome(
    outcome: &Spanned<OutcomeExpression>,
    inputs_scope: &BTreeMap<String, (FieldId, ValueType)>,
    bindings_scope: &BTreeMap<String, BindingExpressionScope>,
    step_name_to_topo: &BTreeMap<String, usize>,
    terminal_outcome_shapes: &mut BTreeMap<String, (Span, BTreeMap<String, ValueType>)>,
    lowerer: &mut ExpressionLowerer<'_>,
    diagnostics: &mut Vec<CompilerDiagnostic>,
) -> Option<SagaTerminalOutcomePlanV1> {
    if outcome.value.payload.value.fields.len() > MAX_SAGA_OUTCOME_FIELDS {
        diagnostics.push(CompilerDiagnostic::bound_exceeded(
            CompilerBoundResource::DeclarationCount,
            outcome.value.payload.value.fields.len(),
            MAX_SAGA_OUTCOME_FIELDS,
            outcome.span,
        ));
        return None;
    }

    lowerer.set_scope(ExpressionScope::Command {
        command_id: CommandId::first(),
        inputs: inputs_scope.clone(),
        service_values: BTreeMap::new(),
        bindings: bindings_scope.clone(),
        collection_element: None,
    });

    let mut seen_fields = BTreeMap::<String, Span>::new();
    let mut resolved_shape = BTreeMap::<String, ValueType>::new();
    let mut fields = Vec::with_capacity(outcome.value.payload.value.fields.len());
    let mut had_error = false;

    for field in &outcome.value.payload.value.fields {
        let field_name = &field.value.name;
        if field_name.value == "type" {
            diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::InvalidOutcome,
                field_name.span,
            ));
            had_error = true;
            continue;
        }
        if let Some(&first_span) = seen_fields.get(&field_name.value) {
            diagnostics.push(
                CompilerDiagnostic::new(CompilerDiagnosticCode::DuplicateName, field_name.span)
                    .with_related_span(first_span),
            );
            had_error = true;
            continue;
        }
        seen_fields.insert(field_name.value.clone(), field_name.span);

        if check_non_dominating_step_refs(
            &field.value.value,
            bindings_scope,
            step_name_to_topo,
            diagnostics,
        ) {
            had_error = true;
            continue;
        }

        let expected = terminal_outcome_shapes
            .get(&outcome.value.name.value)
            .and_then(|(_, prior_shape)| prior_shape.get(&field_name.value));

        match lowerer.lower(&field.value.value, expected) {
            Ok((expr_id, value_type)) => {
                resolved_shape.insert(field_name.value.clone(), value_type);
                match SagaOutcomeFieldPlanV1::new(field_name.value.clone(), expr_id) {
                    Ok(field_plan) => fields.push(field_plan),
                    Err(error) => {
                        diagnostics.push(CompilerDiagnostic::from_ir_error(error, field.span));
                        had_error = true;
                    }
                }
            }
            Err(diag) => {
                diagnostics.push(diag);
                had_error = true;
            }
        }
    }

    if !had_error {
        match terminal_outcome_shapes.get(&outcome.value.name.value) {
            Some((first_span, prior_shape)) => {
                if prior_shape != &resolved_shape {
                    diagnostics.push(
                        CompilerDiagnostic::new(
                            CompilerDiagnosticCode::InvalidOutcome,
                            outcome.value.name.span,
                        )
                        .with_related_span(*first_span),
                    );
                    had_error = true;
                }
            }
            None => {
                terminal_outcome_shapes.insert(
                    outcome.value.name.value.clone(),
                    (outcome.value.name.span, resolved_shape),
                );
            }
        }
    }

    if had_error {
        return None;
    }

    match SagaTerminalOutcomePlanV1::new(outcome.value.name.value.clone(), fields) {
        Ok(plan) => Some(plan),
        Err(error) => {
            diagnostics.push(CompilerDiagnostic::from_ir_error(error, outcome.span));
            None
        }
    }
}

fn check_non_dominating_step_refs(
    expression: &Spanned<Expression>,
    bindings_scope: &BTreeMap<String, BindingExpressionScope>,
    step_name_to_topo: &BTreeMap<String, usize>,
    diagnostics: &mut Vec<CompilerDiagnostic>,
) -> bool {
    match &expression.value {
        Expression::Literal(_) => false,
        Expression::Path(path) => {
            if path.value.segments.len() == 2 {
                let first = &path.value.segments[0].value;
                let second = &path.value.segments[1].value;
                if step_name_to_topo.contains_key(first) {
                    let in_dominating_scope = bindings_scope
                        .get(first)
                        .is_some_and(|b| b.fields.contains_key(second));
                    if !in_dominating_scope {
                        diagnostics.push(
                            CompilerDiagnostic::new(
                                CompilerDiagnosticCode::InvalidExpression,
                                path.span,
                            )
                            .with_cause(CompilerDiagnosticCause::SagaNonDominatingStepReference),
                        );
                        return true;
                    }
                }
            }
            false
        }
        Expression::Parenthesized(inner) | Expression::Unary { operand: inner, .. } => {
            check_non_dominating_step_refs(inner, bindings_scope, step_name_to_topo, diagnostics)
        }
        Expression::Binary { left, right, .. } => {
            let l = check_non_dominating_step_refs(
                left,
                bindings_scope,
                step_name_to_topo,
                diagnostics,
            );
            let r = check_non_dominating_step_refs(
                right,
                bindings_scope,
                step_name_to_topo,
                diagnostics,
            );
            l || r
        }
    }
}

fn intersect_outcome_fields(
    command: &CommandPlan,
    outcomes: &[&OutcomeSchema],
) -> BTreeMap<String, (FieldId, ValueType)> {
    let Some(first) = outcomes.first() else {
        return BTreeMap::new();
    };
    let mut common = BTreeMap::new();
    for field in first.payload().fields() {
        let same_in_all = outcomes[1..].iter().all(|other| {
            other
                .payload()
                .fields()
                .iter()
                .any(|f| f.name() == field.name() && f.value_type() == field.value_type())
        });
        if same_in_all
            && let Some(step_field_id) = saga_step_outcome_field_id(command, field.name())
        {
            common.insert(
                field.name().to_owned(),
                (step_field_id, field.value_type().clone()),
            );
        }
    }
    common
}

fn command_outcome_effect_class(
    hir_cmd: &HirCommand,
    cmd_plan: &CommandPlan,
    outcome_id: OutcomeId,
) -> SagaOutcomeEffectClassV1 {
    if outcome_id == cmd_plan.success_outcome() && is_command_mutating(hir_cmd) {
        SagaOutcomeEffectClassV1::Effectful
    } else {
        SagaOutcomeEffectClassV1::NoEffect
    }
}

fn is_command_mutating(hir_cmd: &HirCommand) -> bool {
    let has_mutating_binding = |bindings: &[HirBinding]| {
        bindings
            .iter()
            .any(|b| b.mode != riffdb_contract_ir::BindingMode::Read)
    };
    if has_mutating_binding(&hir_cmd.bindings) || !hir_cmd.effects.is_empty() {
        return true;
    }
    for decision in &hir_cmd.decisions {
        for action in decision
            .when_arms
            .iter()
            .map(|arm| &arm.action)
            .chain(std::iter::once(&decision.else_action))
        {
            if let HirDecisionAction::Apply {
                bindings, effects, ..
            } = action
                && (!bindings.is_empty() || !effects.is_empty())
            {
                return true;
            }
        }
    }
    false
}

fn is_valid_idempotency_type(ty: &ValueType) -> bool {
    match ty.tag() {
        ValueTypeTag::Uuid => true,
        ValueTypeTag::String => ty
            .byte_bound()
            .is_some_and(|bound| (1..=128).contains(&bound)),
        _ => false,
    }
}

fn max_value_type_bytes(ty: &ValueType, schema: &SchemaIr) -> Option<usize> {
    if let Some(RecordTypeRef::Entity(entity_id)) = ty.record_ref() {
        let entity = schema.entity(*entity_id)?;
        let mut sum = 6_usize;
        for field in entity.record().fields() {
            sum = sum.checked_add(max_value_type_bytes(field.value_type(), schema)?)?;
        }
        return Some(sum);
    }
    ty.maximum_canonical_bytes().ok().flatten()
}

fn detect_saga_cycles(
    u: usize,
    adj: &[Vec<(usize, Span)>],
    color: &mut [u8],
    has_cycle: &mut bool,
    diagnostics: &mut Vec<CompilerDiagnostic>,
) {
    color[u] = 1;
    for &(v, span) in &adj[u] {
        if color[v] == 1 {
            *has_cycle = true;
            diagnostics.push(
                CompilerDiagnostic::new(CompilerDiagnosticCode::InvalidWorkflowTransition, span)
                    .with_cause(CompilerDiagnosticCause::SagaStepCycle),
            );
        } else if color[v] == 0 {
            detect_saga_cycles(v, adj, color, has_cycle, diagnostics);
        }
    }
    color[u] = 2;
}

fn compute_topological_order(step_count: usize, adj: &[Vec<(usize, Span)>]) -> Option<Vec<usize>> {
    let mut in_degree = vec![0_usize; step_count];
    for edges in adj {
        for &(v, _) in edges {
            in_degree[v] += 1;
        }
    }
    let mut ready = BTreeSet::new();
    for (idx, &deg) in in_degree.iter().enumerate() {
        if deg == 0 {
            ready.insert(idx);
        }
    }
    let mut order = Vec::with_capacity(step_count);
    while let Some(u) = ready.pop_first() {
        order.push(u);
        for &(v, _) in &adj[u] {
            in_degree[v] -= 1;
            if in_degree[v] == 0 {
                ready.insert(v);
            }
        }
    }
    (order.len() == step_count && order.first() == Some(&0)).then_some(order)
}

fn canonical_name_cmp(left: &str, right: &str) -> std::cmp::Ordering {
    let left_length = u16::try_from(left.len()).expect("parser name bound fits u16");
    let right_length = u16::try_from(right.len()).expect("parser name bound fits u16");
    left_length
        .to_be_bytes()
        .cmp(&right_length.to_be_bytes())
        .then_with(|| left.as_bytes().cmp(right.as_bytes()))
}
