//! Per-assignment source evidence and explicit business associations.
use std::collections::BTreeSet;

use riffdb_contract_ir::{
    BinaryOperator, BindingId, BindingMode, ExprId, ExpressionKind, ValueTypeTag,
};
use riffdb_contract_syntax::ast::WriteSpelling;
use riffdb_types::{CanonicalValue, FieldId};

use crate::diagnostic::{CompilerDiagnostic, CompilerDiagnosticCode};
use crate::hir::{
    HirBinding, HirCommand, HirDecisionAction, HirEffect, HirExpressionArena, TypedContractHir,
};
use crate::locality::command_expression_fingerprint;

/// Validate each assignment independently; unrelated reads never grant evidence.
pub(crate) fn validate_write_spellings(
    hir: &TypedContractHir,
    command: &HirCommand,
    diagnostics: &mut Vec<CompilerDiagnostic>,
) {
    let arena = match command.expressions.to_ir(command.span) {
        Ok(arena) => arena,
        Err(error) => {
            diagnostics.push(error);
            return;
        }
    };
    let mut writes = command
        .effects
        .iter()
        .map(|effect| (effect, false))
        .collect::<Vec<_>>();
    for decision in &command.decisions {
        for action in decision
            .when_arms
            .iter()
            .map(|arm| &arm.action)
            .chain(std::iter::once(&decision.else_action))
        {
            if let HirDecisionAction::Apply { effects, .. } = action {
                writes.extend(effects.iter().map(|effect| (effect, true)));
            }
        }
    }
    let mut fenced = BTreeSet::new();
    collect_fenced_bindings(hir, command, &mut fenced);
    for (effect, conditional) in writes {
        let HirEffect::Set {
            target_span,
            spelling,
            binding,
            field,
            value,
            ..
        } = effect
        else {
            continue;
        };
        let Some(bound) = command.bindings.iter().find(|b| b.id == *binding) else {
            continue;
        };
        if !matches!(
            bound.mode,
            BindingMode::Mutate
                | BindingMode::InitOrMutate
                | BindingMode::ObserveOrInitialize
                | BindingMode::Create
        ) {
            continue;
        }
        let association = match association_index(hir, command, effect, conditional) {
            Ok(association) => association,
            Err(error) => {
                diagnostics.push(error);
                continue;
            }
        };
        let derived = riffdb_contract_ir::is_state_derived_write(
            &arena,
            value.id,
            *binding,
            *field,
            |b, f| {
                command
                    .bindings
                    .iter()
                    .find(|candidate| candidate.id == b)
                    .and_then(|b| hir.entity(b.entity_id))
                    .is_some_and(|e| e.key_fields.contains(&f))
            },
        )
        .unwrap_or(false);
        let has_evidence = bound.mode == BindingMode::Create
            || derived
            || association.is_some()
            || fenced.contains(binding);
        match (spelling, has_evidence) {
            (WriteSpelling::Set, false) => diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::UnguardedSet,
                *target_span,
            )),
            (WriteSpelling::Overwrite, true) => diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::GuardedOverwrite,
                *target_span,
            )),
            _ => {}
        }
    }
}

/// Resolve the author association using source scope and actual execution dominance.
pub(crate) fn association_index(
    hir: &TypedContractHir,
    command: &HirCommand,
    effect: &HirEffect,
    conditional: bool,
) -> Result<Option<u32>, CompilerDiagnostic> {
    let HirEffect::Set {
        checked_by: Some(name),
        target_span,
        spelling,
        binding,
        collection_local,
        ..
    } = effect
    else {
        return Ok(None);
    };
    let invalid =
        || CompilerDiagnostic::new(CompilerDiagnosticCode::InvalidWriteAssociation, name.span);
    if conditional
        || *spelling != WriteSpelling::Set
        || command
            .bindings
            .iter()
            .any(|b| b.id == *binding && b.mode == BindingMode::Create)
    {
        return Err(invalid());
    }
    let matches = command
        .requirements
        .iter()
        .enumerate()
        .filter(|(_, r)| r.name == name.value && (!r.collection_local || *collection_local))
        .collect::<Vec<_>>();
    let [(index, requirement)] = matches.as_slice() else {
        return Err(invalid());
    };
    // Bulk outer requirements execute after the repeated instruction range.
    if requirement.collection_local != *collection_local
        || requirement.condition.span.end() >= target_span.start()
    {
        return Err(invalid());
    }
    let deps = command
        .expressions
        .dependencies(requirement.condition.id, requirement.condition.span)?;
    if !deps.bound_fields().iter().any(|(b, field)| {
        *b == *binding
            && command
                .bindings
                .iter()
                .find(|bound| bound.id == *b)
                .and_then(|bound| hir.entity(bound.entity_id))
                .is_some_and(|entity| !entity.key_fields.contains(field))
    }) {
        return Err(invalid());
    }
    Ok(Some(*index as u32))
}

/// Recognize only the closed structural proof in ADR-0256. Decision-arm
/// predicates/effects deliberately cannot supply either half of this fence.
fn collect_fenced_bindings(
    hir: &TypedContractHir,
    command: &HirCommand,
    guarded: &mut BTreeSet<BindingId>,
) {
    let arena = &command.expressions;
    for requirement in &command.requirements {
        if requirement.collection_local {
            continue;
        }
        let Some(ExpressionKind::Binary {
            operator: BinaryOperator::Equal,
            left,
            right,
        }) = arena.node(requirement.condition.id).map(|node| &node.kind)
        else {
            continue;
        };
        let field = compared_input_field(arena, *left, *right)
            .or_else(|| compared_input_field(arena, *right, *left));
        let Some((binding_id, field_id)) = field else {
            continue;
        };
        let Some(root_binding) = command.bindings.iter().find(|binding| {
            binding.id == binding_id
                && binding.mode == BindingMode::Mutate
                && !binding.collection_local
        }) else {
            continue;
        };
        let Some(aggregate) = hir
            .aggregates
            .iter()
            .find(|aggregate| aggregate.root == root_binding.entity_id)
        else {
            continue;
        };
        if !command.effects.iter().any(|effect| {
            let HirEffect::Set {
                binding,
                field,
                value,
                ..
            } = effect
            else {
                return false;
            };
            *binding == binding_id
                && *field == field_id
                && advances_revision(arena, value.id, binding_id, field_id)
        }) {
            continue;
        }
        let Some(root) = hir.entity(aggregate.root) else {
            continue;
        };
        for binding in &command.bindings {
            if (binding.entity_id == aggregate.root
                || aggregate
                    .children
                    .iter()
                    .any(|(entity, _)| *entity == binding.entity_id))
                && same_root_instance(arena, root_binding, binding, root.key_fields.len())
            {
                guarded.insert(binding.id);
            }
        }
    }
}

fn compared_input_field(
    arena: &HirExpressionArena,
    stored: ExprId,
    input: ExprId,
) -> Option<(BindingId, FieldId)> {
    let stored = arena.node(stored)?;
    let input = arena.node(input)?;
    match (&stored.kind, &input.kind) {
        (ExpressionKind::BoundField { binding, field }, ExpressionKind::InputField(_))
            if matches!(
                stored.value_type.tag(),
                ValueTypeTag::I64 | ValueTypeTag::U64
            ) && input.value_type == stored.value_type =>
        {
            Some((*binding, *field))
        }
        _ => None,
    }
}

fn advances_revision(
    arena: &HirExpressionArena,
    value: ExprId,
    binding: BindingId,
    field: FieldId,
) -> bool {
    let Some(ExpressionKind::Binary {
        operator: BinaryOperator::Add,
        left,
        right,
    }) = arena.node(value).map(|node| &node.kind)
    else {
        return false;
    };
    let is_pair = |prior, increment| {
        matches!(arena.node(prior).map(|node| &node.kind),
            Some(ExpressionKind::BoundField { binding: b, field: f }) if *b == binding && *f == field)
            && matches!(
                arena.node(increment).map(|node| &node.kind),
                Some(ExpressionKind::Constant(
                    CanonicalValue::I64(1) | CanonicalValue::U64(1)
                ))
            )
    };
    is_pair(*left, *right) || is_pair(*right, *left)
}

fn same_root_instance(
    arena: &HirExpressionArena,
    root: &HirBinding,
    binding: &HirBinding,
    key_count: usize,
) -> bool {
    root.arguments.len() == key_count
        && binding.arguments.len() >= key_count
        && root
            .arguments
            .iter()
            .zip(&binding.arguments)
            .all(|(left, right)| {
                command_expression_fingerprint(arena, left.id)
                    == command_expression_fingerprint(arena, right.id)
            })
}
