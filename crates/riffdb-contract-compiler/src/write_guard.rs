//! Source spelling proof for local reads and same-root revision fences.
use std::collections::BTreeSet;

use riffdb_contract_ir::{
    BinaryOperator, BindingId, BindingMode, ExprId, ExpressionKind, ValueTypeTag,
};
use riffdb_contract_syntax::{Span, ast::WriteSpelling};
use riffdb_types::{CanonicalValue, FieldId};

use crate::diagnostic::{CompilerDiagnostic, CompilerDiagnosticCode};
use crate::hir::{
    HirBinding, HirCommand, HirDecisionAction, HirEffect, HirExpressionArena, TypedContractHir,
};
use crate::locality::command_expression_fingerprint;

/// A local pre-image dependency guards the binding. A same-root compare-and-
/// increment fence guards its root instance's child bindings too. Invariants
/// only constrain post-images and cannot prove either protection.
pub(crate) fn validate_write_spellings(
    hir: &TypedContractHir,
    command: &HirCommand,
    diagnostics: &mut Vec<CompilerDiagnostic>,
) {
    // Every write the command can perform, including those a compiler-sealed
    // decision arm performs conditionally.
    let mut writes = command.effects.iter().collect::<Vec<_>>();
    let mut predicates = command
        .requirements
        .iter()
        .map(|requirement| &requirement.condition)
        .collect::<Vec<_>>();
    for decision in &command.decisions {
        for arm in &decision.when_arms {
            predicates.push(&arm.predicate);
            if let HirDecisionAction::Apply { effects, .. } = &arm.action {
                writes.extend(effects.iter());
            }
        }
        if let HirDecisionAction::Apply { effects, .. } = &decision.else_action {
            writes.extend(effects.iter());
        }
    }

    let mut guarded_bindings = BTreeSet::new();
    for predicate in predicates {
        collect_read_bindings(
            &command.expressions,
            predicate.id,
            predicate.span,
            &mut guarded_bindings,
            diagnostics,
        );
    }
    for effect in &writes {
        if let HirEffect::Set { value, .. } = effect {
            collect_read_bindings(
                &command.expressions,
                value.id,
                value.span,
                &mut guarded_bindings,
                diagnostics,
            );
        }
    }

    collect_fenced_bindings(hir, command, &mut guarded_bindings);

    for effect in writes {
        let HirEffect::Set {
            target_span,
            spelling,
            binding,
            ..
        } = effect
        else {
            continue;
        };
        let Some(bound) = command
            .bindings
            .iter()
            .find(|candidate| candidate.id == *binding)
        else {
            continue;
        };

        let has_preimage = matches!(
            bound.mode,
            BindingMode::Mutate | BindingMode::InitOrMutate | BindingMode::ObserveOrInitialize
        );
        if !has_preimage {
            if *spelling == WriteSpelling::Overwrite {
                diagnostics.push(CompilerDiagnostic::new(
                    CompilerDiagnosticCode::GuardedOverwrite,
                    *target_span,
                ));
            }
            continue;
        }

        match (spelling, guarded_bindings.contains(binding)) {
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

/// Bindings whose stored fields an expression reads.
///
/// Reading a binding's *key* is not reading its state: a command that names a
/// record by key has observed nothing about what the record holds.
fn collect_read_bindings(
    expressions: &HirExpressionArena,
    root: ExprId,
    span: Span,
    into: &mut BTreeSet<BindingId>,
    diagnostics: &mut Vec<CompilerDiagnostic>,
) {
    match expressions.dependencies(root, span) {
        Ok(dependencies) => into.extend(
            dependencies
                .bound_fields()
                .iter()
                .map(|(binding, _)| *binding),
        ),
        Err(diagnostic) => diagnostics.push(diagnostic),
    }
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
