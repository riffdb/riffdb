//! Source-spanned ownership and shape diagnostics for atomic indexed selection.
use crate::diagnostic::{CompilerDiagnostic, CompilerDiagnosticCode};
use crate::hir::{HirCommand, HirDecisionAction, TypedContractHir};
use riffdb_contract_ir::IndexFieldEncodingV1;

pub(crate) fn validate(
    hir: &TypedContractHir,
    command: &HirCommand,
    diagnostics: &mut Vec<CompilerDiagnostic>,
) {
    let selectors: Vec<_> = command
        .bindings
        .iter()
        .filter(|binding| binding.selection.is_some())
        .collect();
    for binding in &selectors {
        let Some((id, prefix)) = &binding.selection else {
            continue;
        };
        let branch = command.decisions.iter().flat_map(|decision| decision.when_arms.iter().map(|arm| &arm.action).chain(std::iter::once(&decision.else_action)))
            .any(|action| matches!(action, HirDecisionAction::Apply { bindings, .. } if bindings.contains(&binding.id)));
        let index = hir
            .entity(binding.entity_id)
            .and_then(|entity| entity.indexes.iter().find(|index| index.id == *id));
        let valid_prefix = index.is_some_and(|index| {
            !hir.entity(binding.entity_id).is_some_and(|entity| {
                entity.fields.iter().any(|field| {
                    field.secret_span.is_some()
                        && index.fields.iter().any(|(id, _)| *id == field.id)
                })
            }) && !prefix.is_empty()
                && prefix.len() <= index.fields.len()
                && index
                    .encodings
                    .iter()
                    .take(prefix.len())
                    .all(|encoding| *encoding == IndexFieldEncodingV1::Canonical)
                && prefix.iter().all(|root| {
                    command
                        .expressions
                        .dependencies(root.id, root.span)
                        .is_ok_and(|deps| deps.is_input_computable())
                })
        });
        let valid_scope = selectors.len() == 1
            && command.collection_expansion.is_none()
            && !branch
            && !command.bindings.iter().any(|other| {
                (other.id != binding.id && other.entity_id == binding.entity_id)
                    || other.restriction_failure.is_some()
                    || other.cascade_failure.is_some()
            });
        let owned = hir.aggregates.iter().find(|aggregate| {
            aggregate.root == binding.entity_id
                || aggregate
                    .children
                    .iter()
                    .any(|(id, _)| *id == binding.entity_id)
        });
        let valid_ownership = owned.is_some_and(|aggregate| {
            let Some(root) = hir.entity(aggregate.root) else {
                return false;
            };
            std::iter::once(&aggregate.keys.partition)
                .chain(aggregate.keys.conflicts.iter())
                .all(|template| {
                    aggregate
                        .keys
                        .expressions
                        .dependencies(template.id, template.span)
                        .is_ok_and(|deps| {
                            deps.schema_fields().iter().all(|(_, field)| {
                                root.key_fields
                                    .iter()
                                    .position(|id| id == field)
                                    .and_then(|position| binding.arguments.get(position))
                                    .is_some_and(|value| {
                                        command
                                            .expressions
                                            .dependencies(value.id, value.span)
                                            .is_ok_and(|deps| deps.is_input_computable())
                                    })
                            })
                        })
                })
        });
        if !valid_prefix || !valid_scope || !valid_ownership {
            diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::InvalidIndexedSelection,
                binding.entity_span,
            ));
        }
    }
}
