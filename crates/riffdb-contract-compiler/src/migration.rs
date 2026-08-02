//! Exact-parent migration compilation and proof coverage.

use std::collections::{BTreeMap, BTreeSet};

use riffdb_contract_ir::{
    CompatibilityClass, CompatibilityCode, ContractBundle, ContractCandidateV1, EntitySchema,
    ExpressionArena, FieldSchema, MigrationBundleV1, MigrationConversionV1, MigrationEnumMappingV1,
    MigrationExpressionV1, MigrationResourceBoundsV1, MigrationStepId, MigrationStepKindV1,
    MigrationStepV1, StableIdNamespaceTag, StableIdentity, StableIdentityRename, ValueType,
    compare_successor,
};
use riffdb_contract_syntax::ast::Expression;
use riffdb_contract_syntax::{
    MigrationDeclaration, MigrationDocument, MigrationEnumMap, MigrationIdentityKind,
    MigrationTransformClause,
};
use riffdb_contract_syntax::{Span, Spanned, parse_migration};
use riffdb_types::{ContractVersion, EntityTypeId, FieldId, hash_migration_source};

use crate::compiler::CompilationError;
use crate::diagnostic::{CompilerDiagnostic, CompilerDiagnosticCode, CompilerDiagnostics};
use crate::expression_lowering::{ExpressionLowerer, ExpressionScope};
use crate::symbols::GenesisSymbols;

/// Binds the rename declarations that must participate in successor ID allocation.
pub(crate) fn bind_identity_renames(
    document: &MigrationDocument,
    parent: &ContractBundle,
) -> Result<Vec<StableIdentityRename>, CompilationError> {
    let mut renames = Vec::new();
    let mut diagnostics = Vec::new();
    for declaration in &document.migration.value.declarations {
        let MigrationDeclaration::Rename(rename) = &declaration.value else {
            continue;
        };
        let Some(from) = resolve_parent_identity(
            parent,
            rename.kind.value,
            &rename
                .old_path
                .iter()
                .map(|part| part.value.as_str())
                .collect::<Vec<_>>(),
        ) else {
            diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::MissingMigrationProof,
                declaration.span,
            ));
            continue;
        };
        let to = StableIdentity::new(from.namespace().clone(), rename.new_name.value.clone())
            .and_then(|to| StableIdentityRename::new(from, to))
            .map_err(|_| {
                semantic_error(
                    CompilerDiagnosticCode::InvalidMigrationIdentity,
                    declaration.span,
                )
            })?;
        renames.push(to);
    }
    if diagnostics.is_empty() {
        Ok(renames)
    } else {
        Err(CompilationError::Semantic(
            CompilerDiagnostics::new(diagnostics).expect("rename diagnostics are nonempty"),
        ))
    }
}

/// Compiles one direct-parent migration source into a canonical typed artifact.
pub fn compile_migration_source(
    source: &str,
    parent: &ContractBundle,
    candidate: &ContractBundle,
) -> Result<MigrationBundleV1, CompilationError> {
    let document = parse_migration(source).map_err(CompilationError::Syntax)?;
    validate_identity(&document, parent, candidate)?;
    let direct_compatibility = ContractCandidateV1::new(
        candidate.schema(),
        candidate.commands(),
        candidate.projections(),
        candidate.mcp_command_names(),
    )
    .and_then(|direct| compare_successor(parent, direct))
    .map_err(|_| semantic_error(CompilerDiagnosticCode::InvalidIr, document.migration.span))?;
    if direct_compatibility.entries().iter().any(|entry| {
        entry.class() == CompatibilityClass::Incompatible
            && entry.code() != CompatibilityCode::RemovedIdentity
    }) {
        return Err(semantic_error(
            CompilerDiagnosticCode::UnsupportedMigrationStep,
            document.migration.span,
        ));
    }

    let required_fields = required_field_additions(parent, candidate);
    let symbols = migration_symbols(candidate);
    let mut proofs = BTreeMap::<(EntityTypeId, FieldId), (Span, MigrationStepKindV1)>::new();
    let mut declaration_steps = Vec::<(String, MigrationStepKindV1)>::new();
    let mut covered_removed = BTreeSet::<StableIdentity>::new();
    let mut diagnostics = Vec::new();

    for declaration in &document.migration.value.declarations {
        if let MigrationDeclaration::Rename(rename) = &declaration.value {
            let one = MigrationDocument {
                migration: Spanned::new(
                    riffdb_contract_syntax::Migration {
                        lineage: document.migration.value.lineage.clone(),
                        from: document.migration.value.from.clone(),
                        to: document.migration.value.to.clone(),
                        declarations: vec![declaration.clone()],
                    },
                    document.migration.span,
                ),
            };
            match bind_identity_renames(&one, parent) {
                Ok(bound) => {
                    let Some(rename_identity) = bound.into_iter().next() else {
                        diagnostics.push(CompilerDiagnostic::new(
                            CompilerDiagnosticCode::MissingMigrationProof,
                            declaration.span,
                        ));
                        continue;
                    };
                    let Some(stable_id) = candidate.ledger().active_id(rename_identity.to()) else {
                        diagnostics.push(CompilerDiagnostic::new(
                            CompilerDiagnosticCode::MissingMigrationProof,
                            declaration.span,
                        ));
                        continue;
                    };
                    if !candidate.ledger().aliases().iter().any(|alias| {
                        alias.identity() == rename_identity.from() && alias.id() == stable_id
                    }) {
                        diagnostics.push(CompilerDiagnostic::new(
                            CompilerDiagnosticCode::MissingMigrationProof,
                            declaration.span,
                        ));
                        continue;
                    }
                    declaration_steps.push((
                        format!(
                            "01:{:02x}:{stable_id:010}",
                            rename_identity.to().namespace().tag() as u8
                        ),
                        MigrationStepKindV1::RenameIdentity {
                            namespace: rename_identity.to().namespace().tag(),
                            owner_kind: rename_identity.to().namespace().owner_kind(),
                            owner_ids: rename_identity.to().namespace().owner_ids().to_vec(),
                            stable_id,
                            new_name: rename.new_name.value.clone(),
                        },
                    ));
                    covered_removed.insert(rename_identity.from().clone());
                }
                Err(_) => diagnostics.push(CompilerDiagnostic::new(
                    CompilerDiagnosticCode::InvalidMigrationIdentity,
                    declaration.span,
                )),
            }
            continue;
        }
        if let MigrationDeclaration::Retire(retirement) = &declaration.value {
            let path = retirement
                .path
                .iter()
                .map(|part| part.value.as_str())
                .collect::<Vec<_>>();
            let Some(identity) = resolve_parent_identity(parent, retirement.kind.value, &path)
            else {
                diagnostics.push(CompilerDiagnostic::new(
                    CompilerDiagnosticCode::UnnecessaryMigrationProof,
                    declaration.span,
                ));
                continue;
            };
            let Some(stable_id) = ledger_identity_id(parent, &identity) else {
                diagnostics.push(CompilerDiagnostic::new(
                    CompilerDiagnosticCode::InvalidMigrationIdentity,
                    declaration.span,
                ));
                continue;
            };
            if !ledger_identity_is_tombstone(candidate, &identity, stable_id) {
                diagnostics.push(CompilerDiagnostic::new(
                    CompilerDiagnosticCode::UnnecessaryMigrationProof,
                    declaration.span,
                ));
                continue;
            }
            covered_removed.extend(retirement_identity_closure(parent, &identity, stable_id));
            declaration_steps.push((
                format!(
                    "02:{:02x}:{stable_id:010}",
                    identity.namespace().tag() as u8
                ),
                MigrationStepKindV1::RetireIdentity {
                    namespace: identity.namespace().tag(),
                    owner_kind: identity.namespace().owner_kind(),
                    owner_ids: identity.namespace().owner_ids().to_vec(),
                    stable_id,
                },
            ));
            continue;
        }
        if let MigrationDeclaration::EnumMap(mapping) = &declaration.value {
            match lower_enum_map(mapping, parent, candidate) {
                Ok((kind, removed)) => {
                    covered_removed.extend(removed);
                    let enum_id = match &kind {
                        MigrationStepKindV1::MapEnum { enumeration, .. } => enumeration.get(),
                        _ => unreachable!("enum lowering returns enum map"),
                    };
                    declaration_steps.push((format!("13:{enum_id:010}"), kind));
                }
                Err(code) => diagnostics.push(CompilerDiagnostic::new(code, declaration.span)),
            }
            continue;
        }
        let MigrationDeclaration::Transform(transform) = &declaration.value else {
            diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::UnsupportedMigrationStep,
                declaration.span,
            ));
            continue;
        };
        let Some(parent_entity) = entity_named(parent, &transform.entity.value) else {
            diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::UnnecessaryMigrationProof,
                transform.entity.span,
            ));
            continue;
        };
        let Some(candidate_entity) = candidate.schema().entity(parent_entity.id()) else {
            diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::UnnecessaryMigrationProof,
                transform.entity.span,
            ));
            continue;
        };

        for clause in &transform.clauses {
            if let MigrationTransformClause::Replace {
                old_field,
                new_field,
                conversion,
            } = &clause.value
            {
                let Some(old) = parent_entity
                    .record()
                    .fields()
                    .iter()
                    .find(|field| field.name() == old_field.value)
                else {
                    diagnostics.push(CompilerDiagnostic::new(
                        CompilerDiagnosticCode::UnnecessaryMigrationProof,
                        old_field.span,
                    ));
                    continue;
                };
                let Some(new) = candidate_entity
                    .record()
                    .fields()
                    .iter()
                    .find(|field| field.name() == new_field.value)
                else {
                    diagnostics.push(CompilerDiagnostic::new(
                        CompilerDiagnosticCode::MissingMigrationProof,
                        new_field.span,
                    ));
                    continue;
                };
                let Some(conversion_kind) = parse_conversion(&conversion.value) else {
                    diagnostics.push(CompilerDiagnostic::new(
                        CompilerDiagnosticCode::InvalidMigrationExpression,
                        conversion.span,
                    ));
                    continue;
                };
                if old.id() == new.id()
                    || !conversion_types_compatible(
                        conversion_kind,
                        old.value_type(),
                        new.value_type(),
                    )
                {
                    diagnostics.push(CompilerDiagnostic::new(
                        CompilerDiagnosticCode::InvalidMigrationExpression,
                        clause.span,
                    ));
                    continue;
                }
                let identity = stable_identity_for_field(parent_entity.id(), old.name());
                if !ledger_identity_is_tombstone(candidate, &identity, old.id().get()) {
                    diagnostics.push(CompilerDiagnostic::new(
                        CompilerDiagnosticCode::InvalidMigrationIdentity,
                        clause.span,
                    ));
                    continue;
                }
                let key = (candidate_entity.id(), new.id());
                if proofs.contains_key(&key) {
                    diagnostics.push(CompilerDiagnostic::new(
                        CompilerDiagnosticCode::DuplicateMigrationProof,
                        clause.span,
                    ));
                    continue;
                }
                covered_removed.insert(identity);
                proofs.insert(
                    key,
                    (
                        clause.span,
                        MigrationStepKindV1::ReplaceField {
                            entity: candidate_entity.id(),
                            old_field: old.id(),
                            new_field: new.id(),
                            conversion: conversion_kind,
                        },
                    ),
                );
                continue;
            }
            if let MigrationTransformClause::Require(expression) = &clause.value {
                match lower_required_expression(expression, parent_entity, &symbols) {
                    Ok(predicate) => declaration_steps.push((
                        format!("12:{:010}", parent_entity.id().get()),
                        MigrationStepKindV1::RequireEntity {
                            entity: parent_entity.id(),
                            predicate,
                        },
                    )),
                    Err(diagnostic) => diagnostics.push(diagnostic),
                }
                continue;
            }
            let MigrationTransformClause::Set { field, expression } = &clause.value else {
                diagnostics.push(CompilerDiagnostic::new(
                    CompilerDiagnosticCode::UnsupportedMigrationStep,
                    clause.span,
                ));
                continue;
            };
            let Some(candidate_field) = candidate_entity
                .record()
                .fields()
                .iter()
                .find(|candidate| candidate.name() == field.value)
            else {
                diagnostics.push(CompilerDiagnostic::new(
                    CompilerDiagnosticCode::UnnecessaryMigrationProof,
                    field.span,
                ));
                continue;
            };
            let key = (candidate_entity.id(), candidate_field.id());
            if !required_fields.contains_key(&key) {
                diagnostics.push(CompilerDiagnostic::new(
                    CompilerDiagnosticCode::UnnecessaryMigrationProof,
                    field.span,
                ));
                continue;
            }
            if let Some((first_span, _)) = proofs.get(&key) {
                diagnostics.push(
                    CompilerDiagnostic::new(
                        CompilerDiagnosticCode::DuplicateMigrationProof,
                        field.span,
                    )
                    .with_related_span(*first_span),
                );
                continue;
            }
            match lower_set_expression(expression, parent_entity, candidate_field, &symbols) {
                Ok(expression) => {
                    proofs.insert(
                        key,
                        (
                            field.span,
                            MigrationStepKindV1::SetField {
                                entity: candidate_entity.id(),
                                field: candidate_field.id(),
                                expression,
                            },
                        ),
                    );
                }
                Err(diagnostic) => diagnostics.push(diagnostic),
            }
        }
    }

    for key in required_fields.keys() {
        if !proofs.contains_key(key) {
            diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::MissingMigrationProof,
                document.migration.span,
            ));
        }
    }
    for identity in removed_identities(parent, candidate) {
        if !covered_removed.contains(&identity) {
            diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::MissingMigrationProof,
                document.migration.span,
            ));
        }
    }
    if !diagnostics.is_empty() {
        return Err(CompilationError::Semantic(
            CompilerDiagnostics::new(diagnostics).expect("migration diagnostics are nonempty"),
        ));
    }

    let mut ordered = proofs
        .into_iter()
        .map(|((entity, field), (_, kind))| {
            (format!("10:{:010}:{:010}", entity.get(), field.get()), kind)
        })
        .collect::<Vec<_>>();
    ordered.extend(declaration_steps);
    derive_successor_steps(parent, candidate, &mut ordered);
    ordered.sort_unstable_by(|left, right| left.0.cmp(&right.0));

    if ordered.is_empty() {
        return Err(semantic_error(
            CompilerDiagnosticCode::UnnecessaryMigrationProof,
            document.migration.span,
        ));
    }
    let steps = ordered
        .into_iter()
        .enumerate()
        .map(|(index, (_, kind))| {
            let numeric_id = u32::try_from(index + 1).expect("migration step bound fits u32");
            let id = MigrationStepId::new(numeric_id).expect("one-based migration step ID");
            let dependencies = if numeric_id == 1 {
                Vec::new()
            } else {
                vec![MigrationStepId::new(numeric_id - 1).expect("positive predecessor step")]
            };
            MigrationStepV1::new(id, dependencies, kind)
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| semantic_error(CompilerDiagnosticCode::InvalidIr, document.migration.span))?;

    MigrationBundleV1::new(
        env!("CARGO_PKG_VERSION"),
        candidate.lineage().clone(),
        parent.contract_version(),
        parent.bundle_hash(),
        candidate.contract_version(),
        candidate.bundle_hash(),
        hash_migration_source(source.as_bytes()),
        steps,
        MigrationResourceBoundsV1::fixed(),
    )
    .map_err(|_| semantic_error(CompilerDiagnosticCode::InvalidIr, document.migration.span))
}

fn validate_identity(
    document: &MigrationDocument,
    parent: &ContractBundle,
    candidate: &ContractBundle,
) -> Result<(), CompilationError> {
    let migration = &document.migration.value;
    let from = migration
        .from
        .value
        .parse::<u64>()
        .ok()
        .and_then(ContractVersion::new);
    let to = migration
        .to
        .value
        .parse::<u64>()
        .ok()
        .and_then(ContractVersion::new);
    if migration.lineage.value != parent.lineage().as_str()
        || parent.lineage() != candidate.lineage()
        || from != Some(parent.contract_version())
        || to != Some(candidate.contract_version())
        || parent.contract_version() >= candidate.contract_version()
    {
        return Err(semantic_error(
            CompilerDiagnosticCode::InvalidMigrationIdentity,
            document.migration.span,
        ));
    }
    Ok(())
}

fn required_field_additions<'a>(
    parent: &'a ContractBundle,
    candidate: &'a ContractBundle,
) -> BTreeMap<(EntityTypeId, FieldId), (&'a EntitySchema, &'a FieldSchema)> {
    let mut additions = BTreeMap::new();
    for entity in candidate.schema().entities() {
        let Some(old) = parent.schema().entity(entity.id()) else {
            continue;
        };
        for field in entity.record().fields() {
            if old.record().field(field.id()).is_none() && !field.value_type().is_optional() {
                additions.insert((entity.id(), field.id()), (entity, field));
            }
        }
    }
    additions
}

fn lower_set_expression(
    expression: &Spanned<Expression>,
    parent_entity: &EntitySchema,
    candidate_field: &FieldSchema,
    symbols: &GenesisSymbols,
) -> Result<MigrationExpressionV1, CompilerDiagnostic> {
    let fields = parent_entity
        .record()
        .fields()
        .iter()
        .map(|field| {
            (
                field.name().to_owned(),
                (field.id(), field.value_type().clone()),
            )
        })
        .collect();
    let mut lowerer = ExpressionLowerer::new(
        symbols,
        ExpressionScope::Migration {
            entity_id: parent_entity.id(),
            fields,
        },
    );
    let (result, _) = lowerer
        .lower(expression, Some(candidate_field.value_type()))
        .map_err(|_| {
            CompilerDiagnostic::new(
                CompilerDiagnosticCode::InvalidMigrationExpression,
                expression.span,
            )
        })?;
    let arena: ExpressionArena = lowerer
        .finish_hir(expression.span)
        .and_then(|arena| arena.to_ir(expression.span))
        .map_err(|_| {
            CompilerDiagnostic::new(
                CompilerDiagnosticCode::InvalidMigrationExpression,
                expression.span,
            )
        })?;
    MigrationExpressionV1::new(arena, result).map_err(|_| {
        CompilerDiagnostic::new(
            CompilerDiagnosticCode::InvalidMigrationExpression,
            expression.span,
        )
    })
}

fn lower_required_expression(
    expression: &Spanned<Expression>,
    parent_entity: &EntitySchema,
    symbols: &GenesisSymbols,
) -> Result<MigrationExpressionV1, CompilerDiagnostic> {
    let expected = ValueType::bool();
    let synthetic = FieldSchema::new(
        FieldId::new(1).expect("synthetic field ID"),
        "migration_predicate",
        expected,
    )
    .expect("synthetic predicate field");
    lower_set_expression(expression, parent_entity, &synthetic, symbols)
}

fn parse_conversion(name: &str) -> Option<MigrationConversionV1> {
    Some(match name {
        "identity" => MigrationConversionV1::Identity,
        "wrap_optional" => MigrationConversionV1::WrapOptional,
        "assert_unwrap_optional" => MigrationConversionV1::AssertUnwrapOptional,
        "checked_i64_to_u64" => MigrationConversionV1::CheckedI64ToU64,
        "checked_u64_to_i64" => MigrationConversionV1::CheckedU64ToI64,
        "decimal_exact" => MigrationConversionV1::ExactDecimal,
        "assert_bounded_narrow" => MigrationConversionV1::AssertBoundedNarrow,
        "list_elements" => MigrationConversionV1::ListElements,
        "uuid_to_string" => MigrationConversionV1::UuidToString,
        "string_to_uuid" => MigrationConversionV1::StringToUuid,
        _ => return None,
    })
}

fn conversion_types_compatible(
    conversion: MigrationConversionV1,
    old: &ValueType,
    new: &ValueType,
) -> bool {
    conversion.accepts(old, new)
}

fn lower_enum_map(
    mapping: &MigrationEnumMap,
    parent: &ContractBundle,
    candidate: &ContractBundle,
) -> Result<(MigrationStepKindV1, BTreeSet<StableIdentity>), CompilerDiagnosticCode> {
    let old = parent
        .schema()
        .enums()
        .iter()
        .find(|value| value.name() == mapping.enumeration.value)
        .ok_or(CompilerDiagnosticCode::UnnecessaryMigrationProof)?;
    let new = candidate
        .schema()
        .enumeration(old.id())
        .ok_or(CompilerDiagnosticCode::MissingMigrationProof)?;
    let mut by_from = BTreeMap::new();
    for item in &mapping.mappings {
        let from = old
            .variants()
            .iter()
            .find(|value| value.name() == item.value.from.value)
            .ok_or(CompilerDiagnosticCode::InvalidMigrationExpression)?;
        let to = new
            .variants()
            .iter()
            .find(|value| value.name() == item.value.to.value)
            .ok_or(CompilerDiagnosticCode::InvalidMigrationExpression)?;
        if by_from
            .insert(from.id(), MigrationEnumMappingV1::new(from.id(), to.id()))
            .is_some()
        {
            return Err(CompilerDiagnosticCode::DuplicateMigrationProof);
        }
    }
    if by_from.len() != old.variants().len() {
        return Err(CompilerDiagnosticCode::MissingMigrationProof);
    }
    let removed = old
        .variants()
        .iter()
        .filter(|variant| new.variants().iter().all(|next| next.id() != variant.id()))
        .map(|variant| {
            StableIdentity::new(
                riffdb_contract_ir::StableIdNamespace::new(
                    StableIdNamespaceTag::EnumVariant,
                    0x01,
                    vec![old.id().get()],
                )
                .expect("enum variant namespace"),
                variant.name(),
            )
            .expect("checked enum variant identity")
        })
        .collect();
    Ok((
        MigrationStepKindV1::MapEnum {
            enumeration: old.id(),
            mappings: by_from.into_values().collect(),
        },
        removed,
    ))
}

fn stable_identity_for_field(entity: EntityTypeId, name: &str) -> StableIdentity {
    StableIdentity::new(
        riffdb_contract_ir::StableIdNamespace::new(
            StableIdNamespaceTag::Field,
            0x01,
            vec![entity.get()],
        )
        .expect("entity field namespace"),
        name,
    )
    .expect("checked field identity")
}

fn ledger_identity_id(bundle: &ContractBundle, identity: &StableIdentity) -> Option<u32> {
    bundle
        .ledger()
        .allocations()
        .iter()
        .flat_map(|allocation| allocation.entries())
        .find(|entry| entry.identity() == identity)
        .map(|entry| entry.id())
}

fn ledger_identity_is_tombstone(
    bundle: &ContractBundle,
    identity: &StableIdentity,
    id: u32,
) -> bool {
    bundle
        .ledger()
        .allocations()
        .iter()
        .flat_map(|allocation| allocation.entries())
        .any(|entry| {
            entry.identity() == identity
                && entry.id() == id
                && entry.state() == riffdb_contract_ir::LineageEntryState::Tombstone
        })
}

fn removed_identities(
    parent: &ContractBundle,
    candidate: &ContractBundle,
) -> BTreeSet<StableIdentity> {
    parent
        .ledger()
        .allocations()
        .iter()
        .flat_map(|allocation| allocation.entries())
        .filter(|entry| entry.state() == riffdb_contract_ir::LineageEntryState::Active)
        .filter(|entry| candidate.ledger().active_id(entry.identity()).is_none())
        .map(|entry| entry.identity().clone())
        .collect()
}

fn retirement_identity_closure(
    parent: &ContractBundle,
    identity: &StableIdentity,
    id: u32,
) -> BTreeSet<StableIdentity> {
    let retired_aggregates = if identity.namespace().tag() == StableIdNamespaceTag::Entity {
        parent
            .schema()
            .aggregates()
            .iter()
            .filter(|aggregate| {
                aggregate.root().get() == id
                    || aggregate.children().iter().any(|child| child.get() == id)
            })
            .map(|aggregate| aggregate.id().get())
            .collect::<BTreeSet<_>>()
    } else {
        BTreeSet::new()
    };
    parent
        .ledger()
        .allocations()
        .iter()
        .flat_map(|allocation| allocation.entries())
        .filter(|entry| entry.state() == riffdb_contract_ir::LineageEntryState::Active)
        .filter(|entry| {
            entry.identity() == identity
                || entry.identity().namespace().tag() == StableIdNamespaceTag::Aggregate
                    && retired_aggregates.contains(&entry.id())
                || entry.identity().namespace().tag() == StableIdNamespaceTag::Invariant
                    && entry.identity().namespace().owner_kind() == 0x02
                    && entry
                        .identity()
                        .namespace()
                        .owner_ids()
                        .first()
                        .is_some_and(|owner| retired_aggregates.contains(owner))
                || match identity.namespace().tag() {
                    StableIdNamespaceTag::Entity => {
                        matches!(
                            (
                                entry.identity().namespace().tag(),
                                entry.identity().namespace().owner_kind()
                            ),
                            (StableIdNamespaceTag::Field, 0x01)
                                | (StableIdNamespaceTag::Index, 0x01)
                                | (StableIdNamespaceTag::Invariant, 0x01)
                        ) && entry.identity().namespace().owner_ids().first() == Some(&id)
                    }
                    StableIdNamespaceTag::Event => {
                        entry.identity().namespace().tag() == StableIdNamespaceTag::Field
                            && entry.identity().namespace().owner_kind() == 0x02
                            && entry.identity().namespace().owner_ids().first() == Some(&id)
                    }
                    StableIdNamespaceTag::Enum => {
                        entry.identity().namespace().tag() == StableIdNamespaceTag::EnumVariant
                            && entry.identity().namespace().owner_kind() == 0x01
                            && entry.identity().namespace().owner_ids().first() == Some(&id)
                    }
                    StableIdNamespaceTag::Command => {
                        matches!(
                            (
                                entry.identity().namespace().tag(),
                                entry.identity().namespace().owner_kind()
                            ),
                            (StableIdNamespaceTag::Field, 0x03 | 0x04)
                                | (StableIdNamespaceTag::Outcome, 0x01)
                        ) && entry.identity().namespace().owner_ids().first() == Some(&id)
                    }
                    StableIdNamespaceTag::Projection => {
                        entry.identity().namespace().tag() == StableIdNamespaceTag::Field
                            && entry.identity().namespace().owner_kind() == 0x05
                            && entry.identity().namespace().owner_ids().first() == Some(&id)
                    }
                    _ => false,
                }
        })
        .map(|entry| entry.identity().clone())
        .collect()
}

fn derive_successor_steps(
    parent: &ContractBundle,
    candidate: &ContractBundle,
    ordered: &mut Vec<(String, MigrationStepKindV1)>,
) {
    for entity in candidate.schema().entities() {
        let Some(old) = parent.schema().entity(entity.id()) else {
            continue;
        };
        let old_indexes = old
            .indexes()
            .iter()
            .map(|index| index.id())
            .collect::<BTreeSet<_>>();
        for index in entity.indexes() {
            if !old_indexes.contains(&index.id()) {
                ordered.push((
                    format!("20:{:010}:{:010}", entity.id().get(), index.id().get()),
                    MigrationStepKindV1::RebuildIndex {
                        entity: entity.id(),
                        index: index.id(),
                    },
                ));
            }
        }
        let old_invariants = old
            .invariants()
            .iter()
            .map(|invariant| invariant.id())
            .collect::<BTreeSet<_>>();
        for invariant in entity.invariants() {
            if !old_invariants.contains(&invariant.id()) {
                ordered.push((
                    format!(
                        "50:01:{:010}:{:010}",
                        entity.id().get(),
                        invariant.id().get()
                    ),
                    MigrationStepKindV1::ValidateInvariant {
                        owner_namespace: StableIdNamespaceTag::Entity,
                        owner_id: entity.id().get(),
                        invariant: invariant.id(),
                    },
                ));
            }
        }
    }

    let old_relationships = parent
        .schema()
        .relationships()
        .iter()
        .map(|value| (value.source_entity(), value.name()))
        .collect::<BTreeSet<_>>();
    for relationship in candidate.schema().relationships() {
        if parent
            .schema()
            .entity(relationship.source_entity())
            .is_some()
            && !old_relationships.contains(&(relationship.source_entity(), relationship.name()))
        {
            ordered.push((
                format!(
                    "30:{:010}:{}",
                    relationship.source_entity().get(),
                    relationship.name()
                ),
                MigrationStepKindV1::ValidateRelationship {
                    source_entity: relationship.source_entity(),
                    name: relationship.name().to_owned(),
                },
            ));
        }
    }

    let old_unique = parent
        .schema()
        .unique_keys()
        .iter()
        .map(|value| (value.source_entity(), value.name()))
        .collect::<BTreeSet<_>>();
    for unique in candidate.schema().unique_keys() {
        if parent.schema().entity(unique.source_entity()).is_some()
            && !old_unique.contains(&(unique.source_entity(), unique.name()))
        {
            ordered.push((
                format!(
                    "40:{:010}:{:010}",
                    unique.source_entity().get(),
                    unique.index_id().get()
                ),
                MigrationStepKindV1::ValidateUnique {
                    entity: unique.source_entity(),
                    index: unique.index_id(),
                },
            ));
        }
    }

    for aggregate in candidate.schema().aggregates() {
        let Some(old) = parent.schema().aggregate(aggregate.id()) else {
            continue;
        };
        let old_invariants = old
            .invariants()
            .iter()
            .map(|invariant| invariant.id())
            .collect::<BTreeSet<_>>();
        for invariant in aggregate.invariants() {
            if !old_invariants.contains(&invariant.id()) {
                ordered.push((
                    format!(
                        "50:02:{:010}:{:010}",
                        aggregate.id().get(),
                        invariant.id().get()
                    ),
                    MigrationStepKindV1::ValidateInvariant {
                        owner_namespace: StableIdNamespaceTag::Aggregate,
                        owner_id: aggregate.id().get(),
                        invariant: invariant.id(),
                    },
                ));
            }
        }
    }

    let old_projections = parent
        .projections()
        .iter()
        .map(|projection| projection.projection_id())
        .collect::<BTreeSet<_>>();
    for projection in candidate.projections() {
        if !old_projections.contains(&projection.projection_id())
            && parent.schema().event(projection.source_event()).is_some()
        {
            ordered.push((
                format!("60:{:010}", projection.projection_id().get()),
                MigrationStepKindV1::RebuildProjection {
                    projection: projection.projection_id(),
                },
            ));
        }
    }
}

fn entity_named<'a>(bundle: &'a ContractBundle, name: &str) -> Option<&'a EntitySchema> {
    bundle
        .schema()
        .entities()
        .iter()
        .find(|entity| entity.name() == name)
}

fn resolve_parent_identity(
    parent: &ContractBundle,
    kind: MigrationIdentityKind,
    path: &[&str],
) -> Option<StableIdentity> {
    let (tag, owner_kind, owner_ids, name) = match (kind, path) {
        (MigrationIdentityKind::Entity, [name]) => {
            let value = parent
                .schema()
                .entities()
                .iter()
                .find(|value| value.name() == *name)?;
            (StableIdNamespaceTag::Entity, 0, vec![], value.name())
        }
        (MigrationIdentityKind::Event, [name]) => {
            let value = parent
                .schema()
                .events()
                .iter()
                .find(|value| value.name() == *name)?;
            (StableIdNamespaceTag::Event, 0, vec![], value.name())
        }
        (MigrationIdentityKind::Enum, [name]) => {
            let value = parent
                .schema()
                .enums()
                .iter()
                .find(|value| value.name() == *name)?;
            (StableIdNamespaceTag::Enum, 0, vec![], value.name())
        }
        (MigrationIdentityKind::Command, [name]) => {
            let value = parent
                .commands()
                .iter()
                .find(|value| value.name() == *name)?;
            (StableIdNamespaceTag::Command, 0, vec![], value.name())
        }
        (MigrationIdentityKind::Projection, [name]) => {
            let value = parent
                .projections()
                .iter()
                .find(|value| value.name() == *name)?;
            (StableIdNamespaceTag::Projection, 0, vec![], value.name())
        }
        (MigrationIdentityKind::EnumVariant, [owner, name]) => {
            let owner = parent
                .schema()
                .enums()
                .iter()
                .find(|value| value.name() == *owner)?;
            let value = owner
                .variants()
                .iter()
                .find(|value| value.name() == *name)?;
            (
                StableIdNamespaceTag::EnumVariant,
                0x01,
                vec![owner.id().get()],
                value.name(),
            )
        }
        (MigrationIdentityKind::Outcome, [command, name]) => {
            let command = parent
                .commands()
                .iter()
                .find(|value| value.name() == *command)?;
            let value = command
                .outcomes()
                .iter()
                .find(|value| value.name() == *name)?;
            (
                StableIdNamespaceTag::Outcome,
                0x01,
                vec![command.command_id().get()],
                value.name(),
            )
        }
        (MigrationIdentityKind::Field, [owner, name]) => {
            if let Some(owner) = parent
                .schema()
                .entities()
                .iter()
                .find(|value| value.name() == *owner)
            {
                let value = owner
                    .record()
                    .fields()
                    .iter()
                    .find(|value| value.name() == *name)?;
                (
                    StableIdNamespaceTag::Field,
                    0x01,
                    vec![owner.id().get()],
                    value.name(),
                )
            } else if let Some(owner) = parent
                .schema()
                .events()
                .iter()
                .find(|value| value.name() == *owner)
            {
                let value = owner
                    .payload()
                    .fields()
                    .iter()
                    .find(|value| value.name() == *name)?;
                (
                    StableIdNamespaceTag::Field,
                    0x02,
                    vec![owner.id().get()],
                    value.name(),
                )
            } else if let Some(owner) = parent
                .commands()
                .iter()
                .find(|value| value.name() == *owner)
            {
                let value = owner
                    .input()
                    .record()
                    .fields()
                    .iter()
                    .find(|value| value.name() == *name)?;
                (
                    StableIdNamespaceTag::Field,
                    0x03,
                    vec![owner.command_id().get()],
                    value.name(),
                )
            } else {
                let owner = parent
                    .projections()
                    .iter()
                    .find(|value| value.name() == *owner)?;
                let value = owner
                    .group_schema()
                    .measures()
                    .fields()
                    .iter()
                    .find(|value| value.name() == *name)?;
                (
                    StableIdNamespaceTag::Field,
                    0x05,
                    vec![owner.projection_id().get()],
                    value.name(),
                )
            }
        }
        (MigrationIdentityKind::Field, [command, outcome, name]) => {
            let command = parent
                .commands()
                .iter()
                .find(|value| value.name() == *command)?;
            let outcome = command
                .outcomes()
                .iter()
                .find(|value| value.name() == *outcome)?;
            let value = outcome
                .payload()
                .fields()
                .iter()
                .find(|value| value.name() == *name)?;
            (
                StableIdNamespaceTag::Field,
                0x04,
                vec![command.command_id().get(), outcome.id().get()],
                value.name(),
            )
        }
        _ => return None,
    };
    StableIdentity::new(
        riffdb_contract_ir::StableIdNamespace::new(tag, owner_kind, owner_ids).ok()?,
        name,
    )
    .ok()
}

fn migration_symbols(candidate: &ContractBundle) -> GenesisSymbols {
    let enums = candidate
        .schema()
        .enums()
        .iter()
        .map(|enumeration| (enumeration.name().to_owned(), enumeration.id()))
        .collect();
    let enum_variants = candidate
        .schema()
        .enums()
        .iter()
        .flat_map(|enumeration| {
            enumeration
                .variants()
                .iter()
                .map(move |variant| ((enumeration.id(), variant.name().to_owned()), variant.id()))
        })
        .collect();
    GenesisSymbols {
        contract_version: candidate.contract_version(),
        entities: BTreeMap::new(),
        events: BTreeMap::new(),
        enums,
        aggregates: BTreeMap::new(),
        commands: BTreeMap::new(),
        projections: BTreeMap::new(),
        entity_fields: BTreeMap::new(),
        event_fields: BTreeMap::new(),
        command_inputs: BTreeMap::new(),
        outcomes: BTreeMap::new(),
        outcome_fields: BTreeMap::new(),
        enum_variants,
        projection_measures: BTreeMap::new(),
        indexes: BTreeMap::new(),
        entity_invariants: BTreeMap::new(),
        aggregate_invariants: BTreeMap::new(),
        renames: Vec::new(),
    }
}

fn semantic_error(code: CompilerDiagnosticCode, span: Span) -> CompilationError {
    CompilationError::Semantic(CompilerDiagnostics::single(CompilerDiagnostic::new(
        code, span,
    )))
}
