//! Exact-parent migration compilation and proof coverage.

use std::collections::{BTreeMap, BTreeSet};

use riffdb_contract_ir::{
    CompatibilityClass, ContractBundle, ContractCandidateV1, EntitySchema, ExpressionArena,
    FieldSchema, MigrationBundleV1, MigrationExpressionV1, MigrationResourceBoundsV1,
    MigrationStepId, MigrationStepKindV1, MigrationStepV1, StableIdNamespaceTag, compare_successor,
};
use riffdb_contract_syntax::ast::Expression;
use riffdb_contract_syntax::{MigrationDeclaration, MigrationDocument, MigrationTransformClause};
use riffdb_contract_syntax::{Span, Spanned, parse_migration};
use riffdb_types::{ContractVersion, EntityTypeId, FieldId, hash_migration_source};

use crate::compiler::CompilationError;
use crate::diagnostic::{CompilerDiagnostic, CompilerDiagnosticCode, CompilerDiagnostics};
use crate::expression_lowering::{ExpressionLowerer, ExpressionScope};
use crate::symbols::GenesisSymbols;

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
    if direct_compatibility
        .entries()
        .iter()
        .any(|entry| entry.class() == CompatibilityClass::Incompatible)
    {
        return Err(semantic_error(
            CompilerDiagnosticCode::UnsupportedMigrationStep,
            document.migration.span,
        ));
    }

    let required_fields = required_field_additions(parent, candidate);
    let symbols = migration_symbols(candidate);
    let mut proofs = BTreeMap::<(EntityTypeId, FieldId), (Span, MigrationStepKindV1)>::new();
    let mut diagnostics = Vec::new();

    for declaration in &document.migration.value.declarations {
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
    }
}

fn semantic_error(code: CompilerDiagnosticCode, span: Span) -> CompilationError {
    CompilationError::Semantic(CompilerDiagnostics::single(CompilerDiagnostic::new(
        code, span,
    )))
}
