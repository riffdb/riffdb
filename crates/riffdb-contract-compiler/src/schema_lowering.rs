//! Checked executable schema lowering from compiler-private typed HIR.

use riffdb_contract_ir::{
    AggregateKeyPlan, AggregateSchema, EntitySchema, EnumSchema, EnumVariantSchema, EventSchema,
    FieldSchema, IndexSchema, InvariantPlan, KeyComponentSchema, KeyPurpose, KeySchema,
    RecordSchema, RecordTypeRef, SchemaIr, ValueType,
};
use riffdb_contract_syntax::Span;
use riffdb_types::{EnumTypeId, EnumVariantId};

use crate::diagnostic::{CompilerDiagnostic, CompilerDiagnosticCode, CompilerDiagnostics};
use crate::hir::{HirInvariant, TypedContractHir};

/// Lowers the complete typed HIR schema into checked executable IR.
pub(crate) fn lower_schema(hir: &TypedContractHir) -> Result<SchemaIr, CompilerDiagnostics> {
    let mut diagnostics = Vec::new();
    let enums = lower_enums(hir, &mut diagnostics);
    let entities = lower_entities(hir, &mut diagnostics);
    let events = lower_events(hir, &mut diagnostics);
    let aggregates = lower_aggregates(hir, &mut diagnostics);
    if !diagnostics.is_empty() {
        return Err(CompilerDiagnostics::new(diagnostics).expect("nonempty diagnostics"));
    }
    SchemaIr::new(entities, events, enums, aggregates)
        .map_err(|_| CompilerDiagnostics::single(ir_diagnostic(hir.span)))
}

fn lower_enums(
    hir: &TypedContractHir,
    diagnostics: &mut Vec<CompilerDiagnostic>,
) -> Vec<EnumSchema> {
    hir.enums
        .iter()
        .filter_map(|enumeration| {
            let variants = enumeration
                .variants
                .iter()
                .filter_map(|variant| {
                    EnumVariantSchema::new(variant.id, variant.name.clone())
                        .map_err(|_| ir_diagnostic(variant.span))
                        .map_err(|diagnostic| diagnostics.push(diagnostic))
                        .ok()
                })
                .collect();
            EnumSchema::new(enumeration.id, enumeration.name.clone(), variants)
                .map_err(|_| ir_diagnostic(enumeration.span))
                .map_err(|diagnostic| diagnostics.push(diagnostic))
                .ok()
        })
        .collect()
}

fn lower_entities(
    hir: &TypedContractHir,
    diagnostics: &mut Vec<CompilerDiagnostic>,
) -> Vec<EntitySchema> {
    let mut result = Vec::new();
    for entity in &hir.entities {
        let record_fields = entity
            .fields
            .iter()
            .filter_map(|field| {
                FieldSchema::new(field.id, field.name.clone(), field.value_type.clone())
                    .map_err(|_| ir_diagnostic(field.name_span))
                    .map_err(|diagnostic| diagnostics.push(diagnostic))
                    .ok()
            })
            .collect();
        let Ok(record) = RecordSchema::new(RecordTypeRef::Entity(entity.id), record_fields) else {
            diagnostics.push(ir_diagnostic(entity.span));
            continue;
        };
        let mut key_components = Vec::new();
        for field_id in &entity.key_fields {
            let Some(field) = entity.fields.iter().find(|field| field.id == *field_id) else {
                diagnostics.push(ir_diagnostic(entity.span));
                continue;
            };
            match key_component(field.value_type.clone(), hir) {
                Ok(component) => key_components.push(component),
                Err(code) => diagnostics.push(CompilerDiagnostic::new(code, field.type_span)),
            }
        }
        let Ok(primary_key) = KeySchema::new(KeyPurpose::Entity(entity.id), key_components) else {
            diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::BoundExceeded,
                entity.span,
            ));
            continue;
        };
        let invariants = entity
            .invariants
            .iter()
            .filter_map(|invariant| lower_invariant(invariant, diagnostics))
            .collect();
        let mut indexes = Vec::new();
        for index in &entity.indexes {
            let mut components = Vec::new();
            for (field_id, span) in &index.fields {
                let Some(field) = entity.fields.iter().find(|field| field.id == *field_id) else {
                    diagnostics.push(ir_diagnostic(*span));
                    continue;
                };
                match key_component(field.value_type.clone(), hir) {
                    Ok(component) => components.push(component),
                    Err(code) => diagnostics.push(CompilerDiagnostic::new(code, *span)),
                }
            }
            let key_schema = KeySchema::index(index.id, entity.id, components, primary_key.clone());
            match key_schema.and_then(|key_schema| {
                IndexSchema::new(
                    index.id,
                    index.name.clone(),
                    index.fields.iter().map(|field| field.0).collect(),
                    key_schema,
                )
            }) {
                Ok(schema) => indexes.push(schema),
                Err(_) => diagnostics.push(CompilerDiagnostic::new(
                    CompilerDiagnosticCode::BoundExceeded,
                    index.span,
                )),
            }
        }
        match EntitySchema::new(
            entity.id,
            entity.name.clone(),
            record,
            entity.key_fields.clone(),
            primary_key,
            invariants,
            indexes,
        ) {
            Ok(schema) => result.push(schema),
            Err(_) => diagnostics.push(ir_diagnostic(entity.span)),
        }
    }
    result
}

fn lower_events(
    hir: &TypedContractHir,
    diagnostics: &mut Vec<CompilerDiagnostic>,
) -> Vec<EventSchema> {
    hir.events
        .iter()
        .filter_map(|event| {
            let fields = event
                .fields
                .iter()
                .filter_map(|field| {
                    FieldSchema::new(field.id, field.name.clone(), field.value_type.clone())
                        .map_err(|_| ir_diagnostic(field.name_span))
                        .map_err(|diagnostic| diagnostics.push(diagnostic))
                        .ok()
                })
                .collect();
            RecordSchema::new(RecordTypeRef::Event(event.id), fields)
                .and_then(|record| EventSchema::new(event.id, event.name.clone(), record))
                .map_err(|_| ir_diagnostic(event.span))
                .map_err(|diagnostic| diagnostics.push(diagnostic))
                .ok()
        })
        .collect()
}

fn lower_aggregates(
    hir: &TypedContractHir,
    diagnostics: &mut Vec<CompilerDiagnostic>,
) -> Vec<AggregateSchema> {
    let mut result = Vec::new();
    for aggregate in &hir.aggregates {
        let arena = match aggregate.keys.expressions.to_ir(aggregate.span) {
            Ok(arena) => arena,
            Err(diagnostic) => {
                diagnostics.push(diagnostic);
                continue;
            }
        };
        let partition_component =
            match key_component(aggregate.keys.partition.value_type.clone(), hir) {
                Ok(component) => component,
                Err(code) => {
                    diagnostics.push(CompilerDiagnostic::new(code, aggregate.keys.partition.span));
                    continue;
                }
            };
        let mut conflict_components = Vec::new();
        for conflict in &aggregate.keys.conflicts {
            match key_component(conflict.value_type.clone(), hir) {
                Ok(component) => conflict_components.push(component),
                Err(code) => diagnostics.push(CompilerDiagnostic::new(code, conflict.span)),
            }
        }
        let keys = KeySchema::new(
            KeyPurpose::Partition(aggregate.id),
            vec![partition_component],
        )
        .and_then(|partition_schema| {
            KeySchema::new(KeyPurpose::Conflict(aggregate.id), conflict_components).and_then(
                |conflict_schema| {
                    AggregateKeyPlan::new(
                        arena,
                        aggregate.keys.partition.id,
                        aggregate
                            .keys
                            .conflicts
                            .iter()
                            .map(|root| root.id)
                            .collect(),
                        partition_schema,
                        conflict_schema,
                    )
                },
            )
        });
        let Ok(keys) = keys else {
            diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::BoundExceeded,
                aggregate.span,
            ));
            continue;
        };
        let invariants = aggregate
            .invariants
            .iter()
            .filter_map(|invariant| lower_invariant(invariant, diagnostics))
            .collect();
        match AggregateSchema::new(
            aggregate.id,
            aggregate.name.clone(),
            aggregate.root,
            aggregate.children.iter().map(|child| child.0).collect(),
            keys,
            invariants,
        ) {
            Ok(schema) => result.push(schema),
            Err(_) => diagnostics.push(ir_diagnostic(aggregate.span)),
        }
    }
    result
}

fn lower_invariant(
    invariant: &HirInvariant,
    diagnostics: &mut Vec<CompilerDiagnostic>,
) -> Option<InvariantPlan> {
    let arena = invariant
        .expressions
        .to_ir(invariant.expression.span)
        .map_err(|diagnostic| diagnostics.push(diagnostic))
        .ok()?;
    InvariantPlan::new(
        invariant.id,
        invariant.name.clone(),
        arena,
        invariant.expression.id,
    )
    .map_err(|_| ir_diagnostic(invariant.name_span))
    .map_err(|diagnostic| diagnostics.push(diagnostic))
    .ok()
}

fn key_component(
    value_type: ValueType,
    hir: &TypedContractHir,
) -> Result<KeyComponentSchema, CompilerDiagnosticCode> {
    let variants = match value_type.enum_type_id() {
        Some(enum_id) => enum_variants(enum_id, hir),
        None => Vec::new(),
    };
    KeyComponentSchema::new(value_type, variants).map_err(|error| match error {
        riffdb_contract_ir::IrValidationError::LimitExceeded { .. }
        | riffdb_contract_ir::IrValidationError::SizeOverflow { .. } => {
            CompilerDiagnosticCode::BoundExceeded
        }
        _ => CompilerDiagnosticCode::InvalidType,
    })
}

fn enum_variants(enum_id: EnumTypeId, hir: &TypedContractHir) -> Vec<EnumVariantId> {
    hir.enums
        .iter()
        .find(|enumeration| enumeration.id == enum_id)
        .map(|enumeration| {
            enumeration
                .variants
                .iter()
                .map(|variant| variant.id)
                .collect()
        })
        .unwrap_or_default()
}

fn ir_diagnostic(span: Span) -> CompilerDiagnostic {
    CompilerDiagnostic::new(CompilerDiagnosticCode::InvalidIr, span)
}

#[cfg(test)]
mod tests {
    use riffdb_contract_syntax::parse_contract;

    use super::*;
    use crate::hir::lower_contract_hir;
    use crate::symbols::allocate_genesis_symbols;
    use crate::typecheck::resolve_declared_types;

    fn hir(source: &str) -> TypedContractHir {
        let document = parse_contract(source).expect("syntax");
        let symbols = allocate_genesis_symbols(&document).expect("symbols");
        let types = resolve_declared_types(&document, &symbols).expect("types");
        lower_contract_hir(&document, &symbols, &types).expect("HIR")
    }

    #[test]
    fn canonical_budget_lowers_structural_schema_and_aggregate_keys() {
        let hir = hir(include_str!("../../../contracts/examples/budget.riff"));
        let schema = lower_schema(&hir).expect("schema");
        assert_eq!(schema.entities().len(), 1);
        assert_eq!(schema.events().len(), 1);
        assert_eq!(schema.aggregates().len(), 1);
        assert_eq!(schema.entities()[0].invariants().len(), 2);
        assert_eq!(
            schema.aggregates()[0].keys().conflict_expressions().len(),
            2
        );
    }

    #[test]
    fn decimal_authoritative_key_rejects_at_declared_type_span() {
        let source = r#"
contract Invalid version 1 {
  entity Row { key (amount: decimal<8,2>) }
}
"#;
        let hir = hir(source);
        let diagnostics = lower_schema(&hir).expect_err("rejects");
        let diagnostic = diagnostics
            .as_slice()
            .iter()
            .find(|diagnostic| diagnostic.code() == CompilerDiagnosticCode::InvalidType)
            .expect("invalid type");
        let start = source.find("decimal<8,2>").expect("type start");
        assert_eq!(
            diagnostic.primary_span(),
            Span::new(start, start + 12).expect("span")
        );
    }
}
