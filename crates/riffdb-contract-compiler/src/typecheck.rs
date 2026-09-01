//! Grammar-v1 source-type resolution into IR-owned value types.

use riffdb_contract_ir::{RecordTypeRef, ValueType};
use riffdb_contract_syntax::ast::{Declaration, EntityItem, ServiceValueKind, TypeExpression};
use riffdb_contract_syntax::{ContractDocument, Spanned};
use riffdb_types::{CommandId, EntityTypeId, EventTypeId, FieldId};
use riffdb_types::{CurrencyCode, DecimalSpec};
use std::collections::BTreeMap;

use crate::diagnostic::{CompilerDiagnostic, CompilerDiagnosticCode, CompilerDiagnostics};
use crate::symbols::GenesisSymbols;

/// IR-owned field types indexed by compiler-resolved stable source paths.
#[derive(Clone, Debug)]
pub(crate) struct ResolvedTypes {
    pub(crate) entity_fields: BTreeMap<(EntityTypeId, FieldId), ValueType>,
    pub(crate) event_fields: BTreeMap<(EventTypeId, FieldId), ValueType>,
    pub(crate) command_inputs: BTreeMap<(CommandId, FieldId), ValueType>,
    pub(crate) command_service_values: BTreeMap<(CommandId, FieldId), ValueType>,
    pub(crate) principal_facts: BTreeMap<String, ValueType>,
}

/// Resolves and checks every source-declared value type.
#[cfg(test)]
pub(crate) fn validate_declared_types(
    document: &ContractDocument,
    symbols: &GenesisSymbols,
) -> Result<(), CompilerDiagnostics> {
    resolve_declared_types(document, symbols).map(|_| ())
}

/// Resolves all source declarations into stable-ID-indexed IR-owned types.
pub(crate) fn resolve_declared_types(
    document: &ContractDocument,
    symbols: &GenesisSymbols,
) -> Result<ResolvedTypes, CompilerDiagnostics> {
    let mut diagnostics = Vec::new();
    let mut entity_fields = BTreeMap::new();
    let mut event_fields = BTreeMap::new();
    let mut command_inputs = BTreeMap::new();
    let mut command_service_values = BTreeMap::new();
    let mut principal_facts = BTreeMap::new();
    for declaration in &document.contract.value.declarations {
        match &declaration.value {
            Declaration::Entity(entity) => {
                let entity_id = symbols.entities.get(&entity.name.value).copied();
                for item in &entity.items {
                    match &item.value {
                        EntityItem::Key(key) => {
                            for field in &key.fields {
                                if let (Some(entity_id), Some(value_type), Some(field_id)) = (
                                    entity_id,
                                    resolve_type(&field.value.ty, symbols, &mut diagnostics),
                                    entity_id.and_then(|entity_id| {
                                        symbols
                                            .entity_fields
                                            .get(&(entity_id, field.value.name.value.clone()))
                                            .copied()
                                    }),
                                ) {
                                    entity_fields.insert((entity_id, field_id), value_type);
                                }
                            }
                        }
                        EntityItem::Field(field) => {
                            if let (Some(entity_id), Some(value_type), Some(field_id)) = (
                                entity_id,
                                resolve_type(&field.ty, symbols, &mut diagnostics),
                                entity_id.and_then(|entity_id| {
                                    symbols
                                        .entity_fields
                                        .get(&(entity_id, field.name.value.clone()))
                                        .copied()
                                }),
                            ) {
                                entity_fields.insert((entity_id, field_id), value_type);
                            }
                        }
                        EntityItem::Invariant(_)
                        | EntityItem::Index(_)
                        | EntityItem::Unique(_)
                        | EntityItem::Reference(_)
                        | EntityItem::TextIndex(_)
                        | EntityItem::LongPattern(_)
                        | EntityItem::DeletePolicy(_) => {}
                        EntityItem::VectorField(vector_field) => {
                            // Resolve the vector field to ValueType::vector(dimension).
                            if let (Some(entity_id), Some(field_id)) = (
                                entity_id,
                                entity_id.and_then(|entity_id| {
                                    symbols
                                        .entity_fields
                                        .get(&(entity_id, vector_field.name.value.clone()))
                                        .copied()
                                }),
                            ) && let Ok(dim) = vector_field.dimension.value.parse::<u32>()
                                && let Some(dimension) = riffdb_types::VectorDimension::new(dim)
                            {
                                entity_fields
                                    .insert((entity_id, field_id), ValueType::vector(dimension));
                            }
                        }
                    }
                }
            }
            Declaration::Event(event) => {
                let event_id = symbols.events.get(&event.name.value).copied();
                for field in &event.fields {
                    if let (Some(event_id), Some(value_type), Some(field_id)) = (
                        event_id,
                        resolve_type(&field.value.ty, symbols, &mut diagnostics),
                        event_id.and_then(|event_id| {
                            symbols
                                .event_fields
                                .get(&(event_id, field.value.name.value.clone()))
                                .copied()
                        }),
                    ) {
                        event_fields.insert((event_id, field_id), value_type);
                    }
                }
            }
            Declaration::Command(command) => {
                let command_id = symbols.commands.get(&command.name.value).copied();
                if command.idempotency.is_none() {
                    diagnostics.extend(command.service_values.iter().map(|value| {
                        CompilerDiagnostic::new(
                            CompilerDiagnosticCode::InvalidServiceValue,
                            value.span,
                        )
                    }));
                }
                for input in &command.inputs {
                    if let (Some(command_id), Some(value_type), Some(field_id)) = (
                        command_id,
                        resolve_type(&input.value.field.ty, symbols, &mut diagnostics),
                        command_id.and_then(|command_id| {
                            symbols
                                .command_inputs
                                .get(&(command_id, input.value.field.name.value.clone()))
                                .copied()
                        }),
                    ) {
                        command_inputs.insert((command_id, field_id), value_type);
                    }
                }
                for value in &command.service_values {
                    let value_type = match value.value.kind.value {
                        ServiceValueKind::UuidV7 => ValueType::uuid(),
                        ServiceValueKind::TransactionTime => ValueType::timestamp(),
                    };
                    if let (Some(command_id), Some(field_id)) = (
                        command_id,
                        command_id.and_then(|command_id| {
                            symbols
                                .command_service_values
                                .get(&(command_id, value.value.name.value.clone()))
                                .copied()
                        }),
                    ) {
                        command_service_values.insert((command_id, field_id), value_type);
                    }
                }
            }
            Declaration::PrincipalFact(fact) => {
                if let Some(value_type) = resolve_type(&fact.ty, symbols, &mut diagnostics) {
                    let valid = value_type.is_projection_group_scalar()
                        || value_type.list_parts().is_some_and(|(element, maximum)| {
                            element.is_projection_group_scalar() && maximum <= 64
                        });
                    if valid {
                        principal_facts.insert(fact.name.value.clone(), value_type);
                    } else {
                        diagnostics.push(CompilerDiagnostic::new(
                            CompilerDiagnosticCode::InvalidPrincipalFact,
                            fact.ty.span,
                        ));
                    }
                }
            }
            Declaration::Enum(_)
            | Declaration::Aggregate(_)
            | Declaration::Workflow(_)
            | Declaration::Projection(_)
            | Declaration::RowPolicy(_) => {}
        }
    }
    if diagnostics.is_empty() {
        Ok(ResolvedTypes {
            entity_fields,
            event_fields,
            command_inputs,
            command_service_values,
            principal_facts,
        })
    } else {
        Err(CompilerDiagnostics::new(diagnostics).expect("nonempty diagnostics"))
    }
}

/// Resolves one source type into the checked IR-owned representation.
pub(crate) fn resolve_type(
    source: &Spanned<TypeExpression>,
    symbols: &GenesisSymbols,
    diagnostics: &mut Vec<CompilerDiagnostic>,
) -> Option<ValueType> {
    let resolved = match &source.value {
        TypeExpression::Bool => Some(ValueType::bool()),
        TypeExpression::I64 => Some(ValueType::i64()),
        TypeExpression::U64 => Some(ValueType::u64()),
        TypeExpression::Timestamp => Some(ValueType::timestamp()),
        TypeExpression::Date => Some(ValueType::date()),
        TypeExpression::Uuid => Some(ValueType::uuid()),
        TypeExpression::Decimal { precision, scale } => {
            let precision = precision.value.parse::<u8>().ok();
            let scale = scale.value.parse::<u8>().ok();
            precision
                .zip(scale)
                .and_then(|(precision, scale)| DecimalSpec::new(precision, scale).ok())
                .map(ValueType::decimal)
        }
        TypeExpression::Money { currency } => CurrencyCode::new(&currency.value)
            .ok()
            .map(ValueType::money),
        TypeExpression::String { maximum } => maximum
            .value
            .parse::<usize>()
            .ok()
            .and_then(|maximum| ValueType::string(maximum).ok()),
        TypeExpression::Bytes { maximum } => maximum
            .value
            .parse::<usize>()
            .ok()
            .and_then(|maximum| ValueType::bytes(maximum).ok()),
        TypeExpression::Vector { dimension } => dimension
            .value
            .parse::<u32>()
            .ok()
            .and_then(riffdb_types::VectorDimension::new)
            .map(ValueType::vector),
        TypeExpression::Optional(inner) => resolve_type(inner, symbols, diagnostics)
            .and_then(|inner| ValueType::optional(inner).ok()),
        TypeExpression::List {
            element,
            minimum: _,
            maximum,
        } => {
            let element = resolve_type(element, symbols, diagnostics);
            let maximum = maximum.value.parse::<usize>().ok();
            element
                .zip(maximum)
                .and_then(|(element, maximum)| ValueType::list(element, maximum).ok())
        }
        TypeExpression::Named(name) => symbols
            .enums
            .get(&name.value)
            .copied()
            .map(ValueType::enumeration)
            .or_else(|| {
                symbols
                    .entities
                    .get(&name.value)
                    .copied()
                    .map(|entity| ValueType::record(RecordTypeRef::Entity(entity)))
            }),
    };

    if resolved.is_none() {
        diagnostics.push(CompilerDiagnostic::new(
            CompilerDiagnosticCode::InvalidType,
            source.span,
        ));
    }
    resolved
}

#[cfg(test)]
mod tests {
    use riffdb_contract_syntax::parse_contract;

    use super::*;
    use crate::symbols::allocate_genesis_symbols;

    #[test]
    fn declared_bounds_and_named_enums_resolve() {
        let source = r#"
contract Types version 1 {
  enum State { Open }
  entity Row {
    key (id: uuid)
    field status: State
    field note: optional<string<8>>
    field values: list<decimal<12,2>, 4>
  }
}
"#;
        let document = parse_contract(source).expect("valid syntax");
        let symbols = allocate_genesis_symbols(&document).expect("valid symbols");
        validate_declared_types(&document, &symbols).expect("valid types");
    }

    #[test]
    fn bounded_command_lists_resolve_named_entity_records() {
        let source = r#"
contract Types version 1 {
  entity TupleInput {
    key (id: uuid)
    field relation: string<32>
  }
  bulk command PutTuples {
    input request_id: uuid
    input tuples: list<TupleInput, 1..256>
    idempotency_key request_id
    for tuple in tuples {
      create TupleInput(tuple.id) as row else Duplicate {}
    }
    return Written {}
  }
}
"#;
        let document = parse_contract(source).expect("valid syntax");
        let symbols = allocate_genesis_symbols(&document).expect("valid symbols");
        let types = resolve_declared_types(&document, &symbols).expect("valid types");
        let command = symbols.commands["PutTuples"];
        let field = symbols.command_inputs[&(command, "tuples".to_owned())];
        let (element, maximum) = types.command_inputs[&(command, field)]
            .list_parts()
            .expect("bounded list");
        assert_eq!(maximum, 256);
        assert_eq!(
            element.record_ref(),
            Some(&RecordTypeRef::Entity(symbols.entities["TupleInput"]))
        );
    }

    #[test]
    fn invalid_bounds_nested_optional_and_unknown_named_types_reject() {
        let source = r#"
contract Types version 1 {
  entity Row {
    key (id: uuid)
    field empty: string<0>
    field nested: optional<optional<i64>>
    field mystery: Missing
  }
}
"#;
        let document = parse_contract(source).expect("valid syntax");
        let symbols = allocate_genesis_symbols(&document).expect("valid symbols");
        let diagnostics =
            validate_declared_types(&document, &symbols).expect_err("invalid types reject");
        assert!(
            diagnostics
                .as_slice()
                .iter()
                .all(|diagnostic| diagnostic.code() == CompilerDiagnosticCode::InvalidType)
        );
    }
}
