//! Deterministic Python application-binding generation.

use std::collections::BTreeSet;
use std::fmt::{self, Write as _};

use crate::infallible_string_write::InfallibleStringWrite as _;

use riffdb_contract_ir::{ContractBundle, RecordTypeRef, ValueType, ValueTypeTag};
use riffdb_contract_syntax::ast::{Declaration, EntityItem, OutcomeExpression};
use riffdb_query_ir::{NamedTypeSchema, ReactiveModulePlanV1, ReactiveOperationPlanV1};
use riffdb_riffql_syntax::{FieldSelection, Selection};
use serde_json::{Value, json};

use crate::QueryModule;
use crate::generation::{
    command_secret_outputs, embedding_command_facades, rust_compact_result_shape,
    vector_inspection_facades, workflow_revision_bindings, workflow_success_outcome_name,
};
use crate::template_generation::{
    generate_canonical_generation_model, render_python_client_header,
};

/// Source symbol responsible for one Python name collision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PythonGenerationError {
    query_name: Option<String>,
    symbol_path: Vec<String>,
}

/// Located Python generation diagnostic in one caller-owned source file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PythonGenerationLocation {
    query_name: Option<String>,
    symbol_path: Vec<String>,
    span: (u32, u32),
}

impl PythonGenerationLocation {
    /// Query source name, or `None` for the contract source.
    #[must_use]
    pub fn query_name(&self) -> Option<&str> {
        self.query_name.as_deref()
    }

    /// Stable symbolic source path.
    #[must_use]
    pub fn symbol_path(&self) -> &[String] {
        &self.symbol_path
    }

    /// Half-open UTF-8 byte span in the selected source.
    #[must_use]
    pub const fn span(&self) -> (u32, u32) {
        self.span
    }
}

impl PythonGenerationError {
    fn contract(symbol_path: Vec<String>) -> Self {
        Self {
            query_name: None,
            symbol_path,
        }
    }

    fn query(query_name: &str, symbol_path: Vec<String>) -> Self {
        Self {
            query_name: Some(query_name.to_owned()),
            symbol_path,
        }
    }

    /// Locates the rejected source symbol using the parser-owned AST spans.
    #[must_use]
    pub fn locate(
        &self,
        contract_source: &str,
        query_sources: &[(&str, &str)],
    ) -> Option<PythonGenerationLocation> {
        let span = match self.query_name.as_deref() {
            Some(query_name) => {
                let source = query_sources
                    .iter()
                    .find_map(|(name, source)| (*name == query_name).then_some(*source))?;
                locate_query_symbol(source, &self.symbol_path)?
            }
            None => locate_contract_symbol(contract_source, &self.symbol_path)?,
        };
        Some(PythonGenerationLocation {
            query_name: self.query_name.clone(),
            symbol_path: self.symbol_path.clone(),
            span,
        })
    }
}

impl fmt::Display for PythonGenerationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("application symbols collide after Python name normalization")
    }
}

impl std::error::Error for PythonGenerationError {}

fn locate_contract_symbol(source: &str, path: &[String]) -> Option<(u32, u32)> {
    let document = riffdb_contract_syntax::parse_contract(source).ok()?;
    let contract = &document.contract.value;
    match path {
        [kind, name] if kind == "contract" && contract.name.value == *name => {
            Some(contract_span(contract.name.span))
        }
        [kind, name] if kind == "enum" => contract.declarations.iter().find_map(|declaration| {
            let Declaration::Enum(enumeration) = &declaration.value else {
                return None;
            };
            (enumeration.name.value == *name).then(|| contract_span(enumeration.name.span))
        }),
        [kind, enum_name, variant_kind, variant] if kind == "enum" && variant_kind == "variant" => {
            contract.declarations.iter().find_map(|declaration| {
                let Declaration::Enum(enumeration) = &declaration.value else {
                    return None;
                };
                (enumeration.name.value == *enum_name)
                    .then(|| {
                        enumeration
                            .variants
                            .iter()
                            .find(|candidate| candidate.value == *variant)
                            .map(|candidate| contract_span(candidate.span))
                    })
                    .flatten()
            })
        }
        [kind, entity_name] if kind == "entity" => {
            contract.declarations.iter().find_map(|declaration| {
                let Declaration::Entity(entity) = &declaration.value else {
                    return None;
                };
                (entity.name.value == *entity_name).then(|| contract_span(entity.name.span))
            })
        }
        [kind, entity_name, field_kind, field] if kind == "entity" && field_kind == "field" => {
            contract.declarations.iter().find_map(|declaration| {
                let Declaration::Entity(entity) = &declaration.value else {
                    return None;
                };
                if entity.name.value != *entity_name {
                    return None;
                }
                entity.items.iter().find_map(|item| match &item.value {
                    EntityItem::Key(key) => key
                        .fields
                        .iter()
                        .find(|candidate| candidate.value.name.value == *field)
                        .map(|candidate| contract_span(candidate.value.name.span)),
                    EntityItem::Field(candidate) if candidate.name.value == *field => {
                        Some(contract_span(candidate.name.span))
                    }
                    EntityItem::Field(_)
                    | EntityItem::DeletePolicy(_)
                    | EntityItem::Invariant(_)
                    | EntityItem::Index(_)
                    | EntityItem::Unique(_)
                    | EntityItem::Reference(_)
                    | EntityItem::TextIndex(_)
                    | EntityItem::LongPattern(_)
                    | EntityItem::VectorField(_) => None,
                })
            })
        }
        [kind, command_name] if kind == "command" => {
            contract.declarations.iter().find_map(|declaration| {
                let Declaration::Command(command) = &declaration.value else {
                    return None;
                };
                (command.name.value == *command_name).then(|| contract_span(command.name.span))
            })
        }
        [kind, command_name, input_kind, input] if kind == "command" && input_kind == "input" => {
            contract.declarations.iter().find_map(|declaration| {
                let Declaration::Command(command) = &declaration.value else {
                    return None;
                };
                (command.name.value == *command_name)
                    .then(|| {
                        command
                            .inputs
                            .iter()
                            .find(|candidate| candidate.value.field.name.value == *input)
                            .map(|candidate| contract_span(candidate.value.field.name.span))
                    })
                    .flatten()
            })
        }
        [kind, command_name, outcome_kind, outcome]
            if kind == "command" && outcome_kind == "outcome" =>
        {
            locate_contract_outcome(contract, command_name, outcome)
                .map(|candidate| contract_span(candidate.name.span))
        }
        [kind, command_name, outcome_kind, outcome, field_kind, field]
            if kind == "command" && outcome_kind == "outcome" && field_kind == "field" =>
        {
            locate_contract_outcome(contract, command_name, outcome).and_then(|candidate| {
                candidate
                    .payload
                    .value
                    .fields
                    .iter()
                    .find(|candidate| candidate.value.name.value == *field)
                    .map(|candidate| contract_span(candidate.value.name.span))
            })
        }
        _ => None,
    }
}

fn locate_contract_outcome<'a>(
    contract: &'a riffdb_contract_syntax::ast::Contract,
    command_name: &str,
    outcome_name: &str,
) -> Option<&'a OutcomeExpression> {
    let command = contract.declarations.iter().find_map(|declaration| {
        let Declaration::Command(command) = &declaration.value else {
            return None;
        };
        (command.name.value == command_name).then_some(command)
    })?;
    command
        .bindings
        .iter()
        .filter_map(|binding| binding.value.failure().map(|failure| &failure.value))
        .chain(
            command
                .requirements
                .iter()
                .map(|requirement| &requirement.value.rejection.value),
        )
        .chain(std::iter::once(&command.return_clause.value.outcome.value))
        .find(|outcome| outcome.name.value == outcome_name)
}

fn contract_span(span: riffdb_contract_syntax::Span) -> (u32, u32) {
    (span.start(), span.end())
}

fn locate_query_symbol(source: &str, path: &[String]) -> Option<(u32, u32)> {
    let document = riffdb_riffql_syntax::parse_query(source).ok()?;
    let [kind, query_name, rest @ ..] = path else {
        return None;
    };
    if kind != "query"
        || document
            .name
            .as_ref()
            .is_some_and(|name| name.value.as_str() != query_name)
    {
        return None;
    }
    match rest {
        [] => document.name.map(|name| query_span(name.span)),
        [parameter_kind, parameter, ..] if parameter_kind == "parameter" => document
            .parameters
            .iter()
            .find(|candidate| candidate.name.value.as_str() == parameter)
            .map(|candidate| query_span(candidate.name.span)),
        [outcome_kind, outcome] if outcome_kind == "outcome" => document
            .body
            .outcomes
            .iter()
            .chain(document.body.outcome.iter())
            .find(|candidate| candidate.value.as_str() == outcome)
            .map(|candidate| query_span(candidate.span)),
        [field_kind, fields @ ..] if field_kind == "field" && !fields.is_empty() => {
            locate_query_field(&document.body.selection, fields)
        }
        _ => None,
    }
}

fn locate_query_field(selection: &Selection, path: &[String]) -> Option<(u32, u32)> {
    let (name, remaining) = path.split_first()?;
    let field = selection
        .fields
        .iter()
        .find(|field| query_field_name(field) == name)?;
    if remaining.is_empty() {
        return Some(query_field_span(field));
    }
    field
        .nested
        .as_ref()
        .and_then(|nested| locate_query_field(nested, remaining))
        .or_else(|| Some(query_field_span(field)))
}

fn query_field_name(field: &FieldSelection) -> &str {
    field.alias.as_ref().map_or_else(
        || {
            field
                .source
                .value
                .0
                .last()
                .expect("the parser only produces nonempty paths")
                .value
                .as_str()
        },
        |alias| alias.value.as_str(),
    )
}

fn query_field_span(field: &FieldSelection) -> (u32, u32) {
    field.alias.as_ref().map_or_else(
        || {
            query_span(
                field
                    .source
                    .value
                    .0
                    .last()
                    .expect("the parser only produces nonempty paths")
                    .span,
            )
        },
        |alias| query_span(alias.span),
    )
}

const fn query_span(span: riffdb_riffql_syntax::Span) -> (u32, u32) {
    (span.start, span.end)
}

pub(crate) fn python_generation_model(module: &QueryModule, contract: &ContractBundle) -> Value {
    let queries = module
        .queries()
        .iter()
        .map(|query| {
            let wire_name = query.name();
            let name = pascal(wire_name);
            let schemas = query.plan().schemas();
            let redacted = !query.plan().secret_outputs().is_empty();
            let parameter_nested = schemas
                .parameters()
                .iter()
                .flat_map(|parameter| {
                    python_named_nested_models(
                        &format!("{name}Params{}", pascal(parameter.name())),
                        parameter.value_type(),
                        contract,
                        false,
                    )
                })
                .collect::<Vec<_>>();
            let compact = query.plan().common_covered_result().and_then(
                |(result_name, layout, selected_fields)| {
                    rust_compact_result_shape(
                        schemas,
                        &result_name,
                        &layout,
                        &selected_fields,
                        contract,
                    )
                },
            ).map(|shape| {
                let nested = format!(
                    "{name}{}{}",
                    pascal(&shape.outcome),
                    pascal(&shape.result_name)
                );
                json!({
                    "method": python_identifier(&snake(&name)),
                    "outcome": format!("{:?}", shape.outcome),
                    "result_name": format!("{:?}", shape.result_name),
                    "entity": format!("{:?}", shape.entity),
                    "fields": shape.fields.iter().map(|field| format!("{:?}", field.name)).collect::<Vec<_>>(),
                    "maximum_rows": shape.maximum_rows,
                    "width": shape.fields.len(),
                    "nested": nested,
                    "branch": format!("{name}{}", pascal(&shape.outcome)),
                    "result_field": python_identifier(&shape.result_name),
                    "decodes": shape.fields.iter().enumerate().map(|(index, field)| json!({
                        "field": python_identifier(&field.name),
                        "expression": python_decode_compact_expr(&format!("row[{index}]"), &field.value_type, contract),
                    })).collect::<Vec<_>>(),
                })
            });
            let cursor = schemas.parameters().iter().find(|parameter| is_cursor(parameter.value_type())).map(|parameter| json!({
                "field": python_identifier(parameter.name()),
                "wire_name": format!("{:?}", parameter.name()),
            }));
            json!({
                "name": name,
                "wire_name": format!("{:?}", wire_name),
                "method": python_identifier(&snake(wire_name)),
                "constant": screaming_snake(wire_name),
                "plan_hash": hex(query.plan().identity().as_bytes()),
                "secret_outputs": query.plan().secret_outputs().iter().map(|secret| json!({
                    "query": format!("{:?}", wire_name),
                    "entity": format!("{:?}", secret.entity()),
                    "field": format!("{:?}", secret.field()),
                })).collect::<Vec<_>>(),
                "parameter_nested": parameter_nested,
                "parameters": schemas.parameters().iter().map(|parameter| {
                    let nested = format!("{name}Params{}", pascal(parameter.name()));
                    let defaulted = parameter.has_default()
                        || matches!(parameter.value_type(), NamedTypeSchema::Optional(_));
                    let mut value_type = python_named_type(parameter.value_type(), &nested, contract);
                    if defaulted && !value_type.ends_with(" | None") {
                        value_type.push_str(" | None");
                    }
                    json!({
                        "name": python_identifier(parameter.name()),
                        "type": value_type,
                        "defaulted": defaulted,
                    })
                }).collect::<Vec<_>>(),
                "results": schemas.results().iter().map(|branch| {
                    let branch_name = format!("{name}{}", pascal(branch.name()));
                    let nested = branch.fields().iter().flat_map(|field| {
                        python_named_nested_models(
                            &format!("{branch_name}{}", pascal(field.name())),
                            field.value_type(),
                            contract,
                            redacted,
                        )
                    }).collect::<Vec<_>>();
                    json!({
                        "name": branch_name,
                        "wire_name": format!("{:?}", branch.name()),
                        "nested": nested,
                        "fields": branch.fields().iter().map(|field| json!({
                            "name": python_identifier(field.name()),
                            "type": python_named_type(
                                field.value_type(),
                                &format!("{branch_name}{}", pascal(field.name())),
                                contract,
                            ),
                            "redacted": redacted,
                        })).collect::<Vec<_>>(),
                    })
                }).collect::<Vec<_>>(),
                "limit_checks": schemas.parameters().iter().filter_map(|parameter| {
                    let NamedTypeSchema::BoundedLimit { maximum } = parameter.value_type() else {
                        return None;
                    };
                    Some(json!({
                        "field": python_identifier(parameter.name()),
                        "maximum": maximum,
                        "message": format!("{:?}", format!("{} must be an integer from 1 through {maximum}", parameter.name())),
                    }))
                }).collect::<Vec<_>>(),
                "cursor": cursor,
                "compact": compact,
            })
        })
        .collect::<Vec<_>>();

    let mut commands = contract
        .commands()
        .iter()
        .filter(|command| !command.is_reimport())
        .collect::<Vec<_>>();
    commands.sort_by(|left, right| left.name().cmp(right.name()));
    let commands = commands
        .into_iter()
        .map(|command| {
            let wire_name = command.name();
            let name = pascal(wire_name);
            let secret_outputs = command_secret_outputs(command, contract);
            let workflow_revisions = workflow_revision_bindings(command);
            let collection = command.collection_expansion().map(|expansion| {
                let field = command
                    .input()
                    .record()
                    .field(expansion.input_field())
                    .expect("validated collection input field");
                let mut collection = json!({
                    "field": python_identifier(field.name()),
                    "minimum": expansion.minimum_elements(),
                    "maximum": expansion.maximum_elements(),
                    "message": format!("{:?}", format!("invalid bounded collection length for {}.{}", command.name(), field.name())),
                    "aggregate": expansion.maximum_aggregate_element_bytes().map(|maximum| json!({
                        "maximum": maximum,
                        "type": python_contract_type(expansion.element_type(), contract),
                        "message": format!("{:?}", format!("invalid aggregate collection bytes for {}.{}", command.name(), field.name())),
                    })),
                });
                if expansion.maximum_aggregate_element_bytes().is_some() {
                    let object = collection
                        .as_object_mut()
                        .expect("collection generation model object");
                    object.insert(
                        "wire_field".to_owned(),
                        Value::String(field.name().to_owned()),
                    );
                    object.insert(
                        "individual_checks".to_owned(),
                        json!(python_collection_individual_checks(
                            expansion.element_type(),
                            field.name(),
                            contract,
                        )),
                    );
                }
                collection
            });
            let facades = embedding_command_facades(command, contract).into_iter().map(|facade| {
                let field_constant = screaming_snake(&facade.vector_field_name);
                let prefix = screaming_snake(command.name());
                json!({
                    "identity_constant": format!("{prefix}_{field_constant}_MODEL_IDENTITY"),
                    "version_constant": format!("{prefix}_{field_constant}_MODEL_VERSION"),
                    "model_identity": format!("{:?}", facade.model_identity),
                    "model_version": format!("{:?}", facade.model_version),
                    "function": python_identifier(&format!("{}_for_{}", command.name(), facade.vector_field_name)),
                    "arguments": command.input().record().fields().iter().filter(|field| {
                        field.name() != facade.model_input_name && field.name() != facade.version_input_name
                    }).map(|field| json!({
                        "name": python_identifier(field.name()),
                        "type": python_contract_type(field.value_type(), contract),
                    })).collect::<Vec<_>>(),
                    "assignments": command.input().record().fields().iter().map(|field| {
                        let field_name = python_identifier(field.name());
                        let value = if field.name() == facade.model_input_name {
                            format!("{prefix}_{field_constant}_MODEL_IDENTITY")
                        } else if field.name() == facade.version_input_name {
                            format!("{prefix}_{field_constant}_MODEL_VERSION")
                        } else {
                            field_name.clone()
                        };
                        json!({"field": field_name, "value": value})
                    }).collect::<Vec<_>>(),
                    "model_input": python_identifier(&facade.model_input_name),
                    "version_input": python_identifier(&facade.version_input_name),
                })
            }).collect::<Vec<_>>();
            json!({
                "name": name,
                "wire_name": format!("{:?}", wire_name),
                "method": python_identifier(&snake(wire_name)),
                "constant": screaming_snake(wire_name),
                "plan_hash": hex(command.plan_hash().as_bytes()),
                "secret_outputs": secret_outputs.iter().map(|secret| json!({
                    "outcome": format!("{:?}", secret.outcome),
                    "field": format!("{:?}", secret.field),
                    "entity": format!("{:?}", secret.entity),
                    "source_field": format!("{:?}", secret.source_field),
                })).collect::<Vec<_>>(),
                "inputs": command.input().record().fields().iter().map(|field| json!({
                    "name": python_identifier(field.name()),
                    "type": python_contract_type(field.value_type(), contract),
                })).collect::<Vec<_>>(),
                "facades": facades,
                "outcomes": command.outcomes().iter().map(|outcome| json!({
                    "name": format!("{name}{}", pascal(outcome.name())),
                    "wire_name": format!("{:?}", outcome.name()),
                    "fields": outcome.payload().fields().iter().map(|field| json!({
                        "name": python_identifier(field.name()),
                        "type": python_contract_type(field.value_type(), contract),
                    })).collect::<Vec<_>>(),
                    "redacted": secret_outputs.iter().any(|secret| secret.outcome == outcome.name()),
                })).collect::<Vec<_>>(),
                "collection": collection,
                "workflow": {
                    "success": format!("{name}{}", pascal(workflow_success_outcome_name(command))),
                    "revisions": workflow_revisions.iter().map(|revision| json!({
                        "binding": format!("{:?}", revision.binding_name),
                        "input": python_identifier(revision.input_name),
                    })).collect::<Vec<_>>(),
                },
            })
        })
        .collect::<Vec<_>>();

    let vector_inspections = vector_inspection_facades(module, contract)
        .into_iter()
        .flat_map(|inspection| {
            let function = format!(
                "inspect_{}_{}",
                snake(&inspection.entity),
                snake(&inspection.field)
            );
            let partition_type = python_contract_type(&inspection.partition_type, contract);
            [
                ("staleness", "staleness", "VectorStalenessResult"),
                (
                    "model_versions",
                    "model_versions",
                    "VectorModelVersionResult",
                ),
            ]
            .into_iter()
            .map(move |(suffix, kind, result_type)| {
                json!({
                    "function": function,
                    "suffix": suffix,
                    "kind": format!("{:?}", kind),
                    "result_type": result_type,
                    "partition_type": partition_type,
                    "entity": format!("{:?}", inspection.entity),
                    "field": format!("{:?}", inspection.field),
                })
            })
        })
        .collect::<Vec<_>>();

    json!({
        "class_name": pascal(module.contract_lineage().as_str()),
        "enums": contract.schema().enums().iter().map(|enumeration| json!({
            "name": pascal(enumeration.name()),
            "variants": enumeration.variants().iter().map(|variant| json!({
                "name": screaming_snake(variant.name()),
                "literal": format!("{:?}", variant.name()),
            })).collect::<Vec<_>>(),
        })).collect::<Vec<_>>(),
        "entities": contract.schema().entities().iter().map(|entity| json!({
            "name": pascal(entity.name()),
            "fields": entity.record().fields().iter().map(|field| json!({
                "name": python_identifier(field.name()),
                "type": python_contract_type(field.value_type(), contract),
            })).collect::<Vec<_>>(),
        })).collect::<Vec<_>>(),
        "queries": queries,
        "commands": commands,
        "vector_inspections": vector_inspections,
    })
}

fn python_collection_individual_checks(
    element_type: &ValueType,
    collection: &str,
    contract: &ContractBundle,
) -> Vec<Value> {
    let Some(RecordTypeRef::Entity(entity_id)) = element_type.record_ref() else {
        return Vec::new();
    };
    contract
        .schema()
        .entity(*entity_id)
        .expect("validated collection element entity")
        .record()
        .fields()
        .iter()
        .filter_map(|field| {
            let (value_type, optional) = match field.value_type().optional_inner() {
                Some(inner) => (inner, true),
                None => (field.value_type(), false),
            };
            if !matches!(value_type.tag(), ValueTypeTag::String | ValueTypeTag::Bytes) {
                return None;
            }
            let maximum = value_type.byte_bound().expect("bounded text or bytes");
            let member = python_identifier(field.name());
            let measured = if value_type.tag() == ValueTypeTag::String {
                format!("item.{member}.encode(\"utf-8\")")
            } else {
                format!("item.{member}")
            };
            let condition = if optional {
                format!("item.{member} is not None and len({measured}) > {maximum}")
            } else {
                format!("len({measured}) > {maximum}")
            };
            Some(json!({
                "condition": condition,
                "collection": collection,
                "leaf": field.name(),
            }))
        })
        .collect()
}

pub(crate) fn python_reactive_generation_model(
    module: &QueryModule,
    contract: &ContractBundle,
    reactive_modules: &[ReactiveModulePlanV1],
) -> Vec<Value> {
    reactive_modules
        .iter()
        .map(|reactive| {
            let operations = reactive.operations().iter().map(|operation| {
                let name = pascal(operation.name().as_str());
                let method = python_identifier(&snake(operation.name().as_str()));
                let parameters = match operation.plan() {
                    ReactiveOperationPlanV1::Stream { parameters, .. }
                    | ReactiveOperationPlanV1::Watch { parameters, .. }
                    | ReactiveOperationPlanV1::Subscription { parameters, .. } => parameters,
                };
                let schema = parameters.iter().map(|parameter| {
                    (
                        parameter.name().to_owned(),
                        python_reactive_schema(parameter.type_name(), contract),
                    )
                }).collect::<serde_json::Map<_, _>>();
                let parameters = json!({
                    "fields": parameters.iter().map(|parameter| json!({
                        "name": python_identifier(parameter.name()),
                        "type": python_reactive_type(parameter.type_name(), contract),
                    })).collect::<Vec<_>>(),
                    "schema": serde_json::to_string(&schema).expect("reactive schema JSON"),
                });
                match operation.plan() {
                    ReactiveOperationPlanV1::Stream { events, .. } => json!({
                        "kind": "stream",
                        "name": name,
                        "method": method,
                        "wire_name": format!("{:?}", operation.name().as_str()),
                        "parameters": parameters,
                        "events": events.iter().map(|event| json!({
                            "name": format!("{name}{}", pascal(event.name())),
                            "wire_name": format!("{:?}", event.name()),
                            "fields": event.fields().iter().map(|field| json!({
                                "name": python_identifier(field.name()),
                                "type": python_reactive_type(field.type_name(), contract),
                            })).collect::<Vec<_>>(),
                        })).collect::<Vec<_>>(),
                    }),
                    ReactiveOperationPlanV1::Watch { query, .. } => {
                        let query_module = module.query(query.query_name())
                            .expect("reactive compiler retained exact query dependency");
                        json!({
                            "kind": "watch",
                            "name": name,
                            "method": method,
                            "wire_name": format!("{:?}", operation.name().as_str()),
                            "parameters": parameters,
                            "query": query.query_name(),
                            "outcomes": query_module.plan().schemas().results().iter().map(|branch| json!({
                                "wire_name": format!("{:?}", branch.name()),
                                "class": format!("{}{}", pascal(query.query_name()), pascal(branch.name())),
                            })).collect::<Vec<_>>(),
                        })
                    }
                    ReactiveOperationPlanV1::Subscription { stream_name, reactions, .. } => {
                        let stream_operation = reactive.operations().iter()
                            .find(|candidate| candidate.name() == stream_name)
                            .expect("reactive compiler retained exact stream dependency");
                        let ReactiveOperationPlanV1::Stream { events, .. } = stream_operation.plan() else {
                            unreachable!("subscription stream dependency is a stream");
                        };
                        json!({
                            "kind": "subscription",
                            "name": name,
                            "method": method,
                            "wire_name": format!("{:?}", operation.name().as_str()),
                            "parameters": parameters,
                            "stream": pascal(stream_name.as_str()),
                            "events": events.iter().map(|event| json!({
                                "wire_name": format!("{:?}", event.name()),
                                "class": format!("{}{}", pascal(stream_name.as_str()), pascal(event.name())),
                            })).collect::<Vec<_>>(),
                            "reactions": reactions.iter().map(|reaction| {
                                let command = contract.commands().iter()
                                    .find(|command| command.name() == reaction.command_name())
                                    .expect("reactive compiler retained exact command dependency");
                                let command_name = pascal(command.name());
                                let revisions = workflow_revision_bindings(command);
                                json!({
                                    "method": python_identifier(&snake(reaction.reaction_name())),
                                    "reaction_name": format!("{:?}", reaction.reaction_name()),
                                    "command_wire_name": format!("{:?}", command.name()),
                                    "command_name": command_name,
                                    "plan_constant": screaming_snake(command.name()),
                                    "outcomes": command.outcomes().iter().map(|outcome| json!({
                                        "wire_name": format!("{:?}", outcome.name()),
                                        "class": format!("{command_name}{}", pascal(outcome.name())),
                                    })).collect::<Vec<_>>(),
                                    "workflow": {
                                        "success": format!("{command_name}{}", pascal(workflow_success_outcome_name(command))),
                                        "revisions": revisions.iter().map(|revision| json!({
                                            "binding": format!("{:?}", revision.binding_name),
                                            "input": python_identifier(revision.input_name),
                                        })).collect::<Vec<_>>(),
                                    },
                                })
                            }).collect::<Vec<_>>(),
                        })
                    }
                }
            }).collect::<Vec<_>>();
            json!({
                "constant": screaming_snake(reactive.name()),
                "hash": hex(reactive.identity().as_bytes()),
                "client_base": pascal(module.contract_lineage().as_str()),
                "operations": operations,
            })
        })
        .collect()
}

fn python_named_nested_models(
    name: &str,
    value_type: &NamedTypeSchema,
    contract: &ContractBundle,
    redacted: bool,
) -> Vec<Value> {
    match value_type {
        NamedTypeSchema::Optional(inner)
        | NamedTypeSchema::Set(inner)
        | NamedTypeSchema::BoundedSet { element: inner, .. }
        | NamedTypeSchema::List { element: inner, .. } => {
            python_named_nested_models(name, inner, contract, redacted)
        }
        NamedTypeSchema::Record(fields) => {
            let mut models = fields
                .iter()
                .flat_map(|field| {
                    python_named_nested_models(
                        &format!("{name}{}", pascal(field.name())),
                        field.value_type(),
                        contract,
                        redacted,
                    )
                })
                .collect::<Vec<_>>();
            models.push(json!({
                "name": name,
                "fields": fields.iter().map(|field| json!({
                    "name": python_identifier(field.name()),
                    "type": python_named_type(
                        field.value_type(),
                        &format!("{name}{}", pascal(field.name())),
                        contract,
                    ),
                    "redacted": redacted,
                })).collect::<Vec<_>>(),
            }));
            models
        }
        NamedTypeSchema::Scalar(_)
        | NamedTypeSchema::Cursor
        | NamedTypeSchema::Limit
        | NamedTypeSchema::BoundedLimit { .. } => Vec::new(),
    }
}

/// Generates one complete, identity-pinned Python application module.
pub fn generate_python_client(
    module: &QueryModule,
    contract: &ContractBundle,
) -> Result<String, PythonGenerationError> {
    validate_names(module, contract)?;
    let model = generate_canonical_generation_model(module, contract, &[]);
    let mut output = render_python_client_header(&model).expect("static Python template and model");
    while output.ends_with("\n\n") {
        output.pop();
    }
    Ok(output)
}

/// Generates one complete Python application module including native-backed
/// async reactive iterators. The presentation layer never imports gRPC.
pub fn generate_python_application_client(
    module: &QueryModule,
    contract: &ContractBundle,
    reactive_modules: &[ReactiveModulePlanV1],
) -> Result<String, PythonGenerationError> {
    validate_names(module, contract)?;
    let model = generate_canonical_generation_model(module, contract, reactive_modules);
    let mut output = render_python_client_header(&model).expect("static Python template and model");
    while output.ends_with("\n\n") {
        output.pop();
    }
    Ok(output)
}

// Python type-model helpers used while assembling the canonical model.

fn python_reactive_schema(type_name: &str, contract: &ContractBundle) -> serde_json::Value {
    if let Some(enumeration) = contract
        .schema()
        .enums()
        .iter()
        .find(|value| value.name() == type_name)
    {
        return serde_json::json!({
            "kind": "enum",
            "type_id": enumeration.id().get(),
            "variants": enumeration.variants().iter().map(|variant| {
                (variant.name().to_owned(), serde_json::json!(variant.id().get()))
            }).collect::<serde_json::Map<_, _>>(),
        });
    }
    if let Some((precision, scale)) = decimal_type_parts(type_name) {
        return serde_json::json!({"kind":"decimal", "precision":precision, "scale":scale});
    }
    if let Some(currency) = money_type_currency(type_name) {
        return serde_json::json!({"kind":"money", "precision":38, "scale":2, "currency":currency});
    }
    if let Some(dimension) = vector_type_dimension(type_name) {
        return serde_json::json!({"kind":"vector", "dimension":dimension});
    }
    let kind = if type_name.starts_with("bytes<") {
        "bytes"
    } else if type_name.starts_with("string<") || type_name == "cursor" {
        "string"
    } else if type_name == "limit" {
        "u64"
    } else {
        type_name
    };
    serde_json::json!({"kind":kind})
}

fn decimal_type_parts(type_name: &str) -> Option<(u8, u8)> {
    let body = type_name.strip_prefix("decimal<")?.strip_suffix('>')?;
    let (precision, scale) = body.split_once(',')?;
    Some((precision.parse().ok()?, scale.parse().ok()?))
}

fn money_type_currency(type_name: &str) -> Option<String> {
    type_name
        .strip_prefix("money<")?
        .strip_suffix('>')
        .map(str::to_owned)
}

fn vector_type_dimension(type_name: &str) -> Option<u32> {
    type_name
        .strip_prefix("vector<")?
        .strip_suffix('>')?
        .parse()
        .ok()
}

fn python_reactive_type(type_name: &str, contract: &ContractBundle) -> String {
    if contract
        .schema()
        .enums()
        .iter()
        .any(|value| value.name() == type_name)
    {
        return pascal(type_name);
    }
    if type_name.starts_with("decimal<") {
        return "Decimal".to_owned();
    }
    if type_name.starts_with("money<") {
        return "Money".to_owned();
    }
    if type_name.starts_with("bytes<") {
        return "bytes".to_owned();
    }
    if let Some(dimension) = vector_type_dimension(type_name) {
        return format!("Annotated[tuple[float, ...], \"vector<{dimension}>\"]");
    }
    if type_name.starts_with("string<") {
        return "str".to_owned();
    }
    match type_name {
        "bool" => "bool",
        "i64" | "u64" => "int",
        "uuid" => "UUID",
        "date" => "RiffDate",
        "timestamp" => "Timestamp",
        "limit" => "int",
        "cursor" => "str",
        _ => "str",
    }
    .to_owned()
}

fn python_decode_compact_expr(value: &str, ty: &ValueType, contract: &ContractBundle) -> String {
    if let Some(inner) = ty.optional_inner() {
        return format!(
            "None if {value} is None else {}",
            python_decode_compact_expr(value, inner, contract)
        );
    }
    match ty.tag() {
        ValueTypeTag::Bool => format!(
            "({value} if type({value}) is bool else (_ for _ in ()).throw(ValueError(\"invalid RiffDB compact bool\")))"
        ),
        ValueTypeTag::I64 => format!(
            "({value} if type({value}) is int and -(2**63) <= {value} < 2**63 else (_ for _ in ()).throw(ValueError(\"invalid RiffDB compact i64\")))"
        ),
        ValueTypeTag::U64 => format!(
            "({value} if type({value}) is int and 0 <= {value} < 2**64 else (_ for _ in ()).throw(ValueError(\"invalid RiffDB compact u64\")))"
        ),
        ValueTypeTag::String => format!(
            "({value} if isinstance({value}, str) and len({value}.encode(\"utf-8\")) <= {} else (_ for _ in ()).throw(ValueError(\"invalid RiffDB compact string\")))",
            ty.byte_bound().expect("string bound")
        ),
        ValueTypeTag::Uuid => format!(
            "UUID(str(_compact_tag({value}, \"uuid\", frozenset((\"$riffdb\", \"value\")))[\"value\"]))"
        ),
        ValueTypeTag::Date => format!(
            "RiffDate(int(_compact_tag({value}, \"date\", frozenset((\"$riffdb\", \"value\")))[\"value\"]))"
        ),
        ValueTypeTag::Timestamp => format!(
            "Timestamp(seconds=int(_compact_tag({value}, \"timestamp\", frozenset((\"$riffdb\", \"seconds\", \"nanos\")))[\"seconds\"]), nanos=int(_compact_tag({value}, \"timestamp\", frozenset((\"$riffdb\", \"seconds\", \"nanos\")))[\"nanos\"]))"
        ),
        ValueTypeTag::Enum => {
            let enumeration = contract
                .schema()
                .enumeration(ty.enum_type_id().expect("enum identity"))
                .expect("validated enum");
            format!(
                "{}(str(_compact_tag({value}, \"enum\", frozenset((\"$riffdb\", \"value\")))[\"value\"]))",
                pascal(enumeration.name())
            )
        }
        ValueTypeTag::Optional => unreachable!("handled above"),
        _ => "(_ for _ in ()).throw(ValueError(\"unsupported RiffDB compact value\"))".to_owned(),
    }
}

#[allow(dead_code)]
fn python_named_type(
    value_type: &NamedTypeSchema,
    nested_name: &str,
    contract: &ContractBundle,
) -> String {
    match value_type {
        NamedTypeSchema::Scalar(name) => match name.as_str() {
            "bool" => "bool".to_owned(),
            "i64" => "Annotated[int, \"i64\"]".to_owned(),
            "u64" => "Annotated[int, \"u64\"]".to_owned(),
            "uuid" => "UUID".to_owned(),
            "timestamp" => "Timestamp".to_owned(),
            "date" => "RiffDate".to_owned(),
            value if value.starts_with("bytes<") => "bytes".to_owned(),
            value if value.starts_with("vector<") => format!(
                "Annotated[tuple[float, ...], \"vector<{}>\"]",
                vector_type_dimension(value).expect("vector dimension")
            ),
            value if value.starts_with("decimal<") => "Decimal".to_owned(),
            value if value.starts_with("money<") => "Money".to_owned(),
            value
                if contract
                    .schema()
                    .enums()
                    .iter()
                    .any(|enumeration| enumeration.name() == value) =>
            {
                pascal(value)
            }
            _ => "str".to_owned(),
        },
        NamedTypeSchema::Optional(inner) => {
            format!("{} | None", python_named_type(inner, nested_name, contract))
        }
        NamedTypeSchema::Set(inner)
        | NamedTypeSchema::BoundedSet { element: inner, .. }
        | NamedTypeSchema::List { element: inner, .. } => {
            format!(
                "tuple[{}, ...]",
                python_named_type(inner, nested_name, contract)
            )
        }
        NamedTypeSchema::Record(_) => nested_name.to_owned(),
        NamedTypeSchema::Cursor => "str".to_owned(),
        NamedTypeSchema::Limit => "Annotated[int, \"u64\"]".to_owned(),
        NamedTypeSchema::BoundedLimit { .. } => "Annotated[int, \"u64\"]".to_owned(),
    }
}

pub(crate) fn python_contract_type(value_type: &ValueType, contract: &ContractBundle) -> String {
    if let Some(inner) = value_type.optional_inner() {
        return format!("{} | None", python_contract_type(inner, contract));
    }
    if let Some((inner, _)) = value_type.list_parts() {
        return format!("tuple[{}, ...]", python_contract_type(inner, contract));
    }
    match value_type.tag() {
        ValueTypeTag::Bool => "bool".to_owned(),
        ValueTypeTag::I64 => "Annotated[int, \"i64\"]".to_owned(),
        ValueTypeTag::U64 => "Annotated[int, \"u64\"]".to_owned(),
        ValueTypeTag::Bytes => "bytes".to_owned(),
        ValueTypeTag::Decimal => "Decimal".to_owned(),
        ValueTypeTag::Money => "Money".to_owned(),
        ValueTypeTag::Timestamp => "Timestamp".to_owned(),
        ValueTypeTag::Date => "RiffDate".to_owned(),
        ValueTypeTag::Uuid => "UUID".to_owned(),
        ValueTypeTag::Enum => value_type
            .enum_type_id()
            .and_then(|id| contract.schema().enumeration(id))
            .map_or_else(|| "str".to_owned(), |value| pascal(value.name())),
        ValueTypeTag::Record => match value_type.record_ref() {
            Some(RecordTypeRef::Entity(id)) => contract
                .schema()
                .entity(*id)
                .map_or_else(|| "str".to_owned(), |entity| pascal(entity.name())),
            _ => "str".to_owned(),
        },
        ValueTypeTag::String => "str".to_owned(),
        ValueTypeTag::Vector => format!(
            "Annotated[tuple[float, ...], \"vector<{}>\"]",
            value_type
                .vector_dimension()
                .expect("vector dimension")
                .get()
        ),
        ValueTypeTag::Optional | ValueTypeTag::List => {
            unreachable!("handled above")
        }
    }
}

fn validate_names(
    module: &QueryModule,
    contract: &ContractBundle,
) -> Result<(), PythonGenerationError> {
    let mut top_level = PYTHON_RESERVED_TOP_LEVEL
        .iter()
        .map(|name| (*name).to_owned())
        .collect::<BTreeSet<_>>();
    let mut methods = BTreeSet::new();

    for enumeration in contract.schema().enums() {
        insert_name(
            &mut top_level,
            pascal(enumeration.name()),
            PythonGenerationError::contract(vec!["enum".to_owned(), enumeration.name().to_owned()]),
        )?;
        validate_normalized_names(
            enumeration.variants().iter().map(|variant| {
                (
                    screaming_snake(variant.name()),
                    PythonGenerationError::contract(vec![
                        "enum".to_owned(),
                        enumeration.name().to_owned(),
                        "variant".to_owned(),
                        variant.name().to_owned(),
                    ]),
                )
            }),
            false,
        )?;
    }
    for entity in contract.schema().entities() {
        insert_name(
            &mut top_level,
            pascal(entity.name()),
            PythonGenerationError::contract(vec!["entity".to_owned(), entity.name().to_owned()]),
        )?;
        validate_normalized_names(
            entity.record().fields().iter().map(|field| {
                (
                    python_identifier(field.name()),
                    PythonGenerationError::contract(vec![
                        "entity".to_owned(),
                        entity.name().to_owned(),
                        "field".to_owned(),
                        field.name().to_owned(),
                    ]),
                )
            }),
            false,
        )?;
    }
    for query in module.queries() {
        let name = pascal(query.name());
        let query_origin = || {
            PythonGenerationError::query(
                query.name(),
                vec!["query".to_owned(), query.name().to_owned()],
            )
        };
        insert_name(&mut top_level, format!("{name}Params"), query_origin())?;
        insert_name(&mut top_level, format!("{name}Result"), query_origin())?;
        insert_name(
            &mut top_level,
            format!("{}_QUERY_PLAN_HASH", screaming_snake(query.name())),
            query_origin(),
        )?;
        insert_name(
            &mut methods,
            python_identifier(&snake(query.name())),
            query_origin(),
        )?;

        let schemas = query.plan().schemas();
        validate_normalized_names(
            schemas.parameters().iter().map(|field| {
                (
                    python_identifier(field.name()),
                    PythonGenerationError::query(
                        query.name(),
                        vec![
                            "query".to_owned(),
                            query.name().to_owned(),
                            "parameter".to_owned(),
                            field.name().to_owned(),
                        ],
                    ),
                )
            }),
            false,
        )?;
        for parameter in schemas.parameters() {
            register_nested_names(
                &mut top_level,
                &format!("{name}Params{}", pascal(parameter.name())),
                parameter.value_type(),
                PythonGenerationError::query(
                    query.name(),
                    vec![
                        "query".to_owned(),
                        query.name().to_owned(),
                        "parameter".to_owned(),
                        parameter.name().to_owned(),
                    ],
                ),
            )?;
        }
        for branch in schemas.results() {
            let branch_name = format!("{name}{}", pascal(branch.name()));
            insert_name(
                &mut top_level,
                branch_name.clone(),
                PythonGenerationError::query(
                    query.name(),
                    vec![
                        "query".to_owned(),
                        query.name().to_owned(),
                        "outcome".to_owned(),
                        branch.name().to_owned(),
                    ],
                ),
            )?;
            validate_normalized_names(
                branch.fields().iter().map(|field| {
                    (
                        python_identifier(field.name()),
                        PythonGenerationError::query(
                            query.name(),
                            vec![
                                "query".to_owned(),
                                query.name().to_owned(),
                                "field".to_owned(),
                                field.name().to_owned(),
                            ],
                        ),
                    )
                }),
                true,
            )?;
            for field in branch.fields() {
                register_nested_names(
                    &mut top_level,
                    &format!("{branch_name}{}", pascal(field.name())),
                    field.value_type(),
                    PythonGenerationError::query(
                        query.name(),
                        vec![
                            "query".to_owned(),
                            query.name().to_owned(),
                            "field".to_owned(),
                            field.name().to_owned(),
                        ],
                    ),
                )?;
            }
        }
    }
    for command in contract.commands() {
        let name = pascal(command.name());
        let command_origin = || {
            PythonGenerationError::contract(vec!["command".to_owned(), command.name().to_owned()])
        };
        insert_name(&mut top_level, format!("{name}Input"), command_origin())?;
        insert_name(&mut top_level, format!("{name}Outcome"), command_origin())?;
        insert_name(
            &mut top_level,
            format!("{}_PLAN_HASH", screaming_snake(command.name())),
            command_origin(),
        )?;
        let method = python_identifier(&snake(command.name()));
        insert_name(&mut methods, method.clone(), command_origin())?;
        insert_name(&mut methods, format!("{method}_batch"), command_origin())?;
        validate_normalized_names(
            command.input().record().fields().iter().map(|field| {
                (
                    python_identifier(field.name()),
                    PythonGenerationError::contract(vec![
                        "command".to_owned(),
                        command.name().to_owned(),
                        "input".to_owned(),
                        field.name().to_owned(),
                    ]),
                )
            }),
            false,
        )?;
        for outcome in command.outcomes() {
            insert_name(
                &mut top_level,
                format!("{name}{}", pascal(outcome.name())),
                PythonGenerationError::contract(vec![
                    "command".to_owned(),
                    command.name().to_owned(),
                    "outcome".to_owned(),
                    outcome.name().to_owned(),
                ]),
            )?;
            validate_normalized_names(
                outcome.payload().fields().iter().map(|field| {
                    (
                        python_identifier(field.name()),
                        PythonGenerationError::contract(vec![
                            "command".to_owned(),
                            command.name().to_owned(),
                            "outcome".to_owned(),
                            outcome.name().to_owned(),
                            "field".to_owned(),
                            field.name().to_owned(),
                        ]),
                    )
                }),
                true,
            )?;
        }
    }
    insert_name(
        &mut top_level,
        format!("{}Client", pascal(module.contract_lineage().as_str())),
        PythonGenerationError::contract(vec![
            "contract".to_owned(),
            module.contract_lineage().as_str().to_owned(),
        ]),
    )?;
    insert_name(
        &mut top_level,
        format!("Async{}Client", pascal(module.contract_lineage().as_str())),
        PythonGenerationError::contract(vec![
            "contract".to_owned(),
            module.contract_lineage().as_str().to_owned(),
        ]),
    )?;
    Ok(())
}

fn register_nested_names(
    top_level: &mut BTreeSet<String>,
    name: &str,
    value_type: &NamedTypeSchema,
    origin: PythonGenerationError,
) -> Result<(), PythonGenerationError> {
    match value_type {
        NamedTypeSchema::Optional(inner)
        | NamedTypeSchema::Set(inner)
        | NamedTypeSchema::BoundedSet { element: inner, .. }
        | NamedTypeSchema::List { element: inner, .. } => {
            register_nested_names(top_level, name, inner, origin)
        }
        NamedTypeSchema::Record(fields) => {
            insert_name(top_level, name.to_owned(), origin.clone())?;
            validate_normalized_names(
                fields.iter().map(|field| {
                    let mut field_origin = origin.clone();
                    field_origin.symbol_path.push(field.name().to_owned());
                    (python_identifier(field.name()), field_origin)
                }),
                false,
            )?;
            for field in fields {
                let mut field_origin = origin.clone();
                field_origin.symbol_path.push(field.name().to_owned());
                register_nested_names(
                    top_level,
                    &format!("{name}{}", pascal(field.name())),
                    field.value_type(),
                    field_origin,
                )?;
            }
            Ok(())
        }
        NamedTypeSchema::Scalar(_)
        | NamedTypeSchema::Cursor
        | NamedTypeSchema::Limit
        | NamedTypeSchema::BoundedLimit { .. } => Ok(()),
    }
}

fn validate_normalized_names(
    names: impl IntoIterator<Item = (String, PythonGenerationError)>,
    reserve_outcome: bool,
) -> Result<(), PythonGenerationError> {
    let mut normalized = BTreeSet::new();
    if reserve_outcome {
        normalized.insert("outcome".to_owned());
    }
    for (name, origin) in names {
        insert_name(&mut normalized, name, origin)?;
    }
    Ok(())
}

fn insert_name(
    names: &mut BTreeSet<String>,
    name: String,
    origin: PythonGenerationError,
) -> Result<(), PythonGenerationError> {
    if name.is_empty() || !names.insert(name) {
        return Err(origin);
    }
    Ok(())
}

const PYTHON_RESERVED_TOP_LEVEL: &[&str] = &[
    "Annotated",
    "AsyncApplicationTransport",
    "AttemptBudget",
    "CONTRACT_BUNDLE_HASH",
    "CONTRACT_LINEAGE",
    "CONTRACT_VERSION",
    "Callable",
    "CommandBatchOptions",
    "CommandBatchProgress",
    "CommandBatchResult",
    "Decimal",
    "Final",
    "Literal",
    "Money",
    "QUERY_MODULE_HASH",
    "QueryOptions",
    "RiffDate",
    "Sequence",
    "StrEnum",
    "SyncApplicationTransport",
    "Timestamp",
    "TypeAlias",
    "TypedCommandResult",
    "TypedQueryResult",
    "UUID",
    "dataclass",
    "decode_variant",
    "encode_record",
    "field",
];

fn is_cursor(value_type: &NamedTypeSchema) -> bool {
    matches!(value_type, NamedTypeSchema::Cursor)
        || matches!(value_type, NamedTypeSchema::Optional(inner) if is_cursor(inner))
}

pub(crate) fn python_identifier(name: &str) -> String {
    let value = snake(name);
    if PYTHON_KEYWORDS.contains(&value.as_str()) {
        format!("{value}_")
    } else {
        value
    }
}

const PYTHON_KEYWORDS: &[&str] = &[
    "and", "as", "assert", "async", "await", "break", "case", "class", "continue", "def", "del",
    "elif", "else", "except", "false", "finally", "for", "from", "global", "if", "import", "in",
    "is", "lambda", "match", "none", "nonlocal", "not", "or", "pass", "raise", "return", "true",
    "try", "while", "with", "yield",
];

fn snake(name: &str) -> String {
    separated(name, '_', false)
}

pub(crate) fn screaming_snake(name: &str) -> String {
    separated(name, '_', true)
}

pub(crate) fn pascal(name: &str) -> String {
    name.split(|character: char| !character.is_ascii_alphanumeric())
        .filter(|part| !part.is_empty())
        .flat_map(|part| {
            let mut chars = part.chars();
            chars
                .next()
                .map(|character| character.to_ascii_uppercase())
                .into_iter()
                .chain(chars)
        })
        .collect()
}

fn separated(name: &str, separator: char, uppercase: bool) -> String {
    let mut output = String::new();
    let mut previous_lower = false;
    for character in name.chars() {
        if !character.is_ascii_alphanumeric() {
            if !output.ends_with(separator) && !output.is_empty() {
                output.push(separator);
            }
            previous_lower = false;
        } else {
            if character.is_ascii_uppercase() && previous_lower {
                output.push(separator);
            }
            output.push(if uppercase {
                character.to_ascii_uppercase()
            } else {
                character.to_ascii_lowercase()
            });
            previous_lower = character.is_ascii_lowercase() || character.is_ascii_digit();
        }
    }
    output
}

fn hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(output, "{byte:02x}").infallible();
    }
    output
}

#[cfg(test)]
mod tests {
    use super::{
        PythonGenerationError, locate_contract_symbol, locate_query_symbol, python_identifier,
        validate_normalized_names,
    };

    #[test]
    fn keywords_gain_one_suffix_and_collisions_are_rejected() {
        assert_eq!(python_identifier("class"), "class_");
        assert_eq!(python_identifier("ordinaryName"), "ordinary_name");
        let first = PythonGenerationError::contract(vec!["first".to_owned()]);
        let second = PythonGenerationError::contract(vec!["second".to_owned()]);
        assert_eq!(
            validate_normalized_names(
                [
                    (python_identifier("class"), first),
                    (python_identifier("class_"), second.clone()),
                ],
                false,
            ),
            Err(second.clone())
        );
        assert_eq!(
            validate_normalized_names([(python_identifier("outcome"), second.clone())], true),
            Err(second)
        );
    }

    #[test]
    fn generation_symbols_resolve_to_exact_contract_and_query_spans() {
        let contract = "contract Demo version 1 {\n  entity Item {\n    key (id: uuid)\n    field class_: string<32>\n  }\n}\n";
        let contract_span = locate_contract_symbol(
            contract,
            &[
                "entity".to_owned(),
                "Item".to_owned(),
                "field".to_owned(),
                "class_".to_owned(),
            ],
        )
        .expect("contract field span");
        assert_eq!(
            &contract[contract_span.0 as usize..contract_span.1 as usize],
            "class_"
        );

        let query = "query ItemPage($id: Item.id) {\n  one item from Item where id == $id else NotFound\n  return Found { renamed: item { id } }\n  outcomes Found | NotFound\n}\n";
        let query_span = locate_query_symbol(
            query,
            &[
                "query".to_owned(),
                "ItemPage".to_owned(),
                "field".to_owned(),
                "renamed".to_owned(),
            ],
        )
        .expect("query alias span");
        assert_eq!(
            &query[query_span.0 as usize..query_span.1 as usize],
            "renamed"
        );
    }
}
