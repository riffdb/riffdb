//! Shared, deterministic application-facade template rendering.

use minijinja::{AutoEscape, Environment, UndefinedBehavior};
use riffdb_contract_ir::ContractBundle;
use riffdb_query_ir::{ReactiveModulePlanV1, ReactiveOperationPlanV1, ReactiveParameterV1};
use serde_json::{Value, json};

use crate::QueryModule;
use crate::generation::{
    command_secret_outputs, embedding_command_facades, generate_mcp_commands,
    generate_mcp_reactive_tools, generate_vector_inspection_tools,
    generated_query_driver_operations, rust_compact_result_shape, rust_generation_model,
    typescript_generation_model, vector_inspection_facades, workflow_revision_bindings,
    workflow_success_outcome_name,
};
use crate::go_generation::{
    decode_expr, decode_named_expr, encode_expr, encode_named_expr, go_decode_compact_expr,
    go_named_type, go_public, go_reactive_decode_expr, go_reactive_encode_expr, go_reactive_type,
    go_snake, go_type, is_cursor, schema_hash,
};
use crate::python_generation::{python_generation_model, python_reactive_generation_model};

const GO_CLIENT_HEADER: &str = include_str!("../../../templates/generators/go/client.go.j2");
const PYTHON_CLIENT_HEADER: &str =
    include_str!("../../../templates/generators/python/client.py.j2");
const RUST_CLIENT_HEADER: &str = include_str!("../../../templates/generators/rust/client.rs.j2");
const RUST_RUNTIME: &str = include_str!("../../../templates/generators/rust/runtime.rs.j2");
const RUST_REACTIVE: &str = include_str!("../../../templates/generators/rust/reactive.rs.j2");
const TYPESCRIPT_CLIENT_HEADER: &str =
    include_str!("../../../templates/generators/typescript/client.ts.j2");

/// Builds the one ordered, language-neutral model consumed by application templates.
#[must_use]
pub fn generate_canonical_generation_model(
    module: &QueryModule,
    contract: &ContractBundle,
    reactive_modules: &[ReactiveModulePlanV1],
) -> Value {
    let has_query_secret_outputs = module
        .queries()
        .iter()
        .any(|query| !query.plan().secret_outputs().is_empty());
    let has_vector = !contract.schema().vector_field_specs().is_empty();
    let has_vector_inspection = !vector_inspection_facades(module, contract).is_empty();
    let has_compact_result = module
        .queries()
        .iter()
        .any(|query| query.plan().common_covered_result().is_some());
    let has_aggregate_collection_budget = contract.commands().iter().any(|command| {
        command
            .collection_expansion()
            .is_some_and(|expansion| expansion.maximum_aggregate_element_bytes().is_some())
    });
    let mut commands = contract
        .commands()
        .iter()
        .filter(|command| !command.is_reimport())
        .collect::<Vec<_>>();
    commands.sort_by(|left, right| left.name().cmp(right.name()));
    let query_driver_operations =
        generated_query_driver_operations(module).expect("validated query operations");
    let command_driver_operations = generate_mcp_commands(module, contract)
        .expect("validated command operations")
        .into_iter()
        .map(|operation| {
            (
                operation.operation_name,
                (operation.name, schema_hash(&operation.input_schema)),
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    let vector_driver_operations = generate_vector_inspection_tools(module, contract)
        .expect("validated vector inspection operations")
        .into_iter()
        .map(|operation| {
            (
                (operation.entity, operation.field, operation.inspection_kind),
                (operation.name, schema_hash(&operation.input_schema)),
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    let go_enums = contract
        .schema()
        .enums()
        .iter()
        .map(|enumeration| {
            let name = go_public(enumeration.name());
            json!({
                "name": name,
                "variants": enumeration.variants().iter().map(|variant| json!({
                    "name": format!("{}{}", name, go_public(variant.name())),
                    "literal": format!("{:?}", variant.name()),
                })).collect::<Vec<_>>(),
            })
        })
        .collect::<Vec<_>>();
    let go_entities = contract
        .schema()
        .entities()
        .iter()
        .map(|entity| {
            json!({
                "name": go_public(entity.name()),
            "fields": entity.record().fields().iter().map(|field| json!({
                "name": go_public(field.name()),
                "type": go_type(field.value_type(), contract),
                "wire_literal": format!("{:?}", field.name()),
                "encode": encode_expr(
                    &format!("value.{}", go_public(field.name())),
                    field.value_type(),
                    contract,
                ),
                "decode": decode_expr("raw", field.value_type(), contract),
            })).collect::<Vec<_>>(),
            })
        })
        .collect::<Vec<_>>();
    let go_queries = module
        .queries()
        .iter()
        .map(|query| {
            let name = go_public(query.name());
            let redacted = !query.plan().secret_outputs().is_empty();
            let schemas = query.plan().schemas();
            let (operation, schema_hash) = query_driver_operations
                .get(query.name())
                .expect("generated query operation");
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
            ).map(|shape| json!({
                "outcome": format!("{:?}", shape.outcome),
                "result_name": format!("{:?}", shape.result_name),
                "result_field": go_public(&shape.result_name),
                "entity": format!("{:?}", shape.entity),
                "width": shape.fields.len(),
                "maximum_rows": shape.maximum_rows,
                "expected": shape.fields.iter().map(|field| format!("{:?}", field.name)).collect::<Vec<_>>(),
                "branch": format!("{}{}", name, go_public(&shape.outcome)),
                "row_fields": shape.result_fields.iter().map(|field| json!({
                    "name": go_public(&field.name),
                    "type": go_type(&field.value_type, contract),
                })).collect::<Vec<_>>(),
                "decodes": shape.fields.iter().enumerate().map(|(index, field)| json!({
                    "field": go_public(&field.name),
                    "expression": go_decode_compact_expr(
                        &format!("rawRow[{index}]"),
                        &field.value_type,
                        contract,
                    ),
                })).collect::<Vec<_>>(),
            }));
            let cursor = schemas.parameters().iter().find(|parameter| is_cursor(parameter.value_type())).map(|parameter| json!({
                "field": go_public(parameter.name()),
                "optional": matches!(
                    parameter.value_type(),
                    riffdb_query_ir::NamedTypeSchema::Optional(_)
                ),
            }));
            json!({
                "name": name,
                "source_name": format!("{:?}", query.name()),
                "plan_hash": hex(query.plan().identity().as_bytes()),
                "operation": format!("{:?}", operation),
                "schema_hash": format!("{:?}", schema_hash),
                "secret_outputs": query.plan().secret_outputs().iter().map(|secret| json!({
                    "query": format!("{:?}", query.name()),
                    "entity": format!("{:?}", secret.entity()),
                    "field": format!("{:?}", secret.field()),
                })).collect::<Vec<_>>(),
                "params": schemas.parameters().iter().map(|parameter| {
                    let mut value_type = go_named_type(parameter.value_type(), contract);
                    if (parameter.has_default()
                        || matches!(
                            parameter.value_type(),
                            riffdb_query_ir::NamedTypeSchema::Optional(_)
                        )) && !value_type.starts_with('*')
                    {
                        value_type = format!("*{value_type}");
                    }
                    json!({
                        "name": go_public(parameter.name()),
                        "type": value_type,
                    })
                }).collect::<Vec<_>>(),
                "results": schemas.results().iter().map(|branch| json!({
                    "name": format!("{}{}", name, go_public(branch.name())),
                    "wire_name": format!("{:?}", branch.name()),
                    "fields": branch.fields().iter().map(|field| json!({
                        "name": go_public(field.name()),
                        "type": go_named_type(field.value_type(), contract),
                        "wire_name": format!("{:?}", field.name()),
                        "decode": decode_named_expr("raw", field.value_type(), contract),
                    })).collect::<Vec<_>>(),
                    "redacted": redacted,
                })).collect::<Vec<_>>(),
                "limit_checks": schemas.parameters().iter().filter_map(|parameter| {
                    let riffdb_query_ir::NamedTypeSchema::BoundedLimit { maximum } = parameter.value_type() else {
                        return None;
                    };
                    Some(json!({
                        "field": go_public(parameter.name()),
                        "maximum": maximum,
                        "defaulted": parameter.has_default(),
                        "message": format!("{:?}", format!("{} must be from 1 through {maximum}", parameter.name())),
                    }))
                }).collect::<Vec<_>>(),
                "cursor": cursor,
                "encoded_params": schemas.parameters().iter().filter(|parameter| !is_cursor(parameter.value_type())).map(|parameter| {
                    let field = go_public(parameter.name());
                    let conditional = parameter.has_default()
                        || matches!(
                            parameter.value_type(),
                            riffdb_query_ir::NamedTypeSchema::Cursor
                                | riffdb_query_ir::NamedTypeSchema::Optional(_)
                        );
                    let access = if conditional {
                        if go_named_type(parameter.value_type(), contract).starts_with('*') {
                            format!("parameters.{field}")
                        } else {
                            format!("*parameters.{field}")
                        }
                    } else {
                        format!("parameters.{field}")
                    };
                    json!({
                        "field": field,
                        "wire_name": format!("{:?}", parameter.name()),
                        "conditional": conditional,
                        "encode": encode_named_expr(&access, parameter.value_type(), contract),
                    })
                }).collect::<Vec<_>>(),
                "compact": compact,
            })
        })
        .collect::<Vec<_>>();
    let go_commands = commands
        .iter()
        .map(|command| {
            let name = go_public(command.name());
            let secret_outputs = command_secret_outputs(command, contract);
            let workflow_revisions = workflow_revision_bindings(command);
            let (operation, operation_schema_hash) = command_driver_operations
                .get(command.name())
                .expect("generated command operation");
            let collection_check = command.collection_expansion().map(|expansion| {
                let field = command
                    .input()
                    .record()
                    .field(expansion.input_field())
                    .expect("validated collection input field");
                json!({
                    "field": go_public(field.name()),
                    "minimum": expansion.minimum_elements(),
                    "maximum": expansion.maximum_elements(),
                    "message": format!("{:?}", format!("invalid bounded collection length for {}.{}", command.name(), field.name())),
                    "aggregate": expansion.maximum_aggregate_element_bytes().map(|maximum| json!({
                        "maximum": maximum,
                        "encode": encode_expr("item", expansion.element_type(), contract),
                        "message": format!("{:?}", format!("invalid aggregate collection bytes for {}.{}", command.name(), field.name())),
                    })),
                })
            });
            let vector_checks = command
                .input()
                .record()
                .fields()
                .iter()
                .filter(|field| field.value_type().tag() == riffdb_contract_ir::ValueTypeTag::Vector)
                .map(|field| json!({
                    "field": go_public(field.name()),
                    "dimension": field.value_type().vector_dimension().expect("vector dimension").get(),
                    "message": format!("{:?}", format!("invalid vector dimension for {}.{}", command.name(), field.name())),
                }))
                .collect::<Vec<_>>();
            let has_validation = collection_check.is_some() || !vector_checks.is_empty();
            let facades = embedding_command_facades(command, contract)
                .into_iter()
                .map(|facade| {
                    let field_name = go_public(&facade.vector_field_name);
                    let identity_constant = format!("{name}{field_name}ModelIdentity");
                    let version_constant = format!("{name}{field_name}ModelVersion");
                    json!({
                        "field_name": field_name,
                        "declared_name": format!("{name}For{field_name}Input"),
                        "identity_constant": identity_constant,
                        "version_constant": version_constant,
                        "model_identity": format!("{:?}", facade.model_identity),
                        "model_version": format!("{:?}", facade.model_version),
                        "fields": command.input().record().fields().iter().filter(|field| {
                            field.name() != facade.model_input_name
                                && field.name() != facade.version_input_name
                        }).map(|field| json!({
                            "name": go_public(field.name()),
                            "type": go_type(field.value_type(), contract),
                        })).collect::<Vec<_>>(),
                        "assignments": command.input().record().fields().iter().map(|field| {
                            let public = go_public(field.name());
                            let value = if field.name() == facade.model_input_name {
                                identity_constant.clone()
                            } else if field.name() == facade.version_input_name {
                                version_constant.clone()
                            } else {
                                format!("input.{public}")
                            };
                            json!({"field": public, "value": value})
                        }).collect::<Vec<_>>(),
                        "model_input": go_public(&facade.model_input_name),
                        "version_input": go_public(&facade.version_input_name),
                    })
                })
                .collect::<Vec<_>>();
            json!({
                "name": name,
                "plan_hash": hex(command.plan_hash().as_bytes()),
                "operation": format!("{:?}", operation),
                "schema_hash": format!("{:?}", operation_schema_hash),
                "secret_outputs": secret_outputs.iter().map(|secret| json!({
                    "outcome": format!("{:?}", secret.outcome),
                    "field": format!("{:?}", secret.field),
                    "entity": format!("{:?}", secret.entity),
                    "source_field": format!("{:?}", secret.source_field),
                })).collect::<Vec<_>>(),
                "inputs": command.input().record().fields().iter().map(|field| json!({
                    "name": go_public(field.name()),
                    "type": go_type(field.value_type(), contract),
                    "wire_name": format!("{:?}", field.name()),
                    "encode": encode_expr(
                        &format!("input.{}", go_public(field.name())),
                        field.value_type(),
                        contract,
                    ),
                })).collect::<Vec<_>>(),
                "facades": facades,
                "outcomes": command.outcomes().iter().map(|outcome| json!({
                    "name": format!("{}{}", name, go_public(outcome.name())),
                    "wire_name": format!("{:?}", outcome.name()),
                    "fields": outcome.payload().fields().iter().map(|field| json!({
                        "name": go_public(field.name()),
                        "type": go_type(field.value_type(), contract),
                        "wire_name": format!("{:?}", field.name()),
                        "decode": decode_expr("raw", field.value_type(), contract),
                    })).collect::<Vec<_>>(),
                    "redacted": secret_outputs.iter().any(|secret| secret.outcome == outcome.name()),
                })).collect::<Vec<_>>(),
                "has_outcome_fields": command.outcomes().iter().any(|outcome| !outcome.payload().fields().is_empty()),
                "collection_check": collection_check,
                "vector_checks": vector_checks,
                "has_validation": has_validation,
                "workflow": {
                    "success_outcome": go_public(workflow_success_outcome_name(command)),
                    "revisions": workflow_revisions.iter().map(|revision| json!({
                        "binding": format!("{:?}", revision.binding_name),
                        "input": go_public(revision.input_name),
                    })).collect::<Vec<_>>(),
                },
            })
        })
        .collect::<Vec<_>>();
    let go_vector_inspections = vector_inspection_facades(module, contract)
        .into_iter()
        .flat_map(|facade| {
            let operations = &vector_driver_operations;
            let base = format!(
                "Inspect{}{}",
                go_public(&facade.entity),
                go_public(&facade.field)
            );
            let partition_type = go_type(&facade.partition_type, contract);
            let partition = encode_expr("partition", &facade.partition_type, contract);
            [
                (
                    "staleness",
                    "Staleness",
                    "VectorStalenessResult",
                    "decodeVectorStaleness",
                ),
                (
                    "model_versions",
                    "ModelVersions",
                    "VectorModelVersionResult",
                    "decodeVectorModelVersions",
                ),
            ]
            .into_iter()
            .map(move |(kind, suffix, result_type, decoder)| {
                let (operation, operation_schema_hash) = operations
                    .get(&(facade.entity.clone(), facade.field.clone(), kind.to_owned()))
                    .expect("generated vector inspection operation");
                json!({
                    "base": base.clone(),
                    "suffix": suffix,
                    "result_type": result_type,
                    "decoder": decoder,
                    "operation": format!("{:?}", operation),
                    "schema_hash": format!("{:?}", operation_schema_hash),
                    "partition_type": partition_type.clone(),
                    "partition": partition.clone(),
                })
            })
        })
        .collect::<Vec<_>>();
    let go_reactive_modules = go_reactive_generation_model(reactive_modules, contract);
    let mut python = python_generation_model(module, contract);
    python["reactive_modules"] = json!(python_reactive_generation_model(
        module,
        contract,
        reactive_modules,
    ));
    let rust = rust_generation_model(module, contract, reactive_modules);
    let typescript = typescript_generation_model(module, contract, reactive_modules);

    json!({
        "schema": "riffdb.application-generation-model/v1",
        "module": {
            "name": module.name().as_str(),
            "identity": hex(module.identity().as_bytes()),
        },
        "contract": {
            "lineage": module.contract_lineage().as_str(),
            "version": module.contract_version().get(),
            "bundle_hash": hex(module.contract_hash().as_bytes()),
        },
        "features": {
            "query_secret_outputs": has_query_secret_outputs,
            "reactive": !reactive_modules.is_empty(),
            "vector": has_vector,
            "vector_inspection": has_vector_inspection,
            "compact_result": has_compact_result,
            "aggregate_collection_budget": has_aggregate_collection_budget,
        },
        "enums": contract.schema().enums().iter().map(|enumeration| json!({
            "name": enumeration.name(),
            "variants": enumeration.variants().iter().map(|variant| variant.name()).collect::<Vec<_>>(),
        })).collect::<Vec<_>>(),
        "entities": contract.schema().entities().iter().map(|entity| entity.name()).collect::<Vec<_>>(),
        "queries": module.queries().iter().map(|query| query.name()).collect::<Vec<_>>(),
        "commands": commands.into_iter().map(|command| command.name()).collect::<Vec<_>>(),
        "reactive_modules": reactive_modules.iter().map(|reactive| json!({
            "name": reactive.name(),
            "version": reactive.version(),
            "operations": reactive.operations().iter().map(|operation| operation.name().as_str()).collect::<Vec<_>>(),
        })).collect::<Vec<_>>(),
        "languages": {
            "go": {
                "package": go_package(module.name().as_str()),
                "enums": go_enums,
                "entities": go_entities,
                "queries": go_queries,
                "commands": go_commands,
                "vector_inspections": go_vector_inspections,
                "reactive_modules": go_reactive_modules,
            },
            "python": python,
            "rust": rust,
            "typescript": typescript,
        },
    })
}

fn go_reactive_generation_model(
    reactive_modules: &[ReactiveModulePlanV1],
    contract: &ContractBundle,
) -> Vec<Value> {
    reactive_modules
        .iter()
        .map(|reactive| {
            let driver_operations = generate_mcp_reactive_tools(reactive, contract)
                .expect("validated reactive operations")
                .into_iter()
                .map(|operation| {
                    (
                        (operation.operation_name, operation.action),
                        (operation.name, schema_hash(&operation.input_schema)),
                    )
                })
                .collect::<std::collections::BTreeMap<_, _>>();
            let operations = reactive
                .operations()
                .iter()
                .map(|operation| {
                    let source_name = operation.name().as_str();
                    let name = go_public(source_name);
                    let parameters = match operation.plan() {
                        ReactiveOperationPlanV1::Stream { parameters, .. }
                        | ReactiveOperationPlanV1::Watch { parameters, .. }
                        | ReactiveOperationPlanV1::Subscription { parameters, .. } => parameters,
                    };
                    let parameters = go_reactive_parameters_model(&name, parameters, contract);
                    let operation_vars = driver_operations
                        .iter()
                        .filter(|((operation_name, action), _)| {
                            operation_name == source_name && !action.starts_with("react_")
                        })
                        .map(|((_, action), (driver_name, operation_schema_hash))| json!({
                            "suffix": go_public(action),
                            "driver_name": format!("{:?}", driver_name),
                            "schema_hash": format!("{:?}", operation_schema_hash),
                        }))
                        .collect::<Vec<_>>();
                    match operation.plan() {
                        ReactiveOperationPlanV1::Stream { events, .. } => json!({
                            "kind": "stream",
                            "name": name,
                            "parameters": parameters,
                            "operation_vars": operation_vars,
                            "events": events.iter().map(|event| json!({
                                "name": go_public(event.name()),
                                "wire_name": format!("{:?}", event.name()),
                                "fields": event.fields().iter().map(|field| json!({
                                    "name": go_public(field.name()),
                                    "type": go_reactive_type(field.type_name(), contract),
                                    "wire_name": format!("{:?}", field.name()),
                                    "decode": go_reactive_decode_expr("raw", field.type_name(), contract),
                                })).collect::<Vec<_>>(),
                            })).collect::<Vec<_>>(),
                        }),
                        ReactiveOperationPlanV1::Watch { query, .. } => json!({
                            "kind": "watch",
                            "name": name,
                            "parameters": parameters,
                            "operation_vars": operation_vars,
                            "query": go_public(query.query_name()),
                        }),
                        ReactiveOperationPlanV1::Subscription {
                            stream_name,
                            reactions,
                            ..
                        } => json!({
                            "kind": "subscription",
                            "name": name,
                            "parameters": parameters,
                            "operation_vars": operation_vars,
                            "stream": go_public(stream_name.as_str()),
                            "reactions": reactions.iter().map(|reaction| {
                                let command = go_public(reaction.command_name());
                                let command_plan = contract.commands().iter().find(|candidate| {
                                    candidate.name() == reaction.command_name()
                                }).expect("reactive compiler retained exact command dependency");
                                let reaction_method = go_public(reaction.reaction_name());
                                let action = format!("react_{}", go_snake(reaction.reaction_name()));
                                let (driver_name, operation_schema_hash) = driver_operations
                                    .get(&(source_name.to_owned(), action))
                                    .expect("generated reaction operation");
                                json!({
                                    "command": command,
                                    "method": reaction_method,
                                    "reaction_name": format!("{:?}", reaction.reaction_name()),
                                    "command_name": format!("{:?}", reaction.command_name()),
                                    "driver_name": format!("{:?}", driver_name),
                                    "schema_hash": format!("{:?}", operation_schema_hash),
                                    "workflow_revisions": !workflow_revision_bindings(command_plan).is_empty(),
                                })
                            }).collect::<Vec<_>>(),
                        }),
                    }
                })
                .collect::<Vec<_>>();
            json!({
                "name": go_public(reactive.name()),
                "hash": hex(reactive.identity().as_bytes()),
                "operations": operations,
            })
        })
        .collect()
}

fn go_reactive_parameters_model(
    operation_name: &str,
    parameters: &[ReactiveParameterV1],
    contract: &ContractBundle,
) -> Value {
    json!({
        "name": operation_name,
        "fields": parameters.iter().map(|parameter| json!({
            "name": go_public(parameter.name()),
            "type": go_reactive_type(parameter.type_name(), contract),
            "wire_name": format!("{:?}", parameter.name()),
            "encode": go_reactive_encode_expr(
                &format!("parameters.{}", go_public(parameter.name())),
                parameter.type_name(),
                contract,
            ),
        })).collect::<Vec<_>>(),
    })
}

fn go_package(value: &str) -> String {
    let mut output = String::new();
    for (index, ch) in value.chars().enumerate() {
        if ch.is_ascii_uppercase() && index != 0 {
            output.push('_');
        }
        output.push(ch.to_ascii_lowercase());
    }
    output.replace('-', "_")
}

pub(crate) fn render_go_client_header(model: &Value) -> Result<String, minijinja::Error> {
    render(GO_CLIENT_HEADER, model).map(|mut rendered| {
        rendered.push('\n');
        rendered
    })
}

pub(crate) fn render_python_client_header(model: &Value) -> Result<String, minijinja::Error> {
    render(PYTHON_CLIENT_HEADER, model)
}

pub(crate) fn render_rust_client_header(model: &Value) -> Result<String, minijinja::Error> {
    let mut rendered = render(RUST_CLIENT_HEADER, model)?;
    rendered.push_str("\n\n");
    rendered.push_str(&render(RUST_RUNTIME, model)?);
    if model["features"]["reactive"] == Value::Bool(true) {
        rendered.push('\n');
        rendered.push_str(&render(RUST_REACTIVE, model)?);
    }
    Ok(rendered)
}

pub(crate) fn render_typescript_client_header(model: &Value) -> Result<String, minijinja::Error> {
    render(TYPESCRIPT_CLIENT_HEADER, model)
}

fn render(template: &str, model: &Value) -> Result<String, minijinja::Error> {
    let mut environment = Environment::new();
    environment.set_undefined_behavior(UndefinedBehavior::Strict);
    environment.set_auto_escape_callback(|_| AutoEscape::None);
    environment.render_str(template, model)
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(DIGITS[usize::from(byte >> 4)]));
        output.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    output
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        GO_CLIENT_HEADER, PYTHON_CLIENT_HEADER, RUST_CLIENT_HEADER, RUST_REACTIVE, RUST_RUNTIME,
        TYPESCRIPT_CLIENT_HEADER, render,
    };

    fn function_body<'a>(source: &'a str, start: &str, end: &str) -> &'a str {
        source
            .split_once(start)
            .expect("generator entry point must exist")
            .1
            .split_once(end)
            .expect("generator entry point boundary must exist")
            .0
    }

    // req: GEN-004
    #[test]
    fn template_rendering_is_strict_and_does_not_autoescape_source() {
        let model = json!({"source": "Vec<T> & value"});
        assert_eq!(
            render("{{ source }}", &model).expect("render source"),
            "Vec<T> & value"
        );
        assert!(render("{{ missing }}", &model).is_err());
    }

    // req: GEN-004
    #[test]
    fn checked_in_language_templates_do_not_accept_preassembled_source_blobs() {
        for template in [
            GO_CLIENT_HEADER,
            PYTHON_CLIENT_HEADER,
            RUST_CLIENT_HEADER,
            RUST_RUNTIME,
            RUST_REACTIVE,
            TYPESCRIPT_CLIENT_HEADER,
        ] {
            assert!(!template.contains("languages.typescript.source"));
            assert!(!template.contains("{{ source }}"));
        }
    }

    // req: GEN-004
    #[test]
    fn application_generator_entry_points_only_build_and_render_the_canonical_model() {
        let generation = include_str!("generation.rs");
        let go = include_str!("go_generation.rs");
        let python = include_str!("python_generation.rs");
        let bodies = [
            function_body(
                generation,
                "pub fn generate_rust_client(",
                "pub fn generate_rust_application_client(",
            ),
            function_body(
                generation,
                "pub fn generate_rust_application_client(",
                "fn rust_reactive_type(",
            ),
            function_body(
                generation,
                "pub fn generate_typescript_client(",
                "fn generate_typescript_source_base(",
            ),
            function_body(
                generation,
                "pub fn generate_typescript_application_client(",
                "pub(crate) fn typescript_generation_source(",
            ),
            function_body(
                go,
                "fn generate_go_client_inner(",
                "// Template helper expressions",
            ),
            function_body(
                python,
                "pub fn generate_python_client(",
                "pub fn generate_python_application_client(",
            ),
            function_body(
                python,
                "pub fn generate_python_application_client(",
                "// Python type-model helpers",
            ),
        ];

        for body in bodies {
            assert!(
                !body.contains("emit_")
                    && !body.contains("write!(")
                    && !body.contains("writeln!(")
                    && !body.contains("push_str("),
                "application generator still assembles source outside its checked-in template"
            );
        }
    }
}
