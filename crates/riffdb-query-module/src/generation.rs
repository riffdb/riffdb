//! Reproducible, name-addressed client source generation.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use crate::infallible_string_write::InfallibleStringWrite as _;

use riffdb_contract_ir::{
    CommandPlan, ContractBundle, ExpressionKind, Instruction, RecordTypeRef, SchemaArtifactKey,
    SecretRevealDestinationV1, ValueType, ValueTypeTag, WorkflowLeaseOperation,
};
use riffdb_query_ir::{
    CoveredResultLayoutV1, NamedQuerySchemas, NamedTypeSchema, PageBound, QueryAccessKind,
    ReactiveModulePlanV1, ReactiveOperationPlanV1, max_query_page_take,
};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};

use crate::QueryModule;
use crate::template_generation::{
    generate_canonical_generation_model, render_rust_client_header, render_typescript_client_header,
};

const MCP_SCHEMA_DIALECT: &str = "https://json-schema.org/draft/2020-12/schema";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WorkflowRevisionBinding<'a> {
    pub(crate) binding_name: &'a str,
    pub(crate) input_name: &'a str,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CommandSecretOutput<'a> {
    pub(crate) outcome: &'a str,
    pub(crate) field: &'a str,
    pub(crate) entity: &'a str,
    pub(crate) source_field: &'a str,
}

pub(crate) fn command_secret_outputs<'a>(
    command: &'a CommandPlan,
    contract: &'a ContractBundle,
) -> Vec<CommandSecretOutput<'a>> {
    command
        .secret_reveals()
        .iter()
        .filter_map(|reveal| {
            let SecretRevealDestinationV1::OutcomeField { outcome, field } = reveal.destination()
            else {
                return None;
            };
            let binding = command
                .bindings()
                .iter()
                .find(|binding| binding.id() == reveal.source_binding())
                .expect("checked secret source binding");
            let entity = contract
                .schema()
                .entity(binding.entity_type())
                .expect("checked secret source entity");
            let source_field = entity
                .record()
                .field(reveal.source_field())
                .expect("checked secret source field");
            let outcome = command
                .outcomes()
                .iter()
                .find(|candidate| candidate.id() == outcome)
                .expect("checked secret destination outcome");
            let field = outcome
                .payload()
                .field(field)
                .expect("checked secret destination field");
            Some(CommandSecretOutput {
                outcome: outcome.name(),
                field: field.name(),
                entity: entity.name(),
                source_field: source_field.name(),
            })
        })
        .collect()
}

/// One compiler-proven production embedding assignment used to generate a
/// convenience constructor that supplies the contract-sealed model evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct EmbeddingCommandFacade {
    pub(crate) vector_field_name: String,
    pub(crate) model_input_name: String,
    pub(crate) version_input_name: String,
    pub(crate) model_identity: String,
    pub(crate) model_version: String,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct VectorInspectionFacade {
    pub(crate) entity: String,
    pub(crate) field: String,
    pub(crate) partition_type: ValueType,
    pub(crate) source_queries: Vec<String>,
}

pub(crate) fn vector_inspection_facades(
    module: &QueryModule,
    contract: &ContractBundle,
) -> Vec<VectorInspectionFacade> {
    let mut facades = BTreeMap::<(String, String, ValueType), BTreeSet<String>>::new();
    for query in module.queries() {
        for step in query.plan().representative_program().steps() {
            let QueryAccessKind::Nearest { vector_field, .. } = step.access() else {
                continue;
            };
            let entity = contract
                .schema()
                .entity(step.internal_entity_id())
                .expect("checked nearest entity");
            let aggregate = contract
                .schema()
                .aggregate_for_entity(entity.id())
                .expect("checked nearest aggregate");
            let partition_type = aggregate.keys().partition_schema().components()[0]
                .value_type()
                .clone();
            facades
                .entry((
                    entity.name().to_owned(),
                    vector_field.clone(),
                    partition_type,
                ))
                .or_default()
                .insert(query.name().to_owned());
        }
    }
    facades
        .into_iter()
        .map(
            |((entity, field, partition_type), source_queries)| VectorInspectionFacade {
                entity,
                field,
                partition_type,
                source_queries: source_queries.into_iter().collect(),
            },
        )
        .collect()
}

pub(crate) fn embedding_command_facades(
    command: &CommandPlan,
    contract: &ContractBundle,
) -> Vec<EmbeddingCommandFacade> {
    command_effect_instructions(command)
        .filter_map(|instruction| {
            let Instruction::SetEmbedding {
                binding,
                field,
                model_identity,
                model_version,
                ..
            } = instruction
            else {
                return None;
            };
            let input_field = |expression| {
                let expression = command
                    .expressions()
                    .get(expression)
                    .expect("checked embedding evidence expression");
                let ExpressionKind::InputField(field) = expression.kind() else {
                    panic!("checked embedding evidence must be a direct input");
                };
                command
                    .input()
                    .record()
                    .field(*field)
                    .expect("checked embedding evidence input")
            };
            let binding = command
                .bindings()
                .get(binding.get() as usize)
                .expect("checked embedding binding");
            let entity = contract
                .schema()
                .entity(binding.entity_type())
                .expect("checked embedding entity");
            let vector_field = entity
                .record()
                .field(*field)
                .expect("checked embedding field");
            let production = contract
                .schema()
                .vector_production_spec(binding.entity_type(), *field)
                .expect("checked production embedding declaration");
            Some(EmbeddingCommandFacade {
                vector_field_name: vector_field.name().to_owned(),
                model_input_name: input_field(*model_identity).name().to_owned(),
                version_input_name: input_field(*model_version).name().to_owned(),
                model_identity: production.metadata().model_identity().to_owned(),
                model_version: production.metadata().model_version().to_owned(),
            })
        })
        .collect()
}

pub(crate) fn workflow_success_outcome_name(command: &CommandPlan) -> &str {
    command
        .outcomes()
        .iter()
        .find(|outcome| outcome.id() == command.success_outcome())
        .expect("checked command success outcome")
        .name()
}

pub(crate) fn workflow_revision_bindings(
    command: &CommandPlan,
) -> Vec<WorkflowRevisionBinding<'_>> {
    let mut revisions = BTreeMap::new();
    for instruction in command_effect_instructions(command) {
        let (binding, expected_revision) = match instruction {
            Instruction::WorkflowTransition {
                binding,
                expected_revision,
                ..
            } => (*binding, *expected_revision),
            Instruction::WorkflowLease {
                binding, operation, ..
            } => {
                let expected_revision = match operation {
                    WorkflowLeaseOperation::Claim {
                        expected_revision, ..
                    }
                    | WorkflowLeaseOperation::Renew {
                        expected_revision, ..
                    }
                    | WorkflowLeaseOperation::Release {
                        expected_revision, ..
                    }
                    | WorkflowLeaseOperation::Expire {
                        expected_revision, ..
                    }
                    | WorkflowLeaseOperation::Fence {
                        expected_revision, ..
                    } => *expected_revision,
                };
                (*binding, expected_revision)
            }
            _ => continue,
        };
        let input_id = match command
            .expressions()
            .get(expected_revision)
            .expect("checked workflow revision expression")
            .kind()
        {
            ExpressionKind::InputField(field) => *field,
            _ => panic!("checked workflow revision must be a direct input"),
        };
        let binding_name = command
            .bindings()
            .get(binding.get() as usize)
            .expect("checked workflow binding")
            .name();
        let input_name = command
            .input()
            .record()
            .field(input_id)
            .expect("checked workflow revision input")
            .name();
        if let Some(previous) = revisions.insert(binding, (binding_name, input_name)) {
            assert_eq!(
                previous,
                (binding_name, input_name),
                "one workflow binding must use one exact revision input"
            );
        }
    }
    revisions
        .into_values()
        .map(|(binding_name, input_name)| WorkflowRevisionBinding {
            binding_name,
            input_name,
        })
        .collect()
}

fn command_effect_instructions(command: &CommandPlan) -> impl Iterator<Item = &Instruction> {
    command.instructions().iter().chain(
        command
            .decisions()
            .iter()
            .flat_map(|decision| {
                decision
                    .when_arms()
                    .iter()
                    .map(|arm| arm.action())
                    .chain(std::iter::once(decision.else_action()))
            })
            .flat_map(riffdb_contract_ir::CommandDecisionActionV1::instructions),
    )
}

/// One compiler-owned generated MCP tool for a visible named query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GeneratedMcpTool {
    /// Exact source-level query symbol.
    pub operation_name: String,
    /// Stable module-qualified tool name.
    pub name: String,
    /// Human-facing title.
    pub title: String,
    /// Bounded safe description.
    pub description: String,
    /// Canonical input JSON Schema.
    pub input_schema: String,
    /// Canonical result JSON Schema.
    pub result_schema: String,
    /// Exact immutable module identity.
    pub module_hash: [u8; 32],
}

/// One compiler-owned generated MCP command operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GeneratedMcpCommand {
    /// Stable compiler-owned command identity.
    pub command_id: u32,
    /// Exact source-level command symbol.
    pub operation_name: String,
    /// Stable module-qualified tool name.
    pub name: String,
    /// Human-facing title.
    pub title: String,
    /// Bounded safe description.
    pub description: String,
    /// Canonical input JSON Schema.
    pub input_schema: String,
    /// Canonical declared-outcome JSON Schema.
    pub result_schema: String,
    /// Exact compiler-owned input schema used by hosted MCP discovery.
    pub compiler_input_schema: String,
    /// Exact compiler-owned outcome union used by hosted MCP discovery.
    pub compiler_outcome_schema: String,
    /// Domain-separated hash of the compiler-owned input schema.
    pub input_schema_hash: [u8; 32],
    /// Domain-separated hash of the compiler-owned outcome-union schema.
    pub outcome_schema_hash: [u8; 32],
    /// Exact ADR-0064 compiler-owned hosted MCP name.
    pub mcp_tool_name: String,
    /// Exact contract bundle identity.
    pub contract_bundle_hash: [u8; 32],
    /// Exact command plan identity.
    pub plan_hash: [u8; 32],
}

/// One compiler-owned generated symbolic vector-state inspection operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GeneratedVectorInspectionTool {
    /// Stable compiler-owned local operation name.
    pub name: String,
    /// Symbolic contract entity.
    pub entity: String,
    /// Symbolic vector field.
    pub field: String,
    /// Closed inspection kind (`staleness` or `model_versions`).
    pub inspection_kind: String,
    /// Named nearest queries whose exact role grants derive this authority.
    pub source_queries: Vec<String>,
    /// Exact immutable query-module identity for every source query.
    pub module_hash: [u8; 32],
    /// Exact contract bundle identity.
    pub contract_bundle_hash: [u8; 32],
    /// Canonical input JSON Schema.
    pub input_schema: String,
    /// Canonical closed result JSON Schema.
    pub result_schema: String,
}

/// One compiler-owned generated MCP operation for a reactive stream or watch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GeneratedMcpReactiveTool {
    /// Exact source-level reactive operation symbol.
    pub operation_name: String,
    /// Exact closed generated action name.
    pub action: String,
    /// Closed source operation class: stream, watch, or subscription.
    pub operation_kind: String,
    /// Exact declared reaction name for a reaction action.
    pub reaction_name: Option<String>,
    /// Exact target command symbol for a reaction action.
    pub reaction_command_name: Option<String>,
    /// Stable compiler-owned target command identity for proof forwarding.
    pub reaction_command_id: Option<u32>,
    /// Stable underscore-only module, operation, and action name.
    pub name: String,
    /// Human-facing title.
    pub title: String,
    /// Bounded safe description.
    pub description: String,
    /// Canonical input JSON Schema.
    pub input_schema: String,
    /// Canonical result JSON Schema.
    pub result_schema: String,
    /// Exact immutable reactive-module identity.
    pub reactive_module_hash: [u8; 32],
}

/// Closed generated-tool failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum McpToolGenerationError {
    /// Two source names normalize to the same tool name.
    NameCollision,
    /// A generated schema could not be serialized.
    InvalidSchema,
}

/// Generates the exact MCP actions for each stream, watch, and contextual
/// subscription in one immutable reactive module.
pub fn generate_mcp_reactive_tools(
    module: &ReactiveModulePlanV1,
    contract: &ContractBundle,
) -> Result<Vec<GeneratedMcpReactiveTool>, McpToolGenerationError> {
    let mut tools = Vec::new();
    let mut names = BTreeSet::new();
    for operation in module.operations() {
        let parameters = match operation.plan() {
            ReactiveOperationPlanV1::Stream { parameters, .. }
            | ReactiveOperationPlanV1::Watch { parameters, .. }
            | ReactiveOperationPlanV1::Subscription { parameters, .. } => parameters,
        };
        let parameter_properties = parameters
            .iter()
            .map(|parameter| {
                (
                    parameter.name().to_owned(),
                    reactive_type_schema(parameter.type_name(), contract),
                )
            })
            .collect::<Map<_, _>>();
        let parameter_required = parameters
            .iter()
            .map(|parameter| Value::String(parameter.name().to_owned()))
            .collect::<Vec<_>>();
        let actions = match operation.plan() {
            ReactiveOperationPlanV1::Stream { .. } => ["ack", "nack", "next", "seek", "status"]
                .into_iter()
                .map(str::to_owned)
                .collect::<Vec<_>>(),
            ReactiveOperationPlanV1::Watch { .. } => vec!["watch".to_owned()],
            ReactiveOperationPlanV1::Subscription { reactions, .. } => {
                let mut actions = vec![
                    "ack".to_owned(),
                    "nack".to_owned(),
                    "next".to_owned(),
                    "status".to_owned(),
                ];
                actions.extend(
                    reactions
                        .iter()
                        .map(|reaction| format!("react_{}", snake(reaction.reaction_name()))),
                );
                actions
            }
        };
        let operation_kind = match operation.plan() {
            ReactiveOperationPlanV1::Stream { .. } => "stream",
            ReactiveOperationPlanV1::Watch { .. } => "watch",
            ReactiveOperationPlanV1::Subscription { .. } => "subscription",
        };
        for action in &actions {
            let name = format!(
                "{}_{}_{}",
                snake(module.name()),
                snake(operation.name().as_str()),
                action
            );
            if !names.insert(name.clone()) {
                return Err(McpToolGenerationError::NameCollision);
            }
            let mut properties = Map::new();
            let reaction = if action.starts_with("react_") {
                match operation.plan() {
                    ReactiveOperationPlanV1::Subscription { reactions, .. } => {
                        reactions.iter().find(|reaction| {
                            action == &format!("react_{}", snake(reaction.reaction_name()))
                        })
                    }
                    _ => None,
                }
            } else {
                None
            };
            properties.insert(
                "parameters".to_owned(),
                json!({
                    "type": "object",
                    "additionalProperties": false,
                    "properties": parameter_properties,
                    "required": parameter_required,
                }),
            );
            let mut required = vec![Value::String("parameters".to_owned())];
            if matches!(action.as_str(), "ack" | "nack") {
                properties.insert("event_id".to_owned(), json!({"type":"string"}));
                properties.insert("lease_token".to_owned(), json!({"type":"string"}));
                properties.insert(
                    "history_incarnation".to_owned(),
                    json!({"type":"string", "pattern":"^[1-9][0-9]*$"}),
                );
                required.extend(
                    ["event_id", "lease_token", "history_incarnation"]
                        .map(|name| Value::String(name.to_owned())),
                );
            }
            if matches!(action.as_str(), "next" | "ack" | "nack" | "seek" | "status")
                || action.starts_with("react_")
            {
                properties.insert(
                    "consumer_name".to_owned(),
                    json!({"type":"string", "pattern":"^[A-Za-z][A-Za-z0-9_-]{0,63}$"}),
                );
                required.push(Value::String("consumer_name".to_owned()));
            }
            if action == "next" && operation_kind == "stream" {
                properties.insert(
                    "batch_limit".to_owned(),
                    json!({"type":"integer", "minimum":1, "maximum":64, "default":1}),
                );
                properties.insert(
                    "in_flight_limit".to_owned(),
                    json!({"type":"integer", "minimum":1, "maximum":64, "default":16}),
                );
                properties.insert(
                    "lease_seconds".to_owned(),
                    json!({"type":"integer", "minimum":5, "maximum":900, "default":60}),
                );
            }
            if action == "nack" {
                properties.insert(
                    "retry_delay_nanos".to_owned(),
                    json!({
                        "type":"string",
                        "pattern":"^(0|[1-9][0-9]{0,17})$",
                        "default":"0"
                    }),
                );
            }
            if action == "seek" {
                properties.insert("checkpoint".to_owned(), json!({"type":"string"}));
                required.push(Value::String("checkpoint".to_owned()));
            }
            if action == "watch" {
                properties.insert("cursor".to_owned(), json!({"type":["string", "null"]}));
            }
            if action.starts_with("react_") {
                let reaction = reaction.expect("action was built from one declared reaction");
                let command = contract
                    .commands()
                    .iter()
                    .find(|command| command.name() == reaction.command_name())
                    .expect("reactive compiler retained exact command dependency");
                let command_properties = command
                    .input()
                    .record()
                    .fields()
                    .iter()
                    .filter(|field| command.idempotency_input() != Some(field.id()))
                    .map(|field| {
                        (
                            field.name().to_owned(),
                            mcp_contract_type_schema(field.value_type(), contract),
                        )
                    })
                    .collect::<Map<_, _>>();
                let command_required = command
                    .input()
                    .record()
                    .fields()
                    .iter()
                    .filter(|field| command.idempotency_input() != Some(field.id()))
                    .map(|field| Value::String(field.name().to_owned()))
                    .collect::<Vec<_>>();
                properties.insert(
                    "causation_token".to_owned(),
                    json!({"type":"string", "contentEncoding":"base64"}),
                );
                properties.insert(
                    "input".to_owned(),
                    json!({
                        "type":"object",
                        "additionalProperties":false,
                        "properties":command_properties,
                        "required":command_required,
                    }),
                );
                required.extend(
                    ["causation_token", "input"].map(|name| Value::String(name.to_owned())),
                );
            }
            let input = json!({
                "$schema": MCP_SCHEMA_DIALECT,
                "type": "object",
                "additionalProperties": false,
                "properties": properties,
                "required": required,
            });
            let result = json!({
                "$schema": MCP_SCHEMA_DIALECT,
                "type": "object",
                "additionalProperties": true,
            });
            tools.push(GeneratedMcpReactiveTool {
                operation_name: operation.name().as_str().to_owned(),
                action: action.clone(),
                operation_kind: operation_kind.to_owned(),
                reaction_name: reaction.map(|value| value.reaction_name().to_owned()),
                reaction_command_name: reaction.map(|value| value.command_name().to_owned()),
                reaction_command_id: reaction.map(|value| {
                    contract
                        .commands()
                        .iter()
                        .find(|command| command.name() == value.command_name())
                        .expect("compiled reaction command")
                        .command_id()
                        .get()
                }),
                title: format!("{} {}", action, operation.name().as_str()),
                description: format!(
                    "Run the authorized {} action for exact reactive operation {}.",
                    action,
                    operation.name().as_str()
                ),
                name,
                input_schema: serde_json::to_string(&input)
                    .map_err(|_| McpToolGenerationError::InvalidSchema)?,
                result_schema: serde_json::to_string(&result)
                    .map_err(|_| McpToolGenerationError::InvalidSchema)?,
                reactive_module_hash: *module.identity().as_bytes(),
            });
        }
    }
    tools.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(tools)
}

fn reactive_type_schema(type_name: &str, contract: &ContractBundle) -> Value {
    let tagged = |kind: &str, properties: Map<String, Value>, required: Vec<&str>| {
        let mut properties = properties;
        properties.insert("type".to_owned(), json!({"const":kind}));
        json!({
            "type":"object",
            "additionalProperties":false,
            "properties":properties,
            "required": required.into_iter().chain(["type"]).collect::<Vec<_>>(),
        })
    };
    if let Some(enumeration) = contract
        .schema()
        .enums()
        .iter()
        .find(|value| value.name() == type_name)
    {
        let variants = enumeration
            .variants()
            .iter()
            .map(|variant| {
                tagged(
                    "enum",
                    Map::from_iter([
                        (
                            "type_id".to_owned(),
                            json!({"const":enumeration.id().get()}),
                        ),
                        ("variant_id".to_owned(), json!({"const":variant.id().get()})),
                        ("name".to_owned(), json!({"const":variant.name()})),
                    ]),
                    vec!["type_id", "variant_id", "name"],
                )
            })
            .collect::<Vec<_>>();
        return json!({"oneOf": variants});
    }
    if let Some((precision, scale)) = decimal_type_parts(type_name) {
        return tagged(
            "decimal",
            Map::from_iter([
                (
                    "coefficient_twos_complement".to_owned(),
                    json!({"type":"string","contentEncoding":"base64"}),
                ),
                ("precision".to_owned(), json!({"const":precision})),
                ("scale".to_owned(), json!({"const":scale})),
            ]),
            vec!["coefficient_twos_complement", "precision", "scale"],
        );
    }
    let (kind, properties, required) = if let Some(currency) = money_type_currency(type_name) {
        (
            "money",
            Map::from_iter([
                ("currency".to_owned(), json!({"const":currency})),
                (
                    "amount".to_owned(),
                    json!({
                        "type":"object", "additionalProperties":false,
                        "properties":{
                            "coefficient_twos_complement":{"type":"string","contentEncoding":"base64"},
                            "precision":{"const":38}, "scale":{"const":2}
                        },
                        "required":["coefficient_twos_complement","precision","scale"]
                    }),
                ),
            ]),
            vec!["currency", "amount"],
        )
    } else if type_name == "bool" {
        (
            "bool",
            Map::from_iter([("value".to_owned(), json!({"type":"boolean"}))]),
            vec!["value"],
        )
    } else if matches!(type_name, "i64" | "u64") {
        let pattern = if type_name == "i64" {
            "^-?(0|[1-9][0-9]*)$"
        } else {
            "^(0|[1-9][0-9]*)$"
        };
        (
            type_name,
            Map::from_iter([(
                "value".to_owned(),
                json!({"type":"string","pattern":pattern}),
            )]),
            vec!["value"],
        )
    } else if type_name == "uuid" {
        (
            "uuid",
            Map::from_iter([("value".to_owned(), json!({"type":"string","format":"uuid"}))]),
            vec!["value"],
        )
    } else if type_name == "date" {
        (
            "date",
            Map::from_iter([(
                "days_since_unix_epoch".to_owned(),
                json!({"type":"integer"}),
            )]),
            vec!["days_since_unix_epoch"],
        )
    } else if type_name == "timestamp" {
        (
            "timestamp",
            Map::from_iter([
                ("seconds".to_owned(), json!({"type":"string"})),
                (
                    "nanos".to_owned(),
                    json!({"type":"integer","minimum":0,"maximum":999999999}),
                ),
            ]),
            vec!["seconds", "nanos"],
        )
    } else if type_name.starts_with("bytes<") {
        (
            "bytes",
            Map::from_iter([(
                "value".to_owned(),
                json!({"type":"string","contentEncoding":"base64"}),
            )]),
            vec!["value"],
        )
    } else {
        (
            "string",
            Map::from_iter([("value".to_owned(), json!({"type":"string"}))]),
            vec!["value"],
        )
    };
    tagged(kind, properties, required)
}

/// Generates deterministic read-only MCP tool artifacts for public named queries.
pub fn generate_mcp_tools(
    module: &QueryModule,
) -> Result<Vec<GeneratedMcpTool>, McpToolGenerationError> {
    generate_all_query_tools(module).map(|tools| {
        tools
            .into_iter()
            .filter(|tool| {
                module
                    .query(&tool.operation_name)
                    .is_some_and(|query| query.plan().secret_outputs().is_empty())
            })
            .collect()
    })
}

/// Generates SDK-only query registry entries excluded from MCP invocation.
pub fn generate_sdk_only_query_tools(
    module: &QueryModule,
) -> Result<Vec<GeneratedMcpTool>, McpToolGenerationError> {
    generate_all_query_tools(module).map(|tools| {
        tools
            .into_iter()
            .filter(|tool| {
                module
                    .query(&tool.operation_name)
                    .is_some_and(|query| !query.plan().secret_outputs().is_empty())
            })
            .collect()
    })
}

/// Generates deterministic symbolic vector-state operations for every nearest
/// source in this exact query module. The source-query list is retained so the
/// shared driver catalog can expose an operation only when the selected role
/// contains at least one compiler-derived inspection grant.
pub fn generate_vector_inspection_tools(
    module: &QueryModule,
    contract: &ContractBundle,
) -> Result<Vec<GeneratedVectorInspectionTool>, McpToolGenerationError> {
    let mut names = BTreeSet::new();
    let mut tools = Vec::new();
    for facade in vector_inspection_facades(module, contract) {
        let input = json!({
            "$schema": MCP_SCHEMA_DIALECT,
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "partition": mcp_contract_type_schema(&facade.partition_type, contract),
                "limit": {"type": "integer", "minimum": 1, "maximum": 500},
            },
            "required": ["partition", "limit"],
        });
        for (inspection_kind, suffix, result) in [
            ("staleness", "staleness", vector_staleness_result_schema()),
            (
                "model_versions",
                "model_versions",
                vector_model_versions_result_schema(),
            ),
        ] {
            let name = format!(
                "{}_inspect_{}_{}_{}",
                snake(module.name().as_str()),
                snake(&facade.entity),
                snake(&facade.field),
                suffix,
            );
            if !names.insert(name.clone()) {
                return Err(McpToolGenerationError::NameCollision);
            }
            tools.push(GeneratedVectorInspectionTool {
                name,
                entity: facade.entity.clone(),
                field: facade.field.clone(),
                inspection_kind: inspection_kind.to_owned(),
                source_queries: facade.source_queries.clone(),
                module_hash: *module.identity().as_bytes(),
                contract_bundle_hash: *contract.bundle_hash().as_bytes(),
                input_schema: serde_json::to_string(&input)
                    .map_err(|_| McpToolGenerationError::InvalidSchema)?,
                result_schema: serde_json::to_string(&result)
                    .map_err(|_| McpToolGenerationError::InvalidSchema)?,
            });
        }
    }
    Ok(tools)
}

fn vector_staleness_result_schema() -> Value {
    json!({
        "$schema": MCP_SCHEMA_DIALECT,
        "oneOf": [
            {
                "type": "object", "additionalProperties": false,
                "properties": {
                    "kind": {"const": "staleness_summary"},
                    "total_entities": {"type": "integer", "minimum": 0},
                    "stale_count": {"type": "integer", "minimum": 0},
                    "stale_entity_count_threshold": {"type": "integer", "minimum": 0},
                    "slo_breached": {"type": "boolean"},
                },
                "required": ["kind", "total_entities", "stale_count", "stale_entity_count_threshold", "slo_breached"],
            },
            {
                "type": "object", "additionalProperties": false,
                "properties": {
                    "kind": {"const": "stale_entities"},
                    "items": {"type": "array", "maxItems": 500, "items": {
                        "type": "object", "additionalProperties": false,
                        "properties": {
                            "entity_key": {"type": "string"},
                            "newest_source_write": {"type": "integer", "minimum": 1},
                            "embedding_write": {"type": ["integer", "null"], "minimum": 1},
                        },
                        "required": ["entity_key", "newest_source_write", "embedding_write"],
                    }},
                    "observed_frontier": {"type": ["integer", "null"], "minimum": 1},
                },
                "required": ["kind", "items", "observed_frontier"],
            },
        ],
    })
}

fn vector_model_versions_result_schema() -> Value {
    json!({
        "$schema": MCP_SCHEMA_DIALECT,
        "oneOf": [
            {
                "type": "object", "additionalProperties": false,
                "properties": {
                    "kind": {"const": "model_version_summary"},
                    "current_count": {"type": "integer", "minimum": 0},
                    "outdated_count": {"type": "integer", "minimum": 0},
                },
                "required": ["kind", "current_count", "outdated_count"],
            },
            {
                "type": "object", "additionalProperties": false,
                "properties": {
                    "kind": {"const": "outdated_model_entities"},
                    "items": {"type": "array", "maxItems": 500, "items": {
                        "type": "object", "additionalProperties": false,
                        "properties": {
                            "entity_key": {"type": "string"},
                            "model": {"type": "string", "maxLength": 256},
                            "model_version": {"type": "string", "maxLength": 256},
                            "embedding_write": {"type": "integer", "minimum": 1},
                        },
                        "required": ["entity_key", "model", "model_version", "embedding_write"],
                    }},
                    "observed_frontier": {"type": ["integer", "null"], "minimum": 1},
                },
                "required": ["kind", "items", "observed_frontier"],
            },
        ],
    })
}

fn generate_all_query_tools(
    module: &QueryModule,
) -> Result<Vec<GeneratedMcpTool>, McpToolGenerationError> {
    let mut names = BTreeSet::new();
    module
        .queries()
        .iter()
        .map(|query| {
            let name = format!("{}_{}", snake(module.name().as_str()), snake(query.name()));
            if !names.insert(name.clone()) {
                return Err(McpToolGenerationError::NameCollision);
            }
            let schemas = query.plan().schemas();
            let pagination = mcp_pagination_parameter(query);
            let input = mcp_query_input_schema(query, pagination);
            let branches = schemas
                .results()
                .iter()
                .map(|branch| {
                    let mut fields = Map::new();
                    fields.insert(
                        "outcome".to_owned(),
                        json!({"const": branch.name(), "type": "string"}),
                    );
                    let mut required = vec![Value::String("outcome".to_owned())];
                    for field in branch.fields() {
                        fields.insert(field.name().to_owned(), mcp_type_schema(field.value_type()));
                        required.push(Value::String(field.name().to_owned()));
                    }
                    json!({
                        "type": "object",
                        "additionalProperties": false,
                        "properties": fields,
                        "required": required,
                    })
                })
                .collect::<Vec<_>>();
            let mut business_result = json!({
                "$schema": MCP_SCHEMA_DIALECT,
                "oneOf": branches,
            });
            let result = if pagination.is_some() {
                business_result
                    .as_object_mut()
                    .expect("generated business result is an object")
                    .remove("$schema");
                json!({
                    "$schema": MCP_SCHEMA_DIALECT,
                    "type": "object",
                    "additionalProperties": false,
                    "properties": {
                        "result": business_result,
                        "page": {
                            "type": "object",
                            "additionalProperties": false,
                            "properties": {
                                "next_cursor": mcp_nullable_cursor_schema(),
                            },
                            "required": ["next_cursor"],
                        },
                    },
                    "required": ["result", "page"],
                    "x-riffdb-resultProfile": "paginated-envelope-v1",
                })
            } else {
                business_result
            };
            let input_schema =
                serde_json::to_string(&input).map_err(|_| McpToolGenerationError::InvalidSchema)?;
            let result_schema = serde_json::to_string(&result)
                .map_err(|_| McpToolGenerationError::InvalidSchema)?;
            if input_schema.len() > 65_536 || result_schema.len() > 65_536 {
                return Err(McpToolGenerationError::InvalidSchema);
            }
            Ok(GeneratedMcpTool {
                operation_name: query.name().to_owned(),
                title: format!("Run {}", query.name()),
                description: format!(
                    "Execute the exact {} named query from immutable module {}.",
                    query.name(),
                    module.name().as_str()
                ),
                name,
                input_schema,
                result_schema,
                module_hash: *module.identity().as_bytes(),
            })
        })
        .collect()
}

pub(crate) fn generated_query_driver_operations(
    module: &QueryModule,
) -> Result<BTreeMap<String, (String, String)>, McpToolGenerationError> {
    let mut generated_names = BTreeSet::new();
    module
        .queries()
        .iter()
        .map(|query| {
            let generated_name =
                format!("{}_{}", snake(module.name().as_str()), snake(query.name()));
            if !generated_names.insert(generated_name.clone()) {
                return Err(McpToolGenerationError::NameCollision);
            }
            let input = serde_json::to_string(&mcp_query_input_schema(
                query,
                mcp_pagination_parameter(query),
            ))
            .map_err(|_| McpToolGenerationError::InvalidSchema)?;
            Ok((
                query.name().to_owned(),
                (generated_name, json_schema_hash(&input)),
            ))
        })
        .collect()
}

fn mcp_pagination_parameter(query: &crate::CompiledNamedQuery) -> Option<&str> {
    if !query.plan().secret_outputs().is_empty() {
        return None;
    }
    query
        .plan()
        .schemas()
        .parameters()
        .iter()
        .find(|parameter| is_cursor_type(parameter.value_type()))
        .map(|parameter| parameter.name())
}

fn mcp_query_input_schema(query: &crate::CompiledNamedQuery, pagination: Option<&str>) -> Value {
    let schemas = query.plan().schemas();
    let mut properties = Map::new();
    let mut required = Vec::new();
    for parameter in schemas.parameters() {
        properties.insert(
            parameter.name().to_owned(),
            mcp_query_parameter_schema(query, parameter),
        );
        if !parameter.has_default()
            && !is_cursor_type(parameter.value_type())
            && !is_optional_type(parameter.value_type())
        {
            required.push(Value::String(parameter.name().to_owned()));
        }
    }
    let mut input = json!({
        "$schema": MCP_SCHEMA_DIALECT,
        "type": "object",
        "additionalProperties": false,
        "properties": properties,
        "required": required,
    });
    if let Some(parameter) = pagination {
        input
            .as_object_mut()
            .expect("generated input is an object")
            .insert(
                "x-riffdb-continuationParameter".to_owned(),
                Value::String(parameter.to_owned()),
            );
    }
    input
}

fn mcp_cursor_schema() -> Value {
    json!({
        "type": "string",
        "pattern": "^[A-Za-z0-9_-]{22}$",
        "minLength": 22,
        "maxLength": 22,
    })
}

fn mcp_nullable_cursor_schema() -> Value {
    json!({
        "anyOf": [mcp_cursor_schema(), {"type": "null"}],
    })
}

/// Generates deterministic mutating MCP operation artifacts for every command.
pub fn generate_mcp_commands(
    module: &QueryModule,
    contract: &ContractBundle,
) -> Result<Vec<GeneratedMcpCommand>, McpToolGenerationError> {
    let mut commands = contract
        .commands()
        .iter()
        .filter(|command| !command.is_reimport())
        .collect::<Vec<_>>();
    commands.sort_by(|left, right| left.name().cmp(right.name()));
    commands
        .into_iter()
        .map(|command| {
            let input_properties = command
                .input()
                .record()
                .fields()
                .iter()
                .map(|field| {
                    let mut schema = mcp_contract_type_schema(field.value_type(), contract);
                    if let Some(expansion) = command
                        .collection_expansion()
                        .filter(|expansion| expansion.input_field() == field.id())
                    {
                        let object = schema
                            .as_object_mut()
                            .expect("validated collection input has an object schema");
                        object.insert(
                            "minItems".to_owned(),
                            Value::from(expansion.minimum_elements()),
                        );
                        object.insert(
                            "maxItems".to_owned(),
                            Value::from(expansion.maximum_elements()),
                        );
                        if let Some(maximum) = expansion.maximum_aggregate_element_bytes() {
                            object.insert(
                                "x-riffdb-aggregateCanonicalElementBytes".to_owned(),
                                Value::from(maximum),
                            );
                        }
                    }
                    (field.name().to_owned(), schema)
                })
                .collect::<Map<_, _>>();
            let input_required = command
                .input()
                .record()
                .fields()
                .iter()
                .map(|field| Value::String(field.name().to_owned()))
                .collect::<Vec<_>>();
            let outcomes = command
                .outcomes()
                .iter()
                .map(|outcome| {
                    let mut properties = outcome
                        .payload()
                        .fields()
                        .iter()
                        .map(|field| {
                            (
                                field.name().to_owned(),
                                mcp_contract_type_schema(field.value_type(), contract),
                            )
                        })
                        .collect::<Map<_, _>>();
                    properties.insert(
                        "outcome".to_owned(),
                        json!({"const": outcome.name(), "type": "string"}),
                    );
                    let mut required = outcome
                        .payload()
                        .fields()
                        .iter()
                        .map(|field| Value::String(field.name().to_owned()))
                        .collect::<Vec<_>>();
                    required.push(Value::String("outcome".to_owned()));
                    json!({
                        "additionalProperties": false,
                        "properties": properties,
                        "required": required,
                        "type": "object",
                    })
                })
                .collect::<Vec<_>>();
            let secret_outputs = command_secret_outputs(command, contract)
                .into_iter()
                .map(|output| {
                    json!({
                        "outcome": output.outcome,
                        "field": output.field,
                        "entity": output.entity,
                        "sourceField": output.source_field,
                    })
                })
                .collect::<Vec<_>>();
            let mut result_schema = json!({
                "$schema": MCP_SCHEMA_DIALECT,
                "oneOf": outcomes,
            });
            if !secret_outputs.is_empty() {
                result_schema
                    .as_object_mut()
                    .expect("generated result schema object")
                    .insert(
                        "x-riffdb-secretOutputs".to_owned(),
                        Value::Array(secret_outputs),
                    );
            }
            let input = contract
                .schema_artifacts()
                .iter()
                .find(|artifact| {
                    artifact.key() == SchemaArtifactKey::CommandInput(command.command_id())
                })
                .ok_or(McpToolGenerationError::InvalidSchema)?;
            let outcome = contract
                .schema_artifacts()
                .iter()
                .find(|artifact| {
                    artifact.key() == SchemaArtifactKey::CommandOutcomeUnion(command.command_id())
                })
                .ok_or(McpToolGenerationError::InvalidSchema)?;
            let name = contract
                .mcp_command_names()
                .get(command.command_id())
                .filter(|entry| entry.source_command_name() == command.name())
                .ok_or(McpToolGenerationError::InvalidSchema)?;
            Ok(GeneratedMcpCommand {
                command_id: command.command_id().get(),
                operation_name: command.name().to_owned(),
                name: format!(
                    "{}_{}",
                    snake(module.name().as_str()),
                    snake(command.name())
                ),
                title: format!("Run {}", command.name()),
                description: format!(
                    "Execute the exact {} compiled command from contract {}.",
                    command.name(),
                    contract.lineage().as_str()
                ),
                input_schema: serde_json::to_string(&json!({
                    "$schema": MCP_SCHEMA_DIALECT,
                    "additionalProperties": false,
                    "properties": input_properties,
                    "required": input_required,
                    "type": "object",
                }))
                .map_err(|_| McpToolGenerationError::InvalidSchema)?,
                result_schema: serde_json::to_string(&result_schema)
                    .map_err(|_| McpToolGenerationError::InvalidSchema)?,
                compiler_input_schema: input.canonical_json().to_owned(),
                compiler_outcome_schema: outcome.canonical_json().to_owned(),
                input_schema_hash: *input.hash().as_bytes(),
                outcome_schema_hash: *outcome.hash().as_bytes(),
                mcp_tool_name: name.tool_name().as_str().to_owned(),
                contract_bundle_hash: *contract.bundle_hash().as_bytes(),
                plan_hash: *command.plan_hash().as_bytes(),
            })
        })
        .collect()
}

fn json_schema_hash(schema: &str) -> String {
    let value: Value = serde_json::from_str(schema).expect("generated JSON schema");
    let canonical = serde_json::to_vec(&value).expect("generated JSON schema is serializable");
    let digest: [u8; 32] = Sha256::digest(canonical).into();
    hex(&digest)
}

fn mcp_contract_type_schema(value_type: &ValueType, contract: &ContractBundle) -> Value {
    if let Some(inner) = value_type.optional_inner() {
        return json!({"anyOf": [mcp_contract_type_schema(inner, contract), {"type": "null"}]});
    }
    if let Some((inner, maximum)) = value_type.list_parts() {
        return json!({
            "items": mcp_contract_type_schema(inner, contract),
            "maxItems": maximum,
            "type": "array",
        });
    }
    match value_type.tag() {
        ValueTypeTag::Bool => json!({"type": "boolean"}),
        ValueTypeTag::I64 | ValueTypeTag::U64 => json!({"type": "integer"}),
        ValueTypeTag::Bytes => json!({"contentEncoding": "base64", "type": "string"}),
        ValueTypeTag::Record => match value_type.record_ref() {
            Some(RecordTypeRef::Entity(entity_id)) => {
                let entity = contract
                    .schema()
                    .entity(*entity_id)
                    .expect("validated record entity");
                let properties = entity
                    .record()
                    .fields()
                    .iter()
                    .map(|field| {
                        (
                            field.name().to_owned(),
                            mcp_contract_type_schema(field.value_type(), contract),
                        )
                    })
                    .collect::<Map<_, _>>();
                let required = entity
                    .record()
                    .fields()
                    .iter()
                    .map(|field| Value::String(field.name().to_owned()))
                    .collect::<Vec<_>>();
                json!({
                    "additionalProperties": false,
                    "properties": properties,
                    "required": required,
                    "type": "object",
                })
            }
            _ => json!({"type": "object"}),
        },
        ValueTypeTag::Vector => {
            let dimension = value_type
                .vector_dimension()
                .expect("vector dimension")
                .get();
            json!({
                "type": "array",
                "items": {"type": "number"},
                "minItems": dimension,
                "maxItems": dimension,
            })
        }
        _ => json!({"type": "string"}),
    }
}

fn mcp_type_schema(value_type: &NamedTypeSchema) -> Value {
    match value_type {
        NamedTypeSchema::Scalar(name) => match name.as_str() {
            "Bool" => json!({"type": "boolean"}),
            "I64" | "U64" => json!({"type": "integer"}),
            "Bytes" => json!({"contentEncoding": "base64", "type": "string"}),
            value if value.starts_with("vector<") => {
                let dimension = vector_type_dimension(value).expect("vector dimension");
                json!({"type":"array", "items":{"type":"number"}, "minItems":dimension, "maxItems":dimension})
            }
            _ => json!({"type": "string"}),
        },
        NamedTypeSchema::Optional(inner) => {
            json!({"anyOf": [mcp_type_schema(inner), {"type": "null"}]})
        }
        NamedTypeSchema::Set(inner) => {
            json!({
                "type": "array",
                "items": mcp_type_schema(inner),
                "maxItems": riffdb_query_ir::MAX_EXACT_SET_VALUES_V1,
                "uniqueItems": true
            })
        }
        NamedTypeSchema::BoundedSet { element, maximum } => {
            json!({
                "type": "array",
                "items": mcp_type_schema(element),
                "maxItems": maximum,
                "uniqueItems": true
            })
        }
        NamedTypeSchema::List { element, maximum } => {
            let maximum = match maximum {
                PageBound::Literal(maximum) => *maximum,
                // Parameterized page bound is capped by the continuation-aware max take.
                PageBound::Parameter(_) => max_query_page_take(),
                PageBound::BoundedParameter { maximum, .. } => *maximum,
            };
            json!({"type": "array", "items": mcp_type_schema(element), "maxItems": maximum})
        }
        NamedTypeSchema::Record(fields) => {
            let properties = fields
                .iter()
                .map(|field| (field.name().to_owned(), mcp_type_schema(field.value_type())))
                .collect::<Map<_, _>>();
            let required = fields
                .iter()
                .map(|field| Value::String(field.name().to_owned()))
                .collect::<Vec<_>>();
            json!({
                "type": "object",
                "additionalProperties": false,
                "properties": properties,
                "required": required,
            })
        }
        NamedTypeSchema::Cursor => json!({"type": "string"}),
        NamedTypeSchema::Limit => {
            json!({"maximum": max_query_page_take(), "minimum": 1, "type": "integer"})
        }
        NamedTypeSchema::BoundedLimit { maximum } => {
            json!({"maximum": maximum, "minimum": 1, "type": "integer"})
        }
    }
}

fn mcp_query_parameter_schema(
    query: &crate::CompiledNamedQuery,
    parameter: &riffdb_query_ir::NamedParameterSchema,
) -> Value {
    if let Some(family) = query.order_family()
        && family.selector_parameter() == parameter.name()
    {
        return json!({
            "type": "string",
            "enum": family.members().iter().map(|member| member.variant_name()).collect::<Vec<_>>(),
        });
    }
    if matches!(parameter.value_type(), NamedTypeSchema::Cursor) {
        return mcp_cursor_schema();
    }
    if matches!(
        parameter.value_type(),
        NamedTypeSchema::Optional(inner) if matches!(inner.as_ref(), NamedTypeSchema::Cursor)
    ) {
        return mcp_nullable_cursor_schema();
    }
    mcp_type_schema(parameter.value_type())
}

fn is_cursor_type(value_type: &NamedTypeSchema) -> bool {
    matches!(value_type, NamedTypeSchema::Cursor)
        || matches!(
            value_type,
            NamedTypeSchema::Optional(inner)
                if matches!(inner.as_ref(), NamedTypeSchema::Cursor)
        )
}

fn is_optional_type(value_type: &NamedTypeSchema) -> bool {
    matches!(value_type, NamedTypeSchema::Optional(_))
}

pub(crate) fn rust_generation_model(
    module: &QueryModule,
    contract: &ContractBundle,
    reactive_modules: &[ReactiveModulePlanV1],
) -> Value {
    let queries = module.queries().iter().map(|query| {
        let name = query.name();
        let schemas = query.plan().schemas();
        let order_selector = query.order_family().and_then(|family| {
            schemas.parameters().iter()
                .find(|parameter| parameter.name() == family.selector_parameter())
                .and_then(|parameter| {
                    if let NamedTypeSchema::Scalar(enumeration) = parameter.value_type() {
                        Some((parameter.name(), enumeration.as_str()))
                    } else {
                        None
                    }
                })
        });
        let params = rust_fields_model(
            &format!("{name}Params"),
            schemas.parameters().iter().map(|parameter| (parameter.name(), parameter.value_type())).collect(),
            false,
            order_selector,
        );
        let redacted = !query.plan().secret_outputs().is_empty();
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
        ).map(|shape| rust_compact_model(name, &shape, contract));
        let cursor = schemas.parameters().iter()
            .find(|parameter| is_cursor_type(parameter.value_type()))
            .map(|parameter| {
                let field = rust_identifier(parameter.name());
                json!({
                    "expression": if matches!(parameter.value_type(), NamedTypeSchema::Cursor) {
                        format!("Some(self.0.{field})")
                    } else {
                        format!("self.0.{field}")
                    },
                })
            });
        json!({
            "name": name,
            "constant": screaming_snake(name),
            "plan_hash_bytes": query.plan().identity().as_bytes().iter().map(|byte| format!("0x{byte:02x}")).collect::<Vec<_>>().join(", "),
            "secret_outputs": query.plan().secret_outputs().iter().map(|secret| json!({
                "query": format!("{:?}", name),
                "entity": format!("{:?}", secret.entity()),
                "field": format!("{:?}", secret.field()),
            })).collect::<Vec<_>>(),
            "params": params,
            "limit_checks": schemas.parameters().iter().filter_map(|parameter| {
                let NamedTypeSchema::BoundedLimit { maximum } = parameter.value_type() else {
                    return None;
                };
                Some(json!({"field": rust_identifier(parameter.name()), "maximum": maximum}))
            }).collect::<Vec<_>>(),
            "cursor": cursor,
            "encoded_parameters": schemas.parameters().iter()
                .filter(|parameter| !is_cursor_type(parameter.value_type()))
                .map(|parameter| {
                    let access = format!("self.0.{}", rust_identifier(parameter.name()));
                    let expression = if order_selector.map(|(selector, _)| selector) == Some(parameter.name()) {
                        format!("ApplicationValue::Enum({access}.as_str().to_owned())")
                    } else {
                        rust_encode_application_expr(parameter.value_type(), &access)
                    };
                    json!({"wire_name": parameter.name(), "expression": expression})
                }).collect::<Vec<_>>(),
            "results": schemas.results().iter().map(|branch| {
                let variant = pascal(branch.name());
                json!({
                    "variant": variant,
                    "wire_name": branch.name(),
                    "struct": rust_fields_model(
                        &format!("{name}{variant}"),
                        branch.fields().iter().map(|field| (field.name(), field.value_type())).collect(),
                        redacted,
                        None,
                    ),
                    "decodes": branch.fields().iter().map(|field| json!({
                        "field": rust_identifier(field.name()),
                        "expression": rust_decode_top_expression(
                            field.value_type(),
                            &format!("{name}{variant}{}", pascal(field.name())),
                            field.name(),
                            "response.fields",
                        ),
                    })).collect::<Vec<_>>(),
                    "record_decoders": branch.fields().iter().flat_map(|field| {
                        rust_query_decoder_models(
                            &format!("{name}{variant}{}", pascal(field.name())),
                            field.value_type(),
                        )
                    }).collect::<Vec<_>>(),
                })
            }).collect::<Vec<_>>(),
            "compact": compact,
        })
    }).collect::<Vec<_>>();
    let entities = contract
        .schema()
        .entities()
        .iter()
        .map(|entity| {
            json!({
                "name": entity.name(),
                "function": snake(entity.name()),
                "fields": wire_model_fields(entity.record()).map(|field| json!({
                    "name": rust_identifier(field.name()),
                    "type": rust_contract_type(field.value_type(), contract),
                    "id": field.id().get(),
                    "encode": rust_encode_wire_expr(
                        field.value_type(),
                        &format!("&value.{}", rust_identifier(field.name())),
                        contract,
                    ),
                    "decode": rust_decode_wire_expr(
                        field.value_type(),
                        &format!("take_wire_field(&mut fields, {})?", field.id().get()),
                        contract,
                    ),
                })).collect::<Vec<_>>(),
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
        .map(|command| rust_command_model(command, contract))
        .collect::<Vec<_>>();
    let vector_inspections = vector_inspection_facades(module, contract)
        .into_iter()
        .flat_map(|inspection| {
            let function = format!(
                "inspect_{}_{}",
                snake(&inspection.entity),
                snake(&inspection.field)
            );
            let partition_type = rust_contract_type(&inspection.partition_type, contract);
            let partition =
                rust_application_partition_expr(&inspection.partition_type, "partition");
            [
                ("staleness", "VectorStateInspectionKind::Stale"),
                ("model_versions", "VectorStateInspectionKind::OutdatedModel"),
            ]
            .into_iter()
            .map(move |(suffix, kind)| {
                json!({
                        "function": function,
                        "suffix": suffix,
                        "kind": kind,
                        "partition_type": partition_type,
                        "partition": partition,
                "entity": inspection.entity,
                "field": inspection.field,
                "entity_literal": format!("{:?}", inspection.entity),
                "field_literal": format!("{:?}", inspection.field),
                    })
            })
        })
        .collect::<Vec<_>>();
    json!({
        "module_hash_bytes": module.identity().as_bytes().iter().map(|byte| format!("0x{byte:02x}")).collect::<Vec<_>>().join(", "),
        "contract_hash_bytes": module.contract_hash().as_bytes().iter().map(|byte| format!("0x{byte:02x}")).collect::<Vec<_>>().join(", "),
        "enums": if module.queries().iter().any(|query| query.order_family().is_some()) {
            contract.schema().enums().iter().map(|enumeration| json!({
                "name": pascal(enumeration.name()),
                "variants": enumeration.variants().iter().map(|variant| json!({
                    "name": pascal(variant.name()),
                    "wire_name": format!("{:?}", variant.name()),
                })).collect::<Vec<_>>(),
            })).collect::<Vec<_>>()
        } else {
            Vec::new()
        },
        "queries": queries,
        "entities": entities,
        "commands": commands,
        "client": {
            "name": format!("{}Client", pascal(module.contract_lineage().as_str())),
            "queries": module.queries().iter().map(|query| json!({
                "name": query.name(),
                "function": snake(query.name()),
            })).collect::<Vec<_>>(),
            "vector_inspections": vector_inspections,
        },
        "reactive_modules": rust_reactive_generation_model(
            module,
            contract,
            reactive_modules,
        ),
    })
}

fn rust_reactive_generation_model(
    module: &QueryModule,
    contract: &ContractBundle,
    reactive_modules: &[ReactiveModulePlanV1],
) -> Vec<Value> {
    let client_name = format!("{}Client", pascal(module.contract_lineage().as_str()));
    reactive_modules
        .iter()
        .map(|reactive| {
            let operations = reactive
                .operations()
                .iter()
                .map(|operation| {
                    let name = pascal(operation.name().as_str());
                    let method = snake(operation.name().as_str());
                    match operation.plan() {
                        ReactiveOperationPlanV1::Stream {
                            parameters, events, ..
                        } => json!({
                            "kind": "stream",
                            "name": name,
                            "method": method,
                            "wire_name": format!("{:?}", operation.name().as_str()),
                            "parameters": rust_reactive_parameters_model(
                                parameters,
                                "self.parameters.",
                                contract,
                            ),
                            "events": events.iter().map(|event| json!({
                                "name": pascal(event.name()),
                                "wire_name": format!("{:?}", event.name()),
                                "fields": event.fields().iter().map(|field| json!({
                                    "name": rust_identifier(field.name()),
                                    "wire_name": format!("{:?}", field.name()),
                                    "type": rust_reactive_type(field.type_name(), contract),
                                    "decoder": rust_reactive_decoder(field.type_name(), contract),
                                })).collect::<Vec<_>>(),
                            })).collect::<Vec<_>>(),
                        }),
                        ReactiveOperationPlanV1::Watch {
                            parameters, query, ..
                        } => json!({
                            "kind": "watch",
                            "name": name,
                            "method": method,
                            "wire_name": format!("{:?}", operation.name().as_str()),
                            "parameters": rust_reactive_parameters_model(
                                parameters,
                                "self.",
                                contract,
                            ),
                            "query": pascal(query.query_name()),
                        }),
                        ReactiveOperationPlanV1::Subscription {
                            parameters,
                            stream_name,
                            reactions,
                            ..
                        } => json!({
                            "kind": "subscription",
                            "name": name,
                            "method": method,
                            "wire_name": format!("{:?}", operation.name().as_str()),
                            "parameters": rust_reactive_parameters_model(
                                parameters,
                                "self.parameters.",
                                contract,
                            ),
                            "stream": pascal(stream_name.as_str()),
                            "reactions": reactions.iter().map(|reaction| json!({
                                "method": snake(reaction.reaction_name()),
                                "wire_name": format!("{:?}", reaction.reaction_name()),
                                "command": reaction.command_name(),
                                "command_wire_name": format!("{:?}", reaction.command_name()),
                            })).collect::<Vec<_>>(),
                        }),
                    }
                })
                .collect::<Vec<_>>();
            json!({
                "constant": screaming_snake(reactive.name()),
                "hash_bytes": reactive.identity().as_bytes().iter().map(u8::to_string).collect::<Vec<_>>().join(", "),
                "client_name": client_name,
                "operations": operations,
            })
        })
        .collect()
}

fn rust_reactive_parameters_model(
    parameters: &[riffdb_query_ir::ReactiveParameterV1],
    access_prefix: &str,
    contract: &ContractBundle,
) -> Value {
    json!({
        "fields": parameters.iter().map(|parameter| json!({
            "name": rust_identifier(parameter.name()),
            "type": rust_reactive_type(parameter.type_name(), contract),
            "wire_name": format!("{:?}", parameter.name()),
            "encode": rust_reactive_application_value(
                parameter.type_name(),
                &format!("{access_prefix}{}", rust_identifier(parameter.name())),
                contract,
            ),
        })).collect::<Vec<_>>(),
    })
}

fn rust_command_model(command: &CommandPlan, contract: &ContractBundle) -> Value {
    let name = command.name();
    let input_name = format!("{name}Input");
    let secret_outputs = command_secret_outputs(command, contract);
    let redacted = !secret_outputs.is_empty();
    let workflow_revisions = workflow_revision_bindings(command);
    let idempotency_field = command
        .idempotency_input()
        .and_then(|id| command.input().record().field(id))
        .map(|field| rust_identifier(field.name()));
    let collection = command.collection_expansion().map(|expansion| {
        let field = command
            .input()
            .record()
            .field(expansion.input_field())
            .expect("validated collection input field");
        let field = rust_identifier(field.name());
        let mut collection = json!({
            "field": field,
            "lower": if expansion.minimum_elements() == 1 {
                format!("self.{field}.is_empty()")
            } else {
                format!("self.{field}.len() < {}", expansion.minimum_elements())
            },
            "maximum": expansion.maximum_elements(),
            "aggregate": expansion.maximum_aggregate_element_bytes().map(|maximum| json!({
                "maximum": maximum,
                "encode": rust_encode_wire_expr(expansion.element_type(), "value", contract),
            })),
        });
        if expansion.maximum_aggregate_element_bytes().is_some() {
            let object = collection
                .as_object_mut()
                .expect("collection generation model object");
            object.insert("wire_field".to_owned(), Value::String(field.clone()));
            object.insert(
                "individual_checks".to_owned(),
                json!(rust_collection_individual_checks(
                    expansion.element_type(),
                    field.as_str(),
                    contract,
                )),
            );
        }
        collection
    });
    let facades = embedding_command_facades(command, contract).into_iter().map(|facade| {
        let constant = screaming_snake(&facade.vector_field_name);
        json!({
            "constant": constant,
            "function": rust_identifier(&format!("for_{}", facade.vector_field_name)),
            "model_identity": format!("{:?}", facade.model_identity),
            "model_version": format!("{:?}", facade.model_version),
            "arguments": command.input().record().fields().iter().filter(|field| {
                field.name() != facade.model_input_name && field.name() != facade.version_input_name
            }).map(|field| json!({
                "name": rust_identifier(field.name()),
                "type": rust_contract_type(field.value_type(), contract),
            })).collect::<Vec<_>>(),
            "assignments": command.input().record().fields().iter().map(|field| {
                let field_name = rust_identifier(field.name());
                let value = if field.name() == facade.model_input_name {
                    format!("Self::{constant}_MODEL_IDENTITY.to_owned()")
                } else if field.name() == facade.version_input_name {
                    format!("Self::{constant}_MODEL_VERSION.to_owned()")
                } else {
                    field_name.clone()
                };
                json!({
                    "field": field_name,
                    "value": value,
                    "shorthand": field.name() != facade.model_input_name
                        && field.name() != facade.version_input_name,
                })
            }).collect::<Vec<_>>(),
            "field": rust_identifier(&facade.vector_field_name),
            "model_field": rust_identifier(&facade.model_input_name),
            "version_field": rust_identifier(&facade.version_input_name),
        })
    }).collect::<Vec<_>>();
    json!({
        "name": name,
        "function": snake(name),
        "input_name": input_name,
        "constant": screaming_snake(name),
        "plan_hash_bytes": command.plan_hash().as_bytes().iter().map(|byte| format!("0x{byte:02x}")).collect::<Vec<_>>().join(", "),
        "secret_outputs": secret_outputs.iter().map(|secret| json!({
            "outcome": format!("{:?}", secret.outcome),
            "field": format!("{:?}", secret.field),
            "entity": format!("{:?}", secret.entity),
            "source_field": format!("{:?}", secret.source_field),
        })).collect::<Vec<_>>(),
        "redacted": redacted,
        "inputs": command.input().record().fields().iter().map(|field| json!({
            "name": rust_identifier(field.name()),
            "wire_name": field.name(),
            "type": rust_contract_type(field.value_type(), contract),
            "encode": rust_encode_wire_expr(
                field.value_type(),
                &format!("&self.{}", rust_identifier(field.name())),
                contract,
            ),
        })).collect::<Vec<_>>(),
        "facades": facades,
        "collection": collection,
        "idempotency_field": idempotency_field,
        "outcomes_mutable": command.outcomes().iter().any(|outcome| !outcome.payload().fields().is_empty()),
        "outcomes": command.outcomes().iter().map(|outcome| {
            let variant = pascal(outcome.name());
            json!({
                "variant": variant,
                "wire_name": outcome.name(),
                "fields": outcome.payload().fields().iter().map(|field| json!({
                    "name": rust_identifier(field.name()),
                    "type": rust_contract_type(field.value_type(), contract),
                    "decode": rust_decode_wire_expr(
                        field.value_type(),
                        &format!("take_wire_field(&mut fields, {})?", field.id().get()),
                        contract,
                    ),
                })).collect::<Vec<_>>(),
            })
        }).collect::<Vec<_>>(),
        "workflow": {
            "success": format!("{:?}", workflow_success_outcome_name(command)),
            "revisions": workflow_revisions.iter().map(|revision| json!({
                "binding": format!("{:?}", revision.binding_name),
                "input": rust_identifier(revision.input_name),
            })).collect::<Vec<_>>(),
        },
    })
}

fn rust_collection_individual_checks(
    element_type: &ValueType,
    collection: &str,
    contract: &ContractBundle,
) -> Vec<String> {
    let Some(RecordTypeRef::Entity(entity_id)) = element_type.record_ref() else {
        return Vec::new();
    };
    let entity = contract
        .schema()
        .entity(*entity_id)
        .expect("validated collection element entity");
    entity
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
            let member = rust_identifier(field.name());
            let failure = format!(
                "GeneratedCommandError::input_budget(riffdb_client_rust::generated::GeneratedInputBudgetCause::IndividualValueBytes, {collection:?}, Some(index), Some({:?}))",
                field.name(),
            );
            Some(if optional {
                format!(
                    "if let Some(leaf) = value.{member}.as_ref() && leaf.len() > {maximum}usize {{ return Err({failure}); }}"
                )
            } else {
                format!(
                    "if value.{member}.len() > {maximum}usize {{ return Err({failure}); }}"
                )
            })
        })
        .collect()
}

fn rust_compact_model(
    query_name: &str,
    shape: &RustCompactResultShape,
    contract: &ContractBundle,
) -> Value {
    let variant = pascal(&shape.outcome);
    json!({
        "outcome": format!("{:?}", shape.outcome),
        "result_name": format!("{:?}", shape.result_name),
        "entity": format!("{:?}", shape.entity),
        "field_names": shape.fields.iter().map(|field| format!("{:?}.to_owned()", field.name)).collect::<Vec<_>>(),
        "maximum_rows": shape.maximum_rows,
        "bindings": (0..shape.fields.len()).map(|index| format!("value_{index}")).collect::<Vec<_>>(),
        "width": shape.fields.len(),
        "nested_name": format!("{query_name}{variant}{}", pascal(&shape.result_name)),
        "decodes": shape.fields.iter().enumerate().map(|(index, field)| json!({
            "field": rust_identifier(&field.name),
            "expression": rust_decode_compact_wire_expr(&field.value_type, &format!("value_{index}"), contract),
        })).collect::<Vec<_>>(),
        "result_variant": variant,
        "result_field": rust_identifier(&shape.result_name),
    })
}

fn rust_query_decoder_models(name: &str, value_type: &NamedTypeSchema) -> Vec<Value> {
    match value_type {
        NamedTypeSchema::Optional(inner)
        | NamedTypeSchema::Set(inner)
        | NamedTypeSchema::BoundedSet { element: inner, .. }
        | NamedTypeSchema::List { element: inner, .. } => rust_query_decoder_models(name, inner),
        NamedTypeSchema::Record(fields) => {
            let mut models = fields
                .iter()
                .flat_map(|field| {
                    rust_query_decoder_models(
                        &format!("{name}{}", pascal(field.name())),
                        field.value_type(),
                    )
                })
                .collect::<Vec<_>>();
            models.push(json!({
                "name": name,
                "function": snake(name),
                "fields": fields.iter().map(|field| json!({
                    "name": rust_identifier(field.name()),
                    "expression": rust_decode_record_field_expr(
                        field.value_type(),
                        field.name(),
                        &format!("{name}{}", pascal(field.name())),
                    ),
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

fn rust_fields_model(
    name: &str,
    fields: Vec<(&str, &NamedTypeSchema)>,
    redacted: bool,
    order_selector: Option<(&str, &str)>,
) -> Value {
    let nested = fields
        .iter()
        .flat_map(|(field, value_type)| {
            rust_nested_models(&format!("{name}{}", pascal(field)), value_type, redacted)
        })
        .collect::<Vec<_>>();
    json!({
        "name": name,
        "redacted": redacted,
        "nested": nested,
        "fields": fields.into_iter().map(|(field, value_type)| json!({
            "name": rust_identifier(field),
            "type": order_selector.filter(|(selector, _)| selector == &field)
                .map_or_else(
                    || rust_query_type(value_type, &format!("{name}{}", pascal(field))),
                    |(_, enumeration)| pascal(enumeration),
                ),
        })).collect::<Vec<_>>(),
    })
}

fn rust_nested_models(name: &str, value_type: &NamedTypeSchema, redacted: bool) -> Vec<Value> {
    match value_type {
        NamedTypeSchema::Optional(inner)
        | NamedTypeSchema::Set(inner)
        | NamedTypeSchema::BoundedSet { element: inner, .. }
        | NamedTypeSchema::List { element: inner, .. } => rust_nested_models(name, inner, redacted),
        NamedTypeSchema::Record(fields) => {
            let mut nested = fields
                .iter()
                .flat_map(|field| {
                    rust_nested_models(
                        &format!("{name}{}", pascal(field.name())),
                        field.value_type(),
                        redacted,
                    )
                })
                .collect::<Vec<_>>();
            nested.push(json!({
                "name": name,
                "redacted": redacted,
                "fields": fields.iter().map(|field| json!({
                    "name": rust_identifier(field.name()),
                    "type": rust_query_type(
                        field.value_type(),
                        &format!("{name}{}", pascal(field.name())),
                    ),
                })).collect::<Vec<_>>(),
            }));
            nested
        }
        NamedTypeSchema::Scalar(_)
        | NamedTypeSchema::Cursor
        | NamedTypeSchema::Limit
        | NamedTypeSchema::BoundedLimit { .. } => Vec::new(),
    }
}

/// Generates a dependency-free Rust request model for every named query and command.
#[must_use]
pub fn generate_rust_client(module: &QueryModule, contract: &ContractBundle) -> String {
    let model = generate_canonical_generation_model(module, contract, &[]);
    render_rust_client_header(&model).expect("static Rust templates and model")
}

/// Generates one complete Rust application client including exact reactive
/// stream and watch bindings.
#[must_use]
pub fn generate_rust_application_client(
    module: &QueryModule,
    contract: &ContractBundle,
    reactive_modules: &[ReactiveModulePlanV1],
) -> String {
    let model = generate_canonical_generation_model(module, contract, reactive_modules);
    render_rust_client_header(&model).expect("static Rust templates and model")
}

fn rust_reactive_type(type_name: &str, contract: &ContractBundle) -> &'static str {
    if contract
        .schema()
        .enums()
        .iter()
        .any(|value| value.name() == type_name)
    {
        return "String";
    }
    if type_name.starts_with("decimal<") {
        return "DecimalValue";
    }
    if type_name.starts_with("money<") {
        return "MoneyValue";
    }
    if type_name.starts_with("bytes<") {
        return "Vec<u8>";
    }
    if type_name.starts_with("vector<") {
        return "CanonicalVector";
    }
    if type_name.starts_with("string<") {
        return "String";
    }
    match type_name {
        "bool" => "bool",
        "i64" => "i64",
        "u64" => "u64",
        "uuid" => "String",
        "date" => "i32",
        "timestamp" => "TimestampValue",
        "limit" => "u64",
        "cursor" => "String",
        _ => "String",
    }
}

fn rust_reactive_application_value(
    type_name: &str,
    access: &str,
    contract: &ContractBundle,
) -> String {
    if let Some(enumeration) = contract
        .schema()
        .enums()
        .iter()
        .find(|value| value.name() == type_name)
    {
        let variants = enumeration
            .variants()
            .iter()
            .map(|variant| {
                format!(
                    "{:?} => ApplicationValue::EnumIdentity {{ type_id: {}, variant_id: {}, name: {access}.clone() }},",
                    variant.name(),
                    enumeration.id().get(),
                    variant.id().get()
                )
            })
            .collect::<Vec<_>>()
            .join(" ");
        return format!(
            "match {access}.as_str() {{ {variants} _ => return Err(ApplicationClientError::InvalidInput), }}"
        );
    }
    if let Some((precision, _scale)) = decimal_type_parts(type_name) {
        return format!(
            "ApplicationValue::Decimal {{ coefficient_twos_complement: {access}.coefficient_twos_complement, scale: {access}.scale, precision: Some({precision}) }}"
        );
    }
    if type_name.starts_with("money<") {
        return format!(
            "ApplicationValue::Money {{ currency: {access}.currency, amount: Box::new(ApplicationValue::Decimal {{ coefficient_twos_complement: {access}.amount.coefficient_twos_complement, scale: {access}.amount.scale, precision: Some(38) }}) }}"
        );
    }
    if type_name.starts_with("bytes<") {
        return format!("ApplicationValue::Bytes({access})");
    }
    if type_name.starts_with("vector<") {
        return format!("ApplicationValue::Vector({access})");
    }
    if type_name.starts_with("string<") || type_name == "cursor" {
        return format!("ApplicationValue::String({access})");
    }
    match type_name {
        "bool" => format!("ApplicationValue::Bool({access})"),
        "i64" => format!("ApplicationValue::I64({access})"),
        "u64" => format!("ApplicationValue::U64({access})"),
        "uuid" => format!("ApplicationValue::Uuid(ApplicationUuid::from_text({access})?)"),
        "date" => format!("ApplicationValue::Date({access})"),
        "timestamp" => format!(
            "ApplicationValue::Timestamp {{ seconds: {access}.seconds, nanos: {access}.nanos }}"
        ),
        "limit" => format!("ApplicationValue::U64({access})"),
        _ => format!("ApplicationValue::String({access})"),
    }
}

fn rust_reactive_decoder(type_name: &str, contract: &ContractBundle) -> &'static str {
    if contract
        .schema()
        .enums()
        .iter()
        .any(|value| value.name() == type_name)
    {
        return "application_enum";
    }
    if type_name.starts_with("decimal<") {
        return "application_decimal";
    }
    if type_name.starts_with("money<") {
        return "(|value| { if let ApplicationValue::Money { currency, amount } = value { Ok(MoneyValue { currency, amount: application_decimal(*amount)? }) } else { Err(ApplicationClientError::InvalidResponse) } })";
    }
    if type_name.starts_with("bytes<") {
        return "application_bytes";
    }
    if type_name.starts_with("vector<") {
        return "application_vector";
    }
    if type_name.starts_with("string<") || type_name == "cursor" {
        return "application_string";
    }
    match type_name {
        "bool" => "application_bool",
        "i64" => "application_i64",
        "u64" => "application_u64",
        "uuid" => "application_uuid",
        "date" => "application_date",
        "timestamp" => "application_timestamp",
        "limit" => "application_u64",
        _ => "application_string",
    }
}

pub(crate) struct RustCompactResultField {
    pub(crate) name: String,
    pub(crate) value_type: ValueType,
}

pub(crate) struct RustCompactResultShape {
    pub(crate) outcome: String,
    pub(crate) result_name: String,
    pub(crate) entity: String,
    pub(crate) maximum_rows: u64,
    /// Public record fields in the declared result order. Go anonymous struct
    /// identity includes field order, so direct decoders must construct this
    /// exact type even when the sealed positional cover uses another order.
    pub(crate) result_fields: Vec<RustCompactResultField>,
    pub(crate) fields: Vec<RustCompactResultField>,
}

pub(crate) fn rust_compact_result_shape(
    schemas: &NamedQuerySchemas,
    result_name: &str,
    layout: &CoveredResultLayoutV1,
    selected_fields: &[String],
    contract: &ContractBundle,
) -> Option<RustCompactResultShape> {
    let [branch] = schemas.results() else {
        return None;
    };
    let [result_field] = branch.fields() else {
        return None;
    };
    if result_field.name() != result_name {
        return None;
    }
    let NamedTypeSchema::List { element, maximum } = result_field.value_type() else {
        return None;
    };
    let NamedTypeSchema::Record(record_fields) = element.as_ref() else {
        return None;
    };
    if record_fields.len() != selected_fields.len() {
        return None;
    }
    let entity = contract
        .schema()
        .entities()
        .iter()
        .find(|entity| entity.name() == layout.entity())?;
    let mut fields = Vec::with_capacity(selected_fields.len());
    for selected in selected_fields {
        let position = layout
            .fields()
            .iter()
            .find(|field| field.name() == selected)?;
        let named = record_fields
            .iter()
            .find(|field| field.name() == position.name())?;
        let field = entity.record().field(position.internal_field_id())?;
        if field.name() != named.name() || !compact_wire_type_supported(field.value_type()) {
            return None;
        }
        fields.push(RustCompactResultField {
            name: position.name().to_owned(),
            value_type: field.value_type().clone(),
        });
    }
    let mut result_fields = Vec::with_capacity(record_fields.len());
    for named in record_fields {
        let field = entity
            .record()
            .fields()
            .iter()
            .find(|field| field.name() == named.name())?;
        if !compact_wire_type_supported(field.value_type()) {
            return None;
        }
        result_fields.push(RustCompactResultField {
            name: named.name().to_owned(),
            value_type: field.value_type().clone(),
        });
    }
    let maximum_rows = match maximum {
        PageBound::Literal(value) => *value,
        PageBound::Parameter(_) => max_query_page_take(),
        PageBound::BoundedParameter { maximum, .. } => *maximum,
    };
    Some(RustCompactResultShape {
        outcome: branch.name().to_owned(),
        result_name: result_name.to_owned(),
        entity: layout.entity().to_owned(),
        maximum_rows,
        result_fields,
        fields,
    })
}

fn compact_wire_type_supported(value_type: &ValueType) -> bool {
    if let Some(inner) = value_type.optional_inner() {
        return compact_wire_type_supported(inner);
    }
    matches!(
        value_type.tag(),
        ValueTypeTag::Bool
            | ValueTypeTag::I64
            | ValueTypeTag::U64
            | ValueTypeTag::String
            | ValueTypeTag::Timestamp
            | ValueTypeTag::Date
            | ValueTypeTag::Uuid
            | ValueTypeTag::Enum
    )
}

fn rust_decode_compact_wire_expr(
    value_type: &ValueType,
    access: &str,
    contract: &ContractBundle,
) -> String {
    if let Some(inner) = value_type.optional_inner() {
        return format!(
            "if matches!({access}.kind.as_ref(), Some(WireKind::NullValue(_))) {{ None }} else {{ Some({}) }}",
            rust_decode_compact_wire_expr(inner, access, contract)
        );
    }
    if let Some((inner, maximum)) = value_type.list_parts() {
        return format!(
            "if let Some(WireKind::ListValue(list)) = {access}.kind {{ if list.values.len() > {maximum}usize {{ return Err(ApplicationClientError::InvalidResponse); }} list.values.into_iter().map(|value| Ok({})).collect::<Result<Vec<_>, ApplicationClientError>>()? }} else {{ return Err(ApplicationClientError::InvalidResponse); }}",
            rust_decode_compact_wire_expr(inner, "value", contract)
        );
    }
    let invalid = "return Err(ApplicationClientError::InvalidResponse)";
    match value_type.tag() {
        ValueTypeTag::Bool => format!(
            "if let Some(WireKind::BoolValue(value)) = {access}.kind {{ value }} else {{ {invalid}; }}"
        ),
        ValueTypeTag::I64 => format!(
            "if let Some(WireKind::I64Value(value)) = {access}.kind {{ value }} else {{ {invalid}; }}"
        ),
        ValueTypeTag::U64 => format!(
            "if let Some(WireKind::U64Value(value)) = {access}.kind {{ value }} else {{ {invalid}; }}"
        ),
        ValueTypeTag::String => format!(
            "if let Some(WireKind::StringValue(value)) = {access}.kind {{ if value.len() > {}usize {{ {invalid}; }} value }} else {{ {invalid}; }}",
            value_type.byte_bound().expect("string bound")
        ),
        ValueTypeTag::Bytes => format!(
            "if let Some(WireKind::BytesValue(value)) = {access}.kind {{ if value.len() > {}usize {{ {invalid}; }} value }} else {{ {invalid}; }}",
            value_type.byte_bound().expect("bytes bound")
        ),
        ValueTypeTag::Timestamp => format!(
            "if let Some(WireKind::TimestampValue(value)) = {access}.kind {{ if value.nanos >= 1_000_000_000 {{ {invalid}; }} TimestampValue {{ seconds: value.seconds, nanos: value.nanos }} }} else {{ {invalid}; }}"
        ),
        ValueTypeTag::Date => format!(
            "if let Some(WireKind::DateValue(value)) = {access}.kind {{ value.days_since_unix_epoch }} else {{ {invalid}; }}"
        ),
        ValueTypeTag::Uuid => format!(
            "decode_wire_uuid({access}).map_err(|_| ApplicationClientError::InvalidResponse)?"
        ),
        ValueTypeTag::Decimal => {
            let spec = value_type.decimal_spec().expect("decimal spec");
            format!(
                "if let Some(WireKind::DecimalValue(value)) = {access}.kind {{ if value.scale != {} || value.precision != Some({}) {{ {invalid}; }} DecimalValue {{ coefficient_twos_complement: value.coefficient_twos_complement, scale: value.scale, precision: value.precision }} }} else {{ {invalid}; }}",
                spec.scale(),
                spec.precision()
            )
        }
        ValueTypeTag::Money => format!(
            "decode_wire_money({access}, {:?}).map_err(|_| ApplicationClientError::InvalidResponse)?",
            value_type.currency().expect("money currency").to_string()
        ),
        ValueTypeTag::Enum => {
            let enumeration = contract
                .schema()
                .enumeration(value_type.enum_type_id().expect("enum identity"))
                .expect("validated enum identity");
            let variants = enumeration
                .variants()
                .iter()
                .map(|variant| {
                    format!(
                        "({}, {}, {:?}) => {:?}.to_owned()",
                        enumeration.id().get(),
                        variant.id().get(),
                        variant.name(),
                        variant.name()
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "if let Some(WireKind::EnumValue(value)) = {access}.kind {{ match (value.type_id, value.variant_id, value.name.as_str()) {{ {variants}, _ => {{ {invalid}; }} }} }} else {{ {invalid}; }}"
            )
        }
        ValueTypeTag::Vector => format!(
            "if let Some(WireKind::VectorValue(value)) = {access}.kind {{ if value.components.len() != {}usize {{ {invalid}; }} CanonicalVector::new(value.components).map_err(|_| ApplicationClientError::InvalidResponse)? }} else {{ {invalid}; }}",
            value_type
                .vector_dimension()
                .expect("vector dimension")
                .get()
        ),
        ValueTypeTag::Optional | ValueTypeTag::List => unreachable!("handled above"),
        ValueTypeTag::Record => invalid.to_owned(),
    }
}

#[cfg(test)]
fn emit_typescript_compact_query_decoder(
    output: &mut String,
    query_name: &str,
    shape: &RustCompactResultShape,
    contract: &ContractBundle,
) {
    let function = format!("decode{}Compact", pascal(query_name));
    let fields = shape
        .fields
        .iter()
        .map(|field| format!("{:?}", field.name))
        .collect::<Vec<_>>()
        .join(", ");
    writeln!(
        output,
        "function {function}(value: CompactNamedQueryResult): {query_name}Result {{\n\
           if (value.outcome !== {outcome:?} || value.resultName !== {result_name:?} || value.entity !== {entity:?}\n\
               || value.fields.length !== {width} || value.fields.some((field, index) => field !== [{fields}][index])\n\
               || value.rows.length > {maximum_rows}) throw new Error(\"invalid RiffDB compact result\");\n\
           return {{ outcome: {outcome:?}, {result}: value.rows.map((row) => {{\n\
             if (row.length !== {width}) throw new Error(\"invalid RiffDB compact result\");\n\
             return {{",
        outcome = shape.outcome,
        result_name = shape.result_name,
        entity = shape.entity,
        width = shape.fields.len(),
        maximum_rows = shape.maximum_rows,
        result = ts_identifier(&shape.result_name),
    )
    .infallible();
    for (index, field) in shape.fields.iter().enumerate() {
        let expression =
            ts_decode_compact_wire_expr(&field.value_type, &format!("row[{index}]!"), contract);
        writeln!(
            output,
            "      {}: {expression},",
            ts_identifier(&field.name)
        )
        .infallible();
    }
    writeln!(output, "    }};\n  }}) }};\n}}\n").infallible();
}

#[cfg(test)]
#[allow(dead_code)]
fn emit_typescript_packed_query_decoder(
    output: &mut String,
    query_name: &str,
    shape: &RustCompactResultShape,
    contract: &ContractBundle,
) {
    let function = format!("decode{}Packed", pascal(query_name));
    let fields = shape
        .fields
        .iter()
        .map(|field| format!("{:?}", field.name))
        .collect::<Vec<_>>()
        .join(", ");
    writeln!(output, "function {function}(value: PackedNamedQueryResult): {query_name}Result {{\n  if (value.outcome !== {outcome:?} || value.resultName !== {result_name:?} || value.entity !== {entity:?}\n      || value.fields.length !== {width} || value.fields.some((field, index) => field !== [{fields}][index])\n      || value.columns.length !== {width} || value.rowCount > {maximum_rows}) throw new Error(\"invalid RiffDB packed result\");\n  for (const column of value.columns) {{ if (column.offsets.length !== value.rowCount + 1 || column.offsets[0] !== 0 || column.offsets.at(-1) !== column.data.length || column.offsets.some((offset, index) => !Number.isInteger(offset) || offset < 0 || offset > column.data.length || (index > 0 && column.offsets[index - 1]! > offset))) throw new Error(\"invalid RiffDB packed result\"); }}\n  const rows = [];\n  for (let row = 0; row < value.rowCount; row += 1) {{ rows.push({{", outcome=shape.outcome, result_name=shape.result_name, entity=shape.entity, width=shape.fields.len(), maximum_rows=shape.maximum_rows).infallible();
    for (index, field) in shape.fields.iter().enumerate() {
        let expression = ts_decode_packed_expr(
            &field.value_type,
            &format!("packedCell(value, {index}, row)"),
            contract,
        );
        writeln!(output, "    {}: {expression},", ts_identifier(&field.name)).infallible();
    }
    writeln!(
        output,
        "  }}); }}\n  return {{ outcome: {:?}, {}: rows }};\n}}\n",
        shape.outcome,
        ts_identifier(&shape.result_name)
    )
    .infallible();
}

#[allow(dead_code)]
#[cfg(test)]
fn ts_decode_packed_expr(
    value_type: &ValueType,
    access: &str,
    contract: &ContractBundle,
) -> String {
    if let Some(inner) = value_type.optional_inner() {
        return format!(
            "(() => {{ const raw = {access}; return raw.length === 2 && raw[0] === 1 && raw[1] === 0 ? null : {}; }})()",
            ts_decode_packed_expr(inner, "raw", contract)
        );
    }
    match value_type.tag() {
        ValueTypeTag::Bool => format!(
            "(() => {{ const raw = {access}; packedTag(raw, 1, 3); if (raw[2] !== 0 && raw[2] !== 1) throw new Error(\"invalid RiffDB packed bool\"); return raw[2] === 1; }})()"
        ),
        ValueTypeTag::I64 => format!("packedI64({access}, 2)"),
        ValueTypeTag::U64 => format!("packedI64({access}, 3)"),
        ValueTypeTag::String => format!(
            "packedString({access}, {})",
            value_type.byte_bound().expect("string bound")
        ),
        ValueTypeTag::Timestamp => format!(
            "(() => {{ const view = packedTag({access}, 8, 14); const nanos = view.getUint32(10); if (nanos >= 1_000_000_000) throw new Error(\"invalid RiffDB packed timestamp\"); return {{ seconds: view.getBigInt64(2), nanos }}; }})()"
        ),
        ValueTypeTag::Date => format!("packedTag({access}, 9, 6).getInt32(2)"),
        ValueTypeTag::Uuid => format!("packedUuid({access})"),
        ValueTypeTag::Enum => {
            let enumeration = contract
                .schema()
                .enumeration(value_type.enum_type_id().expect("enum identity"))
                .expect("validated enum identity");
            let cases = enumeration
                .variants()
                .iter()
                .map(|variant| format!("case {}: return {:?};", variant.id().get(), variant.name()))
                .collect::<Vec<_>>()
                .join(" ");
            format!(
                "(() => {{ const view = packedTag({access}, 11, 10); if (view.getUint32(2) !== {}) throw new Error(\"invalid RiffDB packed enum\"); switch (view.getUint32(6)) {{ {cases} default: throw new Error(\"invalid RiffDB packed enum\"); }} }})()",
                enumeration.id().get()
            )
        }
        ValueTypeTag::Optional => unreachable!("handled above"),
        _ => "(() => { throw new Error(\"unsupported RiffDB packed value\"); })()".to_owned(),
    }
}

fn ts_decode_compact_wire_expr(
    value_type: &ValueType,
    access: &str,
    contract: &ContractBundle,
) -> String {
    if let Some(inner) = value_type.optional_inner() {
        return format!(
            "({access}.type === \"null\" ? null : {})",
            ts_decode_compact_wire_expr(inner, access, contract)
        );
    }
    match value_type.tag() {
        ValueTypeTag::Bool => format!(
            "(() => {{ const value = compactPayload({access}, \"bool\"); if (typeof value !== \"boolean\") throw new Error(\"invalid RiffDB compact value\"); return value; }})()"
        ),
        ValueTypeTag::I64 => format!("compactInteger({access}, \"i64\")"),
        ValueTypeTag::U64 => format!("compactInteger({access}, \"u64\")"),
        ValueTypeTag::String => format!(
            "compactString({access}, \"string\", {})",
            value_type.byte_bound().expect("string bound")
        ),
        ValueTypeTag::Timestamp => format!("compactTimestamp({access})"),
        ValueTypeTag::Date => format!(
            "(() => {{ const text = compactString({access}, \"date\", 12); if (!/^-?(?:0|[1-9][0-9]*)$/.test(text)) throw new Error(\"invalid RiffDB compact value\"); const value = Number(text); if (!Number.isInteger(value) || value < -2147483648 || value > 2147483647) throw new Error(\"invalid RiffDB compact value\"); return value; }})()"
        ),
        ValueTypeTag::Uuid => format!(
            "(() => {{ const value = compactString({access}, \"uuid\", 36); if (!/^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$/.test(value)) throw new Error(\"invalid RiffDB compact value\"); return value; }})()"
        ),
        ValueTypeTag::Enum => {
            let enumeration = contract
                .schema()
                .enumeration(value_type.enum_type_id().expect("enum identity"))
                .expect("validated enum identity");
            let variants = enumeration
                .variants()
                .iter()
                .map(|variant| format!("{:?}", variant.name()))
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "(() => {{ const value = compactString({access}, \"enum\", 256); if (![{variants}].includes(value)) throw new Error(\"invalid RiffDB compact value\"); return value; }})()"
            )
        }
        ValueTypeTag::Optional => unreachable!("handled above"),
        ValueTypeTag::Bytes
        | ValueTypeTag::Decimal
        | ValueTypeTag::Money
        | ValueTypeTag::List
        | ValueTypeTag::Record
        | ValueTypeTag::Vector => {
            "(() => { throw new Error(\"invalid RiffDB compact value\"); })()".to_owned()
        }
    }
}

fn rust_decode_record_field_expr(
    value_type: &NamedTypeSchema,
    field_name: &str,
    nested_name: &str,
) -> String {
    if let NamedTypeSchema::Optional(inner) = value_type
        && matches!(inner.as_ref(), NamedTypeSchema::Optional(_))
    {
        return format!(
            "match record.fields.remove(\"{field_name}\") {{ None => None, Some(value) => Some({}) }}",
            rust_decode_application_expr(inner, "value", nested_name)
        );
    }
    rust_decode_application_expr(
        value_type,
        &format!("take_application_value(&mut record.fields, \"{field_name}\")?"),
        nested_name,
    )
}

fn rust_encode_application_expr(value_type: &NamedTypeSchema, access: &str) -> String {
    match value_type {
        NamedTypeSchema::Scalar(name) => match name.as_str() {
            "bool" => format!("ApplicationValue::Bool({access})"),
            "i64" => format!("ApplicationValue::I64({access})"),
            "u64" => format!("ApplicationValue::U64({access})"),
            "uuid" => format!("ApplicationValue::Uuid(ApplicationUuid::from_text({access})?)"),
            "timestamp" => format!(
                "ApplicationValue::Timestamp {{ seconds: {access}.seconds, nanos: {access}.nanos }}"
            ),
            "date" => format!("ApplicationValue::Date({access})"),
            value if value.starts_with("bytes<") => format!("ApplicationValue::Bytes({access})"),
            value if value.starts_with("vector<") => format!("ApplicationValue::Vector({access})"),
            value if value.starts_with("decimal<") => format!(
                "ApplicationValue::Decimal {{ coefficient_twos_complement: {access}.coefficient_twos_complement, \
                 scale: {access}.scale, precision: {access}.precision }}"
            ),
            value if value.starts_with("money<") => format!(
                "ApplicationValue::Money {{ currency: {access}.currency, amount: Box::new(ApplicationValue::Decimal {{ \
                 coefficient_twos_complement: {access}.amount.coefficient_twos_complement, scale: {access}.amount.scale, \
                 precision: {access}.amount.precision }}) }}"
            ),
            value if value.starts_with("string<") => format!("ApplicationValue::String({access})"),
            _ => format!("ApplicationValue::Enum({access})"),
        },
        NamedTypeSchema::Optional(inner) => format!(
            "match {access} {{ Some(value) => {}, None => ApplicationValue::Null }}",
            rust_encode_application_expr(inner, "value")
        ),
        NamedTypeSchema::Set(inner)
        | NamedTypeSchema::BoundedSet { element: inner, .. }
        | NamedTypeSchema::List { element: inner, .. } => {
            let element = rust_encode_application_expr(inner, "value");
            if let Some(constructor) = element
                .strip_suffix("(value)")
                .filter(|constructor| constructor.starts_with("ApplicationValue::"))
            {
                format!("ApplicationValue::List({access}.into_iter().map({constructor}).collect())")
            } else {
                format!(
                    "ApplicationValue::List({access}.into_iter().map(|value| {element}).collect())"
                )
            }
        }
        NamedTypeSchema::Cursor => format!("ApplicationValue::String({access})"),
        NamedTypeSchema::Limit => format!("ApplicationValue::U64({access})"),
        NamedTypeSchema::BoundedLimit { .. } => format!("ApplicationValue::U64({access})"),
        NamedTypeSchema::Record(_) => "ApplicationValue::Null".to_owned(),
    }
}

fn rust_decode_top_expression(
    value_type: &NamedTypeSchema,
    nested_name: &str,
    field_name: &str,
    fields: &str,
) -> String {
    let take = format!("take_result_field(&mut {fields}, \"{field_name}\")?");
    match value_type {
        NamedTypeSchema::Record(_) => format!(
            "decode_{}_record(one_result_record({take})?)?",
            snake(nested_name)
        ),
        NamedTypeSchema::Optional(inner)
            if matches!(inner.as_ref(), NamedTypeSchema::Record(_)) =>
        {
            format!(
                "optional_result_record({take})?.map(decode_{}_record).transpose()?",
                snake(nested_name)
            )
        }
        NamedTypeSchema::List { element, .. }
            if matches!(element.as_ref(), NamedTypeSchema::Record(_)) =>
        {
            format!(
                "many_result_records({take})?.into_iter().map(decode_{}_record).collect::<Result<Vec<_>, _>>()?",
                snake(nested_name)
            )
        }
        _ => "return Err(ApplicationClientError::InvalidResponse)".to_owned(),
    }
}

fn rust_decode_application_expr(
    value_type: &NamedTypeSchema,
    access: &str,
    nested_name: &str,
) -> String {
    match value_type {
        NamedTypeSchema::Scalar(name) => match name.as_str() {
            "bool" => format!("application_bool({access})?"),
            "i64" => format!("application_i64({access})?"),
            "u64" => format!("application_u64({access})?"),
            "uuid" => format!("application_uuid({access})?"),
            "timestamp" => format!("application_timestamp({access})?"),
            "date" => format!("application_date({access})?"),
            value if value.starts_with("bytes<") => format!("application_bytes({access})?"),
            value if value.starts_with("vector<") => format!("application_vector({access})?"),
            value if value.starts_with("decimal<") => format!("application_decimal({access})?"),
            value if value.starts_with("money<") => format!(
                "match {access} {{ ApplicationValue::Money {{ currency, amount }} => MoneyValue {{ currency, amount: application_decimal(*amount)? }}, \
                 _ => return Err(ApplicationClientError::InvalidResponse) }}"
            ),
            value if value.starts_with("string<") => format!("application_string({access})?"),
            _ => format!("application_enum({access})?"),
        },
        NamedTypeSchema::Optional(inner) => format!(
            "match {access} {{ ApplicationValue::Null => None, value => Some({}) }}",
            rust_decode_application_expr(inner, "value", nested_name)
        ),
        NamedTypeSchema::Set(inner)
        | NamedTypeSchema::BoundedSet { element: inner, .. }
        | NamedTypeSchema::List { element: inner, .. }
            if matches!(inner.as_ref(), NamedTypeSchema::Record(_)) =>
        {
            format!(
                "application_list({access})?.into_iter().map(|value| decode_{}_record(application_record(value)?)).collect::<Result<Vec<_>, ApplicationClientError>>()?",
                snake(nested_name)
            )
        }
        NamedTypeSchema::Set(inner)
        | NamedTypeSchema::BoundedSet { element: inner, .. }
        | NamedTypeSchema::List { element: inner, .. } => format!(
            "application_list({access})?.into_iter().map(|value| Ok({})).collect::<Result<Vec<_>, ApplicationClientError>>()?",
            rust_decode_application_expr(inner, "value", nested_name)
        ),
        NamedTypeSchema::Record(_) => format!(
            "decode_{}_record(application_record({access})?)?",
            snake(nested_name)
        ),
        NamedTypeSchema::Cursor => format!("application_string({access})?"),
        NamedTypeSchema::Limit => format!("application_u64({access})?"),
        NamedTypeSchema::BoundedLimit { .. } => format!("application_u64({access})?"),
    }
}

/// Wire-model fields of an entity record. Every contract field has a distinct
/// public wire arm, including canonical vectors.
fn wire_model_fields(
    record: &riffdb_contract_ir::RecordSchema,
) -> impl Iterator<Item = &riffdb_contract_ir::FieldSchema> {
    record.fields().iter()
}

fn rust_application_partition_expr(value_type: &ValueType, access: &str) -> String {
    match value_type.tag() {
        ValueTypeTag::Bool => format!("ApplicationValue::Bool({access})"),
        ValueTypeTag::I64 => format!("ApplicationValue::I64({access})"),
        ValueTypeTag::U64 => format!("ApplicationValue::U64({access})"),
        ValueTypeTag::String => format!("ApplicationValue::String({access})"),
        ValueTypeTag::Bytes => format!("ApplicationValue::Bytes({access})"),
        ValueTypeTag::Uuid => {
            format!("ApplicationValue::Uuid(ApplicationUuid::from_text({access})?)")
        }
        ValueTypeTag::Date => format!("ApplicationValue::Date({access})"),
        ValueTypeTag::Timestamp => format!(
            "ApplicationValue::Timestamp {{ seconds: {access}.seconds, nanos: {access}.nanos }}"
        ),
        ValueTypeTag::Decimal => format!(
            "ApplicationValue::Decimal {{ coefficient_twos_complement: {access}.coefficient_twos_complement, scale: {access}.scale, precision: {access}.precision }}"
        ),
        ValueTypeTag::Money => format!(
            "ApplicationValue::Money {{ currency: {access}.currency, amount: Box::new(ApplicationValue::Decimal {{ coefficient_twos_complement: {access}.amount.coefficient_twos_complement, scale: {access}.amount.scale, precision: {access}.amount.precision }}) }}"
        ),
        ValueTypeTag::Enum => format!("ApplicationValue::Enum({access})"),
        ValueTypeTag::Vector
        | ValueTypeTag::Record
        | ValueTypeTag::Optional
        | ValueTypeTag::List => {
            panic!("checked aggregate partition must be an authoritative scalar")
        }
    }
}

fn rust_encode_wire_expr(
    value_type: &ValueType,
    access: &str,
    contract: &ContractBundle,
) -> String {
    if let Some(inner) = value_type.optional_inner() {
        return format!(
            "match {access}.as_ref() {{ Some(value) => {}, None => wire_null() }}",
            rust_encode_wire_expr(inner, "value", contract)
        );
    }
    if let Some((inner, _)) = value_type.list_parts() {
        let item = rust_encode_wire_expr(inner, "value", contract);
        let mapper = match item.strip_suffix('?') {
            Some(result) => match result.strip_suffix("(value)") {
                Some(function)
                    if function.starts_with("encode_") && function.ends_with("_entity") =>
                {
                    function.to_owned()
                }
                _ => format!("|value| {result}"),
            },
            None => format!("|value| Ok({item})"),
        };
        let iterable = access.strip_prefix('&').unwrap_or(access);
        return format!(
            "v1::Value {{ kind: Some(WireKind::ListValue(v1::ValueList {{ values: ({iterable}).iter().map({mapper}).collect::<Result<Vec<_>, GeneratedCommandError>>()? }})) }}"
        );
    }
    let copied = access
        .strip_prefix('&')
        .map_or_else(|| format!("*({access})"), ToOwned::to_owned);
    match value_type.tag() {
        ValueTypeTag::Bool => format!("wire_bool({copied})"),
        ValueTypeTag::I64 => format!("wire_i64({copied})"),
        ValueTypeTag::U64 => format!("wire_u64({copied})"),
        ValueTypeTag::Decimal => format!("wire_decimal({access})"),
        ValueTypeTag::Money => format!(
            "wire_money({access}, {:?})?",
            value_type
                .currency()
                .expect("validated money type has currency")
                .to_string()
        ),
        ValueTypeTag::String => format!("wire_string(Clone::clone({access}))"),
        ValueTypeTag::Bytes => format!("wire_bytes(Clone::clone({access}))"),
        ValueTypeTag::Timestamp => format!("wire_timestamp({access})?"),
        ValueTypeTag::Date => format!("wire_date({copied})"),
        ValueTypeTag::Uuid => format!("wire_uuid({access})?"),
        ValueTypeTag::Enum => format!("wire_enum(Clone::clone({access}))"),
        ValueTypeTag::Record => match value_type.record_ref() {
            Some(RecordTypeRef::Entity(entity_id)) => {
                let entity = contract
                    .schema()
                    .entity(*entity_id)
                    .expect("validated record entity");
                format!("encode_{}_entity({access})?", snake(entity.name()))
            }
            _ => "return Err(GeneratedCommandError::InvalidInputShape)".to_owned(),
        },
        ValueTypeTag::Vector => format!(
            "wire_vector({access}, {})?",
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

fn rust_decode_wire_expr(
    value_type: &ValueType,
    access: &str,
    contract: &ContractBundle,
) -> String {
    if let Some(inner) = value_type.optional_inner() {
        if let Some(function) = rust_decode_wire_function(inner, contract) {
            return format!("decode_wire_optional({access}, {function})?");
        }
        return format!(
            "decode_wire_optional({access}, |value| {})?",
            rust_decode_wire_result_expr(inner, "value", contract)
        );
    }
    if let Some((inner, _)) = value_type.list_parts() {
        return format!(
            "if let Some(WireKind::ListValue(list)) = {access}.kind {{ list.values.into_iter().map(|value| Ok({})).collect::<Result<Vec<_>, GeneratedCommandError>>()? }} else {{ return Err(GeneratedCommandError::InvalidOutcomeShape); }}",
            rust_decode_wire_expr(inner, "value", contract)
        );
    }
    match value_type.tag() {
        ValueTypeTag::Bool => format!("decode_wire_bool({access})?"),
        ValueTypeTag::I64 => format!("decode_wire_i64({access})?"),
        ValueTypeTag::U64 => format!("decode_wire_u64({access})?"),
        ValueTypeTag::Decimal => format!("decode_wire_decimal({access})?"),
        ValueTypeTag::Money => format!(
            "decode_wire_money({access}, {:?})?",
            value_type
                .currency()
                .expect("validated money type has currency")
                .to_string()
        ),
        ValueTypeTag::String => format!("decode_wire_string({access})?"),
        ValueTypeTag::Bytes => format!("decode_wire_bytes({access})?"),
        ValueTypeTag::Timestamp => format!("decode_wire_timestamp({access})?"),
        ValueTypeTag::Date => format!("decode_wire_date({access})?"),
        ValueTypeTag::Uuid => format!("decode_wire_uuid({access})?"),
        ValueTypeTag::Enum => format!("decode_wire_enum({access})?"),
        ValueTypeTag::Record => match value_type.record_ref() {
            Some(RecordTypeRef::Entity(entity_id)) => {
                let entity = contract
                    .schema()
                    .entity(*entity_id)
                    .expect("validated record entity");
                format!("decode_{}_entity({access})?", snake(entity.name()))
            }
            _ => "return Err(GeneratedCommandError::InvalidOutcomeShape)".to_owned(),
        },
        ValueTypeTag::Vector => format!(
            "decode_wire_vector({access}, {})?",
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

fn rust_decode_wire_function(value_type: &ValueType, contract: &ContractBundle) -> Option<String> {
    let function = match value_type.tag() {
        ValueTypeTag::Bool => "decode_wire_bool",
        ValueTypeTag::I64 => "decode_wire_i64",
        ValueTypeTag::U64 => "decode_wire_u64",
        ValueTypeTag::Decimal => "decode_wire_decimal",
        ValueTypeTag::String => "decode_wire_string",
        ValueTypeTag::Bytes => "decode_wire_bytes",
        ValueTypeTag::Timestamp => "decode_wire_timestamp",
        ValueTypeTag::Date => "decode_wire_date",
        ValueTypeTag::Uuid => "decode_wire_uuid",
        ValueTypeTag::Enum => "decode_wire_enum",
        ValueTypeTag::Record => {
            let entity = value_type.record_ref().and_then(|record| match record {
                RecordTypeRef::Entity(entity_id) => contract.schema().entity(*entity_id),
                _ => None,
            })?;
            return Some(format!("decode_{}_entity", snake(entity.name())));
        }
        ValueTypeTag::Money
        | ValueTypeTag::Optional
        | ValueTypeTag::List
        | ValueTypeTag::Vector => return None,
    };
    Some(function.to_owned())
}

fn rust_decode_wire_result_expr(
    value_type: &ValueType,
    access: &str,
    contract: &ContractBundle,
) -> String {
    let expression = rust_decode_wire_expr(value_type, access, contract);
    expression
        .strip_suffix('?')
        .map_or_else(|| format!("Ok({expression})"), str::to_owned)
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct TypescriptCompactHelperUsage {
    payload: bool,
    string: bool,
    integer: bool,
    timestamp: bool,
}

impl TypescriptCompactHelperUsage {
    fn include(&mut self, value_type: &ValueType) {
        if let Some(inner) = value_type.optional_inner() {
            self.include(inner);
            return;
        }
        match value_type.tag() {
            ValueTypeTag::Bool => self.payload = true,
            ValueTypeTag::I64 | ValueTypeTag::U64 => {
                self.payload = true;
                self.integer = true;
            }
            ValueTypeTag::String | ValueTypeTag::Date | ValueTypeTag::Uuid | ValueTypeTag::Enum => {
                self.payload = true;
                self.string = true;
            }
            ValueTypeTag::Timestamp => {
                self.payload = true;
                self.timestamp = true;
            }
            ValueTypeTag::Bytes
            | ValueTypeTag::Decimal
            | ValueTypeTag::Money
            | ValueTypeTag::List
            | ValueTypeTag::Record
            | ValueTypeTag::Vector => {}
            ValueTypeTag::Optional => unreachable!("handled above"),
        }
    }
}

fn typescript_compact_helper_usage(
    module: &QueryModule,
    contract: &ContractBundle,
) -> TypescriptCompactHelperUsage {
    let mut usage = TypescriptCompactHelperUsage::default();
    for query in module.queries() {
        let schemas = query.plan().schemas();
        let Some(shape) = query.plan().common_covered_result().and_then(
            |(result_name, layout, selected_fields)| {
                rust_compact_result_shape(
                    schemas,
                    &result_name,
                    &layout,
                    &selected_fields,
                    contract,
                )
            },
        ) else {
            continue;
        };
        for field in &shape.fields {
            usage.include(&field.value_type);
        }
    }
    usage
}

#[cfg(test)]
fn emit_typescript_compact_helpers(output: &mut String, usage: TypescriptCompactHelperUsage) {
    if usage.payload {
        output.push_str(
            r#"function compactPayload(value: CompactApplicationValue, expected: string): unknown {
  if (typeof value !== "object" || value === null || Array.isArray(value) || value.type !== expected) throw new Error("invalid RiffDB compact value");
  const keys = Object.keys(value);
  if (expected === "null") { if (keys.length !== 1) throw new Error("invalid RiffDB compact value"); return null; }
  if (keys.length !== 2 || !Object.hasOwn(value, "value")) throw new Error("invalid RiffDB compact value");
  return value.value;
}
"#,
        );
    }
    if usage.string {
        output.push_str(
            r#"function compactString(value: CompactApplicationValue, expected: string, maximum: number): string {
  const payload = compactPayload(value, expected);
  if (typeof payload !== "string" || new TextEncoder().encode(payload).length > maximum) throw new Error("invalid RiffDB compact value");
  return payload;
}
"#,
        );
    }
    if usage.integer {
        output.push_str(
            r#"function compactInteger(value: CompactApplicationValue, expected: "i64" | "u64"): bigint {
  const payload = compactPayload(value, expected);
  if (typeof payload !== "string" || !/^-?(?:0|[1-9][0-9]*)$/.test(payload)) throw new Error("invalid RiffDB compact value");
  const parsed = BigInt(payload);
  if ((expected === "i64" && (parsed < -9223372036854775808n || parsed > 9223372036854775807n))
      || (expected === "u64" && (parsed < 0n || parsed > 18446744073709551615n))) throw new Error("invalid RiffDB compact value");
  return parsed;
}
"#,
        );
    }
    if usage.timestamp {
        output.push_str(
            r#"function compactTimestamp(value: CompactApplicationValue): { readonly seconds: bigint; readonly nanos: number } {
  const payload = compactPayload(value, "timestamp");
  if (typeof payload !== "object" || payload === null || Array.isArray(payload)) throw new Error("invalid RiffDB compact value");
  const record = payload as Record<string, unknown>;
  if (Object.keys(record).length !== 2 || typeof record.seconds !== "string" || !/^-?(?:0|[1-9][0-9]*)$/.test(record.seconds)
      || !Number.isInteger(record.nanos) || (record.nanos as number) < 0 || (record.nanos as number) >= 1_000_000_000) throw new Error("invalid RiffDB compact value");
  const seconds = BigInt(record.seconds);
  if (seconds < -9223372036854775808n || seconds > 9223372036854775807n) throw new Error("invalid RiffDB compact value");
  return { seconds, nanos: record.nanos as number };
}
"#,
        );
    }
    if usage.payload {
        output.push('\n');
    }
}

/// Generates a dependency-free TypeScript request model for every named query and command.
#[must_use]
pub fn generate_typescript_client(module: &QueryModule, contract: &ContractBundle) -> String {
    let model = generate_canonical_generation_model(module, contract, &[]);
    render_typescript_client_header(&model).expect("static TypeScript template and model")
}

pub(crate) fn typescript_generation_model(
    module: &QueryModule,
    contract: &ContractBundle,
    reactive_modules: &[ReactiveModulePlanV1],
) -> Value {
    let query_driver_operations = generated_query_driver_operations(module)
        .expect("validated query module has distinct generated operation names");
    let command_driver_operations = generate_mcp_commands(module, contract)
        .expect("validated contract has distinct generated operation names")
        .into_iter()
        .map(|operation| {
            (
                operation.operation_name,
                (operation.name, json_schema_hash(&operation.input_schema)),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let vector_driver_operations = generate_vector_inspection_tools(module, contract)
        .expect("validated nearest sources have distinct inspection names")
        .into_iter()
        .map(|operation| {
            (
                (operation.entity, operation.field, operation.inspection_kind),
                (operation.name, json_schema_hash(&operation.input_schema)),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let compact_helpers = typescript_compact_helper_usage(module, contract);
    let queries = module
        .queries()
        .iter()
        .map(|query| {
            let name = query.name();
            let schemas = query.plan().schemas();
            let compact_shape = query.plan().common_covered_result().and_then(
                |(result_name, layout, selected_fields)| {
                    rust_compact_result_shape(
                        schemas,
                        &result_name,
                        &layout,
                        &selected_fields,
                        contract,
                    )
                },
            );
            let (driver_operation_name, driver_input_schema_hash) = query_driver_operations
                .get(name)
                .expect("generated query operation");
            let compact = compact_shape.as_ref().map(|shape| {
                json!({
                    "function": format!("decode{}Compact", pascal(name)),
                    "outcome": format!("{:?}", shape.outcome),
                    "result_name": format!("{:?}", shape.result_name),
                    "result_field": ts_identifier(&shape.result_name),
                    "entity": format!("{:?}", shape.entity),
                    "width": shape.fields.len(),
                    "field_literals": shape.fields.iter().map(|field| format!("{:?}", field.name)).collect::<Vec<_>>(),
                    "maximum_rows": shape.maximum_rows,
                    "decodes": shape.fields.iter().enumerate().map(|(index, field)| json!({
                        "field": ts_identifier(&field.name),
                        "expression": ts_decode_compact_wire_expr(
                            &field.value_type,
                            &format!("row[{index}]!"),
                            contract,
                        ),
                    })).collect::<Vec<_>>(),
                })
            });
            let parameter_schema = ts_named_record_schema(
                schemas
                    .parameters()
                    .iter()
                    .map(|parameter| (parameter.name(), parameter.value_type())),
                contract,
            );
            let result_schemas = Value::Object(
                schemas
                    .results()
                    .iter()
                    .map(|branch| {
                        (
                            branch.name().to_owned(),
                            ts_named_record_schema(
                                branch
                                    .fields()
                                    .iter()
                                    .map(|field| (field.name(), field.value_type())),
                                contract,
                            ),
                        )
                    })
                    .collect(),
            );
            let cursor = schemas
                .parameters()
                .iter()
                .find(|parameter| is_cursor_type(parameter.value_type()))
                .map(|parameter| json!({"field": ts_identifier(parameter.name())}));
            json!({
                "name": name,
                "function": camel(name),
                "pascal": pascal(name),
                "constant": screaming_snake(name),
                "plan_hash": hex(query.plan().identity().as_bytes()),
                "operation": driver_operation_name,
                "schema_hash": driver_input_schema_hash,
                "secret_outputs": query.plan().secret_outputs().iter().map(|secret| json!({
                    "query": format!("{:?}", name),
                    "entity": format!("{:?}", secret.entity()),
                    "field": format!("{:?}", secret.field()),
                })).collect::<Vec<_>>(),
                "params": schemas.parameters().iter().map(|parameter| json!({
                    "name": ts_identifier(parameter.name()),
                    "optional": parameter.has_default()
                        || is_optional_type(parameter.value_type()),
                    "type": ts_query_parameter_type(query, parameter),
                })).collect::<Vec<_>>(),
                "results": schemas.results().iter().map(|branch| json!({
                    "name": format!("{name}{}", pascal(branch.name())),
                    "outcome": branch.name(),
                    "fields": branch.fields().iter().map(|field| json!({
                        "name": ts_identifier(field.name()),
                        "type": ts_query_type(field.value_type()),
                    })).collect::<Vec<_>>(),
                })).collect::<Vec<_>>(),
                "compact": compact,
                "parameter_schema": serde_json::to_string(&parameter_schema).expect("generated parameter schema"),
                "result_schemas": serde_json::to_string(&result_schemas).expect("generated result schemas"),
                "limit_checks": schemas.parameters().iter().filter_map(|parameter| {
                    let NamedTypeSchema::BoundedLimit { maximum } = parameter.value_type() else {
                        return None;
                    };
                    Some(json!({
                        "field": ts_identifier(parameter.name()),
                        "maximum": maximum,
                        "message": format!("{:?}", format!(
                            "{} must be an integer from 1 through {maximum}",
                            parameter.name(),
                        )),
                    }))
                }).collect::<Vec<_>>(),
                "cursor": cursor,
            })
        })
        .collect::<Vec<_>>();
    let mut commands = contract
        .commands()
        .iter()
        .filter(|command| !command.is_reimport())
        .collect::<Vec<_>>();
    commands.sort_by(|left, right| left.name().cmp(right.name()));
    let command_models = commands
        .iter()
        .map(|command| {
            let name = command.name();
            let secret_outputs = command_secret_outputs(command, contract);
            let (driver_operation_name, driver_input_schema_hash) = command_driver_operations
                .get(name)
                .expect("generated command operation");
            let mut input_schema = ts_contract_record_schema(
                command.input().record().fields().iter().map(|field| {
                    (field.name(), field.id().get(), field.value_type())
                }),
                contract,
                false,
            );
            if let Some(expansion) = command.collection_expansion() {
                let field = command
                    .input()
                    .record()
                    .field(expansion.input_field())
                    .expect("validated collection input field");
                let fields = input_schema
                    .get_mut("fields")
                    .and_then(Value::as_array_mut)
                    .expect("generated command input record schema");
                let field_schema = fields
                    .iter_mut()
                    .find(|candidate| {
                        candidate.get("name") == Some(&Value::String(field.name().to_owned()))
                    })
                    .and_then(|candidate| candidate.get_mut("schema"))
                    .expect("generated collection input schema");
                if expansion.maximum_aggregate_element_bytes().is_some() {
                    *field_schema = ts_contract_value_schema_with_bounds(
                        field.value_type(),
                        contract,
                        true,
                    );
                }
                let field_schema = field_schema
                    .as_object_mut()
                    .expect("generated collection input schema object");
                field_schema.insert(
                    "minimum".to_owned(),
                    Value::from(expansion.minimum_elements()),
                );
                field_schema.insert(
                    "maximum".to_owned(),
                    Value::from(expansion.maximum_elements()),
                );
                if let Some(maximum) = expansion.maximum_aggregate_element_bytes() {
                    field_schema.insert(
                        "aggregateCanonicalElementBytes".to_owned(),
                        Value::from(maximum),
                    );
                }
            }
            let outcome_schemas = Value::Object(
                command
                    .outcomes()
                    .iter()
                    .map(|outcome| {
                        (
                            outcome.name().to_owned(),
                            ts_contract_record_schema(
                                outcome.payload().fields().iter().map(|field| {
                                    (field.name(), field.id().get(), field.value_type())
                                }),
                                contract,
                                true,
                            ),
                        )
                    })
                    .collect(),
            );
            let workflow_revisions = workflow_revision_bindings(command);
            json!({
                "name": name,
                "function": camel(name),
                "pascal": pascal(name),
                "constant": screaming_snake(name),
                "plan_hash": hex(command.plan_hash().as_bytes()),
                "operation": driver_operation_name,
                "schema_hash": driver_input_schema_hash,
                "idempotency": command.idempotency_input()
                    .and_then(|id| command.input().record().field(id))
                    .map_or("idempotency_key", |field| field.name()),
                "inputs": command.input().record().fields().iter().map(|field| json!({
                    "name": ts_identifier(field.name()),
                    "type": ts_contract_type(field.value_type(), contract),
                })).collect::<Vec<_>>(),
                "secret_outputs": secret_outputs.iter().map(|secret| json!({
                    "outcome": format!("{:?}", secret.outcome),
                    "field": format!("{:?}", secret.field),
                    "entity": format!("{:?}", secret.entity),
                    "source_field": format!("{:?}", secret.source_field),
                })).collect::<Vec<_>>(),
                "facades": embedding_command_facades(command, contract).into_iter().map(|facade| {
                    let field_constant = screaming_snake(&facade.vector_field_name);
                    json!({
                        "field_constant": field_constant,
                        "helper": format!("{}For{}", camel(name), pascal(&facade.vector_field_name)),
                        "model_key": ts_identifier(&facade.model_input_name),
                        "version_key": ts_identifier(&facade.version_input_name),
                        "model_literal": format!("{:?}", facade.model_input_name),
                        "version_literal": format!("{:?}", facade.version_input_name),
                        "model_identity": format!("{:?}", facade.model_identity),
                        "model_version": format!("{:?}", facade.model_version),
                    })
                }).collect::<Vec<_>>(),
                "outcomes": command.outcomes().iter().map(|outcome| json!({
                    "name": outcome.name(),
                    "fields": outcome.payload().fields().iter().map(|field| json!({
                        "name": ts_identifier(field.name()),
                        "type": ts_contract_type(field.value_type(), contract),
                    })).collect::<Vec<_>>(),
                })).collect::<Vec<_>>(),
                "input_schema": serde_json::to_string(&input_schema).expect("generated input schema"),
                "outcome_schemas": serde_json::to_string(&outcome_schemas).expect("generated outcome schemas"),
                "workflow": {
                    "success_outcome": format!("{:?}", workflow_success_outcome_name(command)),
                    "revisions": workflow_revisions.iter().map(|revision| json!({
                        "binding": format!("{:?}", revision.binding_name),
                        "input": ts_identifier(revision.input_name),
                    })).collect::<Vec<_>>(),
                },
            })
        })
        .collect::<Vec<_>>();
    let vector_inspections = vector_inspection_facades(module, contract)
        .into_iter()
        .flat_map(|inspection| {
            let operations = &vector_driver_operations;
            let method = format!(
                "inspect{}{}",
                pascal(&inspection.entity),
                pascal(&inspection.field),
            );
            let partition_type = ts_contract_type(&inspection.partition_type, contract);
            let partition_schema = serde_json::to_string(&ts_contract_value_schema(
                &inspection.partition_type,
                contract,
            ))
            .expect("generated partition schema");
            [
                ("staleness", "Staleness", "VectorStalenessResult"),
                (
                    "model_versions",
                    "ModelVersions",
                    "VectorModelVersionResult",
                ),
            ]
            .into_iter()
            .map(move |(kind, suffix, result_type)| {
                let (operation, schema_hash) = operations
                    .get(&(
                        inspection.entity.clone(),
                        inspection.field.clone(),
                        kind.to_owned(),
                    ))
                    .expect("generated vector inspection operation");
                json!({
                    "method": method,
                    "suffix": suffix,
                    "result_type": result_type,
                    "partition_type": partition_type,
                    "partition_schema": partition_schema,
                    "operation": format!("{:?}", operation),
                    "schema_hash": format!("{:?}", schema_hash),
                    "entity": format!("{:?}", inspection.entity),
                    "field": format!("{:?}", inspection.field),
                    "kind": format!("{:?}", kind),
                })
            })
        })
        .collect::<Vec<_>>();
    let reactive_models = typescript_reactive_generation_model(module, contract, reactive_modules);
    json!({
        "compact_helpers": {
            "payload": compact_helpers.payload,
            "integer": compact_helpers.integer,
            "string": compact_helpers.string,
            "timestamp": compact_helpers.timestamp,
        },
        "queries": queries,
        "commands": command_models,
        "client": {
            "name": format!("{}Client", pascal(module.contract_lineage().as_str())),
            "vector_inspections": vector_inspections,
        },
        "reactive_modules": reactive_models,
    })
}

fn typescript_reactive_generation_model(
    module: &QueryModule,
    contract: &ContractBundle,
    reactive_modules: &[ReactiveModulePlanV1],
) -> Vec<Value> {
    let client_name = format!(
        "{}ReactiveClient",
        pascal(module.contract_lineage().as_str()),
    );
    reactive_modules
        .iter()
        .map(|reactive| {
            let driver_operations = generate_mcp_reactive_tools(reactive, contract)
                .expect("validated reactive operations")
                .into_iter()
                .map(|operation| {
                    (
                        (operation.operation_name, operation.action),
                        (operation.name, json_schema_hash(&operation.input_schema)),
                    )
                })
                .collect::<BTreeMap<_, _>>();
            let operations = reactive
                .operations()
                .iter()
                .map(|operation| {
                    let source_name = operation.name().as_str();
                    let name = pascal(source_name);
                    let parameters = match operation.plan() {
                        ReactiveOperationPlanV1::Stream { parameters, .. }
                        | ReactiveOperationPlanV1::Watch { parameters, .. }
                        | ReactiveOperationPlanV1::Subscription { parameters, .. } => parameters,
                    };
                    let parameter_model = json!({
                        "fields": parameters.iter().map(|parameter| json!({
                            "name": ts_identifier(parameter.name()),
                            "wire_name": format!("{:?}", parameter.name()),
                            "type": ts_reactive_type(parameter.type_name(), contract),
                            "schema": ts_reactive_schema(parameter.type_name(), contract),
                        })).collect::<Vec<_>>(),
                    });
                    let operation_driver_map = driver_operations
                        .iter()
                        .filter(|((operation_name, _), _)| operation_name == source_name)
                        .map(|((_, action), (driver_name, schema_hash))| json!({
                            "action": format!("{:?}", action),
                            "name": format!("{:?}", driver_name),
                            "schema_hash": format!("{:?}", schema_hash),
                        }))
                        .collect::<Vec<_>>();
                    match operation.plan() {
                        ReactiveOperationPlanV1::Stream { events, .. } => json!({
                            "kind": "stream",
                            "name": name,
                            "source_name": source_name,
                            "source_literal": format!("{:?}", source_name),
                            "method": camel(source_name),
                            "pascal_method": pascal(source_name),
                            "parameters": parameter_model,
                            "driver_operations": operation_driver_map,
                            "events": events.iter().map(|event| json!({
                                "name": format!("{name}{}", pascal(event.name())),
                                "wire_name": event.name(),
                                "fields": event.fields().iter().map(|field| json!({
                                    "name": ts_identifier(field.name()),
                                    "type": ts_reactive_type(field.type_name(), contract),
                                })).collect::<Vec<_>>(),
                            })).collect::<Vec<_>>(),
                        }),
                        ReactiveOperationPlanV1::Watch { query, .. } => json!({
                            "kind": "watch",
                            "name": name,
                            "source_name": source_name,
                            "source_literal": format!("{:?}", source_name),
                            "method": pascal(source_name),
                            "parameters": parameter_model,
                            "driver_operations": operation_driver_map,
                            "query": query.query_name(),
                        }),
                        ReactiveOperationPlanV1::Subscription {
                            stream_name,
                            reactions,
                            ..
                        } => json!({
                            "kind": "subscription",
                            "name": name,
                            "source_name": source_name,
                            "source_literal": format!("{:?}", source_name),
                            "status_method": camel(source_name),
                            "parameters": parameter_model,
                            "driver_operations": operation_driver_map,
                            "stream": pascal(stream_name.as_str()),
                            "reactions": reactions.iter().map(|reaction| {
                                let command = reaction.command_name();
                                let command_plan = contract.commands().iter().find(|candidate| {
                                    candidate.name() == command
                                }).expect("reactive compiler retained exact command dependency");
                                json!({
                                    "method": pascal(reaction.reaction_name()),
                                    "name_literal": format!("{:?}", reaction.reaction_name()),
                                    "command": command,
                                    "command_literal": format!("{:?}", command),
                                    "command_function": camel(command),
                                    "workflow": {
                                        "success_outcome": format!("{:?}", workflow_success_outcome_name(command_plan)),
                                        "revisions": workflow_revision_bindings(command_plan).iter().map(|revision| json!({
                                            "binding": format!("{:?}", revision.binding_name),
                                            "input": ts_identifier(revision.input_name),
                                        })).collect::<Vec<_>>(),
                                    },
                                })
                            }).collect::<Vec<_>>(),
                        }),
                    }
                })
                .collect::<Vec<_>>();
            json!({
                "constant": screaming_snake(reactive.name()),
                "hash": hex(reactive.identity().as_bytes()),
                "client_name": client_name,
                "has_watch": reactive.operations().iter().any(|operation| {
                    matches!(operation.plan(), ReactiveOperationPlanV1::Watch { .. })
                }),
                "operations": operations,
            })
        })
        .collect()
}

#[cfg(test)]
fn generate_typescript_source_base(module: &QueryModule, contract: &ContractBundle) -> String {
    let query_driver_operations = generated_query_driver_operations(module)
        .expect("validated query module has distinct generated operation names");
    let command_driver_operations = generate_mcp_commands(module, contract)
        .expect("validated contract has distinct generated operation names")
        .into_iter()
        .map(|operation| {
            (
                operation.operation_name,
                (operation.name, json_schema_hash(&operation.input_schema)),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let vector_driver_operations = generate_vector_inspection_tools(module, contract)
        .expect("validated nearest sources have distinct inspection names")
        .into_iter()
        .map(|operation| {
            (
                (operation.entity, operation.field, operation.inspection_kind),
                (operation.name, json_schema_hash(&operation.input_schema)),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let compact_helper_usage = typescript_compact_helper_usage(module, contract);
    let mut output = format!(
        "// @generated by riffdb-query-module; do not edit.\nexport const QUERY_MODULE_HASH = \"{}\" as const;\nexport const CONTRACT_LINEAGE = \"{}\" as const;\nexport const CONTRACT_VERSION = {} as const;\nexport const CONTRACT_BUNDLE_HASH = \"{}\" as const;\n\n",
        hex(module.identity().as_bytes()),
        module.contract_lineage().as_str(),
        module.contract_version().get(),
        hex(module.contract_hash().as_bytes()),
    );
    emit_typescript_application_errors(&mut output);
    let vector_schema = if contract.schema().vector_field_specs().is_empty() {
        ""
    } else {
        "  | { readonly kind: \"vector\"; readonly dimension: number }\n"
    };
    writeln!(
        output,
        "export type ApplicationValueSchema =\n  | {{ readonly kind: \"bool\" | \"i64\" | \"u64\" | \"string\" | \"uuid\" | \"enum\" | \"bytes\" | \"date\" | \"timestamp\" | \"cursor\" }}\n  | {{ readonly kind: \"limit\"; readonly maximum?: number }}\n{vector_schema}  | {{ readonly kind: \"decimal\"; readonly precision?: number; readonly scale?: number }}\n  | {{ readonly kind: \"money\"; readonly precision?: number; readonly scale?: number; readonly currency?: string }}\n  | {{ readonly kind: \"optional\"; readonly value: ApplicationValueSchema }}\n  | {{ readonly kind: \"list\"; readonly value: ApplicationValueSchema; readonly minimum?: number; readonly maximum?: number; readonly aggregateCanonicalElementBytes?: number }}\n  | {{ readonly kind: \"record\"; readonly fields: ReadonlyArray<{{ readonly name: string; readonly schema: ApplicationValueSchema; readonly wireId?: number }}> }};\n\
         export interface DriverOperationIdentity {{ readonly name: string; readonly inputSchemaHash: string; }}\n\
         export interface CompactApplicationValue {{ readonly type: string; readonly value?: unknown; }}\n\
         export interface CompactNamedQueryResult {{ readonly outcome: string; readonly resultName: string; readonly entity: string; readonly fields: ReadonlyArray<string>; readonly rows: ReadonlyArray<ReadonlyArray<CompactApplicationValue>>; }}\n\
         export interface NamedQueryRequest<P, R> {{ readonly driverOperation: DriverOperationIdentity; readonly contractLineage: typeof CONTRACT_LINEAGE; \
         readonly contractVersion: typeof CONTRACT_VERSION; readonly contractBundleHash: typeof CONTRACT_BUNDLE_HASH; \
         readonly moduleHash: typeof QUERY_MODULE_HASH; readonly queryName: string; readonly planHash: string; readonly parameters: P; \
         readonly parameterSchema: ApplicationValueSchema; readonly resultSchemas: Readonly<Record<string, ApplicationValueSchema>>; \
         readonly compactDecoder?: (value: CompactNamedQueryResult) => R; readonly decodeError: typeof decodeApplicationError; readonly resultType?: R; }}\n\
         export interface QueryResponseIdentity {{ readonly contractLineage: string; readonly contractVersion: number; \
         readonly contractBundleHash: string; readonly moduleHash: string; readonly queryName: string; readonly planHash: string; }}\n\
         export function acceptsIdentity<P, R>(request: NamedQueryRequest<P, R>, identity: QueryResponseIdentity): boolean {{\n\
         \x20 return identity.contractLineage === request.contractLineage\n    \
         && identity.contractVersion === request.contractVersion\n    \
         && identity.contractBundleHash === request.contractBundleHash\n    \
         && identity.moduleHash === request.moduleHash\n    \
         && identity.queryName === request.queryName\n    \
         && identity.planHash === request.planHash;\n}}\n\
         export interface CommandRequest<I, R> {{ readonly driverOperation: DriverOperationIdentity; readonly contractLineage: typeof CONTRACT_LINEAGE; \
         readonly contractVersion: typeof CONTRACT_VERSION; readonly commandName: string; readonly planHash: string; \
         readonly input: I; readonly idempotencyKey: string; readonly inputSchema: ApplicationValueSchema; \
         readonly outcomeSchemas: Readonly<Record<string, ApplicationValueSchema>>; \
         readonly decodeError: typeof decodeApplicationError; readonly outcomeType?: R; }}\n\
         export interface TypedQueryResult<T> {{ readonly identity: QueryResponseIdentity; readonly value: T; readonly applicationHead: bigint; readonly nextCursor?: string; }}\n\
         export interface WorkflowSuccessorRevision {{ readonly binding: string; readonly revision: bigint; }}\n\
         export interface TypedCommandResult<T> {{ readonly outcome: T; readonly commitSequence?: bigint; \
         readonly contractVersion: number; readonly planHash: string; readonly replayed: boolean; readonly outcomeUri?: string; \
         readonly workflowRevisions?: ReadonlyArray<WorkflowSuccessorRevision>; }}\n\
         export type QueryConsistency = \"admissionHead\";\n\
         export interface QueryOptions {{ readonly cursor?: string; readonly readAfterCommit?: bigint; readonly consistency?: QueryConsistency; }}\n\
         export interface CommandBatchProgress {{ readonly completed: number; readonly total: number; readonly checkpoint: number; }}\n\
         export const MAX_COMMAND_BATCH_CONCURRENCY = 384;\n\
         export interface CommandBatchOptions {{ readonly concurrency: number; readonly checkpoint?: number; readonly onProgress?: (progress: CommandBatchProgress) => void; }}\n\
         export interface CommandBatchItem<T> {{ readonly index: number; readonly result?: TypedCommandResult<T>; readonly error?: unknown; }}\n\
         export interface CommandBatchResult<T> {{ readonly items: ReadonlyArray<CommandBatchItem<T>>; readonly checkpoint: number; }}\n\
         export interface ApplicationTransport {{\n  executeNamedQuery<P, R>(request: NamedQueryRequest<P, R>, options?: QueryOptions): Promise<TypedQueryResult<R>>;\n  \
         executeCommand<I, R>(request: CommandRequest<I, R>, attemptBudget: number): Promise<TypedCommandResult<R>>;\n  \
         executeCommandBatch?<I, R>(request: CommandRequest<I, R>, inputs: ReadonlyArray<I>, concurrency: number, checkpoint: number, attemptBudget: number): Promise<CommandBatchResult<R>>;\n  \
         executeVectorInspection?<P, R>(request: VectorInspectionRequest<P, R>, options?: VectorInspectionOptions): Promise<TypedVectorInspectionResult<R>>;\n}}\n\
         export interface VectorInspectionRequest<P, R> {{ readonly driverOperation: DriverOperationIdentity; readonly contractLineage: typeof CONTRACT_LINEAGE; readonly contractVersion: typeof CONTRACT_VERSION; readonly contractBundleHash: typeof CONTRACT_BUNDLE_HASH; readonly entity: string; readonly field: string; readonly inspectionKind: \"staleness\" | \"model_versions\"; readonly partition: P; readonly partitionSchema: ApplicationValueSchema; readonly limit: number; readonly resultType?: R; }}\n\
         export interface VectorInspectionOptions {{ readonly cursor?: string; }}\n\
         export interface TypedVectorInspectionResult<T> {{ readonly value: T; readonly applicationHead?: bigint; readonly nextCursor?: string; }}\n\
         export interface VectorStalenessItem {{ readonly entityKey: Uint8Array; readonly newestSourceWrite: bigint; readonly embeddingWrite: bigint | null; }}\n\
         export type VectorStalenessResult = {{ readonly kind: \"staleness_summary\"; readonly totalEntities: bigint; readonly staleCount: bigint; readonly staleEntityCountThreshold: bigint; readonly sloBreached: boolean }} | {{ readonly kind: \"stale_entities\"; readonly items: ReadonlyArray<VectorStalenessItem>; readonly observedFrontier: bigint | null }};\n\
         export interface VectorModelVersionItem {{ readonly entityKey: Uint8Array; readonly model: string; readonly modelVersion: string; readonly embeddingWrite: bigint; }}\n\
         export type VectorModelVersionResult = {{ readonly kind: \"model_version_summary\"; readonly currentCount: bigint; readonly outdatedCount: bigint }} | {{ readonly kind: \"outdated_model_entities\"; readonly items: ReadonlyArray<VectorModelVersionItem>; readonly observedFrontier: bigint | null }};\n"
    )
    .infallible();
    emit_typescript_compact_helpers(&mut output, compact_helper_usage);
    for query in module.queries() {
        let name = query.name();
        let schemas = query.plan().schemas();
        let compact_shape = query.plan().common_covered_result().and_then(
            |(result_name, layout, selected_fields)| {
                rust_compact_result_shape(
                    schemas,
                    &result_name,
                    &layout,
                    &selected_fields,
                    contract,
                )
            },
        );
        let plan_hash = hex(query.plan().identity().as_bytes());
        let (driver_operation_name, driver_input_schema_hash) = query_driver_operations
            .get(name)
            .expect("generated query operation");
        let constant = format!("{}_QUERY_PLAN_HASH", screaming_snake(name));
        writeln!(
            output,
            "export const {constant} = \"{plan_hash}\" as const;"
        )
        .infallible();
        if !query.plan().secret_outputs().is_empty() {
            writeln!(
                output,
                "export const {}_SECRET_OUTPUTS = [",
                screaming_snake(name)
            )
            .infallible();
            for secret in query.plan().secret_outputs() {
                writeln!(
                    output,
                    "  {{ query: {:?}, entity: {:?}, field: {:?} }},",
                    name,
                    secret.entity(),
                    secret.field()
                )
                .infallible();
            }
            writeln!(output, "] as const;").infallible();
        }
        writeln!(output, "export interface {name}Params {{").infallible();
        for parameter in schemas.parameters() {
            let optional = if parameter.has_default() || is_optional_type(parameter.value_type()) {
                "?"
            } else {
                ""
            };
            writeln!(
                output,
                "  readonly {}{}: {};",
                ts_identifier(parameter.name()),
                optional,
                ts_query_parameter_type(query, parameter)
            )
            .infallible();
        }
        writeln!(output, "}}\n").infallible();
        for branch in schemas.results() {
            let branch_name = format!("{name}{}", pascal(branch.name()));
            writeln!(output, "export interface {branch_name} {{").infallible();
            writeln!(output, "  readonly outcome: \"{}\";", branch.name()).infallible();
            for field in branch.fields() {
                writeln!(
                    output,
                    "  readonly {}: {};",
                    ts_identifier(field.name()),
                    ts_query_type(field.value_type())
                )
                .infallible();
            }
            writeln!(output, "}}\n").infallible();
        }
        write!(output, "export type {name}Result = ").infallible();
        for (index, branch) in schemas.results().iter().enumerate() {
            if index != 0 {
                write!(output, " | ").infallible();
            }
            write!(output, "{name}{}", pascal(branch.name())).infallible();
        }
        writeln!(output, ";\n").infallible();
        if let Some(shape) = compact_shape.as_ref() {
            emit_typescript_compact_query_decoder(&mut output, name, shape, contract);
        }
        if !query.plan().secret_outputs().is_empty() {
            writeln!(
                output,
                "export function redact{}Result(value: {}Result): {{ readonly outcome: string; readonly redactedSecretOutputs: typeof {}_SECRET_OUTPUTS }} {{\n  return {{ outcome: value.outcome, redactedSecretOutputs: {}_SECRET_OUTPUTS }};\n}}\n",
                pascal(name),
                name,
                screaming_snake(name),
                screaming_snake(name),
            )
            .infallible();
        }
        let parameter_schema = ts_named_record_schema(
            schemas
                .parameters()
                .iter()
                .map(|parameter| (parameter.name(), parameter.value_type())),
            contract,
        );
        let result_schemas = Value::Object(
            schemas
                .results()
                .iter()
                .map(|branch| {
                    (
                        branch.name().to_owned(),
                        ts_named_record_schema(
                            branch
                                .fields()
                                .iter()
                                .map(|field| (field.name(), field.value_type())),
                            contract,
                        ),
                    )
                })
                .collect(),
        );
        let compact_decoder = compact_shape.as_ref().map_or_else(String::new, |_| {
            format!(", compactDecoder: decode{}Compact", pascal(name))
        });
        let bounded_validation = schemas
            .parameters()
            .iter()
            .filter_map(|parameter| match parameter.value_type() {
                NamedTypeSchema::BoundedLimit { maximum } => Some(format!(
                    "  if (parameters.{field} !== undefined && (!Number.isSafeInteger(parameters.{field}) || parameters.{field} < 1 || parameters.{field} > {maximum})) throw new RangeError({message:?});\n",
                    field = ts_identifier(parameter.name()),
                    message = format!(
                        "{} must be an integer from 1 through {maximum}",
                        parameter.name()
                    ),
                )),
                _ => None,
            })
            .collect::<String>();
        writeln!(
            output,
            "export function {function}(parameters: {name}Params): NamedQueryRequest<{name}Params, {name}Result> {{\n\
             {bounded_validation}\
             \x20 return {{ driverOperation: {{ name: \"{driver_operation_name}\", inputSchemaHash: \"{driver_input_schema_hash}\" }}, \
             contractLineage: CONTRACT_LINEAGE, contractVersion: CONTRACT_VERSION, \
             contractBundleHash: CONTRACT_BUNDLE_HASH, moduleHash: QUERY_MODULE_HASH, queryName: \"{name}\", planHash: {constant}, parameters, \
             parameterSchema: {parameter_schema}, resultSchemas: {result_schemas}{compact_decoder}, decodeError: decodeApplicationError }};\n\
             }}\n",
            function = camel(name),
        )
        .infallible();
    }

    let mut commands = contract
        .commands()
        .iter()
        .filter(|command| !command.is_reimport())
        .collect::<Vec<_>>();
    commands.sort_by(|left, right| left.name().cmp(right.name()));
    for command in &commands {
        let name = command.name();
        let (driver_operation_name, driver_input_schema_hash) = command_driver_operations
            .get(name)
            .expect("generated command operation");
        writeln!(output, "export interface {name}Input {{").infallible();
        for field in command.input().record().fields() {
            writeln!(
                output,
                "  readonly {}: {};",
                ts_identifier(field.name()),
                ts_contract_type(field.value_type(), contract)
            )
            .infallible();
        }
        writeln!(output, "}}\n").infallible();
        let secret_outputs = command_secret_outputs(command, contract);
        if !secret_outputs.is_empty() {
            writeln!(
                output,
                "export const {}_SECRET_OUTPUTS = [",
                screaming_snake(name)
            )
            .infallible();
            for secret in &secret_outputs {
                writeln!(
                    output,
                    "  {{ outcome: {:?}, field: {:?}, entity: {:?}, sourceField: {:?} }},",
                    secret.outcome, secret.field, secret.entity, secret.source_field
                )
                .infallible();
            }
            writeln!(output, "] as const;\n").infallible();
        }
        emit_typescript_embedding_constructors(&mut output, command, contract);
        write!(output, "export type {name}Outcome = ").infallible();
        for (index, outcome) in command.outcomes().iter().enumerate() {
            if index != 0 {
                write!(output, " | ").infallible();
            }
            write!(output, "{{ readonly outcome: \"{}\"", outcome.name()).infallible();
            for field in outcome.payload().fields() {
                write!(
                    output,
                    "; readonly {}: {}",
                    ts_identifier(field.name()),
                    ts_contract_type(field.value_type(), contract)
                )
                .infallible();
            }
            write!(output, " }}").infallible();
        }
        writeln!(output, ";\n").infallible();
        if !secret_outputs.is_empty() {
            writeln!(
                output,
                "export function redact{}Outcome(value: {}Outcome): {{ readonly outcome: string; readonly redactedSecretOutputs: typeof {}_SECRET_OUTPUTS }} {{\n  return {{ outcome: value.outcome, redactedSecretOutputs: {}_SECRET_OUTPUTS }};\n}}\n",
                pascal(name),
                name,
                screaming_snake(name),
                screaming_snake(name),
            )
            .infallible();
        }
        let idempotency = command
            .idempotency_input()
            .and_then(|id| command.input().record().field(id))
            .map_or("idempotency_key", |field| field.name());
        let mut input_schema = ts_contract_record_schema(
            command
                .input()
                .record()
                .fields()
                .iter()
                .map(|field| (field.name(), field.id().get(), field.value_type())),
            contract,
            false,
        );
        if let Some(expansion) = command.collection_expansion() {
            let field = command
                .input()
                .record()
                .field(expansion.input_field())
                .expect("validated collection input field");
            let fields = input_schema
                .get_mut("fields")
                .and_then(Value::as_array_mut)
                .expect("generated command input record schema");
            let field_schema = fields
                .iter_mut()
                .find(|candidate| {
                    candidate.get("name") == Some(&Value::String(field.name().to_owned()))
                })
                .and_then(|candidate| candidate.get_mut("schema"))
                .expect("generated collection input schema");
            if expansion.maximum_aggregate_element_bytes().is_some() {
                *field_schema =
                    ts_contract_value_schema_with_bounds(field.value_type(), contract, true);
            }
            let field_schema = field_schema
                .as_object_mut()
                .expect("generated collection input schema object");
            field_schema.insert(
                "minimum".to_owned(),
                Value::from(expansion.minimum_elements()),
            );
            field_schema.insert(
                "maximum".to_owned(),
                Value::from(expansion.maximum_elements()),
            );
            if let Some(maximum) = expansion.maximum_aggregate_element_bytes() {
                field_schema.insert(
                    "aggregateCanonicalElementBytes".to_owned(),
                    Value::from(maximum),
                );
            }
        }
        let outcome_schemas =
            Value::Object(
                command
                    .outcomes()
                    .iter()
                    .map(|outcome| {
                        (
                            outcome.name().to_owned(),
                            ts_contract_record_schema(
                                outcome.payload().fields().iter().map(|field| {
                                    (field.name(), field.id().get(), field.value_type())
                                }),
                                contract,
                                true,
                            ),
                        )
                    })
                    .collect(),
            );
        writeln!(
            output,
            "export const {constant}_PLAN_HASH = \"{plan_hash}\" as const;\n\
             export function {function}(input: {name}Input): CommandRequest<{name}Input, {name}Outcome> {{\n\
             \x20 return {{ driverOperation: {{ name: \"{driver_operation_name}\", inputSchemaHash: \"{driver_input_schema_hash}\" }}, \
             contractLineage: CONTRACT_LINEAGE, contractVersion: CONTRACT_VERSION, \
             commandName: \"{name}\", planHash: {constant}_PLAN_HASH, input, idempotencyKey: input.{idempotency}, \
             inputSchema: {input_schema}, outcomeSchemas: {outcome_schemas}, decodeError: decodeApplicationError }};\n\
             }}\n",
            function = camel(name),
            constant = screaming_snake(name),
            plan_hash = hex(command.plan_hash().as_bytes()),
        )
        .infallible();
    }
    emit_typescript_client_facade(
        &mut output,
        module,
        &commands,
        contract,
        &vector_driver_operations,
    );
    output
}

#[cfg(test)]
fn emit_typescript_embedding_constructors(
    output: &mut String,
    command: &CommandPlan,
    contract: &ContractBundle,
) {
    let command_name = command.name();
    for facade in embedding_command_facades(command, contract) {
        let field_constant = screaming_snake(&facade.vector_field_name);
        let helper = format!(
            "{}For{}",
            camel(command_name),
            pascal(&facade.vector_field_name)
        );
        let model_key = ts_identifier(&facade.model_input_name);
        let version_key = ts_identifier(&facade.version_input_name);
        writeln!(
            output,
            "export const {command}_{field_constant}_MODEL_IDENTITY = {:?} as const;\nexport const {command}_{field_constant}_MODEL_VERSION = {:?} as const;",
            facade.model_identity,
            facade.model_version,
            command = screaming_snake(command_name),
        )
        .infallible();
        writeln!(
            output,
            "export function {helper}(input: Omit<{command_name}Input, {model:?} | {version:?}>): {command_name}Input {{\n  return {{ ...input, {model_key}: {command}_{field_constant}_MODEL_IDENTITY, {version_key}: {command}_{field_constant}_MODEL_VERSION }};\n}}\nexport function {helper}Model(input: {command_name}Input): {{ readonly identity: string; readonly version: string }} {{\n  return {{ identity: input.{model_key}, version: input.{version_key} }};\n}}\n",
            model = model_key,
            version = version_key,
            command = screaming_snake(command_name),
        )
        .infallible();
    }
}

/// Generates one complete TypeScript application client including typed async
/// iterators, a framework-neutral live store, and a credential-free SSE relay
/// adapter for application servers.
#[must_use]
pub fn generate_typescript_application_client(
    module: &QueryModule,
    contract: &ContractBundle,
    reactive_modules: &[ReactiveModulePlanV1],
) -> String {
    let model = generate_canonical_generation_model(module, contract, reactive_modules);
    render_typescript_client_header(&model).expect("static TypeScript template and model")
}

#[cfg(test)]
pub(crate) fn typescript_generation_source(
    module: &QueryModule,
    contract: &ContractBundle,
    reactive_modules: &[ReactiveModulePlanV1],
) -> String {
    let mut output = generate_typescript_source_base(module, contract);
    for reactive in reactive_modules {
        emit_typescript_reactive_module(&mut output, module, contract, reactive);
    }
    output.truncate(output.trim_end().len());
    output.push('\n');
    output
}

#[cfg(test)]
fn emit_typescript_reactive_module(
    output: &mut String,
    module: &QueryModule,
    contract: &ContractBundle,
    reactive: &ReactiveModulePlanV1,
) {
    let driver_operations = generate_mcp_reactive_tools(reactive, contract)
        .expect("validated reactive operations")
        .into_iter()
        .map(|operation| {
            (
                (operation.operation_name, operation.action),
                (operation.name, json_schema_hash(&operation.input_schema)),
            )
        })
        .collect::<BTreeMap<_, _>>();
    writeln!(
        output,
        "\nexport const {}_REACTIVE_MODULE_HASH = \"{}\" as const;",
        screaming_snake(reactive.name()),
        hex(reactive.identity().as_bytes())
    )
    .infallible();
    output.push_str(
        r#"export type ReactiveParameterSchema =
  | { readonly kind: "bool" | "i64" | "u64" | "string" | "uuid" | "bytes" | "date" | "timestamp" | "cursor" }
  | { readonly kind: "limit"; readonly maximum?: number }
  | { readonly kind: "decimal"; readonly precision: number; readonly scale: number }
  | { readonly kind: "money"; readonly precision: number; readonly scale: number; readonly currency: string }
  | { readonly kind: "enum"; readonly typeId: number; readonly variants: Readonly<Record<string, number>> }
  | { readonly kind: "record"; readonly fields: ReadonlyArray<{ readonly name: string; readonly schema: ReactiveParameterSchema }> };
export interface ReactiveConsumerRequest<P> { readonly driverOperations: Readonly<Record<string, DriverOperationIdentity>>; readonly reactiveModuleHash: string; readonly operationName: string; readonly parameters: P; readonly parameterSchema: ReactiveParameterSchema; readonly consumerName: string; }
export interface ReactiveEventDelivery<E> { readonly eventId: string; readonly event: E; readonly attempt: number; readonly leaseToken: string; readonly expiresAt: string; readonly historyIncarnation: bigint; }
export interface ReactiveConsumerStatus { readonly revision: bigint; readonly checkpoint: string; readonly historyIncarnation: bigint; readonly liveLeases: number; readonly retries: number; readonly deadLetters: number; }
export interface ReactiveConsumerBatch<E> { readonly events: ReadonlyArray<ReactiveEventDelivery<E>>; readonly waitTimedOut: boolean; readonly status: ReactiveConsumerStatus; }
export interface ReactiveConsumerOptions { readonly batchLimit?: number; readonly inFlightLimit?: number; readonly leaseSeconds?: number; readonly maximumWaitMs?: number; readonly signal?: AbortSignal; }
export interface ContextualHydration { readonly name: string; readonly outcome: string; readonly fields: Readonly<Record<string, unknown>>; }
export interface ContextualReaction { readonly name: string; readonly commandName: string; readonly commandId: number; readonly causationToken: string; }
export interface ContextualWorkItem<E> { readonly delivery: ReactiveEventDelivery<E>; readonly contextHead: bigint; readonly hydrations: ReadonlyArray<ContextualHydration>; readonly availableReactions: ReadonlyArray<ContextualReaction>; }
export interface ContextualBatch<E> { readonly items: ReadonlyArray<ContextualWorkItem<E>>; readonly waitTimedOut: boolean; readonly status: ReactiveConsumerStatus; }
export type ReactiveEventMutationResult = "applied" | "state_changed" | "not_found" | "outstanding_lease" | "stale_lease" | "lease_expired";
export type LiveQueryPatchOperation =
  | { readonly type: "insert"; readonly index: number; readonly record: Readonly<Record<string, unknown>> }
  | { readonly type: "remove"; readonly index: number; readonly key: Readonly<Record<string, unknown>> }
  | { readonly type: "replace"; readonly index: number; readonly record: Readonly<Record<string, unknown>> }
  | { readonly type: "move"; readonly from: number; readonly to: number; readonly key: Readonly<Record<string, unknown>> };
export type LiveQueryUpdate<T> =
  | { readonly type: "snapshot"; readonly value: T; readonly cursor: string; readonly applicationHead: bigint; readonly historyIncarnation: bigint }
  | { readonly type: "patch"; readonly resultField: string; readonly operations: ReadonlyArray<LiveQueryPatchOperation>; readonly cursor: string; readonly applicationHead: bigint; readonly historyIncarnation: bigint }
  | { readonly type: "reset"; readonly reason: string; readonly value: T; readonly cursor: string; readonly applicationHead: bigint; readonly historyIncarnation: bigint }
  | { readonly type: "checkpoint"; readonly cursor: string; readonly applicationHead: bigint; readonly historyIncarnation: bigint }
  | { readonly type: "terminal"; readonly reason: string; readonly lastApplicationHead?: bigint; readonly historyIncarnation?: bigint };
export interface ReactiveApplicationTransport {
  consumeEventStream<P, E>(request: ReactiveConsumerRequest<P>, options?: ReactiveConsumerOptions): AsyncIterable<ReactiveConsumerBatch<E>>;
  acknowledgeEvent<P>(request: ReactiveConsumerRequest<P>, delivery: ReactiveEventDelivery<unknown>): Promise<ReactiveEventMutationResult>;
  negativeAcknowledgeEvent<P>(request: ReactiveConsumerRequest<P>, delivery: ReactiveEventDelivery<unknown>, retryDelayMs?: number): Promise<ReactiveEventMutationResult>;
  seekEventConsumer<P>(request: ReactiveConsumerRequest<P>, checkpoint: string): Promise<ReactiveEventMutationResult>;
  eventConsumerStatus<P>(request: ReactiveConsumerRequest<P>): Promise<ReactiveConsumerStatus | undefined>;
  consumeContextualSubscription<P, E>(request: ReactiveConsumerRequest<P>, maximumWaitMs?: number, signal?: AbortSignal): Promise<ContextualBatch<E>>;
  acknowledgeContextualItem<P>(request: ReactiveConsumerRequest<P>, item: ContextualWorkItem<unknown>): Promise<ReactiveEventMutationResult>;
  negativeAcknowledgeContextualItem<P>(request: ReactiveConsumerRequest<P>, item: ContextualWorkItem<unknown>, retryDelayMs?: number): Promise<ReactiveEventMutationResult>;
  contextualSubscriptionStatus<P>(request: ReactiveConsumerRequest<P>): Promise<ReactiveConsumerStatus | undefined>;
  executeContextualReaction<P, I, R>(request: ReactiveConsumerRequest<P>, reaction: ContextualReaction, command: CommandRequest<I, R>): Promise<TypedCommandResult<R>>;
  watchNamedQuery<P, T>(request: { readonly driverOperations: Readonly<Record<string, DriverOperationIdentity>>; readonly reactiveModuleHash: string; readonly operationName: string; readonly parameters: P; readonly parameterSchema: ReactiveParameterSchema; readonly cursor?: string; readonly signal?: AbortSignal }): AsyncIterable<LiveQueryUpdate<T>>;
}
export interface LiveStore<T> { readonly current: T | undefined; readonly connected: boolean; subscribe(listener: (value: T | undefined) => void): () => void; connect(updates: AsyncIterable<LiveQueryUpdate<T>>): Promise<void>; close(): void; }
function createLiveStore<T>(): LiveStore<T> {
  let current: T | undefined;
  let connected = false;
  const listeners = new Set<(value: T | undefined) => void>();
  const publish = (): void => { for (const listener of listeners) listener(current); };
  return {
    get current() { return current; }, get connected() { return connected; },
    subscribe(listener) { listeners.add(listener); return () => listeners.delete(listener); },
    async connect(updates) { connected = true; try { for await (const update of updates) { if (update.type === "snapshot" || update.type === "reset") { current = update.value; publish(); } else if (update.type === "patch") { current = applyLivePatch(current, update); publish(); } else if (update.type === "terminal") { current = undefined; publish(); break; } } } finally { connected = false; if (current !== undefined) { current = undefined; publish(); } } },
    close() { connected = false; current = undefined; publish(); listeners.clear(); },
  };
}
function applyLivePatch<T>(current: T | undefined, update: Extract<LiveQueryUpdate<T>, { readonly type: "patch" }>): T {
  if (current === undefined || typeof current !== "object" || current === null || Array.isArray(current)) throw new Error("live patch has no current result");
  const root = current as Record<string, unknown>;
  const existing = root[update.resultField];
  if (!Array.isArray(existing)) throw new Error("live patch result field is not a collection");
  const records = existing.slice();
  for (const operation of update.operations) {
    if (operation.type === "insert") { if (operation.index > records.length) throw new Error("live patch insert index is invalid"); records.splice(operation.index, 0, operation.record); }
    else if (operation.type === "replace") { if (operation.index >= records.length) throw new Error("live patch replace index is invalid"); records[operation.index] = operation.record; }
    else if (operation.type === "remove") { if (operation.index >= records.length || !liveKeyMatches(records[operation.index], operation.key)) throw new Error("live patch remove key is invalid"); records.splice(operation.index, 1); }
    else { if (operation.from >= records.length || operation.to >= records.length || !liveKeyMatches(records[operation.from], operation.key)) throw new Error("live patch move key is invalid"); const [record] = records.splice(operation.from, 1); records.splice(operation.to, 0, record); }
  }
  return { ...root, [update.resultField]: records } as T;
}
function liveKeyMatches(value: unknown, key: Readonly<Record<string, unknown>>): boolean {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
  const record = value as Record<string, unknown>;
  return Object.entries(key).every(([name, expected]) => Object.is(record[name], expected));
}
async function* applicationSseRelay<T>(authorized: () => Promise<boolean>, updates: AsyncIterable<LiveQueryUpdate<T>>): AsyncIterable<string> {
  try {
    for await (const update of updates) {
      if (!(await authorized())) { yield `event: terminal\ndata: {"type":"terminal","reason":"authorization_changed"}\n\n`; return; }
      yield `event: ${update.type}\ndata: ${JSON.stringify(update, (_key, value) => typeof value === "bigint" ? value.toString() : value)}\n\n`;
      if (update.type === "terminal") return;
    }
    yield `event: terminal\ndata: {"type":"terminal","reason":"service_unavailable"}\n\n`;
  } catch {
    yield `event: terminal\ndata: {"type":"terminal","reason":"service_unavailable"}\n\n`;
  }
}
"#,
    );
    for operation in reactive.operations() {
        match operation.plan() {
            ReactiveOperationPlanV1::Stream {
                parameters, events, ..
            } => {
                let name = pascal(operation.name().as_str());
                emit_typescript_reactive_parameters(output, &name, parameters, contract);
                for event in events {
                    writeln!(
                        output,
                        "export interface {name}{} {{ readonly type: \"{}\";",
                        pascal(event.name()),
                        event.name()
                    )
                    .infallible();
                    for field in event.fields() {
                        writeln!(
                            output,
                            "  readonly {}: {};",
                            ts_identifier(field.name()),
                            ts_reactive_type(field.type_name(), contract)
                        )
                        .infallible();
                    }
                    writeln!(output, "}}\n").infallible();
                }
                write!(output, "export type {name}Event = ").infallible();
                for (index, event) in events.iter().enumerate() {
                    if index != 0 {
                        output.push_str(" | ");
                    }
                    write!(output, "{name}{}", pascal(event.name())).infallible();
                }
                writeln!(
                    output,
                    ";\nexport type {name}Delivery = ReactiveEventDelivery<{name}Event>;\nexport type {name}Stream = AsyncIterable<ReactiveConsumerBatch<{name}Event>>;"
                )
                .infallible();
            }
            ReactiveOperationPlanV1::Watch {
                parameters, query, ..
            } => {
                let name = pascal(operation.name().as_str());
                emit_typescript_reactive_parameters(output, &name, parameters, contract);
                writeln!(
                    output,
                    "export type {name}Update = LiveQueryUpdate<{}Result>;",
                    query.query_name()
                )
                .infallible();
            }
            ReactiveOperationPlanV1::Subscription {
                parameters,
                stream_name,
                ..
            } => {
                let name = pascal(operation.name().as_str());
                let stream = pascal(stream_name.as_str());
                emit_typescript_reactive_parameters(output, &name, parameters, contract);
                writeln!(
                    output,
                    "export type {name}Item = ContextualWorkItem<{stream}Event>;\nexport type {name}Batch = ContextualBatch<{stream}Event>;"
                )
                .infallible();
            }
        }
    }
    let client_name = format!(
        "{}ReactiveClient",
        pascal(module.contract_lineage().as_str())
    );
    writeln!(
        output,
        "export class {client_name} {{\n  public constructor(private readonly transport: ReactiveApplicationTransport) {{}}"
    )
    .infallible();
    for operation in reactive.operations() {
        let operation_driver_map =
            typescript_driver_action_map(operation.name().as_str(), &driver_operations);
        match operation.plan() {
            ReactiveOperationPlanV1::Stream { .. } => {
                let name = pascal(operation.name().as_str());
                writeln!(
                    output,
                    "  public {method}(parameters: {name}Params, consumerName: string, options: ReactiveConsumerOptions = {{}}): AsyncIterable<ReactiveConsumerBatch<{name}Event>> {{ return this.transport.consumeEventStream({{ driverOperations: {operation_driver_map}, reactiveModuleHash: {}_REACTIVE_MODULE_HASH, operationName: \"{}\", parameters, parameterSchema: {name}ParameterSchema, consumerName }}, options); }}",
                    screaming_snake(reactive.name()),
                    operation.name().as_str(),
                    method = camel(operation.name().as_str())
                )
                .infallible();
                writeln!(
                    output,
                    "  public ack{method}(parameters: {name}Params, consumerName: string, delivery: {name}Delivery): Promise<ReactiveEventMutationResult> {{ return this.transport.acknowledgeEvent({{ driverOperations: {operation_driver_map}, reactiveModuleHash: {module}_REACTIVE_MODULE_HASH, operationName: {operation:?}, parameters, parameterSchema: {name}ParameterSchema, consumerName }}, delivery); }}\n  public nack{method}(parameters: {name}Params, consumerName: string, delivery: {name}Delivery, retryDelayMs = 0): Promise<ReactiveEventMutationResult> {{ return this.transport.negativeAcknowledgeEvent({{ driverOperations: {operation_driver_map}, reactiveModuleHash: {module}_REACTIVE_MODULE_HASH, operationName: {operation:?}, parameters, parameterSchema: {name}ParameterSchema, consumerName }}, delivery, retryDelayMs); }}\n  public seek{method}(parameters: {name}Params, consumerName: string, checkpoint = \"before-first\"): Promise<ReactiveEventMutationResult> {{ return this.transport.seekEventConsumer({{ driverOperations: {operation_driver_map}, reactiveModuleHash: {module}_REACTIVE_MODULE_HASH, operationName: {operation:?}, parameters, parameterSchema: {name}ParameterSchema, consumerName }}, checkpoint); }}\n  public {status_method}Status(parameters: {name}Params, consumerName: string): Promise<ReactiveConsumerStatus | undefined> {{ return this.transport.eventConsumerStatus({{ driverOperations: {operation_driver_map}, reactiveModuleHash: {module}_REACTIVE_MODULE_HASH, operationName: {operation:?}, parameters, parameterSchema: {name}ParameterSchema, consumerName }}); }}",
                    method = pascal(operation.name().as_str()),
                    status_method = camel(operation.name().as_str()),
                    module = screaming_snake(reactive.name()),
                    operation = operation.name().as_str(),
                )
                .infallible();
            }
            ReactiveOperationPlanV1::Watch { .. } => {
                let name = pascal(operation.name().as_str());
                writeln!(
                    output,
                    "  public watch{method}(parameters: {name}Params, cursor?: string, signal?: AbortSignal): AsyncIterable<{name}Update> {{ return this.transport.watchNamedQuery({{ driverOperations: {operation_driver_map}, reactiveModuleHash: {}_REACTIVE_MODULE_HASH, operationName: \"{}\", parameters, parameterSchema: {name}ParameterSchema, ...(cursor === undefined ? {{}} : {{ cursor }}), ...(signal === undefined ? {{}} : {{ signal }}) }}); }}",
                    screaming_snake(reactive.name()),
                    operation.name().as_str(),
                    method = pascal(operation.name().as_str())
                )
                .infallible();
            }
            ReactiveOperationPlanV1::Subscription { reactions, .. } => {
                let name = pascal(operation.name().as_str());
                let request = format!(
                    "{{ driverOperations: {operation_driver_map}, reactiveModuleHash: {}_REACTIVE_MODULE_HASH, operationName: {:?}, parameters, parameterSchema: {name}ParameterSchema, consumerName }}",
                    screaming_snake(reactive.name()),
                    operation.name().as_str(),
                );
                writeln!(
                    output,
                    "  public next{name}(parameters: {name}Params, consumerName: string, maximumWaitMs = 30000, signal?: AbortSignal): Promise<{name}Batch> {{ return this.transport.consumeContextualSubscription({request}, maximumWaitMs, signal); }}\n  public ack{name}(parameters: {name}Params, consumerName: string, item: {name}Item): Promise<ReactiveEventMutationResult> {{ return this.transport.acknowledgeContextualItem({request}, item); }}\n  public nack{name}(parameters: {name}Params, consumerName: string, item: {name}Item, retryDelayMs = 0): Promise<ReactiveEventMutationResult> {{ return this.transport.negativeAcknowledgeContextualItem({request}, item, retryDelayMs); }}\n  public {status}Status(parameters: {name}Params, consumerName: string): Promise<ReactiveConsumerStatus | undefined> {{ return this.transport.contextualSubscriptionStatus({request}); }}",
                    status = camel(operation.name().as_str()),
                )
                .infallible();
                for reaction in reactions {
                    let command = reaction.command_name();
                    let command_plan = contract
                        .commands()
                        .iter()
                        .find(|candidate| candidate.name() == command)
                        .expect("reactive compiler retained exact command dependency");
                    let workflow_revisions = workflow_revision_bindings(command_plan);
                    let success_outcome = workflow_success_outcome_name(command_plan);
                    writeln!(
                        output,
                        "  public async react{reaction_method}(parameters: {name}Params, consumerName: string, item: {name}Item, input: {command}Input): Promise<TypedCommandResult<{command}Outcome>> {{ const reaction = item.availableReactions.find((value) => value.name === {reaction_name:?} && value.commandName === {command:?}); if (reaction === undefined) throw new Error(\"contextual reaction is unavailable\"); const result = await this.transport.executeContextualReaction({request}, reaction, {command_function}(input));",
                        reaction_method = pascal(reaction.reaction_name()),
                        reaction_name = reaction.reaction_name(),
                        command_function = camel(command),
                    )
                    .infallible();
                    if workflow_revisions.is_empty() {
                        writeln!(output, "    return result; }}").infallible();
                    } else {
                        writeln!(output, "    if (result.outcome.outcome !== {success_outcome:?}) return {{ ...result, workflowRevisions: [] }};").infallible();
                        for revision in &workflow_revisions {
                            writeln!(output, "    if (input.{} === 18446744073709551615n) throw new Error(\"RiffDB workflow successor revision overflow\");", ts_identifier(revision.input_name)).infallible();
                        }
                        writeln!(output, "    return {{ ...result, workflowRevisions: [")
                            .infallible();
                        for revision in workflow_revisions {
                            writeln!(
                                output,
                                "      {{ binding: {:?}, revision: input.{} + 1n }},",
                                revision.binding_name,
                                ts_identifier(revision.input_name)
                            )
                            .infallible();
                        }
                        writeln!(output, "    ] }}; }}").infallible();
                    }
                }
            }
        }
    }
    writeln!(output, "}}\n").infallible();
    for operation in reactive.operations() {
        if let ReactiveOperationPlanV1::Watch { query, .. } = operation.plan() {
            let name = pascal(operation.name().as_str());
            writeln!(
                output,
                "export function create{name}Store(): LiveStore<{}Result> {{ return createLiveStore(); }}\nexport function create{name}SseRelay(authorized: () => Promise<boolean>, updates: AsyncIterable<{name}Update>): AsyncIterable<string> {{ return applicationSseRelay(authorized, updates); }}",
                query.query_name()
            )
            .infallible();
        }
    }
}

#[cfg(test)]
fn emit_typescript_reactive_parameters(
    output: &mut String,
    operation: &str,
    parameters: &[riffdb_query_ir::ReactiveParameterV1],
    contract: &ContractBundle,
) {
    writeln!(output, "export interface {operation}Params {{").infallible();
    for parameter in parameters {
        writeln!(
            output,
            "  readonly {}: {};",
            ts_identifier(parameter.name()),
            ts_reactive_type(parameter.type_name(), contract)
        )
        .infallible();
    }
    writeln!(output, "}}").infallible();
    writeln!(
        output,
        "export const {operation}ParameterSchema: ReactiveParameterSchema = {{ kind: \"record\", fields: ["
    )
    .infallible();
    for parameter in parameters {
        writeln!(
            output,
            "  {{ name: {:?}, schema: {} }},",
            parameter.name(),
            ts_reactive_schema(parameter.type_name(), contract)
        )
        .infallible();
    }
    writeln!(output, "] }};\n").infallible();
}

#[cfg(test)]
fn typescript_driver_action_map(
    operation_name: &str,
    operations: &BTreeMap<(String, String), (String, String)>,
) -> String {
    let entries = operations
        .iter()
        .filter(|((operation, _), _)| operation == operation_name)
        .map(|((_, action), (name, schema_hash))| {
            format!("{action:?}: {{ name: {name:?}, inputSchemaHash: {schema_hash:?} }}")
        })
        .collect::<Vec<_>>();
    format!("{{ {} }}", entries.join(", "))
}

fn ts_reactive_type(type_name: &str, contract: &ContractBundle) -> String {
    if let Some(enumeration) = contract
        .schema()
        .enums()
        .iter()
        .find(|value| value.name() == type_name)
    {
        return enumeration
            .variants()
            .iter()
            .map(|variant| format!("{:?}", variant.name()))
            .collect::<Vec<_>>()
            .join(" | ");
    }
    if type_name.starts_with("decimal<") {
        return "{ readonly coefficientTwosComplement: Uint8Array; readonly scale: number; readonly precision: number }".to_owned();
    }
    if type_name.starts_with("money<") {
        return "{ readonly currency: string; readonly amount: { readonly coefficientTwosComplement: Uint8Array; readonly scale: number; readonly precision: number } }".to_owned();
    }
    if type_name.starts_with("bytes<") {
        return "Uint8Array".to_owned();
    }
    if type_name.starts_with("vector<") {
        return "ReadonlyArray<number>".to_owned();
    }
    match type_name {
        "bool" => "boolean",
        "i64" | "u64" => "bigint",
        "date" | "limit" => "number",
        "timestamp" => "{ readonly seconds: bigint; readonly nanos: number }",
        "uuid" | "cursor" => "string",
        value if value.starts_with("string<") => "string",
        _ => "never",
    }
    .to_owned()
}

fn ts_reactive_schema(type_name: &str, contract: &ContractBundle) -> String {
    if let Some(enumeration) = contract
        .schema()
        .enums()
        .iter()
        .find(|value| value.name() == type_name)
    {
        let variants = enumeration
            .variants()
            .iter()
            .map(|variant| format!("{:?}: {}", variant.name(), variant.id().get()))
            .collect::<Vec<_>>()
            .join(", ");
        return format!(
            "{{ kind: \"enum\", typeId: {}, variants: {{ {variants} }} }}",
            enumeration.id().get()
        );
    }
    if let Some((precision, scale)) = decimal_type_parts(type_name) {
        return format!("{{ kind: \"decimal\", precision: {precision}, scale: {scale} }}");
    }
    if let Some(currency) = money_type_currency(type_name) {
        return format!("{{ kind: \"money\", precision: 38, scale: 2, currency: {currency:?} }}");
    }
    if let Some(dimension) = vector_type_dimension(type_name) {
        return format!("{{ kind: \"vector\", dimension: {dimension} }}");
    }
    let kind = if type_name.starts_with("bytes<") {
        "bytes"
    } else if type_name.starts_with("string<") {
        "string"
    } else {
        type_name
    };
    format!("{{ kind: {kind:?} }}")
}

fn decimal_type_parts(type_name: &str) -> Option<(u8, u8)> {
    let body = type_name.strip_prefix("decimal<")?.strip_suffix('>')?;
    let (precision, scale) = body.split_once(',')?;
    Some((precision.parse().ok()?, scale.parse().ok()?))
}

fn vector_type_dimension(type_name: &str) -> Option<u32> {
    type_name
        .strip_prefix("vector<")?
        .strip_suffix('>')?
        .parse()
        .ok()
}

fn money_type_currency(type_name: &str) -> Option<String> {
    type_name
        .strip_prefix("money<")?
        .strip_suffix('>')
        .map(str::to_owned)
}

#[cfg(test)]
fn emit_typescript_application_errors(output: &mut String) {
    output.push_str(
        r#"export const APPLICATION_ERROR_REGISTRY = {
  "RDB-APP-0001": ["application request is structurally invalid", "input", "correct_request", ["correct_input"]],
  "RDB-INPUT-0101": ["application input is invalid", "input", "correct_request", ["correct_input"]],
  "RDB-AUTH-0214": ["application operation is not authorized", "authorization", "obtain_permission", ["bind_application_role"]],
  "RDB-CONTRACT-0101": ["application contract does not match", "contract", "refresh_contract", ["refresh_contract"]],
  "RDB-QUERY-0101": ["RiffQL query is invalid", "query", "correct_request", ["correct_input"]],
  "RDB-QUERY-0102": ["RiffQL query is unavailable", "query", "refresh_contract", ["pin_active_module"]],
  "RDB-MODULE-0101": ["query module is unavailable", "module", "refresh_contract", ["pin_active_module"]],
  "RDB-CURSOR-0101": ["query cursor is invalid or stale", "cursor", "correct_request", ["restart_from_first_page"]],
  "RDB-RESOURCE-0101": ["application result exceeds the service limit", "resource", "correct_request", ["correct_input"]],
  "RDB-STORAGE-0101": ["storage is temporarily unavailable", "storage", "retry", ["retry_later"]],
  "RDB-UNCERTAIN-0101": ["command outcome is not yet known", "uncertainty", "resolve_with_same_idempotency_key", ["resolve_with_same_idempotency_key"]],
  "RDB-APP-0002": ["application request was cancelled", "control", "none", []],
  "RDB-APP-0003": ["application request deadline elapsed", "control", "retry", ["retry_later"]],
  "RDB-INTERNAL-0001": ["an internal error occurred", "internal", "contact_operator", ["contact_operator_with_incident"]],
  "RDB-COMMAND-0101": ["idempotency key was reused with different application input", "command", "correct_request", ["correct_input"]],
  "RDB-COMMAND-0102": ["command execution failed", "command", "contact_operator", []],
  "RDB-AUTH-0215": ["application capability is revoked", "authorization", "obtain_permission", ["bind_application_role"]],
  "RDB-PROTOCOL-0101": ["the RiffDB peer returned an invalid application response", "protocol", "contact_operator", []],
  "RDB-HISTORY-0101": ["observed history predates a database restore", "history", "correct_request", ["correct_input"]],
  "RDB-HISTORY-0102": ["requested history has been pruned", "history", "correct_request", ["correct_input"]],
  "RDB-CAPACITY-0101": ["service is over capacity", "capacity", "retry", ["retry_later"]],
  "RDB-PROJECTION-0101": ["query projections cannot prove one common snapshot", "query", "retry", ["retry_later"]],
  "RDB-PROJECTION-0102": ["the requested query snapshot has been retired", "query", "correct_request", ["restart_from_first_page"]],
  "RDB-PROJECTION-0103": ["no query snapshot satisfies the requested freshness", "query", "retry", ["retry_later"]],
  "RDB-PROJECTION-0104": ["nearest query requires a compiler-owned projected source", "query", "refresh_contract", ["pin_active_module"]],
  "RDB-REP-0101": ["operation is unavailable in follower mode", "control", "correct_request", []],
  "RDB-REP-0102": ["primary is durably fenced", "control", "contact_operator", []],
} as const;

export type ApplicationErrorCode = keyof typeof APPLICATION_ERROR_REGISTRY;
export type ApplicationOperation = "DescribeContract" | "CheckQuery" | "ExplainQuery" | "ExecuteQuery" | "DeployQueryModule" | "GetQueryModule" | "ExecuteCommand" | "BatchCommand" | "InspectVectorState";
export type ApplicationErrorCategory = typeof APPLICATION_ERROR_REGISTRY[ApplicationErrorCode][1];
export type ApplicationRecoveryAction = typeof APPLICATION_ERROR_REGISTRY[ApplicationErrorCode][2];
export type ApplicationFixCode = typeof APPLICATION_ERROR_REGISTRY[ApplicationErrorCode][3][number];

export interface ApplicationErrorDetails {
  readonly type: "application";
  readonly code: ApplicationErrorCode;
  readonly message: string;
  readonly category: ApplicationErrorCategory;
  readonly recoveryAction: ApplicationRecoveryAction;
  readonly operation: ApplicationOperation;
  readonly contractLineage?: string;
  readonly contractVersion?: number;
  readonly operationSymbol?: string;
  readonly symbolPath: ReadonlyArray<string>;
  readonly sourceSpan?: { readonly start: number; readonly end: number };
  readonly fixes: ReadonlyArray<ApplicationFixCode>;
  readonly traceId?: string;
  readonly incidentId?: string;
}

export class RiffDbApplicationError extends Error {
  public override readonly name = "RiffDbApplicationError";
  public constructor(public readonly details: ApplicationErrorDetails) {
    super(`${details.code}: ${details.message} [${details.operation}]`);
  }
}

const APPLICATION_OPERATIONS: ReadonlySet<string> = new Set([
  "DescribeContract", "CheckQuery", "ExplainQuery", "ExecuteQuery",
  "DeployQueryModule", "GetQueryModule", "ExecuteCommand", "BatchCommand", "InspectVectorState",
]);
const SAFE_SYMBOL = /^[A-Za-z0-9_.-]{1,256}$/;
const UUID_V7 = /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;

export function decodeApplicationError(value: unknown): RiffDbApplicationError {
  if (typeof value !== "object" || value === null || Array.isArray(value)) throw new Error("invalid RiffDB application error");
  const input = value as Record<string, unknown>;
  const code = input.code;
  if (typeof code !== "string" || !(code in APPLICATION_ERROR_REGISTRY)) throw new Error("invalid RiffDB application error");
  const rule = APPLICATION_ERROR_REGISTRY[code as ApplicationErrorCode];
  if (input.type !== "application" || input.message !== rule[0] || input.category !== rule[1]
      || input.recoveryAction !== rule[2] || typeof input.operation !== "string"
      || !APPLICATION_OPERATIONS.has(input.operation)) throw new Error("invalid RiffDB application error");
  if (!Array.isArray(input.fixes) || input.fixes.length !== rule[3].length
      || input.fixes.some((fix, index) => fix !== rule[3][index])) throw new Error("invalid RiffDB application error");
  const symbolPath = input.symbolPath;
  if (!Array.isArray(symbolPath) || symbolPath.length > 16
      || symbolPath.some((symbol) => typeof symbol !== "string" || !SAFE_SYMBOL.test(symbol))) throw new Error("invalid RiffDB application error");
  for (const key of ["contractLineage", "operationSymbol"] as const) {
    if (input[key] !== undefined && (typeof input[key] !== "string" || !SAFE_SYMBOL.test(input[key]))) throw new Error("invalid RiffDB application error");
  }
  if ((input.contractLineage === undefined) !== (input.contractVersion === undefined)
      || (input.contractVersion !== undefined && (!Number.isSafeInteger(input.contractVersion) || (input.contractVersion as number) < 1))) throw new Error("invalid RiffDB application error");
  if (input.sourceSpan !== undefined) {
    const span = input.sourceSpan as Record<string, unknown>;
    if (typeof span !== "object" || span === null || !Number.isSafeInteger(span.start)
        || !Number.isSafeInteger(span.end) || (span.start as number) < 0
        || (span.start as number) > (span.end as number) || (span.end as number) > 262144) throw new Error("invalid RiffDB application error");
  }
  for (const key of ["traceId", "incidentId"] as const) {
    if (input[key] !== undefined && (typeof input[key] !== "string" || !UUID_V7.test(input[key]))) throw new Error("invalid RiffDB application error");
  }
  return new RiffDbApplicationError({
    type: "application", code: code as ApplicationErrorCode, message: rule[0],
    category: rule[1], recoveryAction: rule[2], operation: input.operation as ApplicationOperation,
    symbolPath: symbolPath as ReadonlyArray<string>,
    fixes: rule[3],
    ...(input.contractLineage === undefined ? {} : { contractLineage: input.contractLineage as string }),
    ...(input.contractVersion === undefined ? {} : { contractVersion: input.contractVersion as number }),
    ...(input.operationSymbol === undefined ? {} : { operationSymbol: input.operationSymbol as string }),
    ...(input.sourceSpan === undefined ? {} : { sourceSpan: input.sourceSpan as { readonly start: number; readonly end: number } }),
    ...(input.traceId === undefined ? {} : { traceId: input.traceId as string }),
    ...(input.incidentId === undefined ? {} : { incidentId: input.incidentId as string }),
  });
}

"#,
    );
}

#[cfg(test)]
fn emit_typescript_client_facade(
    output: &mut String,
    module: &QueryModule,
    commands: &[&CommandPlan],
    contract: &ContractBundle,
    vector_driver_operations: &BTreeMap<(String, String, String), (String, String)>,
) {
    let client_name = format!("{}Client", pascal(module.contract_lineage().as_str()));
    writeln!(
        output,
        "export class {client_name} {{\n  public constructor(\n    private readonly transport: ApplicationTransport,\n    \
         private readonly commandAttemptBudget: number,\n  ) {{\n    if (!Number.isInteger(commandAttemptBudget) || commandAttemptBudget < 1) \
         throw new Error(\"invalid command attempt budget\");\n  }}\n"
    )
    .infallible();
    for query in module.queries() {
        let name = query.name();
        let cursor = query
            .plan()
            .schemas()
            .parameters()
            .iter()
            .find(|parameter| is_cursor_type(parameter.value_type()));
        let (parameter_routing, options_name) = cursor.map_or_else(
            || (String::new(), "options".to_owned()),
            |parameter| {
                let field = ts_identifier(parameter.name());
                (
                    format!(
                        "    const {{ {field}: generatedCursor, ...routedParameters }} = parameters;\n    if (generatedCursor != null && options.cursor !== undefined) throw new Error(\"generated cursor conflicts with query options\");\n    const routedOptions: QueryOptions = generatedCursor == null ? options : {{ ...options, cursor: generatedCursor }};\n"
                    ),
                    "routedOptions".to_owned(),
                )
            },
        );
        let request_argument = if cursor.is_some() {
            format!("routedParameters as unknown as {name}Params")
        } else {
            "parameters".to_owned()
        };
        writeln!(
            output,
            "  public async {function}(parameters: {name}Params, options: QueryOptions = {{}}): \
             Promise<TypedQueryResult<{name}Result>> {{\n{parameter_routing}    const request = {function}({request_argument});\n    \
             const result = await this.transport.executeNamedQuery<{name}Params, {name}Result>(request, {options_name});\n    \
             if (!acceptsIdentity(request, result.identity)) throw new Error(\"RiffDB application identity mismatch\");\n    return result;\n  }}\n",
            function = camel(name)
        )
        .infallible();
    }
    for command in commands {
        let name = command.name();
        let workflow_revisions = workflow_revision_bindings(command);
        let success_outcome = workflow_success_outcome_name(command);
        writeln!(
            output,
            "  public async {function}(input: {name}Input): Promise<TypedCommandResult<{name}Outcome>> {{\n    \
             const result = await this.transport.executeCommand<{name}Input, {name}Outcome>({function}(input), this.commandAttemptBudget);\n    \
             if (result.contractVersion !== CONTRACT_VERSION || result.planHash !== {constant}_PLAN_HASH) \
             throw new Error(\"RiffDB application identity mismatch\");",
            function = camel(name),
            constant = screaming_snake(name),
        )
        .infallible();
        if workflow_revisions.is_empty() {
            writeln!(output, "    return result;\n  }}\n").infallible();
        } else {
            writeln!(
                output,
                "    if (result.outcome.outcome !== {success_outcome:?}) return {{ ...result, workflowRevisions: [] }};"
            )
            .infallible();
            for revision in &workflow_revisions {
                writeln!(
                    output,
                    "    if (input.{} === 18446744073709551615n) throw new Error(\"RiffDB workflow successor revision overflow\");",
                    ts_identifier(revision.input_name),
                )
                .infallible();
            }
            writeln!(output, "    return {{ ...result, workflowRevisions: [").infallible();
            for revision in workflow_revisions {
                writeln!(
                    output,
                    "      {{ binding: {:?}, revision: input.{} + 1n }},",
                    revision.binding_name,
                    ts_identifier(revision.input_name),
                )
                .infallible();
            }
            writeln!(output, "    ] }};\n  }}\n").infallible();
        }
        writeln!(
            output,
            "  public async {function}Batch(inputs: ReadonlyArray<{name}Input>, options: CommandBatchOptions): Promise<CommandBatchResult<{name}Outcome>> {{\n    \
             if (!Number.isInteger(options.concurrency) || options.concurrency < 1 || options.concurrency > MAX_COMMAND_BATCH_CONCURRENCY \
             || inputs.length < 1 || inputs.length > 4096) throw new Error(\"invalid command batch bounds\");\n    \
             const start = options.checkpoint ?? 0;\n    if (!Number.isInteger(start) || start < 0 || start > inputs.length) throw new Error(\"invalid command batch checkpoint\");\n    \
             if (start === inputs.length) return {{ items: [], checkpoint: start }};\n    \
             if (this.transport.executeCommandBatch !== undefined) {{ const result = await this.transport.executeCommandBatch(\
             {function}(inputs[start]!), inputs, options.concurrency, start, this.commandAttemptBudget); \
             options.onProgress?.({{ completed: result.checkpoint, total: inputs.length, checkpoint: result.checkpoint }}); return result; }}\n    \
             const items: CommandBatchItem<{name}Outcome>[] = [];\n    let next = start;\n    let completed = start;\n    let checkpoint = start;\n    const completedAfterCheckpoint = new Set<number>();\n    \
             const worker = async (): Promise<void> => {{ while (true) {{ const index = next++; if (index >= inputs.length) return; \
             try {{ items.push({{ index, result: await this.{function}(inputs[index]!) }}); }} catch (error) {{ items.push({{ index, error }}); }} \
             completed += 1; completedAfterCheckpoint.add(index); while (completedAfterCheckpoint.delete(checkpoint)) checkpoint += 1; \
             options.onProgress?.({{ completed, total: inputs.length, checkpoint }}); }} }};\n    \
             await Promise.all(Array.from({{ length: Math.min(options.concurrency, inputs.length - start) }}, worker));\n    \
             items.sort((left, right) => left.index - right.index);\n    return {{ items, checkpoint }};\n  }}\n",
            function = camel(name),
        )
        .infallible();
    }
    for inspection in vector_inspection_facades(module, contract) {
        let method = format!(
            "inspect{}{}",
            pascal(&inspection.entity),
            pascal(&inspection.field)
        );
        let partition_type = ts_contract_type(&inspection.partition_type, contract);
        let partition_schema = ts_contract_value_schema(&inspection.partition_type, contract);
        for (kind, suffix, result_type) in [
            ("staleness", "Staleness", "VectorStalenessResult"),
            (
                "model_versions",
                "ModelVersions",
                "VectorModelVersionResult",
            ),
        ] {
            let (operation, schema_hash) = vector_driver_operations
                .get(&(
                    inspection.entity.clone(),
                    inspection.field.clone(),
                    kind.to_owned(),
                ))
                .expect("generated vector inspection operation");
            writeln!(
                output,
                "  public async {method}{suffix}(partition: {partition_type}, limit = 50, options: VectorInspectionOptions = {{}}): Promise<TypedVectorInspectionResult<{result_type}>> {{\n    if (!Number.isInteger(limit) || limit < 1 || limit > 500) throw new Error(\"invalid vector inspection limit\");\n    if (this.transport.executeVectorInspection === undefined) throw new Error(\"RiffDB transport does not support vector inspection\");\n    return this.transport.executeVectorInspection<{partition_type}, {result_type}>({{ driverOperation: {{ name: {operation:?}, inputSchemaHash: {schema_hash:?} }}, contractLineage: CONTRACT_LINEAGE, contractVersion: CONTRACT_VERSION, contractBundleHash: CONTRACT_BUNDLE_HASH, entity: {entity:?}, field: {field:?}, inspectionKind: {kind:?}, partition, partitionSchema: {partition_schema}, limit }}, options);\n  }}\n",
                entity = inspection.entity,
                field = inspection.field,
            )
            .infallible();
        }
    }
    writeln!(output, "}}").infallible();
}

fn rust_query_type(value_type: &NamedTypeSchema, nested_name: &str) -> String {
    match value_type {
        NamedTypeSchema::Scalar(name) => match name.as_str() {
            "bool" => "bool".to_owned(),
            "i64" => "i64".to_owned(),
            "u64" => "u64".to_owned(),
            "uuid" => "String".to_owned(),
            "timestamp" => "TimestampValue".to_owned(),
            "date" => "i32".to_owned(),
            value if value.starts_with("bytes<") => "Vec<u8>".to_owned(),
            value if value.starts_with("vector<") => "CanonicalVector".to_owned(),
            value if value.starts_with("decimal<") => "DecimalValue".to_owned(),
            value if value.starts_with("money<") => "MoneyValue".to_owned(),
            value if value.starts_with("string<") => "String".to_owned(),
            _ => "String".to_owned(),
        },
        NamedTypeSchema::Optional(inner) => {
            format!("Option<{}>", rust_query_type(inner, nested_name))
        }
        NamedTypeSchema::Set(inner)
        | NamedTypeSchema::BoundedSet { element: inner, .. }
        | NamedTypeSchema::List { element: inner, .. } => {
            format!("Vec<{}>", rust_query_type(inner, nested_name))
        }
        NamedTypeSchema::Record(_) => nested_name.to_owned(),
        NamedTypeSchema::Cursor => "String".to_owned(),
        NamedTypeSchema::Limit => "u64".to_owned(),
        NamedTypeSchema::BoundedLimit { .. } => "u64".to_owned(),
    }
}

fn rust_contract_type(value_type: &ValueType, contract: &ContractBundle) -> String {
    if let Some(inner) = value_type.optional_inner() {
        return format!("Option<{}>", rust_contract_type(inner, contract));
    }
    if let Some((inner, _)) = value_type.list_parts() {
        return format!("Vec<{}>", rust_contract_type(inner, contract));
    }
    match value_type.tag() {
        ValueTypeTag::Bool => "bool",
        ValueTypeTag::I64 => "i64",
        ValueTypeTag::U64 => "u64",
        ValueTypeTag::Bytes => "Vec<u8>",
        ValueTypeTag::Decimal => "DecimalValue",
        ValueTypeTag::Money => "MoneyValue",
        ValueTypeTag::Timestamp => "TimestampValue",
        ValueTypeTag::Date => "i32",
        ValueTypeTag::Vector => "CanonicalVector",
        ValueTypeTag::Record => match value_type.record_ref() {
            Some(RecordTypeRef::Entity(entity_id)) => {
                return contract
                    .schema()
                    .entity(*entity_id)
                    .expect("validated record entity")
                    .name()
                    .to_owned();
            }
            _ => "String",
        },
        _ => "String",
    }
    .to_owned()
}

fn ts_query_parameter_type(
    query: &crate::CompiledNamedQuery,
    parameter: &riffdb_query_ir::NamedParameterSchema,
) -> String {
    if let Some(family) = query.order_family()
        && family.selector_parameter() == parameter.name()
    {
        return family
            .members()
            .iter()
            .map(|member| format!("{:?}", member.variant_name()))
            .collect::<Vec<_>>()
            .join(" | ");
    }
    ts_query_type(parameter.value_type())
}

fn ts_query_type(value_type: &NamedTypeSchema) -> String {
    match value_type {
        NamedTypeSchema::Scalar(name) => match name.as_str() {
            "bool" => "boolean".to_owned(),
            "i64" | "u64" => "bigint".to_owned(),
            "timestamp" => "{ readonly seconds: bigint; readonly nanos: number }".to_owned(),
            "date" => "number".to_owned(),
            value if value.starts_with("bytes<") => "Uint8Array".to_owned(),
            value if value.starts_with("vector<") => "ReadonlyArray<number>".to_owned(),
            value if value.starts_with("decimal<") => {
                "{ readonly coefficientTwosComplement: Uint8Array; readonly scale: number; readonly precision: number }".to_owned()
            }
            value if value.starts_with("money<") => {
                "{ readonly currency: string; readonly amount: { readonly coefficientTwosComplement: Uint8Array; readonly scale: number; readonly precision: number } }".to_owned()
            }
            _ => "string".to_owned(),
        },
        NamedTypeSchema::Optional(inner) => format!("{} | null", ts_query_type(inner)),
        NamedTypeSchema::Set(inner)
        | NamedTypeSchema::BoundedSet { element: inner, .. }
        | NamedTypeSchema::List { element: inner, .. } => {
            format!("ReadonlyArray<{}>", ts_query_type(inner))
        }
        NamedTypeSchema::Record(fields) => {
            let body = fields
                .iter()
                .map(|field| {
                    format!(
                        "readonly {}: {}",
                        ts_identifier(field.name()),
                        ts_query_type(field.value_type())
                    )
                })
                .collect::<Vec<_>>()
                .join("; ");
            format!("{{ {body} }}")
        }
        NamedTypeSchema::Cursor => "string".to_owned(),
        NamedTypeSchema::Limit => "number".to_owned(),
        NamedTypeSchema::BoundedLimit { .. } => "number".to_owned(),
    }
}

fn ts_named_record_schema<'a>(
    fields: impl Iterator<Item = (&'a str, &'a NamedTypeSchema)>,
    contract: &ContractBundle,
) -> Value {
    json!({
        "kind": "record",
        "fields": fields.map(|(name, schema)| json!({
            "name": name,
            "schema": ts_named_value_schema(schema, contract),
        })).collect::<Vec<_>>(),
    })
}

fn ts_named_value_schema(value_type: &NamedTypeSchema, contract: &ContractBundle) -> Value {
    match value_type {
        NamedTypeSchema::Scalar(name) => {
            if let Some(dimension) = vector_type_dimension(name) {
                return json!({"kind": "vector", "dimension": dimension});
            }
            if let Some((precision, scale)) = decimal_type_parts(name) {
                return json!({"kind": "decimal", "precision": precision, "scale": scale});
            }
            if let Some(currency) = money_type_currency(name) {
                return json!({
                    "kind": "money",
                    "precision": 38,
                    "scale": 2,
                    "currency": currency,
                });
            }
            let kind = match name.as_str() {
                "bool" => "bool",
                "i64" => "i64",
                "u64" => "u64",
                "uuid" => "uuid",
                "timestamp" => "timestamp",
                "date" => "date",
                value if value.starts_with("bytes<") => "bytes",
                value
                    if contract
                        .schema()
                        .enums()
                        .iter()
                        .any(|enumeration| enumeration.name() == value) =>
                {
                    "enum"
                }
                _ => "string",
            };
            json!({"kind": kind})
        }
        NamedTypeSchema::Optional(inner) => {
            json!({"kind": "optional", "value": ts_named_value_schema(inner, contract)})
        }
        NamedTypeSchema::Set(inner) => {
            json!({"kind": "list", "value": ts_named_value_schema(inner, contract)})
        }
        NamedTypeSchema::BoundedSet { element, maximum } => {
            json!({
                "kind": "list",
                "value": ts_named_value_schema(element, contract),
                "maximum": maximum,
                "unique": true
            })
        }
        NamedTypeSchema::Record(fields) => ts_named_record_schema(
            fields
                .iter()
                .map(|field| (field.name(), field.value_type())),
            contract,
        ),
        NamedTypeSchema::List { element, maximum } => {
            let mut schema = Map::new();
            schema.insert("kind".to_owned(), Value::String("list".to_owned()));
            schema.insert("value".to_owned(), ts_named_value_schema(element, contract));
            if let PageBound::Literal(value) = maximum {
                schema.insert("maximum".to_owned(), Value::from(*value));
            }
            Value::Object(schema)
        }
        NamedTypeSchema::Cursor => json!({"kind": "cursor"}),
        NamedTypeSchema::Limit => json!({"kind": "limit"}),
        NamedTypeSchema::BoundedLimit { maximum } => {
            json!({"kind": "limit", "maximum": maximum})
        }
    }
}

fn ts_contract_record_schema<'a>(
    fields: impl Iterator<Item = (&'a str, u32, &'a ValueType)>,
    contract: &ContractBundle,
    wire_ids: bool,
) -> Value {
    ts_contract_record_schema_with_bounds(fields, contract, wire_ids, false)
}

fn ts_contract_record_schema_with_bounds<'a>(
    fields: impl Iterator<Item = (&'a str, u32, &'a ValueType)>,
    contract: &ContractBundle,
    wire_ids: bool,
    include_value_bounds: bool,
) -> Value {
    json!({
        "kind": "record",
        "fields": fields.map(|(name, wire_id, schema)| {
            let mut field = Map::new();
            field.insert("name".to_owned(), Value::String(name.to_owned()));
            field.insert(
                "schema".to_owned(),
                ts_contract_value_schema_with_bounds(schema, contract, include_value_bounds),
            );
            if wire_ids {
                field.insert("wireId".to_owned(), Value::from(wire_id));
            }
            Value::Object(field)
        }).collect::<Vec<_>>(),
    })
}

fn ts_contract_value_schema(value_type: &ValueType, contract: &ContractBundle) -> Value {
    ts_contract_value_schema_with_bounds(value_type, contract, false)
}

fn ts_contract_value_schema_with_bounds(
    value_type: &ValueType,
    contract: &ContractBundle,
    include_value_bounds: bool,
) -> Value {
    if let Some(inner) = value_type.optional_inner() {
        return json!({
            "kind": "optional",
            "value": ts_contract_value_schema_with_bounds(inner, contract, include_value_bounds),
        });
    }
    if let Some((inner, maximum)) = value_type.list_parts() {
        return json!({
            "kind": "list",
            "value": ts_contract_value_schema_with_bounds(inner, contract, include_value_bounds),
            "maximum": maximum,
        });
    }
    if let Some(spec) = value_type.decimal_spec() {
        return json!({
            "kind": "decimal",
            "precision": spec.precision(),
            "scale": spec.scale(),
        });
    }
    if let Some(currency) = value_type.currency() {
        return json!({
            "kind": "money",
            "precision": 38,
            "scale": 2,
            "currency": currency.to_string(),
        });
    }
    if let Some(dimension) = value_type.vector_dimension() {
        return json!({"kind": "vector", "dimension": dimension.get()});
    }
    let kind = match value_type.tag() {
        ValueTypeTag::Bool => "bool",
        ValueTypeTag::I64 => "i64",
        ValueTypeTag::U64 => "u64",
        ValueTypeTag::Decimal | ValueTypeTag::Money => {
            unreachable!("exact numeric schemas returned above")
        }
        ValueTypeTag::String => "string",
        ValueTypeTag::Bytes => "bytes",
        ValueTypeTag::Timestamp => "timestamp",
        ValueTypeTag::Date => "date",
        ValueTypeTag::Uuid => "uuid",
        ValueTypeTag::Enum => "enum",
        ValueTypeTag::Record => {
            if let Some(RecordTypeRef::Entity(entity_id)) = value_type.record_ref() {
                let entity = contract
                    .schema()
                    .entity(*entity_id)
                    .expect("validated record entity");
                return ts_contract_record_schema_with_bounds(
                    wire_model_fields(entity.record())
                        .map(|field| (field.name(), field.id().get(), field.value_type())),
                    contract,
                    true,
                    include_value_bounds,
                );
            }
            "string"
        }
        ValueTypeTag::Vector => unreachable!("vector schema returned above"),
        ValueTypeTag::Optional | ValueTypeTag::List => {
            unreachable!("handled above")
        }
    };
    let mut schema = Map::from_iter([("kind".to_owned(), Value::String(kind.to_owned()))]);
    if include_value_bounds
        && matches!(value_type.tag(), ValueTypeTag::String | ValueTypeTag::Bytes)
    {
        schema.insert(
            "maximumBytes".to_owned(),
            Value::from(value_type.byte_bound().expect("bounded text or bytes")),
        );
    }
    Value::Object(schema)
}

fn ts_contract_type(value_type: &ValueType, contract: &ContractBundle) -> String {
    if let Some(inner) = value_type.optional_inner() {
        return format!("{} | null", ts_contract_type(inner, contract));
    }
    if let Some((inner, _)) = value_type.list_parts() {
        return format!("ReadonlyArray<{}>", ts_contract_type(inner, contract));
    }
    match value_type.tag() {
        ValueTypeTag::Bool => "boolean",
        ValueTypeTag::I64 | ValueTypeTag::U64 => "bigint",
        ValueTypeTag::Bytes => "Uint8Array",
        ValueTypeTag::Timestamp => "{ readonly seconds: bigint; readonly nanos: number }",
        ValueTypeTag::Date => "number",
        ValueTypeTag::Vector => "ReadonlyArray<number>",
        ValueTypeTag::Decimal => {
            "{ readonly coefficientTwosComplement: Uint8Array; readonly scale: number; readonly precision: number }"
        }
        ValueTypeTag::Money => {
            "{ readonly currency: string; readonly amount: { readonly coefficientTwosComplement: Uint8Array; readonly scale: number; readonly precision: number } }"
        }
        ValueTypeTag::Record => match value_type.record_ref() {
            Some(RecordTypeRef::Entity(entity_id)) => {
                return format!(
                    "{{ {} }}",
                    contract
                        .schema()
                        .entity(*entity_id)
                        .expect("validated record entity")
                        .record()
                        .fields()
                        .iter()
                        .map(|field| format!(
                            "readonly {}: {}",
                            ts_identifier(field.name()),
                            ts_contract_type(field.value_type(), contract)
                        ))
                        .collect::<Vec<_>>()
                        .join("; ")
                );
            }
            _ => "string",
        },
        _ => "string",
    }
    .to_owned()
}

fn rust_identifier(name: &str) -> String {
    match name {
        "type" | "match" | "ref" | "self" | "crate" | "super" | "move" | "where" | "loop"
        | "async" | "await" | "dyn" | "enum" | "struct" | "fn" | "mod" | "use" | "pub" | "impl"
        | "trait" | "const" | "static" | "let" | "in" | "as" | "for" | "while" | "return"
        | "break" | "continue" | "true" | "false" => format!("r#{name}"),
        _ => name.to_owned(),
    }
}

fn ts_identifier(name: &str) -> String {
    if name
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        name.to_owned()
    } else {
        format!("\"{name}\"")
    }
}

fn snake(name: &str) -> String {
    separated(name, '_', false)
}

fn screaming_snake(name: &str) -> String {
    snake(name).to_ascii_uppercase()
}

fn camel(name: &str) -> String {
    let mut chars = name.chars();
    chars
        .next()
        .map(|first| first.to_ascii_lowercase().to_string() + chars.as_str())
        .unwrap_or_default()
}

fn pascal(name: &str) -> String {
    if name.bytes().all(|byte| byte.is_ascii_alphanumeric()) {
        let mut chars = name.chars();
        return chars
            .next()
            .map(|first| first.to_ascii_uppercase().to_string() + chars.as_str())
            .unwrap_or_default();
    }
    separated(name, '\0', true)
}

fn separated(name: &str, separator: char, upper_words: bool) -> String {
    let mut output = String::new();
    let mut word_start = true;
    for character in name.chars() {
        if !character.is_ascii_alphanumeric() {
            word_start = true;
            continue;
        }
        if separator != '\0'
            && ((character.is_ascii_uppercase() && !word_start)
                || (word_start && !output.is_empty()))
        {
            output.push(separator);
        }
        if upper_words && word_start {
            output.push(character.to_ascii_uppercase());
        } else {
            output.push(character.to_ascii_lowercase());
        }
        word_start = false;
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
    use super::*;
    use crate::{NamedQuerySource, QueryModuleCandidate, generate_python_client};
    use riffdb_contract_compiler::compile_contract_source;
    use riffdb_types::{QueryModuleName, QueryModuleVersion};

    #[test]
    fn optional_wire_decode_evaluates_the_field_take_once() {
        let contract = compile_contract_source(include_str!(
            "../../../examples/app-baseline/contracts/ticketdesk.riff"
        ))
        .expect("contract");
        let optional = ValueType::optional(ValueType::i64()).expect("optional i64");
        let access = "take_wire_field(&mut fields, 12)?";

        let expression = rust_decode_wire_expr(&optional, access, &contract);

        assert_eq!(expression.matches(access).count(), 1);
        assert_eq!(
            expression,
            "decode_wire_optional(take_wire_field(&mut fields, 12)?, decode_wire_i64)?"
        );
    }

    #[test]
    fn nested_optional_query_result_uses_record_field_presence_for_outer_absence() {
        let value_type = NamedTypeSchema::Optional(Box::new(NamedTypeSchema::Optional(Box::new(
            NamedTypeSchema::Scalar("i64".to_owned()),
        ))));
        let expression = rust_decode_record_field_expr(&value_type, "minimum", "Minimum");

        assert_eq!(expression.matches("record.fields.remove").count(), 1);
        assert!(expression.contains("None => None"));
        assert!(expression.contains("Some(value) => Some(match value"));
        assert!(expression.contains("ApplicationValue::Null => None"));
    }

    #[test]
    fn generated_rust_facade_owns_query_options_and_read_after_commit() {
        let contract = compile_contract_source(
            "contract Example version 1 {\n\
             entity Item { key (item_id: uuid) field title: string<128> }\n\
             aggregate ItemRoot { root Item partition_by item_id conflict_key (item_id) }\n\
             command CreateItem { input idempotency_key: string<128> input item_id: uuid input title: string<128> idempotency_key idempotency_key create Item(item_id) as item else ItemExists { item_id: item_id } set item.title = title return Created { item: item } }\n\
             }",
        )
        .expect("contract");
        let query = NamedQuerySource::new(
            "ItemPage",
            "query ItemPage($item_id: Item.item_id) { one item from Item where item_id == $item_id else NotFound return Found { item: item { item_id title } } outcomes Found | NotFound }",
        )
        .expect("query source");
        let candidate = QueryModuleCandidate::new(
            QueryModuleName::new("ExampleQueries").expect("module name"),
            QueryModuleVersion::new(1).expect("module version"),
            vec![query],
        )
        .expect("candidate");
        let module = QueryModule::compile(candidate, &contract).expect("module");

        let generated = generate_rust_client(&module, &contract);
        let generated_typescript = generate_typescript_client(&module, &contract);

        assert!(generated.contains("pub use riffdb_client_rust::QueryOptions;"));
        assert!(generated.contains("pub async fn item_page_after_commit("));
        assert!(generated.contains("QueryOptions::new().read_after_commit(commit_sequence)"));
        assert!(generated_typescript.contains("MAX_COMMAND_BATCH_CONCURRENCY = 384"));
        assert!(
            generated_typescript.contains("options.concurrency > MAX_COMMAND_BATCH_CONCURRENCY")
        );
        assert!(!generated_typescript.contains("options.concurrency > 32"));
        assert!(!generated_typescript.contains("function compactString("));
        assert!(!generated_typescript.contains("function compactInteger("));
        assert!(!generated_typescript.contains("function compactTimestamp("));
    }

    // req: OQ-004, OQ-006, OQ-015, OQ-016, OQ-031, API-001, SAFE-001
    #[test]
    fn generated_facades_route_typed_cursors_only_through_query_options() {
        let contract = compile_contract_source(include_str!(
            "../../../examples/app-baseline/contracts/ticketdesk.riff"
        ))
        .expect("TicketDesk contract");
        let query = NamedQuerySource::new(
            "ListComments",
            include_str!("../../../queries/ticketdesk/list_comments.riffq"),
        )
        .expect("query source");
        let module = QueryModule::compile(
            QueryModuleCandidate::new(
                QueryModuleName::new("TicketDeskComments").expect("module name"),
                QueryModuleVersion::new(1).expect("module version"),
                vec![query],
            )
            .expect("candidate"),
            &contract,
        )
        .expect("module");

        let rust = generate_rust_client(&module, &contract);
        let go = crate::generate_go_client(&module, &contract);
        let typescript = generate_typescript_client(&module, &contract);
        let python = generate_python_client(&module, &contract).expect("Python");

        assert!(rust.contains("let generated_cursor = self.0.after;"));
        assert!(rust.contains("options.with_generated_cursor(generated_cursor)?"));
        assert!(!rust.contains("parameters.insert(\"after\""));
        assert!(rust.contains("parameters: ListCommentsParams, options: QueryOptions"));
        assert!(!rust.contains("list_comments_all"));

        assert!(go.contains("options.Cursor = *parameters.After"));
        assert!(go.contains("if options.Cursor != \"\""));
        assert!(!go.contains("input[\"after\"]"));
        assert!(!go.contains("ListCommentsAll"));

        assert!(typescript.contains("after: generatedCursor, ...routedParameters"));
        assert!(typescript.contains("cursor: generatedCursor"));
        assert!(typescript.contains("options.cursor !== undefined"));
        assert!(!typescript.contains("const request = listComments(parameters);"));
        assert!(!typescript.contains("listCommentsAll"));

        assert!(python.contains("generated_cursor = parameters.after"));
        assert!(python.contains("encoded_parameters.pop(\"after\", None)"));
        assert!(python.contains("QueryOptions(cursor=generated_cursor"));
        assert!(python.contains("if options.cursor is not None:"));
        assert!(!python.contains("list_comments_all"));
    }

    // req: OQ-031
    #[test]
    fn generated_facades_preserve_required_cursor_parameter_types() {
        let contract = compile_contract_source(include_str!(
            "../../../examples/app-baseline/contracts/ticketdesk.riff"
        ))
        .expect("TicketDesk contract");
        let source = include_str!("../../../queries/ticketdesk/list_comments.riffq")
            .replace("$after: Cursor?,", "$after: Cursor,");
        let module = QueryModule::compile(
            QueryModuleCandidate::new(
                QueryModuleName::new("TicketDeskRequiredCursor").expect("module name"),
                QueryModuleVersion::new(1).expect("module version"),
                vec![NamedQuerySource::new("ListComments", source).expect("query source")],
            )
            .expect("candidate"),
            &contract,
        )
        .expect("required cursor module");

        let rust = generate_rust_client(&module, &contract);
        let go = crate::generate_go_client(&module, &contract);
        let typescript = generate_typescript_client(&module, &contract);
        let python = generate_python_client(&module, &contract).expect("Python");

        assert!(rust.contains("pub after: String,"));
        assert!(rust.contains("let generated_cursor = Some(self.0.after);"));
        assert!(go.contains("After string"));
        assert!(go.contains("options.Cursor = parameters.After"));
        assert!(typescript.contains("readonly after: string;"));
        assert!(python.contains("after: str\n"));
    }

    // req: OQ-004, OQ-006, OQ-016, OQ-058, OQ-061
    #[test]
    fn generated_paginated_mcp_queries_expose_collision_safe_cursor_envelope() {
        let contract = compile_contract_source(include_str!(
            "../../../examples/app-baseline/contracts/ticketdesk.riff"
        ))
        .expect("TicketDesk contract");
        let query = NamedQuerySource::new(
            "ListComments",
            include_str!("../../../queries/ticketdesk/list_comments.riffq"),
        )
        .expect("query source");
        let module = QueryModule::compile(
            QueryModuleCandidate::new(
                QueryModuleName::new("TicketDeskComments").expect("module name"),
                QueryModuleVersion::new(1).expect("module version"),
                vec![query],
            )
            .expect("candidate"),
            &contract,
        )
        .expect("module");

        let [tool] = generate_mcp_tools(&module)
            .expect("MCP generation")
            .try_into()
            .expect("one generated tool");
        let input: Value = serde_json::from_str(&tool.input_schema).expect("input schema");
        let result: Value = serde_json::from_str(&tool.result_schema).expect("result schema");

        assert_eq!(input["x-riffdb-continuationParameter"], "after");
        assert!(
            !input["required"]
                .as_array()
                .expect("required")
                .contains(&json!("after"))
        );
        assert_eq!(
            input["properties"]["after"],
            json!({
                "anyOf": [{
                    "type": "string",
                    "pattern": "^[A-Za-z0-9_-]{22}$",
                    "minLength": 22,
                    "maxLength": 22,
                }, {"type": "null"}],
            })
        );
        assert_eq!(result["x-riffdb-resultProfile"], "paginated-envelope-v1");
        assert_eq!(result["additionalProperties"], false);
        assert_eq!(result["required"], json!(["result", "page"]));
        assert!(result["properties"]["result"]["oneOf"].is_array());
        assert_eq!(
            result["properties"]["page"],
            json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "next_cursor": {
                        "anyOf": [{
                            "type": "string",
                            "pattern": "^[A-Za-z0-9_-]{22}$",
                            "minLength": 22,
                            "maxLength": 22,
                        }, {"type": "null"}],
                    },
                },
                "required": ["next_cursor"],
            })
        );
        assert!(tool.input_schema.len() <= 65_536);
        assert!(tool.result_schema.len() <= 65_536);

        let business = serde_json::to_string(&json!({"outcome":"Found"})).expect("business");
        let with_cursor = serde_json::to_string(&json!({
            "result": {"outcome":"Found"},
            "page": {"next_cursor":"AAAAAAAAAAAAAAAAAAAAAA"},
        }))
        .expect("cursor envelope");
        let final_page = serde_json::to_string(&json!({
            "result": {"outcome":"Found"},
            "page": {"next_cursor": null},
        }))
        .expect("final envelope");
        assert_eq!(with_cursor.len(), business.len() + 59);
        assert_eq!(final_page.len(), business.len() + 39);
    }

    // req: OQ-031, SAFE-001
    #[test]
    fn query_module_rejects_cursor_parameters_without_one_continuation_slot() {
        let contract = compile_contract_source(include_str!(
            "../../../examples/app-baseline/contracts/ticketdesk.riff"
        ))
        .expect("TicketDesk contract");
        let source = include_str!("../../../queries/ticketdesk/list_comments.riffq").replace(
            "$after: Cursor?,",
            "$after: Cursor?,\n  $unrouted: Cursor?,",
        );
        let candidate = QueryModuleCandidate::new(
            QueryModuleName::new("TicketDeskAmbiguousCursor").expect("module name"),
            QueryModuleVersion::new(1).expect("module version"),
            vec![NamedQuerySource::new("ListComments", source).expect("query source")],
        )
        .expect("candidate");

        assert!(
            QueryModule::compile(candidate, &contract).is_err(),
            "an unbound cursor parameter would have no protected continuation route"
        );
    }

    // req: OQ-004, OQ-006, OQ-016, OQ-031
    #[test]
    fn generated_cursor_fixture_covers_closed_continuation_failures_and_bounds() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../fixtures/riffql/generated-cursor-routing-v1.json"
        ))
        .expect("cursor routing fixture");
        assert_eq!(
            fixture["languages"],
            serde_json::json!(["rust", "go", "typescript", "python"])
        );
        assert_eq!(fixture["transport"]["field"], "Options.Cursor");
        assert_eq!(fixture["transport"]["cursor_bytes"], "unchanged");
        assert_eq!(fixture["transport"]["symbolic_parameter"], "omitted");
        assert_eq!(fixture["transport"]["maximum_pages_per_invocation"], 1);
        assert_eq!(fixture["transport"]["generated_page_walks"], false);
        let cases = fixture["cases"]
            .as_array()
            .expect("cursor routing cases")
            .iter()
            .map(|case| case["name"].as_str().expect("case name"))
            .collect::<Vec<_>>();
        assert_eq!(
            cases,
            [
                "first_page",
                "continuation",
                "null",
                "malformed",
                "wrong_operation",
                "wrong_database_history",
                "expiry",
                "cancellation",
                "bounded_response",
            ]
        );
    }

    #[test]
    fn typescript_compact_helpers_follow_decoder_reachability() {
        let mut output = String::new();
        emit_typescript_compact_helpers(
            &mut output,
            TypescriptCompactHelperUsage {
                payload: true,
                timestamp: true,
                ..TypescriptCompactHelperUsage::default()
            },
        );

        assert!(output.contains("function compactPayload("));
        assert!(output.contains("function compactTimestamp("));
        assert!(!output.contains("function compactString("));
        assert!(!output.contains("function compactInteger("));

        let mut empty = String::new();
        emit_typescript_compact_helpers(&mut empty, TypescriptCompactHelperUsage::default());
        assert!(empty.is_empty());
    }

    // req: GEN-004
    #[test]
    fn typescript_template_matches_the_independent_legacy_rendering_oracle() {
        let contract = compile_contract_source(include_str!(
            "../../../examples/app-baseline/contracts/ticketdesk.riff"
        ))
        .expect("TicketDesk contract");
        let query = NamedQuerySource::new(
            "ListComments",
            include_str!("../../../queries/ticketdesk/list_comments.riffq"),
        )
        .expect("query source");
        let module = QueryModule::compile(
            QueryModuleCandidate::new(
                QueryModuleName::new("TicketDeskComments").expect("module name"),
                QueryModuleVersion::new(1).expect("module version"),
                vec![query],
            )
            .expect("candidate"),
            &contract,
        )
        .expect("module");

        assert_eq!(
            generate_typescript_client(&module, &contract),
            typescript_generation_source(&module, &contract, &[]),
        );
    }

    #[test]
    fn generated_rust_facade_exposes_role_derived_vector_inspection() {
        let contract = compile_contract_source(include_str!(
            "../../../fixtures/compiler/production-embedding/contract.riff"
        ))
        .expect("production vector contract");
        let query = NamedQuerySource::new(
            "SimilarDocuments",
            r#"query SimilarDocuments(
                $org_id: Document.org_id,
                $query_vec: Document.embedding,
            ) {
                source projected Document.embedding
                freshness available

                many results from Document
                    where org_id == $org_id
                    nearest(embedding, $query_vec, 10)
                return Found { results: results { title } }
                outcomes Found
            }"#,
        )
        .expect("query source");
        let module = QueryModule::compile(
            QueryModuleCandidate::new(
                QueryModuleName::new("documents").expect("module name"),
                QueryModuleVersion::new(1).expect("module version"),
                vec![query],
            )
            .expect("candidate"),
            &contract,
        )
        .expect("module");

        let generated = generate_rust_client(&module, &contract);
        let generated_typescript = generate_typescript_client(&module, &contract);
        let generated_go = crate::generate_go_client(&module, &contract);
        let generated_python = generate_python_client(&module, &contract).expect("Python");

        assert!(generated.contains("VectorStateInspectionResult"));
        assert!(generated.contains("pub async fn inspect_document_embedding_staleness("));
        assert!(generated.contains("pub async fn inspect_document_embedding_model_versions("));
        assert!(generated.contains("partition: String"));
        assert!(generated.contains("ApplicationUuid::from_text(partition)?"));
        assert!(generated.contains("VectorStateInspectionKind::Stale"));
        assert!(generated.contains("VectorStateInspectionKind::OutdatedModel"));
        assert!(generated.contains("ApplicationContract::Exact"));
        assert!(generated_typescript.contains("inspectDocumentEmbeddingStaleness("));
        assert!(generated_typescript.contains("inspectDocumentEmbeddingModelVersions("));
        assert!(generated_typescript.contains("executeVectorInspection"));
        assert!(generated_go.contains("InspectDocumentEmbeddingStaleness("));
        assert!(generated_go.contains("InspectDocumentEmbeddingModelVersions("));
        assert!(generated_go.contains("InspectDocumentEmbeddingStalenessOperation"));
        assert!(generated_python.contains("def inspect_document_embedding_staleness("));
        assert!(generated_python.contains("def inspect_document_embedding_model_versions("));
        assert!(generated_python.contains("inspection_kind=\"staleness\""));
        assert!(generated_python.contains("inspection_kind=\"model_versions\""));
    }

    #[test]
    fn generated_clients_preserve_exact_money_query_results() {
        let contract = compile_contract_source(
            "contract Commerce version 1 {\n\
             entity Product { key (product_id: uuid) field price: money<USD> }\n\
             aggregate Products { root Product partition_by product_id conflict_key (product_id) }\n\
             }",
        )
        .expect("contract");
        let query = NamedQuerySource::new(
            "ProductPage",
            "query ProductPage($product_id: Product.product_id) { one product from Product where product_id == $product_id else NotFound return Found { product: product { product_id price } } outcomes Found | NotFound }",
        )
        .expect("query source");
        let candidate = QueryModuleCandidate::new(
            QueryModuleName::new("CommerceQueries").expect("module name"),
            QueryModuleVersion::new(1).expect("module version"),
            vec![query],
        )
        .expect("candidate");
        let module = QueryModule::compile(candidate, &contract).expect("module");

        let generated = generate_typescript_client(&module, &contract);
        let generated_rust = generate_rust_client(&module, &contract);
        let generated_python = generate_python_client(&module, &contract).expect("Python");

        assert!(generated.contains(
            "readonly price: { readonly currency: string; readonly amount: { readonly coefficientTwosComplement: Uint8Array; readonly scale: number; readonly precision: number } }"
        ));
        assert!(!generated.contains("readonly price: string;"));
        assert!(
            generated.contains(r#"{"currency":"USD","kind":"money","precision":38,"scale":2}"#)
        );
        assert!(generated_rust.contains("pub price: MoneyValue,"));
        assert!(generated_rust.contains("fn wire_money("));
        assert!(generated_rust.contains("fn decode_wire_money("));
        assert!(generated_python.contains("    price: Money"));
        assert!(!generated_python.contains("    price: str"));

        let price = contract
            .schema()
            .entities()
            .iter()
            .find(|entity| entity.name() == "Product")
            .and_then(|entity| {
                entity
                    .record()
                    .fields()
                    .iter()
                    .find(|field| field.name() == "price")
            })
            .expect("price field");
        assert_eq!(
            rust_encode_wire_expr(price.value_type(), "&entity.price", &contract),
            "wire_money(&entity.price, \"USD\")?"
        );
        assert_eq!(
            rust_decode_wire_expr(
                price.value_type(),
                "take_wire_field(&mut fields, 2)?",
                &contract
            ),
            "decode_wire_money(take_wire_field(&mut fields, 2)?, \"USD\")?"
        );
        assert_eq!(
            ts_contract_value_schema(price.value_type(), &contract),
            json!({"kind": "money", "precision": 38, "scale": 2, "currency": "USD"})
        );
    }

    #[test]
    fn failed_packed_activation_removes_generated_selection_and_decoder_bulk() {
        let contract = compile_contract_source(include_str!(
            "../../../examples/app-baseline/contracts/ticketdesk.riff"
        ))
        .expect("TicketDesk contract");
        let query = NamedQuerySource::new(
            "BoardPage450",
            include_str!("../../../queries/ticketdesk/board_page_450.riffq"),
        )
        .expect("query source");
        let module = QueryModule::compile(
            QueryModuleCandidate::new(
                QueryModuleName::new("TicketDeskBoard").expect("module name"),
                QueryModuleVersion::new(1).expect("module version"),
                vec![query],
            )
            .expect("candidate"),
            &contract,
        )
        .expect("module");

        let rust = generate_rust_client(&module, &contract);
        let go = crate::generate_go_client(&module, &contract);
        let typescript = generate_typescript_client(&module, &contract);
        let python = generate_python_client(&module, &contract).expect("Python");

        assert!(!rust.contains("fn decode_packed_result("));
        assert!(!rust.contains(".accept_packed_result_v1()"));
        assert!(!go.contains("func decodeBoardPage450Packed("));
        assert!(!go.contains("AcceptPackedResult = true"));
        assert!(!typescript.contains("function decodeBoardPage450Packed("));
        assert!(!typescript.contains("packedDecoder: decodeBoardPage450Packed"));
        assert!(!python.contains("def _decode_board_page450_packed("));
        assert!(!python.contains("accept_packed_result="));
    }
}
