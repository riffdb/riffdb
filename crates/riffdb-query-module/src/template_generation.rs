//! Shared, deterministic application-facade template rendering.

use minijinja::{AutoEscape, Environment, UndefinedBehavior};
use riffdb_contract_ir::{ContractBundle, RecordTypeRef, ValueType, ValueTypeTag};
use riffdb_query_ir::{ReactiveModulePlanV1, ReactiveOperationPlanV1, ReactiveParameterV1};
use serde_json::{Value, json};

use crate::QueryModule;
use crate::generation::{
    camel, command_secret_outputs, embedding_command_facades, generate_mcp_commands,
    generate_mcp_reactive_tools, generate_vector_inspection_tools,
    generated_query_driver_operations, pascal, query_set_chunking_shape, rust_compact_result_shape,
    rust_contract_type, rust_encode_wire_expr, rust_generation_model, rust_identifier,
    screaming_snake, snake, ts_contract_record_schema, ts_contract_type, ts_identifier,
    typescript_generation_model, vector_inspection_facades, workflow_revision_bindings,
    workflow_success_outcome_name,
};
use crate::go_generation::{
    decode_expr, decode_named_expr, encode_expr, encode_named_expr, go_decode_compact_expr,
    go_named_type, go_public, go_reactive_decode_expr, go_reactive_encode_expr, go_reactive_type,
    go_snake, go_type, is_cursor, schema_hash,
};
use crate::python_generation::{
    python_contract_type, python_generation_model, python_identifier,
    python_reactive_generation_model, with_optional_wire_name,
};

const GO_CLIENT_HEADER: &str = include_str!("../../../templates/generators/go/client.go.j2");
const PYTHON_CLIENT_HEADER: &str =
    include_str!("../../../templates/generators/python/client.py.j2");
const RUST_CLIENT_HEADER: &str = include_str!("../../../templates/generators/rust/client.rs.j2");
const RUST_RUNTIME: &str = include_str!("../../../templates/generators/rust/runtime.rs.j2");
const RUST_REACTIVE: &str = include_str!("../../../templates/generators/rust/reactive.rs.j2");
const TYPESCRIPT_CLIENT_HEADER: &str =
    include_str!("../../../templates/generators/typescript/client.ts.j2");

const RUST_SAGAS: &str = r#"
{% for saga in languages.rust.sagas %}#[derive(Clone, Debug, Eq, PartialEq)]
pub struct {{ saga.input_name }} {
{% for input in saga.inputs %}    pub {{ input.name }}: {{ input.type }},
{% endfor %}}

const {{ saga.constant }}_SAGA_PLAN_HASH: [u8; 32] = [{{ saga.plan_hash_bytes }}];
{% endfor %}#[allow(dead_code)]
impl {{ languages.rust.client.name }} {
{% for saga in languages.rust.sagas %}    pub async fn start_{{ saga.method }}(&mut self, input: &{{ saga.input_name }}) -> Result<riffdb_client_rust::ClientStartSagaResult, ApplicationClientError> {
        let fields = vec![
{% for input in saga.inputs %}            wire_named_field("{{ input.wire_name }}", {{ input.encode }}),
{% endfor %}        ];
        self.client.start_saga("{{ saga.name }}", Some(CONTRACT_VERSION), wire_record(fields), &self.metadata, self.command_attempts).await.map_err(Into::into)
    }

    pub async fn get_{{ saga.method }}(&mut self, saga_id: &str) -> Result<Option<riffdb_client_rust::ClientSagaInstanceView>, ApplicationClientError> {
        self.client.get_saga(saga_id, &self.metadata).await.map_err(Into::into)
    }

    pub async fn await_{{ saga.method }}(&mut self, saga_id: &str, await_millis: u64) -> Result<riffdb_client_rust::ClientSagaInstanceView, ApplicationClientError> {
        self.client.await_saga(saga_id, await_millis, &self.metadata).await.map_err(Into::into)
    }

    pub async fn cancel_{{ saga.method }}(&mut self, saga_id: &str, cancel_idempotency_key: &str) -> Result<riffdb_client_rust::ClientSagaInstanceView, ApplicationClientError> {
        self.client.cancel_saga(saga_id, cancel_idempotency_key, &self.metadata, self.command_attempts).await.map_err(Into::into)
    }

    pub async fn resume_{{ saga.method }}(&mut self, saga_id: &str) -> Result<riffdb_client_rust::ClientSagaInstanceView, ApplicationClientError> {
        self.client.resume_saga(saga_id, &self.metadata).await.map_err(Into::into)
    }
{% endfor %}}
"#;

const TYPESCRIPT_SAGAS: &str = r#"
export interface SagaInstanceView {
  readonly outcome: "Started" | "Completed" | "FailedAndCompensated" | "CompensationEscalated" | "InProgress" | "NotFound";
  readonly saga_id: string;
  readonly saga_name: string;
  readonly status: string;
  readonly disposition?: string;
  readonly current_step_ordinal: bigint;
  readonly current_step_name: string;
  readonly final_outcome_tag: string;
  readonly escalation_reason: string;
  readonly step_ledger_json: string;
  readonly started_at_commit_seq: bigint;
  readonly updated_at_commit_seq: bigint;
  readonly replayed?: boolean;
}
{% for saga in languages.typescript.sagas %}
export interface {{ saga.name }}Input {
{% for input in saga.inputs %}  readonly {{ input.name }}{% if input.optional %}?{% endif %}: {{ input.type }};
{% endfor %}}
export const {{ saga.constant }}_SAGA_PLAN_HASH = "{{ saga.plan_hash }}" as const;
export function start{{ saga.pascal }}(input: {{ saga.name }}Input): CommandRequest<{{ saga.name }}Input, SagaInstanceView> {
  const idempotencyKey = input.{{ saga.idempotency }} && input.{{ saga.idempotency }}.length > 0 ? input.{{ saga.idempotency }} : globalThis.crypto.randomUUID();
  const resolvedInput: {{ saga.name }}Input = input.{{ saga.idempotency }} === idempotencyKey ? input : { ...input, {{ saga.idempotency }}: idempotencyKey };
  return { driverOperation: { name: "{{ saga.operation }}", inputSchemaHash: "{{ saga.schema_hash }}" }, contractLineage: CONTRACT_LINEAGE, contractVersion: CONTRACT_VERSION, commandName: "__saga_start__{{ saga.name }}", planHash: {{ saga.constant }}_SAGA_PLAN_HASH, input: resolvedInput, idempotencyKey, inputSchema: {{ saga.input_schema }}, outcomeSchemas: {}, decodeError: decodeApplicationError };
}
export function get{{ saga.pascal }}(sagaId: string): CommandRequest<{ readonly saga_id: string }, SagaInstanceView> {
  return { driverOperation: { name: "{{ saga.operation }}_get", inputSchemaHash: "{{ saga.schema_hash }}" }, contractLineage: CONTRACT_LINEAGE, contractVersion: CONTRACT_VERSION, commandName: "__saga_get__", planHash: {{ saga.constant }}_SAGA_PLAN_HASH, input: { saga_id: sagaId }, idempotencyKey: sagaId, inputSchema: { kind: "record", fields: [{ name: "saga_id", schema: { kind: "string" } }] }, outcomeSchemas: {}, decodeError: decodeApplicationError };
}
export function await{{ saga.pascal }}(sagaId: string, awaitMillis = 5000n): CommandRequest<{ readonly saga_id: string; readonly await_millis: bigint }, SagaInstanceView> {
  return { driverOperation: { name: "{{ saga.operation }}_await", inputSchemaHash: "{{ saga.schema_hash }}" }, contractLineage: CONTRACT_LINEAGE, contractVersion: CONTRACT_VERSION, commandName: "__saga_await__", planHash: {{ saga.constant }}_SAGA_PLAN_HASH, input: { saga_id: sagaId, await_millis: awaitMillis }, idempotencyKey: sagaId, inputSchema: { kind: "record", fields: [{ name: "saga_id", schema: { kind: "string" } }, { name: "await_millis", schema: { kind: "u64" } }] }, outcomeSchemas: {}, decodeError: decodeApplicationError };
}
export function cancel{{ saga.pascal }}(sagaId: string, cancelIdempotencyKey?: string): CommandRequest<{ readonly saga_id: string; readonly idempotency_key: string }, SagaInstanceView> {
  const key = cancelIdempotencyKey && cancelIdempotencyKey.length > 0 ? cancelIdempotencyKey : globalThis.crypto.randomUUID();
  return { driverOperation: { name: "{{ saga.operation }}_cancel", inputSchemaHash: "{{ saga.schema_hash }}" }, contractLineage: CONTRACT_LINEAGE, contractVersion: CONTRACT_VERSION, commandName: "__saga_cancel__", planHash: {{ saga.constant }}_SAGA_PLAN_HASH, input: { saga_id: sagaId, idempotency_key: key }, idempotencyKey: key, inputSchema: { kind: "record", fields: [{ name: "saga_id", schema: { kind: "string" } }, { name: "idempotency_key", schema: { kind: "string" } }] }, outcomeSchemas: {}, decodeError: decodeApplicationError };
}
export function resume{{ saga.pascal }}(sagaId: string): CommandRequest<{ readonly saga_id: string }, SagaInstanceView> {
  return { driverOperation: { name: "{{ saga.operation }}_resume", inputSchemaHash: "{{ saga.schema_hash }}" }, contractLineage: CONTRACT_LINEAGE, contractVersion: CONTRACT_VERSION, commandName: "__saga_resume__", planHash: {{ saga.constant }}_SAGA_PLAN_HASH, input: { saga_id: sagaId }, idempotencyKey: sagaId, inputSchema: { kind: "record", fields: [{ name: "saga_id", schema: { kind: "string" } }] }, outcomeSchemas: {}, decodeError: decodeApplicationError };
}
{% endfor %}
"#;

const PYTHON_SAGAS: &str = r#"
{% for saga in languages.python.sagas %}
@dataclass(frozen=True, slots=True, kw_only=True)
class {{ saga.name }}Input:
{% for input in saga.inputs %}    {{ input.name }}: {{ input.type }}{% if input.is_idempotency and input.wire_name is defined %} = field(default_factory=lambda: str(__import__("uuid").uuid4()), metadata={"wire": {{ input.wire_name }}}){% elif input.is_idempotency %} = field(default_factory=lambda: str(__import__("uuid").uuid4())){% elif input.wire_name is defined %} = field(metadata={"wire": {{ input.wire_name }}}){% endif %}
{% endfor %}
{{ saga.constant }}_SAGA_PLAN_HASH: Final[str] = "{{ saga.plan_hash }}"

def start_{{ saga.method }}(client: {{ languages.python.class_name }}Client, input: {{ saga.name }}Input) -> object:
    return client._transport._execute_command(
        contract_lineage=CONTRACT_LINEAGE, contract_version=CONTRACT_VERSION,
        command_name="__saga_start__{{ saga.wire_raw }}", plan_hash={{ saga.constant }}_SAGA_PLAN_HASH,
        input=encode_record(input), attempts=client._command_attempts,
    )

def get_{{ saga.method }}(client: {{ languages.python.class_name }}Client, saga_id: str) -> object:
    return client._transport._execute_command(
        contract_lineage=CONTRACT_LINEAGE, contract_version=CONTRACT_VERSION,
        command_name="__saga_get__", plan_hash={{ saga.constant }}_SAGA_PLAN_HASH,
        input={"saga_id": saga_id}, attempts=client._command_attempts,
    )

def await_{{ saga.method }}(client: {{ languages.python.class_name }}Client, saga_id: str, await_millis: int = 5000) -> object:
    return client._transport._execute_command(
        contract_lineage=CONTRACT_LINEAGE, contract_version=CONTRACT_VERSION,
        command_name="__saga_await__", plan_hash={{ saga.constant }}_SAGA_PLAN_HASH,
        input={"saga_id": saga_id, "await_millis": await_millis}, attempts=client._command_attempts,
    )

def cancel_{{ saga.method }}(client: {{ languages.python.class_name }}Client, saga_id: str, cancel_idempotency_key: str) -> object:
    return client._transport._execute_command(
        contract_lineage=CONTRACT_LINEAGE, contract_version=CONTRACT_VERSION,
        command_name="__saga_cancel__", plan_hash={{ saga.constant }}_SAGA_PLAN_HASH,
        input={"saga_id": saga_id, "idempotency_key": cancel_idempotency_key}, attempts=client._command_attempts,
    )

def resume_{{ saga.method }}(client: {{ languages.python.class_name }}Client, saga_id: str) -> object:
    return client._transport._execute_command(
        contract_lineage=CONTRACT_LINEAGE, contract_version=CONTRACT_VERSION,
        command_name="__saga_resume__", plan_hash={{ saga.constant }}_SAGA_PLAN_HASH,
        input={"saga_id": saga_id}, attempts=client._command_attempts,
    )

setattr({{ languages.python.class_name }}Client, "start_{{ saga.method }}", start_{{ saga.method }})
setattr({{ languages.python.class_name }}Client, "get_{{ saga.method }}", get_{{ saga.method }})
setattr({{ languages.python.class_name }}Client, "await_{{ saga.method }}", await_{{ saga.method }})
setattr({{ languages.python.class_name }}Client, "cancel_{{ saga.method }}", cancel_{{ saga.method }})
setattr({{ languages.python.class_name }}Client, "resume_{{ saga.method }}", resume_{{ saga.method }})
{% endfor %}
"#;

const GO_SAGAS: &str = r#"
{% for saga in languages.go.sagas %}type {{ saga.name }}Input struct {
{% for field in saga.inputs %}	{{ field.name }} {{ field.type }}
{% endfor %}}
const {{ saga.name }}SagaPlanHash = "{{ saga.plan_hash }}"
var {{ saga.name }}SagaOperation = riffdb.Operation{Name: {{ saga.operation }}, InputSchemaHash: {{ saga.schema_hash }}}
func encode{{ saga.name }}SagaInput(input {{ saga.name }}Input) map[string]riffdb.Value { {% if saga.idempotency_field %}input.{{ saga.idempotency_field }} = riffdb.DefaultIdempotencyKey(input.{{ saga.idempotency_field }}); {% endif %}return map[string]riffdb.Value{
{% for field in saga.inputs %}	{{ field.wire_name }}: {{ field.encode }},
{% endfor %}} }
func (client *Client) Start{{ saga.name }}(ctx context.Context, input {{ saga.name }}Input) (riffdb.Response, error) { return client.session.Invoke(ctx, {{ saga.name }}SagaOperation, encode{{ saga.name }}SagaInput(input), riffdb.Options{MaximumAttempts: client.commandAttempts}) }
func (client *Client) Get{{ saga.name }}(ctx context.Context, sagaID string) (riffdb.Response, error) { return client.session.Invoke(ctx, {{ saga.name }}SagaOperation, map[string]riffdb.Value{"__saga_op": riffdb.String("get"), "saga_id": riffdb.String(sagaID)}, riffdb.Options{}) }
func (client *Client) Await{{ saga.name }}(ctx context.Context, sagaID string, awaitMillis uint64) (riffdb.Response, error) { return client.session.Invoke(ctx, {{ saga.name }}SagaOperation, map[string]riffdb.Value{"__saga_op": riffdb.String("await"), "saga_id": riffdb.String(sagaID), "await_millis": riffdb.U64(awaitMillis)}, riffdb.Options{}) }
func (client *Client) Cancel{{ saga.name }}(ctx context.Context, sagaID string, cancelIdempotencyKey string) (riffdb.Response, error) { return client.session.Invoke(ctx, {{ saga.name }}SagaOperation, map[string]riffdb.Value{"__saga_op": riffdb.String("cancel"), "saga_id": riffdb.String(sagaID), "idempotency_key": riffdb.DefaultIdempotencyKey(cancelIdempotencyKey)}, riffdb.Options{MaximumAttempts: client.commandAttempts}) }
func (client *Client) Resume{{ saga.name }}(ctx context.Context, sagaID string) (riffdb.Response, error) { return client.session.Invoke(ctx, {{ saga.name }}SagaOperation, map[string]riffdb.Value{"__saga_op": riffdb.String("resume"), "saga_id": riffdb.String(sagaID)}, riffdb.Options{}) }
{% endfor %}
"#;

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
    let mut go_claimed_methods = std::collections::BTreeSet::from([
        "Close".to_owned(),
        "WithReadAfterCommit".to_owned(),
        "CausalSession".to_owned(),
        "LastCommitSequence".to_owned(),
        "observeCommit".to_owned(),
    ]);
    for query in module.queries() {
        go_claimed_methods.insert(go_public(query.name()));
    }
    for command in &commands {
        go_claimed_methods.insert(go_public(command.name()));
    }
    for inspection in vector_inspection_facades(module, contract) {
        let base = format!(
            "Inspect{}{}",
            go_public(&inspection.entity),
            go_public(&inspection.field)
        );
        go_claimed_methods.insert(format!("{base}Staleness"));
        go_claimed_methods.insert(format!("{base}ModelVersions"));
    }
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
            let set_chunking = query_set_chunking_shape(query.plan(), contract).map(|shape| json!({
                "constant": format!("{}{}MaxItems", name, go_public(&shape.param_name)),
                "maximum": shape.maximum,
                "param_field": go_public(&shape.param_name),
                "param_wire": shape.param_name,
                "elem_type": go_named_type(&riffdb_query_ir::NamedTypeSchema::Scalar(shape.element_type), contract),
                "found_wire": format!("{:?}", shape.found_outcome),
                "result_field": go_public(&shape.result_field),
            }));
            let has_after_commit = go_claimed_methods.insert(format!("{name}AfterCommit"));
            let has_pages = cursor.is_some() && go_claimed_methods.insert(format!("{name}Pages"));
            let has_all =
                set_chunking.is_some() && go_claimed_methods.insert(format!("{name}All"));
            json!({
                "name": name,
                "has_after_commit": has_after_commit,
                "has_pages": has_pages,
                "has_all": has_all,
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
                "has_found": schemas.results().iter().any(|branch| branch.name().eq_ignore_ascii_case("found")),
                "results": schemas.results().iter().map(|branch| json!({
                    "name": format!("{}{}", name, go_public(branch.name())),
                    "is_found": branch.name().eq_ignore_ascii_case("found"),
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
                "set_chunking": set_chunking,
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
            let has_batch = go_claimed_methods.insert(format!("{name}Batch"));
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
                let mut collection = json!({
                    "field": go_public(field.name()),
                    "minimum": expansion.minimum_elements(),
                    "maximum": expansion.maximum_elements(),
                    "message": format!("{:?}", format!("invalid bounded collection length for {}.{}", command.name(), field.name())),
                    "aggregate": expansion.maximum_aggregate_element_bytes().map(|maximum| json!({
                        "maximum": maximum,
                        "encode": encode_expr("item", expansion.element_type(), contract),
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
                        json!(go_collection_individual_checks(
                            expansion.element_type(),
                            field.name(),
                            contract,
                        )),
                    );
                }
                collection
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
                "has_batch": has_batch,
                "plan_hash": hex(command.plan_hash().as_bytes()),
                "operation": format!("{:?}", operation),
                "schema_hash": format!("{:?}", operation_schema_hash),
                "idempotency_field": command
                    .idempotency_input()
                    .and_then(|id| command.input().record().field(id))
                    .filter(|field| field.value_type().tag() == ValueTypeTag::String)
                    .map(|field| go_public(field.name())),
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
    let mut sagas = contract.schema().saga_plans().iter().collect::<Vec<_>>();
    sagas.sort_by(|left, right| left.name().cmp(right.name()));

    let mut result = json!({
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
    });
    if !sagas.is_empty() {
        let go_sagas = sagas
            .iter()
            .map(|saga| {
                let name = go_public(saga.name());
                let (operation, operation_schema_hash) = command_driver_operations
                    .get(saga.name())
                    .expect("generated saga operation");
                json!({
                    "name": name,
                    "plan_hash": hex(saga.plan_hash().as_bytes()),
                    "operation": format!("{:?}", operation),
                    "schema_hash": format!("{:?}", operation_schema_hash),
                    "idempotency_field": saga_idempotency_field(saga)
                        .filter(|field| field.value_type().tag() == ValueTypeTag::String)
                        .map(|field| go_public(field.name())),
                    "inputs": saga.inputs().iter().map(|field| json!({
                        "name": go_public(field.name()),
                        "type": go_type(field.value_type(), contract),
                        "wire_name": format!("{:?}", field.name()),
                        "encode": encode_expr(
                            &format!("input.{}", go_public(field.name())),
                            field.value_type(),
                            contract,
                        ),
                    })).collect::<Vec<_>>(),
                })
            })
            .collect::<Vec<_>>();
        let rust_sagas = sagas
            .iter()
            .map(|saga| {
                let name = saga.name();
                json!({
                    "name": name,
                    "method": snake(name),
                    "input_name": format!("{name}Input"),
                    "constant": screaming_snake(name),
                    "plan_hash_bytes": saga.plan_hash().as_bytes().iter().map(|byte| format!("0x{byte:02x}")).collect::<Vec<_>>().join(", "),
                    "inputs": saga.inputs().iter().map(|field| json!({
                        "name": rust_identifier(field.name()),
                        "wire_name": field.name(),
                        "type": rust_contract_type(field.value_type(), contract),
                        "encode": rust_encode_wire_expr(
                            field.value_type(),
                            &format!("&input.{}", rust_identifier(field.name())),
                            contract,
                        ),
                    })).collect::<Vec<_>>(),
                })
            })
            .collect::<Vec<_>>();
        let ts_sagas = sagas
            .iter()
            .map(|saga| {
                let name = saga.name();
                let (driver_operation_name, driver_input_schema_hash) = command_driver_operations
                    .get(name)
                    .expect("generated saga operation");
                let input_schema = ts_contract_record_schema(
                    saga.inputs().iter().map(|field| {
                        (field.name(), field.id().get(), field.value_type())
                    }),
                    contract,
                    false,
                );
                let idempotency_field_name =
                    saga_idempotency_field(saga).map_or("idempotency_key", |field| field.name());
                json!({
                    "name": name,
                    "function": camel(name),
                    "pascal": pascal(name),
                    "constant": screaming_snake(name),
                    "plan_hash": hex(saga.plan_hash().as_bytes()),
                    "operation": driver_operation_name,
                    "schema_hash": driver_input_schema_hash,
                    "idempotency": idempotency_field_name,
                    "inputs": saga.inputs().iter().map(|field| json!({
                        "name": ts_identifier(field.name()),
                        "type": ts_contract_type(field.value_type(), contract),
                        "optional": field.name() == idempotency_field_name && field.value_type().tag() == ValueTypeTag::String,
                    })).collect::<Vec<_>>(),
                    "input_schema": serde_json::to_string(&input_schema).expect("generated saga input schema"),
                })
            })
            .collect::<Vec<_>>();
        let py_sagas = sagas
            .iter()
            .map(|saga| {
                let wire_name = saga.name();
                let name = pascal(wire_name);
                let idempotency_field = saga_idempotency_field(saga).map(|field| field.name());
                json!({
                    "name": name,
                    "wire_raw": wire_name,
                    "method": python_identifier(&snake(wire_name)),
                    "constant": screaming_snake(wire_name),
                    "plan_hash": hex(saga.plan_hash().as_bytes()),
                    "inputs": saga.inputs().iter().map(|field| with_optional_wire_name(
                        json!({
                            "name": python_identifier(field.name()),
                            "type": python_contract_type(field.value_type(), contract),
                            "is_idempotency": idempotency_field == Some(field.name()) && field.value_type().tag() == ValueTypeTag::String,
                        }),
                        field.name(),
                    )).collect::<Vec<_>>(),
                })
            })
            .collect::<Vec<_>>();
        result["sagas"] = json!(sagas.iter().map(|saga| saga.name()).collect::<Vec<_>>());
        result["languages"]["go"]["sagas"] = json!(go_sagas);
        result["languages"]["rust"]["sagas"] = json!(rust_sagas);
        result["languages"]["typescript"]["sagas"] = json!(ts_sagas);
        result["languages"]["python"]["sagas"] = json!(py_sagas);
    }
    result
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
    let mut rendered = render(GO_CLIENT_HEADER, model)?;
    rendered.push('\n');
    if model
        .get("sagas")
        .and_then(Value::as_array)
        .is_some_and(|sagas| !sagas.is_empty())
    {
        rendered.push_str(&render(GO_SAGAS, model)?);
    }
    Ok(rendered)
}

fn go_collection_individual_checks(
    element_type: &ValueType,
    collection: &str,
    contract: &ContractBundle,
) -> Vec<String> {
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
            let member = go_public(field.name());
            let condition = if optional {
                format!("item.{member} != nil && len(*item.{member}) > {maximum}")
            } else {
                format!("len(item.{member}) > {maximum}")
            };
            Some(format!(
                "if {condition} {{ return &InputBudgetError{{Cause: IndividualValueBytes, Path: InputBudgetPath{{Collection: {collection:?}, Index: &index, Leaf: {:?}}}}} }}",
                field.name(),
            ))
        })
        .collect()
}

pub(crate) fn render_python_client_header(model: &Value) -> Result<String, minijinja::Error> {
    let mut rendered = render(PYTHON_CLIENT_HEADER, model)?;
    if model
        .get("sagas")
        .and_then(Value::as_array)
        .is_some_and(|sagas| !sagas.is_empty())
    {
        rendered.push_str(&render(PYTHON_SAGAS, model)?);
    }
    Ok(rendered)
}

pub(crate) fn render_rust_client_header(model: &Value) -> Result<String, minijinja::Error> {
    let mut rendered = render(RUST_CLIENT_HEADER, model)?;
    rendered.push_str("\n\n");
    rendered.push_str(&render(RUST_RUNTIME, model)?);
    if model["features"]["reactive"] == Value::Bool(true) {
        rendered.push('\n');
        rendered.push_str(&render(RUST_REACTIVE, model)?);
    }
    if model
        .get("sagas")
        .and_then(Value::as_array)
        .is_some_and(|sagas| !sagas.is_empty())
    {
        rendered.push_str(&render(RUST_SAGAS, model)?);
    }
    Ok(rendered)
}

pub(crate) fn render_typescript_client_header(model: &Value) -> Result<String, minijinja::Error> {
    let mut rendered = render(TYPESCRIPT_CLIENT_HEADER, model)?;
    if model
        .get("sagas")
        .and_then(Value::as_array)
        .is_some_and(|sagas| !sagas.is_empty())
    {
        rendered.push_str(&render(TYPESCRIPT_SAGAS, model)?);
    }
    Ok(rendered)
}

fn render(template: &str, model: &Value) -> Result<String, minijinja::Error> {
    let mut environment = Environment::new();
    environment.set_undefined_behavior(UndefinedBehavior::Strict);
    environment.set_auto_escape_callback(|_| AutoEscape::None);
    environment.render_str(template, model)
}

fn saga_idempotency_field(
    saga: &riffdb_contract_ir::SagaPlanV1,
) -> Option<&riffdb_contract_ir::FieldSchema> {
    if let Some(node) = saga.expressions().get(saga.idempotency_expression())
        && let riffdb_contract_ir::ExpressionKind::InputField(id) = node.kind()
    {
        return saga.input(*id);
    }
    saga.inputs()
        .iter()
        .find(|field| field.name() == "idempotency_key")
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

    // req: WF-002, WF-005, WF-011, WF-012, WF-013, ID-001, ID-002, TXN-001
    #[test]
    fn generated_rust_ts_python_go_and_mcp_saga_bindings_preserve_authority_and_idempotency() {
        let contract = riffdb_contract_compiler::compile_contract_source(
            r#"
            contract CheckoutSagaApp version 1 {
              entity Inventory {
                key (sku: u64)
                field available: u64
              }
              aggregate Inventories {
                root Inventory
                partition_by sku
                conflict_key (sku)
              }
              command ReserveInventory {
                input idempotency_key: string<128>
                input sku: u64
                input quantity: u64
                idempotency_key idempotency_key
                mutate Inventory(sku) as inv else MissingInventory { sku: sku }
                require has_stock: inv.available >= quantity else OutOfStock { sku: inv.sku }
                set inv.available = inv.available - quantity
                return Reserved { sku: inv.sku }
              }
              command ReleaseInventory {
                input idempotency_key: string<128>
                input sku: u64
                input quantity: u64
                idempotency_key idempotency_key
                mutate Inventory(sku) as inv else MissingInventory { sku: sku }
                set inv.available = inv.available + quantity
                return Released { sku: inv.sku }
              }
              role CheckoutOrchestrator {
                commands (ReserveInventory, ReleaseInventory)
              }
              saga CheckoutOrder {
                partition_by sku
                input idempotency_key: string<128>
                input saga_id: uuid
                input sku: u64
                input quantity: u64
                idempotency_key idempotency_key
                execution_role CheckoutOrchestrator
                step reserve {
                  command ReserveInventory(idempotency_key, sku, quantity)
                  compensate ReleaseInventory(idempotency_key, sku, quantity)
                  on Reserved (effectful) => complete OrderCompleted { sku: sku }
                  on MissingInventory (no_effect) => abort OrderMissing { sku: sku }
                  on OutOfStock (no_effect) => abort OrderOutOfStock { sku: sku }
                }
              }
            }
            "#,
        )
        .expect("compiled saga contract");
        let query = crate::NamedQuerySource::new(
            "GetInventory",
            "query GetInventory($sku: Inventory.sku) { one inv from Inventory where sku == $sku else NotFound return Found { inv: inv { sku available } } outcomes Found | NotFound }",
        )
        .expect("query source");
        let module = crate::QueryModule::compile(
            crate::QueryModuleCandidate::new(
                crate::QueryModuleName::new("CheckoutQueries").expect("module name"),
                crate::QueryModuleVersion::new(1).expect("module version"),
                vec![query],
            )
            .expect("candidate"),
            &contract,
        )
        .expect("module");

        let mcp_commands =
            crate::generate_mcp_commands(&module, &contract).expect("MCP commands + sagas");
        assert!(
            mcp_commands
                .iter()
                .any(|cmd| cmd.operation_name == "CheckoutOrder"),
            "MCP command descriptors must include compiled sagas"
        );

        let rust = crate::generate_rust_client(&module, &contract);
        let ts = crate::generate_typescript_client(&module, &contract);
        let py = crate::generate_python_client(&module, &contract).expect("Python client");
        let go = crate::generate_go_client(&module, &contract);

        assert!(rust.contains("pub struct CheckoutOrderInput"));
        assert!(rust.contains("pub async fn start_checkout_order("));
        assert!(rust.contains("pub async fn get_checkout_order("));
        assert!(rust.contains("pub async fn await_checkout_order("));
        assert!(rust.contains("pub async fn cancel_checkout_order("));
        assert!(rust.contains("pub async fn resume_checkout_order("));

        assert!(ts.contains("export interface CheckoutOrderInput"));
        assert!(ts.contains("export function startCheckoutOrder("));
        assert!(ts.contains("export function getCheckoutOrder("));
        assert!(ts.contains("export function awaitCheckoutOrder("));
        assert!(ts.contains("export function cancelCheckoutOrder("));
        assert!(ts.contains("export function resumeCheckoutOrder("));

        assert!(py.contains("class CheckoutOrderInput:"));
        assert!(py.contains("def start_checkout_order("));
        assert!(py.contains("def get_checkout_order("));
        assert!(py.contains("def await_checkout_order("));
        assert!(py.contains("def cancel_checkout_order("));
        assert!(py.contains("def resume_checkout_order("));

        assert!(go.contains("type CheckoutOrderInput struct"));
        assert!(go.contains("func (client *Client) StartCheckoutOrder("));
        assert!(go.contains("func (client *Client) GetCheckoutOrder("));
        assert!(go.contains("func (client *Client) AwaitCheckoutOrder("));
        assert!(go.contains("func (client *Client) CancelCheckoutOrder("));
        assert!(go.contains("func (client *Client) ResumeCheckoutOrder("));
    }
}
