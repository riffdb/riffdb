//! Reproducible, name-addressed client source generation.

use std::collections::BTreeSet;
use std::fmt::Write as _;

use riffdb_contract_ir::{CommandPlan, ContractBundle, RecordTypeRef, ValueType, ValueTypeTag};
use riffdb_query_ir::{NamedQuerySchemas, NamedTypeSchema, PageBound, max_query_page_take};
use serde_json::{Map, Value, json};

use crate::QueryModule;

const MCP_SCHEMA_DIALECT: &str = "https://json-schema.org/draft/2020-12/schema";

/// One compiler-owned generated MCP tool for a visible named query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GeneratedMcpTool {
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
    /// Exact contract bundle identity.
    pub contract_bundle_hash: [u8; 32],
    /// Exact command plan identity.
    pub plan_hash: [u8; 32],
}

/// Closed generated-tool failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum McpToolGenerationError {
    /// Two source names normalize to the same tool name.
    NameCollision,
    /// A generated schema could not be serialized.
    InvalidSchema,
}

/// Generates deterministic read-only MCP tool artifacts for every named query.
pub fn generate_mcp_tools(
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
            let schemas = query.program().surface().schemas();
            let mut properties = Map::new();
            let mut required = Vec::new();
            for parameter in schemas.parameters() {
                properties.insert(
                    parameter.name().to_owned(),
                    mcp_type_schema(parameter.value_type()),
                );
                if !parameter.has_default() && !is_cursor_type(parameter.value_type()) {
                    required.push(Value::String(parameter.name().to_owned()));
                }
            }
            let input = json!({
                "$schema": MCP_SCHEMA_DIALECT,
                "type": "object",
                "additionalProperties": false,
                "properties": properties,
                "required": required,
            });
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
            let result = json!({
                "$schema": MCP_SCHEMA_DIALECT,
                "oneOf": branches,
            });
            Ok(GeneratedMcpTool {
                title: format!("Run {}", query.name()),
                description: format!(
                    "Execute the exact {} named query from immutable module {}.",
                    query.name(),
                    module.name().as_str()
                ),
                name,
                input_schema: serde_json::to_string(&input)
                    .map_err(|_| McpToolGenerationError::InvalidSchema)?,
                result_schema: serde_json::to_string(&result)
                    .map_err(|_| McpToolGenerationError::InvalidSchema)?,
                module_hash: *module.identity().as_bytes(),
            })
        })
        .collect()
}

/// Generates deterministic mutating MCP operation artifacts for every command.
pub fn generate_mcp_commands(
    module: &QueryModule,
    contract: &ContractBundle,
) -> Result<Vec<GeneratedMcpCommand>, McpToolGenerationError> {
    let mut commands = contract.commands().iter().collect::<Vec<_>>();
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
                    (
                        field.name().to_owned(),
                        mcp_contract_type_schema(field.value_type(), contract),
                    )
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
            Ok(GeneratedMcpCommand {
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
                result_schema: serde_json::to_string(&json!({
                    "$schema": MCP_SCHEMA_DIALECT,
                    "oneOf": outcomes,
                }))
                .map_err(|_| McpToolGenerationError::InvalidSchema)?,
                contract_bundle_hash: *contract.bundle_hash().as_bytes(),
                plan_hash: *command.plan_hash().as_bytes(),
            })
        })
        .collect()
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
        _ => json!({"type": "string"}),
    }
}

fn mcp_type_schema(value_type: &NamedTypeSchema) -> Value {
    match value_type {
        NamedTypeSchema::Scalar(name) => match name.as_str() {
            "Bool" => json!({"type": "boolean"}),
            "I64" | "U64" => json!({"type": "integer"}),
            "Bytes" => json!({"contentEncoding": "base64", "type": "string"}),
            _ => json!({"type": "string"}),
        },
        NamedTypeSchema::Optional(inner) => {
            json!({"anyOf": [mcp_type_schema(inner), {"type": "null"}]})
        }
        NamedTypeSchema::Set(inner) => {
            json!({"type": "array", "items": mcp_type_schema(inner), "uniqueItems": true})
        }
        NamedTypeSchema::List { element, maximum } => {
            let maximum = match maximum {
                PageBound::Literal(maximum) => *maximum,
                // Parameterized page bound is capped by the continuation-aware max take.
                PageBound::Parameter(_) => max_query_page_take(),
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
    }
}

fn is_cursor_type(value_type: &NamedTypeSchema) -> bool {
    matches!(value_type, NamedTypeSchema::Cursor)
        || matches!(
            value_type,
            NamedTypeSchema::Optional(inner)
                if matches!(inner.as_ref(), NamedTypeSchema::Cursor)
        )
}

/// Generates a dependency-free Rust request model for every named query and command.
#[must_use]
pub fn generate_rust_client(module: &QueryModule, contract: &ContractBundle) -> String {
    let mut output = String::new();
    writeln!(
        output,
        "// @generated by riffdb-query-module; do not edit.\n\
         use std::collections::BTreeMap;\n\
         use riffdb_client_rust::generated::{{GeneratedCommand, GeneratedCommandError, GeneratedQuery}};\n\
         use riffdb_client_rust::{{ApplicationCardinality, ApplicationClientError, ApplicationContract, ApplicationUuid, \
         ApplicationRecord, ApplicationValue, AttemptBudget, CallMetadata, GeneratedBatchError, GeneratedBatchOptions, \
         GeneratedBatchProgress, GeneratedBatchResult, IdempotentCommand, NamedQuery, NamedQueryResult, QueryOptions, \
         StableApplicationClient, TypedCommandResult, TypedQueryResult, v1}};\n\
         use riffdb_client_rust::v1::value::Kind as WireKind;\n"
    )
    .expect("string");
    emit_rust_identity(&mut output, module);
    emit_rust_common_value_types(&mut output);

    for query in module.queries() {
        let name = query.name();
        let schemas = query.program().surface().schemas();
        let params_name = format!("{name}Params");
        emit_rust_fields_struct(
            &mut output,
            &params_name,
            schemas
                .parameters()
                .iter()
                .map(|parameter| (parameter.name(), parameter.value_type())),
        );
        for branch in schemas.results() {
            let branch_name = format!("{name}{}", pascal(branch.name()));
            emit_rust_fields_struct(
                &mut output,
                &branch_name,
                branch
                    .fields()
                    .iter()
                    .map(|field| (field.name(), field.value_type())),
            );
        }
        writeln!(
            output,
            "#[derive(Clone, Debug, Eq, PartialEq)]\npub enum {name}Result {{"
        )
        .expect("string");
        for branch in schemas.results() {
            let variant = pascal(branch.name());
            writeln!(output, "    {variant}(Box<{name}{variant}>),").expect("string");
        }
        writeln!(output, "}}\n").expect("string");
        emit_rust_generated_query_impl(
            &mut output,
            name,
            schemas,
            query.program().identity().hash().as_bytes(),
        );
    }

    let mut commands = contract.commands().iter().collect::<Vec<_>>();
    commands.sort_by(|left, right| left.name().cmp(right.name()));
    emit_rust_entity_types(&mut output, contract);
    for command in &commands {
        let name = command.name();
        let input_name = format!("{name}Input");
        writeln!(
            output,
            "#[derive(Clone, Debug, Eq, PartialEq)]\npub struct {input_name} {{"
        )
        .expect("string");
        for field in command.input().record().fields() {
            writeln!(
                output,
                "    pub {}: {},",
                rust_identifier(field.name()),
                rust_contract_type(field.value_type(), contract)
            )
            .expect("string");
        }
        writeln!(output, "}}\n").expect("string");
        emit_rust_command_outcome(&mut output, command, contract);
        emit_rust_generated_command_impl(&mut output, command, contract);
    }
    emit_rust_client_facade(&mut output, module, &commands);
    emit_rust_runtime_helpers(&mut output);
    output
}

fn emit_rust_common_value_types(output: &mut String) {
    writeln!(
        output,
        "#[derive(Clone, Debug, Eq, PartialEq)]\n\
         pub struct DecimalValue {{\n    pub coefficient_twos_complement: Vec<u8>,\n    pub scale: u32,\n    pub precision: Option<u32>,\n}}\n\
         #[derive(Clone, Debug, Eq, PartialEq)]\n\
         pub struct MoneyValue {{\n    pub currency: String,\n    pub amount: DecimalValue,\n}}\n\
         #[derive(Clone, Copy, Debug, Eq, PartialEq)]\n\
         pub struct TimestampValue {{\n    pub seconds: i64,\n    pub nanos: u32,\n}}\n"
    )
    .expect("string");
}

fn emit_rust_generated_query_impl(
    output: &mut String,
    name: &str,
    schemas: &NamedQuerySchemas,
    plan_hash: &[u8; 32],
) {
    let params_name = format!("{name}Params");
    let query_type = format!("{name}Query");
    let plan_hash_constant = format!("{}_QUERY_PLAN_HASH", screaming_snake(name));
    write!(output, "pub const {plan_hash_constant}: [u8; 32] = [").expect("string");
    for (index, byte) in plan_hash.iter().enumerate() {
        if index != 0 {
            write!(output, ", ").expect("string");
        }
        write!(output, "0x{byte:02x}").expect("string");
    }
    writeln!(output, "];").expect("string");
    writeln!(
        output,
        "#[derive(Clone, Debug, Eq, PartialEq)]\n\
         pub struct {query_type}(pub {params_name});\n\
         impl GeneratedQuery for {query_type} {{\n    type Output = {name}Result;\n\
         \n    fn named_query(self, options: QueryOptions) -> Result<NamedQuery, ApplicationClientError> {{\n\
         \x20       let mut parameters = BTreeMap::new();"
    )
    .expect("string");
    for parameter in schemas.parameters() {
        let expression = rust_encode_application_expr(
            parameter.value_type(),
            &format!("self.0.{}", rust_identifier(parameter.name())),
        );
        writeln!(
            output,
            "        parameters.insert(\"{}\".to_owned(), {expression});",
            parameter.name()
        )
        .expect("string");
    }
    writeln!(
        output,
        "        NamedQuery::new(\n            ApplicationContract::Exact {{\n                \
         lineage: CONTRACT_LINEAGE.to_owned(),\n                version: CONTRACT_VERSION,\n                \
         bundle_hash: Some(CONTRACT_BUNDLE_HASH),\n            }},\n            \"{name}\",\n            \
         Some(QUERY_MODULE_HASH),\n            parameters,\n            None,\n        )?.expect_plan_hash({plan_hash_constant}).with_options(options)\n    }}\n\
         \n    fn decode_result(mut response: NamedQueryResult) -> Result<Self::Output, ApplicationClientError> {{\n\
         \x20       let outcome = response.outcome.clone();\n        match outcome.as_str() {{"
    )
    .expect("string");
    for branch in schemas.results() {
        let variant = pascal(branch.name());
        writeln!(
            output,
            "            \"{}\" => {{\n                let decoded = {name}{variant} {{",
            branch.name()
        )
        .expect("string");
        for field in branch.fields() {
            let nested_name = format!("{name}{variant}{}", pascal(field.name()));
            let expression = rust_decode_top_expression(
                field.value_type(),
                &nested_name,
                field.name(),
                "response.fields",
            );
            writeln!(
                output,
                "                    {}: {expression},",
                rust_identifier(field.name())
            )
            .expect("string");
        }
        writeln!(
            output,
            "                }};\n                if !response.fields.is_empty() {{ return Err(ApplicationClientError::InvalidResponse); }}\n\
             \x20               Ok({name}Result::{variant}(Box::new(decoded)))\n            }},"
        )
        .expect("string");
    }
    writeln!(
        output,
        "            _ => Err(ApplicationClientError::InvalidResponse),\n        }}\n    }}\n}}\n"
    )
    .expect("string");
    for branch in schemas.results() {
        let variant = pascal(branch.name());
        for field in branch.fields() {
            emit_rust_query_decoder(
                output,
                &format!("{name}{variant}{}", pascal(field.name())),
                field.value_type(),
            );
        }
    }
}

fn emit_rust_query_decoder(output: &mut String, name: &str, value_type: &NamedTypeSchema) {
    match value_type {
        NamedTypeSchema::Optional(inner)
        | NamedTypeSchema::Set(inner)
        | NamedTypeSchema::List { element: inner, .. } => {
            emit_rust_query_decoder(output, name, inner);
        }
        NamedTypeSchema::Record(fields) => {
            for field in fields {
                emit_rust_query_decoder(
                    output,
                    &format!("{name}{}", pascal(field.name())),
                    field.value_type(),
                );
            }
            writeln!(
                output,
                "fn decode_{function}_record(mut record: ApplicationRecord) -> Result<{name}, ApplicationClientError> {{\n\
                 \x20   let value = {name} {{",
                function = snake(name),
            )
            .expect("string");
            for field in fields {
                let expression = rust_decode_application_expr(
                    field.value_type(),
                    &format!(
                        "take_application_value(&mut record.fields, \"{}\")?",
                        field.name()
                    ),
                    &format!("{name}{}", pascal(field.name())),
                );
                writeln!(
                    output,
                    "        {}: {expression},",
                    rust_identifier(field.name())
                )
                .expect("string");
            }
            writeln!(
                output,
                "    }};\n    if !record.fields.is_empty() {{ return Err(ApplicationClientError::InvalidResponse); }}\n\
                 \x20   Ok(value)\n}}\n"
            )
            .expect("string");
        }
        NamedTypeSchema::Scalar(_) | NamedTypeSchema::Cursor | NamedTypeSchema::Limit => {}
    }
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
            value if value.starts_with("decimal<") => format!(
                "ApplicationValue::Decimal {{ coefficient_twos_complement: {access}.coefficient_twos_complement, \
                 scale: {access}.scale, precision: {access}.precision }}"
            ),
            value if value.starts_with("string<") => format!("ApplicationValue::String({access})"),
            _ => format!("ApplicationValue::Enum({access})"),
        },
        NamedTypeSchema::Optional(inner) => format!(
            "match {access} {{ Some(value) => {}, None => ApplicationValue::Null }}",
            rust_encode_application_expr(inner, "value")
        ),
        NamedTypeSchema::Set(inner) | NamedTypeSchema::List { element: inner, .. } => {
            let element = rust_encode_application_expr(inner, "value");
            if element == "ApplicationValue::Enum(value)" {
                format!(
                    "ApplicationValue::List({access}.into_iter().map(ApplicationValue::Enum).collect())"
                )
            } else {
                format!(
                    "ApplicationValue::List({access}.into_iter().map(|value| {element}).collect())"
                )
            }
        }
        NamedTypeSchema::Cursor => format!("ApplicationValue::String({access})"),
        NamedTypeSchema::Limit => format!("ApplicationValue::U64({access})"),
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
            value if value.starts_with("decimal<") => format!("application_decimal({access})?"),
            value if value.starts_with("string<") => format!("application_string({access})?"),
            _ => format!("application_enum({access})?"),
        },
        NamedTypeSchema::Optional(inner) => format!(
            "match {access} {{ ApplicationValue::Null => None, value => Some({}) }}",
            rust_decode_application_expr(inner, "value", nested_name)
        ),
        NamedTypeSchema::Set(inner) | NamedTypeSchema::List { element: inner, .. } => format!(
            "application_list({access})?.into_iter().map(|value| Ok({})).collect::<Result<Vec<_>, ApplicationClientError>>()?",
            rust_decode_application_expr(inner, "value", nested_name)
        ),
        NamedTypeSchema::Record(_) => format!(
            "decode_{}_record(application_record({access})?)?",
            snake(nested_name)
        ),
        NamedTypeSchema::Cursor => format!("application_string({access})?"),
        NamedTypeSchema::Limit => format!("application_u64({access})?"),
    }
}

fn emit_rust_entity_types(output: &mut String, contract: &ContractBundle) {
    for entity in contract.schema().entities() {
        writeln!(
            output,
            "#[derive(Clone, Debug, Eq, PartialEq)]\npub struct {} {{",
            entity.name()
        )
        .expect("string");
        for field in entity.record().fields() {
            writeln!(
                output,
                "    pub {}: {},",
                rust_identifier(field.name()),
                rust_contract_type(field.value_type(), contract)
            )
            .expect("string");
        }
        writeln!(output, "}}\n").expect("string");
        writeln!(
            output,
            "fn decode_{}_entity(value: v1::Value) -> Result<{}, GeneratedCommandError> {{\n\
             \x20   let mut fields = wire_record_fields(value)?;\n    let entity = {} {{",
            snake(entity.name()),
            entity.name(),
            entity.name(),
        )
        .expect("string");
        for field in entity.record().fields() {
            let expression = rust_decode_wire_expr(
                field.value_type(),
                &format!("take_wire_field(&mut fields, {})?", field.id().get()),
                contract,
            );
            writeln!(
                output,
                "        {}: {expression},",
                rust_identifier(field.name())
            )
            .expect("string");
        }
        writeln!(
            output,
            "    }};\n    if !fields.is_empty() {{ return Err(GeneratedCommandError::InvalidOutcomeShape); }}\n\
             \x20   Ok(entity)\n}}\n"
        )
        .expect("string");
    }
}

fn emit_rust_command_outcome(
    output: &mut String,
    command: &CommandPlan,
    contract: &ContractBundle,
) {
    let name = command.name();
    write!(
        output,
        "#[allow(clippy::large_enum_variant)]\n\
         #[derive(Clone, Debug, Eq, PartialEq)]\n\
         pub enum {name}Outcome {{"
    )
    .expect("string");
    for outcome in command.outcomes() {
        let variant = pascal(outcome.name());
        if outcome.payload().fields().is_empty() {
            writeln!(output, "\n    {variant},").expect("string");
        } else {
            writeln!(output, "\n    {variant} {{").expect("string");
            for field in outcome.payload().fields() {
                writeln!(
                    output,
                    "        {}: {},",
                    rust_identifier(field.name()),
                    rust_contract_type(field.value_type(), contract)
                )
                .expect("string");
            }
            writeln!(output, "    }},").expect("string");
        }
    }
    writeln!(output, "}}\n").expect("string");
}

fn emit_rust_generated_command_impl(
    output: &mut String,
    command: &CommandPlan,
    contract: &ContractBundle,
) {
    let name = command.name();
    let input_name = format!("{name}Input");
    let idempotency_field = command
        .idempotency_input()
        .and_then(|id| command.input().record().field(id))
        .map(|field| rust_identifier(field.name()));
    write!(
        output,
        "const {}_PLAN_HASH: [u8; 32] = [",
        screaming_snake(name)
    )
    .expect("string");
    for (index, byte) in command.plan_hash().as_bytes().iter().enumerate() {
        if index != 0 {
            write!(output, ", ").expect("string");
        }
        write!(output, "0x{byte:02x}").expect("string");
    }
    writeln!(output, "];").expect("string");
    writeln!(
        output,
        "impl GeneratedCommand for {input_name} {{\n    type Outcome = {name}Outcome;\n\
         \n    fn idempotent_command(&self) -> Result<IdempotentCommand, GeneratedCommandError> {{\n\
         \x20       let fields = vec!["
    )
    .expect("string");
    for field in command.input().record().fields() {
        let expression = rust_encode_wire_expr(
            field.value_type(),
            &format!("&self.{}", rust_identifier(field.name())),
            contract,
        );
        writeln!(
            output,
            "            wire_named_field(\"{}\", {expression}),",
            field.name()
        )
        .expect("string");
    }
    writeln!(
        output,
        "        ];\n        IdempotentCommand::new(\"{name}\", Some(CONTRACT_VERSION), wire_record(fields)).map_err(Into::into)\n    }}"
    )
    .expect("string");
    if let Some(idempotency_field) = idempotency_field {
        writeln!(
            output,
            "\n    fn outcome_request(&self, request_id: riffdb_client_rust::RequestId) -> Result<v1::GetOutcomeRequest, GeneratedCommandError> {{\n\
             \x20       Ok(v1::GetOutcomeRequest {{\n            request_id: request_id.into_bytes().to_vec(),\n            \
             contract_lineage: CONTRACT_LINEAGE.to_owned(),\n            command_name: \"{name}\".to_owned(),\n            \
             idempotency_key: self.{idempotency_field}.clone(),\n            outcome_uri: None,\n        }})\n    }}"
        )
        .expect("string");
    } else {
        writeln!(
            output,
            "\n    fn outcome_request(&self, _request_id: riffdb_client_rust::RequestId) -> Result<v1::GetOutcomeRequest, GeneratedCommandError> {{\n\
             \x20       Err(GeneratedCommandError::InvalidInputShape)\n    }}"
        )
        .expect("string");
    }
    writeln!(
        output,
        "\n    fn decode_outcome(&self, response: &v1::ExecuteCommandResponse) -> Result<Self::Outcome, GeneratedCommandError> {{\n\
         \x20       let mut fields = wire_outcome_fields(response, &{}_PLAN_HASH)?;\n        match response.outcome_type.as_str() {{",
        screaming_snake(name)
    )
    .expect("string");
    for outcome in command.outcomes() {
        let variant = pascal(outcome.name());
        if outcome.payload().fields().is_empty() {
            writeln!(
                output,
                "            \"{}\" => Ok(Self::Outcome::{variant}),",
                outcome.name()
            )
            .expect("string");
        } else {
            writeln!(
                output,
                "            \"{}\" => {{\n                let outcome = Self::Outcome::{variant} {{",
                outcome.name()
            )
            .expect("string");
            for field in outcome.payload().fields() {
                let expression = rust_decode_wire_expr(
                    field.value_type(),
                    &format!("take_wire_field(&mut fields, {})?", field.id().get()),
                    contract,
                );
                writeln!(
                    output,
                    "                    {}: {expression},",
                    rust_identifier(field.name())
                )
                .expect("string");
            }
            writeln!(
                output,
                "                }};\n                if !fields.is_empty() {{ return Err(GeneratedCommandError::InvalidOutcomeShape); }}\n\
                 \x20               Ok(outcome)\n            }},"
            )
            .expect("string");
        }
    }
    writeln!(
        output,
        "            _ => Err(GeneratedCommandError::InvalidOutcomeShape),\n        }}\n    }}\n}}\n"
    )
    .expect("string");
}

fn emit_rust_client_facade(output: &mut String, module: &QueryModule, commands: &[&CommandPlan]) {
    let client_name = format!("{}Client", pascal(module.contract_lineage().as_str()));
    writeln!(
        output,
        "pub struct {client_name} {{\n    client: StableApplicationClient,\n    metadata: CallMetadata,\n    \
         command_attempts: AttemptBudget,\n}}\n\
         impl {client_name} {{\n    pub const fn new(client: StableApplicationClient, metadata: CallMetadata, \
         command_attempts: AttemptBudget) -> Self {{\n        Self {{ client, metadata, command_attempts }}\n    }}\n"
    )
    .expect("string");
    for query in module.queries() {
        let name = query.name();
        writeln!(
            output,
            "    pub async fn {function}(&mut self, parameters: {name}Params) \
             -> Result<{name}Result, ApplicationClientError> {{\n\
             \x20       Ok(self.{function}_with_options(parameters, QueryOptions::new()).await?.value)\n    }}\n\
             \x20   pub async fn {function}_with_options(&mut self, parameters: {name}Params, options: QueryOptions) \
             -> Result<TypedQueryResult<{name}Result>, ApplicationClientError> {{\n\
             \x20       self.client.execute_generated_query({name}Query(parameters), options, &self.metadata).await\n    }}\n",
            function = snake(name)
        )
        .expect("string");
    }
    for command in commands {
        let name = command.name();
        writeln!(
            output,
            "    pub async fn {function}(&mut self, input: {name}Input) \
             -> Result<TypedCommandResult<{name}Outcome>, ApplicationClientError> {{\n\
             \x20       self.client.execute_generated_command(&input, self.command_attempts, &self.metadata).await.map_err(Into::into)\n    }}\n",
            function = snake(name)
        )
        .expect("string");
        writeln!(
            output,
            "    pub async fn {function}_batch(&self, inputs: Vec<{name}Input>, options: GeneratedBatchOptions) \
             -> Result<GeneratedBatchResult<{name}Outcome>, GeneratedBatchError> {{\n\
             \x20       self.client.execute_generated_command_batch(inputs, options, self.command_attempts, &self.metadata).await\n    }}\n",
            function = snake(name)
        )
        .expect("string");
        writeln!(
            output,
            "    pub async fn {function}_batch_with_progress<F>(&self, inputs: Vec<{name}Input>, options: GeneratedBatchOptions, progress: F) \
             -> Result<GeneratedBatchResult<{name}Outcome>, GeneratedBatchError>\n    where\n        F: FnMut(GeneratedBatchProgress),\n    {{\n\
             \x20       self.client.execute_generated_command_batch_with_progress(inputs, options, self.command_attempts, &self.metadata, progress).await\n    }}\n",
            function = snake(name)
        )
        .expect("string");
    }
    writeln!(output, "}}\n").expect("string");
}

fn emit_rust_runtime_helpers(output: &mut String) {
    output.push_str(
        r#"fn take_result_field(fields: &mut BTreeMap<String, riffdb_client_rust::ApplicationResultField>, name: &str) -> Result<riffdb_client_rust::ApplicationResultField, ApplicationClientError> {
    fields.remove(name).ok_or(ApplicationClientError::InvalidResponse)
}
fn one_result_record(field: riffdb_client_rust::ApplicationResultField) -> Result<ApplicationRecord, ApplicationClientError> {
    if field.cardinality != ApplicationCardinality::One || field.records.len() != 1 { return Err(ApplicationClientError::InvalidResponse); }
    field.records.into_iter().next().ok_or(ApplicationClientError::InvalidResponse)
}
fn optional_result_record(field: riffdb_client_rust::ApplicationResultField) -> Result<Option<ApplicationRecord>, ApplicationClientError> {
    if field.cardinality != ApplicationCardinality::Maybe || field.records.len() > 1 { return Err(ApplicationClientError::InvalidResponse); }
    Ok(field.records.into_iter().next())
}
fn many_result_records(field: riffdb_client_rust::ApplicationResultField) -> Result<Vec<ApplicationRecord>, ApplicationClientError> {
    if field.cardinality != ApplicationCardinality::Many { return Err(ApplicationClientError::InvalidResponse); }
    Ok(field.records)
}
fn take_application_value(fields: &mut BTreeMap<String, ApplicationValue>, name: &str) -> Result<ApplicationValue, ApplicationClientError> {
    fields.remove(name).ok_or(ApplicationClientError::InvalidResponse)
}
fn application_bool(value: ApplicationValue) -> Result<bool, ApplicationClientError> { if let ApplicationValue::Bool(value) = value { Ok(value) } else { Err(ApplicationClientError::InvalidResponse) } }
fn application_i64(value: ApplicationValue) -> Result<i64, ApplicationClientError> { if let ApplicationValue::I64(value) = value { Ok(value) } else { Err(ApplicationClientError::InvalidResponse) } }
fn application_u64(value: ApplicationValue) -> Result<u64, ApplicationClientError> { if let ApplicationValue::U64(value) = value { Ok(value) } else { Err(ApplicationClientError::InvalidResponse) } }
fn application_string(value: ApplicationValue) -> Result<String, ApplicationClientError> { if let ApplicationValue::String(value) = value { Ok(value) } else { Err(ApplicationClientError::InvalidResponse) } }
fn application_uuid(value: ApplicationValue) -> Result<String, ApplicationClientError> { if let ApplicationValue::Uuid(value) = value { Ok(value.into_string()) } else { Err(ApplicationClientError::InvalidResponse) } }
fn application_enum(value: ApplicationValue) -> Result<String, ApplicationClientError> { if let ApplicationValue::Enum(value) = value { Ok(value) } else { Err(ApplicationClientError::InvalidResponse) } }
fn application_bytes(value: ApplicationValue) -> Result<Vec<u8>, ApplicationClientError> { if let ApplicationValue::Bytes(value) = value { Ok(value) } else { Err(ApplicationClientError::InvalidResponse) } }
fn application_date(value: ApplicationValue) -> Result<i32, ApplicationClientError> { if let ApplicationValue::Date(value) = value { Ok(value) } else { Err(ApplicationClientError::InvalidResponse) } }
fn application_timestamp(value: ApplicationValue) -> Result<TimestampValue, ApplicationClientError> { if let ApplicationValue::Timestamp { seconds, nanos } = value { Ok(TimestampValue { seconds, nanos }) } else { Err(ApplicationClientError::InvalidResponse) } }
fn application_decimal(value: ApplicationValue) -> Result<DecimalValue, ApplicationClientError> { if let ApplicationValue::Decimal { coefficient_twos_complement, scale, precision } = value { Ok(DecimalValue { coefficient_twos_complement, scale, precision }) } else { Err(ApplicationClientError::InvalidResponse) } }
fn application_list(value: ApplicationValue) -> Result<Vec<ApplicationValue>, ApplicationClientError> { if let ApplicationValue::List(value) = value { Ok(value) } else { Err(ApplicationClientError::InvalidResponse) } }
fn application_record(value: ApplicationValue) -> Result<ApplicationRecord, ApplicationClientError> { if let ApplicationValue::Record(fields) = value { Ok(ApplicationRecord { entity: String::new(), fields }) } else { Err(ApplicationClientError::InvalidResponse) } }
fn wire_named_field(name: &str, value: v1::Value) -> v1::ValueField { v1::ValueField { field_id: None, name: name.to_owned(), value: Some(value) } }
fn wire_record(mut fields: Vec<v1::ValueField>) -> v1::Value {
    fields.sort_by(|left, right| left.name.cmp(&right.name));
    v1::Value { kind: Some(WireKind::RecordValue(v1::ValueRecord { fields })) }
}
fn wire_null() -> v1::Value { v1::Value { kind: Some(WireKind::NullValue(v1::NullValue::NullValue as i32)) } }
fn wire_string(value: String) -> v1::Value { v1::Value { kind: Some(WireKind::StringValue(value)) } }
fn wire_bool(value: bool) -> v1::Value { v1::Value { kind: Some(WireKind::BoolValue(value)) } }
fn wire_i64(value: i64) -> v1::Value { v1::Value { kind: Some(WireKind::I64Value(value)) } }
fn wire_u64(value: u64) -> v1::Value { v1::Value { kind: Some(WireKind::U64Value(value)) } }
fn wire_bytes(value: Vec<u8>) -> v1::Value { v1::Value { kind: Some(WireKind::BytesValue(value)) } }
fn wire_date(value: i32) -> v1::Value { v1::Value { kind: Some(WireKind::DateValue(v1::Date { days_since_unix_epoch: value })) } }
fn wire_timestamp(value: &TimestampValue) -> Result<v1::Value, GeneratedCommandError> { if value.nanos >= 1_000_000_000 { return Err(GeneratedCommandError::InvalidInputShape); } Ok(v1::Value { kind: Some(WireKind::TimestampValue(v1::Timestamp { seconds: value.seconds, nanos: value.nanos })) }) }
fn wire_decimal(value: &DecimalValue) -> v1::Value { v1::Value { kind: Some(WireKind::DecimalValue(v1::Decimal { coefficient_twos_complement: value.coefficient_twos_complement.clone(), scale: value.scale, precision: value.precision })) } }
fn wire_enum(value: String) -> v1::Value { v1::Value { kind: Some(WireKind::EnumValue(v1::EnumValue { type_id: 0, variant_id: 0, name: value })) } }
fn wire_uuid(value: &str) -> Result<v1::Value, GeneratedCommandError> {
    if value.len() != 36 { return Err(GeneratedCommandError::InvalidInputShape); }
    let compact = value.bytes().filter(|byte| *byte != b'-').collect::<Vec<_>>();
    if compact.len() != 32 { return Err(GeneratedCommandError::InvalidInputShape); }
    let mut bytes = Vec::with_capacity(16);
    for pair in compact.chunks_exact(2) {
        let text = std::str::from_utf8(pair).map_err(|_| GeneratedCommandError::InvalidInputShape)?;
        bytes.push(u8::from_str_radix(text, 16).map_err(|_| GeneratedCommandError::InvalidInputShape)?);
    }
    Ok(v1::Value { kind: Some(WireKind::UuidValue(bytes)) })
}
fn wire_outcome_fields(response: &v1::ExecuteCommandResponse, plan_hash: &[u8; 32]) -> Result<BTreeMap<u32, v1::Value>, GeneratedCommandError> {
    if response.contract_version != CONTRACT_VERSION || response.plan_hash.as_slice() != plan_hash { return Err(GeneratedCommandError::InvalidOutcomeShape); }
    wire_record_fields(response.outcome.clone().ok_or(GeneratedCommandError::InvalidOutcomeShape)?)
}
fn wire_record_fields(value: v1::Value) -> Result<BTreeMap<u32, v1::Value>, GeneratedCommandError> {
    let Some(WireKind::RecordValue(record)) = value.kind else { return Err(GeneratedCommandError::InvalidOutcomeShape); };
    let mut fields = BTreeMap::new();
    for field in record.fields {
        let id = field.field_id.ok_or(GeneratedCommandError::InvalidOutcomeShape)?;
        if id == 0 || fields.insert(id, field.value.ok_or(GeneratedCommandError::InvalidOutcomeShape)?).is_some() { return Err(GeneratedCommandError::InvalidOutcomeShape); }
    }
    Ok(fields)
}
fn take_wire_field(fields: &mut BTreeMap<u32, v1::Value>, id: u32) -> Result<v1::Value, GeneratedCommandError> { fields.remove(&id).ok_or(GeneratedCommandError::InvalidOutcomeShape) }
fn decode_wire_optional<T>(value: v1::Value, decode: impl FnOnce(v1::Value) -> Result<T, GeneratedCommandError>) -> Result<Option<T>, GeneratedCommandError> {
    if matches!(value.kind.as_ref(), Some(WireKind::NullValue(_))) { Ok(None) } else { decode(value).map(Some) }
}
fn decode_wire_bool(value: v1::Value) -> Result<bool, GeneratedCommandError> { if let Some(WireKind::BoolValue(value)) = value.kind { Ok(value) } else { Err(GeneratedCommandError::InvalidOutcomeShape) } }
fn decode_wire_i64(value: v1::Value) -> Result<i64, GeneratedCommandError> { if let Some(WireKind::I64Value(value)) = value.kind { Ok(value) } else { Err(GeneratedCommandError::InvalidOutcomeShape) } }
fn decode_wire_u64(value: v1::Value) -> Result<u64, GeneratedCommandError> { if let Some(WireKind::U64Value(value)) = value.kind { Ok(value) } else { Err(GeneratedCommandError::InvalidOutcomeShape) } }
fn decode_wire_string(value: v1::Value) -> Result<String, GeneratedCommandError> { if let Some(WireKind::StringValue(value)) = value.kind { Ok(value) } else { Err(GeneratedCommandError::InvalidOutcomeShape) } }
fn decode_wire_uuid(value: v1::Value) -> Result<String, GeneratedCommandError> {
    let Some(WireKind::UuidValue(bytes)) = value.kind else { return Err(GeneratedCommandError::InvalidOutcomeShape); };
    if bytes.len() != 16 { return Err(GeneratedCommandError::InvalidOutcomeShape); }
    Ok(format!("{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}", bytes[0],bytes[1],bytes[2],bytes[3],bytes[4],bytes[5],bytes[6],bytes[7],bytes[8],bytes[9],bytes[10],bytes[11],bytes[12],bytes[13],bytes[14],bytes[15]))
}
fn decode_wire_enum(value: v1::Value) -> Result<String, GeneratedCommandError> { if let Some(WireKind::EnumValue(value)) = value.kind { if value.name.is_empty() { Err(GeneratedCommandError::InvalidOutcomeShape) } else { Ok(value.name) } } else { Err(GeneratedCommandError::InvalidOutcomeShape) } }
fn decode_wire_bytes(value: v1::Value) -> Result<Vec<u8>, GeneratedCommandError> { if let Some(WireKind::BytesValue(value)) = value.kind { Ok(value) } else { Err(GeneratedCommandError::InvalidOutcomeShape) } }
fn decode_wire_date(value: v1::Value) -> Result<i32, GeneratedCommandError> { if let Some(WireKind::DateValue(value)) = value.kind { Ok(value.days_since_unix_epoch) } else { Err(GeneratedCommandError::InvalidOutcomeShape) } }
fn decode_wire_timestamp(value: v1::Value) -> Result<TimestampValue, GeneratedCommandError> { if let Some(WireKind::TimestampValue(value)) = value.kind { if value.nanos < 1_000_000_000 { Ok(TimestampValue { seconds: value.seconds, nanos: value.nanos }) } else { Err(GeneratedCommandError::InvalidOutcomeShape) } } else { Err(GeneratedCommandError::InvalidOutcomeShape) } }
fn decode_wire_decimal(value: v1::Value) -> Result<DecimalValue, GeneratedCommandError> { if let Some(WireKind::DecimalValue(value)) = value.kind { Ok(DecimalValue { coefficient_twos_complement: value.coefficient_twos_complement, scale: value.scale, precision: value.precision }) } else { Err(GeneratedCommandError::InvalidOutcomeShape) } }
"#,
    );
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
        return format!(
            "v1::Value {{ kind: Some(WireKind::ListValue(v1::ValueList {{ values: {access}.iter().map(|value| Ok({})).collect::<Result<Vec<_>, GeneratedCommandError>>()? }})) }}",
            rust_encode_wire_expr(inner, "value", contract)
        );
    }
    match value_type.tag() {
        ValueTypeTag::Bool => format!("wire_bool(*({access}))"),
        ValueTypeTag::I64 => format!("wire_i64(*({access}))"),
        ValueTypeTag::U64 => format!("wire_u64(*({access}))"),
        ValueTypeTag::Decimal => format!("wire_decimal({access})"),
        ValueTypeTag::Money => "return Err(GeneratedCommandError::InvalidInputShape)".to_owned(),
        ValueTypeTag::String => format!("wire_string(Clone::clone({access}))"),
        ValueTypeTag::Bytes => format!("wire_bytes(Clone::clone({access}))"),
        ValueTypeTag::Timestamp => format!("wire_timestamp({access})?"),
        ValueTypeTag::Date => format!("wire_date(*({access}))"),
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
        ValueTypeTag::Optional | ValueTypeTag::List => unreachable!("handled above"),
    }
}

fn rust_decode_wire_expr(
    value_type: &ValueType,
    access: &str,
    contract: &ContractBundle,
) -> String {
    if let Some(inner) = value_type.optional_inner() {
        return format!(
            "decode_wire_optional({access}, |value| Ok({}))?",
            rust_decode_wire_expr(inner, "value", contract)
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
        ValueTypeTag::Money => "return Err(GeneratedCommandError::InvalidOutcomeShape)".to_owned(),
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
        ValueTypeTag::Optional | ValueTypeTag::List => unreachable!("handled above"),
    }
}

/// Generates a dependency-free TypeScript request model for every named query and command.
#[must_use]
pub fn generate_typescript_client(module: &QueryModule, contract: &ContractBundle) -> String {
    let mut output = String::new();
    writeln!(output, "// @generated by riffdb-query-module; do not edit.").expect("string");
    writeln!(
        output,
        "export const QUERY_MODULE_HASH = \"{}\" as const;",
        hex(module.identity().as_bytes())
    )
    .expect("string");
    writeln!(
        output,
        "export const CONTRACT_LINEAGE = \"{}\" as const;\nexport const CONTRACT_VERSION = {} as const;\n\
         export const CONTRACT_BUNDLE_HASH = \"{}\" as const;\n",
        module.contract_lineage().as_str(),
        module.contract_version().get(),
        hex(module.contract_hash().as_bytes())
    )
    .expect("string");
    emit_typescript_application_errors(&mut output);
    writeln!(
        output,
        "export type ApplicationValueSchema =\n  | {{ readonly kind: \"bool\" | \"i64\" | \"u64\" | \"string\" | \"uuid\" | \"enum\" | \"bytes\" | \"date\" | \"timestamp\" | \"decimal\" | \"money\" | \"cursor\" | \"limit\" }}\n  | {{ readonly kind: \"optional\"; readonly value: ApplicationValueSchema }}\n  | {{ readonly kind: \"list\"; readonly value: ApplicationValueSchema; readonly maximum?: number }}\n  | {{ readonly kind: \"record\"; readonly fields: ReadonlyArray<{{ readonly name: string; readonly schema: ApplicationValueSchema; readonly wireId?: number }}> }};\n\
         export interface NamedQueryRequest<P, R> {{ readonly contractLineage: typeof CONTRACT_LINEAGE; \
         readonly contractVersion: typeof CONTRACT_VERSION; readonly contractBundleHash: typeof CONTRACT_BUNDLE_HASH; \
         readonly moduleHash: typeof QUERY_MODULE_HASH; readonly queryName: string; readonly planHash: string; readonly parameters: P; \
         readonly parameterSchema: ApplicationValueSchema; readonly resultSchemas: Readonly<Record<string, ApplicationValueSchema>>; \
         readonly decodeError: typeof decodeApplicationError; readonly resultType?: R; }}\n\
         export interface QueryResponseIdentity {{ readonly contractLineage: string; readonly contractVersion: number; \
         readonly contractBundleHash: string; readonly moduleHash: string; readonly queryName: string; readonly planHash: string; }}\n\
         export function acceptsIdentity<P, R>(request: NamedQueryRequest<P, R>, identity: QueryResponseIdentity): boolean {{\n\
         \x20 return identity.contractLineage === request.contractLineage\n    \
         && identity.contractVersion === request.contractVersion\n    \
         && identity.contractBundleHash === request.contractBundleHash\n    \
         && identity.moduleHash === request.moduleHash\n    \
         && identity.queryName === request.queryName\n    \
         && identity.planHash === request.planHash;\n}}\n\
         export interface CommandRequest<I, R> {{ readonly contractLineage: typeof CONTRACT_LINEAGE; \
         readonly contractVersion: typeof CONTRACT_VERSION; readonly commandName: string; readonly planHash: string; \
         readonly input: I; readonly idempotencyKey: string; readonly inputSchema: ApplicationValueSchema; \
         readonly outcomeSchemas: Readonly<Record<string, ApplicationValueSchema>>; \
         readonly decodeError: typeof decodeApplicationError; readonly outcomeType?: R; }}\n\
         export interface TypedQueryResult<T> {{ readonly identity: QueryResponseIdentity; readonly value: T; readonly applicationHead: bigint; readonly nextCursor?: string; }}\n\
         export interface TypedCommandResult<T> {{ readonly outcome: T; readonly commitSequence?: bigint; \
         readonly contractVersion: number; readonly planHash: string; readonly replayed: boolean; readonly outcomeUri?: string; }}\n\
         export interface QueryOptions {{ readonly cursor?: string; readonly readAfterCommit?: bigint; }}\n\
         export interface CommandBatchProgress {{ readonly completed: number; readonly total: number; readonly checkpoint: number; }}\n\
         export interface CommandBatchOptions {{ readonly concurrency: number; readonly checkpoint?: number; readonly onProgress?: (progress: CommandBatchProgress) => void; }}\n\
         export interface CommandBatchItem<T> {{ readonly index: number; readonly result?: TypedCommandResult<T>; readonly error?: unknown; }}\n\
         export interface CommandBatchResult<T> {{ readonly items: ReadonlyArray<CommandBatchItem<T>>; readonly checkpoint: number; }}\n\
         export interface ApplicationTransport {{\n  executeNamedQuery<P, R>(request: NamedQueryRequest<P, R>, options?: QueryOptions): Promise<TypedQueryResult<R>>;\n  \
         executeCommand<I, R>(request: CommandRequest<I, R>, attemptBudget: number): Promise<TypedCommandResult<R>>;\n}}\n"
    )
    .expect("string");

    for query in module.queries() {
        let name = query.name();
        let schemas = query.program().surface().schemas();
        let plan_hash = hex(query.program().identity().hash().as_bytes());
        let constant = format!("{}_QUERY_PLAN_HASH", screaming_snake(name));
        writeln!(
            output,
            "export const {constant} = \"{plan_hash}\" as const;"
        )
        .expect("string");
        writeln!(output, "export interface {name}Params {{").expect("string");
        for parameter in schemas.parameters() {
            let optional = if parameter.has_default() || is_cursor_type(parameter.value_type()) {
                "?"
            } else {
                ""
            };
            writeln!(
                output,
                "  readonly {}{}: {};",
                ts_identifier(parameter.name()),
                optional,
                ts_query_type(parameter.value_type())
            )
            .expect("string");
        }
        writeln!(output, "}}\n").expect("string");
        for branch in schemas.results() {
            let branch_name = format!("{name}{}", pascal(branch.name()));
            writeln!(output, "export interface {branch_name} {{").expect("string");
            writeln!(output, "  readonly outcome: \"{}\";", branch.name()).expect("string");
            for field in branch.fields() {
                writeln!(
                    output,
                    "  readonly {}: {};",
                    ts_identifier(field.name()),
                    ts_query_type(field.value_type())
                )
                .expect("string");
            }
            writeln!(output, "}}\n").expect("string");
        }
        write!(output, "export type {name}Result = ").expect("string");
        for (index, branch) in schemas.results().iter().enumerate() {
            if index != 0 {
                write!(output, " | ").expect("string");
            }
            write!(output, "{name}{}", pascal(branch.name())).expect("string");
        }
        writeln!(output, ";\n").expect("string");
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
        writeln!(
            output,
            "export function {function}(parameters: {name}Params): NamedQueryRequest<{name}Params, {name}Result> {{\n\
             \x20 return {{ contractLineage: CONTRACT_LINEAGE, contractVersion: CONTRACT_VERSION, \
             contractBundleHash: CONTRACT_BUNDLE_HASH, moduleHash: QUERY_MODULE_HASH, queryName: \"{name}\", planHash: {constant}, parameters, \
             parameterSchema: {parameter_schema}, resultSchemas: {result_schemas}, decodeError: decodeApplicationError }};\n\
             }}\n",
            function = camel(name),
        )
        .expect("string");
    }

    let mut commands = contract.commands().iter().collect::<Vec<_>>();
    commands.sort_by(|left, right| left.name().cmp(right.name()));
    for command in &commands {
        let name = command.name();
        writeln!(output, "export interface {name}Input {{").expect("string");
        for field in command.input().record().fields() {
            writeln!(
                output,
                "  readonly {}: {};",
                ts_identifier(field.name()),
                ts_contract_type(field.value_type(), contract)
            )
            .expect("string");
        }
        writeln!(output, "}}\n").expect("string");
        write!(output, "export type {name}Outcome = ").expect("string");
        for (index, outcome) in command.outcomes().iter().enumerate() {
            if index != 0 {
                write!(output, " | ").expect("string");
            }
            write!(output, "{{ readonly outcome: \"{}\"", outcome.name()).expect("string");
            for field in outcome.payload().fields() {
                write!(
                    output,
                    "; readonly {}: {}",
                    ts_identifier(field.name()),
                    ts_contract_type(field.value_type(), contract)
                )
                .expect("string");
            }
            write!(output, " }}").expect("string");
        }
        writeln!(output, ";\n").expect("string");
        let idempotency = command
            .idempotency_input()
            .and_then(|id| command.input().record().field(id))
            .map_or("idempotency_key", |field| field.name());
        let input_schema = ts_contract_record_schema(
            command
                .input()
                .record()
                .fields()
                .iter()
                .map(|field| (field.name(), field.id().get(), field.value_type())),
            contract,
            false,
        );
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
             \x20 return {{ contractLineage: CONTRACT_LINEAGE, contractVersion: CONTRACT_VERSION, \
             commandName: \"{name}\", planHash: {constant}_PLAN_HASH, input, idempotencyKey: input.{idempotency}, \
             inputSchema: {input_schema}, outcomeSchemas: {outcome_schemas}, decodeError: decodeApplicationError }};\n\
             }}\n",
            function = camel(name),
            constant = screaming_snake(name),
            plan_hash = hex(command.plan_hash().as_bytes()),
        )
        .expect("string");
    }
    emit_typescript_client_facade(&mut output, module, &commands);
    output
}

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
  "RDB-CAPACITY-0101": ["service is over capacity", "capacity", "retry", ["retry_later"]],
} as const;

export type ApplicationErrorCode = keyof typeof APPLICATION_ERROR_REGISTRY;
export type ApplicationOperation = "DescribeContract" | "CheckQuery" | "ExplainQuery" | "ExecuteQuery" | "DeployQueryModule" | "GetQueryModule" | "ExecuteCommand" | "BatchCommand";
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
  "DeployQueryModule", "GetQueryModule", "ExecuteCommand", "BatchCommand",
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

fn emit_typescript_client_facade(
    output: &mut String,
    module: &QueryModule,
    commands: &[&CommandPlan],
) {
    let client_name = format!("{}Client", pascal(module.contract_lineage().as_str()));
    writeln!(
        output,
        "export class {client_name} {{\n  public constructor(\n    private readonly transport: ApplicationTransport,\n    \
         private readonly commandAttemptBudget: number,\n  ) {{\n    if (!Number.isInteger(commandAttemptBudget) || commandAttemptBudget < 1) \
         throw new Error(\"invalid command attempt budget\");\n  }}\n"
    )
    .expect("string");
    for query in module.queries() {
        let name = query.name();
        writeln!(
            output,
            "  public async {function}(parameters: {name}Params, options: QueryOptions = {{}}): \
             Promise<TypedQueryResult<{name}Result>> {{\n    const request = {function}(parameters);\n    \
             const result = await this.transport.executeNamedQuery<{name}Params, {name}Result>(request, options);\n    \
             if (!acceptsIdentity(request, result.identity)) throw new Error(\"RiffDB application identity mismatch\");\n    return result;\n  }}\n",
            function = camel(name)
        )
        .expect("string");
    }
    for command in commands {
        let name = command.name();
        writeln!(
            output,
            "  public async {function}(input: {name}Input): Promise<TypedCommandResult<{name}Outcome>> {{\n    \
             const result = await this.transport.executeCommand<{name}Input, {name}Outcome>({function}(input), this.commandAttemptBudget);\n    \
             if (result.contractVersion !== CONTRACT_VERSION || result.planHash !== {constant}_PLAN_HASH) \
             throw new Error(\"RiffDB application identity mismatch\");\n    return result;\n  }}\n",
            function = camel(name),
            constant = screaming_snake(name),
        )
        .expect("string");
        writeln!(
            output,
            "  public async {function}Batch(inputs: ReadonlyArray<{name}Input>, options: CommandBatchOptions): Promise<CommandBatchResult<{name}Outcome>> {{\n    \
             if (!Number.isInteger(options.concurrency) || options.concurrency < 1 || options.concurrency > 32 \
             || inputs.length < 1 || inputs.length > 4096) throw new Error(\"invalid command batch bounds\");\n    \
             const start = options.checkpoint ?? 0;\n    if (!Number.isInteger(start) || start < 0 || start > inputs.length) throw new Error(\"invalid command batch checkpoint\");\n    \
             const items: CommandBatchItem<{name}Outcome>[] = [];\n    let next = start;\n    let completed = start;\n    let checkpoint = start;\n    const completedAfterCheckpoint = new Set<number>();\n    \
             const worker = async (): Promise<void> => {{ while (true) {{ const index = next++; if (index >= inputs.length) return; \
             try {{ items.push({{ index, result: await this.{function}(inputs[index]!) }}); }} catch (error) {{ items.push({{ index, error }}); }} \
             completed += 1; completedAfterCheckpoint.add(index); while (completedAfterCheckpoint.delete(checkpoint)) checkpoint += 1; \
             options.onProgress?.({{ completed, total: inputs.length, checkpoint }}); }} }};\n    \
             await Promise.all(Array.from({{ length: Math.min(options.concurrency, inputs.length - start) }}, worker));\n    \
             items.sort((left, right) => left.index - right.index);\n    return {{ items, checkpoint }};\n  }}\n",
            function = camel(name),
        )
        .expect("string");
    }
    writeln!(output, "}}\n").expect("string");
}

fn emit_rust_identity(output: &mut String, module: &QueryModule) {
    write!(output, "pub const QUERY_MODULE_HASH: [u8; 32] = [").expect("string");
    for (index, byte) in module.identity().as_bytes().iter().enumerate() {
        if index != 0 {
            write!(output, ", ").expect("string");
        }
        write!(output, "0x{byte:02x}").expect("string");
    }
    writeln!(output, "];").expect("string");
    writeln!(
        output,
        "pub const CONTRACT_LINEAGE: &str = \"{}\";\npub const CONTRACT_VERSION: u64 = {};\n",
        module.contract_lineage().as_str(),
        module.contract_version().get()
    )
    .expect("string");
    write!(output, "pub const CONTRACT_BUNDLE_HASH: [u8; 32] = [").expect("string");
    for (index, byte) in module.contract_hash().as_bytes().iter().enumerate() {
        if index != 0 {
            write!(output, ", ").expect("string");
        }
        write!(output, "0x{byte:02x}").expect("string");
    }
    writeln!(output, "];\n").expect("string");
}

fn emit_rust_fields_struct<'a>(
    output: &mut String,
    name: &str,
    fields: impl Iterator<Item = (&'a str, &'a NamedTypeSchema)>,
) {
    let fields = fields.collect::<Vec<_>>();
    for (field, value_type) in &fields {
        emit_rust_nested_type(output, &format!("{name}{}", pascal(field)), value_type);
    }
    writeln!(
        output,
        "#[derive(Clone, Debug, Eq, PartialEq)]\npub struct {name} {{"
    )
    .expect("string");
    for (field, value_type) in fields {
        writeln!(
            output,
            "    pub {}: {},",
            rust_identifier(field),
            rust_query_type(value_type, &format!("{name}{}", pascal(field)))
        )
        .expect("string");
    }
    writeln!(output, "}}\n").expect("string");
}

fn emit_rust_nested_type(output: &mut String, name: &str, value_type: &NamedTypeSchema) {
    match value_type {
        NamedTypeSchema::Optional(inner)
        | NamedTypeSchema::Set(inner)
        | NamedTypeSchema::List { element: inner, .. } => {
            emit_rust_nested_type(output, name, inner);
        }
        NamedTypeSchema::Record(fields) => {
            for field in fields {
                emit_rust_nested_type(
                    output,
                    &format!("{name}{}", pascal(field.name())),
                    field.value_type(),
                );
            }
            writeln!(
                output,
                "#[derive(Clone, Debug, Eq, PartialEq)]\npub struct {name} {{"
            )
            .expect("string");
            for field in fields {
                writeln!(
                    output,
                    "    pub {}: {},",
                    rust_identifier(field.name()),
                    rust_query_type(
                        field.value_type(),
                        &format!("{name}{}", pascal(field.name()))
                    )
                )
                .expect("string");
            }
            writeln!(output, "}}\n").expect("string");
        }
        NamedTypeSchema::Scalar(_) | NamedTypeSchema::Cursor | NamedTypeSchema::Limit => {}
    }
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
            value if value.starts_with("decimal<") => "DecimalValue".to_owned(),
            _ => "String".to_owned(),
        },
        NamedTypeSchema::Optional(inner) => {
            format!("Option<{}>", rust_query_type(inner, nested_name))
        }
        NamedTypeSchema::Set(inner) | NamedTypeSchema::List { element: inner, .. } => {
            format!("Vec<{}>", rust_query_type(inner, nested_name))
        }
        NamedTypeSchema::Record(_) => nested_name.to_owned(),
        NamedTypeSchema::Cursor => "String".to_owned(),
        NamedTypeSchema::Limit => "u64".to_owned(),
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

fn ts_query_type(value_type: &NamedTypeSchema) -> String {
    match value_type {
        NamedTypeSchema::Scalar(name) => match name.as_str() {
            "bool" => "boolean".to_owned(),
            "i64" | "u64" => "bigint".to_owned(),
            "timestamp" => "{ readonly seconds: bigint; readonly nanos: number }".to_owned(),
            "date" => "number".to_owned(),
            value if value.starts_with("bytes<") => "Uint8Array".to_owned(),
            value if value.starts_with("decimal<") => {
                "{ readonly coefficientTwosComplement: Uint8Array; readonly scale: number; readonly precision?: number }".to_owned()
            }
            _ => "string".to_owned(),
        },
        NamedTypeSchema::Optional(inner) => format!("{} | null", ts_query_type(inner)),
        NamedTypeSchema::Set(inner) | NamedTypeSchema::List { element: inner, .. } => {
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
            let kind = match name.as_str() {
                "bool" => "bool",
                "i64" => "i64",
                "u64" => "u64",
                "uuid" => "uuid",
                "timestamp" => "timestamp",
                "date" => "date",
                value if value.starts_with("bytes<") => "bytes",
                value if value.starts_with("decimal<") => "decimal",
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
    }
}

fn ts_contract_record_schema<'a>(
    fields: impl Iterator<Item = (&'a str, u32, &'a ValueType)>,
    contract: &ContractBundle,
    wire_ids: bool,
) -> Value {
    json!({
        "kind": "record",
        "fields": fields.map(|(name, wire_id, schema)| {
            let mut field = Map::new();
            field.insert("name".to_owned(), Value::String(name.to_owned()));
            field.insert("schema".to_owned(), ts_contract_value_schema(schema, contract));
            if wire_ids {
                field.insert("wireId".to_owned(), Value::from(wire_id));
            }
            Value::Object(field)
        }).collect::<Vec<_>>(),
    })
}

fn ts_contract_value_schema(value_type: &ValueType, contract: &ContractBundle) -> Value {
    if let Some(inner) = value_type.optional_inner() {
        return json!({"kind": "optional", "value": ts_contract_value_schema(inner, contract)});
    }
    if let Some((inner, maximum)) = value_type.list_parts() {
        return json!({
            "kind": "list",
            "value": ts_contract_value_schema(inner, contract),
            "maximum": maximum,
        });
    }
    let kind = match value_type.tag() {
        ValueTypeTag::Bool => "bool",
        ValueTypeTag::I64 => "i64",
        ValueTypeTag::U64 => "u64",
        ValueTypeTag::Decimal => "decimal",
        ValueTypeTag::Money => "money",
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
                return ts_contract_record_schema(
                    entity
                        .record()
                        .fields()
                        .iter()
                        .map(|field| (field.name(), field.id().get(), field.value_type())),
                    contract,
                    true,
                );
            }
            "string"
        }
        ValueTypeTag::Optional | ValueTypeTag::List => unreachable!("handled above"),
    };
    json!({"kind": kind})
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
        ValueTypeTag::Decimal => {
            "{ readonly coefficientTwosComplement: Uint8Array; readonly scale: number; readonly precision?: number }"
        }
        ValueTypeTag::Money => {
            "{ readonly currency: string; readonly amount: { readonly coefficientTwosComplement: Uint8Array; readonly scale: number; readonly precision?: number } }"
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
        write!(output, "{byte:02x}").expect("string");
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_contract_compiler::compile_contract_source;

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
            "decode_wire_optional(take_wire_field(&mut fields, 12)?, |value| Ok(decode_wire_i64(value)?))?"
        );
    }
}
