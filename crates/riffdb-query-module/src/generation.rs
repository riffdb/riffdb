//! Reproducible, name-addressed client source generation.

use std::collections::BTreeSet;
use std::fmt::Write as _;

use riffdb_contract_ir::{ContractBundle, ValueType, ValueTypeTag};
use riffdb_query_ir::{NamedTypeSchema, PageBound};
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
            let name = format!("{}.{}", snake(module.name().as_str()), snake(query.name()));
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
                PageBound::Parameter(_) => 500,
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
        NamedTypeSchema::Limit => json!({"maximum": 500, "minimum": 1, "type": "integer"}),
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
        "// @generated by riffdb-query-module; do not edit.\n"
    )
    .expect("string");
    emit_rust_identity(&mut output, module);
    writeln!(
        output,
        "#[derive(Clone, Debug, Eq, PartialEq)]\n\
         pub struct NamedQueryRequest<P> {{\n    pub contract_lineage: &'static str,\n    \
         pub contract_version: u64,\n    pub contract_bundle_hash: [u8; 32],\n    \
         pub module_hash: [u8; 32],\n    pub query_name: &'static str,\n    pub parameters: P,\n}}\n\
         #[derive(Clone, Debug, Eq, PartialEq)]\n\
         pub struct QueryResponseIdentity<'a> {{\n    pub contract_lineage: &'a str,\n    \
         pub contract_version: u64,\n    pub contract_bundle_hash: [u8; 32],\n    \
         pub module_hash: [u8; 32],\n    pub query_name: &'a str,\n}}\n\
         impl<P> NamedQueryRequest<P> {{\n    pub fn accepts_identity(&self, identity: &QueryResponseIdentity<'_>) -> bool {{\n        \
         identity.contract_lineage == self.contract_lineage\n            \
         && identity.contract_version == self.contract_version\n            \
         && identity.contract_bundle_hash == self.contract_bundle_hash\n            \
         && identity.module_hash == self.module_hash\n            \
         && identity.query_name == self.query_name\n    }}\n}}\n"
    )
    .expect("string");
    writeln!(
        output,
        "#[derive(Clone, Debug, Eq, PartialEq)]\n\
         pub struct CommandRequest<I> {{\n    pub command_name: &'static str,\n    \
         pub input: I,\n    pub idempotency_key: String,\n}}\n"
    )
    .expect("string");

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
        writeln!(
            output,
            "pub fn {function}(parameters: {params_name}) -> NamedQueryRequest<{params_name}> {{\n\
             \x20   NamedQueryRequest {{ contract_lineage: CONTRACT_LINEAGE, contract_version: CONTRACT_VERSION, \
             contract_bundle_hash: CONTRACT_BUNDLE_HASH, module_hash: QUERY_MODULE_HASH, query_name: \"{name}\", parameters }}\n\
             }}\n",
            function = snake(name),
        )
        .expect("string");
    }

    let mut commands = contract.commands().iter().collect::<Vec<_>>();
    commands.sort_by(|left, right| left.name().cmp(right.name()));
    for command in commands {
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
                rust_contract_type(field.value_type())
            )
            .expect("string");
        }
        writeln!(output, "}}\n").expect("string");
        writeln!(
            output,
            "pub fn {function}(input: {input_name}, idempotency_key: String) -> CommandRequest<{input_name}> {{\n\
             \x20   CommandRequest {{ command_name: \"{name}\", input, idempotency_key }}\n\
             }}\n",
            function = snake(name),
        )
        .expect("string");
    }
    output
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
    writeln!(
        output,
        "export interface NamedQueryRequest<P> {{ readonly contractLineage: typeof CONTRACT_LINEAGE; \
         readonly contractVersion: typeof CONTRACT_VERSION; readonly contractBundleHash: typeof CONTRACT_BUNDLE_HASH; \
         readonly moduleHash: typeof QUERY_MODULE_HASH; readonly queryName: string; readonly parameters: P; }}\n\
         export interface QueryResponseIdentity {{ readonly contractLineage: string; readonly contractVersion: number; \
         readonly contractBundleHash: string; readonly moduleHash: string; readonly queryName: string; }}\n\
         export function acceptsIdentity<P>(request: NamedQueryRequest<P>, identity: QueryResponseIdentity): boolean {{\n\
         \x20 return identity.contractLineage === request.contractLineage\n    \
         && identity.contractVersion === request.contractVersion\n    \
         && identity.contractBundleHash === request.contractBundleHash\n    \
         && identity.moduleHash === request.moduleHash\n    \
         && identity.queryName === request.queryName;\n}}\n\
         export interface CommandRequest<I> {{ readonly commandName: string; readonly input: I; \
         readonly idempotencyKey: string; }}\n"
    )
    .expect("string");

    for query in module.queries() {
        let name = query.name();
        let schemas = query.program().surface().schemas();
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
        writeln!(
            output,
            "export function {function}(parameters: {name}Params): NamedQueryRequest<{name}Params> {{\n\
             \x20 return {{ contractLineage: CONTRACT_LINEAGE, contractVersion: CONTRACT_VERSION, \
             contractBundleHash: CONTRACT_BUNDLE_HASH, moduleHash: QUERY_MODULE_HASH, queryName: \"{name}\", parameters }};\n\
             }}\n",
            function = camel(name),
        )
        .expect("string");
    }

    let mut commands = contract.commands().iter().collect::<Vec<_>>();
    commands.sort_by(|left, right| left.name().cmp(right.name()));
    for command in commands {
        let name = command.name();
        writeln!(output, "export interface {name}Input {{").expect("string");
        for field in command.input().record().fields() {
            writeln!(
                output,
                "  readonly {}: {};",
                ts_identifier(field.name()),
                ts_contract_type(field.value_type())
            )
            .expect("string");
        }
        writeln!(output, "}}\n").expect("string");
        writeln!(
            output,
            "export function {function}(input: {name}Input, idempotencyKey: string): CommandRequest<{name}Input> {{\n\
             \x20 return {{ commandName: \"{name}\", input, idempotencyKey }};\n\
             }}\n",
            function = camel(name),
        )
        .expect("string");
    }
    output
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
            "Bool" => "bool".to_owned(),
            "I64" => "i64".to_owned(),
            "U64" | "Limit" => "u64".to_owned(),
            "Bytes" => "Vec<u8>".to_owned(),
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

fn rust_contract_type(value_type: &ValueType) -> String {
    if let Some(inner) = value_type.optional_inner() {
        return format!("Option<{}>", rust_contract_type(inner));
    }
    if let Some((inner, _)) = value_type.list_parts() {
        return format!("Vec<{}>", rust_contract_type(inner));
    }
    match value_type.tag() {
        ValueTypeTag::Bool => "bool",
        ValueTypeTag::I64 => "i64",
        ValueTypeTag::U64 => "u64",
        ValueTypeTag::Bytes => "Vec<u8>",
        _ => "String",
    }
    .to_owned()
}

fn ts_query_type(value_type: &NamedTypeSchema) -> String {
    match value_type {
        NamedTypeSchema::Scalar(name) => match name.as_str() {
            "Bool" => "boolean".to_owned(),
            "I64" | "U64" => "bigint".to_owned(),
            "Bytes" => "Uint8Array".to_owned(),
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

fn ts_contract_type(value_type: &ValueType) -> String {
    if let Some(inner) = value_type.optional_inner() {
        return format!("{} | null", ts_contract_type(inner));
    }
    if let Some((inner, _)) = value_type.list_parts() {
        return format!("ReadonlyArray<{}>", ts_contract_type(inner));
    }
    match value_type.tag() {
        ValueTypeTag::Bool => "boolean",
        ValueTypeTag::I64 | ValueTypeTag::U64 => "bigint",
        ValueTypeTag::Bytes => "Uint8Array",
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
