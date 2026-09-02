//! Deterministic Go application-facade generation over `riffdb-driverd`.

use std::fmt::Write as _;

use crate::infallible_string_write::InfallibleStringWrite as _;

use riffdb_contract_ir::{ContractBundle, RecordTypeRef, ValueType, ValueTypeTag};
use riffdb_query_ir::{NamedTypeSchema, ReactiveModulePlanV1};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::QueryModule;
use crate::template_generation::{generate_canonical_generation_model, render_go_client_header};

/// Generates one dependency-light Go package for all named queries and commands.
#[must_use]
pub fn generate_go_client(module: &QueryModule, contract: &ContractBundle) -> String {
    generate_go_client_inner(module, contract, &[])
}

/// Generates one dependency-light Go package including exact reactive facades.
#[must_use]
pub fn generate_go_application_client(
    module: &QueryModule,
    contract: &ContractBundle,
    reactive_modules: &[ReactiveModulePlanV1],
) -> String {
    generate_go_client_inner(module, contract, reactive_modules)
}

fn generate_go_client_inner(
    module: &QueryModule,
    contract: &ContractBundle,
    reactive_modules: &[ReactiveModulePlanV1],
) -> String {
    let model = generate_canonical_generation_model(module, contract, reactive_modules);
    render_go_client_header(&model).expect("static Go template and model")
}

// Template helper expressions used while assembling the canonical model.

#[allow(dead_code)]
pub(crate) fn go_decode_compact_expr(
    value: &str,
    ty: &ValueType,
    contract: &ContractBundle,
) -> String {
    if let Some(inner) = ty.optional_inner() {
        return format!(
            "riffdb.DecodeOptional({value}, func(item riffdb.Value) ({}, error) {{ return {} }})",
            go_type(inner, contract),
            go_decode_compact_expr("item", inner, contract)
        );
    }
    match ty.tag() {
        ValueTypeTag::String => format!("func() (string, error) {{ result, err := riffdb.StringValue({value}); if err != nil || len(result) > {} {{ return \"\", errors.New(\"invalid RiffDB compact value\") }}; return result, nil }}()", ty.byte_bound().expect("string bound")),
        ValueTypeTag::Enum => {
            let enumeration = contract.schema().enumeration(ty.enum_type_id().expect("enum identity")).expect("validated enum");
            let name = go_public(enumeration.name());
            let cases = enumeration.variants().iter().map(|variant| format!("{:?}", variant.name())).collect::<Vec<_>>().join(", ");
            format!("func() ({name}, error) {{ result, err := riffdb.EnumValue({value}); if err != nil {{ return \"\", err }}; switch result {{ case {cases}: return {name}(result), nil; default: return \"\", errors.New(\"invalid RiffDB compact enum\") }} }}()")
        }
        ValueTypeTag::Bool | ValueTypeTag::I64 | ValueTypeTag::U64 | ValueTypeTag::Timestamp | ValueTypeTag::Date | ValueTypeTag::Uuid | ValueTypeTag::Vector => decode_expr(value, ty, contract),
        ValueTypeTag::Optional => unreachable!("handled above"),
        _ => "func() (string, error) { return \"\", errors.New(\"unsupported RiffDB compact value\") }()".to_owned(),
    }
}

#[allow(dead_code)]
pub(crate) fn go_reactive_type(type_name: &str, contract: &ContractBundle) -> String {
    if let Some(enumeration) = contract
        .schema()
        .enums()
        .iter()
        .find(|value| value.name() == type_name)
    {
        return go_public(enumeration.name());
    }
    if type_name.starts_with("decimal<") {
        return "riffdb.ExactDecimal".to_owned();
    }
    if type_name.starts_with("money<") {
        return "riffdb.ExactMoney".to_owned();
    }
    if type_name.starts_with("bytes<") {
        return "[]byte".to_owned();
    }
    if type_name.starts_with("string<") || type_name == "cursor" {
        return "string".to_owned();
    }
    match type_name {
        "bool" => "bool",
        "i64" => "int64",
        "u64" | "limit" => "uint64",
        "uuid" => "string",
        "date" => "int32",
        "timestamp" => "riffdb.Instant",
        _ => "string",
    }
    .to_owned()
}

pub(crate) fn go_reactive_encode_expr(
    value: &str,
    type_name: &str,
    contract: &ContractBundle,
) -> String {
    if contract
        .schema()
        .enums()
        .iter()
        .any(|enumeration| enumeration.name() == type_name)
    {
        return format!("riffdb.Enum(string({value}))");
    }
    if type_name.starts_with("decimal<") {
        return format!("riffdb.DecimalFrom({value})");
    }
    if type_name.starts_with("money<") {
        return format!("riffdb.MoneyFrom({value})");
    }
    if type_name.starts_with("bytes<") {
        return format!("riffdb.BytesFrom({value})");
    }
    if type_name.starts_with("string<") || type_name == "cursor" {
        return format!("riffdb.String({value})");
    }
    match type_name {
        "bool" => format!("riffdb.Bool({value})"),
        "i64" => format!("riffdb.I64({value})"),
        "u64" | "limit" => format!("riffdb.U64({value})"),
        "uuid" => format!("riffdb.UUID({value})"),
        "date" => format!("riffdb.Date({value})"),
        "timestamp" => format!("riffdb.TimestampFrom({value})"),
        _ => format!("riffdb.String({value})"),
    }
}

pub(crate) fn go_reactive_decode_expr(
    value: &str,
    type_name: &str,
    contract: &ContractBundle,
) -> String {
    if let Some(enumeration) = contract
        .schema()
        .enums()
        .iter()
        .find(|enumeration| enumeration.name() == type_name)
    {
        let name = go_public(enumeration.name());
        return format!(
            "func() ({name}, error) {{ raw, err := riffdb.EnumValue({value}); return {name}(raw), err }}()"
        );
    }
    if type_name.starts_with("decimal<") {
        return format!("riffdb.DecimalValue({value})");
    }
    if type_name.starts_with("money<") {
        return format!("riffdb.MoneyValue({value})");
    }
    if type_name.starts_with("bytes<") {
        return format!("riffdb.BytesValue({value})");
    }
    if type_name.starts_with("string<") || type_name == "cursor" {
        return format!("riffdb.StringValue({value})");
    }
    match type_name {
        "bool" => format!("riffdb.BoolValue({value})"),
        "i64" => format!("riffdb.I64Value({value})"),
        "u64" | "limit" => format!("riffdb.U64Value({value})"),
        "uuid" => format!("riffdb.UUIDValue({value})"),
        "date" => format!("riffdb.DateValue({value})"),
        "timestamp" => format!("riffdb.TimestampValue({value})"),
        _ => format!("riffdb.StringValue({value})"),
    }
}

pub(crate) fn go_snake(value: &str) -> String {
    let mut output = String::new();
    for (index, character) in value.chars().enumerate() {
        if character.is_ascii_uppercase() {
            if index != 0 {
                output.push('_');
            }
            output.push(character.to_ascii_lowercase());
        } else if character.is_ascii_alphanumeric() {
            output.push(character.to_ascii_lowercase());
        } else if !output.ends_with('_') {
            output.push('_');
        }
    }
    output.trim_matches('_').to_owned()
}

pub(crate) fn go_type(value: &ValueType, contract: &ContractBundle) -> String {
    if let Some(inner) = value.optional_inner() {
        return format!("*{}", go_type(inner, contract));
    }
    if let Some((inner, _)) = value.list_parts() {
        return format!("[]{}", go_type(inner, contract));
    }
    match value.tag() {
        ValueTypeTag::Bool => "bool".into(),
        ValueTypeTag::I64 => "int64".into(),
        ValueTypeTag::U64 => "uint64".into(),
        ValueTypeTag::String | ValueTypeTag::Uuid => "string".into(),
        ValueTypeTag::Bytes => "[]byte".into(),
        ValueTypeTag::Date => "int32".into(),
        ValueTypeTag::Timestamp => "riffdb.Instant".into(),
        ValueTypeTag::Decimal => "riffdb.ExactDecimal".into(),
        ValueTypeTag::Money => "riffdb.ExactMoney".into(),
        ValueTypeTag::Enum => value
            .enum_type_id()
            .and_then(|id| contract.schema().enumeration(id))
            .map_or_else(|| "string".into(), |item| go_public(item.name())),
        ValueTypeTag::Record => match value.record_ref() {
            Some(RecordTypeRef::Entity(id)) => contract.schema().entity(*id).map_or_else(
                || "map[string]riffdb.Value".into(),
                |item| go_public(item.name()),
            ),
            _ => "map[string]riffdb.Value".into(),
        },
        ValueTypeTag::Vector => "[]float32".into(),
        ValueTypeTag::Optional | ValueTypeTag::List => unreachable!(),
    }
}

pub(crate) fn encode_expr(value: &str, ty: &ValueType, contract: &ContractBundle) -> String {
    if let Some(inner) = ty.optional_inner() {
        return format!(
            "riffdb.Optional({value}, func(item {}) riffdb.Value {{ return {} }})",
            go_type(inner, contract),
            encode_expr("item", inner, contract)
        );
    }
    if let Some((inner, _)) = ty.list_parts() {
        return format!(
            "riffdb.Values({value}, func(item {}) riffdb.Value {{ return {} }})",
            go_type(inner, contract),
            encode_expr("item", inner, contract)
        );
    }
    match ty.tag() {
        ValueTypeTag::Bool => format!("riffdb.Bool({value})"),
        ValueTypeTag::I64 => format!("riffdb.I64({value})"),
        ValueTypeTag::U64 => format!("riffdb.U64({value})"),
        ValueTypeTag::String => format!("riffdb.String({value})"),
        ValueTypeTag::Uuid => format!("riffdb.UUID({value})"),
        ValueTypeTag::Enum => format!("riffdb.Enum(string({value}))"),
        ValueTypeTag::Bytes => format!("riffdb.BytesFrom({value})"),
        ValueTypeTag::Date => format!("riffdb.Date({value})"),
        ValueTypeTag::Timestamp => format!("riffdb.TimestampFrom({value})"),
        ValueTypeTag::Decimal => format!("riffdb.DecimalFrom({value})"),
        ValueTypeTag::Money => format!("riffdb.MoneyFrom({value})"),
        ValueTypeTag::Record => match ty.record_ref() {
            Some(RecordTypeRef::Entity(id)) => format!(
                "encode{}({value})",
                contract
                    .schema()
                    .entity(*id)
                    .map_or("Record", |item| item.name())
            ),
            _ => format!("riffdb.Record({value})"),
        },
        ValueTypeTag::Vector => format!("riffdb.VectorFrom({value})"),
        ValueTypeTag::Optional | ValueTypeTag::List => unreachable!(),
    }
}

pub(crate) fn decode_expr(value: &str, ty: &ValueType, contract: &ContractBundle) -> String {
    if let Some(inner) = ty.optional_inner() {
        return format!(
            "riffdb.DecodeOptional({value}, func(item riffdb.Value) ({}, error) {{ return {} }})",
            go_type(inner, contract),
            decode_expr("item", inner, contract)
        );
    }
    if let Some((inner, _)) = ty.list_parts() {
        return format!(
            "riffdb.DecodeValues({value}, func(item riffdb.Value) ({}, error) {{ return {} }})",
            go_type(inner, contract),
            decode_expr("item", inner, contract)
        );
    }
    match ty.tag() {
        ValueTypeTag::Bool => format!("riffdb.BoolValue({value})"),
        ValueTypeTag::I64 => format!("riffdb.I64Value({value})"),
        ValueTypeTag::U64 => format!("riffdb.U64Value({value})"),
        ValueTypeTag::String => format!("riffdb.StringValue({value})"),
        ValueTypeTag::Uuid => format!("riffdb.UUIDValue({value})"),
        ValueTypeTag::Bytes => format!("riffdb.BytesValue({value})"),
        ValueTypeTag::Date => format!("riffdb.DateValue({value})"),
        ValueTypeTag::Timestamp => format!("riffdb.TimestampValue({value})"),
        ValueTypeTag::Decimal => ty.decimal_spec().map_or_else(
            || format!("riffdb.DecimalValue({value})"),
            |spec| {
                format!(
                    "riffdb.DecimalValueWithSchema({value}, {}, {})",
                    spec.precision(),
                    spec.scale()
                )
            },
        ),
        ValueTypeTag::Money => format!("riffdb.MoneyValue({value})"),
        ValueTypeTag::Enum => {
            let name = ty
                .enum_type_id()
                .and_then(|id| contract.schema().enumeration(id))
                .map_or("string".into(), |item| go_public(item.name()));
            format!(
                "func() ({name}, error) {{ raw, err := riffdb.EnumValue({value}); return {name}(raw), err }}()"
            )
        }
        ValueTypeTag::Record => match ty.record_ref() {
            Some(RecordTypeRef::Entity(id)) => format!(
                "decode{}({value})",
                contract
                    .schema()
                    .entity(*id)
                    .map_or("Record", |item| item.name())
            ),
            _ => format!("riffdb.RecordFields({value})"),
        },
        ValueTypeTag::Vector => format!(
            "func() ([]float32, error) {{ result, err := riffdb.VectorValue({value}); if err != nil || len(result) != {} {{ return nil, errors.New(\"invalid RiffDB vector value\") }}; return result, nil }}()",
            ty.vector_dimension().expect("vector dimension").get()
        ),
        ValueTypeTag::Optional | ValueTypeTag::List => unreachable!(),
    }
}

pub(crate) fn go_named_type(value: &NamedTypeSchema, contract: &ContractBundle) -> String {
    match value {
        NamedTypeSchema::Scalar(name) => match name.as_str() {
            "bool" => "bool".into(),
            "i64" => "int64".into(),
            "u64" => "uint64".into(),
            "timestamp" => "riffdb.Instant".into(),
            "date" => "int32".into(),
            name if name.starts_with("bytes<") => "[]byte".into(),
            name if name.starts_with("vector<") => "[]float32".into(),
            name if name.starts_with("decimal<") => "riffdb.ExactDecimal".into(),
            name if name.starts_with("money<") => "riffdb.ExactMoney".into(),
            name => contract
                .schema()
                .enums()
                .iter()
                .find(|item| item.name() == name)
                .map_or_else(|| "string".into(), |item| go_public(item.name())),
        },
        NamedTypeSchema::Optional(inner) => format!("*{}", go_named_type(inner, contract)),
        NamedTypeSchema::Set(inner)
        | NamedTypeSchema::BoundedSet { element: inner, .. }
        | NamedTypeSchema::List { element: inner, .. } => {
            format!("[]{}", go_named_type(inner, contract))
        }
        NamedTypeSchema::Record(fields) => format!(
            "struct {{ {} }}",
            fields
                .iter()
                .map(|field| format!(
                    "{} {}",
                    go_public(field.name()),
                    go_named_type(field.value_type(), contract)
                ))
                .collect::<Vec<_>>()
                .join("; ")
        ),
        NamedTypeSchema::Cursor => "string".into(),
        NamedTypeSchema::Limit => "uint32".into(),
        NamedTypeSchema::BoundedLimit { .. } => "uint32".into(),
    }
}

pub(crate) fn encode_named_expr(
    value: &str,
    ty: &NamedTypeSchema,
    contract: &ContractBundle,
) -> String {
    match ty {
        NamedTypeSchema::Optional(inner) => format!(
            "riffdb.Optional({value}, func(item {}) riffdb.Value {{ return {} }})",
            go_named_type(inner, contract),
            encode_named_expr("item", inner, contract)
        ),
        NamedTypeSchema::Set(inner)
        | NamedTypeSchema::BoundedSet { element: inner, .. }
        | NamedTypeSchema::List { element: inner, .. } => format!(
            "riffdb.Values({value}, func(item {}) riffdb.Value {{ return {} }})",
            go_named_type(inner, contract),
            encode_named_expr("item", inner, contract)
        ),
        NamedTypeSchema::Record(fields) => format!(
            "riffdb.Record(map[string]riffdb.Value{{{}}})",
            fields
                .iter()
                .map(|field| format!(
                    "\"{}\": {}",
                    field.name(),
                    encode_named_expr(
                        &format!("{value}.{}", go_public(field.name())),
                        field.value_type(),
                        contract
                    )
                ))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        NamedTypeSchema::Cursor => format!("riffdb.String({value})"),
        NamedTypeSchema::Limit => format!("riffdb.U64(uint64({value}))"),
        NamedTypeSchema::BoundedLimit { .. } => format!("riffdb.U64(uint64({value}))"),
        NamedTypeSchema::Scalar(name) => match name.as_str() {
            "bool" => format!("riffdb.Bool({value})"),
            "i64" => format!("riffdb.I64({value})"),
            "u64" => format!("riffdb.U64({value})"),
            "uuid" => format!("riffdb.UUID({value})"),
            "timestamp" => format!("riffdb.TimestampFrom({value})"),
            "date" => format!("riffdb.Date({value})"),
            name if name.starts_with("bytes<") => format!("riffdb.BytesFrom({value})"),
            name if name.starts_with("vector<") => format!("riffdb.VectorFrom({value})"),
            name if name.starts_with("decimal<") => format!("riffdb.DecimalFrom({value})"),
            name if name.starts_with("money<") => format!("riffdb.MoneyFrom({value})"),
            name if contract
                .schema()
                .enums()
                .iter()
                .any(|item| item.name() == name) =>
            {
                format!("riffdb.Enum(string({value}))")
            }
            _ => format!("riffdb.String({value})"),
        },
    }
}

pub(crate) fn decode_named_expr(
    value: &str,
    ty: &NamedTypeSchema,
    contract: &ContractBundle,
) -> String {
    match ty {
        NamedTypeSchema::Optional(inner) => format!(
            "riffdb.DecodeOptional({value}, func(item riffdb.Value) ({}, error) {{ return {} }})",
            go_named_type(inner, contract),
            decode_named_expr("item", inner, contract)
        ),
        NamedTypeSchema::Set(inner)
        | NamedTypeSchema::BoundedSet { element: inner, .. }
        | NamedTypeSchema::List { element: inner, .. } => format!(
            "riffdb.DecodeValues({value}, func(item riffdb.Value) ({}, error) {{ return {} }})",
            go_named_type(inner, contract),
            decode_named_expr("item", inner, contract)
        ),
        NamedTypeSchema::Record(fields) => {
            let ty_name = go_named_type(ty, contract);
            let mut body = format!(
                "func() ({ty_name}, error) {{ fields, err := riffdb.RecordFields({value}); if err != nil {{ return {ty_name}{{}}, err }}; var result {ty_name}; var raw riffdb.Value; "
            );
            for field in fields {
                write!(body, "raw, err = requiredField(fields, \"{}\"); if err != nil {{ return result, err }}; result.{}, err = {}; if err != nil {{ return result, err }}; ", field.name(), go_public(field.name()), decode_named_expr("raw", field.value_type(), contract)).infallible();
            }
            body.push_str("return result, nil }()");
            body
        }
        NamedTypeSchema::Cursor => format!("riffdb.StringValue({value})"),
        NamedTypeSchema::Limit => format!(
            "func() (uint32, error) {{ raw, err := riffdb.U64Value({value}); return uint32(raw), err }}()"
        ),
        NamedTypeSchema::BoundedLimit { .. } => format!(
            "func() (uint32, error) {{ raw, err := riffdb.U64Value({value}); return uint32(raw), err }}()"
        ),
        NamedTypeSchema::Scalar(name) => match name.as_str() {
            "bool" => format!("riffdb.BoolValue({value})"),
            "i64" => format!("riffdb.I64Value({value})"),
            "u64" => format!("riffdb.U64Value({value})"),
            "uuid" => format!("riffdb.UUIDValue({value})"),
            "timestamp" => format!("riffdb.TimestampValue({value})"),
            "date" => format!("riffdb.DateValue({value})"),
            name if name.starts_with("bytes<") => format!("riffdb.BytesValue({value})"),
            name if name.starts_with("vector<") => format!("riffdb.VectorValue({value})"),
            name if name.starts_with("decimal<") => decimal_type_parts(name).map_or_else(
                || format!("riffdb.DecimalValue({value})"),
                |(precision, scale)| {
                    format!("riffdb.DecimalValueWithSchema({value}, {precision}, {scale})")
                },
            ),
            name if name.starts_with("money<") => format!("riffdb.MoneyValue({value})"),
            name if contract
                .schema()
                .enums()
                .iter()
                .any(|item| item.name() == name) =>
            {
                let enumeration = go_public(name);
                format!(
                    "func() ({enumeration}, error) {{ raw, err := riffdb.EnumValue({value}); return {enumeration}(raw), err }}()"
                )
            }
            _ => format!("riffdb.StringValue({value})"),
        },
    }
}

fn decimal_type_parts(type_name: &str) -> Option<(u8, u8)> {
    let body = type_name.strip_prefix("decimal<")?.strip_suffix('>')?;
    let (precision, scale) = body.split_once(',')?;
    Some((precision.parse().ok()?, scale.parse().ok()?))
}

pub(crate) fn is_cursor(value_type: &NamedTypeSchema) -> bool {
    matches!(value_type, NamedTypeSchema::Cursor)
        || matches!(value_type, NamedTypeSchema::Optional(inner) if is_cursor(inner))
}

pub(crate) fn schema_hash(schema: &str) -> String {
    let value: Value = serde_json::from_str(schema).expect("schema");
    let bytes = serde_json::to_vec(&value).expect("schema");
    let digest: [u8; 32] = Sha256::digest(bytes).into();
    hex(&digest)
}
fn hex(bytes: &[u8; 32]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
pub(crate) fn go_public(value: &str) -> String {
    let mut output = String::new();
    for part in snake(value).split('_').filter(|part| !part.is_empty()) {
        let mut chars = part.chars();
        if let Some(first) = chars.next() {
            output.extend(first.to_uppercase());
            output.extend(chars);
        }
    }
    if output.is_empty() {
        "Value".into()
    } else {
        output
    }
}
fn snake(value: &str) -> String {
    let mut output = String::new();
    for (index, ch) in value.chars().enumerate() {
        if ch.is_ascii_uppercase() && index != 0 {
            output.push('_');
        }
        output.push(ch.to_ascii_lowercase());
    }
    output
}
