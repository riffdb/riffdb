//! Schema-directed presentation of declared command outcomes.
//!
//! The types in this module retain only the historical schema names needed to
//! present an already validated canonical outcome. They carry no catalog,
//! authority, storage handle, or independent semantic value.

use std::collections::BTreeSet;
use std::fmt;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use serde::{Serialize, Serializer};
use serde_json::{Map, Number, Value};

use crate::schema::RiffDbSchemaValidator;
use crate::{
    McpPresentationError, McpResourceLocator, McpToolResult, SchemaDocument, parse_resource_locator,
};

const MAX_SCHEMA_BOUND_DEPTH: usize = 32;
const MAX_SCHEMA_BOUND_NODES: usize = 262_144;
const MAX_SOURCE_NAME_BYTES: usize = 256;
const UUID_PATTERN: &str = "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$";

/// One historical-schema-bound record field.
#[derive(Clone, Eq, PartialEq)]
pub struct McpSchemaBoundField {
    field_id: u32,
    field_name: String,
    value: McpSchemaBoundValue,
}

impl McpSchemaBoundField {
    /// Checks one nonzero stable ID and its exact source field name.
    pub fn new(
        field_id: u32,
        field_name: impl Into<String>,
        value: McpSchemaBoundValue,
    ) -> Result<Self, McpPresentationError> {
        let field_name = field_name.into();
        if field_id == 0 || !is_source_name(&field_name) {
            return Err(McpPresentationError);
        }
        Ok(Self {
            field_id,
            field_name,
            value,
        })
    }

    /// Returns the stable field ID retained as validation evidence.
    #[must_use]
    pub const fn field_id(&self) -> u32 {
        self.field_id
    }

    /// Borrows the exact historical source field name.
    #[must_use]
    pub fn field_name(&self) -> &str {
        &self.field_name
    }

    /// Borrows the recursively bound field value.
    #[must_use]
    pub const fn value(&self) -> &McpSchemaBoundValue {
        &self.value
    }
}

impl fmt::Debug for McpSchemaBoundField {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("McpSchemaBoundField([REDACTED])")
    }
}

/// Transport-neutral value joined to exact historical schema names.
#[derive(Clone, Eq, PartialEq)]
pub enum McpSchemaBoundValue {
    /// Explicit absence.
    Null,
    /// Boolean.
    Bool(bool),
    /// Signed 64-bit integer.
    I64(i64),
    /// Unsigned 64-bit integer.
    U64(u64),
    /// Fixed-scale decimal.
    Decimal {
        /// Signed coefficient.
        coefficient: i128,
        /// Exact declared precision.
        precision: u8,
        /// Exact declared scale.
        scale: u8,
    },
    /// Currency-qualified fixed-scale decimal.
    Money {
        /// Exact schema currency.
        currency: String,
        /// Signed coefficient.
        coefficient: i128,
        /// Exact declared precision.
        precision: u8,
        /// Exact declared scale.
        scale: u8,
    },
    /// Bounded UTF-8 string.
    String(String),
    /// Bounded opaque bytes.
    Bytes(Vec<u8>),
    /// UTC timestamp.
    Timestamp {
        /// Seconds from the Unix epoch.
        seconds: i64,
        /// Nanosecond fraction.
        nanos: u32,
    },
    /// Days from the Unix epoch.
    Date(i32),
    /// UUID network-order bytes.
    Uuid([u8; 16]),
    /// Enumeration value with redundant stable identity and exact source name.
    Enum {
        /// Stable enum type ID.
        type_id: u32,
        /// Stable enum variant ID.
        variant_id: u32,
        /// Exact historical variant source name.
        variant_name: String,
    },
    /// Ordered bounded list.
    List(Vec<Self>),
    /// Stable-ID-ordered historical-schema-bound record.
    Record(Vec<McpSchemaBoundField>),
}

impl fmt::Debug for McpSchemaBoundValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("McpSchemaBoundValue([REDACTED])")
    }
}

/// One declared outcome and its historical-schema-bound payload record.
#[derive(Clone, Eq, PartialEq)]
pub struct McpSchemaBoundOutcome {
    outcome_id: u32,
    outcome_name: String,
    payload: Vec<McpSchemaBoundField>,
}

impl fmt::Debug for McpSchemaBoundOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("McpSchemaBoundOutcome([REDACTED])")
    }
}

impl McpSchemaBoundOutcome {
    /// Checks the declared outcome identity and requires a record payload.
    pub fn new(
        outcome_id: u32,
        outcome_name: impl Into<String>,
        value: McpSchemaBoundValue,
    ) -> Result<Self, McpPresentationError> {
        let outcome_name = outcome_name.into();
        let McpSchemaBoundValue::Record(payload) = value else {
            return Err(McpPresentationError);
        };
        if outcome_id == 0 || !is_source_name(&outcome_name) {
            return Err(McpPresentationError);
        }
        Ok(Self {
            outcome_id,
            outcome_name,
            payload,
        })
    }

    /// Returns the stable outcome ID retained as validation evidence.
    #[must_use]
    pub const fn outcome_id(&self) -> u32 {
        self.outcome_id
    }

    /// Borrows the exact historical outcome source name.
    #[must_use]
    pub fn outcome_name(&self) -> &str {
        &self.outcome_name
    }
}

/// Opaque natural JSON outcome validated against one exact compiler schema.
#[derive(Clone, Eq, PartialEq)]
pub struct McpNaturalOutcome {
    outcome_name: String,
    schema_hash: [u8; 32],
    value: Value,
}

impl McpNaturalOutcome {
    /// Converts historical-schema-bound evidence to natural business JSON.
    ///
    /// The complete result is validated against the same compiler-owned
    /// declared-outcome union before this witness can be constructed.
    pub fn from_schema_bound(
        outcome: &McpSchemaBoundOutcome,
        schema: &SchemaDocument,
    ) -> Result<Self, McpPresentationError> {
        Self::from_parts(&outcome.outcome_name, &outcome.payload, schema)
    }

    /// Converts a public response carrying exact historical names.
    ///
    /// The public command response intentionally omits the internal outcome ID.
    /// The exact contract/version fence and compiler-owned outcome schema bind
    /// the supplied name and every recursively named field.
    pub fn from_public_response(
        outcome_name: impl Into<String>,
        value: McpSchemaBoundValue,
        schema: &SchemaDocument,
    ) -> Result<Self, McpPresentationError> {
        let outcome_name = outcome_name.into();
        let McpSchemaBoundValue::Record(payload) = value else {
            return Err(McpPresentationError);
        };
        if !is_source_name(&outcome_name) {
            return Err(McpPresentationError);
        }
        Self::from_parts(&outcome_name, &payload, schema)
    }

    fn from_parts(
        outcome_name: &str,
        payload: &[McpSchemaBoundField],
        schema: &SchemaDocument,
    ) -> Result<Self, McpPresentationError> {
        let value = render_outcome_value(outcome_name, payload, schema)?;
        RiffDbSchemaValidator
            .validate(schema, &value)
            .map_err(|_| McpPresentationError)?;
        Ok(Self {
            outcome_name: outcome_name.to_owned(),
            schema_hash: schema.schema_hash_bytes(),
            value,
        })
    }

    /// Borrows the exact historical outcome name.
    #[must_use]
    pub fn outcome_name(&self) -> &str {
        &self.outcome_name
    }
}

impl Serialize for McpNaturalOutcome {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.value.serialize(serializer)
    }
}

impl fmt::Debug for McpNaturalOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("McpNaturalOutcome([REDACTED])")
    }
}

/// Journaled command completion status.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum McpJournaledCommandStatus {
    /// The invocation committed a new result.
    Committed,
    /// The invocation resolved an existing idempotent result.
    Replayed,
}

impl McpJournaledCommandStatus {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Committed => "committed",
            Self::Replayed => "replayed",
        }
    }
}

/// Public durability name for one journaled command.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum McpCommandDurability {
    /// Synchronous durability.
    Synchronous,
    /// Group durability.
    Group,
}

impl McpCommandDurability {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Synchronous => "sync",
            Self::Group => "group",
        }
    }
}

/// Inputs for one committed or replayed dynamic-command result.
#[derive(Clone, Eq, PartialEq)]
pub struct McpJournaledCommandResultParts {
    /// Nonzero authoritative commit sequence.
    pub commit_sequence: u64,
    /// Nonzero immutable contract version.
    pub contract_version: u64,
    /// Exact command plan hash.
    pub plan_hash: [u8; 32],
    /// Schema-validated natural declared outcome.
    pub outcome: McpNaturalOutcome,
    /// Exact canonical provenance resource URI.
    pub provenance_uri: String,
    /// Exact accepted durability mode.
    pub durability: McpCommandDurability,
    /// Exact canonical persisted-outcome resource URI.
    pub outcome_uri: String,
}

impl fmt::Debug for McpJournaledCommandResultParts {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("McpJournaledCommandResultParts([REDACTED])")
    }
}

/// Inputs for one unjournaled read-only dynamic-command result.
#[derive(Clone, Eq, PartialEq)]
pub struct McpReadOnlyCommandResultParts {
    /// Nonzero immutable contract version.
    pub contract_version: u64,
    /// Exact command plan hash.
    pub plan_hash: [u8; 32],
    /// Schema-validated natural declared outcome.
    pub outcome: McpNaturalOutcome,
}

impl fmt::Debug for McpReadOnlyCommandResultParts {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("McpReadOnlyCommandResultParts([REDACTED])")
    }
}

/// Checked completion envelope for a dynamic command.
#[derive(Clone, Eq, PartialEq)]
pub enum McpDynamicCommandCompletion {
    /// A committed or replayed journaled result.
    Journaled {
        /// Exact completion status.
        status: McpJournaledCommandStatus,
        /// Complete checked result parts.
        parts: McpJournaledCommandResultParts,
    },
    /// An unjournaled read-only result.
    ReadOnly(McpReadOnlyCommandResultParts),
}

impl fmt::Debug for McpDynamicCommandCompletion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("McpDynamicCommandCompletion([REDACTED])")
    }
}

impl McpDynamicCommandCompletion {
    /// Checks a committed or replayed result and all canonical resource links.
    pub fn journaled(
        status: McpJournaledCommandStatus,
        parts: McpJournaledCommandResultParts,
    ) -> Result<Self, McpPresentationError> {
        if parts.commit_sequence == 0
            || parts.contract_version == 0
            || !matches!(
                parse_resource_locator(&parts.provenance_uri),
                Ok(McpResourceLocator::Provenance(_))
            )
            || !matches!(
                parse_resource_locator(&parts.outcome_uri),
                Ok(McpResourceLocator::Outcome { .. })
            )
        {
            return Err(McpPresentationError);
        }
        Ok(Self::Journaled { status, parts })
    }

    /// Checks an unjournaled read-only result.
    pub fn read_only(parts: McpReadOnlyCommandResultParts) -> Result<Self, McpPresentationError> {
        if parts.contract_version == 0 {
            return Err(McpPresentationError);
        }
        Ok(Self::ReadOnly(parts))
    }
}

/// Composes and validates the exact advertised dynamic-command result object.
///
/// The natural outcome witness must have been constructed from the exact same
/// outcome schema used to compose `result_schema`.
pub fn compose_dynamic_command_result(
    completion: &McpDynamicCommandCompletion,
    outcome_schema: &SchemaDocument,
    result_schema: &SchemaDocument,
) -> Result<McpToolResult, McpPresentationError> {
    let (outcome, value) = match completion {
        McpDynamicCommandCompletion::Journaled { status, parts } => {
            let mut object = Map::new();
            object.insert(
                "commit_sequence".to_owned(),
                Value::String(parts.commit_sequence.to_string()),
            );
            object.insert(
                "contract_version".to_owned(),
                Value::Number(Number::from(parts.contract_version)),
            );
            object.insert(
                "durability_mode".to_owned(),
                Value::String(parts.durability.as_str().to_owned()),
            );
            object.insert("outcome".to_owned(), parts.outcome.value.clone());
            object.insert(
                "outcome_uri".to_owned(),
                Value::String(parts.outcome_uri.clone()),
            );
            object.insert(
                "plan_hash".to_owned(),
                Value::String(lower_hex(&parts.plan_hash)),
            );
            object.insert(
                "provenance_uri".to_owned(),
                Value::String(parts.provenance_uri.clone()),
            );
            object.insert(
                "status".to_owned(),
                Value::String(status.as_str().to_owned()),
            );
            (&parts.outcome, Value::Object(object))
        }
        McpDynamicCommandCompletion::ReadOnly(parts) => {
            let mut object = Map::new();
            object.insert("commit_sequence".to_owned(), Value::Null);
            object.insert(
                "contract_version".to_owned(),
                Value::Number(Number::from(parts.contract_version)),
            );
            object.insert("durability_mode".to_owned(), Value::Null);
            object.insert("outcome".to_owned(), parts.outcome.value.clone());
            object.insert("outcome_uri".to_owned(), Value::Null);
            object.insert(
                "plan_hash".to_owned(),
                Value::String(lower_hex(&parts.plan_hash)),
            );
            object.insert("provenance_uri".to_owned(), Value::Null);
            object.insert(
                "status".to_owned(),
                Value::String("executed_read_only".to_owned()),
            );
            (&parts.outcome, Value::Object(object))
        }
    };
    if outcome.schema_hash != outcome_schema.schema_hash_bytes() {
        return Err(McpPresentationError);
    }
    RiffDbSchemaValidator
        .validate(result_schema, &value)
        .map_err(|_| McpPresentationError)?;
    McpToolResult::from_serializable(&value).map_err(|_| McpPresentationError)
}

fn render_outcome_value(
    outcome_name: &str,
    payload: &[McpSchemaBoundField],
    schema: &SchemaDocument,
) -> Result<Value, McpPresentationError> {
    let root = schema.json_object();
    let branches = root
        .get("oneOf")
        .and_then(Value::as_array)
        .ok_or(McpPresentationError)?;
    let mut matches = branches.iter().filter(|branch| {
        branch
            .as_object()
            .and_then(|object| object.get("properties"))
            .and_then(Value::as_object)
            .and_then(|properties| properties.get("type"))
            .and_then(Value::as_object)
            .and_then(|schema| schema.get("const"))
            .and_then(Value::as_str)
            == Some(outcome_name)
    });
    let branch = matches.next().ok_or(McpPresentationError)?;
    if matches.next().is_some() {
        return Err(McpPresentationError);
    }
    let branch = branch.as_object().ok_or(McpPresentationError)?;
    let properties = branch
        .get("properties")
        .and_then(Value::as_object)
        .ok_or(McpPresentationError)?;
    if branch.get("type").and_then(Value::as_str) != Some("object")
        || properties
            .get("type")
            .and_then(Value::as_object)
            .and_then(|schema| schema.get("const"))
            .and_then(Value::as_str)
            != Some(outcome_name)
    {
        return Err(McpPresentationError);
    }

    let mut state = RenderState { nodes: 0 };
    let mut object = Map::new();
    object.insert("type".to_owned(), Value::String(outcome_name.to_owned()));
    render_record_fields(payload, properties, &mut object, 0, &mut state)?;
    Ok(Value::Object(object))
}

struct RenderState {
    nodes: usize,
}

impl RenderState {
    fn visit(&mut self, depth: usize) -> Result<(), McpPresentationError> {
        if depth > MAX_SCHEMA_BOUND_DEPTH {
            return Err(McpPresentationError);
        }
        self.nodes = self.nodes.checked_add(1).ok_or(McpPresentationError)?;
        if self.nodes > MAX_SCHEMA_BOUND_NODES {
            return Err(McpPresentationError);
        }
        Ok(())
    }
}

fn render_schema_bound_value(
    schema: &Value,
    value: &McpSchemaBoundValue,
    depth: usize,
    state: &mut RenderState,
) -> Result<Value, McpPresentationError> {
    state.visit(depth)?;
    let schema = schema.as_object().ok_or(McpPresentationError)?;
    if let Some(branches) = schema.get("oneOf").and_then(Value::as_array) {
        let mut null_branch = None;
        let mut value_branch = None;
        for branch in branches {
            if branch
                .as_object()
                .and_then(|branch| branch.get("type"))
                .and_then(Value::as_str)
                == Some("null")
            {
                if null_branch.replace(branch).is_some() {
                    return Err(McpPresentationError);
                }
            } else if value_branch.replace(branch).is_some() {
                return Err(McpPresentationError);
            }
        }
        return match value {
            McpSchemaBoundValue::Null => render_schema_bound_value(
                null_branch.ok_or(McpPresentationError)?,
                value,
                depth + 1,
                state,
            ),
            _ => render_schema_bound_value(
                value_branch.ok_or(McpPresentationError)?,
                value,
                depth + 1,
                state,
            ),
        };
    }

    match value {
        McpSchemaBoundValue::Null if schema_type(schema) == Some("null") => Ok(Value::Null),
        McpSchemaBoundValue::Bool(value) if schema_type(schema) == Some("boolean") => {
            Ok(Value::Bool(*value))
        }
        McpSchemaBoundValue::I64(value) if is_i64_schema(schema) => {
            Ok(Value::Number(Number::from(*value)))
        }
        McpSchemaBoundValue::U64(value) if is_u64_schema(schema) => {
            Ok(Value::Number(Number::from(*value)))
        }
        McpSchemaBoundValue::Date(value) if is_date_schema(schema) => {
            Ok(Value::Number(Number::from(*value)))
        }
        McpSchemaBoundValue::Decimal {
            coefficient,
            precision,
            scale,
        } if decimal_schema_matches(schema, *precision, *scale, None) => {
            Ok(Value::String(format_decimal(*coefficient, *scale)?))
        }
        McpSchemaBoundValue::Money {
            currency,
            coefficient,
            precision,
            scale,
        } if decimal_schema_matches(schema, *precision, *scale, Some(currency)) => {
            Ok(Value::String(format_decimal(*coefficient, *scale)?))
        }
        McpSchemaBoundValue::String(value) if is_plain_string_schema(schema) => {
            Ok(Value::String(value.clone()))
        }
        McpSchemaBoundValue::Bytes(value) if is_bytes_schema(schema) => {
            Ok(Value::String(STANDARD.encode(value)))
        }
        McpSchemaBoundValue::Timestamp { seconds, nanos } if is_timestamp_schema(schema) => {
            let mut object = Map::new();
            object.insert(
                "nanos".to_owned(),
                Value::Number(Number::from(u64::from(*nanos))),
            );
            object.insert("seconds".to_owned(), Value::String(seconds.to_string()));
            Ok(Value::Object(object))
        }
        McpSchemaBoundValue::Uuid(bytes) if is_uuid_schema(schema) => {
            Ok(Value::String(format_uuid(*bytes)))
        }
        McpSchemaBoundValue::Enum {
            type_id,
            variant_id,
            variant_name,
        } if *type_id != 0
            && *variant_id != 0
            && is_source_name(variant_name)
            && schema
                .get("enum")
                .and_then(Value::as_array)
                .is_some_and(|variants| {
                    variants
                        .iter()
                        .any(|variant| variant.as_str() == Some(variant_name))
                }) =>
        {
            Ok(Value::String(variant_name.clone()))
        }
        McpSchemaBoundValue::List(values) if schema_type(schema) == Some("array") => {
            let item_schema = schema.get("items").ok_or(McpPresentationError)?;
            if !item_schema.is_object() {
                return Err(McpPresentationError);
            }
            values
                .iter()
                .map(|value| render_schema_bound_value(item_schema, value, depth + 1, state))
                .collect::<Result<Vec<_>, _>>()
                .map(Value::Array)
        }
        McpSchemaBoundValue::Record(fields) if schema_type(schema) == Some("object") => {
            let properties = schema
                .get("properties")
                .and_then(Value::as_object)
                .ok_or(McpPresentationError)?;
            let mut object = Map::new();
            render_record_fields(fields, properties, &mut object, depth + 1, state)?;
            Ok(Value::Object(object))
        }
        _ => Err(McpPresentationError),
    }
}

fn render_record_fields(
    fields: &[McpSchemaBoundField],
    properties: &Map<String, Value>,
    object: &mut Map<String, Value>,
    depth: usize,
    state: &mut RenderState,
) -> Result<(), McpPresentationError> {
    let mut previous_id = None;
    let mut names = BTreeSet::new();
    for field in fields {
        if previous_id.is_some_and(|previous| previous >= field.field_id)
            || !names.insert(field.field_name.as_str())
            || object.contains_key(&field.field_name)
        {
            return Err(McpPresentationError);
        }
        previous_id = Some(field.field_id);
        let field_schema = properties
            .get(&field.field_name)
            .ok_or(McpPresentationError)?;
        object.insert(
            field.field_name.clone(),
            render_schema_bound_value(field_schema, &field.value, depth + 1, state)?,
        );
    }
    Ok(())
}

fn schema_type(schema: &Map<String, Value>) -> Option<&str> {
    schema.get("type").and_then(Value::as_str)
}

fn exact_integer_bounds(schema: &Map<String, Value>, minimum: i128, maximum: u128) -> bool {
    schema_type(schema) == Some("integer")
        && schema.get("minimum").and_then(number_i128) == Some(minimum)
        && schema.get("maximum").and_then(number_u128) == Some(maximum)
}

fn is_i64_schema(schema: &Map<String, Value>) -> bool {
    exact_integer_bounds(schema, i128::from(i64::MIN), i64::MAX as u128)
}

fn is_u64_schema(schema: &Map<String, Value>) -> bool {
    exact_integer_bounds(schema, 0, u64::MAX as u128)
}

fn is_date_schema(schema: &Map<String, Value>) -> bool {
    exact_integer_bounds(schema, i128::from(i32::MIN), i32::MAX as u128)
}

fn decimal_schema_matches(
    schema: &Map<String, Value>,
    precision: u8,
    scale: u8,
    currency: Option<&str>,
) -> bool {
    schema_type(schema) == Some("string")
        && schema
            .get("x-riffdb-decimalPrecision")
            .and_then(Value::as_u64)
            == Some(u64::from(precision))
        && schema.get("x-riffdb-decimalScale").and_then(Value::as_u64) == Some(u64::from(scale))
        && schema.get("x-riffdb-moneyCurrency").and_then(Value::as_str) == currency
}

fn is_plain_string_schema(schema: &Map<String, Value>) -> bool {
    schema_type(schema) == Some("string")
        && !schema.contains_key("enum")
        && !schema.contains_key("contentEncoding")
        && !schema.contains_key("x-riffdb-decimalPrecision")
        && !schema.contains_key("x-riffdb-integerType")
        && schema.get("pattern").and_then(Value::as_str) != Some(UUID_PATTERN)
}

fn is_bytes_schema(schema: &Map<String, Value>) -> bool {
    schema_type(schema) == Some("string")
        && schema.get("contentEncoding").and_then(Value::as_str) == Some("base64")
}

fn is_uuid_schema(schema: &Map<String, Value>) -> bool {
    schema_type(schema) == Some("string")
        && schema.get("pattern").and_then(Value::as_str) == Some(UUID_PATTERN)
}

fn is_timestamp_schema(schema: &Map<String, Value>) -> bool {
    if schema_type(schema) != Some("object")
        || schema.get("additionalProperties") != Some(&Value::Bool(false))
    {
        return false;
    }
    let Some(properties) = schema.get("properties").and_then(Value::as_object) else {
        return false;
    };
    properties.len() == 2
        && properties
            .get("seconds")
            .and_then(Value::as_object)
            .and_then(|seconds| seconds.get("x-riffdb-integerType"))
            .and_then(Value::as_str)
            == Some("i64")
        && properties
            .get("nanos")
            .and_then(Value::as_object)
            .is_some_and(|nanos| {
                nanos.get("minimum").and_then(Value::as_u64) == Some(0)
                    && nanos.get("maximum").and_then(Value::as_u64) == Some(999_999_999)
            })
}

fn format_decimal(coefficient: i128, scale: u8) -> Result<String, McpPresentationError> {
    if scale == 0 {
        return Ok(coefficient.to_string());
    }
    let negative = coefficient.is_negative();
    let magnitude = coefficient.unsigned_abs().to_string();
    let scale = usize::from(scale);
    let mut output = String::new();
    if negative {
        output.push('-');
    }
    if magnitude.len() <= scale {
        output.push('0');
        output.push('.');
        for _ in magnitude.len()..scale {
            output.push('0');
        }
        output.push_str(&magnitude);
    } else {
        let split = magnitude.len() - scale;
        output.push_str(&magnitude[..split]);
        output.push('.');
        output.push_str(&magnitude[split..]);
    }
    if negative && coefficient == 0 {
        return Err(McpPresentationError);
    }
    Ok(output)
}

fn number_i128(value: &Value) -> Option<i128> {
    let number = value.as_number()?;
    number
        .as_i64()
        .map(i128::from)
        .or_else(|| number.as_u64().map(i128::from))
}

fn number_u128(value: &Value) -> Option<u128> {
    let number = value.as_number()?;
    number
        .as_u64()
        .map(u128::from)
        .or_else(|| number.as_i64().and_then(|value| u128::try_from(value).ok()))
}

fn is_source_name(value: &str) -> bool {
    let mut bytes = value.bytes();
    bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
        && value.len() <= MAX_SOURCE_NAME_BYTES
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn lower_hex(bytes: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn format_uuid(bytes: [u8; 16]) -> String {
    let hex = lower_hex_16(bytes);
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

fn lower_hex_16(bytes: [u8; 16]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(32);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

#[cfg(test)]
mod tests {
    use riffdb_types::hash_schema;
    use serde_json::{Value, json};

    use super::*;
    use crate::fixed_tool_registry;
    use crate::registry::SchemaDocument;
    use crate::schema::compose_command_result_schema;

    const OUTCOME_SCHEMA: &str = concat!(
        "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\",",
        "\"oneOf\":[{\"additionalProperties\":false,\"properties\":{",
        "\"account\":{\"additionalProperties\":false,\"properties\":{",
        "\"balance\":{\"pattern\":\"^-?(0|[1-9][0-9]{0,3})\\\\.[0-9]{2}$\",",
        "\"type\":\"string\",\"x-riffdb-decimalPrecision\":6,",
        "\"x-riffdb-decimalScale\":2},",
        "\"state\":{\"enum\":[\"Open\",\"Closed\"],\"type\":\"string\"}},",
        "\"required\":[\"balance\",\"state\"],\"type\":\"object\"},",
        "\"type\":{\"const\":\"Accepted\"}},",
        "\"required\":[\"type\",\"account\"],\"type\":\"object\"}]}"
    );

    fn outcome_schema() -> SchemaDocument {
        SchemaDocument::from_public_parts(
            "riffdb.generated-schema/command-outcome-union/7/v1",
            hash_schema(OUTCOME_SCHEMA.as_bytes()).as_bytes(),
            OUTCOME_SCHEMA,
        )
        .expect("checked outcome schema")
    }

    fn bound_outcome() -> McpSchemaBoundOutcome {
        McpSchemaBoundOutcome::new(
            3,
            "Accepted",
            McpSchemaBoundValue::Record(vec![
                McpSchemaBoundField::new(
                    1,
                    "account",
                    McpSchemaBoundValue::Record(vec![
                        McpSchemaBoundField::new(
                            1,
                            "balance",
                            McpSchemaBoundValue::Decimal {
                                coefficient: 12_345,
                                precision: 6,
                                scale: 2,
                            },
                        )
                        .expect("field"),
                        McpSchemaBoundField::new(
                            2,
                            "state",
                            McpSchemaBoundValue::Enum {
                                type_id: 4,
                                variant_id: 1,
                                variant_name: "Open".to_owned(),
                            },
                        )
                        .expect("field"),
                    ]),
                )
                .expect("field"),
            ]),
        )
        .expect("bound outcome")
    }

    #[test]
    fn schema_bound_outcome_uses_exact_natural_names_and_validates() {
        let schema = outcome_schema();
        let natural =
            McpNaturalOutcome::from_schema_bound(&bound_outcome(), &schema).expect("natural");
        assert_eq!(
            serde_json::to_value(&natural).expect("serialize natural outcome"),
            json!({
                "account": {"balance": "123.45", "state": "Open"},
                "type": "Accepted"
            })
        );
        assert_eq!(natural.outcome_name(), "Accepted");
        assert_eq!(format!("{natural:?}"), "McpNaturalOutcome([REDACTED])");

        let public = McpNaturalOutcome::from_public_response(
            "Accepted",
            McpSchemaBoundValue::Record(bound_outcome().payload),
            &schema,
        )
        .expect("public natural outcome");
        assert_eq!(
            serde_json::to_value(public).expect("serialize public outcome"),
            serde_json::to_value(natural).expect("serialize service outcome")
        );
    }

    #[test]
    fn schema_bound_outcome_rejects_guessed_or_mismatched_names_and_types() {
        let schema = outcome_schema();
        let wrong_name =
            McpSchemaBoundOutcome::new(3, "Other", McpSchemaBoundValue::Record(Vec::new()))
                .expect("structurally valid name");
        assert_eq!(
            McpNaturalOutcome::from_schema_bound(&wrong_name, &schema),
            Err(McpPresentationError)
        );

        let wrong_precision = McpSchemaBoundOutcome::new(
            3,
            "Accepted",
            McpSchemaBoundValue::Record(vec![
                McpSchemaBoundField::new(
                    1,
                    "account",
                    McpSchemaBoundValue::Record(vec![
                        McpSchemaBoundField::new(
                            1,
                            "balance",
                            McpSchemaBoundValue::Decimal {
                                coefficient: 12_345,
                                precision: 5,
                                scale: 2,
                            },
                        )
                        .expect("field"),
                        McpSchemaBoundField::new(
                            2,
                            "state",
                            McpSchemaBoundValue::Enum {
                                type_id: 4,
                                variant_id: 1,
                                variant_name: "Open".to_owned(),
                            },
                        )
                        .expect("field"),
                    ]),
                )
                .expect("field"),
            ]),
        )
        .expect("bound outcome");
        assert_eq!(
            McpNaturalOutcome::from_schema_bound(&wrong_precision, &schema),
            Err(McpPresentationError)
        );
    }

    #[test]
    fn dynamic_result_composition_freezes_all_three_operation_branches() {
        let outcome_schema = outcome_schema();
        let envelope = fixed_tool_registry()
            .expect("registry")
            .operation_schemas()
            .first()
            .expect("operation schema");
        let result_schema =
            compose_command_result_schema(&outcome_schema, envelope).expect("composition");
        let natural = McpNaturalOutcome::from_schema_bound(&bound_outcome(), &outcome_schema)
            .expect("natural outcome");
        let read_only = McpDynamicCommandCompletion::read_only(McpReadOnlyCommandResultParts {
            contract_version: 8,
            plan_hash: [0x33; 32],
            outcome: natural,
        })
        .expect("read-only completion");
        assert!(
            compose_dynamic_command_result(&read_only, &outcome_schema, &result_schema).is_ok()
        );

        let wrong_schema_source = OUTCOME_SCHEMA.replace("\"Accepted\"", "\"Different\"");
        let wrong_schema = SchemaDocument::from_public_parts(
            "riffdb.generated-schema/command-outcome-union/7/v1",
            hash_schema(wrong_schema_source.as_bytes()).as_bytes(),
            wrong_schema_source,
        )
        .expect("other checked schema");
        assert_eq!(
            compose_dynamic_command_result(&read_only, &wrong_schema, &result_schema),
            Err(McpPresentationError)
        );
    }

    #[test]
    fn decimal_formatter_is_canonical_at_scale_boundaries() {
        assert_eq!(format_decimal(0, 2), Ok("0.00".to_owned()));
        assert_eq!(format_decimal(7, 2), Ok("0.07".to_owned()));
        assert_eq!(format_decimal(-7, 2), Ok("-0.07".to_owned()));
        assert_eq!(format_decimal(700, 2), Ok("7.00".to_owned()));
        assert_eq!(format_decimal(i128::MIN, 0), Ok(i128::MIN.to_string()));
    }

    #[test]
    fn natural_outcome_serializes_as_an_object_not_as_evidence_metadata() {
        let natural = McpNaturalOutcome::from_schema_bound(&bound_outcome(), &outcome_schema())
            .expect("natural outcome");
        let serialized = serde_json::to_value(natural).expect("serialize");
        assert!(matches!(serialized, Value::Object(_)));
        assert!(serialized.get("schema_hash").is_none());
        assert!(serialized.get("outcome_id").is_none());
    }
}
