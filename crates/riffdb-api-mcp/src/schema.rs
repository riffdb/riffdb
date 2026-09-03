use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use riffdb_types::{DatabaseAlias, hash_schema};
use serde_json::{Map, Value};

use crate::{McpResourceLocator, SchemaDocument, parse_resource_locator};

const DIALECT: &str = "https://json-schema.org/draft/2020-12/schema";
const COMMAND_ENVELOPE_ID: &str = "riffdb.command-operation-envelope/v1";
const MAX_SCHEMA_DEPTH: usize = 64;
const MAX_INSTANCE_DEPTH: usize = 32;
const MAX_VALIDATION_NODES: usize = 262_144;
const MAX_INPUT_VIOLATION_PATH_BYTES: usize = 512;
const JSON_VECTOR_DIMENSION: &str = "x-riffdb-vectorDimension";
const JSON_AGGREGATE_CANONICAL_ELEMENT_BYTES: &str = "x-riffdb-aggregateCanonicalElementBytes";
const MAX_AGGREGATE_CANONICAL_ELEMENT_BYTES: u64 = 16_777_216;

/// The fail-closed validator for the exact JSON Schema subset emitted by RiffDB.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct RiffDbSchemaValidator;

impl RiffDbSchemaValidator {
    /// Validates an instance against a checked schema document.
    pub(crate) fn validate(
        self,
        schema: &SchemaDocument,
        instance: &Value,
    ) -> Result<(), SchemaValidationError> {
        let root = Value::Object(schema.json_object());
        let mut state = ValidationState {
            root: &root,
            visited: 0,
        };
        state.validate_instance(&root, instance, 0)
    }

    /// Returns one deterministic, redacted first violation for invalid tool input.
    pub(crate) fn input_violation(
        self,
        schema: &SchemaDocument,
        instance: &Value,
    ) -> InputSchemaViolation {
        let root = Value::Object(schema.json_object());
        diagnose_input_violation(&root, &root, instance, "", 0).unwrap_or_else(|| {
            InputSchemaViolation::new(
                InputViolationCode::ConstraintFailed,
                "",
                "declared schema constraint",
            )
        })
    }
}

impl crate::presentation::StructuredContentValidator for RiffDbSchemaValidator {
    fn validate(
        &self,
        schema: &SchemaDocument,
        instance: &Value,
    ) -> Result<(), crate::McpPresentationError> {
        (*self)
            .validate(schema, instance)
            .map_err(|_| crate::McpPresentationError)
    }
}

/// Mechanically composes one compiler-owned outcome union into the accepted
/// service-owned command-operation envelope.
pub(crate) fn compose_command_result_schema(
    outcome_schema: &SchemaDocument,
    operation_envelope: &SchemaDocument,
) -> Result<SchemaDocument, SchemaCompositionError> {
    if operation_envelope.schema_id() != COMMAND_ENVELOPE_ID {
        return Err(SchemaCompositionError);
    }
    let mut envelope = operation_envelope.json_object();
    let definitions = envelope
        .get_mut("$defs")
        .and_then(Value::as_object_mut)
        .ok_or(SchemaCompositionError)?;
    if definitions.len() != 1 || definitions.get("outcome") != Some(&Value::Bool(false)) {
        return Err(SchemaCompositionError);
    }

    let mut outcome = Value::Object(outcome_schema.json_object());
    let outcome_object = outcome.as_object_mut().ok_or(SchemaCompositionError)?;
    if outcome_object
        .remove("$schema")
        .as_ref()
        .and_then(Value::as_str)
        != Some(DIALECT)
        || outcome_object.contains_key("$defs")
    {
        return Err(SchemaCompositionError);
    }
    definitions.insert("outcome".to_owned(), outcome);

    let value = Value::Object(envelope);
    validate_schema_source(&value).map_err(|_| SchemaCompositionError)?;
    let canonical_json = serde_json::to_string(&value).map_err(|_| SchemaCompositionError)?;
    let schema_hash = hash_schema(canonical_json.as_bytes());
    SchemaDocument::from_canonical(
        "riffdb.command-operation-envelope/v1+compiler-outcome",
        schema_hash,
        canonical_json,
    )
    .map_err(|_| SchemaCompositionError)
}

/// A schema instance did not satisfy RiffDB's bounded accepted subset.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SchemaValidationError;

impl fmt::Display for SchemaValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("schema validation failed")
    }
}

impl Error for SchemaValidationError {}

/// Stable public code for one redacted tool-input violation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InputViolationCode {
    RequiredPropertyMissing,
    UnexpectedProperty,
    WrongType,
    ConstraintFailed,
    OneOfNoMatch,
}

impl InputViolationCode {
    const fn as_str(self) -> &'static str {
        match self {
            Self::RequiredPropertyMissing => "required_property_missing",
            Self::UnexpectedProperty => "unexpected_property",
            Self::WrongType => "wrong_type",
            Self::ConstraintFailed => "constraint_failed",
            Self::OneOfNoMatch => "one_of_no_match",
        }
    }
}

/// One bounded public-safe MCP input diagnostic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct InputSchemaViolation {
    code: InputViolationCode,
    path: String,
    expected: &'static str,
}

impl InputSchemaViolation {
    fn new(code: InputViolationCode, path: &str, expected: &'static str) -> Self {
        Self {
            code,
            path: bounded_pointer(path),
            expected,
        }
    }

    pub(crate) fn as_json(&self) -> Value {
        serde_json::json!({
            "schema": "riffdb.mcp.input-error/v1",
            "code": self.code.as_str(),
            "path": self.path,
            "expected": self.expected,
        })
    }
}

fn diagnose_input_violation(
    root: &Value,
    schema: &Value,
    instance: &Value,
    path: &str,
    depth: usize,
) -> Option<InputSchemaViolation> {
    let mut validation = ValidationState { root, visited: 0 };
    if validation
        .validate_instance(schema, instance, depth)
        .is_ok()
    {
        return None;
    }
    if depth > MAX_INSTANCE_DEPTH {
        return Some(InputSchemaViolation::new(
            InputViolationCode::ConstraintFailed,
            path,
            "bounded nesting depth",
        ));
    }
    if schema == &Value::Bool(false) {
        return Some(InputSchemaViolation::new(
            InputViolationCode::ConstraintFailed,
            path,
            "accepted value",
        ));
    }
    let object = schema.as_object()?;
    if let Some(reference) = object.get("$ref").and_then(Value::as_str) {
        let target = resolve_input_reference(root, reference)?;
        return diagnose_input_violation(root, target, instance, path, depth + 1);
    }
    if object.contains_key("oneOf") || object.contains_key("anyOf") {
        return Some(InputSchemaViolation::new(
            InputViolationCode::OneOfNoMatch,
            path,
            "one declared alternative",
        ));
    }
    if let Some(expected) = object.get("type").and_then(Value::as_str)
        && !instance_has_type(instance, expected)
    {
        return Some(InputSchemaViolation::new(
            InputViolationCode::WrongType,
            path,
            public_type_expectation(expected),
        ));
    }

    if let Some(properties) = object.get("properties").and_then(Value::as_object)
        && let Some(instance_object) = instance.as_object()
    {
        if let Some(required) = object.get("required").and_then(Value::as_array) {
            for name in required.iter().filter_map(Value::as_str) {
                if !instance_object.contains_key(name) {
                    let child = push_pointer(path, name);
                    return Some(InputSchemaViolation::new(
                        InputViolationCode::RequiredPropertyMissing,
                        &child,
                        "required property",
                    ));
                }
            }
        }
        if object.get("additionalProperties") == Some(&Value::Bool(false)) {
            for name in instance_object.keys() {
                if !properties.contains_key(name) {
                    let child = push_pointer(path, name);
                    return Some(InputSchemaViolation::new(
                        InputViolationCode::UnexpectedProperty,
                        &child,
                        "declared property",
                    ));
                }
            }
        }
        for (name, child_schema) in properties {
            if let Some(child_instance) = instance_object.get(name) {
                let child_path = push_pointer(path, name);
                if let Some(violation) = diagnose_input_violation(
                    root,
                    child_schema,
                    child_instance,
                    &child_path,
                    depth + 1,
                ) {
                    return Some(violation);
                }
            }
        }
    }

    if let Some(items) = object.get("items")
        && let Some(values) = instance.as_array()
    {
        for (index, value) in values.iter().enumerate() {
            let child_path = push_pointer(path, &index.to_string());
            if let Some(violation) =
                diagnose_input_violation(root, items, value, &child_path, depth + 1)
            {
                return Some(violation);
            }
        }
    }

    Some(InputSchemaViolation::new(
        InputViolationCode::ConstraintFailed,
        path,
        "declared schema constraint",
    ))
}

fn resolve_input_reference<'a>(root: &'a Value, reference: &str) -> Option<&'a Value> {
    let name = reference.strip_prefix("#/$defs/")?;
    root.get("$defs")?.get(name)
}

fn instance_has_type(instance: &Value, expected: &str) -> bool {
    match expected {
        "null" => instance.is_null(),
        "boolean" => instance.is_boolean(),
        "integer" => instance.as_i64().is_some() || instance.as_u64().is_some(),
        "string" => instance.is_string(),
        "object" => instance.is_object(),
        "array" => instance.is_array(),
        _ => false,
    }
}

fn public_type_expectation(expected: &str) -> &'static str {
    match expected.as_bytes() {
        b"null" => "null",
        b"boolean" => "boolean",
        b"integer" => "integer",
        b"string" => "string",
        b"object" => "object",
        b"array" => "array",
        _ => "declared JSON type",
    }
}

fn push_pointer(path: &str, component: &str) -> String {
    let mut output = String::with_capacity(path.len().saturating_add(component.len() + 1));
    output.push_str(path);
    output.push('/');
    for character in component.chars() {
        match character {
            '~' => output.push_str("~0"),
            '/' => output.push_str("~1"),
            _ => output.push(character),
        }
    }
    output
}

fn bounded_pointer(path: &str) -> String {
    if path.len() <= MAX_INPUT_VIOLATION_PATH_BYTES {
        path.to_owned()
    } else {
        String::new()
    }
}

/// A command result schema could not be composed without changing identities.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SchemaCompositionError;

impl fmt::Display for SchemaCompositionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("command result schema composition failed")
    }
}

impl Error for SchemaCompositionError {}

pub(crate) fn validate_schema_source(root: &Value) -> Result<(), SchemaValidationError> {
    let object = root.as_object().ok_or(SchemaValidationError)?;
    if object.get("$schema").and_then(Value::as_str) != Some(DIALECT) {
        return Err(SchemaValidationError);
    }
    let definitions = object.get("$defs").and_then(Value::as_object);
    let mut visited = 0;
    validate_schema_node(root, definitions, 0, true, &mut visited)
}

fn validate_schema_node(
    schema: &Value,
    definitions: Option<&Map<String, Value>>,
    depth: usize,
    root: bool,
    visited: &mut usize,
) -> Result<(), SchemaValidationError> {
    *visited = visited.checked_add(1).ok_or(SchemaValidationError)?;
    if depth > MAX_SCHEMA_DEPTH || *visited > MAX_VALIDATION_NODES {
        return Err(SchemaValidationError);
    }
    if schema == &Value::Bool(false) {
        return Ok(());
    }
    let object = schema.as_object().ok_or(SchemaValidationError)?;
    for key in object.keys() {
        if !is_schema_keyword(key, root) {
            return Err(SchemaValidationError);
        }
    }
    if let Some(dialect) = object.get("$schema")
        && (!root || dialect.as_str() != Some(DIALECT))
    {
        return Err(SchemaValidationError);
    }
    if let Some(reference) = object.get("$ref") {
        let target = resolve_reference(
            reference.as_str().ok_or(SchemaValidationError)?,
            definitions,
        )?;
        if object.len() != 1 {
            return Err(SchemaValidationError);
        }
        let _ = target;
        return Ok(());
    }
    if let Some(local_definitions) = object.get("$defs") {
        if !root {
            return Err(SchemaValidationError);
        }
        for definition in local_definitions
            .as_object()
            .ok_or(SchemaValidationError)?
            .values()
        {
            validate_schema_node(definition, definitions, depth + 1, false, visited)?;
        }
    }
    if let Some(kind) = object.get("type")
        && !matches!(
            kind.as_str(),
            Some("null" | "boolean" | "integer" | "number" | "string" | "object" | "array")
        )
    {
        return Err(SchemaValidationError);
    }
    if let Some(properties) = object.get("properties") {
        for property in properties
            .as_object()
            .ok_or(SchemaValidationError)?
            .values()
        {
            validate_schema_node(property, definitions, depth + 1, false, visited)?;
        }
    }
    if object
        .get("additionalProperties")
        .is_some_and(|value| value != &Value::Bool(false))
    {
        return Err(SchemaValidationError);
    }
    if let Some(required) = object.get("required") {
        let properties = object
            .get("properties")
            .and_then(Value::as_object)
            .ok_or(SchemaValidationError)?;
        let mut seen = BTreeSet::new();
        for name in required.as_array().ok_or(SchemaValidationError)? {
            let name = name.as_str().ok_or(SchemaValidationError)?;
            if !properties.contains_key(name) || !seen.insert(name) {
                return Err(SchemaValidationError);
            }
        }
    }
    if let Some(branches) = object.get("oneOf") {
        let branches = branches.as_array().ok_or(SchemaValidationError)?;
        if branches.is_empty() {
            return Err(SchemaValidationError);
        }
        for branch in branches {
            validate_schema_node(branch, definitions, depth + 1, false, visited)?;
        }
    }
    if let Some(branches) = object.get("anyOf") {
        let branches = branches.as_array().ok_or(SchemaValidationError)?;
        if branches.is_empty() {
            return Err(SchemaValidationError);
        }
        for branch in branches {
            validate_schema_node(branch, definitions, depth + 1, false, visited)?;
        }
    }
    if let Some(negated) = object.get("not") {
        validate_schema_node(negated, definitions, depth + 1, false, visited)?;
    }
    if let Some(items) = object.get("items") {
        validate_schema_node(items, definitions, depth + 1, false, visited)?;
    }
    if let Some(prefix_items) = object.get("prefixItems") {
        for item in prefix_items.as_array().ok_or(SchemaValidationError)? {
            validate_schema_node(item, definitions, depth + 1, false, visited)?;
        }
    }
    validate_nonnegative_keyword(object, "minLength")?;
    validate_nonnegative_keyword(object, "maxLength")?;
    validate_nonnegative_keyword(object, "minItems")?;
    validate_nonnegative_keyword(object, "maxItems")?;
    if let (Some(minimum), Some(maximum)) = (
        object.get("minimum").and_then(number_i128),
        object.get("maximum").and_then(number_i128),
    ) && minimum > maximum
    {
        return Err(SchemaValidationError);
    }
    if object
        .get("enum")
        .is_some_and(|values| values.as_array().is_none_or(Vec::is_empty))
    {
        return Err(SchemaValidationError);
    }
    if let Some(pattern) = object.get("pattern")
        && !accepted_pattern(pattern.as_str().ok_or(SchemaValidationError)?)
    {
        return Err(SchemaValidationError);
    }
    if object
        .get("contentEncoding")
        .is_some_and(|encoding| encoding.as_str() != Some("base64"))
    {
        return Err(SchemaValidationError);
    }
    validate_extension_shapes(object)
}

fn is_schema_keyword(key: &str, root: bool) -> bool {
    matches!(
        key,
        "$ref"
            | "additionalProperties"
            | "anyOf"
            | "const"
            | "contentEncoding"
            | "default"
            | "description"
            | "enum"
            | "items"
            | "maxItems"
            | "maxLength"
            | "maximum"
            | "minItems"
            | "minLength"
            | "minimum"
            | "not"
            | "oneOf"
            | "pattern"
            | "prefixItems"
            | "properties"
            | "required"
            | "type"
            | "uniqueItems"
            | "x-riffdb-decimalPrecision"
            | "x-riffdb-decimalScale"
            | "x-riffdb-aggregateCanonicalElementBytes"
            | "x-riffdb-integerMaximum"
            | "x-riffdb-integerType"
            | "x-riffdb-maxDecodedBytes"
            | "x-riffdb-maxUtf8Bytes"
            | "x-riffdb-minUtf8Bytes"
            | "x-riffdb-moneyCurrency"
            | "x-riffdb-nonemptyUtf8"
            | "x-riffdb-relation"
            | "x-riffdb-strictlyIncreasing"
            | "x-riffdb-strictlyIncreasingBy"
            | "x-riffdb-uniqueBy"
            | "x-riffdb-vectorDimension"
    ) || (root && matches!(key, "$defs" | "$schema"))
}

fn validate_nonnegative_keyword(
    object: &Map<String, Value>,
    keyword: &str,
) -> Result<(), SchemaValidationError> {
    if object
        .get(keyword)
        .is_some_and(|value| value.as_u64().is_none())
    {
        return Err(SchemaValidationError);
    }
    Ok(())
}

fn validate_extension_shapes(object: &Map<String, Value>) -> Result<(), SchemaValidationError> {
    if let Some(value) = object.get(JSON_AGGREGATE_CANONICAL_ELEMENT_BYTES) {
        let Some(maximum) = value.as_u64() else {
            return Err(SchemaValidationError);
        };
        if !(1..=MAX_AGGREGATE_CANONICAL_ELEMENT_BYTES).contains(&maximum)
            || object.get("type").and_then(Value::as_str) != Some("array")
            || !object.contains_key("items")
        {
            return Err(SchemaValidationError);
        }
    }
    for key in [
        "x-riffdb-decimalPrecision",
        "x-riffdb-decimalScale",
        "x-riffdb-maxDecodedBytes",
        "x-riffdb-maxUtf8Bytes",
        "x-riffdb-minUtf8Bytes",
    ] {
        validate_nonnegative_keyword(object, key)?;
    }
    for key in ["x-riffdb-nonemptyUtf8", "x-riffdb-strictlyIncreasing"] {
        if object
            .get(key)
            .is_some_and(|value| value != &Value::Bool(true))
        {
            return Err(SchemaValidationError);
        }
    }
    if object
        .get("x-riffdb-integerType")
        .is_some_and(|value| !matches!(value.as_str(), Some("i64" | "u64")))
        || object.get("x-riffdb-integerMaximum").is_some_and(|value| {
            value
                .as_str()
                .is_none_or(|text| parse_canonical_u64(text).is_none())
        })
        || object
            .get("x-riffdb-moneyCurrency")
            .is_some_and(|value| value.as_str().is_none_or(is_currency))
        || object
            .get("x-riffdb-relation")
            .is_some_and(|value| value.as_str() != Some("start<=end"))
    {
        return Err(SchemaValidationError);
    }
    for key in ["x-riffdb-strictlyIncreasingBy", "x-riffdb-uniqueBy"] {
        if object
            .get(key)
            .is_some_and(|value| value.as_str().is_none_or(str::is_empty))
        {
            return Err(SchemaValidationError);
        }
    }
    if object
        .get("description")
        .is_some_and(|value| value.as_str().is_none())
    {
        return Err(SchemaValidationError);
    }
    let vector_dimension = object.get(JSON_VECTOR_DIMENSION).map(|value| {
        value
            .as_u64()
            .and_then(|value| u32::try_from(value).ok())
            .and_then(riffdb_types::VectorDimension::new)
            .ok_or(SchemaValidationError)
    });
    if let Some(vector_dimension) = vector_dimension.transpose()? {
        let dimension = u64::from(vector_dimension.get());
        let items = object
            .get("items")
            .and_then(Value::as_object)
            .ok_or(SchemaValidationError)?;
        if object.get("type").and_then(Value::as_str) != Some("array")
            || object.get("minItems").and_then(Value::as_u64) != Some(dimension)
            || object.get("maxItems").and_then(Value::as_u64) != Some(dimension)
            || items.len() != 1
            || items.get("type").and_then(Value::as_str) != Some("number")
        {
            return Err(SchemaValidationError);
        }
    }
    let precision = object
        .get("x-riffdb-decimalPrecision")
        .and_then(Value::as_u64);
    let scale = object.get("x-riffdb-decimalScale").and_then(Value::as_u64);
    if precision.is_some() != scale.is_some()
        || precision.is_some_and(|value| value == 0 || value > 38)
        || scale
            .zip(precision)
            .is_some_and(|(scale, precision)| scale > precision)
    {
        return Err(SchemaValidationError);
    }
    Ok(())
}

struct ValidationState<'a> {
    root: &'a Value,
    visited: usize,
}

impl ValidationState<'_> {
    fn validate_instance(
        &mut self,
        schema: &Value,
        instance: &Value,
        depth: usize,
    ) -> Result<(), SchemaValidationError> {
        self.visited = self.visited.checked_add(1).ok_or(SchemaValidationError)?;
        if depth > MAX_INSTANCE_DEPTH || self.visited > MAX_VALIDATION_NODES {
            return Err(SchemaValidationError);
        }
        if schema == &Value::Bool(false) {
            return Err(SchemaValidationError);
        }
        let object = schema.as_object().ok_or(SchemaValidationError)?;
        if let Some(reference) = object.get("$ref").and_then(Value::as_str) {
            let definitions = self.root.get("$defs").and_then(Value::as_object);
            let target = resolve_reference(reference, definitions)?;
            return self.validate_instance(target, instance, depth + 1);
        }
        if let Some(kind) = object.get("type").and_then(Value::as_str) {
            let matches = match kind {
                "null" => instance.is_null(),
                "boolean" => instance.is_boolean(),
                "integer" => instance.as_i64().is_some() || instance.as_u64().is_some(),
                "number" => instance.is_number(),
                "string" => instance.is_string(),
                "object" => instance.is_object(),
                "array" => instance.is_array(),
                _ => false,
            };
            if !matches {
                return Err(SchemaValidationError);
            }
        }
        if object
            .get("const")
            .is_some_and(|expected| expected != instance)
            || object
                .get("enum")
                .and_then(Value::as_array)
                .is_some_and(|values| !values.contains(instance))
        {
            return Err(SchemaValidationError);
        }
        if let Some(negated) = object.get("not")
            && self.validate_instance(negated, instance, depth + 1).is_ok()
        {
            return Err(SchemaValidationError);
        }
        if let Some(branches) = object.get("oneOf").and_then(Value::as_array) {
            let mut matches = 0_usize;
            for branch in branches {
                if self.validate_instance(branch, instance, depth + 1).is_ok() {
                    matches += 1;
                    if matches > 1 {
                        return Err(SchemaValidationError);
                    }
                }
            }
            if matches != 1 {
                return Err(SchemaValidationError);
            }
        }
        if let Some(branches) = object.get("anyOf").and_then(Value::as_array)
            && !branches
                .iter()
                .any(|branch| self.validate_instance(branch, instance, depth + 1).is_ok())
        {
            return Err(SchemaValidationError);
        }
        self.validate_number(object, instance)?;
        self.validate_string(object, instance)?;
        self.validate_object(object, instance, depth)?;
        self.validate_array(object, instance, depth)?;
        validate_semantic_extensions(object, instance)
    }

    fn validate_number(
        &self,
        schema: &Map<String, Value>,
        instance: &Value,
    ) -> Result<(), SchemaValidationError> {
        if let Some(number) = number_i128(instance)
            && (schema
                .get("minimum")
                .and_then(number_i128)
                .is_some_and(|minimum| number < minimum)
                || schema
                    .get("maximum")
                    .and_then(number_i128)
                    .is_some_and(|maximum| number > maximum))
        {
            return Err(SchemaValidationError);
        }
        Ok(())
    }

    fn validate_string(
        &self,
        schema: &Map<String, Value>,
        instance: &Value,
    ) -> Result<(), SchemaValidationError> {
        let Some(text) = instance.as_str() else {
            return Ok(());
        };
        let characters = text.chars().count() as u64;
        if schema
            .get("minLength")
            .and_then(Value::as_u64)
            .is_some_and(|minimum| characters < minimum)
            || schema
                .get("maxLength")
                .and_then(Value::as_u64)
                .is_some_and(|maximum| characters > maximum)
            || schema
                .get("pattern")
                .and_then(Value::as_str)
                .is_some_and(|pattern| !pattern_matches(pattern, text))
        {
            return Err(SchemaValidationError);
        }
        Ok(())
    }

    fn validate_object(
        &mut self,
        schema: &Map<String, Value>,
        instance: &Value,
        depth: usize,
    ) -> Result<(), SchemaValidationError> {
        let Some(instance) = instance.as_object() else {
            return Ok(());
        };
        let properties = schema.get("properties").and_then(Value::as_object);
        if let Some(required) = schema.get("required").and_then(Value::as_array)
            && required
                .iter()
                .filter_map(Value::as_str)
                .any(|name| !instance.contains_key(name))
        {
            return Err(SchemaValidationError);
        }
        if let Some(properties) = properties {
            if schema.get("additionalProperties") == Some(&Value::Bool(false))
                && instance.keys().any(|name| !properties.contains_key(name))
            {
                return Err(SchemaValidationError);
            }
            for (name, value) in instance {
                if let Some(property_schema) = properties.get(name) {
                    self.validate_instance(property_schema, value, depth + 1)?;
                }
            }
        }
        if schema_declares_tagged_decimal(schema) {
            validate_tagged_decimal(instance)?;
        }
        Ok(())
    }

    fn validate_array(
        &mut self,
        schema: &Map<String, Value>,
        instance: &Value,
        depth: usize,
    ) -> Result<(), SchemaValidationError> {
        let Some(items) = instance.as_array() else {
            return Ok(());
        };
        let length = items.len() as u64;
        if schema
            .get("minItems")
            .and_then(Value::as_u64)
            .is_some_and(|minimum| length < minimum)
            || schema
                .get("maxItems")
                .and_then(Value::as_u64)
                .is_some_and(|maximum| length > maximum)
        {
            return Err(SchemaValidationError);
        }
        let prefix = schema
            .get("prefixItems")
            .and_then(Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        for (index, value) in items.iter().enumerate() {
            if let Some(item_schema) = prefix.get(index).or_else(|| schema.get("items")) {
                self.validate_instance(item_schema, value, depth + 1)?;
            }
        }
        if schema.get("uniqueItems") == Some(&Value::Bool(true)) {
            let mut seen = BTreeSet::new();
            for value in items {
                if !seen.insert(serde_json::to_string(value).map_err(|_| SchemaValidationError)?) {
                    return Err(SchemaValidationError);
                }
            }
        }
        Ok(())
    }
}

fn validate_semantic_extensions(
    schema: &Map<String, Value>,
    instance: &Value,
) -> Result<(), SchemaValidationError> {
    if let Some(text) = instance.as_str() {
        let byte_length = text.len() as u64;
        if schema
            .get("x-riffdb-minUtf8Bytes")
            .and_then(Value::as_u64)
            .is_some_and(|minimum| byte_length < minimum)
            || schema
                .get("x-riffdb-maxUtf8Bytes")
                .and_then(Value::as_u64)
                .is_some_and(|maximum| byte_length > maximum)
            || schema.get("x-riffdb-nonemptyUtf8") == Some(&Value::Bool(true)) && text.is_empty()
        {
            return Err(SchemaValidationError);
        }
        if let Some(kind) = schema.get("x-riffdb-integerType").and_then(Value::as_str) {
            let valid = match kind {
                "i64" => parse_canonical_i64(text).is_some(),
                "u64" => parse_canonical_u64(text).is_some(),
                _ => false,
            };
            if !valid {
                return Err(SchemaValidationError);
            }
        }
        if schema
            .get("x-riffdb-integerMaximum")
            .and_then(Value::as_str)
            .is_some_and(|maximum| {
                parse_canonical_u64(text)
                    .zip(parse_canonical_u64(maximum))
                    .is_none_or(|(value, maximum)| value > maximum)
            })
        {
            return Err(SchemaValidationError);
        }
        if let Some(maximum) = schema
            .get("x-riffdb-maxDecodedBytes")
            .and_then(Value::as_u64)
        {
            let decoded = STANDARD.decode(text).map_err(|_| SchemaValidationError)?;
            if decoded.len() as u64 > maximum || STANDARD.encode(&decoded) != text {
                return Err(SchemaValidationError);
            }
        }
        if let (Some(precision), Some(scale)) = (
            schema
                .get("x-riffdb-decimalPrecision")
                .and_then(Value::as_u64),
            schema.get("x-riffdb-decimalScale").and_then(Value::as_u64),
        ) {
            validate_decimal_text(text, precision, scale)?;
        }
    }
    if let Some(items) = instance.as_array() {
        if let Some(dimension) = schema.get(JSON_VECTOR_DIMENSION).and_then(Value::as_u64) {
            if items.len() as u64 != dimension {
                return Err(SchemaValidationError);
            }
            let components = items
                .iter()
                .map(|item| item.as_f64().map(|value| value as f32))
                .collect::<Option<Vec<_>>>()
                .ok_or(SchemaValidationError)?;
            riffdb_types::CanonicalVector::new(components).map_err(|_| SchemaValidationError)?;
        }
        if schema.get("x-riffdb-strictlyIncreasing") == Some(&Value::Bool(true)) {
            validate_strictly_increasing(items, None)?;
        }
        if let Some(field) = schema
            .get("x-riffdb-strictlyIncreasingBy")
            .and_then(Value::as_str)
        {
            validate_strictly_increasing(items, Some(field))?;
        }
        if let Some(field) = schema.get("x-riffdb-uniqueBy").and_then(Value::as_str) {
            let mut seen = BTreeSet::new();
            for item in items {
                let value = item
                    .as_object()
                    .and_then(|object| object.get(field))
                    .ok_or(SchemaValidationError)?;
                if !seen.insert(serde_json::to_string(value).map_err(|_| SchemaValidationError)?) {
                    return Err(SchemaValidationError);
                }
            }
        }
    }
    if schema.get("x-riffdb-relation").and_then(Value::as_str) == Some("start<=end") {
        let object = instance.as_object().ok_or(SchemaValidationError)?;
        let start = object
            .get("start")
            .and_then(number_i128)
            .ok_or(SchemaValidationError)?;
        let end = object
            .get("end")
            .and_then(number_i128)
            .ok_or(SchemaValidationError)?;
        if start > end {
            return Err(SchemaValidationError);
        }
    }
    Ok(())
}

fn validate_tagged_decimal(object: &Map<String, Value>) -> Result<(), SchemaValidationError> {
    if !matches!(
        object.get("kind").and_then(Value::as_str),
        Some("decimal" | "money")
    ) {
        return Ok(());
    }
    let precision = object
        .get("precision")
        .and_then(Value::as_u64)
        .ok_or(SchemaValidationError)?;
    let scale = object
        .get("scale")
        .and_then(Value::as_u64)
        .ok_or(SchemaValidationError)?;
    let coefficient = object
        .get("coefficient")
        .and_then(Value::as_str)
        .ok_or(SchemaValidationError)?;
    if precision == 0
        || precision > 38
        || scale > precision
        || parse_canonical_i128(coefficient).is_none()
        || coefficient.trim_start_matches('-').len() as u64 > precision
    {
        return Err(SchemaValidationError);
    }
    Ok(())
}

fn schema_declares_tagged_decimal(schema: &Map<String, Value>) -> bool {
    if schema.get("type").and_then(Value::as_str) != Some("object")
        || schema.get("additionalProperties") != Some(&Value::Bool(false))
    {
        return false;
    }
    let Some(properties) = schema.get("properties").and_then(Value::as_object) else {
        return false;
    };
    let Some(kind) = properties
        .get("kind")
        .and_then(Value::as_object)
        .and_then(|kind| kind.get("const"))
        .and_then(Value::as_str)
    else {
        return false;
    };
    let expected_fields: &[&str] = match kind {
        "decimal" => &["coefficient", "kind", "precision", "scale"],
        "money" => &["coefficient", "currency", "kind", "precision", "scale"],
        _ => return false,
    };
    if properties.len() != expected_fields.len()
        || expected_fields
            .iter()
            .any(|field| !properties.contains_key(*field))
    {
        return false;
    }
    let Some(required) = schema.get("required").and_then(Value::as_array) else {
        return false;
    };
    required.len() == expected_fields.len()
        && expected_fields.iter().all(|field| {
            required
                .iter()
                .any(|required| required.as_str() == Some(field))
        })
}

fn validate_decimal_text(
    text: &str,
    precision: u64,
    scale: u64,
) -> Result<(), SchemaValidationError> {
    let unsigned = text.strip_prefix('-').unwrap_or(text);
    if scale == 0 {
        if unsigned.is_empty()
            || unsigned.len() as u64 > precision
            || unsigned.len() > 1 && unsigned.starts_with('0')
            || !unsigned.bytes().all(|byte| byte.is_ascii_digit())
            || text.starts_with('-') && unsigned.bytes().all(|byte| byte == b'0')
        {
            return Err(SchemaValidationError);
        }
        return Ok(());
    }
    let (integer, fraction) = unsigned.split_once('.').ok_or(SchemaValidationError)?;
    let integer_digits = if integer == "0" {
        0
    } else {
        integer.len() as u64
    };
    if integer.is_empty()
        || fraction.len() as u64 != scale
        || integer_digits + scale > precision
        || integer.len() > 1 && integer.starts_with('0')
        || !integer
            .bytes()
            .chain(fraction.bytes())
            .all(|byte| byte.is_ascii_digit())
        || text.starts_with('-')
            && integer.bytes().all(|byte| byte == b'0')
            && fraction.bytes().all(|byte| byte == b'0')
    {
        return Err(SchemaValidationError);
    }
    Ok(())
}

fn validate_strictly_increasing(
    values: &[Value],
    field: Option<&str>,
) -> Result<(), SchemaValidationError> {
    for pair in values.windows(2) {
        let left = select_order_value(&pair[0], field)?;
        let right = select_order_value(&pair[1], field)?;
        if compare_json_scalars(left, right) != Some(Ordering::Less) {
            return Err(SchemaValidationError);
        }
    }
    Ok(())
}

fn select_order_value<'a>(
    value: &'a Value,
    field: Option<&str>,
) -> Result<&'a Value, SchemaValidationError> {
    match field {
        Some(field) => value
            .as_object()
            .and_then(|object| object.get(field))
            .ok_or(SchemaValidationError),
        None => Ok(value),
    }
}

fn compare_json_scalars(left: &Value, right: &Value) -> Option<Ordering> {
    match (left, right) {
        (Value::String(left), Value::String(right)) => Some(left.cmp(right)),
        _ => number_i128(left)?.partial_cmp(&number_i128(right)?),
    }
}

fn resolve_reference<'a>(
    reference: &str,
    definitions: Option<&'a Map<String, Value>>,
) -> Result<&'a Value, SchemaValidationError> {
    let name = reference
        .strip_prefix("#/$defs/")
        .filter(|name| {
            !name.is_empty()
                && !name.contains('/')
                && name
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
        })
        .ok_or(SchemaValidationError)?;
    definitions
        .and_then(|definitions| definitions.get(name))
        .ok_or(SchemaValidationError)
}

fn accepted_pattern(pattern: &str) -> bool {
    matches!(
        pattern,
        "^(0|-?[1-9][0-9]*)$"
            | "^-?(0|[1-9][0-9]*)$"
            | "^(0|[1-9][0-9]*)$"
            | "^[1-9][0-9]*$"
            | "^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$"
            | "^[ -~]+$"
            | "^[!-~]+$"
            | "^[0-9a-f]{32}$"
            | "^[0-9a-f]{64}$"
            | "^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
            | "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
            | "^[A-Z]{3}$"
            | "^[A-Za-z_][A-Za-z0-9_]{0,255}$"
            | "^[a-z][a-z0-9_-]{0,63}$"
            | "^riffdb://outcome/(?:[A-Za-z0-9._~-]|%[0-9A-F]{2})+/(?:[A-Za-z0-9._~-]|%[0-9A-F]{2})+/[1-9][0-9]*/riffdb_cmd_[a-z][a-z0-9_]*_[a-z][a-z0-9_]*/[A-Za-z0-9_-]{50}$"
            | "^riffdb://provenance/[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
    ) || decimal_pattern_shape(pattern).is_some()
}

fn pattern_matches(pattern: &str, text: &str) -> bool {
    match pattern {
        "^(0|-?[1-9][0-9]*)$" => parse_canonical_i128(text).is_some(),
        "^-?(0|[1-9][0-9]*)$" => {
            let unsigned = text.strip_prefix('-').unwrap_or(text);
            parse_canonical_u64(unsigned).is_some()
        }
        "^(0|[1-9][0-9]*)$" => parse_canonical_u64(text).is_some(),
        "^[1-9][0-9]*$" => parse_canonical_u64(text).is_some_and(|value| value != 0),
        "^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$" => STANDARD
            .decode(text)
            .is_ok_and(|decoded| STANDARD.encode(decoded) == text),
        "^[ -~]+$" => !text.is_empty() && text.bytes().all(|byte| (b' '..=b'~').contains(&byte)),
        "^[!-~]+$" => !text.is_empty() && text.bytes().all(|byte| (b'!'..=b'~').contains(&byte)),
        "^[0-9a-f]{32}$" => is_lower_hex(text, 32),
        "^[0-9a-f]{64}$" => is_lower_hex(text, 64),
        "^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$" => {
            uuid_shape(text, true)
        }
        "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$" => uuid_shape(text, false),
        "^[A-Z]{3}$" => is_currency(text),
        "^[A-Za-z_][A-Za-z0-9_]{0,255}$" => source_name(text),
        "^[a-z][a-z0-9_-]{0,63}$" => DatabaseAlias::new(text).is_ok(),
        "^riffdb://outcome/(?:[A-Za-z0-9._~-]|%[0-9A-F]{2})+/(?:[A-Za-z0-9._~-]|%[0-9A-F]{2})+/[1-9][0-9]*/riffdb_cmd_[a-z][a-z0-9_]*_[a-z][a-z0-9_]*/[A-Za-z0-9_-]{50}$" =>
        {
            matches!(
                parse_resource_locator(text),
                Ok(McpResourceLocator::Outcome { .. })
            )
        }
        "^riffdb://provenance/[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$" =>
        {
            matches!(
                parse_resource_locator(text),
                Ok(McpResourceLocator::Provenance(_))
            )
        }
        _ => decimal_pattern_shape(pattern).is_some_and(|(precision, scale)| {
            validate_decimal_text(text, precision, scale).is_ok()
        }),
    }
}

fn decimal_pattern_shape(pattern: &str) -> Option<(u64, u64)> {
    if let Some(scale) = pattern
        .strip_prefix("^-?0\\.[0-9]{")
        .and_then(|suffix| suffix.strip_suffix("}$"))
        .and_then(|scale| scale.parse::<u64>().ok())
    {
        return Some((scale, scale));
    }
    let middle = pattern.strip_prefix("^-?(0|[1-9][0-9]{0,")?;
    if let Some(integer_digits) = middle
        .strip_suffix("})$")
        .and_then(|digits| digits.parse::<u64>().ok())
        .and_then(|digits| digits.checked_add(1))
    {
        return Some((integer_digits, 0));
    }
    let (integer_digits, scale_suffix) = middle.split_once("})\\.[0-9]{")?;
    let scale = scale_suffix.strip_suffix("}$")?.parse::<u64>().ok()?;
    let integer_digits = integer_digits.parse::<u64>().ok()?.checked_add(1)?;
    Some((integer_digits.checked_add(scale)?, scale))
}

fn number_i128(value: &Value) -> Option<i128> {
    let number = value.as_number()?;
    number
        .as_i64()
        .map(i128::from)
        .or_else(|| number.as_u64().map(i128::from))
}

fn parse_canonical_i64(text: &str) -> Option<i64> {
    parse_canonical_i128(text).and_then(|value| i64::try_from(value).ok())
}

fn parse_canonical_i128(text: &str) -> Option<i128> {
    if text == "0" {
        return Some(0);
    }
    let digits = text.strip_prefix('-').unwrap_or(text);
    if digits.is_empty()
        || digits.starts_with('0')
        || !digits.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    text.parse().ok()
}

fn parse_canonical_u64(text: &str) -> Option<u64> {
    if text == "0" {
        return Some(0);
    }
    if text.is_empty() || text.starts_with('0') || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    text.parse().ok()
}

fn is_lower_hex(text: &str, bytes: usize) -> bool {
    text.len() == bytes
        && text
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn uuid_shape(text: &str, v7: bool) -> bool {
    text.len() == 36
        && text.as_bytes()[8] == b'-'
        && text.as_bytes()[13] == b'-'
        && text.as_bytes()[18] == b'-'
        && text.as_bytes()[23] == b'-'
        && text.bytes().enumerate().all(|(index, byte)| {
            matches!(index, 8 | 13 | 18 | 23)
                || byte.is_ascii_digit()
                || (b'a'..=b'f').contains(&byte)
        })
        && (!v7
            || text.as_bytes()[14] == b'7'
                && matches!(text.as_bytes()[19], b'8' | b'9' | b'a' | b'b'))
}

fn is_currency(text: &str) -> bool {
    text.len() == 3 && text.bytes().all(|byte| byte.is_ascii_uppercase())
}

fn source_name(text: &str) -> bool {
    let mut bytes = text.bytes();
    bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
        && text.len() <= 256
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

#[cfg(test)]
mod tests {
    use riffdb_types::hash_schema;
    use serde_json::json;

    use super::*;
    use crate::fixed_tool_registry;

    #[test]
    fn every_accepted_schema_uses_only_the_checked_subset() {
        let registry = fixed_tool_registry().expect("accepted registry");
        for tool in registry.tools() {
            validate_schema_source(&Value::Object(tool.input_schema().json_object()))
                .expect("input subset");
            validate_schema_source(&Value::Object(tool.result_schema().json_object()))
                .expect("result subset");
        }
    }

    fn aggregate_annotation_schema(value: Value) -> Value {
        json!({
            "$schema": DIALECT,
            "additionalProperties": false,
            "properties": {
                "values": {
                    "items": {"type": "string"},
                    "type": "array",
                    JSON_AGGREGATE_CANONICAL_ELEMENT_BYTES: value,
                }
            },
            "required": ["values"],
            "type": "object",
        })
    }

    // req: BLK-019
    #[test]
    fn aggregate_canonical_element_bytes_is_a_bounded_array_annotation_only() {
        let source = serde_json::to_string(&aggregate_annotation_schema(json!(1024)))
            .expect("aggregate annotation schema JSON");
        let schema = SchemaDocument::from_canonical(
            "test/aggregate-canonical-element-bytes/v1",
            hash_schema(source.as_bytes()),
            source,
        )
        .expect("bounded aggregate annotation");
        RiffDbSchemaValidator
            .validate(&schema, &json!({"values": ["a", "b"]}))
            .expect("MCP treats the aggregate-byte keyword as annotation-only");
    }

    // req: BLK-019
    #[test]
    fn aggregate_canonical_element_bytes_refuses_zero() {
        assert_eq!(
            validate_schema_source(&aggregate_annotation_schema(json!(0))),
            Err(SchemaValidationError)
        );
    }

    // req: BLK-019
    #[test]
    fn aggregate_canonical_element_bytes_refuses_values_above_the_global_bound() {
        assert_eq!(
            validate_schema_source(&aggregate_annotation_schema(json!(16_777_217))),
            Err(SchemaValidationError)
        );
    }

    // req: BLK-019
    #[test]
    fn aggregate_canonical_element_bytes_refuses_nonintegers() {
        assert_eq!(
            validate_schema_source(&aggregate_annotation_schema(json!(1.5))),
            Err(SchemaValidationError)
        );
    }

    // req: BLK-019
    #[test]
    fn aggregate_canonical_element_bytes_refuses_wrong_placement() {
        let mut schema = aggregate_annotation_schema(json!(1024));
        let values = schema["properties"]["values"]
            .as_object_mut()
            .expect("values schema");
        values.insert("type".to_owned(), json!("string"));
        assert_eq!(validate_schema_source(&schema), Err(SchemaValidationError));

        let mut schema = aggregate_annotation_schema(json!(1024));
        schema["properties"]["values"]
            .as_object_mut()
            .expect("values schema")
            .remove("items");
        assert_eq!(validate_schema_source(&schema), Err(SchemaValidationError));
    }

    #[test]
    fn fixed_tagged_vector_schema_accepts_only_bounded_numeric_components() {
        let registry = fixed_tool_registry().expect("accepted registry");
        let schema = registry
            .by_name("riffdb_entity_scan_index")
            .expect("scan-index tool")
            .input_schema();
        let request = |components: Value| {
            json!({
                "contract": {"active": {}},
                "index_id": 1,
                "leading_components": [{"kind": "vector", "components": components}],
                "fields": [],
                "page": {},
            })
        };

        RiffDbSchemaValidator
            .validate(schema, &request(json!([0.0, 1.5, -2.25])))
            .expect("bounded vector");
        for invalid in [
            request(json!([])),
            request(Value::Array(vec![Value::from(0.0); 4_097])),
            request(json!(["not-a-number"])),
        ] {
            assert_eq!(
                RiffDbSchemaValidator.validate(schema, &invalid),
                Err(SchemaValidationError)
            );
        }
        let mut extra = request(json!([1.0]));
        extra["leading_components"][0]["extra"] = Value::Bool(true);
        assert_eq!(
            RiffDbSchemaValidator.validate(schema, &extra),
            Err(SchemaValidationError)
        );
    }

    #[test]
    fn validator_enforces_closed_shapes_relations_order_and_canonical_bytes() {
        let source = concat!(
            "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\",",
            "\"additionalProperties\":false,\"properties\":{",
            "\"bytes\":{\"contentEncoding\":\"base64\",\"type\":\"string\",",
            "\"x-riffdb-maxDecodedBytes\":3},",
            "\"fields\":{\"items\":{\"type\":\"integer\"},\"type\":\"array\",",
            "\"x-riffdb-strictlyIncreasing\":true},",
            "\"span\":{\"additionalProperties\":false,\"properties\":{",
            "\"end\":{\"type\":\"integer\"},\"start\":{\"type\":\"integer\"}},",
            "\"required\":[\"start\",\"end\"],\"type\":\"object\",",
            "\"x-riffdb-relation\":\"start<=end\"}},",
            "\"required\":[\"bytes\",\"fields\",\"span\"],\"type\":\"object\"}"
        );
        let schema = SchemaDocument::from_canonical(
            "test/validation/v1",
            hash_schema(source.as_bytes()),
            source,
        )
        .expect("schema");
        let validator = RiffDbSchemaValidator;
        validator
            .validate(
                &schema,
                &json!({"bytes":"AQID","fields":[1,2],"span":{"start":1,"end":2}}),
            )
            .expect("valid");
        for invalid in [
            json!({"bytes":"AQI","fields":[1,2],"span":{"start":1,"end":2}}),
            json!({"bytes":"AQID","fields":[2,1],"span":{"start":1,"end":2}}),
            json!({"bytes":"AQID","fields":[1,2],"span":{"start":2,"end":1}}),
            json!({"bytes":"AQID","fields":[1,2],"span":{"start":1,"end":2},"extra":0}),
        ] {
            assert_eq!(
                validator.validate(&schema, &invalid),
                Err(SchemaValidationError)
            );
        }
    }

    #[test]
    fn any_of_accepts_one_or_more_bounded_branches_and_rejects_no_match() {
        let source = concat!(
            "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\",",
            "\"additionalProperties\":false,\"properties\":{\"value\":{",
            "\"anyOf\":[{\"type\":\"integer\"},{\"minimum\":0,\"type\":\"integer\"},",
            "{\"type\":\"null\"}]}},\"required\":[\"value\"],\"type\":\"object\"}"
        );
        let schema = SchemaDocument::from_canonical(
            "test/any-of/v1",
            hash_schema(source.as_bytes()),
            source,
        )
        .expect("bounded anyOf schema");
        let validator = RiffDbSchemaValidator;
        for accepted in [
            json!({"value": 1}),
            json!({"value": -1}),
            json!({"value": null}),
        ] {
            validator
                .validate(&schema, &accepted)
                .expect("at least one branch matches");
        }
        assert_eq!(
            validator.validate(&schema, &json!({"value": "one"})),
            Err(SchemaValidationError)
        );

        let empty = json!({
            "$schema": DIALECT,
            "anyOf": []
        });
        assert_eq!(validate_schema_source(&empty), Err(SchemaValidationError));
    }

    #[test]
    fn tagged_decimal_checks_follow_the_schema_shape_not_incidental_kind_text() {
        let ordinary_source = concat!(
            "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\",",
            "\"additionalProperties\":false,\"properties\":{",
            "\"kind\":{\"const\":\"decimal\"},\"label\":{\"type\":\"string\"}},",
            "\"required\":[\"kind\",\"label\"],\"type\":\"object\"}"
        );
        let ordinary = SchemaDocument::from_canonical(
            "test/ordinary-decimal-record/v1",
            hash_schema(ordinary_source.as_bytes()),
            ordinary_source,
        )
        .expect("ordinary record schema");
        RiffDbSchemaValidator
            .validate(&ordinary, &json!({"kind":"decimal","label":"ordinary"}))
            .expect("incidental kind text is not a tagged decimal");
        let money_source = ordinary_source.replace("decimal", "money");
        let ordinary_money = SchemaDocument::from_canonical(
            "test/ordinary-money-record/v1",
            hash_schema(money_source.as_bytes()),
            money_source,
        )
        .expect("ordinary money record schema");
        RiffDbSchemaValidator
            .validate(&ordinary_money, &json!({"kind":"money","label":"ordinary"}))
            .expect("incidental kind text is not tagged money");

        let tagged_source = concat!(
            "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\",",
            "\"additionalProperties\":false,\"properties\":{",
            "\"coefficient\":{\"type\":\"string\"},\"kind\":{\"const\":\"decimal\"},",
            "\"precision\":{\"type\":\"integer\"},\"scale\":{\"type\":\"integer\"}},",
            "\"required\":[\"kind\",\"precision\",\"scale\",\"coefficient\"],",
            "\"type\":\"object\"}"
        );
        let tagged = SchemaDocument::from_canonical(
            "test/tagged-decimal/v1",
            hash_schema(tagged_source.as_bytes()),
            tagged_source,
        )
        .expect("tagged decimal schema");
        assert_eq!(
            RiffDbSchemaValidator.validate(
                &tagged,
                &json!({"kind":"decimal","precision":2,"scale":1,"coefficient":"123"})
            ),
            Err(SchemaValidationError)
        );
    }

    #[test]
    fn compiler_decimal_boundary_patterns_and_zero_values_are_accepted() {
        for (pattern, precision, scale, valid) in [
            ("^-?(0|[1-9][0-9]{0,1})$", 2, 0, "0"),
            ("^-?(0|[1-9][0-9]{0,3})\\.[0-9]{2}$", 6, 2, "0.00"),
            ("^-?0\\.[0-9]{2}$", 2, 2, "0.00"),
        ] {
            let source = format!(
                concat!(
                    "{{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\",",
                    "\"additionalProperties\":false,\"properties\":{{\"value\":{{",
                    "\"pattern\":{},\"type\":\"string\",",
                    "\"x-riffdb-decimalPrecision\":{precision},",
                    "\"x-riffdb-decimalScale\":{scale}}}}},",
                    "\"required\":[\"value\"],\"type\":\"object\"}}"
                ),
                serde_json::to_string(pattern).expect("pattern JSON"),
                precision = precision,
                scale = scale,
            );
            let schema = SchemaDocument::from_canonical(
                format!("test/decimal/{precision}/{scale}"),
                hash_schema(source.as_bytes()),
                source,
            )
            .expect("compiler decimal schema");
            RiffDbSchemaValidator
                .validate(&schema, &json!({"value": valid}))
                .expect("canonical minimal decimal");
        }
    }

    #[test]
    fn command_composition_replaces_only_the_accepted_false_placeholder() {
        let registry = fixed_tool_registry().expect("accepted registry");
        let envelope = &registry.operation_schemas()[0];
        let outcome_source = concat!(
            "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\",",
            "\"oneOf\":[{\"additionalProperties\":false,\"properties\":{",
            "\"type\":{\"const\":\"Done\"}},\"required\":[\"type\"],\"type\":\"object\"}]}"
        );
        let outcome = SchemaDocument::from_canonical(
            "compiler.command-outcome/1",
            hash_schema(outcome_source.as_bytes()),
            outcome_source,
        )
        .expect("outcome");
        let composed =
            compose_command_result_schema(&outcome, envelope).expect("composed envelope");

        let committed = json!({
            "status": "committed",
            "commit_sequence": "1",
            "contract_version": 1,
            "plan_hash": "0".repeat(64),
            "outcome": {"type": "Done"},
            "provenance_uri": "riffdb://provenance/00000000-0001-7000-8000-000000000000",
            "durability_mode": "sync",
            "outcome_uri": concat!(
                "riffdb://outcome/actor/orders/1/riffdb_cmd_orders_place/",
                "AQAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
            )
        });
        RiffDbSchemaValidator
            .validate(&composed, &committed)
            .expect("composed instance");
    }
}
