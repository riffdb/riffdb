//! Deterministic JSON Schema draft 2020-12 artifacts.

use std::collections::BTreeMap;

use riffdb_types::{
    CommandId, EntityTypeId, EventTypeId, FieldId, ProjectionId, SchemaHash, hash_schema,
};

use crate::format_registry::{
    JSON_DECIMAL_PRECISION, JSON_DECIMAL_SCALE, JSON_I64_STRING_PATTERN, JSON_INTEGER_TYPE,
    JSON_MAX_DECODED_BYTES, JSON_MAX_UTF8_BYTES, JSON_MIN_UTF8_BYTES, JSON_MONEY_CURRENCY,
    JSON_MONEY_PRECISION, JSON_MONEY_SCALE, JSON_UUID_PATTERN, JsonSchemaShapeConstruction,
    JsonSchemaValueConstruction, json_decimal_pattern, json_schema_shape_template,
    json_schema_value_template,
};
use crate::{
    IrValidationError, OutcomeSchema, RecordSchema, RecordTypeRef, SchemaIr, ValueType,
    ValueTypeTag, checked_len,
};

/// Immutable JSON Schema dialect used by generated artifacts.
pub const JSON_SCHEMA_DIALECT: &str = "https://json-schema.org/draft/2020-12/schema";

/// Maximum canonical bytes in one generated schema artifact.
pub const MAX_JSON_SCHEMA_ARTIFACT_BYTES: usize = 1024 * 1024;

/// Closed generated schema-artifact key registry.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum SchemaArtifactKey {
    /// Entity record schema.
    Entity(EntityTypeId),
    /// Durable event payload schema.
    Event(EventTypeId),
    /// Command input schema.
    CommandInput(CommandId),
    /// Command declared-outcome union.
    CommandOutcomeUnion(CommandId),
    /// One public projection result row.
    ProjectionResult(ProjectionId),
}

impl SchemaArtifactKey {
    /// Immutable encoded key tag.
    #[must_use]
    pub const fn tag(self) -> u8 {
        match self {
            Self::Entity(_) => crate::format_registry::schema_artifact::ENTITY,
            Self::Event(_) => crate::format_registry::schema_artifact::EVENT,
            Self::CommandInput(_) => crate::format_registry::schema_artifact::COMMAND_INPUT,
            Self::CommandOutcomeUnion(_) => {
                crate::format_registry::schema_artifact::COMMAND_OUTCOME_UNION
            }
            Self::ProjectionResult(_) => crate::format_registry::schema_artifact::PROJECTION_RESULT,
        }
    }

    /// Stable owner ID.
    #[must_use]
    pub const fn stable_id(self) -> u32 {
        match self {
            Self::Entity(id) => id.get(),
            Self::Event(id) => id.get(),
            Self::CommandInput(id) | Self::CommandOutcomeUnion(id) => id.get(),
            Self::ProjectionResult(id) => id.get(),
        }
    }

    /// Exact five-byte canonical key.
    #[must_use]
    pub fn to_bytes(self) -> [u8; 5] {
        let id = self.stable_id().to_be_bytes();
        [self.tag(), id[0], id[1], id[2], id[3]]
    }
}

/// One generated canonical JSON Schema and its typed hash.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GeneratedSchemaArtifact {
    key: SchemaArtifactKey,
    canonical_json: String,
    hash: SchemaHash,
}

impl GeneratedSchemaArtifact {
    /// Generates one closed entity-record artifact.
    pub fn entity(
        entity_id: EntityTypeId,
        record: &RecordSchema,
        schema: &SchemaIr,
    ) -> Result<Self, IrValidationError> {
        require_shape(JsonSchemaShapeConstruction::OutputRecord)?;
        let key = SchemaArtifactKey::Entity(entity_id);
        if record.owner() != &RecordTypeRef::Entity(entity_id) {
            return Err(IrValidationError::InvalidReference {
                kind: "record schema artifact owner",
            });
        }
        let expected_size = record_node_size(record, schema, RecordShape::Output, true)?;
        let node = record_node(record, schema, RecordShape::Output, true)?;
        Self::from_node(key, node, expected_size)
    }

    /// Generates one closed durable-event payload artifact.
    pub fn event(
        event_id: EventTypeId,
        record: &RecordSchema,
        schema: &SchemaIr,
    ) -> Result<Self, IrValidationError> {
        require_shape(JsonSchemaShapeConstruction::OutputRecord)?;
        let key = SchemaArtifactKey::Event(event_id);
        if record.owner() != &RecordTypeRef::Event(event_id) {
            return Err(IrValidationError::InvalidReference {
                kind: "record schema artifact owner",
            });
        }
        let expected_size = record_node_size(record, schema, RecordShape::Output, true)?;
        let node = record_node(record, schema, RecordShape::Output, true)?;
        Self::from_node(key, node, expected_size)
    }

    /// Generates one command-input artifact with its exact idempotency bound.
    pub fn command_input(
        command_id: CommandId,
        record: &RecordSchema,
        schema: &SchemaIr,
        idempotency_input: Option<FieldId>,
    ) -> Result<Self, IrValidationError> {
        require_shape(JsonSchemaShapeConstruction::CommandInput)?;
        let key = SchemaArtifactKey::CommandInput(command_id);
        if record.owner() != &RecordTypeRef::CommandInput(command_id) {
            return Err(IrValidationError::InvalidReference {
                kind: "record schema artifact owner",
            });
        }
        if let Some(field_id) = idempotency_input {
            let field = record
                .field(field_id)
                .ok_or(IrValidationError::InvalidReference {
                    kind: "idempotency input schema field",
                })?;
            let supported = match field.value_type().tag() {
                ValueTypeTag::Uuid => true,
                ValueTypeTag::String => field
                    .value_type()
                    .byte_bound()
                    .is_some_and(|bound| bound != 0 && bound <= 128),
                _ => false,
            };
            if !supported || field.value_type().is_optional() {
                return Err(IrValidationError::TypeMismatch {
                    context: "idempotency input schema field",
                });
            }
        }
        let shape = RecordShape::CommandInput { idempotency_input };
        let expected_size = record_node_size(record, schema, shape, true)?;
        let node = record_node(record, schema, shape, true)?;
        Self::from_node(key, node, expected_size)
    }

    /// Generates one flat closed command outcome union artifact.
    pub fn command_outcomes(
        command_id: CommandId,
        outcomes: &[OutcomeSchema],
        schema: &SchemaIr,
    ) -> Result<Self, IrValidationError> {
        require_shape(JsonSchemaShapeConstruction::CommandOutcomeUnion)?;
        if outcomes.is_empty() {
            return Err(IrValidationError::Empty {
                kind: "outcome schema",
            });
        }
        let expected_size = command_outcomes_size(command_id, outcomes, schema)?;
        let mut variants = Vec::with_capacity(outcomes.len());
        let mut previous = None;
        for outcome in outcomes {
            if previous.is_some_and(|id| id >= outcome.id()) {
                return Err(IrValidationError::NonCanonicalOrder {
                    kind: "outcome schemas",
                });
            }
            previous = Some(outcome.id());
            if outcome.payload().owner()
                != &(RecordTypeRef::CommandOutcome {
                    command_id,
                    outcome_id: outcome.id(),
                })
            {
                return Err(IrValidationError::InvalidReference {
                    kind: "outcome schema owner",
                });
            }
            let mut properties = BTreeMap::new();
            properties.insert(
                "type".to_owned(),
                Json::object([("const", Json::String(outcome.name().to_owned()))]),
            );
            let mut required = vec![Json::String("type".to_owned())];
            for field in outcome.payload().fields() {
                properties.insert(
                    field.name().to_owned(),
                    type_node(field.value_type(), schema)?,
                );
                required.push(Json::String(field.name().to_owned()));
            }
            variants.push(Json::object([
                ("additionalProperties", Json::Bool(false)),
                ("properties", Json::Object(properties)),
                ("required", Json::Array(required)),
                ("type", Json::String("object".to_owned())),
            ]));
        }
        let node = Json::object([
            ("$schema", Json::String(JSON_SCHEMA_DIALECT.to_owned())),
            ("oneOf", Json::Array(variants)),
        ]);
        Self::from_node(
            SchemaArtifactKey::CommandOutcomeUnion(command_id),
            node,
            expected_size,
        )
    }

    /// Generates one projection-result row artifact.
    pub fn projection_result(
        projection_id: ProjectionId,
        group_types: &[ValueType],
        measures: &RecordSchema,
        schema: &SchemaIr,
    ) -> Result<Self, IrValidationError> {
        require_shape(JsonSchemaShapeConstruction::ProjectionTupleKey)?;
        require_shape(JsonSchemaShapeConstruction::ProjectionResultRow)?;
        if measures.owner() != &RecordTypeRef::ProjectionResult(projection_id) {
            return Err(IrValidationError::InvalidReference {
                kind: "projection result owner",
            });
        }
        let expected_size = projection_result_size(group_types, measures, schema)?;
        let key = projection_tuple_node(group_types, schema)?;
        let measures = record_node(measures, schema, RecordShape::Output, false)?;
        let node = Json::object([
            ("$schema", Json::String(JSON_SCHEMA_DIALECT.to_owned())),
            ("additionalProperties", Json::Bool(false)),
            (
                "properties",
                Json::object([("key", key), ("measures", measures)]),
            ),
            (
                "required",
                Json::Array(vec![
                    Json::String("key".to_owned()),
                    Json::String("measures".to_owned()),
                ]),
            ),
            ("type", Json::String("object".to_owned())),
        ]);
        Self::from_node(
            SchemaArtifactKey::ProjectionResult(projection_id),
            node,
            expected_size,
        )
    }

    fn from_node(
        key: SchemaArtifactKey,
        node: Json,
        expected_size: usize,
    ) -> Result<Self, IrValidationError> {
        checked_len(
            "generated JSON Schema",
            expected_size,
            MAX_JSON_SCHEMA_ARTIFACT_BYTES,
        )?;
        let mut canonical_json = String::with_capacity(expected_size);
        node.write(&mut canonical_json);
        if canonical_json.len() != expected_size {
            return Err(IrValidationError::HashMismatch {
                kind: "JSON Schema size preflight",
            });
        }
        let hash = hash_schema(canonical_json.as_bytes());
        Ok(Self {
            key,
            canonical_json,
            hash,
        })
    }

    /// Closed artifact key.
    #[must_use]
    pub const fn key(&self) -> SchemaArtifactKey {
        self.key
    }
    /// Canonical no-whitespace JSON UTF-8.
    #[must_use]
    pub fn canonical_json(&self) -> &str {
        &self.canonical_json
    }
    /// Typed hash of canonical JSON bytes.
    #[must_use]
    pub const fn hash(&self) -> SchemaHash {
        self.hash
    }
}

fn require_shape(construction: JsonSchemaShapeConstruction) -> Result<(), IrValidationError> {
    json_schema_shape_template(construction)
        .map(|_| ())
        .ok_or(IrValidationError::TypeMismatch {
            context: "JSON Schema shape registry",
        })
}

#[derive(Clone, Copy)]
enum RecordShape {
    Output,
    CommandInput { idempotency_input: Option<FieldId> },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SchemaNodeSize {
    bytes: usize,
    object_entries: usize,
}

impl SchemaNodeSize {
    fn insert(&mut self, key: &str, value_bytes: usize) -> Result<(), IrValidationError> {
        let separator = usize::from(self.object_entries != 0);
        self.bytes = checked_json_size_sum([
            self.bytes,
            separator,
            json_string_size(key)?,
            1,
            value_bytes,
        ])?;
        self.object_entries =
            self.object_entries
                .checked_add(1)
                .ok_or(IrValidationError::SizeOverflow {
                    kind: "generated JSON Schema",
                })?;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
struct ObjectSizer {
    bytes: usize,
    entries: usize,
}

impl ObjectSizer {
    const fn new() -> Self {
        Self {
            bytes: 2,
            entries: 0,
        }
    }

    fn push(&mut self, key: &str, value_bytes: usize) -> Result<(), IrValidationError> {
        let separator = usize::from(self.entries != 0);
        self.bytes = checked_json_size_sum([
            self.bytes,
            separator,
            json_string_size(key)?,
            1,
            value_bytes,
        ])?;
        self.entries = self
            .entries
            .checked_add(1)
            .ok_or(IrValidationError::SizeOverflow {
                kind: "generated JSON Schema",
            })?;
        Ok(())
    }

    const fn finish(self) -> SchemaNodeSize {
        SchemaNodeSize {
            bytes: self.bytes,
            object_entries: self.entries,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct ArraySizer {
    bytes: usize,
    entries: usize,
}

impl ArraySizer {
    const fn new() -> Self {
        Self {
            bytes: 2,
            entries: 0,
        }
    }

    fn push(&mut self, value_bytes: usize) -> Result<(), IrValidationError> {
        let separator = usize::from(self.entries != 0);
        self.bytes = checked_json_size_sum([self.bytes, separator, value_bytes])?;
        self.entries = self
            .entries
            .checked_add(1)
            .ok_or(IrValidationError::SizeOverflow {
                kind: "generated JSON Schema",
            })?;
        Ok(())
    }

    const fn finish(self) -> usize {
        self.bytes
    }
}

fn checked_json_size_sum<const N: usize>(parts: [usize; N]) -> Result<usize, IrValidationError> {
    let mut total = 0usize;
    for part in parts {
        total = total
            .checked_add(part)
            .ok_or(IrValidationError::SizeOverflow {
                kind: "generated JSON Schema",
            })?;
        checked_len(
            "generated JSON Schema",
            total,
            MAX_JSON_SCHEMA_ARTIFACT_BYTES,
        )?;
    }
    Ok(total)
}

fn json_string_size(value: &str) -> Result<usize, IrValidationError> {
    let mut bytes = 2usize;
    for character in value.chars() {
        let character_bytes = match character {
            '"' | '\\' | '\u{08}' | '\u{0c}' | '\n' | '\r' | '\t' => 2,
            character if character <= '\u{1f}' => 6,
            character => character.len_utf8(),
        };
        bytes = checked_json_size_sum([bytes, character_bytes])?;
    }
    Ok(bytes)
}

fn number_size(value: impl ToString) -> usize {
    value.to_string().len()
}

fn string_value_size(value: &str) -> Result<usize, IrValidationError> {
    json_string_size(value)
}

fn object_size<const N: usize>(
    entries: [(&str, usize); N],
) -> Result<SchemaNodeSize, IrValidationError> {
    let mut object = ObjectSizer::new();
    for (key, value) in entries {
        object.push(key, value)?;
    }
    Ok(object.finish())
}

fn string_array_size<'a>(
    values: impl IntoIterator<Item = &'a str>,
) -> Result<usize, IrValidationError> {
    let mut array = ArraySizer::new();
    for value in values {
        array.push(string_value_size(value)?)?;
    }
    Ok(array.finish())
}

fn record_node_size(
    record: &RecordSchema,
    schema: &SchemaIr,
    shape: RecordShape,
    root: bool,
) -> Result<usize, IrValidationError> {
    let mut properties = ObjectSizer::new();
    let mut required = ArraySizer::new();
    for field in record.fields() {
        let mut node = type_node_size(field.value_type(), schema)?;
        if matches!(shape, RecordShape::CommandInput { .. }) && field.value_type().is_optional() {
            node.insert("default", 4)?;
        } else {
            required.push(string_value_size(field.name())?)?;
        }
        if matches!(
            shape,
            RecordShape::CommandInput { idempotency_input: Some(id) } if id == field.id()
        ) {
            node.insert("minLength", 1)?;
            node.insert(JSON_MIN_UTF8_BYTES, 1)?;
        }
        properties.push(field.name(), node.bytes)?;
    }

    let mut object = ObjectSizer::new();
    if root {
        object.push("$schema", string_value_size(JSON_SCHEMA_DIALECT)?)?;
    }
    object.push("additionalProperties", 5)?;
    object.push("properties", properties.finish().bytes)?;
    object.push("required", required.finish())?;
    object.push("type", string_value_size("object")?)?;
    Ok(object.finish().bytes)
}

fn command_outcomes_size(
    command_id: CommandId,
    outcomes: &[OutcomeSchema],
    schema: &SchemaIr,
) -> Result<usize, IrValidationError> {
    if outcomes.is_empty() {
        return Err(IrValidationError::Empty {
            kind: "outcome schema",
        });
    }
    let mut variants = ArraySizer::new();
    let mut previous = None;
    for outcome in outcomes {
        if previous.is_some_and(|id| id >= outcome.id()) {
            return Err(IrValidationError::NonCanonicalOrder {
                kind: "outcome schemas",
            });
        }
        previous = Some(outcome.id());
        if outcome.payload().owner()
            != &(RecordTypeRef::CommandOutcome {
                command_id,
                outcome_id: outcome.id(),
            })
        {
            return Err(IrValidationError::InvalidReference {
                kind: "outcome schema owner",
            });
        }

        let discriminator = object_size([("const", string_value_size(outcome.name())?)])?;
        let mut properties = ObjectSizer::new();
        properties.push("type", discriminator.bytes)?;
        let mut required = ArraySizer::new();
        required.push(string_value_size("type")?)?;
        for field in outcome.payload().fields() {
            properties.push(
                field.name(),
                type_node_size(field.value_type(), schema)?.bytes,
            )?;
            required.push(string_value_size(field.name())?)?;
        }
        let variant = object_size([
            ("additionalProperties", 5),
            ("properties", properties.finish().bytes),
            ("required", required.finish()),
            ("type", string_value_size("object")?),
        ])?;
        variants.push(variant.bytes)?;
    }

    Ok(object_size([
        ("$schema", string_value_size(JSON_SCHEMA_DIALECT)?),
        ("oneOf", variants.finish()),
    ])?
    .bytes)
}

fn projection_result_size(
    group_types: &[ValueType],
    measures: &RecordSchema,
    schema: &SchemaIr,
) -> Result<usize, IrValidationError> {
    let key = projection_tuple_size(group_types, schema)?;
    let measures = record_node_size(measures, schema, RecordShape::Output, false)?;
    let properties = object_size([("key", key.bytes), ("measures", measures)])?;
    let required = string_array_size(["key", "measures"])?;
    Ok(object_size([
        ("$schema", string_value_size(JSON_SCHEMA_DIALECT)?),
        ("additionalProperties", 5),
        ("properties", properties.bytes),
        ("required", required),
        ("type", string_value_size("object")?),
    ])?
    .bytes)
}

fn projection_tuple_size(
    group_types: &[ValueType],
    schema: &SchemaIr,
) -> Result<SchemaNodeSize, IrValidationError> {
    let mut prefix_items = ArraySizer::new();
    for value_type in group_types {
        prefix_items.push(type_node_size(value_type, schema)?.bytes)?;
    }
    object_size([
        ("items", 5),
        ("maxItems", number_size(group_types.len())),
        ("minItems", number_size(group_types.len())),
        ("prefixItems", prefix_items.finish()),
        ("type", string_value_size("array")?),
    ])
}

fn projection_tuple_node(
    group_types: &[ValueType],
    schema: &SchemaIr,
) -> Result<Json, IrValidationError> {
    let prefix_items = group_types
        .iter()
        .map(|value_type| type_node(value_type, schema))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Json::object([
        ("items", Json::Bool(false)),
        ("maxItems", Json::Number(group_types.len().to_string())),
        ("minItems", Json::Number(group_types.len().to_string())),
        ("prefixItems", Json::Array(prefix_items)),
        ("type", Json::String("array".to_owned())),
    ]))
}

fn type_node_size(
    value_type: &ValueType,
    schema: &SchemaIr,
) -> Result<SchemaNodeSize, IrValidationError> {
    let template = json_schema_value_template(value_type.tag() as u8).ok_or(
        IrValidationError::TypeMismatch {
            context: "JSON Schema value registry",
        },
    )?;
    match template.construction {
        JsonSchemaValueConstruction::Boolean => {
            object_size([("type", string_value_size("boolean")?)])
        }
        JsonSchemaValueConstruction::Integer { minimum, maximum } => object_size([
            ("maximum", maximum.len()),
            ("minimum", minimum.len()),
            ("type", string_value_size("integer")?),
        ]),
        JsonSchemaValueConstruction::Decimal => {
            let spec = value_type.decimal_spec().expect("decimal tag");
            decimal_node_size(spec.precision(), spec.scale(), None)
        }
        JsonSchemaValueConstruction::Money => {
            let currency = value_type.currency().expect("money tag");
            let currency = currency.to_string();
            decimal_node_size(JSON_MONEY_PRECISION, JSON_MONEY_SCALE, Some(&currency))
        }
        JsonSchemaValueConstruction::String => object_size([
            ("type", string_value_size("string")?),
            (
                JSON_MAX_UTF8_BYTES,
                number_size(value_type.byte_bound().expect("string bound")),
            ),
        ]),
        JsonSchemaValueConstruction::Bytes => object_size([
            ("contentEncoding", string_value_size("base64")?),
            ("type", string_value_size("string")?),
            (
                JSON_MAX_DECODED_BYTES,
                number_size(value_type.byte_bound().expect("bytes bound")),
            ),
        ]),
        JsonSchemaValueConstruction::Timestamp => {
            let nanos = object_size([
                ("maximum", 9),
                ("minimum", 1),
                ("type", string_value_size("integer")?),
            ])?;
            let seconds = object_size([
                ("pattern", string_value_size(JSON_I64_STRING_PATTERN)?),
                ("type", string_value_size("string")?),
                (JSON_INTEGER_TYPE, string_value_size("i64")?),
            ])?;
            let properties = object_size([("nanos", nanos.bytes), ("seconds", seconds.bytes)])?;
            object_size([
                ("additionalProperties", 5),
                ("properties", properties.bytes),
                ("required", string_array_size(["seconds", "nanos"])?),
                ("type", string_value_size("object")?),
            ])
        }
        JsonSchemaValueConstruction::Uuid => object_size([
            ("pattern", string_value_size(JSON_UUID_PATTERN)?),
            ("type", string_value_size("string")?),
        ]),
        JsonSchemaValueConstruction::Enum => {
            let enumeration = schema
                .enumeration(value_type.enum_type_id().expect("enum tag"))
                .ok_or(IrValidationError::InvalidReference {
                    kind: "JSON Schema enum",
                })?;
            object_size([
                (
                    "enum",
                    string_array_size(enumeration.variants().iter().map(|variant| variant.name()))?,
                ),
                ("type", string_value_size("string")?),
            ])
        }
        JsonSchemaValueConstruction::Optional => {
            let inner = type_node_size(value_type.optional_inner().expect("optional tag"), schema)?;
            let null = object_size([("type", string_value_size("null")?)])?;
            let mut one_of = ArraySizer::new();
            one_of.push(inner.bytes)?;
            one_of.push(null.bytes)?;
            object_size([("oneOf", one_of.finish())])
        }
        JsonSchemaValueConstruction::List => {
            let (element, maximum) = value_type.list_parts().expect("list tag");
            object_size([
                ("items", type_node_size(element, schema)?.bytes),
                ("maxItems", number_size(maximum)),
                ("type", string_value_size("array")?),
            ])
        }
        JsonSchemaValueConstruction::Record => {
            let record = match value_type.record_ref().expect("record tag") {
                RecordTypeRef::Entity(id) => schema.entity(*id).map(|value| value.record()),
                RecordTypeRef::Event(id) => schema.event(*id).map(|value| value.payload()),
                _ => None,
            }
            .ok_or(IrValidationError::InvalidReference {
                kind: "inline record JSON Schema",
            })?;
            Ok(SchemaNodeSize {
                bytes: record_node_size(record, schema, RecordShape::Output, false)?,
                object_entries: 4,
            })
        }
    }
}

fn decimal_node_size(
    precision: u8,
    scale: u8,
    currency: Option<&str>,
) -> Result<SchemaNodeSize, IrValidationError> {
    let pattern =
        json_decimal_pattern(precision, scale).ok_or(IrValidationError::TypeMismatch {
            context: "JSON Schema decimal",
        })?;
    let mut values = ObjectSizer::new();
    values.push("pattern", string_value_size(&pattern)?)?;
    values.push("type", string_value_size("string")?)?;
    values.push(JSON_DECIMAL_PRECISION, number_size(precision))?;
    values.push(JSON_DECIMAL_SCALE, number_size(scale))?;
    if let Some(currency) = currency {
        values.push(JSON_MONEY_CURRENCY, string_value_size(currency)?)?;
    }
    Ok(values.finish())
}

fn record_node(
    record: &RecordSchema,
    schema: &SchemaIr,
    shape: RecordShape,
    root: bool,
) -> Result<Json, IrValidationError> {
    let mut properties = BTreeMap::new();
    let mut required = Vec::new();
    for field in record.fields() {
        let mut node = type_node(field.value_type(), schema)?;
        if matches!(shape, RecordShape::CommandInput { .. }) && field.value_type().is_optional() {
            node.insert("default", Json::Null)?;
        } else {
            required.push(Json::String(field.name().to_owned()));
        }
        if matches!(
            shape,
            RecordShape::CommandInput { idempotency_input: Some(id) } if id == field.id()
        ) {
            node.insert("minLength", Json::Number("1".to_owned()))?;
            node.insert(JSON_MIN_UTF8_BYTES, Json::Number("1".to_owned()))?;
        }
        properties.insert(field.name().to_owned(), node);
    }
    let mut object = BTreeMap::new();
    if root {
        object.insert(
            "$schema".to_owned(),
            Json::String(JSON_SCHEMA_DIALECT.to_owned()),
        );
    }
    object.insert("additionalProperties".to_owned(), Json::Bool(false));
    object.insert("properties".to_owned(), Json::Object(properties));
    object.insert("required".to_owned(), Json::Array(required));
    object.insert("type".to_owned(), Json::String("object".to_owned()));
    Ok(Json::Object(object))
}

fn type_node(value_type: &ValueType, schema: &SchemaIr) -> Result<Json, IrValidationError> {
    let template = json_schema_value_template(value_type.tag() as u8).ok_or(
        IrValidationError::TypeMismatch {
            context: "JSON Schema value registry",
        },
    )?;
    Ok(match template.construction {
        JsonSchemaValueConstruction::Boolean => {
            Json::object([("type", Json::String("boolean".to_owned()))])
        }
        JsonSchemaValueConstruction::Integer { minimum, maximum } => {
            integer_node(minimum.to_owned(), maximum.to_owned())
        }
        JsonSchemaValueConstruction::Decimal => {
            let spec = value_type.decimal_spec().expect("decimal tag");
            decimal_node(spec.precision(), spec.scale(), None)?
        }
        JsonSchemaValueConstruction::Money => {
            let currency = value_type.currency().expect("money tag");
            decimal_node(
                JSON_MONEY_PRECISION,
                JSON_MONEY_SCALE,
                Some(currency.to_string()),
            )?
        }
        JsonSchemaValueConstruction::String => Json::object([
            ("type", Json::String("string".to_owned())),
            (
                JSON_MAX_UTF8_BYTES,
                Json::Number(value_type.byte_bound().expect("string bound").to_string()),
            ),
        ]),
        JsonSchemaValueConstruction::Bytes => Json::object([
            ("contentEncoding", Json::String("base64".to_owned())),
            ("type", Json::String("string".to_owned())),
            (
                JSON_MAX_DECODED_BYTES,
                Json::Number(value_type.byte_bound().expect("bytes bound").to_string()),
            ),
        ]),
        JsonSchemaValueConstruction::Timestamp => Json::object([
            ("additionalProperties", Json::Bool(false)),
            (
                "properties",
                Json::object([
                    (
                        "nanos",
                        Json::object([
                            ("maximum", Json::Number("999999999".to_owned())),
                            ("minimum", Json::Number("0".to_owned())),
                            ("type", Json::String("integer".to_owned())),
                        ]),
                    ),
                    (
                        "seconds",
                        Json::object([
                            ("pattern", Json::String(JSON_I64_STRING_PATTERN.to_owned())),
                            ("type", Json::String("string".to_owned())),
                            (JSON_INTEGER_TYPE, Json::String("i64".to_owned())),
                        ]),
                    ),
                ]),
            ),
            (
                "required",
                Json::Array(vec![
                    Json::String("seconds".to_owned()),
                    Json::String("nanos".to_owned()),
                ]),
            ),
            ("type", Json::String("object".to_owned())),
        ]),
        JsonSchemaValueConstruction::Uuid => Json::object([
            ("pattern", Json::String(JSON_UUID_PATTERN.to_owned())),
            ("type", Json::String("string".to_owned())),
        ]),
        JsonSchemaValueConstruction::Enum => {
            let enumeration = schema
                .enumeration(value_type.enum_type_id().expect("enum tag"))
                .ok_or(IrValidationError::InvalidReference {
                    kind: "JSON Schema enum",
                })?;
            Json::object([
                (
                    "enum",
                    Json::Array(
                        enumeration
                            .variants()
                            .iter()
                            .map(|variant| Json::String(variant.name().to_owned()))
                            .collect(),
                    ),
                ),
                ("type", Json::String("string".to_owned())),
            ])
        }
        JsonSchemaValueConstruction::Optional => Json::object([(
            "oneOf",
            Json::Array(vec![
                type_node(value_type.optional_inner().expect("optional tag"), schema)?,
                Json::object([("type", Json::String("null".to_owned()))]),
            ]),
        )]),
        JsonSchemaValueConstruction::List => {
            let (element, maximum) = value_type.list_parts().expect("list tag");
            Json::object([
                ("items", type_node(element, schema)?),
                ("maxItems", Json::Number(maximum.to_string())),
                ("type", Json::String("array".to_owned())),
            ])
        }
        JsonSchemaValueConstruction::Record => {
            let record = match value_type.record_ref().expect("record tag") {
                RecordTypeRef::Entity(id) => schema.entity(*id).map(|value| value.record()),
                RecordTypeRef::Event(id) => schema.event(*id).map(|value| value.payload()),
                _ => None,
            }
            .ok_or(IrValidationError::InvalidReference {
                kind: "inline record JSON Schema",
            })?;
            record_node(record, schema, RecordShape::Output, false)?
        }
    })
}

fn integer_node(minimum: String, maximum: String) -> Json {
    Json::object([
        ("maximum", Json::Number(maximum)),
        ("minimum", Json::Number(minimum)),
        ("type", Json::String("integer".to_owned())),
    ])
}

fn decimal_node(
    precision: u8,
    scale: u8,
    currency: Option<String>,
) -> Result<Json, IrValidationError> {
    let pattern =
        json_decimal_pattern(precision, scale).ok_or(IrValidationError::TypeMismatch {
            context: "JSON Schema decimal",
        })?;
    let mut values = BTreeMap::new();
    values.insert("pattern".to_owned(), Json::String(pattern));
    values.insert("type".to_owned(), Json::String("string".to_owned()));
    values.insert(
        JSON_DECIMAL_PRECISION.to_owned(),
        Json::Number(precision.to_string()),
    );
    values.insert(
        JSON_DECIMAL_SCALE.to_owned(),
        Json::Number(scale.to_string()),
    );
    if let Some(currency) = currency {
        values.insert(JSON_MONEY_CURRENCY.to_owned(), Json::String(currency));
    }
    Ok(Json::Object(values))
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Json {
    Null,
    Bool(bool),
    Number(String),
    String(String),
    Array(Vec<Self>),
    Object(BTreeMap<String, Self>),
}

impl Json {
    fn object<const N: usize>(entries: [(&str, Self); N]) -> Self {
        Self::Object(
            entries
                .into_iter()
                .map(|(key, value)| (key.to_owned(), value))
                .collect(),
        )
    }

    fn insert(&mut self, key: &str, value: Self) -> Result<(), IrValidationError> {
        let Self::Object(entries) = self else {
            return Err(IrValidationError::TypeMismatch {
                context: "JSON Schema object",
            });
        };
        if entries.insert(key.to_owned(), value).is_some() {
            return Err(IrValidationError::NonCanonicalOrder {
                kind: "JSON Schema keys",
            });
        }
        Ok(())
    }

    fn write(&self, output: &mut String) {
        match self {
            Self::Null => output.push_str("null"),
            Self::Bool(value) => output.push_str(if *value { "true" } else { "false" }),
            Self::Number(value) => output.push_str(value),
            Self::String(value) => write_json_string(value, output),
            Self::Array(values) => {
                output.push('[');
                for (index, value) in values.iter().enumerate() {
                    if index != 0 {
                        output.push(',');
                    }
                    value.write(output);
                }
                output.push(']');
            }
            Self::Object(values) => {
                output.push('{');
                for (index, (key, value)) in values.iter().enumerate() {
                    if index != 0 {
                        output.push(',');
                    }
                    write_json_string(key, output);
                    output.push(':');
                    value.write(output);
                }
                output.push('}');
            }
        }
    }
}

fn write_json_string(value: &str, output: &mut String) {
    output.push('"');
    for character in value.chars() {
        match character {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            '\u{08}' => output.push_str("\\b"),
            '\u{0c}' => output.push_str("\\f"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            character if character <= '\u{1f}' => {
                use std::fmt::Write as _;
                write!(output, "\\u{:04x}", character as u32)
                    .expect("writing to String cannot fail");
            }
            character => output.push(character),
        }
    }
    output.push('"');
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EnumSchema, EnumVariantSchema, EventSchema, FieldSchema, RecordSchema};
    use riffdb_types::{
        CurrencyCode, DecimalSpec, EntityTypeId, EnumTypeId, EnumVariantId, EventTypeId, FieldId,
        OutcomeId, ProjectionId,
    };

    fn canonical_type_schema(value_type: &ValueType, schema: &SchemaIr) -> String {
        let node = type_node(value_type, schema).expect("type schema");
        let mut output = String::new();
        node.write(&mut output);
        output
    }

    #[test]
    fn canonical_object_keys_are_ascii_sorted_but_required_is_semantic_order() {
        let record = RecordSchema::new(
            RecordTypeRef::CommandInput(CommandId::first()),
            vec![
                FieldSchema::new(FieldId::first(), "z", ValueType::i64()).expect("field"),
                FieldSchema::new(FieldId::new(2).expect("id"), "a", ValueType::u64())
                    .expect("field"),
            ],
        )
        .expect("record");
        let empty = SchemaIr::new(vec![], vec![], vec![], vec![]).expect("schema");
        let artifact =
            GeneratedSchemaArtifact::command_input(CommandId::first(), &record, &empty, None)
                .expect("artifact");
        assert!(artifact.canonical_json().contains("\"properties\":{\"a\""));
        assert!(
            artifact
                .canonical_json()
                .contains("\"required\":[\"z\",\"a\"]")
        );
    }

    #[test]
    fn idempotency_property_has_exact_minimum_byte_keywords() {
        let field_id = FieldId::first();
        let record = RecordSchema::new(
            RecordTypeRef::CommandInput(CommandId::first()),
            vec![
                FieldSchema::new(
                    field_id,
                    "idempotency_key",
                    ValueType::string(128).expect("type"),
                )
                .expect("field"),
            ],
        )
        .expect("record");
        let empty = SchemaIr::new(vec![], vec![], vec![], vec![]).expect("schema");
        let artifact = GeneratedSchemaArtifact::command_input(
            CommandId::first(),
            &record,
            &empty,
            Some(field_id),
        )
        .expect("artifact");
        assert_eq!(
            artifact.canonical_json(),
            concat!(
                "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\",",
                "\"additionalProperties\":false,\"properties\":{\"idempotency_key\":{",
                "\"minLength\":1,\"type\":\"string\",\"x-riffdb-maxUtf8Bytes\":128,",
                "\"x-riffdb-minUtf8Bytes\":1}},\"required\":[\"idempotency_key\"],",
                "\"type\":\"object\"}"
            )
        );
    }

    #[test]
    fn every_value_type_has_an_exact_emitter_vector() {
        let enum_id = EnumTypeId::first();
        let enumeration = EnumSchema::new(
            enum_id,
            "Color",
            vec![
                EnumVariantSchema::new(EnumVariantId::first(), "Red").expect("variant"),
                EnumVariantSchema::new(EnumVariantId::new(2).expect("variant ID"), "Blue")
                    .expect("variant"),
            ],
        )
        .expect("enum");
        let event_id = EventTypeId::first();
        let event = EventSchema::new(
            event_id,
            "Embedded",
            RecordSchema::new(
                RecordTypeRef::Event(event_id),
                vec![FieldSchema::new(FieldId::first(), "flag", ValueType::bool()).expect("field")],
            )
            .expect("record"),
        )
        .expect("event");
        let schema = SchemaIr::new(vec![], vec![event], vec![enumeration], vec![]).expect("schema");
        let usd = CurrencyCode::new("USD").expect("currency");
        let vectors = vec![
            (ValueType::bool(), r#"{"type":"boolean"}"#),
            (
                ValueType::i64(),
                r#"{"maximum":9223372036854775807,"minimum":-9223372036854775808,"type":"integer"}"#,
            ),
            (
                ValueType::u64(),
                r#"{"maximum":18446744073709551615,"minimum":0,"type":"integer"}"#,
            ),
            (
                ValueType::decimal(DecimalSpec::new(5, 2).expect("decimal")),
                r#"{"pattern":"^-?(0|[1-9][0-9]{0,2})\\.[0-9]{2}$","type":"string","x-riffdb-decimalPrecision":5,"x-riffdb-decimalScale":2}"#,
            ),
            (
                ValueType::money(usd),
                r#"{"pattern":"^-?(0|[1-9][0-9]{0,35})\\.[0-9]{2}$","type":"string","x-riffdb-decimalPrecision":38,"x-riffdb-decimalScale":2,"x-riffdb-moneyCurrency":"USD"}"#,
            ),
            (
                ValueType::string(7).expect("string"),
                r#"{"type":"string","x-riffdb-maxUtf8Bytes":7}"#,
            ),
            (
                ValueType::bytes(8).expect("bytes"),
                r#"{"contentEncoding":"base64","type":"string","x-riffdb-maxDecodedBytes":8}"#,
            ),
            (
                ValueType::timestamp(),
                r#"{"additionalProperties":false,"properties":{"nanos":{"maximum":999999999,"minimum":0,"type":"integer"},"seconds":{"pattern":"^-?(0|[1-9][0-9]*)$","type":"string","x-riffdb-integerType":"i64"}},"required":["seconds","nanos"],"type":"object"}"#,
            ),
            (
                ValueType::date(),
                r#"{"maximum":2147483647,"minimum":-2147483648,"type":"integer"}"#,
            ),
            (
                ValueType::uuid(),
                r#"{"pattern":"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$","type":"string"}"#,
            ),
            (
                ValueType::enumeration(enum_id),
                r#"{"enum":["Red","Blue"],"type":"string"}"#,
            ),
            (
                ValueType::optional(ValueType::bool()).expect("optional"),
                r#"{"oneOf":[{"type":"boolean"},{"type":"null"}]}"#,
            ),
            (
                ValueType::list(ValueType::u64(), 3).expect("list"),
                r#"{"items":{"maximum":18446744073709551615,"minimum":0,"type":"integer"},"maxItems":3,"type":"array"}"#,
            ),
            (
                ValueType::record(RecordTypeRef::Event(event_id)),
                r#"{"additionalProperties":false,"properties":{"flag":{"type":"boolean"}},"required":["flag"],"type":"object"}"#,
            ),
        ];
        assert_eq!(
            vectors.len(),
            crate::format_registry::value_type::TAGS.len()
        );
        for (value_type, expected) in vectors {
            assert_eq!(canonical_type_schema(&value_type, &schema), expected);
        }
    }

    #[test]
    fn every_shape_registry_entry_has_an_exact_production_vector() {
        let empty = SchemaIr::new(vec![], vec![], vec![], vec![]).expect("schema");
        let field = FieldSchema::new(FieldId::first(), "flag", ValueType::bool()).expect("field");
        let entity_record = RecordSchema::new(
            RecordTypeRef::Entity(EntityTypeId::first()),
            vec![field.clone()],
        )
        .expect("entity record");
        let output = GeneratedSchemaArtifact::entity(EntityTypeId::first(), &entity_record, &empty)
            .expect("output");
        let event_record =
            RecordSchema::new(RecordTypeRef::Event(EventTypeId::first()), vec![field])
                .expect("event record");
        let event = GeneratedSchemaArtifact::event(EventTypeId::first(), &event_record, &empty)
            .expect("event output");
        assert_eq!(event.canonical_json(), output.canonical_json());

        let input_record = RecordSchema::new(
            RecordTypeRef::CommandInput(CommandId::first()),
            vec![
                FieldSchema::new(
                    FieldId::first(),
                    "flag",
                    ValueType::optional(ValueType::bool()).expect("optional"),
                )
                .expect("field"),
            ],
        )
        .expect("input record");
        let input =
            GeneratedSchemaArtifact::command_input(CommandId::first(), &input_record, &empty, None)
                .expect("input");

        let outcome_id = OutcomeId::first();
        let outcome = OutcomeSchema::new(
            CommandId::first(),
            outcome_id,
            "Done",
            RecordSchema::new(
                RecordTypeRef::CommandOutcome {
                    command_id: CommandId::first(),
                    outcome_id,
                },
                vec![],
            )
            .expect("outcome record"),
        )
        .expect("outcome");
        let outcomes =
            GeneratedSchemaArtifact::command_outcomes(CommandId::first(), &[outcome], &empty)
                .expect("outcomes");

        let mut tuple = String::new();
        projection_tuple_node(&[ValueType::bool()], &empty)
            .expect("tuple")
            .write(&mut tuple);
        let measures = RecordSchema::new(
            RecordTypeRef::ProjectionResult(ProjectionId::first()),
            vec![],
        )
        .expect("measures");
        let projection = GeneratedSchemaArtifact::projection_result(
            ProjectionId::first(),
            &[ValueType::bool()],
            &measures,
            &empty,
        )
        .expect("projection");

        let vectors = [
            (
                JsonSchemaShapeConstruction::OutputRecord,
                output.canonical_json(),
                concat!(
                    "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\",",
                    "\"additionalProperties\":false,\"properties\":{\"flag\":{",
                    "\"type\":\"boolean\"}},\"required\":[\"flag\"],\"type\":\"object\"}",
                ),
            ),
            (
                JsonSchemaShapeConstruction::CommandInput,
                input.canonical_json(),
                concat!(
                    "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\",",
                    "\"additionalProperties\":false,\"properties\":{\"flag\":{",
                    "\"default\":null,\"oneOf\":[{\"type\":\"boolean\"},{",
                    "\"type\":\"null\"}]}},\"required\":[],\"type\":\"object\"}",
                ),
            ),
            (
                JsonSchemaShapeConstruction::CommandOutcomeUnion,
                outcomes.canonical_json(),
                concat!(
                    "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\",",
                    "\"oneOf\":[{\"additionalProperties\":false,\"properties\":{",
                    "\"type\":{\"const\":\"Done\"}},\"required\":[\"type\"],",
                    "\"type\":\"object\"}]}",
                ),
            ),
            (
                JsonSchemaShapeConstruction::ProjectionTupleKey,
                tuple.as_str(),
                concat!(
                    "{\"items\":false,\"maxItems\":1,\"minItems\":1,",
                    "\"prefixItems\":[{\"type\":\"boolean\"}],\"type\":\"array\"}",
                ),
            ),
            (
                JsonSchemaShapeConstruction::ProjectionResultRow,
                projection.canonical_json(),
                concat!(
                    "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\",",
                    "\"additionalProperties\":false,\"properties\":{\"key\":{",
                    "\"items\":false,\"maxItems\":1,\"minItems\":1,",
                    "\"prefixItems\":[{\"type\":\"boolean\"}],\"type\":\"array\"},",
                    "\"measures\":{\"additionalProperties\":false,\"properties\":{},",
                    "\"required\":[],\"type\":\"object\"}},\"required\":[\"key\",",
                    "\"measures\"],\"type\":\"object\"}",
                ),
            ),
        ];
        assert_eq!(
            vectors.map(|(construction, _, _)| construction),
            JsonSchemaShapeConstruction::ALL
        );
        for (_, actual, expected) in vectors {
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn projection_count_decimal_and_money_row_is_byte_exact() {
        let projection_id = ProjectionId::first();
        let measures = RecordSchema::new(
            RecordTypeRef::ProjectionResult(projection_id),
            vec![
                FieldSchema::new(FieldId::first(), "count", ValueType::u64()).expect("count"),
                FieldSchema::new(
                    FieldId::new(2).expect("field"),
                    "decimal",
                    ValueType::decimal(DecimalSpec::new(5, 2).expect("decimal")),
                )
                .expect("decimal"),
                FieldSchema::new(
                    FieldId::new(3).expect("field"),
                    "money",
                    ValueType::money(CurrencyCode::new("USD").expect("currency")),
                )
                .expect("money"),
            ],
        )
        .expect("record");
        let schema = SchemaIr::new(vec![], vec![], vec![], vec![]).expect("schema");
        let artifact = GeneratedSchemaArtifact::projection_result(
            projection_id,
            &[ValueType::bool()],
            &measures,
            &schema,
        )
        .expect("artifact");
        assert_eq!(
            artifact.canonical_json(),
            concat!(
                "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\",",
                "\"additionalProperties\":false,\"properties\":{\"key\":{",
                "\"items\":false,\"maxItems\":1,\"minItems\":1,",
                "\"prefixItems\":[{\"type\":\"boolean\"}],\"type\":\"array\"},",
                "\"measures\":{\"additionalProperties\":false,\"properties\":{",
                "\"count\":{\"maximum\":18446744073709551615,\"minimum\":0,",
                "\"type\":\"integer\"},\"decimal\":{\"pattern\":",
                "\"^-?(0|[1-9][0-9]{0,2})\\\\.[0-9]{2}$\",\"type\":\"string\",",
                "\"x-riffdb-decimalPrecision\":5,\"x-riffdb-decimalScale\":2},",
                "\"money\":{\"pattern\":\"^-?(0|[1-9][0-9]{0,35})\\\\.[0-9]{2}$\",",
                "\"type\":\"string\",\"x-riffdb-decimalPrecision\":38,",
                "\"x-riffdb-decimalScale\":2,\"x-riffdb-moneyCurrency\":\"USD\"}},",
                "\"required\":[\"count\",\"decimal\",\"money\"],\"type\":\"object\"}},",
                "\"required\":[\"key\",\"measures\"],\"type\":\"object\"}"
            )
        );
    }

    #[test]
    fn outcome_named_type_has_an_unambiguous_discriminator() {
        let command_id = CommandId::first();
        let outcome_id = OutcomeId::first();
        let outcome = OutcomeSchema::new(
            command_id,
            outcome_id,
            "type",
            RecordSchema::new(
                RecordTypeRef::CommandOutcome {
                    command_id,
                    outcome_id,
                },
                vec![],
            )
            .expect("payload"),
        )
        .expect("outcome");
        let schema = SchemaIr::new(vec![], vec![], vec![], vec![]).expect("schema");
        let artifact = GeneratedSchemaArtifact::command_outcomes(command_id, &[outcome], &schema)
            .expect("artifact");
        assert_eq!(
            artifact.canonical_json(),
            concat!(
                "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\",",
                "\"oneOf\":[{\"additionalProperties\":false,\"properties\":{",
                "\"type\":{\"const\":\"type\"}},\"required\":[\"type\"],",
                "\"type\":\"object\"}]}"
            )
        );
    }

    #[test]
    fn schema_size_preflight_accepts_exact_limit_and_rejects_one_more_byte() {
        assert_eq!(
            checked_json_size_sum([MAX_JSON_SCHEMA_ARTIFACT_BYTES]).expect("exact limit"),
            MAX_JSON_SCHEMA_ARTIFACT_BYTES
        );
        assert!(matches!(
            checked_json_size_sum([MAX_JSON_SCHEMA_ARTIFACT_BYTES, 1]),
            Err(IrValidationError::LimitExceeded {
                kind: "generated JSON Schema",
                actual,
                maximum: MAX_JSON_SCHEMA_ARTIFACT_BYTES,
            }) if actual == MAX_JSON_SCHEMA_ARTIFACT_BYTES + 1
        ));
    }

    #[test]
    fn oversized_inline_record_amplification_rejects_during_size_preflight() {
        let event_id = EventTypeId::first();
        let event_fields = (1..=1_024)
            .map(|id| {
                FieldSchema::new(
                    FieldId::new(id).expect("field ID"),
                    format!("event_field_{id}"),
                    ValueType::bool(),
                )
                .expect("event field")
            })
            .collect();
        let event = EventSchema::new(
            event_id,
            "Amplified",
            RecordSchema::new(RecordTypeRef::Event(event_id), event_fields).expect("event payload"),
        )
        .expect("event");
        let schema = SchemaIr::new(vec![], vec![event], vec![], vec![]).expect("schema");

        let command_id = CommandId::first();
        let outcome_id = OutcomeId::first();
        let payload_fields = (1..=32)
            .map(|id| {
                FieldSchema::new(
                    FieldId::new(id).expect("field ID"),
                    format!("copy_{id}"),
                    ValueType::record(RecordTypeRef::Event(event_id)),
                )
                .expect("outcome field")
            })
            .collect();
        let outcome = OutcomeSchema::new(
            command_id,
            outcome_id,
            "Amplified",
            RecordSchema::new(
                RecordTypeRef::CommandOutcome {
                    command_id,
                    outcome_id,
                },
                payload_fields,
            )
            .expect("outcome payload"),
        )
        .expect("outcome");

        assert!(matches!(
            GeneratedSchemaArtifact::command_outcomes(command_id, &[outcome], &schema),
            Err(IrValidationError::LimitExceeded {
                kind: "generated JSON Schema",
                maximum: MAX_JSON_SCHEMA_ARTIFACT_BYTES,
                ..
            })
        ));
    }
}
