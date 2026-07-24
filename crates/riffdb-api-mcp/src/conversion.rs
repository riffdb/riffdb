//! Common checked conversion boundary shared by both MCP transports.

use std::error::Error;
use std::fmt;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use serde::Serializer;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::schema::RiffDbSchemaValidator;
use crate::{
    McpToolArguments, McpToolResult, SchemaDocument, decode_mcp_cursor, fixed_tool_registry,
};

const DEFAULT_PAGE_LIMIT: u16 = 50;
const MAX_SCHEMA_DECODE_DEPTH: usize = 32;
const UUID_PATTERN: &str = "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$";

/// A contract selection decoded from an accepted MCP input schema.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum McpContractSelection {
    /// Select the active contract.
    Active,
    /// Select one exact immutable contract version.
    Exact {
        /// Contract lineage text.
        contract_lineage: String,
        /// Nonzero contract version.
        contract_version: u64,
    },
}

/// A decoded bounded page request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct McpPageRequest {
    cursor: Option<[u8; 16]>,
    limit: u16,
}

impl McpPageRequest {
    /// Returns the opaque service cursor.
    #[must_use]
    pub const fn cursor(self) -> Option<[u8; 16]> {
        self.cursor
    }

    /// Returns the requested page limit, including the accepted default.
    #[must_use]
    pub const fn limit(self) -> u16 {
        self.limit
    }
}

/// One submitted record field identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum McpSubmittedFieldIdentity {
    /// Stable field identity used by the tagged fixed-tool representation.
    Id(u32),
    /// Source field name used by compiler-schema business JSON.
    Name(String),
}

/// One submitted record field.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct McpSubmittedField {
    /// Field identity.
    pub identity: McpSubmittedFieldIdentity,
    /// Submitted field value.
    pub value: McpSubmittedValue,
}

/// Transport-neutral submitted value produced by the common MCP decoder.
///
/// This is a presentation DTO, not a second application-service semantic type.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum McpSubmittedValue {
    /// Explicit absence.
    Null,
    /// Boolean.
    Bool(bool),
    /// Signed integer.
    I64(i64),
    /// Unsigned integer.
    U64(u64),
    /// Fixed-scale decimal.
    Decimal {
        /// Signed coefficient.
        coefficient: i128,
        /// Declared precision.
        precision: u8,
        /// Fractional scale.
        scale: u8,
    },
    /// Currency-qualified decimal.
    Money {
        /// Three-letter currency.
        currency: String,
        /// Signed coefficient.
        coefficient: i128,
        /// Declared precision.
        precision: u8,
        /// Fractional scale.
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
    /// Stable enum identity from the tagged fixed-tool representation.
    EnumIdentity {
        /// Stable enum type ID.
        type_id: u32,
        /// Stable enum variant ID.
        variant_id: u32,
    },
    /// Compiler-schema enum source name.
    ///
    /// The application transport must resolve this name against the selected
    /// command schema; it must not infer stable IDs from array positions.
    EnumName(String),
    /// Ordered list.
    List(Vec<Self>),
    /// Ordered record.
    Record(Vec<McpSubmittedField>),
}

/// Canonical padded RFC 4648 base64 presentation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct McpPresentedBytes(Vec<u8>);

impl McpPresentedBytes {
    /// Retains bytes for canonical serialization.
    #[must_use]
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        Self(bytes.into())
    }

    /// Borrows the retained bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl Serialize for McpPresentedBytes {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&STANDARD.encode(&self.0))
    }
}

/// Lowercase hexadecimal presentation of exactly 32 bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct McpPresentedHash([u8; 32]);

impl McpPresentedHash {
    /// Checks the exact hash width.
    pub fn from_slice(bytes: &[u8]) -> Result<Self, McpConversionError> {
        bytes.try_into().map(Self).map_err(|_| McpConversionError)
    }

    /// Returns the exact bytes.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0
    }
}

impl Serialize for McpPresentedHash {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&lower_hex(&self.0))
    }
}

/// Canonical lowercase hyphenated UUID presentation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct McpPresentedUuid([u8; 16]);

impl McpPresentedUuid {
    /// Retains checked network-order UUID bytes.
    #[must_use]
    pub const fn new(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Returns network-order bytes.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 16] {
        self.0
    }
}

impl Serialize for McpPresentedUuid {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format_uuid(self.0))
    }
}

/// A `u64` serialized as its canonical decimal string.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct McpPresentedU64(#[serde(serialize_with = "serialize_u64_string")] u64);

impl McpPresentedU64 {
    /// Creates canonical decimal-string presentation.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the retained integer.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// An `i64` serialized as its canonical decimal string.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct McpPresentedI64(#[serde(serialize_with = "serialize_i64_string")] i64);

impl McpPresentedI64 {
    /// Creates canonical decimal-string presentation.
    #[must_use]
    pub const fn new(value: i64) -> Self {
        Self(value)
    }

    /// Returns the retained integer.
    #[must_use]
    pub const fn get(self) -> i64 {
        self.0
    }
}

/// Canonical MCP timestamp presentation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct McpPresentedTimestamp {
    /// Seconds from the Unix epoch as canonical decimal text.
    pub seconds: McpPresentedI64,
    /// Nanosecond fraction.
    pub nanos: u32,
}

/// One canonical record field in the tagged structural value representation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct McpPresentedField {
    /// Stable nonzero field identity.
    pub field_id: u32,
    /// Canonical field value.
    pub value: McpPresentedValue,
}

/// Canonical tagged structural value emitted by fixed tools and resources.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum McpPresentedValue {
    /// Explicit absence.
    Null,
    /// Boolean.
    Bool {
        /// Boolean value.
        value: bool,
    },
    /// Signed integer.
    I64 {
        /// Canonical signed integer.
        value: McpPresentedI64,
    },
    /// Unsigned integer.
    U64 {
        /// Canonical unsigned integer.
        value: McpPresentedU64,
    },
    /// Fixed-scale decimal.
    Decimal {
        /// Declared precision.
        precision: u8,
        /// Fractional scale.
        scale: u8,
        /// Signed coefficient as canonical decimal text.
        coefficient: String,
    },
    /// Currency-qualified fixed-scale decimal.
    Money {
        /// Three-letter uppercase currency.
        currency: String,
        /// Declared precision.
        precision: u8,
        /// Fractional scale.
        scale: u8,
        /// Signed coefficient as canonical decimal text.
        coefficient: String,
    },
    /// UTF-8 string.
    String {
        /// String value.
        value: String,
    },
    /// Opaque bytes.
    Bytes {
        /// Canonically encoded bytes.
        value: McpPresentedBytes,
    },
    /// UTC timestamp.
    Timestamp {
        /// Seconds from the Unix epoch as canonical decimal text.
        seconds: McpPresentedI64,
        /// Nanosecond fraction.
        nanos: u32,
    },
    /// Days from the Unix epoch.
    Date {
        /// Signed day count.
        days_since_unix_epoch: i32,
    },
    /// UUID.
    Uuid {
        /// Canonical UUID.
        value: McpPresentedUuid,
    },
    /// Stable enumeration value.
    Enum {
        /// Stable enum type ID.
        type_id: u32,
        /// Stable enum variant ID.
        variant_id: u32,
    },
    /// Ordered list.
    List {
        /// Ordered values.
        values: Vec<Self>,
    },
    /// Stable-ID-ordered record.
    Record {
        /// Ordered fields.
        fields: Vec<McpPresentedField>,
    },
}

/// Fixed provenance selector.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum McpProvenanceSelector {
    /// Select by commit sequence.
    CommitSequence(u64),
    /// Select by provenance UUID.
    ProvenanceId([u8; 16]),
}

/// One decoded fixed-tool request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum McpFixedToolRequest {
    /// `riffdb.contract.validate`.
    ValidateContract {
        /// Bounded contract source.
        source: String,
    },
    /// `riffdb.contract.get_active`.
    GetActiveContract,
    /// `riffdb.contract.explain_command`.
    ExplainCommand {
        /// Active or exact contract selection.
        contract: McpContractSelection,
        /// Exact source command name.
        command_name: String,
    },
    /// `riffdb.contract.deploy`.
    DeployContract {
        /// Bounded candidate source.
        source: String,
        /// Optional active-version precondition.
        expected_active_version: Option<u64>,
    },
    /// `riffdb.command.get_outcome` raw identity branch.
    GetOutcomeIdentity {
        /// Exact contract lineage.
        contract_lineage: String,
        /// Exact source command name.
        command_name: String,
        /// Caller-supplied idempotency key.
        idempotency_key: String,
    },
    /// `riffdb.command.get_outcome` locator branch.
    GetOutcomeLocator {
        /// Canonical outcome resource URI.
        outcome_uri: String,
    },
    /// `riffdb.entity.get`.
    GetEntity {
        /// Active or exact contract selection.
        contract: McpContractSelection,
        /// Stable entity type ID.
        entity_type_id: u32,
        /// Canonical entity-key bytes.
        entity_key: Vec<u8>,
        /// Strictly increasing field projection.
        fields: Vec<u32>,
    },
    /// `riffdb.entity.scan_index`.
    ScanIndex {
        /// Active or exact contract selection.
        contract: McpContractSelection,
        /// Stable index ID.
        index_id: u32,
        /// Ordered index-prefix values.
        leading_components: Vec<McpSubmittedValue>,
        /// Strictly increasing field projection.
        fields: Vec<u32>,
        /// Bounded page controls.
        page: McpPageRequest,
    },
    /// `riffdb.commit.get`.
    GetCommit {
        /// Nonzero commit sequence.
        commit_sequence: u64,
    },
    /// `riffdb.commit.scan`.
    ScanCommits {
        /// Bounded page controls.
        page: McpPageRequest,
    },
    /// `riffdb.provenance.trace`.
    TraceProvenance {
        /// Exact trace selector.
        selector: McpProvenanceSelector,
    },
    /// `riffdb.projection.query`.
    QueryProjection {
        /// Active or exact contract selection.
        contract: McpContractSelection,
        /// Stable projection ID.
        projection_id: u32,
        /// Ordered group-prefix values.
        leading_components: Vec<McpSubmittedValue>,
        /// Optional required commit sequence.
        required_sequence: Option<u64>,
        /// Maximum wait in nanoseconds.
        wait_nanos: u64,
        /// Bounded page controls.
        page: McpPageRequest,
    },
    /// `riffdb.projection.status`.
    GetProjectionStatus {
        /// Active or exact contract selection.
        contract: McpContractSelection,
        /// Stable projection ID.
        projection_id: u32,
    },
    /// `riffdb.outbox.list_pending`.
    ListPendingOutboxDeliveries {
        /// Bounded page controls.
        page: McpPageRequest,
    },
    /// `riffdb.server.health`.
    Health,
}

/// Decodes one already-schema-validated fixed-tool input.
pub fn decode_fixed_tool_request(
    tag: u8,
    arguments: &McpToolArguments,
) -> Result<McpFixedToolRequest, McpConversionError> {
    match tag {
        1 => {
            let request: RawSource = arguments.deserialize().map_err(conversion)?;
            Ok(McpFixedToolRequest::ValidateContract {
                source: request.source,
            })
        }
        2 => {
            let _: RawEmpty = arguments.deserialize().map_err(conversion)?;
            Ok(McpFixedToolRequest::GetActiveContract)
        }
        3 => {
            let request: RawExplain = arguments.deserialize().map_err(conversion)?;
            Ok(McpFixedToolRequest::ExplainCommand {
                contract: request.contract.try_into()?,
                command_name: request.command_name,
            })
        }
        4 => {
            let request: RawDeploy = arguments.deserialize().map_err(conversion)?;
            Ok(McpFixedToolRequest::DeployContract {
                source: request.source,
                expected_active_version: parse_optional_u64(request.expected_active_version)?,
            })
        }
        5 => {
            let request: RawOutcome = arguments.deserialize().map_err(conversion)?;
            match request {
                RawOutcome::Identity {
                    contract_lineage,
                    command_name,
                    idempotency_key,
                } => Ok(McpFixedToolRequest::GetOutcomeIdentity {
                    contract_lineage,
                    command_name,
                    idempotency_key,
                }),
                RawOutcome::Locator { outcome_uri } => {
                    Ok(McpFixedToolRequest::GetOutcomeLocator { outcome_uri })
                }
            }
        }
        6 => {
            let request: RawGetEntity = arguments.deserialize().map_err(conversion)?;
            Ok(McpFixedToolRequest::GetEntity {
                contract: request.contract.try_into()?,
                entity_type_id: request.entity_type_id,
                entity_key: decode_base64(&request.entity_key)?,
                fields: request.fields,
            })
        }
        7 => {
            let request: RawScanIndex = arguments.deserialize().map_err(conversion)?;
            Ok(McpFixedToolRequest::ScanIndex {
                contract: request.contract.try_into()?,
                index_id: request.index_id,
                leading_components: request
                    .leading_components
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<_, _>>()?,
                fields: request.fields,
                page: request.page.try_into()?,
            })
        }
        8 => {
            let request: RawCommit = arguments.deserialize().map_err(conversion)?;
            Ok(McpFixedToolRequest::GetCommit {
                commit_sequence: parse_u64(&request.commit_sequence)?,
            })
        }
        9 => {
            let request: RawPageEnvelope = arguments.deserialize().map_err(conversion)?;
            Ok(McpFixedToolRequest::ScanCommits {
                page: request.page.try_into()?,
            })
        }
        10 => {
            let request: RawProvenance = arguments.deserialize().map_err(conversion)?;
            let selector = match request.selector {
                RawProvenanceSelector::CommitSequence { commit_sequence } => {
                    McpProvenanceSelector::CommitSequence(parse_u64(&commit_sequence)?)
                }
                RawProvenanceSelector::ProvenanceId { provenance_id } => {
                    McpProvenanceSelector::ProvenanceId(parse_uuid(&provenance_id)?)
                }
            };
            Ok(McpFixedToolRequest::TraceProvenance { selector })
        }
        11 => {
            let request: RawProjectionQuery = arguments.deserialize().map_err(conversion)?;
            Ok(McpFixedToolRequest::QueryProjection {
                contract: request.contract.try_into()?,
                projection_id: request.projection_id,
                leading_components: request
                    .leading_components
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<_, _>>()?,
                required_sequence: parse_optional_u64(request.required_sequence)?,
                wait_nanos: parse_u64(&request.wait_nanos)?,
                page: request.page.try_into()?,
            })
        }
        12 => {
            let request: RawProjectionStatus = arguments.deserialize().map_err(conversion)?;
            Ok(McpFixedToolRequest::GetProjectionStatus {
                contract: request.contract.try_into()?,
                projection_id: request.projection_id,
            })
        }
        13 => {
            let request: RawPageEnvelope = arguments.deserialize().map_err(conversion)?;
            Ok(McpFixedToolRequest::ListPendingOutboxDeliveries {
                page: request.page.try_into()?,
            })
        }
        14 => {
            let _: RawEmpty = arguments.deserialize().map_err(conversion)?;
            Ok(McpFixedToolRequest::Health)
        }
        _ => Err(McpConversionError),
    }
}

/// Decodes natural compiler-schema business JSON into name-addressed submitted values.
///
/// The input must already have passed validation against `schema`. This second
/// traversal converts representation without treating schema validation as
/// authorization or semantic materialization.
pub fn decode_dynamic_command_input(
    arguments: &McpToolArguments,
    schema: &SchemaDocument,
) -> Result<Vec<McpSubmittedField>, McpConversionError> {
    let value: Value = arguments.deserialize().map_err(conversion)?;
    let schema = Value::Object(schema.json_object());
    let McpSubmittedValue::Record(fields) = decode_schema_value(&schema, &value, 0)? else {
        return Err(McpConversionError);
    };
    Ok(fields)
}

fn decode_schema_value(
    schema: &Value,
    value: &Value,
    depth: usize,
) -> Result<McpSubmittedValue, McpConversionError> {
    if depth > MAX_SCHEMA_DECODE_DEPTH {
        return Err(McpConversionError);
    }
    let object = schema.as_object().ok_or(McpConversionError)?;
    if let Some(branches) = object.get("oneOf").and_then(Value::as_array) {
        if value.is_null() {
            return Ok(McpSubmittedValue::Null);
        }
        let branch = branches
            .iter()
            .find(|branch| {
                branch
                    .as_object()
                    .and_then(|branch| branch.get("type"))
                    .and_then(Value::as_str)
                    != Some("null")
            })
            .ok_or(McpConversionError)?;
        return decode_schema_value(branch, value, depth + 1);
    }

    match object.get("type").and_then(Value::as_str) {
        Some("boolean") => value
            .as_bool()
            .map(McpSubmittedValue::Bool)
            .ok_or(McpConversionError),
        Some("integer") => decode_schema_integer(object, value),
        Some("string") => decode_schema_string(object, value),
        Some("array") => {
            let item_schema = object.get("items").ok_or(McpConversionError)?;
            let values = value
                .as_array()
                .ok_or(McpConversionError)?
                .iter()
                .map(|value| decode_schema_value(item_schema, value, depth + 1))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(McpSubmittedValue::List(values))
        }
        Some("object") => {
            if is_timestamp_schema(object) {
                let value = value.as_object().ok_or(McpConversionError)?;
                return Ok(McpSubmittedValue::Timestamp {
                    seconds: parse_i64(
                        value
                            .get("seconds")
                            .and_then(Value::as_str)
                            .ok_or(McpConversionError)?,
                    )?,
                    nanos: value
                        .get("nanos")
                        .and_then(Value::as_u64)
                        .and_then(|value| u32::try_from(value).ok())
                        .ok_or(McpConversionError)?,
                });
            }
            let properties = object
                .get("properties")
                .and_then(Value::as_object)
                .ok_or(McpConversionError)?;
            let values = value.as_object().ok_or(McpConversionError)?;
            let mut fields = Vec::with_capacity(values.len());
            for (name, value) in values {
                let field_schema = properties.get(name).ok_or(McpConversionError)?;
                fields.push(McpSubmittedField {
                    identity: McpSubmittedFieldIdentity::Name(name.clone()),
                    value: decode_schema_value(field_schema, value, depth + 1)?,
                });
            }
            Ok(McpSubmittedValue::Record(fields))
        }
        _ => Err(McpConversionError),
    }
}

fn decode_schema_integer(
    schema: &Map<String, Value>,
    value: &Value,
) -> Result<McpSubmittedValue, McpConversionError> {
    let minimum = schema.get("minimum").and_then(Value::as_i64);
    let maximum_i64 = schema.get("maximum").and_then(Value::as_i64);
    let maximum_u64 = schema.get("maximum").and_then(Value::as_u64);
    if minimum == Some(i64::from(i32::MIN)) && maximum_i64 == Some(i64::from(i32::MAX)) {
        return value
            .as_i64()
            .and_then(|value| i32::try_from(value).ok())
            .map(McpSubmittedValue::Date)
            .ok_or(McpConversionError);
    }
    if minimum == Some(i64::MIN) && maximum_i64 == Some(i64::MAX) {
        return value
            .as_i64()
            .map(McpSubmittedValue::I64)
            .ok_or(McpConversionError);
    }
    if minimum == Some(0) && maximum_u64 == Some(u64::MAX) {
        return value
            .as_u64()
            .map(McpSubmittedValue::U64)
            .ok_or(McpConversionError);
    }
    Err(McpConversionError)
}

fn decode_schema_string(
    schema: &Map<String, Value>,
    value: &Value,
) -> Result<McpSubmittedValue, McpConversionError> {
    let text = value.as_str().ok_or(McpConversionError)?;
    if let (Some(precision), Some(scale)) = (
        schema
            .get("x-riffdb-decimalPrecision")
            .and_then(Value::as_u64),
        schema.get("x-riffdb-decimalScale").and_then(Value::as_u64),
    ) {
        let precision = u8::try_from(precision).map_err(|_| McpConversionError)?;
        let scale = u8::try_from(scale).map_err(|_| McpConversionError)?;
        let coefficient = parse_decimal_coefficient(text, scale)?;
        if let Some(currency) = schema.get("x-riffdb-moneyCurrency").and_then(Value::as_str) {
            return Ok(McpSubmittedValue::Money {
                currency: currency.to_owned(),
                coefficient,
                precision,
                scale,
            });
        }
        return Ok(McpSubmittedValue::Decimal {
            coefficient,
            precision,
            scale,
        });
    }
    if schema.get("contentEncoding").and_then(Value::as_str) == Some("base64") {
        return decode_base64(text).map(McpSubmittedValue::Bytes);
    }
    if schema.get("pattern").and_then(Value::as_str) == Some(UUID_PATTERN) {
        return parse_uuid(text).map(McpSubmittedValue::Uuid);
    }
    if schema.get("enum").is_some() {
        return Ok(McpSubmittedValue::EnumName(text.to_owned()));
    }
    if schema.get("x-riffdb-maxUtf8Bytes").is_some() {
        return Ok(McpSubmittedValue::String(text.to_owned()));
    }
    Err(McpConversionError)
}

fn is_timestamp_schema(schema: &Map<String, Value>) -> bool {
    schema
        .get("properties")
        .and_then(Value::as_object)
        .and_then(|properties| properties.get("seconds"))
        .and_then(Value::as_object)
        .and_then(|seconds| seconds.get("x-riffdb-integerType"))
        .and_then(Value::as_str)
        == Some("i64")
}

/// Closed fixed-tool result branch registry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum McpFixedResultBranch {
    /// Contract validation succeeded.
    ValidateValid,
    /// Contract validation returned diagnostics.
    ValidateInvalid,
    /// No active contract exists.
    GetActiveAbsent,
    /// Active contract metadata is present.
    GetActivePresent,
    /// Command explanation target was absent.
    ExplainNotFound,
    /// Command explanation was found.
    ExplainFound,
    /// Deployment activated a contract.
    DeployActivated,
    /// The same contract was already active.
    DeployAlreadyActive,
    /// The active-version precondition differed.
    DeployExpectedActiveVersionMismatch,
    /// Immutable bundle identity conflicted.
    DeployBundleConflict,
    /// No persisted outcome matched.
    GetOutcomeNotFound,
    /// A persisted outcome was replayed.
    GetOutcomeReplayed,
    /// Entity was absent.
    EntityNotFound,
    /// Entity was found.
    EntityFound,
    /// Index page.
    ScanIndexPage,
    /// Commit was absent.
    CommitNotFound,
    /// Commit was found.
    CommitFound,
    /// Commit scan page.
    CommitScanPage,
    /// Provenance was absent.
    ProvenanceNotFound,
    /// Provenance was found.
    ProvenanceFound,
    /// Projection query is ready.
    ProjectionReady,
    /// Projection wait timed out.
    ProjectionWaitTimedOut,
    /// Projection is degraded.
    ProjectionDegraded,
    /// Projection query was invalid.
    ProjectionInvalid,
    /// Projection status target was absent.
    ProjectionStatusNotFound,
    /// Projection status was found.
    ProjectionStatusFound,
    /// Pending-outbox page.
    OutboxPage,
    /// Pre-bootstrap health.
    HealthPreBootstrap,
    /// Authenticated health.
    HealthAuthenticated,
}

impl McpFixedResultBranch {
    const fn tag(self) -> u8 {
        match self {
            Self::ValidateValid | Self::ValidateInvalid => 1,
            Self::GetActiveAbsent | Self::GetActivePresent => 2,
            Self::ExplainNotFound | Self::ExplainFound => 3,
            Self::DeployActivated
            | Self::DeployAlreadyActive
            | Self::DeployExpectedActiveVersionMismatch
            | Self::DeployBundleConflict => 4,
            Self::GetOutcomeNotFound | Self::GetOutcomeReplayed => 5,
            Self::EntityNotFound | Self::EntityFound => 6,
            Self::ScanIndexPage => 7,
            Self::CommitNotFound | Self::CommitFound => 8,
            Self::CommitScanPage => 9,
            Self::ProvenanceNotFound | Self::ProvenanceFound => 10,
            Self::ProjectionReady
            | Self::ProjectionWaitTimedOut
            | Self::ProjectionDegraded
            | Self::ProjectionInvalid => 11,
            Self::ProjectionStatusNotFound | Self::ProjectionStatusFound => 12,
            Self::OutboxPage => 13,
            Self::HealthPreBootstrap | Self::HealthAuthenticated => 14,
        }
    }

    const fn key(self) -> &'static str {
        match self {
            Self::ValidateValid => "valid",
            Self::ValidateInvalid => "invalid",
            Self::GetActiveAbsent => "absent",
            Self::GetActivePresent => "present",
            Self::ExplainNotFound
            | Self::EntityNotFound
            | Self::CommitNotFound
            | Self::ProvenanceNotFound
            | Self::ProjectionStatusNotFound
            | Self::GetOutcomeNotFound => "not_found",
            Self::ExplainFound
            | Self::EntityFound
            | Self::CommitFound
            | Self::ProvenanceFound
            | Self::ProjectionStatusFound => "found",
            Self::DeployActivated => "activated",
            Self::DeployAlreadyActive => "already_active",
            Self::DeployExpectedActiveVersionMismatch => "expected_active_version_mismatch",
            Self::DeployBundleConflict => "bundle_conflict",
            Self::GetOutcomeReplayed => "replayed",
            Self::ScanIndexPage | Self::CommitScanPage | Self::OutboxPage => "page",
            Self::ProjectionReady => "ready",
            Self::ProjectionWaitTimedOut => "wait_timed_out",
            Self::ProjectionDegraded => "degraded",
            Self::ProjectionInvalid => "invalid",
            Self::HealthPreBootstrap => "pre_bootstrap",
            Self::HealthAuthenticated => "authenticated",
        }
    }

    const fn payload_mode(self) -> PayloadMode {
        match self {
            Self::ValidateValid
            | Self::GetActiveAbsent
            | Self::ExplainNotFound
            | Self::DeployBundleConflict
            | Self::GetOutcomeNotFound
            | Self::EntityNotFound
            | Self::CommitNotFound
            | Self::ProvenanceNotFound
            | Self::ProjectionStatusNotFound => PayloadMode::Unit,
            Self::GetOutcomeReplayed => PayloadMode::Root,
            _ => PayloadMode::Wrapped,
        }
    }
}

#[derive(Clone, Copy)]
enum PayloadMode {
    Unit,
    Wrapped,
    Root,
}

/// Opaque bounded object used as a fixed-result branch payload.
#[derive(Clone, Eq, PartialEq)]
pub struct McpFixedResultPayload(Value);

impl McpFixedResultPayload {
    /// Creates a payload from an adapter-owned serializable DTO.
    pub fn from_serializable<T: Serialize>(value: &T) -> Result<Self, McpConversionError> {
        let value = serde_json::to_value(value).map_err(conversion)?;
        if !value.is_object() {
            return Err(McpConversionError);
        }
        crate::bounded_json::encoded_len(&value, crate::MCP_OUTBOUND_MESSAGE_MAX_BYTES)
            .map_err(conversion)?;
        Ok(Self(value))
    }

    /// Creates a payload from complete UTF-8 JSON object bytes.
    pub fn from_json_bytes(bytes: &[u8]) -> Result<Self, McpConversionError> {
        if bytes.len() > crate::MCP_OUTBOUND_MESSAGE_MAX_BYTES {
            return Err(McpConversionError);
        }
        let value: Value = serde_json::from_slice(bytes).map_err(conversion)?;
        if !value.is_object() {
            return Err(McpConversionError);
        }
        crate::bounded_json::encoded_len(&value, crate::MCP_OUTBOUND_MESSAGE_MAX_BYTES)
            .map_err(conversion)?;
        Ok(Self(value))
    }
}

impl fmt::Debug for McpFixedResultPayload {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("McpFixedResultPayload([REDACTED])")
    }
}

/// Composes and immediately validates one accepted fixed-tool result branch.
pub fn compose_fixed_tool_result(
    tag: u8,
    branch: McpFixedResultBranch,
    payload: Option<McpFixedResultPayload>,
) -> Result<McpToolResult, McpConversionError> {
    if branch.tag() != tag {
        return Err(McpConversionError);
    }
    let value = match (branch.payload_mode(), payload) {
        (PayloadMode::Unit, None) if branch == McpFixedResultBranch::GetOutcomeNotFound => {
            let mut object = Map::new();
            object.insert("status".to_owned(), Value::String("not_found".to_owned()));
            Value::Object(object)
        }
        (PayloadMode::Unit, None) => {
            let mut object = Map::new();
            object.insert(branch.key().to_owned(), Value::Object(Map::new()));
            Value::Object(object)
        }
        (PayloadMode::Wrapped, Some(payload)) => {
            let mut object = Map::new();
            object.insert(branch.key().to_owned(), payload.0);
            Value::Object(object)
        }
        (PayloadMode::Root, Some(payload)) => payload.0,
        _ => return Err(McpConversionError),
    };
    let registry = fixed_tool_registry().map_err(conversion)?;
    let definition = registry
        .tools()
        .get(usize::from(tag).saturating_sub(1))
        .filter(|definition| definition.kind() == tag)
        .ok_or(McpConversionError)?;
    RiffDbSchemaValidator
        .validate(definition.result_schema(), &value)
        .map_err(|_| McpConversionError)?;
    McpToolResult::from_serializable(&value).map_err(conversion)
}

/// Closed conversion failure that contains no submitted content.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct McpConversionError;

impl fmt::Display for McpConversionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MCP conversion failed")
    }
}

impl Error for McpConversionError {}

fn conversion<T>(_: T) -> McpConversionError {
    McpConversionError
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawEmpty {}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawSource {
    source: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawExplain {
    contract: RawContractSelection,
    command_name: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawDeploy {
    source: String,
    expected_active_version: Option<String>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum RawOutcome {
    Identity {
        contract_lineage: String,
        command_name: String,
        idempotency_key: String,
    },
    Locator {
        outcome_uri: String,
    },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawGetEntity {
    contract: RawContractSelection,
    entity_type_id: u32,
    entity_key: String,
    fields: Vec<u32>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawScanIndex {
    contract: RawContractSelection,
    index_id: u32,
    leading_components: Vec<RawTaggedValue>,
    fields: Vec<u32>,
    page: RawPage,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawCommit {
    commit_sequence: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawPageEnvelope {
    page: RawPage,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawProvenance {
    selector: RawProvenanceSelector,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum RawProvenanceSelector {
    CommitSequence { commit_sequence: String },
    ProvenanceId { provenance_id: String },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawProjectionQuery {
    contract: RawContractSelection,
    projection_id: u32,
    leading_components: Vec<RawTaggedValue>,
    required_sequence: Option<String>,
    wait_nanos: String,
    page: RawPage,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawProjectionStatus {
    contract: RawContractSelection,
    projection_id: u32,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum RawContractSelection {
    Active { active: RawEmpty },
    Exact { exact: RawExactContract },
}

impl TryFrom<RawContractSelection> for McpContractSelection {
    type Error = McpConversionError;

    fn try_from(value: RawContractSelection) -> Result<Self, Self::Error> {
        match value {
            RawContractSelection::Active { active } => {
                let _ = active;
                Ok(Self::Active)
            }
            RawContractSelection::Exact { exact } => Ok(Self::Exact {
                contract_lineage: exact.contract_lineage,
                contract_version: parse_u64(&exact.contract_version)?,
            }),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawExactContract {
    contract_lineage: String,
    contract_version: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawPage {
    cursor: Option<String>,
    limit: Option<u16>,
}

impl TryFrom<RawPage> for McpPageRequest {
    type Error = McpConversionError;

    fn try_from(value: RawPage) -> Result<Self, Self::Error> {
        Ok(Self {
            cursor: value
                .cursor
                .as_deref()
                .map(decode_mcp_cursor)
                .transpose()
                .map_err(conversion)?,
            limit: value.limit.unwrap_or(DEFAULT_PAGE_LIMIT),
        })
    }
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
enum RawTaggedValue {
    Null,
    Bool {
        value: bool,
    },
    I64 {
        value: String,
    },
    U64 {
        value: String,
    },
    Decimal {
        precision: u8,
        scale: u8,
        coefficient: String,
    },
    Money {
        currency: String,
        precision: u8,
        scale: u8,
        coefficient: String,
    },
    String {
        value: String,
    },
    Bytes {
        value: String,
    },
    Timestamp {
        seconds: String,
        nanos: u32,
    },
    Date {
        days_since_unix_epoch: i32,
    },
    Uuid {
        value: String,
    },
    Enum {
        type_id: u32,
        variant_id: u32,
    },
    List {
        values: Vec<Self>,
    },
    Record {
        fields: Vec<RawTaggedField>,
    },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawTaggedField {
    field_id: u32,
    value: RawTaggedValue,
}

impl TryFrom<RawTaggedValue> for McpSubmittedValue {
    type Error = McpConversionError;

    fn try_from(value: RawTaggedValue) -> Result<Self, Self::Error> {
        match value {
            RawTaggedValue::Null => Ok(Self::Null),
            RawTaggedValue::Bool { value } => Ok(Self::Bool(value)),
            RawTaggedValue::I64 { value } => parse_i64(&value).map(Self::I64),
            RawTaggedValue::U64 { value } => parse_u64(&value).map(Self::U64),
            RawTaggedValue::Decimal {
                precision,
                scale,
                coefficient,
            } => Ok(Self::Decimal {
                coefficient: parse_i128(&coefficient)?,
                precision,
                scale,
            }),
            RawTaggedValue::Money {
                currency,
                precision,
                scale,
                coefficient,
            } => Ok(Self::Money {
                currency,
                coefficient: parse_i128(&coefficient)?,
                precision,
                scale,
            }),
            RawTaggedValue::String { value } => Ok(Self::String(value)),
            RawTaggedValue::Bytes { value } => decode_base64(&value).map(Self::Bytes),
            RawTaggedValue::Timestamp { seconds, nanos } => Ok(Self::Timestamp {
                seconds: parse_i64(&seconds)?,
                nanos,
            }),
            RawTaggedValue::Date {
                days_since_unix_epoch,
            } => Ok(Self::Date(days_since_unix_epoch)),
            RawTaggedValue::Uuid { value } => parse_uuid(&value).map(Self::Uuid),
            RawTaggedValue::Enum {
                type_id,
                variant_id,
            } => Ok(Self::EnumIdentity {
                type_id,
                variant_id,
            }),
            RawTaggedValue::List { values } => values
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()
                .map(Self::List),
            RawTaggedValue::Record { fields } => fields
                .into_iter()
                .map(|field| {
                    Ok(McpSubmittedField {
                        identity: McpSubmittedFieldIdentity::Id(field.field_id),
                        value: field.value.try_into()?,
                    })
                })
                .collect::<Result<_, _>>()
                .map(Self::Record),
        }
    }
}

fn parse_optional_u64(value: Option<String>) -> Result<Option<u64>, McpConversionError> {
    value.as_deref().map(parse_u64).transpose()
}

fn parse_u64(value: &str) -> Result<u64, McpConversionError> {
    value.parse().map_err(|_| McpConversionError)
}

fn parse_i64(value: &str) -> Result<i64, McpConversionError> {
    value.parse().map_err(|_| McpConversionError)
}

fn parse_i128(value: &str) -> Result<i128, McpConversionError> {
    value.parse().map_err(|_| McpConversionError)
}

fn parse_decimal_coefficient(value: &str, scale: u8) -> Result<i128, McpConversionError> {
    if scale == 0 {
        return parse_i128(value);
    }
    let mut digits = String::with_capacity(value.len().saturating_sub(1));
    for byte in value.bytes() {
        if byte != b'.' {
            digits.push(char::from(byte));
        }
    }
    parse_i128(&digits)
}

fn decode_base64(value: &str) -> Result<Vec<u8>, McpConversionError> {
    STANDARD.decode(value).map_err(|_| McpConversionError)
}

fn parse_uuid(value: &str) -> Result<[u8; 16], McpConversionError> {
    if value.len() != 36
        || value.as_bytes().get(8) != Some(&b'-')
        || value.as_bytes().get(13) != Some(&b'-')
        || value.as_bytes().get(18) != Some(&b'-')
        || value.as_bytes().get(23) != Some(&b'-')
    {
        return Err(McpConversionError);
    }
    let mut bytes = [0_u8; 16];
    let mut output = 0;
    let mut high = None;
    for byte in value.bytes() {
        if byte == b'-' {
            continue;
        }
        let nibble = match byte {
            b'0'..=b'9' => byte - b'0',
            b'a'..=b'f' => byte - b'a' + 10,
            _ => return Err(McpConversionError),
        };
        if let Some(high) = high.take() {
            let target = bytes.get_mut(output).ok_or(McpConversionError)?;
            *target = high << 4 | nibble;
            output += 1;
        } else {
            high = Some(nibble);
        }
    }
    if output != bytes.len() || high.is_some() {
        return Err(McpConversionError);
    }
    Ok(bytes)
}

fn lower_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn format_uuid(bytes: [u8; 16]) -> String {
    let hex = lower_hex(&bytes);
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

fn serialize_u64_string<S>(value: &u64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&value.to_string())
}

fn serialize_i64_string<S>(value: &i64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&value.to_string())
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;

    fn arguments(value: Value) -> McpToolArguments {
        McpToolArguments::from_validated(value)
    }

    #[test]
    fn fixed_request_decoding_preserves_tagged_values_and_defaults() {
        let request = decode_fixed_tool_request(
            7,
            &arguments(json!({
                "contract": {"active": {}},
                "index_id": 1,
                "leading_components": [
                    {"kind": "decimal", "precision": 4, "scale": 2, "coefficient": "123"},
                    {"kind": "bytes", "value": "AQ=="}
                ],
                "fields": [],
                "page": {}
            })),
        )
        .expect("request");
        let McpFixedToolRequest::ScanIndex {
            leading_components,
            page,
            ..
        } = request
        else {
            panic!("wrong request");
        };
        assert_eq!(page.limit(), DEFAULT_PAGE_LIMIT);
        assert_eq!(
            leading_components,
            vec![
                McpSubmittedValue::Decimal {
                    coefficient: 123,
                    precision: 4,
                    scale: 2
                },
                McpSubmittedValue::Bytes(vec![1])
            ]
        );
    }

    #[test]
    fn dynamic_business_json_is_decoded_by_compiler_schema() {
        let schema = SchemaDocument::from_public_parts(
            "riffdb.command-input/test/v1",
            riffdb_types::hash_schema(
                br#"{"$schema":"https://json-schema.org/draft/2020-12/schema","additionalProperties":false,"properties":{"amount":{"pattern":"^-?(0|[1-9][0-9]{0,2})\\.[0-9]{2}$","type":"string","x-riffdb-decimalPrecision":5,"x-riffdb-decimalScale":2},"id":{"pattern":"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$","type":"string"}},"required":["id","amount"],"type":"object"}"#,
            )
            .as_bytes(),
            r#"{"$schema":"https://json-schema.org/draft/2020-12/schema","additionalProperties":false,"properties":{"amount":{"pattern":"^-?(0|[1-9][0-9]{0,2})\\.[0-9]{2}$","type":"string","x-riffdb-decimalPrecision":5,"x-riffdb-decimalScale":2},"id":{"pattern":"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$","type":"string"}},"required":["id","amount"],"type":"object"}"#,
        )
        .expect("schema");
        let decoded = decode_dynamic_command_input(
            &arguments(json!({
                "amount": "12.34",
                "id": "01900000-0000-7000-8000-000000000001"
            })),
            &schema,
        )
        .expect("decoded");
        assert_eq!(decoded.len(), 2);
        assert!(matches!(
            decoded[0].value,
            McpSubmittedValue::Decimal {
                coefficient: 1234,
                precision: 5,
                scale: 2
            }
        ));
        assert!(matches!(decoded[1].value, McpSubmittedValue::Uuid(_)));
    }

    #[test]
    fn fixed_result_composer_rejects_tag_and_payload_mismatch() {
        assert_eq!(
            compose_fixed_tool_result(2, McpFixedResultBranch::ValidateValid, None),
            Err(McpConversionError)
        );
        assert_eq!(
            compose_fixed_tool_result(
                1,
                McpFixedResultBranch::ValidateValid,
                Some(McpFixedResultPayload::from_serializable(&json!({})).expect("empty payload"))
            ),
            Err(McpConversionError)
        );
        compose_fixed_tool_result(1, McpFixedResultBranch::ValidateValid, None)
            .expect("valid result");
    }
}
