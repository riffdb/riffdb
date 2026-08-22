use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::io;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use riffdb_types::decode_canonical_value;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Exact alpha driver protocol generation.
pub const DRIVER_PROTOCOL_VERSION: u32 = 3;
pub(crate) const DRIVER_PROTOCOL_VERSION_V1: u32 = 1;
pub(crate) const DRIVER_PROTOCOL_VERSION_V2: u32 = 2;
/// Hard bound for one complete local request or response body.
pub const MAX_DRIVER_FRAME_BYTES: usize = 1_048_576;
const MAX_COLLECTION_ITEMS: usize = 4_096;
const MAX_VALUE_DEPTH: usize = 32;

/// A closed target-language request. No variant can carry credentials,
/// endpoints, raw protobuf, or arbitrary remote method names.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum DriverRequest {
    /// Proves the exact local and deployed application identity before use.
    Handshake {
        /// Locally unique request identifier.
        request_id: String,
        /// Exact supported protocol generation.
        protocol_version: u32,
        /// Exact application lock identity.
        application_manifest_hash: String,
        /// Exact generated operation-catalog digest.
        operation_catalog_hash: String,
        /// Exact active contract lineage.
        contract_lineage: String,
        /// Exact active contract version.
        contract_version: u64,
        /// Exact compiled contract bundle identity.
        contract_bundle_hash: String,
        /// Exact application value registry identity.
        value_registry_hash: String,
        /// Exact structured-error registry identity.
        error_registry_hash: String,
        /// Host-selected database alias expected by the generated binding.
        database: String,
        /// Exact symbolic application role.
        role: String,
        /// Exact role-definition hash.
        role_definition_hash: String,
        /// Exact host-configured verified remote identity digest.
        remote_identity_hash: String,
    },
    /// Invokes one generated application operation.
    Invoke {
        /// Locally unique request identifier.
        request_id: String,
        /// Generated operation name from the exact catalog.
        operation: String,
        /// Exact generated input schema digest.
        input_schema_hash: String,
        /// Name-addressed typed values.
        input: BTreeMap<String, DriverValue>,
        /// Bounded transport and consistency controls.
        options: InvokeOptions,
    },
    /// Executes a bounded collection of one exact generated command.
    Batch {
        /// Locally unique batch request identifier.
        request_id: String,
        /// Generated command name from the exact catalog.
        operation: String,
        /// Exact generated input schema digest.
        input_schema_hash: String,
        /// Independently idempotent name-addressed command inputs.
        items: Vec<BTreeMap<String, DriverValue>>,
        /// Bounded independently in-flight item count.
        concurrency: u32,
        /// Already completed contiguous input prefix.
        checkpoint: u32,
        /// Per-item bounded transport and deadline controls.
        options: InvokeOptions,
    },
    /// Requests cancellation of an in-flight local request.
    Cancel {
        /// Locally unique cancellation request identifier.
        request_id: String,
        /// Exact earlier request identifier.
        target_request_id: String,
    },
}

/// Bounded application-call controls owned by the Rust host.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InvokeOptions {
    /// Caller deadline relative to host receipt. Zero is invalid.
    pub deadline_millis: u64,
    /// Maximum transport submissions, including the first.
    pub maximum_attempts: u32,
    /// Optional read-after-commit frontier.
    pub read_after_commit: Option<u64>,
    /// Optional opaque generated-operation cursor.
    pub cursor: Option<String>,
    /// Generated query accepts the exact compiler-sealed positional arm.
    #[serde(default)]
    pub accept_compact_result: bool,
    /// Generated query accepts canonical packed columns.
    #[serde(default)]
    pub accept_packed_result: bool,
}

/// Closed application value registry used by every target language.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(
    tag = "type",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum DriverValue {
    /// Explicit null.
    Null,
    /// Boolean.
    Bool(bool),
    /// Signed 64-bit integer represented as decimal text.
    I64(String),
    /// Unsigned 64-bit integer represented as decimal text.
    U64(String),
    /// Exact string.
    String(String),
    /// Canonical UUID string.
    Uuid(String),
    /// Contract enum variant name.
    Enum(String),
    /// Standard padded Base64 bytes.
    Bytes(String),
    /// Days since Unix epoch represented as decimal text.
    Date(String),
    /// UTC timestamp with integral seconds and nanoseconds.
    Timestamp(DriverTimestamp),
    /// Exact fixed-scale decimal.
    Decimal(DriverDecimal),
    /// Currency-qualified exact decimal.
    Money(DriverMoney),
    /// Canonical binary32 vector encoded as exact component bit patterns.
    Vector(DriverVector),
    /// Ordered bounded values.
    List(Vec<Self>),
    /// Name-addressed bounded record.
    Record(BTreeMap<String, Self>),
}

/// Timestamp carriage without floating point.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DriverTimestamp {
    /// Whole Unix seconds as decimal text.
    pub seconds: String,
    /// Nanosecond fraction.
    pub nanos: u32,
}

/// Decimal carriage without floating point.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DriverDecimal {
    /// Minimal big-endian two's-complement coefficient as standard Base64.
    pub coefficient: String,
    /// Fractional scale.
    pub scale: u32,
    /// Optional declared precision.
    pub precision: Option<u32>,
}

/// Exact currency-qualified decimal.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DriverMoney {
    /// Three-letter currency code.
    pub currency: String,
    /// Exact amount.
    pub amount: DriverDecimal,
}

/// Exact bounded vector carriage without JSON floating-point ambiguity.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DriverVector {
    /// IEEE 754 binary32 component bit patterns in declaration order.
    pub component_bits: Vec<u32>,
}

/// Closed target-language response.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum DriverResponse {
    /// Successful exact handshake.
    Handshake {
        /// Matching request identifier.
        request_id: String,
        /// Exact protocol generation.
        protocol_version: u32,
        /// Exact driver build identity.
        driver_identity: String,
        /// Exact application lock identity.
        application_manifest_hash: String,
        /// Exact catalog digest.
        operation_catalog_hash: String,
        /// Exact active contract lineage.
        contract_lineage: String,
        /// Exact active contract version.
        contract_version: u64,
        /// Exact compiled contract bundle identity.
        contract_bundle_hash: String,
        /// Host-selected database alias.
        database: String,
        /// Exact symbolic application role.
        role: String,
        /// Exact role-definition hash.
        role_definition_hash: String,
        /// Exact host-configured verified remote identity digest.
        remote_identity_hash: String,
    },
    /// Completed application operation.
    Result {
        /// Matching request identifier.
        request_id: String,
        /// Declared result value.
        value: DriverValue,
        /// Application snapshot/commit head when meaningful.
        application_head: Option<u64>,
        /// Optional opaque continuation cursor.
        cursor: Option<String>,
        /// True when a command returned a durable prior outcome.
        replayed: bool,
    },
    /// Completed compiler-sealed named query without per-row record maps.
    CompactQueryResult {
        /// Matching request identifier.
        request_id: String,
        /// Exact declared result outcome.
        outcome: String,
        /// Exact top-level result field name.
        result_name: String,
        /// Exact contract entity name.
        entity: String,
        /// Compiler-sealed field names in positional order.
        fields: Vec<String>,
        /// Bounded positional rows.
        rows: Vec<Vec<DriverValue>>,
        /// Application snapshot frontier.
        application_head: u64,
        /// Optional opaque continuation cursor.
        cursor: Option<String>,
    },
    /// Completed compiler-sealed named query as canonical packed columns.
    PackedQueryResult {
        /// Matching request identifier.
        request_id: String,
        /// Exact declared result outcome.
        outcome: String,
        /// Exact top-level result field name.
        result_name: String,
        /// Exact contract entity name.
        entity: String,
        /// Compiler-sealed field names in positional order.
        fields: Vec<String>,
        /// Bounded row count.
        row_count: u32,
        /// Canonical columns encoded as base64 plus checked offsets.
        columns: Vec<DriverPackedColumn>,
        /// Application snapshot frontier.
        application_head: u64,
        /// Optional opaque continuation cursor.
        cursor: Option<String>,
    },
    /// Completed bounded command batch with independently typed items.
    BatchResult {
        /// Matching batch request identifier.
        request_id: String,
        /// Input-ordered newly attempted items.
        items: Vec<DriverBatchItem>,
        /// Largest contiguous successful input prefix.
        checkpoint: u32,
        /// Total input item count including the initial checkpoint.
        total: u32,
    },
    /// Structured safe public failure.
    Error {
        /// Matching request identifier when one was safely decoded.
        request_id: Option<String>,
        /// Stable public error code.
        code: String,
        /// Stable category.
        category: String,
        /// Symbolic application operation when known.
        operation: Option<String>,
        /// Checked caller-visible symbolic path from the public error envelope.
        symbol_path: Vec<String>,
        /// Exact contract lineage when safely authorized for the caller.
        contract_lineage: Option<String>,
        /// Exact contract version paired with `contract_lineage`.
        contract_version: Option<u64>,
        /// Opaque safe end-to-end request trace identity.
        trace_id: Option<String>,
        /// Opaque public incident identity for operator correlation.
        incident_id: Option<String>,
        /// Safe bounded public message.
        message: String,
        /// Stable retry classification.
        retryability: String,
        /// Stable recovery action.
        recovery_action: String,
        /// True when a command may have committed despite cancellation/failure.
        outcome_uncertain: bool,
    },
    /// Explicit cancellation disposition.
    Cancelled {
        /// Cancellation request identifier.
        request_id: String,
        /// Target request identifier.
        target_request_id: String,
        /// Whether the target had already reached a terminal response.
        terminal: bool,
        /// Whether a command outcome remains uncertain.
        outcome_uncertain: bool,
    },
}

/// One canonical packed result column carried by the language-driver protocol.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DriverPackedColumn {
    /// Canonical RFC 4648 base64 cell bytes.
    pub data: String,
    /// Row boundaries into decoded data.
    pub offsets: Vec<u32>,
}

/// One independently completed generated command batch item.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DriverBatchItem {
    /// Stable zero-based original input position.
    pub index: u32,
    /// Complete terminal result or structured failure.
    pub outcome: DriverBatchOutcome,
}

/// Closed per-item command completion.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum DriverBatchOutcome {
    /// Declared command result.
    Result {
        /// Declared result value.
        value: DriverValue,
        /// Durable commit sequence when one was assigned.
        commit_sequence: Option<u64>,
        /// Opaque durable outcome locator.
        outcome_uri: Option<String>,
        /// Whether the durable outcome was replayed.
        replayed: bool,
    },
    /// Structured application/transport failure.
    Error {
        /// Stable public error code.
        code: String,
        /// Stable category.
        category: String,
        /// Checked source-level application operation when known.
        operation: Option<String>,
        /// Checked caller-visible symbolic path.
        symbol_path: Vec<String>,
        /// Exact contract lineage when safely authorized.
        contract_lineage: Option<String>,
        /// Exact contract version paired with `contract_lineage`.
        contract_version: Option<u64>,
        /// Opaque end-to-end trace identity.
        trace_id: Option<String>,
        /// Opaque public incident identity.
        incident_id: Option<String>,
        /// Safe registry-owned message.
        message: String,
        /// Stable recovery action.
        recovery_action: String,
        /// Whether the item may have committed.
        outcome_uncertain: bool,
    },
}

/// Closed framing or shape failure. It contains no submitted values.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProtocolError {
    /// Frame length is zero or exceeds the fixed bound.
    InvalidFrameLength,
    /// JSON is malformed, non-canonical in shape, or has unknown fields.
    InvalidMessage,
    /// A string, collection, recursion, or invocation bound was exceeded.
    InvalidBounds,
    /// The transport ended before a complete frame arrived.
    Truncated,
    /// Local transport I/O failed.
    Io,
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidFrameLength => "driver frame length is invalid",
            Self::InvalidMessage => "driver message is invalid",
            Self::InvalidBounds => "driver message bounds are invalid",
            Self::Truncated => "driver frame is truncated",
            Self::Io => "driver transport is unavailable",
        })
    }
}

impl std::error::Error for ProtocolError {}

/// Four-byte big-endian length framing with a fixed allocation ceiling.
pub struct FrameCodec;

impl FrameCodec {
    /// Encodes one checked request frame.
    pub fn encode_request(request: &DriverRequest) -> Result<Vec<u8>, ProtocolError> {
        validate_request(request)?;
        encode(request)
    }

    /// Decodes one complete request frame, including its length prefix.
    pub fn decode_request(frame: &[u8]) -> Result<DriverRequest, ProtocolError> {
        let body = framed_body(frame)?;
        let request = serde_json::from_slice(body).map_err(|_| ProtocolError::InvalidMessage)?;
        validate_request(&request)?;
        Ok(request)
    }

    /// Encodes one checked response frame.
    pub fn encode_response(response: &DriverResponse) -> Result<Vec<u8>, ProtocolError> {
        validate_response(response)?;
        encode(response)
    }

    /// Decodes one complete response frame, including its length prefix.
    pub fn decode_response(frame: &[u8]) -> Result<DriverResponse, ProtocolError> {
        let body = framed_body(frame)?;
        let response = serde_json::from_slice(body).map_err(|_| ProtocolError::InvalidMessage)?;
        validate_response(&response)?;
        Ok(response)
    }

    /// Reads one bounded request from an async local stream.
    pub async fn read_request<R: AsyncRead + Unpin>(
        reader: &mut R,
    ) -> Result<DriverRequest, ProtocolError> {
        let body = read_body(reader).await?;
        let request = serde_json::from_slice(&body).map_err(|_| ProtocolError::InvalidMessage)?;
        validate_request(&request)?;
        Ok(request)
    }

    /// Writes one bounded response to an async local stream.
    pub async fn write_response<W: AsyncWrite + Unpin>(
        writer: &mut W,
        response: &DriverResponse,
    ) -> Result<(), ProtocolError> {
        let frame = Self::encode_response(response)?;
        writer
            .write_all(&frame)
            .await
            .map_err(|_| ProtocolError::Io)
    }
}

fn encode<T: Serialize>(message: &T) -> Result<Vec<u8>, ProtocolError> {
    let body = serde_json::to_vec(message).map_err(|_| ProtocolError::InvalidMessage)?;
    if body.is_empty() || body.len() > MAX_DRIVER_FRAME_BYTES {
        return Err(ProtocolError::InvalidFrameLength);
    }
    let length = u32::try_from(body.len()).map_err(|_| ProtocolError::InvalidFrameLength)?;
    let mut frame = Vec::with_capacity(body.len() + 4);
    frame.extend_from_slice(&length.to_be_bytes());
    frame.extend_from_slice(&body);
    Ok(frame)
}

fn framed_body(frame: &[u8]) -> Result<&[u8], ProtocolError> {
    let prefix: [u8; 4] = frame
        .get(..4)
        .ok_or(ProtocolError::Truncated)?
        .try_into()
        .map_err(|_| ProtocolError::Truncated)?;
    let length = usize::try_from(u32::from_be_bytes(prefix))
        .map_err(|_| ProtocolError::InvalidFrameLength)?;
    if length == 0 || length > MAX_DRIVER_FRAME_BYTES {
        return Err(ProtocolError::InvalidFrameLength);
    }
    if frame.len() != length + 4 {
        return Err(ProtocolError::Truncated);
    }
    Ok(&frame[4..])
}

async fn read_body<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Vec<u8>, ProtocolError> {
    let mut prefix = [0_u8; 4];
    reader
        .read_exact(&mut prefix)
        .await
        .map_err(map_read_error)?;
    let length = usize::try_from(u32::from_be_bytes(prefix))
        .map_err(|_| ProtocolError::InvalidFrameLength)?;
    if length == 0 || length > MAX_DRIVER_FRAME_BYTES {
        return Err(ProtocolError::InvalidFrameLength);
    }
    let mut body = vec![0_u8; length];
    reader.read_exact(&mut body).await.map_err(map_read_error)?;
    Ok(body)
}

fn map_read_error(error: io::Error) -> ProtocolError {
    if error.kind() == io::ErrorKind::UnexpectedEof {
        ProtocolError::Truncated
    } else {
        ProtocolError::Io
    }
}

fn validate_request(request: &DriverRequest) -> Result<(), ProtocolError> {
    match request {
        DriverRequest::Handshake {
            request_id,
            protocol_version,
            application_manifest_hash,
            operation_catalog_hash,
            contract_lineage,
            contract_version,
            contract_bundle_hash,
            value_registry_hash,
            error_registry_hash,
            database,
            role,
            role_definition_hash,
            remote_identity_hash,
        } => {
            valid_id(request_id)?;
            if !matches!(
                *protocol_version,
                DRIVER_PROTOCOL_VERSION_V1 | DRIVER_PROTOCOL_VERSION_V2 | DRIVER_PROTOCOL_VERSION
            ) || !is_hash(application_manifest_hash)
                || !is_hash(operation_catalog_hash)
                || contract_lineage.is_empty()
                || contract_lineage.len() > 256
                || *contract_version == 0
                || !is_hash(contract_bundle_hash)
                || !is_hash(value_registry_hash)
                || !is_hash(error_registry_hash)
                || database.is_empty()
                || database.len() > 64
                || role.is_empty()
                || role.len() > 256
                || !is_hash(role_definition_hash)
                || !is_hash(remote_identity_hash)
            {
                return Err(ProtocolError::InvalidBounds);
            }
        }
        DriverRequest::Invoke {
            request_id,
            operation,
            input_schema_hash,
            input,
            options,
        } => {
            valid_id(request_id)?;
            if operation.is_empty()
                || operation.len() > 256
                || !is_hash(input_schema_hash)
                || input.len() > MAX_COLLECTION_ITEMS
                || !(1..=10).contains(&options.maximum_attempts)
                || options.deadline_millis == 0
                || options.deadline_millis > 300_000
                || options.read_after_commit == Some(0)
                || (options.accept_packed_result && !options.accept_compact_result)
                || options
                    .cursor
                    .as_ref()
                    .is_some_and(|value| value.len() > 16_384)
            {
                return Err(ProtocolError::InvalidBounds);
            }
            validate_values(input.values(), 0)?;
        }
        DriverRequest::Batch {
            request_id,
            operation,
            input_schema_hash,
            items,
            concurrency,
            checkpoint,
            options,
        } => {
            valid_id(request_id)?;
            if operation.is_empty()
                || operation.len() > 256
                || !is_hash(input_schema_hash)
                || items.is_empty()
                || items.len() > MAX_COLLECTION_ITEMS
                || !(1..=384).contains(concurrency)
                || usize::try_from(*checkpoint).map_or(true, |value| value > items.len())
                || !(1..=10).contains(&options.maximum_attempts)
                || options.deadline_millis == 0
                || options.deadline_millis > 300_000
                || options.read_after_commit.is_some()
                || options.cursor.is_some()
                || options.accept_compact_result
                || options.accept_packed_result
            {
                return Err(ProtocolError::InvalidBounds);
            }
            for item in items {
                if item.is_empty() || item.len() > MAX_COLLECTION_ITEMS {
                    return Err(ProtocolError::InvalidBounds);
                }
                validate_values(item.values(), 0)?;
            }
        }
        DriverRequest::Cancel {
            request_id,
            target_request_id,
        } => {
            valid_id(request_id)?;
            valid_id(target_request_id)?;
            if request_id == target_request_id {
                return Err(ProtocolError::InvalidBounds);
            }
        }
    }
    Ok(())
}

fn validate_response(response: &DriverResponse) -> Result<(), ProtocolError> {
    match response {
        DriverResponse::Handshake {
            request_id,
            protocol_version,
            driver_identity,
            application_manifest_hash,
            operation_catalog_hash,
            contract_lineage,
            contract_version,
            contract_bundle_hash,
            database,
            role,
            role_definition_hash,
            remote_identity_hash,
        } => {
            valid_id(request_id)?;
            if !matches!(
                *protocol_version,
                DRIVER_PROTOCOL_VERSION_V1 | DRIVER_PROTOCOL_VERSION_V2 | DRIVER_PROTOCOL_VERSION
            ) || driver_identity.is_empty()
                || driver_identity.len() > 128
                || !is_hash(application_manifest_hash)
                || !is_hash(operation_catalog_hash)
                || contract_lineage.is_empty()
                || contract_lineage.len() > 256
                || *contract_version == 0
                || !is_hash(contract_bundle_hash)
                || database.is_empty()
                || database.len() > 64
                || role.is_empty()
                || role.len() > 256
                || !is_hash(role_definition_hash)
                || !is_hash(remote_identity_hash)
            {
                return Err(ProtocolError::InvalidBounds);
            }
        }
        DriverResponse::Result {
            request_id,
            value,
            cursor,
            ..
        } => {
            valid_id(request_id)?;
            validate_values(std::iter::once(value), 0)?;
            if cursor.as_ref().is_some_and(|value| value.len() > 16_384) {
                return Err(ProtocolError::InvalidBounds);
            }
        }
        DriverResponse::CompactQueryResult {
            request_id,
            outcome,
            result_name,
            entity,
            fields,
            rows,
            cursor,
            ..
        } => {
            valid_id(request_id)?;
            if !is_public_symbol(outcome)
                || !is_public_symbol(result_name)
                || !is_public_symbol(entity)
                || fields.is_empty()
                || fields.len() > MAX_COLLECTION_ITEMS
                || rows.len() > MAX_COLLECTION_ITEMS
                || fields.iter().any(|field| !is_public_symbol(field))
                || fields.iter().collect::<BTreeSet<_>>().len() != fields.len()
                || rows.iter().any(|row| row.len() != fields.len())
                || cursor.as_ref().is_some_and(|value| value.len() > 16_384)
            {
                return Err(ProtocolError::InvalidBounds);
            }
            for row in rows {
                validate_values(row.iter(), 0)?;
            }
        }
        DriverResponse::PackedQueryResult {
            request_id,
            outcome,
            result_name,
            entity,
            fields,
            row_count,
            columns,
            cursor,
            ..
        } => {
            valid_id(request_id)?;
            let rows = usize::try_from(*row_count).map_err(|_| ProtocolError::InvalidBounds)?;
            if !is_public_symbol(outcome)
                || !is_public_symbol(result_name)
                || !is_public_symbol(entity)
                || fields.is_empty()
                || fields.len() > MAX_COLLECTION_ITEMS
                || rows > MAX_COLLECTION_ITEMS
                || columns.len() != fields.len()
                || fields.iter().any(|field| !is_public_symbol(field))
                || fields.iter().collect::<BTreeSet<_>>().len() != fields.len()
                || cursor.as_ref().is_some_and(|value| value.len() > 16_384)
            {
                return Err(ProtocolError::InvalidBounds);
            }
            for column in columns {
                let data = BASE64
                    .decode(column.data.as_bytes())
                    .map_err(|_| ProtocolError::InvalidBounds)?;
                if column.offsets.len() != rows.saturating_add(1)
                    || column.offsets.first().copied() != Some(0)
                    || column.offsets.last().copied().map(|value| value as usize)
                        != Some(data.len())
                {
                    return Err(ProtocolError::InvalidBounds);
                }
                for pair in column.offsets.windows(2) {
                    let start = pair[0] as usize;
                    let end = pair[1] as usize;
                    if start > end
                        || end > data.len()
                        || decode_canonical_value(&data[start..end]).is_err()
                    {
                        return Err(ProtocolError::InvalidBounds);
                    }
                }
            }
        }
        DriverResponse::BatchResult {
            request_id,
            items,
            checkpoint,
            total,
        } => {
            valid_id(request_id)?;
            if items.len() > MAX_COLLECTION_ITEMS || *total == 0 || *checkpoint > *total {
                return Err(ProtocolError::InvalidBounds);
            }
            for item in items {
                if item.index >= *total {
                    return Err(ProtocolError::InvalidBounds);
                }
                match &item.outcome {
                    DriverBatchOutcome::Result {
                        value, outcome_uri, ..
                    } => {
                        validate_values(std::iter::once(value), 0)?;
                        if outcome_uri
                            .as_ref()
                            .is_some_and(|value| value.len() > 16_384)
                        {
                            return Err(ProtocolError::InvalidBounds);
                        }
                    }
                    DriverBatchOutcome::Error {
                        code,
                        category,
                        operation,
                        symbol_path,
                        contract_lineage,
                        contract_version,
                        trace_id,
                        incident_id,
                        message,
                        recovery_action,
                        ..
                    } => {
                        if code.is_empty()
                            || code.len() > 64
                            || category.is_empty()
                            || category.len() > 64
                            || operation
                                .as_ref()
                                .is_some_and(|value| !is_public_symbol(value))
                            || symbol_path.len() > 16
                            || symbol_path.iter().any(|value| !is_public_symbol(value))
                            || contract_lineage
                                .as_ref()
                                .is_some_and(|value| !is_public_symbol(value))
                            || contract_lineage.is_some() != contract_version.is_some()
                            || contract_version == &Some(0)
                            || trace_id
                                .as_ref()
                                .is_some_and(|value| valid_id(value).is_err())
                            || incident_id
                                .as_ref()
                                .is_some_and(|value| valid_id(value).is_err())
                            || message.is_empty()
                            || message.len() > 4_096
                            || recovery_action.len() > 64
                        {
                            return Err(ProtocolError::InvalidBounds);
                        }
                    }
                }
            }
        }
        DriverResponse::Error {
            request_id,
            code,
            category,
            operation,
            symbol_path,
            contract_lineage,
            contract_version,
            trace_id,
            incident_id,
            message,
            retryability,
            recovery_action,
            ..
        } => {
            if let Some(id) = request_id {
                valid_id(id)?;
            }
            if code.is_empty()
                || code.len() > 64
                || category.is_empty()
                || category.len() > 64
                || operation
                    .as_ref()
                    .is_some_and(|value| !is_public_symbol(value))
                || symbol_path.len() > 16
                || symbol_path.iter().any(|value| !is_public_symbol(value))
                || contract_lineage
                    .as_ref()
                    .is_some_and(|value| !is_public_symbol(value))
                || contract_lineage.is_some() != contract_version.is_some()
                || contract_version == &Some(0)
                || trace_id
                    .as_ref()
                    .is_some_and(|value| valid_id(value).is_err())
                || incident_id
                    .as_ref()
                    .is_some_and(|value| valid_id(value).is_err())
                || message.is_empty()
                || message.len() > 4_096
                || retryability.len() > 64
                || recovery_action.len() > 64
            {
                return Err(ProtocolError::InvalidBounds);
            }
        }
        DriverResponse::Cancelled {
            request_id,
            target_request_id,
            ..
        } => {
            valid_id(request_id)?;
            valid_id(target_request_id)?;
        }
    }
    Ok(())
}

fn valid_id(value: &str) -> Result<(), ProtocolError> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(ProtocolError::InvalidBounds);
    }
    Ok(())
}

fn is_hash(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn is_public_symbol(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn validate_values<'a>(
    values: impl IntoIterator<Item = &'a DriverValue>,
    depth: usize,
) -> Result<(), ProtocolError> {
    if depth > MAX_VALUE_DEPTH {
        return Err(ProtocolError::InvalidBounds);
    }
    for value in values {
        match value {
            DriverValue::String(value) | DriverValue::Bytes(value) if value.len() > 262_144 => {
                return Err(ProtocolError::InvalidBounds);
            }
            DriverValue::I64(value) if value.len() > 20 || value.parse::<i64>().is_err() => {
                return Err(ProtocolError::InvalidBounds);
            }
            DriverValue::U64(value) if value.len() > 20 || value.parse::<u64>().is_err() => {
                return Err(ProtocolError::InvalidBounds);
            }
            DriverValue::Date(value) if value.len() > 11 || value.parse::<i32>().is_err() => {
                return Err(ProtocolError::InvalidBounds);
            }
            DriverValue::Uuid(value)
                if value.len() != 36
                    || value.bytes().enumerate().any(|(index, byte)| {
                        if matches!(index, 8 | 13 | 18 | 23) {
                            byte != b'-'
                        } else {
                            !byte.is_ascii_hexdigit() || byte.is_ascii_uppercase()
                        }
                    }) =>
            {
                return Err(ProtocolError::InvalidBounds);
            }
            DriverValue::Enum(value) if !is_public_symbol(value) => {
                return Err(ProtocolError::InvalidBounds);
            }
            DriverValue::List(items) if items.len() > MAX_COLLECTION_ITEMS => {
                return Err(ProtocolError::InvalidBounds);
            }
            DriverValue::List(items) => validate_values(items, depth + 1)?,
            DriverValue::Record(fields)
                if fields.len() > MAX_COLLECTION_ITEMS
                    || fields
                        .keys()
                        .any(|name| name.is_empty() || name.len() > 256) =>
            {
                return Err(ProtocolError::InvalidBounds);
            }
            DriverValue::Record(fields) => validate_values(fields.values(), depth + 1)?,
            DriverValue::Money(money) if money.currency.len() != 3 => {
                return Err(ProtocolError::InvalidBounds);
            }
            DriverValue::Timestamp(timestamp)
                if timestamp.seconds.len() > 32 || timestamp.nanos >= 1_000_000_000 =>
            {
                return Err(ProtocolError::InvalidBounds);
            }
            DriverValue::Decimal(decimal) if decimal.coefficient.len() > 1_366 => {
                return Err(ProtocolError::InvalidBounds);
            }
            DriverValue::Vector(vector)
                if riffdb_types::CanonicalVector::new(
                    vector
                        .component_bits
                        .iter()
                        .copied()
                        .map(f32::from_bits)
                        .collect(),
                )
                .is_err() =>
            {
                return Err(ProtocolError::InvalidBounds);
            }
            _ => {}
        }
    }
    Ok(())
}
