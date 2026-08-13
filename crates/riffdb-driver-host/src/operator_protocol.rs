use std::fmt;
use std::io;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Exact operator-driver protocol generation.
pub const OPERATOR_DRIVER_PROTOCOL_VERSION: u32 = 1;
/// One reimport page plus bounded JSON framing.
pub const MAX_OPERATOR_DRIVER_FRAME_BYTES: usize = 4 * 1_024 * 1_024 + 256 * 1_024;
const MAX_TEXT_BYTES: usize = 4_096;

/// Closed operator-only local request family.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[allow(missing_docs)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum OperatorDriverRequest {
    /// Binds the local client to one configured campaign and manifest.
    Handshake {
        request_id: String,
        protocol_version: u32,
        database: String,
        campaign_id: String,
        portability_manifest_hash: String,
    },
    /// Starts or exactly replays the configured campaign.
    Start {
        request_id: String,
        canonical_export_manifest_json: String,
        canonical_export_receipt_json: String,
        maximum_attempts: u32,
    },
    /// Applies one exact source export page.
    ApplyPage {
        request_id: String,
        export_operation_id: String,
        page_number: u64,
        canonical_json_lines: Vec<String>,
        next_cursor_base64: Option<String>,
        class_complete: bool,
        operation_complete: bool,
        page_hash_hex: String,
        maximum_attempts: u32,
    },
    /// Observes one protected checkpoint.
    Status { request_id: String },
    /// Cancels or observes one terminal campaign.
    Cancel {
        request_id: String,
        maximum_attempts: u32,
    },
}

/// Public-safe operator progress returned to every target language.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[allow(missing_docs)]
#[serde(deny_unknown_fields)]
pub struct OperatorReimportOperation {
    pub campaign_id: String,
    pub contract_lineage: String,
    pub scope: String,
    pub portability_manifest_hash: String,
    pub export_manifest_hash: String,
    pub export_receipt_hash: String,
    pub source_database_id: String,
    pub target_database_id: String,
    pub source_rows: String,
    pub source_pages: String,
    pub next_page: String,
    pub rows_applied: String,
    pub phase: String,
    pub failure: Option<String>,
    pub canonical_reimport_receipt_json: Option<String>,
    pub reimport_receipt_hash: Option<String>,
}

/// Closed operator-only response family.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[allow(missing_docs)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum OperatorDriverResponse {
    Handshake {
        request_id: String,
        protocol_version: u32,
        driver_identity: String,
        database: String,
        campaign_id: String,
        portability_manifest_hash: String,
    },
    Operation {
        request_id: String,
        operation: Box<OperatorReimportOperation>,
    },
    NotFound {
        request_id: String,
    },
    Error {
        request_id: Option<String>,
        code: String,
        category: String,
        message: String,
        retryability: String,
        recovery_action: String,
        outcome_uncertain: bool,
    },
}

/// Length-delimited canonical JSON codec for the private operator socket.
pub struct OperatorFrameCodec;

impl OperatorFrameCodec {
    /// Validates and encodes one canonical request body.
    pub fn encode_request(
        request: &OperatorDriverRequest,
    ) -> Result<Vec<u8>, OperatorProtocolError> {
        validate_request(request)?;
        encode(request)
    }

    /// Decodes and validates one canonical request body.
    pub fn decode_request(frame: &[u8]) -> Result<OperatorDriverRequest, OperatorProtocolError> {
        let request = decode(frame)?;
        validate_request(&request)?;
        Ok(request)
    }

    /// Validates and encodes one canonical response body.
    pub fn encode_response(
        response: &OperatorDriverResponse,
    ) -> Result<Vec<u8>, OperatorProtocolError> {
        validate_response(response)?;
        encode(response)
    }

    /// Decodes and validates one canonical response body.
    pub fn decode_response(frame: &[u8]) -> Result<OperatorDriverResponse, OperatorProtocolError> {
        let response = decode(frame)?;
        validate_response(&response)?;
        Ok(response)
    }

    /// Reads one bounded request from a length-delimited stream.
    pub async fn read_request<R: AsyncRead + Unpin>(
        reader: &mut R,
    ) -> Result<OperatorDriverRequest, OperatorProtocolError> {
        Self::decode_request(&read_frame(reader).await?)
    }

    /// Writes one bounded response to a length-delimited stream.
    pub async fn write_response<W: AsyncWrite + Unpin>(
        writer: &mut W,
        response: &OperatorDriverResponse,
    ) -> Result<(), OperatorProtocolError> {
        write_frame(writer, &Self::encode_response(response)?).await
    }
}

fn validate_request(request: &OperatorDriverRequest) -> Result<(), OperatorProtocolError> {
    let (request_id, attempts) = match request {
        OperatorDriverRequest::Handshake {
            request_id,
            protocol_version,
            database,
            campaign_id,
            portability_manifest_hash,
        } => {
            if *protocol_version != OPERATOR_DRIVER_PROTOCOL_VERSION
                || !short(database)
                || !uuid(campaign_id)
                || !hash(portability_manifest_hash)
            {
                return Err(OperatorProtocolError::Invalid);
            }
            (request_id, None)
        }
        OperatorDriverRequest::Start {
            request_id,
            canonical_export_manifest_json,
            canonical_export_receipt_json,
            maximum_attempts,
        } => {
            if !canonical_json(canonical_export_manifest_json, 256 * 1_024)
                || !canonical_json(canonical_export_receipt_json, 256 * 1_024)
            {
                return Err(OperatorProtocolError::Invalid);
            }
            (request_id, Some(*maximum_attempts))
        }
        OperatorDriverRequest::ApplyPage {
            request_id,
            export_operation_id,
            page_number,
            canonical_json_lines,
            next_cursor_base64,
            class_complete,
            operation_complete,
            page_hash_hex,
            maximum_attempts,
        } => {
            let bytes = canonical_json_lines
                .iter()
                .try_fold(0usize, |total, line| total.checked_add(line.len() + 1))
                .ok_or(OperatorProtocolError::TooLarge)?;
            if !uuid(export_operation_id)
                || *page_number == 0
                || canonical_json_lines.is_empty()
                || canonical_json_lines.len() > 500
                || bytes > 4 * 1_024 * 1_024
                || canonical_json_lines
                    .iter()
                    .any(|line| !canonical_json(line, 64 * 1_024))
                || !hash(page_hash_hex)
                || *operation_complete != next_cursor_base64.is_none()
                || (*operation_complete && !*class_complete)
                || next_cursor_base64
                    .as_ref()
                    .is_some_and(|value| value.is_empty() || value.len() > 1_024)
            {
                return Err(OperatorProtocolError::Invalid);
            }
            (request_id, Some(*maximum_attempts))
        }
        OperatorDriverRequest::Status { request_id } => (request_id, None),
        OperatorDriverRequest::Cancel {
            request_id,
            maximum_attempts,
        } => (request_id, Some(*maximum_attempts)),
    };
    if !short(request_id) || attempts.is_some_and(|value| !(1..=10).contains(&value)) {
        return Err(OperatorProtocolError::Invalid);
    }
    Ok(())
}

fn validate_response(response: &OperatorDriverResponse) -> Result<(), OperatorProtocolError> {
    let request_id = match response {
        OperatorDriverResponse::Handshake {
            request_id,
            protocol_version,
            driver_identity,
            database,
            campaign_id,
            portability_manifest_hash,
        } => {
            if *protocol_version != OPERATOR_DRIVER_PROTOCOL_VERSION
                || !short(driver_identity)
                || !short(database)
                || !uuid(campaign_id)
                || !hash(portability_manifest_hash)
            {
                return Err(OperatorProtocolError::Invalid);
            }
            request_id
        }
        OperatorDriverResponse::Operation {
            request_id,
            operation,
        } => {
            validate_operation(operation)?;
            request_id
        }
        OperatorDriverResponse::NotFound { request_id } => request_id,
        OperatorDriverResponse::Error {
            request_id,
            code,
            category,
            message,
            retryability,
            recovery_action,
            ..
        } => {
            if request_id.as_ref().is_some_and(|value| !short(value))
                || !short(code)
                || !short(category)
                || !short(message)
                || !short(retryability)
                || !short(recovery_action)
            {
                return Err(OperatorProtocolError::Invalid);
            }
            return Ok(());
        }
    };
    if !short(request_id) {
        return Err(OperatorProtocolError::Invalid);
    }
    Ok(())
}

fn validate_operation(value: &OperatorReimportOperation) -> Result<(), OperatorProtocolError> {
    if !uuid(&value.campaign_id)
        || !short(&value.contract_lineage)
        || !hash(&value.portability_manifest_hash)
        || !hash(&value.export_manifest_hash)
        || !hash(&value.export_receipt_hash)
        || !uuid(&value.source_database_id)
        || !uuid(&value.target_database_id)
        || value
            .canonical_reimport_receipt_json
            .as_ref()
            .is_some_and(|document| !canonical_json_with_newline(document, 256 * 1_024))
        || value
            .reimport_receipt_hash
            .as_ref()
            .is_some_and(|hash_value| !hash(hash_value))
    {
        return Err(OperatorProtocolError::Invalid);
    }
    Ok(())
}

fn short(value: &str) -> bool {
    !value.is_empty() && value.len() <= MAX_TEXT_BYTES && !value.contains(['\n', '\r', '\0'])
}
fn uuid(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(i, b)| {
            if [8, 13, 18, 23].contains(&i) {
                b == b'-'
            } else {
                b.is_ascii_digit() || (b'a'..=b'f').contains(&b)
            }
        })
        && value.as_bytes().get(14) == Some(&b'7')
        && value
            .as_bytes()
            .get(19)
            .is_some_and(|byte| matches!(byte, b'8' | b'9' | b'a' | b'b'))
}
fn hash(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn canonical_json(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && value.starts_with('{')
        && value.ends_with('}')
        && !value.contains(['\n', '\r'])
}
fn canonical_json_with_newline(value: &str, maximum: usize) -> bool {
    value.len() <= maximum
        && value
            .strip_suffix('\n')
            .is_some_and(|body| canonical_json(body, maximum - 1))
}

fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, OperatorProtocolError> {
    let bytes = serde_json::to_vec(value).map_err(|_| OperatorProtocolError::Invalid)?;
    if bytes.len() > MAX_OPERATOR_DRIVER_FRAME_BYTES {
        Err(OperatorProtocolError::TooLarge)
    } else {
        Ok(bytes)
    }
}
fn decode<T: for<'de> Deserialize<'de> + Serialize>(
    frame: &[u8],
) -> Result<T, OperatorProtocolError> {
    if frame.is_empty() || frame.len() > MAX_OPERATOR_DRIVER_FRAME_BYTES {
        return Err(OperatorProtocolError::TooLarge);
    }
    let value = serde_json::from_slice(frame).map_err(|_| OperatorProtocolError::Invalid)?;
    if serde_json::to_vec(&value).ok().as_deref() != Some(frame) {
        return Err(OperatorProtocolError::NonCanonical);
    }
    Ok(value)
}
async fn read_frame<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> Result<Vec<u8>, OperatorProtocolError> {
    let length = reader.read_u32().await.map_err(OperatorProtocolError::Io)? as usize;
    if length == 0 || length > MAX_OPERATOR_DRIVER_FRAME_BYTES {
        return Err(OperatorProtocolError::TooLarge);
    }
    let mut frame = vec![0; length];
    reader
        .read_exact(&mut frame)
        .await
        .map_err(OperatorProtocolError::Io)?;
    Ok(frame)
}
async fn write_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    frame: &[u8],
) -> Result<(), OperatorProtocolError> {
    let length = u32::try_from(frame.len()).map_err(|_| OperatorProtocolError::TooLarge)?;
    writer
        .write_u32(length)
        .await
        .map_err(OperatorProtocolError::Io)?;
    writer
        .write_all(frame)
        .await
        .map_err(OperatorProtocolError::Io)?;
    writer.flush().await.map_err(OperatorProtocolError::Io)
}

/// Closed local operator-protocol failure.
#[derive(Debug)]
pub enum OperatorProtocolError {
    /// The frame or nested document exceeded its exact ceiling.
    TooLarge,
    /// A decoded request or response violated its closed schema.
    Invalid,
    /// JSON bytes had an alternate encoding of the decoded value.
    NonCanonical,
    /// The private local stream failed.
    Io(io::Error),
}
impl fmt::Display for OperatorProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("operator driver protocol failed closed")
    }
}
impl std::error::Error for OperatorProtocolError {}
