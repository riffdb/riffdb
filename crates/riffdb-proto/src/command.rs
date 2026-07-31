//! Bounded structural validation for the phase-one Execute wire messages.

use std::error::Error;
use std::fmt;

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use prost::Message;
use riffdb_types::{
    CommitSequence, ContractVersion, MAX_ACTOR_ID_BYTES, MAX_CONTRACT_LINEAGE_BYTES, ProvenanceId,
    RequestId,
};

use crate::v1;
use crate::value::{MAX_PROTOCOL_NAME_BYTES, validate_value};
use crate::wire::{self, PreflightError};

/// Default maximum encoded unary command request size.
pub const MAX_EXECUTE_REQUEST_BYTES: usize = 1024 * 1024;

/// Default maximum encoded unary command response size.
pub const MAX_EXECUTE_RESPONSE_BYTES: usize = 4 * 1024 * 1024;

/// Maximum accepted byte length of a provenance resource URI.
pub const MAX_PROVENANCE_URI_BYTES: usize = 2 * 1024;

/// Maximum accepted byte length of a durability-mode identifier.
pub const MAX_DURABILITY_MODE_BYTES: usize = 64;

/// Exact maximum byte length of a canonical outcome resource locator.
pub const MAX_OUTCOME_RESOURCE_LOCATOR_BYTES: usize = 1_745;

/// Decodes and structurally validates a bounded Execute request.
pub fn decode_execute_request(input: &[u8]) -> Result<v1::ExecuteCommandRequest, ExecuteWireError> {
    if input.len() > MAX_EXECUTE_REQUEST_BYTES {
        return Err(ExecuteWireError::MessageTooLarge);
    }
    preflight_result(wire::execute_request(input))?;
    let request = v1::ExecuteCommandRequest::decode(input)
        .map_err(|_| ExecuteWireError::MalformedEncoding)?;
    validate_execute_request(&request)?;
    Ok(request)
}

/// Applies API-neutral structural checks to an Execute request.
pub fn validate_execute_request(
    request: &v1::ExecuteCommandRequest,
) -> Result<(), ExecuteWireError> {
    let request_id: [u8; 16] = request
        .request_id
        .as_slice()
        .try_into()
        .map_err(|_| ExecuteWireError::InvalidRequestId)?;
    RequestId::from_bytes(request_id).map_err(|_| ExecuteWireError::InvalidRequestId)?;
    if request
        .expected_contract_version
        .is_some_and(|version| ContractVersion::new(version).is_none())
    {
        return Err(ExecuteWireError::InvalidContractVersion);
    }
    validate_protocol_name(&request.command_name)?;
    validate_value(
        request
            .input
            .as_ref()
            .ok_or(ExecuteWireError::MissingValue)?,
    )
    .map_err(|_| ExecuteWireError::InvalidValue)?;
    if request.encoded_len() > MAX_EXECUTE_REQUEST_BYTES {
        return Err(ExecuteWireError::MessageTooLarge);
    }
    Ok(())
}

/// Decodes and structurally validates a bounded Execute response.
pub fn decode_execute_response(
    input: &[u8],
) -> Result<v1::ExecuteCommandResponse, ExecuteWireError> {
    if input.len() > MAX_EXECUTE_RESPONSE_BYTES {
        return Err(ExecuteWireError::MessageTooLarge);
    }
    preflight_result(wire::execute_response(input))?;
    let response = v1::ExecuteCommandResponse::decode(input)
        .map_err(|_| ExecuteWireError::MalformedEncoding)?;
    validate_execute_response(&response)?;
    Ok(response)
}

/// Applies public structural checks to a successful Execute response.
pub fn validate_execute_response(
    response: &v1::ExecuteCommandResponse,
) -> Result<(), ExecuteWireError> {
    let journaled = match v1::execute_command_response::CompletionStatus::try_from(response.status)
    {
        Ok(v1::execute_command_response::CompletionStatus::Committed)
        | Ok(v1::execute_command_response::CompletionStatus::Replayed) => true,
        Ok(v1::execute_command_response::CompletionStatus::ExecutedReadOnly) => false,
        Ok(v1::execute_command_response::CompletionStatus::Unspecified) | Err(_) => {
            return Err(ExecuteWireError::InvalidCompletionStatus);
        }
    };
    if journaled {
        CommitSequence::new(response.commit_sequence)
            .ok_or(ExecuteWireError::InvalidCommitSequence)?;
        validate_provenance_uri(&response.provenance_uri)?;
        if !matches!(
            response.durability_mode.as_str(),
            "sync" | "group" | "memory"
        ) {
            return Err(ExecuteWireError::InvalidDurabilityMode);
        }
        if let Some(locator) = response.outcome_uri.as_deref() {
            validate_outcome_resource_locator(locator)?;
        }
    } else if response.commit_sequence != 0
        || !response.provenance_uri.is_empty()
        || !response.durability_mode.is_empty()
        || response.outcome_uri.is_some()
    {
        return Err(ExecuteWireError::InvalidReadOnlySentinel);
    }
    ContractVersion::new(response.contract_version)
        .ok_or(ExecuteWireError::InvalidContractVersion)?;
    if response.plan_hash.len() != 32 {
        return Err(ExecuteWireError::InvalidPlanHash);
    }
    validate_protocol_name(&response.outcome_type)?;
    validate_value(
        response
            .outcome
            .as_ref()
            .ok_or(ExecuteWireError::MissingValue)?,
    )
    .map_err(|_| ExecuteWireError::InvalidValue)?;
    if response.encoded_len() > MAX_EXECUTE_RESPONSE_BYTES {
        return Err(ExecuteWireError::MessageTooLarge);
    }
    Ok(())
}

/// Validates the complete canonical public shape of an outcome resource locator.
///
/// This check is deliberately structural. It does not grant access, resolve a
/// durable identity, or expose any decoded locator component.
pub fn validate_outcome_resource_locator(uri: &str) -> Result<(), ExecuteWireError> {
    const PREFIX: &str = "riffdb://outcome/";
    if uri.len() > MAX_OUTCOME_RESOURCE_LOCATOR_BYTES {
        return Err(ExecuteWireError::InvalidOutcomeUri);
    }
    let path = uri
        .strip_prefix(PREFIX)
        .ok_or(ExecuteWireError::InvalidOutcomeUri)?;
    let mut segments = path.split('/');
    let principal = segments.next().ok_or(ExecuteWireError::InvalidOutcomeUri)?;
    let lineage = segments.next().ok_or(ExecuteWireError::InvalidOutcomeUri)?;
    let command_id = segments.next().ok_or(ExecuteWireError::InvalidOutcomeUri)?;
    let tool_name = segments.next().ok_or(ExecuteWireError::InvalidOutcomeUri)?;
    let digest = segments.next().ok_or(ExecuteWireError::InvalidOutcomeUri)?;
    if segments.next().is_some() {
        return Err(ExecuteWireError::InvalidOutcomeUri);
    }

    decode_percent_encoded_id(principal, MAX_ACTOR_ID_BYTES)?;
    let lineage = decode_percent_encoded_id(lineage, MAX_CONTRACT_LINEAGE_BYTES)?;
    if command_id.starts_with('0')
        || command_id
            .parse::<u32>()
            .ok()
            .is_none_or(|value| value == 0)
    {
        return Err(ExecuteWireError::InvalidOutcomeUri);
    }
    validate_mcp_command_tool_name(tool_name, &lineage)?;
    if digest.len() != 50 {
        return Err(ExecuteWireError::InvalidOutcomeUri);
    }
    let tuple = URL_SAFE_NO_PAD
        .decode(digest)
        .map_err(|_| ExecuteWireError::InvalidOutcomeUri)?;
    if tuple.len() != 37
        || tuple[0] != 1
        || u32::from_be_bytes(
            tuple[1..5]
                .try_into()
                .map_err(|_| ExecuteWireError::InvalidOutcomeUri)?,
        ) == 0
        || URL_SAFE_NO_PAD.encode(&tuple) != digest
    {
        return Err(ExecuteWireError::InvalidOutcomeUri);
    }
    Ok(())
}

fn decode_percent_encoded_id(value: &str, maximum: usize) -> Result<Vec<u8>, ExecuteWireError> {
    let mut decoded = Vec::with_capacity(value.len().min(maximum));
    let bytes = value.as_bytes();
    let mut index = 0usize;
    while index < bytes.len() {
        match bytes[index] {
            byte if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') => {
                decoded.push(byte);
                index += 1;
            }
            b'%' if index + 2 < bytes.len() => {
                let high = uppercase_hex(bytes[index + 1])?;
                let low = uppercase_hex(bytes[index + 2])?;
                let byte = (high << 4) | low;
                if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
                    return Err(ExecuteWireError::InvalidOutcomeUri);
                }
                decoded.push(byte);
                index += 3;
            }
            _ => return Err(ExecuteWireError::InvalidOutcomeUri),
        }
        if decoded.len() > maximum {
            return Err(ExecuteWireError::InvalidOutcomeUri);
        }
    }
    if decoded.is_empty() || std::str::from_utf8(&decoded).is_err() {
        return Err(ExecuteWireError::InvalidOutcomeUri);
    }
    Ok(decoded)
}

fn uppercase_hex(byte: u8) -> Result<u8, ExecuteWireError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(ExecuteWireError::InvalidOutcomeUri),
    }
}

fn validate_mcp_command_tool_name(value: &str, lineage: &[u8]) -> Result<(), ExecuteWireError> {
    if !lineage.is_ascii() {
        return Err(ExecuteWireError::InvalidOutcomeUri);
    }
    let normalized = lineage
        .iter()
        .copied()
        .map(|byte| byte.to_ascii_lowercase())
        .collect::<Vec<_>>();
    let mut prefix = b"riffdb_cmd_".to_vec();
    prefix.extend_from_slice(&normalized);
    prefix.push(b'_');
    let command = value
        .as_bytes()
        .strip_prefix(prefix.as_slice())
        .ok_or(ExecuteWireError::InvalidOutcomeUri)?;
    let command = std::str::from_utf8(command).map_err(|_| ExecuteWireError::InvalidOutcomeUri)?;
    if value.len() > 128 || !valid_mcp_segment(command) {
        return Err(ExecuteWireError::InvalidOutcomeUri);
    }
    Ok(())
}

fn valid_mcp_segment(value: &str) -> bool {
    value.as_bytes().first().is_some_and(u8::is_ascii_lowercase)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

fn preflight_result(result: Result<(), PreflightError>) -> Result<(), ExecuteWireError> {
    match result {
        Ok(()) => Ok(()),
        Err(PreflightError::Malformed) => Err(ExecuteWireError::MalformedEncoding),
        Err(PreflightError::LimitExceeded) => Err(ExecuteWireError::PreflightLimitExceeded),
    }
}

fn validate_protocol_name(name: &str) -> Result<(), ExecuteWireError> {
    if name.is_empty() || name.len() > MAX_PROTOCOL_NAME_BYTES {
        return Err(ExecuteWireError::InvalidProtocolName);
    }
    Ok(())
}

pub(crate) fn validate_provenance_uri(uri: &str) -> Result<(), ExecuteWireError> {
    const PREFIX: &str = "riffdb://provenance/";
    if uri.len() > MAX_PROVENANCE_URI_BYTES {
        return Err(ExecuteWireError::InvalidProvenanceUri);
    }
    let uuid = uri
        .strip_prefix(PREFIX)
        .ok_or(ExecuteWireError::InvalidProvenanceUri)?;
    if uuid.len() != 36 {
        return Err(ExecuteWireError::InvalidProvenanceUri);
    }

    let mut bytes = [0; 16];
    let mut nibble_index = 0;
    for (index, byte) in uuid.bytes().enumerate() {
        if matches!(index, 8 | 13 | 18 | 23) {
            if byte != b'-' {
                return Err(ExecuteWireError::InvalidProvenanceUri);
            }
            continue;
        }
        let nibble = match byte {
            b'0'..=b'9' => byte - b'0',
            b'a'..=b'f' => byte - b'a' + 10,
            _ => return Err(ExecuteWireError::InvalidProvenanceUri),
        };
        let target = &mut bytes[nibble_index / 2];
        if nibble_index % 2 == 0 {
            *target = nibble << 4;
        } else {
            *target |= nibble;
        }
        nibble_index += 1;
    }
    if nibble_index != 32 {
        return Err(ExecuteWireError::InvalidProvenanceUri);
    }
    ProvenanceId::from_bytes(bytes)
        .map(|_| ())
        .map_err(|_| ExecuteWireError::InvalidProvenanceUri)
}

/// A bounded, non-secret Execute message validation failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecuteWireError {
    /// The encoded message exceeds its configured default ceiling.
    MessageTooLarge,
    /// Protobuf decoding failed.
    MalformedEncoding,
    /// A nested wire length, item count, or depth exceeds its pre-allocation limit.
    PreflightLimitExceeded,
    /// The request identifier is not an exact UUIDv7.
    InvalidRequestId,
    /// A contract version is the unassigned zero sentinel.
    InvalidContractVersion,
    /// A command or outcome name is empty or too long.
    InvalidProtocolName,
    /// A required input or outcome value is absent.
    MissingValue,
    /// A nested public value fails structural validation.
    InvalidValue,
    /// The completion status is unspecified or unknown.
    InvalidCompletionStatus,
    /// A committed response carries the unassigned zero sequence sentinel.
    InvalidCommitSequence,
    /// The plan hash is not exactly 32 bytes.
    InvalidPlanHash,
    /// The provenance URI is absent, oversized, or not in the RiffDB namespace.
    InvalidProvenanceUri,
    /// The optional outcome locator is not the exact canonical v1 resource URI.
    InvalidOutcomeUri,
    /// The durability-mode identifier is absent or malformed.
    InvalidDurabilityMode,
    /// A read-only result does not carry the exact zero/empty journal sentinels.
    InvalidReadOnlySentinel,
}

impl fmt::Display for ExecuteWireError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Execute wire message failed structural validation")
    }
}

impl Error for ExecuteWireError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::canonical_value_to_proto;
    use riffdb_types::CanonicalValue;

    fn request_id() -> Vec<u8> {
        vec![
            0x01, 0x9b, 0xf6, 0xaa, 0xa6, 0x40, 0x7d, 0xe6, 0x89, 0xc9, 0x8a, 0x7f, 0x70, 0xbb,
            0xbd, 0x23,
        ]
    }

    #[test]
    fn execute_request_preserves_absent_expected_version() {
        let request = v1::ExecuteCommandRequest {
            request_id: request_id(),
            command_name: "budget.reserve".to_owned(),
            expected_contract_version: None,
            input: Some(canonical_value_to_proto(&CanonicalValue::Null).expect("valid value")),
        };
        validate_execute_request(&request).expect("valid request");
        let decoded = decode_execute_request(&request.encode_to_vec()).expect("valid encoding");
        assert_eq!(decoded.expected_contract_version, None);
    }

    #[test]
    fn execute_request_rejects_non_v7_request_id() {
        let request = v1::ExecuteCommandRequest {
            request_id: vec![0; 16],
            command_name: "budget.reserve".to_owned(),
            expected_contract_version: Some(1),
            input: Some(canonical_value_to_proto(&CanonicalValue::Null).expect("valid value")),
        };
        assert_eq!(
            validate_execute_request(&request),
            Err(ExecuteWireError::InvalidRequestId)
        );
    }

    #[test]
    fn execute_request_rejects_zero_expected_contract_version() {
        let request = v1::ExecuteCommandRequest {
            request_id: request_id(),
            command_name: "budget.reserve".to_owned(),
            expected_contract_version: Some(0),
            input: Some(canonical_value_to_proto(&CanonicalValue::Null).expect("valid value")),
        };
        assert_eq!(
            validate_execute_request(&request),
            Err(ExecuteWireError::InvalidContractVersion)
        );
        assert_eq!(
            decode_execute_request(&request.encode_to_vec()),
            Err(ExecuteWireError::InvalidContractVersion)
        );
    }

    #[test]
    fn successful_response_requires_all_integrity_fields() {
        let response = v1::ExecuteCommandResponse {
            status: v1::execute_command_response::CompletionStatus::Committed as i32,
            commit_sequence: 1,
            contract_version: 2,
            plan_hash: vec![7; 32],
            outcome_type: "Reserved".to_owned(),
            outcome: Some(
                canonical_value_to_proto(&CanonicalValue::Bool(true)).expect("valid value"),
            ),
            provenance_uri: "riffdb://provenance/019bf6aa-a640-7de6-89c9-8a7f70bbbd23".to_owned(),
            durability_mode: "sync".to_owned(),
            outcome_uri: None,
        };
        validate_execute_response(&response).expect("valid response");

        let mut zero_sequence = response.clone();
        zero_sequence.commit_sequence = 0;
        assert_eq!(
            validate_execute_response(&zero_sequence),
            Err(ExecuteWireError::InvalidCommitSequence)
        );
        assert_eq!(
            decode_execute_response(&zero_sequence.encode_to_vec()),
            Err(ExecuteWireError::InvalidCommitSequence)
        );

        let mut zero_version = response.clone();
        zero_version.contract_version = 0;
        assert_eq!(
            validate_execute_response(&zero_version),
            Err(ExecuteWireError::InvalidContractVersion)
        );
        assert_eq!(
            decode_execute_response(&zero_version.encode_to_vec()),
            Err(ExecuteWireError::InvalidContractVersion)
        );

        let mut invalid = response;
        invalid.plan_hash.pop();
        assert_eq!(
            validate_execute_response(&invalid),
            Err(ExecuteWireError::InvalidPlanHash)
        );
    }

    #[test]
    fn provenance_uri_requires_a_canonical_uuidv7_resource() {
        assert!(
            validate_provenance_uri("riffdb://provenance/019bf6aa-a640-7de6-89c9-8a7f70bbbd23")
                .is_ok()
        );
        for invalid in [
            "riffdb://provenance/",
            "riffdb://provenance/019bf6aa-a640-7de6-89c9-8a7f70bbbd2",
            "riffdb://provenance/019BF6AA-A640-7DE6-89C9-8A7F70BBBD23",
            "https://example.test/019bf6aa-a640-7de6-89c9-8a7f70bbbd23",
        ] {
            assert_eq!(
                validate_provenance_uri(invalid),
                Err(ExecuteWireError::InvalidProvenanceUri)
            );
        }
    }

    #[test]
    fn response_accepts_only_specified_durability_modes() {
        let mut response = v1::ExecuteCommandResponse {
            status: v1::execute_command_response::CompletionStatus::Committed as i32,
            commit_sequence: 1,
            contract_version: 2,
            plan_hash: vec![7; 32],
            outcome_type: "Reserved".to_owned(),
            outcome: Some(canonical_value_to_proto(&CanonicalValue::Null).expect("valid outcome")),
            provenance_uri: "riffdb://provenance/019bf6aa-a640-7de6-89c9-8a7f70bbbd23".to_owned(),
            durability_mode: String::new(),
            outcome_uri: None,
        };

        for mode in ["sync", "group", "memory"] {
            response.durability_mode = mode.to_owned();
            validate_execute_response(&response).expect("specified durability mode");
        }
        response.durability_mode = "eventual".to_owned();
        assert_eq!(
            validate_execute_response(&response),
            Err(ExecuteWireError::InvalidDurabilityMode)
        );
    }
}
