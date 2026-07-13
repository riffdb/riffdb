//! Bounded structural validation for the phase-one Execute wire messages.

use std::error::Error;
use std::fmt;

use prost::Message;
use riffdb_types::{ProvenanceId, RequestId};

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
    match v1::execute_command_response::CompletionStatus::try_from(response.status) {
        Ok(v1::execute_command_response::CompletionStatus::Committed)
        | Ok(v1::execute_command_response::CompletionStatus::Replayed) => {}
        Ok(v1::execute_command_response::CompletionStatus::Unspecified) | Err(_) => {
            return Err(ExecuteWireError::InvalidCompletionStatus);
        }
    }
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
    validate_provenance_uri(&response.provenance_uri)?;
    if !matches!(
        response.durability_mode.as_str(),
        "sync" | "group" | "memory"
    ) {
        return Err(ExecuteWireError::InvalidDurabilityMode);
    }
    if response.encoded_len() > MAX_EXECUTE_RESPONSE_BYTES {
        return Err(ExecuteWireError::MessageTooLarge);
    }
    Ok(())
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

fn validate_provenance_uri(uri: &str) -> Result<(), ExecuteWireError> {
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
    /// A command or outcome name is empty or too long.
    InvalidProtocolName,
    /// A required input or outcome value is absent.
    MissingValue,
    /// A nested public value fails structural validation.
    InvalidValue,
    /// The completion status is unspecified or unknown.
    InvalidCompletionStatus,
    /// The plan hash is not exactly 32 bytes.
    InvalidPlanHash,
    /// The provenance URI is absent, oversized, or not in the RiffDB namespace.
    InvalidProvenanceUri,
    /// The durability-mode identifier is absent or malformed.
    InvalidDurabilityMode,
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
        };
        validate_execute_response(&response).expect("valid response");

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
