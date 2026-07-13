//! Exact mapping for the transport-neutral public error boundary.

use std::error::Error;
use std::fmt;

use prost::Message;
use riffdb_errors::{
    PublicError as DomainPublicError, PublicErrorDetails as DomainDetails,
    PublicErrorKind as DomainKind, RecoveryAction as DomainRecoveryAction,
    ValidationCode as DomainValidationCode, ValidationIssue as DomainValidationIssue,
    ValidationIssues as DomainValidationIssues, ValidationPath as DomainValidationPath,
    ValidationPathSegment as DomainPathSegment,
};
use riffdb_types::{ContractVersion, FieldId, IncidentId};

use crate::v1;

/// Conservative ceiling for the bounded public error envelope.
pub const MAX_PUBLIC_ERROR_BYTES: usize = 16 * 1024;

/// Encodes a domain public error without accepting arbitrary diagnostic text.
#[must_use]
pub fn public_error_to_proto(error: &DomainPublicError) -> v1::PublicError {
    let details = match error.details() {
        DomainDetails::None => None,
        DomainDetails::Validation(issues) => Some(v1::public_error::Details::Validation(
            v1::ValidationIssues {
                issues: issues
                    .as_slice()
                    .iter()
                    .map(validation_issue_to_proto)
                    .collect(),
            },
        )),
        DomainDetails::ContractMismatch {
            active_contract_version,
        } => Some(v1::public_error::Details::ContractMismatch(
            v1::ContractMismatchDetails {
                active_contract_version: active_contract_version.get(),
            },
        )),
    };

    v1::PublicError {
        kind: proto_kind(error.kind()) as i32,
        code: error.code().to_owned(),
        safe_message: error.safe_message().to_owned(),
        recovery_action: proto_recovery(error.recovery_action()) as i32,
        details,
        incident_id: error
            .incident_id()
            .map(|incident_id| incident_id.as_bytes().to_vec()),
    }
}

/// Decodes and validates a bounded public error message.
pub fn decode_public_error(input: &[u8]) -> Result<DomainPublicError, PublicErrorWireError> {
    if input.len() > MAX_PUBLIC_ERROR_BYTES {
        return Err(PublicErrorWireError::MessageTooLarge);
    }
    let wire =
        v1::PublicError::decode(input).map_err(|_| PublicErrorWireError::MalformedEncoding)?;
    public_error_from_proto(&wire)
}

/// Validates the redundant wire invariants and reconstructs the domain error.
pub fn public_error_from_proto(
    wire: &v1::PublicError,
) -> Result<DomainPublicError, PublicErrorWireError> {
    if wire.encoded_len() > MAX_PUBLIC_ERROR_BYTES {
        return Err(PublicErrorWireError::MessageTooLarge);
    }
    let kind = domain_kind(wire.kind)?;
    if wire.code != kind.code()
        || wire.safe_message != kind.safe_message()
        || wire.recovery_action != proto_recovery(kind.recovery_action()) as i32
    {
        return Err(PublicErrorWireError::InconsistentBoundary);
    }

    let incident_id = wire
        .incident_id
        .as_deref()
        .map(parse_incident_id)
        .transpose()?;

    let mut error = match (kind, wire.details.as_ref()) {
        (DomainKind::Validation, Some(v1::public_error::Details::Validation(details))) => {
            DomainPublicError::validation(validation_issues_from_proto(details)?)
        }
        (
            DomainKind::ContractMismatch,
            Some(v1::public_error::Details::ContractMismatch(details)),
        ) => DomainPublicError::contract_mismatch(ContractVersion::new(
            details.active_contract_version,
        )),
        (DomainKind::IdempotencyKeyReuse, None) => DomainPublicError::idempotency_key_reuse(),
        (DomainKind::AuthorizationDenied, None) => DomainPublicError::authorization_denied(),
        (DomainKind::ConcurrencyDeadlineExceeded, None) => {
            DomainPublicError::concurrency_deadline_exceeded()
        }
        (DomainKind::StorageUnavailable, None) => DomainPublicError::storage_unavailable(),
        (DomainKind::OutcomeUnknown, None) => DomainPublicError::outcome_unknown(),
        (DomainKind::InternalDefect, None) => DomainPublicError::internal_defect(
            incident_id.ok_or(PublicErrorWireError::MissingInternalIncident)?,
        ),
        _ => return Err(PublicErrorWireError::InconsistentBoundary),
    };

    if kind != DomainKind::InternalDefect
        && let Some(incident_id) = incident_id
    {
        error = error.with_incident_id(incident_id);
    }
    Ok(error)
}

fn validation_issue_to_proto(issue: &DomainValidationIssue) -> v1::ValidationIssue {
    v1::ValidationIssue {
        code: proto_validation_code(issue.code()) as i32,
        path: issue
            .path()
            .segments()
            .iter()
            .map(|segment| v1::ValidationPathSegment {
                segment: Some(match segment {
                    DomainPathSegment::Field(field_id) => {
                        v1::validation_path_segment::Segment::FieldId(field_id.get())
                    }
                    DomainPathSegment::ListIndex(index) => {
                        v1::validation_path_segment::Segment::ListIndex(*index)
                    }
                }),
            })
            .collect(),
    }
}

fn validation_issues_from_proto(
    details: &v1::ValidationIssues,
) -> Result<DomainValidationIssues, PublicErrorWireError> {
    let issues = details
        .issues
        .iter()
        .map(|issue| {
            let code = domain_validation_code(issue.code)?;
            let segments = issue
                .path
                .iter()
                .map(|segment| match segment.segment {
                    Some(v1::validation_path_segment::Segment::FieldId(field_id)) => {
                        Ok(DomainPathSegment::Field(FieldId::new(field_id)))
                    }
                    Some(v1::validation_path_segment::Segment::ListIndex(index)) => {
                        Ok(DomainPathSegment::ListIndex(index))
                    }
                    None => Err(PublicErrorWireError::InvalidValidationDetails),
                })
                .collect::<Result<Vec<_>, _>>()?;
            let path = DomainValidationPath::new(segments)
                .map_err(|_| PublicErrorWireError::InvalidValidationDetails)?;
            Ok(DomainValidationIssue::new(code, path))
        })
        .collect::<Result<Vec<_>, PublicErrorWireError>>()?;
    DomainValidationIssues::new(issues).map_err(|_| PublicErrorWireError::InvalidValidationDetails)
}

fn parse_incident_id(bytes: &[u8]) -> Result<IncidentId, PublicErrorWireError> {
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|_| PublicErrorWireError::InvalidIncidentId)?;
    IncidentId::from_bytes(bytes).map_err(|_| PublicErrorWireError::InvalidIncidentId)
}

const fn proto_kind(kind: DomainKind) -> v1::PublicErrorKind {
    match kind {
        DomainKind::Validation => v1::PublicErrorKind::Validation,
        DomainKind::IdempotencyKeyReuse => v1::PublicErrorKind::IdempotencyKeyReuse,
        DomainKind::AuthorizationDenied => v1::PublicErrorKind::AuthorizationDenied,
        DomainKind::ConcurrencyDeadlineExceeded => v1::PublicErrorKind::ConcurrencyDeadlineExceeded,
        DomainKind::ContractMismatch => v1::PublicErrorKind::ContractMismatch,
        DomainKind::StorageUnavailable => v1::PublicErrorKind::StorageUnavailable,
        DomainKind::OutcomeUnknown => v1::PublicErrorKind::OutcomeUnknown,
        DomainKind::InternalDefect => v1::PublicErrorKind::InternalDefect,
    }
}

fn domain_kind(value: i32) -> Result<DomainKind, PublicErrorWireError> {
    match v1::PublicErrorKind::try_from(value) {
        Ok(v1::PublicErrorKind::Validation) => Ok(DomainKind::Validation),
        Ok(v1::PublicErrorKind::IdempotencyKeyReuse) => Ok(DomainKind::IdempotencyKeyReuse),
        Ok(v1::PublicErrorKind::AuthorizationDenied) => Ok(DomainKind::AuthorizationDenied),
        Ok(v1::PublicErrorKind::ConcurrencyDeadlineExceeded) => {
            Ok(DomainKind::ConcurrencyDeadlineExceeded)
        }
        Ok(v1::PublicErrorKind::ContractMismatch) => Ok(DomainKind::ContractMismatch),
        Ok(v1::PublicErrorKind::StorageUnavailable) => Ok(DomainKind::StorageUnavailable),
        Ok(v1::PublicErrorKind::OutcomeUnknown) => Ok(DomainKind::OutcomeUnknown),
        Ok(v1::PublicErrorKind::InternalDefect) => Ok(DomainKind::InternalDefect),
        Ok(v1::PublicErrorKind::Unspecified) | Err(_) => Err(PublicErrorWireError::UnknownKind),
    }
}

const fn proto_recovery(action: DomainRecoveryAction) -> v1::RecoveryAction {
    match action {
        DomainRecoveryAction::CorrectRequest => v1::RecoveryAction::CorrectRequest,
        DomainRecoveryAction::Retry => v1::RecoveryAction::Retry,
        DomainRecoveryAction::ResolveWithSameIdempotencyKey => {
            v1::RecoveryAction::ResolveWithSameIdempotencyKey
        }
        DomainRecoveryAction::ObtainPermission => v1::RecoveryAction::ObtainPermission,
        DomainRecoveryAction::RefreshContract => v1::RecoveryAction::RefreshContract,
        DomainRecoveryAction::ContactOperator => v1::RecoveryAction::ContactOperator,
    }
}

const fn proto_validation_code(code: DomainValidationCode) -> v1::ValidationCode {
    match code {
        DomainValidationCode::MissingRequiredValue => v1::ValidationCode::MissingRequiredValue,
        DomainValidationCode::TypeMismatch => v1::ValidationCode::TypeMismatch,
        DomainValidationCode::InvalidValue => v1::ValidationCode::InvalidValue,
        DomainValidationCode::OutOfRange => v1::ValidationCode::OutOfRange,
        DomainValidationCode::TooLong => v1::ValidationCode::TooLong,
        DomainValidationCode::TooManyItems => v1::ValidationCode::TooManyItems,
        DomainValidationCode::UnknownField => v1::ValidationCode::UnknownField,
        DomainValidationCode::DuplicateField => v1::ValidationCode::DuplicateField,
    }
}

fn domain_validation_code(value: i32) -> Result<DomainValidationCode, PublicErrorWireError> {
    match v1::ValidationCode::try_from(value) {
        Ok(v1::ValidationCode::MissingRequiredValue) => {
            Ok(DomainValidationCode::MissingRequiredValue)
        }
        Ok(v1::ValidationCode::TypeMismatch) => Ok(DomainValidationCode::TypeMismatch),
        Ok(v1::ValidationCode::InvalidValue) => Ok(DomainValidationCode::InvalidValue),
        Ok(v1::ValidationCode::OutOfRange) => Ok(DomainValidationCode::OutOfRange),
        Ok(v1::ValidationCode::TooLong) => Ok(DomainValidationCode::TooLong),
        Ok(v1::ValidationCode::TooManyItems) => Ok(DomainValidationCode::TooManyItems),
        Ok(v1::ValidationCode::UnknownField) => Ok(DomainValidationCode::UnknownField),
        Ok(v1::ValidationCode::DuplicateField) => Ok(DomainValidationCode::DuplicateField),
        Ok(v1::ValidationCode::Unspecified) | Err(_) => {
            Err(PublicErrorWireError::InvalidValidationDetails)
        }
    }
}

/// A non-secret failure while decoding the public error wire boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublicErrorWireError {
    /// The encoded error exceeds its hard ceiling.
    MessageTooLarge,
    /// Protobuf decoding failed.
    MalformedEncoding,
    /// The error kind is absent or not recognized.
    UnknownKind,
    /// Redundant kind, code, message, recovery, or detail fields disagree.
    InconsistentBoundary,
    /// An incident identifier is not an exact UUIDv7.
    InvalidIncidentId,
    /// An internal defect omitted its required domain incident identifier.
    MissingInternalIncident,
    /// Validation issues or paths violate their closed bounds.
    InvalidValidationDetails,
}

impl fmt::Display for PublicErrorWireError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("public error wire message is invalid")
    }
}

impl Error for PublicErrorWireError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn incident_id() -> IncidentId {
        IncidentId::from_bytes([
            0x01, 0x9b, 0xf6, 0xaa, 0xa6, 0x40, 0x7d, 0xe6, 0x89, 0xc9, 0x8a, 0x7f, 0x70, 0xbb,
            0xbd, 0x23,
        ])
        .expect("valid UUIDv7 fixture")
    }

    fn all_errors() -> Vec<DomainPublicError> {
        let path = DomainValidationPath::new(vec![
            DomainPathSegment::Field(FieldId::new(7)),
            DomainPathSegment::ListIndex(2),
        ])
        .expect("bounded path");
        vec![
            DomainPublicError::validation(DomainValidationIssues::one(DomainValidationIssue::new(
                DomainValidationCode::InvalidValue,
                path,
            ))),
            DomainPublicError::idempotency_key_reuse(),
            DomainPublicError::authorization_denied(),
            DomainPublicError::concurrency_deadline_exceeded(),
            DomainPublicError::contract_mismatch(ContractVersion::new(9)),
            DomainPublicError::storage_unavailable(),
            DomainPublicError::outcome_unknown(),
            DomainPublicError::internal_defect(incident_id()),
        ]
    }

    #[test]
    fn all_domain_errors_round_trip_exactly() {
        for error in all_errors() {
            let wire = public_error_to_proto(&error);
            assert_eq!(public_error_from_proto(&wire), Ok(error));
        }
    }

    #[test]
    fn redundant_safe_fields_must_match_the_closed_kind() {
        let mut wire = public_error_to_proto(&DomainPublicError::authorization_denied());
        wire.safe_message = "caller-controlled secret".to_owned();
        assert_eq!(
            public_error_from_proto(&wire),
            Err(PublicErrorWireError::InconsistentBoundary)
        );
    }

    #[test]
    fn emitted_wire_errors_have_no_arbitrary_diagnostic_channel() {
        let secret = "super-secret-value";
        for error in all_errors() {
            let encoded = public_error_to_proto(&error).encode_to_vec();
            assert!(
                !encoded
                    .windows(secret.len())
                    .any(|window| window == secret.as_bytes())
            );
        }
    }
}
