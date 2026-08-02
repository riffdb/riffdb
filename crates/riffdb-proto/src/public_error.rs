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
use riffdb_types::ExecutionFailureCode as DomainExecutionFailureCode;
use riffdb_types::{ContractVersion, FieldId, IncidentId};

use crate::v1;
use crate::wire::{self, PreflightError};

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
        DomainDetails::CommandExecutionFailed { code } => Some(
            v1::public_error::Details::ExecutionFailure(v1::CommandExecutionFailureDetails {
                code: proto_execution_failure_code(*code) as i32,
            }),
        ),
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
    match wire::public_error(input) {
        Ok(()) => {}
        Err(PreflightError::Malformed) => {
            return Err(PublicErrorWireError::MalformedEncoding);
        }
        Err(PreflightError::LimitExceeded) => {
            return Err(PublicErrorWireError::PreflightLimitExceeded);
        }
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
        ) => DomainPublicError::contract_mismatch(
            ContractVersion::new(details.active_contract_version)
                .ok_or(PublicErrorWireError::InvalidContractMismatchDetails)?,
        ),
        (
            DomainKind::CommandExecutionFailed,
            Some(v1::public_error::Details::ExecutionFailure(details)),
        ) => DomainPublicError::command_execution_failed(domain_execution_failure_code(
            details.code,
        )?),
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
        (DomainKind::HistoryIncarnationMismatch, None) => {
            DomainPublicError::history_incarnation_mismatch()
        }
        (DomainKind::HistoryPruned, None) => DomainPublicError::history_pruned(),
        (DomainKind::Overloaded, None) => DomainPublicError::overloaded(),
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
                        Ok(DomainPathSegment::Field(
                            FieldId::new(field_id)
                                .ok_or(PublicErrorWireError::InvalidValidationDetails)?,
                        ))
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
        DomainKind::CommandExecutionFailed => v1::PublicErrorKind::CommandExecutionFailed,
        DomainKind::HistoryIncarnationMismatch => v1::PublicErrorKind::HistoryIncarnationMismatch,
        DomainKind::HistoryPruned => v1::PublicErrorKind::HistoryPruned,
        DomainKind::Overloaded => v1::PublicErrorKind::Overloaded,
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
        Ok(v1::PublicErrorKind::CommandExecutionFailed) => Ok(DomainKind::CommandExecutionFailed),
        Ok(v1::PublicErrorKind::HistoryIncarnationMismatch) => {
            Ok(DomainKind::HistoryIncarnationMismatch)
        }
        Ok(v1::PublicErrorKind::HistoryPruned) => Ok(DomainKind::HistoryPruned),
        Ok(v1::PublicErrorKind::Overloaded) => Ok(DomainKind::Overloaded),
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

const fn proto_execution_failure_code(
    code: DomainExecutionFailureCode,
) -> v1::ExecutionFailureCode {
    match code {
        DomainExecutionFailureCode::ArithmeticFault => v1::ExecutionFailureCode::ArithmeticFault,
        DomainExecutionFailureCode::ResourceLimit => v1::ExecutionFailureCode::ResourceLimit,
        DomainExecutionFailureCode::UniqueConflict => v1::ExecutionFailureCode::UniqueConflict,
    }
}

fn domain_execution_failure_code(
    value: i32,
) -> Result<DomainExecutionFailureCode, PublicErrorWireError> {
    match v1::ExecutionFailureCode::try_from(value) {
        Ok(v1::ExecutionFailureCode::ArithmeticFault) => {
            Ok(DomainExecutionFailureCode::ArithmeticFault)
        }
        Ok(v1::ExecutionFailureCode::ResourceLimit) => {
            Ok(DomainExecutionFailureCode::ResourceLimit)
        }
        Ok(v1::ExecutionFailureCode::UniqueConflict) => {
            Ok(DomainExecutionFailureCode::UniqueConflict)
        }
        Ok(v1::ExecutionFailureCode::Unspecified) | Err(_) => {
            Err(PublicErrorWireError::InvalidExecutionFailureDetails)
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
    /// A nested wire length or collection count exceeds its pre-allocation limit.
    PreflightLimitExceeded,
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
    /// Contract-mismatch detail contains an unassigned contract version.
    InvalidContractMismatchDetails,
    /// Command-execution detail omits or contains an unknown closed code.
    InvalidExecutionFailureDetails,
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
            DomainPathSegment::Field(FieldId::new(7).expect("field ID is nonzero")),
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
            DomainPublicError::contract_mismatch(
                ContractVersion::new(9).expect("contract version is nonzero"),
            ),
            DomainPublicError::storage_unavailable(),
            DomainPublicError::outcome_unknown(),
            DomainPublicError::internal_defect(incident_id()),
            DomainPublicError::command_execution_failed(
                DomainExecutionFailureCode::ArithmeticFault,
            ),
            DomainPublicError::history_incarnation_mismatch(),
            DomainPublicError::overloaded(),
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

    #[test]
    fn execution_failure_requires_the_exact_closed_detail() {
        let error =
            DomainPublicError::command_execution_failed(DomainExecutionFailureCode::ResourceLimit);
        let wire = public_error_to_proto(&error);
        assert_eq!(public_error_from_proto(&wire), Ok(error));
        let unique =
            DomainPublicError::command_execution_failed(DomainExecutionFailureCode::UniqueConflict);
        assert_eq!(
            public_error_from_proto(&public_error_to_proto(&unique)),
            Ok(unique)
        );

        let mut missing = wire.clone();
        missing.details = None;
        assert_eq!(
            public_error_from_proto(&missing),
            Err(PublicErrorWireError::InconsistentBoundary)
        );
        assert_eq!(
            decode_public_error(&missing.encode_to_vec()),
            Err(PublicErrorWireError::InconsistentBoundary)
        );

        let mut zero = wire.clone();
        zero.details = Some(v1::public_error::Details::ExecutionFailure(
            v1::CommandExecutionFailureDetails { code: 0 },
        ));
        assert_eq!(
            public_error_from_proto(&zero),
            Err(PublicErrorWireError::InvalidExecutionFailureDetails)
        );
        assert_eq!(
            decode_public_error(&zero.encode_to_vec()),
            Err(PublicErrorWireError::InvalidExecutionFailureDetails)
        );

        let mut unknown = wire.clone();
        unknown.details = Some(v1::public_error::Details::ExecutionFailure(
            v1::CommandExecutionFailureDetails { code: 4 },
        ));
        assert_eq!(
            public_error_from_proto(&unknown),
            Err(PublicErrorWireError::InvalidExecutionFailureDetails)
        );
        assert_eq!(
            decode_public_error(&unknown.encode_to_vec()),
            Err(PublicErrorWireError::InvalidExecutionFailureDetails)
        );

        let mut wrong = wire;
        wrong.details = Some(v1::public_error::Details::ContractMismatch(
            v1::ContractMismatchDetails {
                active_contract_version: 1,
            },
        ));
        assert_eq!(
            public_error_from_proto(&wrong),
            Err(PublicErrorWireError::InconsistentBoundary)
        );
        assert_eq!(
            decode_public_error(&wrong.encode_to_vec()),
            Err(PublicErrorWireError::InconsistentBoundary)
        );
    }

    #[test]
    fn zero_contract_and_field_ids_are_rejected() {
        let mut contract = public_error_to_proto(&DomainPublicError::contract_mismatch(
            ContractVersion::new(1).expect("contract version is nonzero"),
        ));
        contract.details = Some(v1::public_error::Details::ContractMismatch(
            v1::ContractMismatchDetails {
                active_contract_version: 0,
            },
        ));
        assert_eq!(
            public_error_from_proto(&contract),
            Err(PublicErrorWireError::InvalidContractMismatchDetails)
        );
        assert_eq!(
            decode_public_error(&contract.encode_to_vec()),
            Err(PublicErrorWireError::InvalidContractMismatchDetails)
        );

        let mut validation = public_error_to_proto(&all_errors().remove(0));
        let Some(v1::public_error::Details::Validation(details)) = validation.details.as_mut()
        else {
            panic!("validation detail fixture");
        };
        details.issues[0].path[0].segment = Some(v1::validation_path_segment::Segment::FieldId(0));
        assert_eq!(
            public_error_from_proto(&validation),
            Err(PublicErrorWireError::InvalidValidationDetails)
        );
        assert_eq!(
            decode_public_error(&validation.encode_to_vec()),
            Err(PublicErrorWireError::InvalidValidationDetails)
        );
    }

    #[test]
    fn application_hint_does_not_change_compatible_kernel_bytes() {
        let base =
            DomainPublicError::validation(DomainValidationIssues::one(DomainValidationIssue::new(
                DomainValidationCode::InvalidValue,
                DomainValidationPath::root(),
            )));
        let hinted = base
            .clone()
            .with_application_code_hint(riffdb_errors::ApplicationErrorCode::CursorInvalid)
            .expect("compatible hint");
        let base_bytes = public_error_to_proto(&base).encode_to_vec();
        let hinted_bytes = public_error_to_proto(&hinted).encode_to_vec();
        assert_eq!(hinted_bytes, base_bytes);
        let decoded = decode_public_error(&hinted_bytes).expect("kernel decode");
        assert_eq!(decoded, base);
        assert_eq!(decoded.application_code_hint(), None);
    }
}
