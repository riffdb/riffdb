//! Exact gRPC carriage for API-neutral service failures.

use riffdb_errors::{
    ApplicationError, ApplicationErrorCode, ErrorClass, MAX_APPLICATION_ERROR_BYTES, PublicError,
    PublicErrorStatusCode,
};
use riffdb_proto::{MAX_PUBLIC_ERROR_BYTES, application_error_to_proto, public_error_to_proto};
use riffdb_service::{ApplicationErrorContextBuilder, ServiceFailure};
use tonic::codegen::Bytes;
use tonic::{Code, Status};
use tonic_prost::prost::Message;

/// Static details-free message for caller cancellation.
pub const CANCELLED_MESSAGE: &str = "request was cancelled";
/// Static details-free message for an elapsed service deadline.
pub const DEADLINE_EXCEEDED_MESSAGE: &str = "request deadline elapsed";
/// Static details-free message for a result that exceeds the response budget.
pub const RESPONSE_TOO_LARGE_MESSAGE: &str = "response exceeds the service limit";

/// Maps one checked API-neutral failure to its exact public gRPC status.
///
/// [`PublicError`] bytes are carried directly as `grpc-status-details-bin`.
/// Closed service-control failures carry no structured details.
#[must_use]
pub fn status_from_service_failure(failure: &ServiceFailure) -> Status {
    match failure {
        ServiceFailure::Public(error) => status_from_public_error(error),
        ServiceFailure::Cancelled => Status::cancelled(CANCELLED_MESSAGE),
        ServiceFailure::DeadlineExceeded => Status::deadline_exceeded(DEADLINE_EXCEEDED_MESSAGE),
        ServiceFailure::ResponseTooLarge => Status::resource_exhausted(RESPONSE_TOO_LARGE_MESSAGE),
        ServiceFailure::EmergencyInternal(_) => Status::internal(crate::EMERGENCY_INTERNAL_MESSAGE),
    }
}

/// Encodes one checked public error without an intermediate status envelope.
#[must_use]
pub fn status_from_public_error(error: &PublicError) -> Status {
    let details = public_error_to_proto(error).encode_to_vec();
    if details.len() > MAX_PUBLIC_ERROR_BYTES {
        return Status::internal(crate::EMERGENCY_INTERNAL_MESSAGE);
    }
    Status::with_details(
        grpc_code_for_public_error(error),
        error.safe_message(),
        Bytes::from(details),
    )
}

/// Encodes one checked application error as the complete app-v1 details payload.
#[must_use]
pub fn status_from_application_error(error: &ApplicationError) -> Status {
    let details = application_error_to_proto(error).encode_to_vec();
    if details.len() > MAX_APPLICATION_ERROR_BYTES {
        return Status::internal(crate::EMERGENCY_INTERNAL_MESSAGE);
    }
    Status::with_details(
        application_grpc_code(error.code()),
        error.safe_message(),
        Bytes::from(details),
    )
}

/// Maps a shared service failure through the service-owned application context.
#[must_use]
pub fn status_from_application_failure(
    failure: &ServiceFailure,
    context: &ApplicationErrorContextBuilder,
) -> Status {
    context.build(failure).map_or_else(
        || status_from_service_failure(failure),
        |error| status_from_application_error(&error),
    )
}

/// Reclassifies an exact pre-service boundary failure for an application RPC.
#[must_use]
pub fn status_from_application_boundary(
    status: Status,
    context: &ApplicationErrorContextBuilder,
) -> Status {
    let error = match status.code() {
        Code::InvalidArgument => context.invalid_request(),
        Code::Unauthenticated | Code::PermissionDenied => context.authorization_denied(),
        Code::Unavailable => context.unavailable(),
        // Emergency containment has no real incident identity and remains
        // details-free instead of fabricating application context.
        _ => return status,
    };
    status_from_application_error(&error)
}

/// Returns the exact gRPC code for an application-semantic error.
#[must_use]
pub const fn application_grpc_code(code: ApplicationErrorCode) -> Code {
    match code {
        ApplicationErrorCode::InvalidRequest
        | ApplicationErrorCode::InputInvalid
        | ApplicationErrorCode::QueryInvalid
        | ApplicationErrorCode::CursorInvalid => Code::InvalidArgument,
        ApplicationErrorCode::AuthorizationDenied | ApplicationErrorCode::CapabilityRevoked => {
            Code::PermissionDenied
        }
        ApplicationErrorCode::ContractMismatch
        | ApplicationErrorCode::QueryUnavailable
        | ApplicationErrorCode::ModuleUnavailable
        | ApplicationErrorCode::CommandExecutionFailed
        | ApplicationErrorCode::HistoryIncarnationMismatch
        | ApplicationErrorCode::HistoryPruned => Code::FailedPrecondition,
        ApplicationErrorCode::ResponseTooLarge | ApplicationErrorCode::Overloaded => {
            Code::ResourceExhausted
        }
        ApplicationErrorCode::StorageUnavailable => Code::Unavailable,
        ApplicationErrorCode::OutcomeUnknown => Code::Unknown,
        ApplicationErrorCode::OperationCancelled => Code::Cancelled,
        ApplicationErrorCode::DeadlineExceeded => Code::DeadlineExceeded,
        ApplicationErrorCode::InternalDefect | ApplicationErrorCode::ProtocolInvalid => {
            Code::Internal
        }
        ApplicationErrorCode::IdempotencyKeyReuse => Code::AlreadyExists,
    }
}

/// Returns the gRPC status code for one public error.
///
/// Delegates to the single authoritative mapping on
/// [`PublicError::status_code`] so server encoding cannot diverge from client
/// validation.
#[must_use]
pub const fn grpc_code_for_public_error(error: &PublicError) -> Code {
    tonic_code(error.status_code())
}

/// Returns the canonical gRPC status code for one protocol-neutral error class.
#[must_use]
pub const fn grpc_code(class: ErrorClass) -> Code {
    match class {
        ErrorClass::InvalidArgument => Code::InvalidArgument,
        ErrorClass::Conflict => Code::AlreadyExists,
        ErrorClass::PermissionDenied => Code::PermissionDenied,
        ErrorClass::DeadlineExceeded => Code::DeadlineExceeded,
        ErrorClass::FailedPrecondition => Code::FailedPrecondition,
        ErrorClass::Unavailable => Code::Unavailable,
        ErrorClass::Uncertain => Code::Unknown,
        ErrorClass::Internal => Code::Internal,
    }
}

const fn tonic_code(code: PublicErrorStatusCode) -> Code {
    match code {
        PublicErrorStatusCode::InvalidArgument => Code::InvalidArgument,
        PublicErrorStatusCode::AlreadyExists => Code::AlreadyExists,
        PublicErrorStatusCode::PermissionDenied => Code::PermissionDenied,
        PublicErrorStatusCode::DeadlineExceeded => Code::DeadlineExceeded,
        PublicErrorStatusCode::FailedPrecondition => Code::FailedPrecondition,
        PublicErrorStatusCode::ResourceExhausted => Code::ResourceExhausted,
        PublicErrorStatusCode::Unavailable => Code::Unavailable,
        PublicErrorStatusCode::Unknown => Code::Unknown,
        PublicErrorStatusCode::Internal => Code::Internal,
    }
}

#[cfg(test)]
mod tests {
    use riffdb_errors::{
        ApplicationError, ApplicationErrorCode, ApplicationErrorContext, ApplicationOperation,
        PublicError,
    };
    use riffdb_proto::{decode_application_error, decode_public_error};

    use super::*;

    #[test]
    fn public_error_is_the_complete_direct_details_payload() {
        let error = PublicError::authorization_denied();
        let status = status_from_public_error(&error);
        assert_eq!(status.code(), Code::PermissionDenied);
        assert_eq!(status.message(), error.safe_message());
        assert_eq!(decode_public_error(status.details()), Ok(error));
    }

    #[test]
    fn canonical_error_class_mapping_is_total() {
        let cases = [
            (ErrorClass::InvalidArgument, Code::InvalidArgument),
            (ErrorClass::Conflict, Code::AlreadyExists),
            (ErrorClass::PermissionDenied, Code::PermissionDenied),
            (ErrorClass::DeadlineExceeded, Code::DeadlineExceeded),
            (ErrorClass::FailedPrecondition, Code::FailedPrecondition),
            (ErrorClass::Unavailable, Code::Unavailable),
            (ErrorClass::Uncertain, Code::Unknown),
            (ErrorClass::Internal, Code::Internal),
        ];
        for (class, expected) in cases {
            assert_eq!(grpc_code(class), expected);
        }
    }

    #[test]
    fn application_error_is_one_direct_checked_details_payload() {
        let error = ApplicationError::new(
            ApplicationErrorCode::AuthorizationDenied,
            ApplicationOperation::ExecuteQuery,
            ApplicationErrorContext::empty(),
            None,
        );
        let status = status_from_application_error(&error);
        assert_eq!(status.code(), Code::PermissionDenied);
        assert_eq!(status.message(), error.safe_message());
        assert_eq!(decode_application_error(status.details()), Ok(error));
    }

    #[test]
    fn staged_public_kinds_map_to_explicit_grpc_codes() {
        let mismatch = PublicError::history_incarnation_mismatch();
        let mismatch_status = status_from_public_error(&mismatch);
        assert_eq!(mismatch_status.code(), Code::FailedPrecondition);
        assert_eq!(mismatch_status.message(), mismatch.safe_message());
        assert_eq!(mismatch.code(), "history_incarnation_mismatch");
        assert_eq!(
            riffdb_errors::ApplicationErrorCode::from_public_kind(mismatch.kind()).as_str(),
            "RDB-HISTORY-0101"
        );
        assert_eq!(decode_public_error(mismatch_status.details()), Ok(mismatch));

        let overloaded = PublicError::overloaded();
        let overloaded_status = status_from_public_error(&overloaded);
        assert_eq!(overloaded_status.code(), Code::ResourceExhausted);
        assert_eq!(overloaded.class(), ErrorClass::Unavailable);
        assert_eq!(overloaded_status.message(), overloaded.safe_message());
        assert_eq!(
            decode_public_error(overloaded_status.details()),
            Ok(overloaded)
        );
    }

    #[test]
    fn every_public_kind_status_is_accepted_by_the_client() {
        use riffdb_client_rust::ClientError;
        use riffdb_errors::{
            PublicErrorKind, ValidationCode, ValidationIssue, ValidationIssues, ValidationPath,
            ValidationPathSegment,
        };
        use riffdb_types::{ContractVersion, ExecutionFailureCode, FieldId, IncidentId};

        let mut incident = [0x42; 16];
        incident[6] = 0x72;
        incident[8] = 0x82;
        let incident_id = IncidentId::from_bytes(incident).expect("UUIDv7");
        let kinds = [
            PublicErrorKind::Validation,
            PublicErrorKind::IdempotencyKeyReuse,
            PublicErrorKind::AuthorizationDenied,
            PublicErrorKind::ConcurrencyDeadlineExceeded,
            PublicErrorKind::ContractMismatch,
            PublicErrorKind::StorageUnavailable,
            PublicErrorKind::OutcomeUnknown,
            PublicErrorKind::InternalDefect,
            PublicErrorKind::CommandExecutionFailed,
            PublicErrorKind::HistoryIncarnationMismatch,
            PublicErrorKind::HistoryPruned,
            PublicErrorKind::Overloaded,
        ];
        for kind in kinds {
            let error = match kind {
                PublicErrorKind::Validation => {
                    PublicError::validation(ValidationIssues::one(ValidationIssue::new(
                        ValidationCode::InvalidValue,
                        ValidationPath::new(vec![ValidationPathSegment::Field(
                            FieldId::new(1).expect("field"),
                        )])
                        .expect("path"),
                    )))
                }
                PublicErrorKind::IdempotencyKeyReuse => PublicError::idempotency_key_reuse(),
                PublicErrorKind::AuthorizationDenied => PublicError::authorization_denied(),
                PublicErrorKind::ConcurrencyDeadlineExceeded => {
                    PublicError::concurrency_deadline_exceeded()
                }
                PublicErrorKind::ContractMismatch => PublicError::contract_mismatch(
                    ContractVersion::new(1).expect("contract version"),
                ),
                PublicErrorKind::StorageUnavailable => PublicError::storage_unavailable(),
                PublicErrorKind::OutcomeUnknown => PublicError::outcome_unknown(),
                PublicErrorKind::InternalDefect => PublicError::internal_defect(incident_id),
                PublicErrorKind::CommandExecutionFailed => {
                    PublicError::command_execution_failed(ExecutionFailureCode::ArithmeticFault)
                }
                PublicErrorKind::HistoryIncarnationMismatch => {
                    PublicError::history_incarnation_mismatch()
                }
                PublicErrorKind::HistoryPruned => PublicError::history_pruned(),
                PublicErrorKind::Overloaded => PublicError::overloaded(),
            };
            assert_eq!(error.kind(), kind);
            let status = status_from_public_error(&error);
            assert_eq!(
                status.code(),
                tonic_code(error.status_code()),
                "server code for {kind:?}"
            );
            match ClientError::from_status(status) {
                ClientError::Public(decoded) => assert_eq!(decoded, error),
                other => panic!("client rejected {kind:?}: {other:?}"),
            }
        }
    }

    #[test]
    fn staged_application_codes_map_to_explicit_grpc_codes() {
        let mismatch = ApplicationError::new(
            ApplicationErrorCode::HistoryIncarnationMismatch,
            ApplicationOperation::ExecuteQuery,
            ApplicationErrorContext::empty(),
            None,
        );
        let mismatch_status = status_from_application_error(&mismatch);
        assert_eq!(mismatch_status.code(), Code::FailedPrecondition);
        assert_eq!(mismatch_status.message(), mismatch.safe_message());
        assert_eq!(
            decode_application_error(mismatch_status.details()),
            Ok(mismatch)
        );

        let overloaded = ApplicationError::new(
            ApplicationErrorCode::Overloaded,
            ApplicationOperation::ExecuteQuery,
            ApplicationErrorContext::empty(),
            None,
        );
        let overloaded_status = status_from_application_error(&overloaded);
        assert_eq!(overloaded_status.code(), Code::ResourceExhausted);
        assert_eq!(overloaded_status.message(), overloaded.safe_message());
        assert_eq!(
            decode_application_error(overloaded_status.details()),
            Ok(overloaded)
        );
    }

    #[test]
    fn control_failures_never_fabricate_public_error_details() {
        let cases = [
            (
                ServiceFailure::Cancelled,
                Code::Cancelled,
                CANCELLED_MESSAGE,
            ),
            (
                ServiceFailure::DeadlineExceeded,
                Code::DeadlineExceeded,
                DEADLINE_EXCEEDED_MESSAGE,
            ),
            (
                ServiceFailure::ResponseTooLarge,
                Code::ResourceExhausted,
                RESPONSE_TOO_LARGE_MESSAGE,
            ),
        ];
        for (failure, code, message) in cases {
            let status = status_from_service_failure(&failure);
            assert_eq!(status.code(), code);
            assert_eq!(status.message(), message);
            assert!(status.details().is_empty());
        }
    }
}
