//! Exact gRPC carriage for API-neutral service failures.

use riffdb_errors::{ErrorClass, PublicError};
use riffdb_proto::{MAX_PUBLIC_ERROR_BYTES, public_error_to_proto};
use riffdb_service::ServiceFailure;
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
        grpc_code(error.class()),
        error.safe_message(),
        Bytes::from(details),
    )
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

#[cfg(test)]
mod tests {
    use riffdb_errors::PublicError;
    use riffdb_proto::decode_public_error;

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
