//! Fail-closed validation of public gRPC failures.

use std::error::Error;
use std::fmt;

use riffdb_errors::{
    ApplicationError, ApplicationErrorCode, MAX_APPLICATION_ERROR_BYTES, PublicError,
    PublicErrorKind, PublicErrorStatusCode,
};
use riffdb_proto::{
    ApplicationErrorWireError, MAX_PUBLIC_ERROR_BYTES, PublicErrorWireError,
    decode_application_error, decode_public_error,
};
use tonic::{Code, Status};

use crate::ids::IdentifierGenerationError;

const AUTHENTICATION_FAILED: &str = "authentication failed";
const REQUEST_CANCELLED: &str = "request was cancelled";
const REQUEST_DEADLINE_ELAPSED: &str = "request deadline elapsed";
const RESPONSE_TOO_LARGE: &str = "response exceeds the service limit";
const EMERGENCY_INTERNAL: &str = "an internal error occurred";

/// A valid details-free status from a closed non-`PublicError` boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DetailsFreeStatus {
    /// Authentication failed before an API-neutral request context existed.
    Unauthenticated,
    /// Cancellation was proven before the applicable admission boundary.
    Cancelled,
    /// The request deadline elapsed before a protected result was released.
    DeadlineExceeded,
    /// One complete service result exceeded the response budget.
    ResponseTooLarge,
    /// Internal containment could not obtain a real incident identifier.
    EmergencyInternal,
    /// The transport became unavailable without a structured server result.
    TransportUnavailable,
}

/// The closed reason an untrusted gRPC failure was rejected.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProtocolFailureKind {
    /// A status that requires `PublicError` detail carried none.
    MissingPublicErrorDetails,
    /// Structured detail exceeded the exact 16 KiB ceiling.
    OversizedPublicErrorDetails,
    /// Structured detail was not valid Protobuf.
    MalformedPublicErrorDetails,
    /// Structured detail named an unknown public-error kind.
    UnknownPublicErrorKind,
    /// Structured detail violated its closed field/detail invariants.
    InvalidPublicErrorDetails,
    /// The gRPC code disagreed with the checked public error class.
    StatusCodeMismatch,
    /// `grpc-message` disagreed with the checked registry-owned safe message.
    StatusMessageMismatch,
    /// A details-free closed status carried an unrecognized code/message pair.
    InvalidDetailsFreeStatus,
    /// A request built by the caller failed public structural validation.
    InvalidOutboundMessage,
    /// A successful response or stream item failed public structural validation.
    InvalidInboundMessage,
    /// An application RPC carried no application error details.
    MissingApplicationErrorDetails,
    /// Application error details exceeded their exact ceiling.
    OversizedApplicationErrorDetails,
    /// Application error details were malformed or noncanonical.
    InvalidApplicationErrorDetails,
    /// The application status code disagreed with its checked semantic code.
    ApplicationStatusCodeMismatch,
    /// The application status message disagreed with its registry-owned message.
    ApplicationStatusMessageMismatch,
}

/// A bounded protocol failure containing no peer-provided text or bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProtocolFailure {
    kind: ProtocolFailureKind,
}

impl ProtocolFailure {
    pub(crate) const fn new(kind: ProtocolFailureKind) -> Self {
        Self { kind }
    }

    /// Returns the closed validation failure kind.
    #[must_use]
    pub const fn kind(self) -> ProtocolFailureKind {
        self.kind
    }
}

impl fmt::Display for ProtocolFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("the RiffDB gRPC peer returned an invalid protocol response")
    }
}

impl Error for ProtocolFailure {}

/// The public client's explicit unresolved idempotent-operation disposition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OutcomeUnknown;

impl fmt::Display for OutcomeUnknown {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("the command outcome remains unknown")
    }
}

impl Error for OutcomeUnknown {}

/// A closed failure returned by the Rust SDK.
#[derive(Debug)]
pub enum ClientError {
    /// A fully checked public-safe RiffDB failure.
    Public(PublicError),
    /// A fully checked symbolic application failure.
    Application(Box<ApplicationError>),
    /// A valid status from a closed details-free boundary.
    DetailsFree(DetailsFreeStatus),
    /// The peer violated the public gRPC contract.
    Protocol(ProtocolFailure),
    /// A local UUIDv7 source failed before the next submission.
    IdentifierGeneration(IdentifierGenerationError),
    /// Channel construction or connection failed before a typed response existed.
    ConnectionFailure,
    /// Verified TLS configuration, trust-root, connection, or peer failure.
    Tls(crate::TlsClientFailure),
    /// Automatic same-input recovery exhausted its explicit attempt budget.
    OutcomeUnknown(OutcomeUnknown),
}

impl ClientError {
    /// Validates an untrusted non-OK gRPC status without trusting its message.
    #[must_use]
    pub fn from_status(status: Status) -> Self {
        checked_status(status)
    }

    /// Returns a checked public error, when this is one.
    #[must_use]
    pub const fn public_error(&self) -> Option<&PublicError> {
        match self {
            Self::Public(error) => Some(error),
            Self::Application(_)
            | Self::DetailsFree(_)
            | Self::Protocol(_)
            | Self::IdentifierGeneration(_)
            | Self::ConnectionFailure
            | Self::Tls(_)
            | Self::OutcomeUnknown(_) => None,
        }
    }

    /// Returns a checked symbolic application failure, when this is one.
    #[must_use]
    pub const fn application_error(&self) -> Option<&ApplicationError> {
        match self {
            Self::Application(error) => Some(error),
            Self::Public(_)
            | Self::DetailsFree(_)
            | Self::Protocol(_)
            | Self::IdentifierGeneration(_)
            | Self::ConnectionFailure
            | Self::Tls(_)
            | Self::OutcomeUnknown(_) => None,
        }
    }
}

impl fmt::Display for ClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Public(error) => error.fmt(formatter),
            Self::Application(error) => error.fmt(formatter),
            Self::DetailsFree(status) => formatter.write_str(match status {
                DetailsFreeStatus::Unauthenticated => AUTHENTICATION_FAILED,
                DetailsFreeStatus::Cancelled => REQUEST_CANCELLED,
                DetailsFreeStatus::DeadlineExceeded => REQUEST_DEADLINE_ELAPSED,
                DetailsFreeStatus::ResponseTooLarge => RESPONSE_TOO_LARGE,
                DetailsFreeStatus::EmergencyInternal => EMERGENCY_INTERNAL,
                DetailsFreeStatus::TransportUnavailable => "the gRPC transport is unavailable",
            }),
            Self::Protocol(error) => error.fmt(formatter),
            Self::IdentifierGeneration(error) => error.fmt(formatter),
            Self::ConnectionFailure => formatter.write_str("the gRPC channel could not connect"),
            Self::Tls(error) => error.fmt(formatter),
            Self::OutcomeUnknown(error) => error.fmt(formatter),
        }
    }
}

impl Error for ClientError {}

pub(crate) fn checked_status(status: Status) -> ClientError {
    let details = status.details();
    if details.is_empty() {
        return checked_details_free_status(
            status.code(),
            status.message(),
            Error::source(&status).is_some(),
        );
    }
    if details.len() > MAX_PUBLIC_ERROR_BYTES {
        return protocol(ProtocolFailureKind::OversizedPublicErrorDetails);
    }

    let error = match decode_public_error(details) {
        Ok(error) => error,
        Err(error) => return protocol(map_wire_error(error)),
    };
    if status.code() != code_for_public_error(&error) {
        return protocol(ProtocolFailureKind::StatusCodeMismatch);
    }
    if status.message() != error.safe_message() {
        return protocol(ProtocolFailureKind::StatusMessageMismatch);
    }
    ClientError::Public(error)
}

pub(crate) fn checked_application_status(status: Status) -> ClientError {
    let details = status.details();
    if details.is_empty() {
        let checked = checked_details_free_status(
            status.code(),
            status.message(),
            Error::source(&status).is_some(),
        );
        return match checked {
            ClientError::DetailsFree(DetailsFreeStatus::EmergencyInternal)
            | ClientError::DetailsFree(DetailsFreeStatus::TransportUnavailable) => checked,
            _ => protocol(ProtocolFailureKind::MissingApplicationErrorDetails),
        };
    }
    if details.len() > MAX_APPLICATION_ERROR_BYTES {
        return protocol(ProtocolFailureKind::OversizedApplicationErrorDetails);
    }
    let error = match decode_application_error(details) {
        Ok(error) => error,
        Err(error) => return protocol(map_application_wire_error(error)),
    };
    if status.code() != code_for_application(error.code()) {
        return protocol(ProtocolFailureKind::ApplicationStatusCodeMismatch);
    }
    if status.message() != error.safe_message() {
        return protocol(ProtocolFailureKind::ApplicationStatusMessageMismatch);
    }
    ClientError::Application(Box::new(error))
}

fn checked_details_free_status(code: Code, message: &str, has_local_source: bool) -> ClientError {
    let status = match (code, message) {
        (Code::Unauthenticated, AUTHENTICATION_FAILED) => DetailsFreeStatus::Unauthenticated,
        (Code::Cancelled, REQUEST_CANCELLED) => DetailsFreeStatus::Cancelled,
        (Code::DeadlineExceeded, REQUEST_DEADLINE_ELAPSED) => DetailsFreeStatus::DeadlineExceeded,
        (Code::ResourceExhausted, RESPONSE_TOO_LARGE) => DetailsFreeStatus::ResponseTooLarge,
        (Code::Internal, EMERGENCY_INTERNAL) => DetailsFreeStatus::EmergencyInternal,
        // Tonic attaches a source only to locally synthesized transport errors.
        // H2 may surface an incomplete response as INTERNAL rather than
        // UNAVAILABLE. A peer-originated status has no local source and still
        // fails closed below.
        (_, _) if has_local_source => DetailsFreeStatus::TransportUnavailable,
        (
            _,
            AUTHENTICATION_FAILED
            | REQUEST_CANCELLED
            | REQUEST_DEADLINE_ELAPSED
            | RESPONSE_TOO_LARGE
            | EMERGENCY_INTERNAL,
        ) => {
            return protocol(ProtocolFailureKind::InvalidDetailsFreeStatus);
        }
        _ => return protocol(ProtocolFailureKind::MissingPublicErrorDetails),
    };
    ClientError::DetailsFree(status)
}

const fn code_for_public_error(error: &PublicError) -> Code {
    tonic_code(error.status_code())
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

const fn code_for_application(code: ApplicationErrorCode) -> Code {
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
        | ApplicationErrorCode::HistoryPruned
        | ApplicationErrorCode::ProjectionDiverged
        | ApplicationErrorCode::SnapshotRetired
        | ApplicationErrorCode::FreshnessUnsatisfied
        | ApplicationErrorCode::ProjectedSourceRequired => Code::FailedPrecondition,
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

const fn map_wire_error(error: PublicErrorWireError) -> ProtocolFailureKind {
    match error {
        PublicErrorWireError::MessageTooLarge => ProtocolFailureKind::OversizedPublicErrorDetails,
        PublicErrorWireError::MalformedEncoding | PublicErrorWireError::PreflightLimitExceeded => {
            ProtocolFailureKind::MalformedPublicErrorDetails
        }
        PublicErrorWireError::UnknownKind => ProtocolFailureKind::UnknownPublicErrorKind,
        PublicErrorWireError::InconsistentBoundary
        | PublicErrorWireError::InvalidIncidentId
        | PublicErrorWireError::MissingInternalIncident
        | PublicErrorWireError::InvalidValidationDetails
        | PublicErrorWireError::InvalidContractMismatchDetails
        | PublicErrorWireError::InvalidExecutionFailureDetails => {
            ProtocolFailureKind::InvalidPublicErrorDetails
        }
    }
}

const fn map_application_wire_error(error: ApplicationErrorWireError) -> ProtocolFailureKind {
    match error {
        ApplicationErrorWireError::MessageTooLarge => {
            ProtocolFailureKind::OversizedApplicationErrorDetails
        }
        ApplicationErrorWireError::MalformedEncoding
        | ApplicationErrorWireError::PreflightLimitExceeded
        | ApplicationErrorWireError::UnknownField
        | ApplicationErrorWireError::DuplicateField
        | ApplicationErrorWireError::UnknownCode
        | ApplicationErrorWireError::UnknownOperation
        | ApplicationErrorWireError::UnknownFix
        | ApplicationErrorWireError::InconsistentRegistry
        | ApplicationErrorWireError::InvalidContext
        | ApplicationErrorWireError::MissingInternalIncident => {
            ProtocolFailureKind::InvalidApplicationErrorDetails
        }
    }
}

const fn protocol(kind: ProtocolFailureKind) -> ClientError {
    ClientError::Protocol(ProtocolFailure::new(kind))
}

pub(crate) const fn is_retryable(error: &ClientError) -> bool {
    match error {
        ClientError::Public(error) => matches!(
            error.recovery_action(),
            riffdb_errors::RecoveryAction::Retry
                | riffdb_errors::RecoveryAction::ResolveWithSameIdempotencyKey
        ),
        ClientError::Application(error) => matches!(
            error.recovery_action(),
            riffdb_errors::ApplicationRecoveryAction::Retry
                | riffdb_errors::ApplicationRecoveryAction::ResolveWithSameIdempotencyKey
        ),
        ClientError::DetailsFree(DetailsFreeStatus::TransportUnavailable) => true,
        ClientError::DetailsFree(_)
        | ClientError::Protocol(_)
        | ClientError::IdentifierGeneration(_)
        | ClientError::ConnectionFailure
        | ClientError::Tls(_)
        | ClientError::OutcomeUnknown(_) => false,
    }
}

pub(crate) const fn carries_uncertainty(error: &ClientError) -> bool {
    matches!(
        error,
        ClientError::Public(error) if matches!(error.kind(), PublicErrorKind::OutcomeUnknown)
    ) || matches!(
        error,
        ClientError::Application(error)
            if matches!(error.code(), ApplicationErrorCode::OutcomeUnknown)
    ) || matches!(
        error,
        ClientError::DetailsFree(DetailsFreeStatus::TransportUnavailable)
            | ClientError::OutcomeUnknown(_)
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use riffdb_errors::ErrorClass;

    use super::*;

    #[derive(Debug)]
    struct LocalTransportFailure;

    impl fmt::Display for LocalTransportFailure {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("local channel failure")
        }
    }

    impl Error for LocalTransportFailure {}

    #[test]
    fn details_free_boundaries_require_exact_code_and_static_message() {
        assert!(matches!(
            checked_status(Status::new(Code::Unauthenticated, AUTHENTICATION_FAILED)),
            ClientError::DetailsFree(DetailsFreeStatus::Unauthenticated)
        ));
        assert!(matches!(
            checked_status(Status::new(Code::ResourceExhausted, RESPONSE_TOO_LARGE)),
            ClientError::DetailsFree(DetailsFreeStatus::ResponseTooLarge)
        ));
        assert!(matches!(
            checked_status(Status::new(Code::Internal, RESPONSE_TOO_LARGE)),
            ClientError::Protocol(ProtocolFailure {
                kind: ProtocolFailureKind::InvalidDetailsFreeStatus
            })
        ));
    }

    #[test]
    fn source_free_unavailable_without_public_error_details_fails_closed() {
        let canary = "peer supplied diagnostic secret";
        let error = checked_status(Status::new(Code::Unavailable, canary));
        assert!(!error.to_string().contains(canary));
        assert!(!is_retryable(&error));
        assert!(!carries_uncertainty(&error));
        assert_protocol_kind(error, ProtocolFailureKind::MissingPublicErrorDetails);
    }

    #[test]
    fn source_bearing_local_unavailable_is_transport_loss_without_trusting_message() {
        let canary = "local diagnostic secret";
        let mut status = Status::new(Code::Unavailable, canary);
        status.set_source(Arc::new(LocalTransportFailure));
        let error = checked_status(status);
        assert!(matches!(
            &error,
            ClientError::DetailsFree(DetailsFreeStatus::TransportUnavailable)
        ));
        assert!(is_retryable(&error));
        assert!(carries_uncertainty(&error));
        assert!(!error.to_string().contains(canary));
    }

    #[test]
    fn source_bearing_local_h2_protocol_failure_is_transport_loss() {
        let canary = "local h2 response-frame diagnostic";
        let mut status = Status::new(Code::Internal, canary);
        status.set_source(Arc::new(LocalTransportFailure));
        let error = checked_status(status);
        assert!(matches!(
            &error,
            ClientError::DetailsFree(DetailsFreeStatus::TransportUnavailable)
        ));
        assert!(is_retryable(&error));
        assert!(carries_uncertainty(&error));
        assert!(!error.to_string().contains(canary));

        let source_free = checked_status(Status::new(Code::Internal, canary));
        assert_protocol_kind(source_free, ProtocolFailureKind::MissingPublicErrorDetails);
    }

    #[test]
    fn missing_malformed_and_oversized_details_fail_closed() {
        assert_protocol_kind(
            checked_status(Status::new(Code::PermissionDenied, "authorization denied")),
            ProtocolFailureKind::MissingPublicErrorDetails,
        );
        assert_protocol_kind(
            checked_status(Status::with_details(
                Code::PermissionDenied,
                "authorization denied",
                vec![0xff].into(),
            )),
            ProtocolFailureKind::MalformedPublicErrorDetails,
        );
        assert_protocol_kind(
            checked_status(Status::with_details(
                Code::PermissionDenied,
                "authorization denied",
                vec![0_u8; MAX_PUBLIC_ERROR_BYTES + 1].into(),
            )),
            ProtocolFailureKind::OversizedPublicErrorDetails,
        );
    }

    #[test]
    fn checked_public_error_requires_consistent_status_and_message() {
        let details = authorization_denied_bytes();
        let checked = checked_status(Status::with_details(
            Code::PermissionDenied,
            "operation is not authorized",
            details.clone().into(),
        ));
        assert!(matches!(
            checked,
            ClientError::Public(ref error)
                if error.kind() == PublicErrorKind::AuthorizationDenied
        ));

        assert_protocol_kind(
            checked_status(Status::with_details(
                Code::InvalidArgument,
                "operation is not authorized",
                details.clone().into(),
            )),
            ProtocolFailureKind::StatusCodeMismatch,
        );
        assert_protocol_kind(
            checked_status(Status::with_details(
                Code::PermissionDenied,
                "caller supplied text",
                details.into(),
            )),
            ProtocolFailureKind::StatusMessageMismatch,
        );
    }

    #[test]
    fn checked_application_error_rejects_downgrade_status_and_message_drift() {
        let application = ApplicationError::new(
            ApplicationErrorCode::AuthorizationDenied,
            riffdb_errors::ApplicationOperation::ExecuteQuery,
            riffdb_errors::ApplicationErrorContext::empty(),
            None,
        );
        let details = riffdb_proto::encode_application_error(&application);
        let checked = checked_application_status(Status::with_details(
            Code::PermissionDenied,
            application.safe_message(),
            details.clone().into(),
        ));
        assert!(matches!(
            checked,
            ClientError::Application(ref error) if error.as_ref() == &application
        ));
        assert_protocol_kind(
            checked_application_status(Status::with_details(
                Code::InvalidArgument,
                application.safe_message(),
                details.clone().into(),
            )),
            ProtocolFailureKind::ApplicationStatusCodeMismatch,
        );
        assert_protocol_kind(
            checked_application_status(Status::with_details(
                Code::PermissionDenied,
                "peer supplied authorization detail",
                details.into(),
            )),
            ProtocolFailureKind::ApplicationStatusMessageMismatch,
        );

        let legacy_kernel_details = authorization_denied_bytes();
        assert_protocol_kind(
            checked_application_status(Status::with_details(
                Code::PermissionDenied,
                "operation is not authorized",
                legacy_kernel_details.into(),
            )),
            ProtocolFailureKind::InvalidApplicationErrorDetails,
        );
    }

    #[test]
    fn concurrency_deadline_status_preserves_the_complete_retry_contract() {
        let details = contextless_error_bytes(
            4,
            "concurrency_deadline_exceeded",
            "concurrency deadline exceeded",
            2,
        );
        let checked = checked_status(Status::with_details(
            Code::DeadlineExceeded,
            "concurrency deadline exceeded",
            details.into(),
        ));
        let ClientError::Public(error) = checked else {
            panic!("checked public error")
        };
        assert_eq!(error.kind(), PublicErrorKind::ConcurrencyDeadlineExceeded);
        assert_eq!(error.code(), "concurrency_deadline_exceeded");
        assert_eq!(error.safe_message(), "concurrency deadline exceeded");
        assert_eq!(
            error.recovery_action(),
            riffdb_errors::RecoveryAction::Retry
        );
        assert_eq!(error.class(), ErrorClass::DeadlineExceeded);
    }

    #[test]
    fn unknown_public_error_kind_never_becomes_retryable() {
        let details = contextless_error_bytes(99, "future", "future", 2);
        assert_protocol_kind(
            checked_status(Status::with_details(
                Code::Unavailable,
                "future",
                details.into(),
            )),
            ProtocolFailureKind::UnknownPublicErrorKind,
        );
    }

    #[test]
    fn staged_public_errors_preserve_retryability_and_certainty() {
        let overloaded = ClientError::Public(PublicError::overloaded());
        assert!(is_retryable(&overloaded));
        assert!(!carries_uncertainty(&overloaded));

        let mismatch = ClientError::Public(PublicError::history_incarnation_mismatch());
        assert!(!is_retryable(&mismatch));
        assert!(!carries_uncertainty(&mismatch));

        let pruned = ClientError::Public(PublicError::history_pruned());
        assert!(!is_retryable(&pruned));
        assert!(!carries_uncertainty(&pruned));

        let overloaded_app = ClientError::Application(Box::new(ApplicationError::new(
            ApplicationErrorCode::Overloaded,
            riffdb_errors::ApplicationOperation::ExecuteQuery,
            riffdb_errors::ApplicationErrorContext::empty(),
            None,
        )));
        assert!(is_retryable(&overloaded_app));
        assert!(!carries_uncertainty(&overloaded_app));

        let mismatch_app = ClientError::Application(Box::new(ApplicationError::new(
            ApplicationErrorCode::HistoryIncarnationMismatch,
            riffdb_errors::ApplicationOperation::ExecuteQuery,
            riffdb_errors::ApplicationErrorContext::empty(),
            None,
        )));
        assert!(!is_retryable(&mismatch_app));
        assert!(!carries_uncertainty(&mismatch_app));

        let pruned_app = ClientError::Application(Box::new(ApplicationError::new(
            ApplicationErrorCode::HistoryPruned,
            riffdb_errors::ApplicationOperation::ExecuteQuery,
            riffdb_errors::ApplicationErrorContext::empty(),
            None,
        )));
        assert!(!is_retryable(&pruned_app));
        assert!(!carries_uncertainty(&pruned_app));
    }

    #[test]
    fn staged_public_status_codes_accept_kind_level_overrides() {
        let overloaded_details =
            contextless_error_bytes(11, "overloaded", "service is over capacity", 2);
        let checked = checked_status(Status::with_details(
            Code::ResourceExhausted,
            "service is over capacity",
            overloaded_details.into(),
        ));
        assert!(matches!(
            checked,
            ClientError::Public(ref error) if error.kind() == PublicErrorKind::Overloaded
        ));

        let mismatch_details = contextless_error_bytes(
            10,
            "history_incarnation_mismatch",
            "observed history predates a database restore",
            1,
        );
        let checked = checked_status(Status::with_details(
            Code::FailedPrecondition,
            "observed history predates a database restore",
            mismatch_details.into(),
        ));
        assert!(matches!(
            checked,
            ClientError::Public(ref error)
                if error.kind() == PublicErrorKind::HistoryIncarnationMismatch
        ));

        let pruned_details =
            contextless_error_bytes(12, "history_pruned", "requested history has been pruned", 1);
        let checked = checked_status(Status::with_details(
            Code::FailedPrecondition,
            "requested history has been pruned",
            pruned_details.into(),
        ));
        assert!(matches!(
            checked,
            ClientError::Public(ref error) if error.kind() == PublicErrorKind::HistoryPruned
        ));
    }

    fn authorization_denied_bytes() -> Vec<u8> {
        contextless_error_bytes(3, "authorization_denied", "operation is not authorized", 4)
    }

    fn contextless_error_bytes(
        kind: u8,
        code: &str,
        safe_message: &str,
        recovery_action: u8,
    ) -> Vec<u8> {
        let mut bytes = Vec::new();
        push_varint_field(&mut bytes, 1, kind);
        push_string_field(&mut bytes, 2, code);
        push_string_field(&mut bytes, 3, safe_message);
        push_varint_field(&mut bytes, 4, recovery_action);
        bytes
    }

    fn push_varint_field(output: &mut Vec<u8>, field: u8, value: u8) {
        output.push(field << 3);
        output.push(value);
    }

    fn push_string_field(output: &mut Vec<u8>, field: u8, value: &str) {
        output.push((field << 3) | 2);
        output.push(u8::try_from(value.len()).expect("short fixture"));
        output.extend_from_slice(value.as_bytes());
    }

    fn assert_protocol_kind(error: ClientError, expected: ProtocolFailureKind) {
        assert!(matches!(
            error,
            ClientError::Protocol(failure) if failure.kind() == expected
        ));
    }
}
