#![forbid(unsafe_code)]
//! Durable primary fencing is a bounded refusal, never retry or unfence authority.
// req: REP-005
use riffdb_errors::{
    ApplicationError, ApplicationErrorCategory, ApplicationErrorCode, ApplicationErrorContext,
    ApplicationOperation, ApplicationRecoveryAction, ErrorClass, PublicError, PublicErrorDetails,
    PublicErrorKind, PublicErrorStatusCode, RecoveryAction,
};

#[test]
fn primary_fenced_is_a_contextless_durable_refusal_without_retry_or_unfence_guidance() {
    let error = PublicError::primary_fenced();
    assert_eq!(error.kind(), PublicErrorKind::PrimaryFenced);
    assert_eq!(error.code(), "primary_fenced");
    assert_eq!(error.safe_message(), "primary is durably fenced");
    assert_eq!(error.details(), &PublicErrorDetails::None);
    assert!(error.incident_id().is_none());
    assert_eq!(error.kind().class(), ErrorClass::FailedPrecondition);
    assert_eq!(
        error.kind().status_code(),
        PublicErrorStatusCode::FailedPrecondition
    );
    assert_eq!(
        error.kind().recovery_action(),
        RecoveryAction::ContactOperator
    );
    let application = ApplicationError::from_public_error(
        &error,
        ApplicationOperation::ExecuteCommand,
        ApplicationErrorContext::empty(),
    );
    let code = application.code();
    assert_eq!(code, ApplicationErrorCode::PrimaryFenced);
    assert_eq!(code.as_str(), "RDB-REP-0102");
    assert_eq!(code.category(), ApplicationErrorCategory::Control);
    assert_eq!(
        code.recovery_action(),
        ApplicationRecoveryAction::ContactOperator
    );
    assert!(code.fixes().is_empty());
    assert!(
        error
            .clone()
            .with_application_code_hint(ApplicationErrorCode::FollowerMode)
            .is_err()
    );
    assert!(error.with_application_code_hint(code).is_ok());
}
