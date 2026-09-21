#![forbid(unsafe_code)]
//! Exact typed fence refusal across both public error envelopes.
// req: REP-005
use riffdb_errors::{ApplicationError, ApplicationErrorContext, ApplicationOperation, PublicError};
use riffdb_proto::{
    app::v1 as app_v1, application_error_from_proto, application_error_to_proto,
    public_error_from_proto, public_error_to_proto, v1,
};

#[test]
fn primary_fenced_wire_preserves_exact_status_and_rejects_retry_or_other_kind() {
    let refusal = PublicError::primary_fenced();
    let wire = public_error_to_proto(&refusal);
    assert_eq!(wire.kind, 14);
    assert_eq!(wire.code, "primary_fenced");
    assert!(wire.details.is_none());
    assert_eq!(public_error_from_proto(&wire).unwrap(), refusal);
    for variant in 0..4 {
        let mut altered = wire.clone();
        match variant {
            0 => altered.recovery_action = v1::RecoveryAction::Retry as i32,
            1 => altered.kind = v1::PublicErrorKind::FollowerMode as i32,
            2 => altered.code = "storage_unavailable".into(),
            _ => altered.safe_message = "retry on the primary".into(),
        }
        assert!(public_error_from_proto(&altered).is_err());
    }
    let application = ApplicationError::from_public_error(
        &refusal,
        ApplicationOperation::ExecuteCommand,
        ApplicationErrorContext::empty(),
    );
    let wire = application_error_to_proto(&application);
    assert_eq!(wire.code, 27);
    assert!(wire.fixes.is_empty());
    assert_eq!(application_error_from_proto(&wire).unwrap(), application);
    for variant in 0..3 {
        let mut altered = wire.clone();
        match variant {
            0 => altered.recovery_action = app_v1::ApplicationRecoveryAction::Retry as i32,
            1 => altered.code = app_v1::ApplicationErrorCode::FollowerMode as i32,
            _ => altered
                .fixes
                .push(app_v1::ApplicationFixCode::RetryLater as i32),
        }
        assert!(application_error_from_proto(&altered).is_err());
    }
    // The additive members preserve their existing predecessors' wire numbers.
    assert_eq!(v1::PublicErrorKind::FollowerMode as i32, 13);
    assert_eq!(app_v1::ApplicationErrorCode::FollowerMode as i32, 26);
}
