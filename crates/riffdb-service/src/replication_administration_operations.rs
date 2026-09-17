//! Audited follower lifecycle calls through the sole coordinator.
use crate::orchestration::BegunInvocationCompletion;
use crate::wait::{ControlledWaitError, wait_with_control};
use crate::{
    AuthoritativeReadinessFailure, FollowerAdministrationResult, PendingTerminalResponse,
    RequestContext, RiffDbServiceInner, ServiceFailure, ServiceResult, ensure_response_budget,
};
use riffdb_auth::ReplicationAdministrationRequestV1;
use riffdb_commit::{
    ControlPlaneExecutionAdmissionError, ControlPlaneExecutionErrorKind, ControlPlaneTerminalAudit,
};
use riffdb_errors::PublicError;
use riffdb_types::{ServiceAuditLinkV1, ServiceAuditPhaseV1};
use std::sync::Arc;

pub(crate) async fn execute(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request: ReplicationAdministrationRequestV1,
) -> ServiceResult<FollowerAdministrationResult> {
    let writer = service.executors.writer()?;
    let begun = service
        .begin_replication_administration_invocation(&context, request)
        .await?;
    let permit = match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        writer.control_plane.reserve_capacity(),
    )
    .await
    {
        Ok(Ok(permit)) => permit,
        Ok(Err(error)) => return Err(admission_failure(&service, &context, &begun, error).await),
        Err(error) => {
            let failure = match error {
                ControlledWaitError::Cancelled => ServiceFailure::Cancelled,
                ControlledWaitError::DeadlineExceeded => ServiceFailure::DeadlineExceeded,
            };
            return Err(finish_failure(
                &service,
                &context,
                &begun,
                ServiceAuditPhaseV1::Cancelled,
                failure,
            )
            .await);
        }
    };
    let preparation = match service.prepare_replication_administration(&context, request) {
        Ok(preparation) => preparation,
        Err((phase, failure)) => {
            return Err(finish_failure(&service, &context, &begun, phase, failure).await);
        }
    };
    let receipt = match permit.submit_replication_administration(*preparation) {
        Ok(receipt) => receipt,
        Err(error) => return Err(admission_failure(&service, &context, &begun, error).await),
    };
    // Accepted work must drain: cancellation cannot discard a durable outcome.
    let execution = match receipt.completion().await {
        Ok(execution) => execution,
        Err(error) => {
            let (phase, failure) = match error.kind() {
                ControlPlaneExecutionErrorKind::AuthorizationDenied => (
                    ServiceAuditPhaseV1::Denied,
                    PublicError::authorization_denied(),
                ),
                ControlPlaneExecutionErrorKind::OutcomeUnknown => (
                    ServiceAuditPhaseV1::OutcomeUncertain,
                    PublicError::outcome_unknown(),
                ),
                _ => (
                    ServiceAuditPhaseV1::Failed,
                    PublicError::storage_unavailable(),
                ),
            };
            if matches!(
                error.kind(),
                ControlPlaneExecutionErrorKind::OutcomeUnknown
                    | ControlPlaneExecutionErrorKind::CoordinatorFenced
                    | ControlPlaneExecutionErrorKind::InternalDefect
            ) {
                service
                    .providers
                    .health
                    .fail_authoritative_readiness(AuthoritativeReadinessFailure::CoordinatorFenced);
            }
            return Err(finish_failure(&service, &context, &begun, phase, failure.into()).await);
        }
    };
    let result = execution.public_outcome();
    let pending =
        PendingTerminalResponse::new(result, execution.terminal_audit(), ensure_response_budget);
    let (phase, link, failure) = match pending.terminal() {
        ControlPlaneTerminalAudit::Succeeded(link) => (
            ServiceAuditPhaseV1::Succeeded,
            link,
            PublicError::outcome_unknown(),
        ),
        ControlPlaneTerminalAudit::Failed => (
            ServiceAuditPhaseV1::Failed,
            ServiceAuditLinkV1::None,
            PublicError::storage_unavailable(),
        ),
    };
    if begun.finish(&service, &context, phase, link).await.is_err() {
        service.note_audit_failure(begun.operation());
        return Err(failure.into());
    }
    pending.into_response()
}

async fn admission_failure(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &BegunInvocationCompletion,
    error: ControlPlaneExecutionAdmissionError,
) -> ServiceFailure {
    if error == ControlPlaneExecutionAdmissionError::Fenced {
        service
            .providers
            .health
            .fail_authoritative_readiness(AuthoritativeReadinessFailure::CoordinatorFenced);
    }
    finish_failure(
        service,
        context,
        begun,
        ServiceAuditPhaseV1::Failed,
        PublicError::storage_unavailable().into(),
    )
    .await
}

async fn finish_failure(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &BegunInvocationCompletion,
    phase: ServiceAuditPhaseV1,
    failure: ServiceFailure,
) -> ServiceFailure {
    if begun
        .finish(service, context, phase, ServiceAuditLinkV1::None)
        .await
        .is_err()
    {
        service.note_audit_failure(begun.operation());
        if matches!(
            phase,
            ServiceAuditPhaseV1::Denied | ServiceAuditPhaseV1::OutcomeUncertain
        ) {
            return failure;
        }
        return PublicError::storage_unavailable().into();
    }
    failure
}
