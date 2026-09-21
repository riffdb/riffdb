//! Audited follower lifecycle calls through the sole coordinator.
use crate::orchestration::BegunInvocationCompletion;
use crate::wait::{ControlledWaitError, wait_with_control};
use crate::{
    AuthoritativeReadinessFailure, FenceReplicationPrimaryResult, PendingTerminalResponse,
    RequestContext, RiffDbServiceInner, ServiceFailure, ServiceResult, ensure_response_budget,
};
use riffdb_auth::PrimaryFenceRequestV1;
use riffdb_commit::{
    ControlPlaneExecutionAdmissionError, ControlPlaneExecutionErrorKind, ControlPlaneTerminalAudit,
};
use riffdb_errors::PublicError;
use riffdb_types::{ServiceAuditLinkV1, ServiceAuditPhaseV1};
use std::sync::Arc;

pub(crate) async fn execute(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request: PrimaryFenceRequestV1,
) -> ServiceResult<FenceReplicationPrimaryResult> {
    let writer = service.executors.writer()?;
    let begun = service
        .begin_primary_fence_invocation(&context, request)
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
    let preparation = match service.prepare_primary_fence(&context, request) {
        Ok(preparation) => preparation,
        Err((phase, failure)) => {
            return Err(finish_failure(&service, &context, &begun, phase, failure).await);
        }
    };
    let receipt = match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        permit.submit_primary_fence(*preparation),
    )
    .await
    {
        Ok(Ok(receipt)) => receipt,
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
    if error == ControlPlaneExecutionAdmissionError::PrimaryFenced {
        if begun
            .finish(
                service,
                context,
                ServiceAuditPhaseV1::Denied,
                ServiceAuditLinkV1::None,
            )
            .await
            .is_err()
        {
            service.note_audit_failure(begun.operation());
            return PublicError::storage_unavailable().into();
        }
        return PublicError::primary_fenced().into();
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::primary_admission_test_support::{ServiceHarness, database_id, run_async};
    use crate::{AdministrationApplication, FenceReplicationPrimaryRequest, RequestControl};
    use riffdb_types::{
        LeadershipEpochV1, ReplicationFenceOperationId, ReplicationFollowerAuditTargetV1,
        ReplicationSourceHoldIdV1, ServiceIngressKindV1, ServiceOperationV1,
    };

    #[test]
    // req: REP-005
    fn primary_fence_service_audits_prestart_cancellation_and_forbidden_mcp() {
        run_async(async {
            for cancelled in [false, true] {
                let mut harness = ServiceHarness::operations();
                let (original, _) = harness.context(0xa4);
                let (control, cancellation) = RequestControl::new(
                    std::time::Instant::now() + std::time::Duration::from_secs(30),
                );
                if cancelled {
                    cancellation.cancel();
                }
                let context = RequestContext::new(
                    original.request_id(),
                    original.principal().clone(),
                    if cancelled {
                        ServiceIngressKindV1::Grpc
                    } else {
                        ServiceIngressKindV1::McpHttp
                    },
                    riffdb_policy::UntrustedInvocationClaims::new(None, None, None, None, None),
                    control,
                    None,
                );
                let target = ReplicationFollowerAuditTargetV1::new(
                    database_id(),
                    1,
                    LeadershipEpochV1::initial(),
                    ReplicationSourceHoldIdV1::new([0x71; 16]).unwrap(),
                )
                .unwrap();
                let operation =
                    ReplicationFenceOperationId::from_bytes(*original.request_id().as_bytes())
                        .unwrap();
                let request = FenceReplicationPrimaryRequest::new(
                    operation,
                    target,
                    riffdb_auth::ChangelogTransactionSequence::new(1).unwrap(),
                );
                let error = harness
                    .service
                    .fence_replication_primary(context, request)
                    .await
                    .unwrap_err();
                if cancelled {
                    assert!(matches!(error, ServiceFailure::Cancelled));
                } else {
                    assert_eq!(
                        error.public_error().unwrap().kind(),
                        riffdb_errors::PublicErrorKind::AuthorizationDenied
                    );
                }
                harness.stop_coordinator();
                let phase = if cancelled {
                    ServiceAuditPhaseV1::Cancelled
                } else {
                    ServiceAuditPhaseV1::Denied
                };
                assert_eq!(harness.audit_phases(0xa4), vec![phase]);
                for record in harness.audit_records(0xa4) {
                    assert_eq!(
                        record.operation(),
                        ServiceOperationV1::FenceReplicationPrimary
                    );
                    assert_eq!(
                        record.targets().as_slice(),
                        &[riffdb_types::ServiceAuditTargetV1::ReplicationFollower(
                            target
                        )]
                    );
                    assert_eq!(record.link(), ServiceAuditLinkV1::None);
                }
            }
        });
    }
}
