//! Owned establishment jobs; continuation never appends another terminal row.
use super::*;
use crate::orchestration::BegunInvocationCompletion;
use crate::{
    RequestContext, RiffDbService, RiffDbServiceInner, ServiceFailure, port_completion_channel,
};
use riffdb_errors::{PublicError, PublicErrorKind};
use riffdb_types::{ServiceAuditLinkV1, ServiceAuditPhaseV1, ServiceOperationV1};

pub(super) fn submit<T: Send + 'static>(
    owner: &RiffDbService,
    ingress: riffdb_types::ServiceIngressKindV1,
    future: impl Future<Output = Result<T, ReplicationFailure>> + Send + 'static,
) -> ReplicationFuture<'static, T> {
    // Keep the specialized closed refusal on an existing one-shot receipt.
    // Supervision observes an ordinary ServiceResult, including real failures.
    let (sender, receipt) = port_completion_channel();
    let completion =
        owner.spawn_operation(ServiceOperationV1::StreamChangelog, ingress, async move {
            let result = future.await;
            let status = match &result {
                Ok(_) => Ok(()),
                Err(ReplicationFailure::AuthorizationDenied) => {
                    Err(PublicError::authorization_denied().into())
                }
                Err(_) => Err(PublicError::storage_unavailable().into()),
            };
            sender.complete(result);
            status
        });
    Box::pin(async move {
        let status = completion.await;
        let result = receipt.await.map_err(|_| ReplicationFailure::Unavailable)?;
        match (status, result) {
            (Ok(()), Ok(value)) => Ok(value),
            (Err(failure), Err(error)) if service_failure_class(&failure) == error_class(error) => {
                Err(error)
            }
            (Err(failure), _) => Err(from_service_failure(failure)),
            _ => Err(ReplicationFailure::Unavailable),
        }
    })
}

fn error_class(error: ReplicationFailure) -> ReplicationFailure {
    match error {
        ReplicationFailure::AuthorizationDenied => error,
        _ => ReplicationFailure::Unavailable,
    }
}

pub(crate) fn from_service_failure(error: ServiceFailure) -> ReplicationFailure {
    service_failure_class(&error)
}

fn service_failure_class(error: &ServiceFailure) -> ReplicationFailure {
    if error
        .public_error()
        .is_some_and(|error| error.kind() == PublicErrorKind::AuthorizationDenied)
    {
        ReplicationFailure::AuthorizationDenied
    } else {
        ReplicationFailure::Unavailable
    }
}

pub(super) async fn finish(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &BegunInvocationCompletion,
    phase: ServiceAuditPhaseV1,
) -> Result<(), ReplicationFailure> {
    if begun
        .finish(service, context, phase, ServiceAuditLinkV1::None)
        .await
        .is_err()
    {
        service.note_audit_failure(ServiceOperationV1::StreamChangelog);
        return Err(if phase == ServiceAuditPhaseV1::Denied {
            ReplicationFailure::AuthorizationDenied
        } else {
            ReplicationFailure::Unavailable
        });
    }
    Ok(())
}

pub(super) async fn establish(
    service: Arc<RiffDbServiceInner>,
    source: Arc<dyn ReplicationSourcePort>,
    context: RequestContext,
    request: ReplicationRequest,
) -> Result<ReplicationSubscription, ReplicationFailure> {
    let database_id = request.database_id;
    let begun = service
        .begin_replication_stream(&context, database_id, request.checked_audit_targets())
        .await?;
    if let Err(error) = authorize_context(&service, &context, database_id) {
        finish(&service, &context, &begun, failure_phase(error)).await?;
        return Err(error);
    }
    if context.control().is_cancelled() || context.control().is_deadline_exceeded() {
        finish(&service, &context, &begun, ServiceAuditPhaseV1::Cancelled).await?;
        return Err(ReplicationFailure::Unavailable);
    }
    let custody = matches!(
        request.phase,
        ReplicationPhase::Follower { .. }
            | ReplicationPhase::Bootstrap { .. }
            | ReplicationPhase::Attach { .. }
    );
    let emission = match request.phase {
        ReplicationPhase::Bootstrap { .. } => Emission::Manifest,
        ReplicationPhase::FenceEvidence { request: selection } => Emission::FenceEvidence(
            selection,
            ChangelogHistoryPointV3::new(
                riffdb_auth::ChangelogTransactionSequence::new(request.after_sequence)
                    .ok_or(ReplicationFailure::Unavailable)?,
                request.after_hash,
                request.after_frontier,
            ),
        ),
        _ => Emission::Tail,
    };
    if custody {
        begun.arm_replication_custody(&context)?;
    }
    // The source contract bounds its work and retains capacity through real
    // completion. Never drop this future because the transport disappeared.
    let source = match source.open(request).await {
        Ok(source) => ItemCustody::new(source),
        Err(error) => {
            let phase = if custody {
                ServiceAuditPhaseV1::OutcomeUncertain
            } else {
                failure_phase(error)
            };
            finish(&service, &context, &begun, phase).await?;
            return Err(error);
        }
    };
    if let Err(error) = authorize_context(&service, &context, database_id) {
        finish(&service, &context, &begun, failure_phase(error)).await?;
        return Err(error);
    }
    let cancelled = context.control().is_cancelled() || context.control().is_deadline_exceeded();
    if cancelled && !custody {
        finish(&service, &context, &begun, ServiceAuditPhaseV1::Cancelled).await?;
        return Err(ReplicationFailure::Unavailable);
    }
    let subscription = ReplicationSubscription {
        service: service.clone(),
        context,
        database_id,
        source: Some(source),
        emission,
        finished: false,
    };
    finish(
        &service,
        &subscription.context,
        &begun,
        ServiceAuditPhaseV1::Succeeded,
    )
    .await?;
    if cancelled
        || subscription.context.control().is_cancelled()
        || subscription.context.control().is_deadline_exceeded()
    {
        return Err(ReplicationFailure::Unavailable);
    }
    Ok(subscription)
}

pub(super) fn failure_phase(error: ReplicationFailure) -> ServiceAuditPhaseV1 {
    if error == ReplicationFailure::AuthorizationDenied {
        ServiceAuditPhaseV1::Denied
    } else {
        ServiceAuditPhaseV1::Failed
    }
}

pub(super) async fn observe_fence(
    service: Arc<RiffDbServiceInner>,
    source: Arc<dyn ReplicationSourcePort>,
    context: RequestContext,
    request: PrimaryFenceRequestV1,
    applied: ChangelogHistoryPointV3,
) -> Result<PrimaryFenceSourceEvidenceV1, ReplicationFailure> {
    let database = request.target().database_id();
    let targets = crate::ServiceAuditTargetMap::replication_follower(request.target())
        .map_err(|_| ReplicationFailure::Unavailable);
    let begun = service
        .begin_replication_stream(&context, database, targets)
        .await?;
    if let Err(error) = authorize_context(&service, &context, database) {
        finish(&service, &context, &begun, failure_phase(error)).await?;
        return Err(error);
    }
    // This port is a pure observation: cancelling its wait cannot abandon a
    // source-control mutation. The lower reader retains blocking-read capacity.
    let evidence = match crate::wait::wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        source.primary_fence_source_evidence(request, applied),
    )
    .await
    {
        Ok(Ok(evidence)) => evidence,
        Ok(Err(error)) => {
            finish(&service, &context, &begun, failure_phase(error)).await?;
            return Err(error);
        }
        Err(_) => {
            finish(&service, &context, &begun, ServiceAuditPhaseV1::Cancelled).await?;
            return Err(ReplicationFailure::Unavailable);
        }
    };
    if let Err(error) = authorize_context(&service, &context, database) {
        finish(&service, &context, &begun, failure_phase(error)).await?;
        return Err(error);
    }
    let fence = evidence.fence();
    if fence.target() != request.target()
        || fence.operation_id() != request.operation_id()
        || fence.generation() != request.generation()
        || evidence.applied() != applied
    {
        finish(&service, &context, &begun, ServiceAuditPhaseV1::Failed).await?;
        return Err(ReplicationFailure::Source(
            ReplicationStreamErrorV3::CorruptHistory,
        ));
    }
    if context.control().is_cancelled() || context.control().is_deadline_exceeded() {
        finish(&service, &context, &begun, ServiceAuditPhaseV1::Cancelled).await?;
        return Err(ReplicationFailure::Unavailable);
    }
    finish(&service, &context, &begun, ServiceAuditPhaseV1::Succeeded).await?;
    if context.control().is_cancelled() || context.control().is_deadline_exceeded() {
        return Err(ReplicationFailure::Unavailable);
    }
    Ok(evidence)
}
