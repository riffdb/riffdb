//! Exact source-mode retries never drain, contact a peer, or choose another cutover.
use std::sync::Arc;

use riffdb_auth::StoredPromotionAdministrationV1;
use riffdb_errors::PublicError;
use riffdb_types::{ServiceAuditLinkV1, ServiceAuditPhaseV1, ServiceOperationV1};

use crate::{
    FollowerPromotionApplication, PromoteFollowerRequest, PromoteFollowerResult, RequestContext,
    RiffDbService, RiffDbServiceInner, ServiceFailure, ServiceFuture, ServiceResult,
    ensure_response_budget,
};

/// Ordinary source service with its immutable, fully reconciled promotion control.
///
/// The server supplies this record only after complete source validation and exact
/// external receipt reconciliation. It survives for this graph's lifetime;
/// offline replacement drains that graph. Neither the record nor this wrapper
/// grants authorization, source readiness, or permission for a new cutover.
pub struct SourcePromotionRetryService {
    service: RiffDbService,
    record: Arc<StoredPromotionAdministrationV1>,
}

impl SourcePromotionRetryService {
    /// Composes current policy and normal source audit with a reconciled result.
    #[must_use]
    pub fn new(service: RiffDbService, record: StoredPromotionAdministrationV1) -> Self {
        Self {
            service,
            record: Arc::new(record),
        }
    }
}

impl FollowerPromotionApplication for SourcePromotionRetryService {
    fn promote_follower(
        &self,
        context: RequestContext,
        request: PromoteFollowerRequest,
    ) -> ServiceFuture<'static, PromoteFollowerResult> {
        let service = self.service.inner.clone();
        let record = self.record.clone();
        self.service.spawn_operation(
            ServiceOperationV1::PromoteFollower,
            context.ingress(),
            execute(service, context, request, record),
        )
    }
}

async fn execute(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request: PromoteFollowerRequest,
    record: Arc<StoredPromotionAdministrationV1>,
) -> ServiceResult<PromoteFollowerResult> {
    let begun = service
        .begin_promotion_retry_invocation(&context, request)
        .await?;
    // The durable-start wait is an authorization boundary. Reload current facts
    // before inspecting or releasing the old result, including exact retries.
    let checked = if context.control().is_cancelled() {
        Err((ServiceAuditPhaseV1::Cancelled, ServiceFailure::Cancelled))
    } else if context.control().is_deadline_exceeded() {
        Err((
            ServiceAuditPhaseV1::Cancelled,
            ServiceFailure::DeadlineExceeded,
        ))
    } else {
        service
            .prepare_promotion_retry(&context, request)
            .map(|_| ())
    };
    let result = checked.and_then(|()| {
        let original = record.attempt().request();
        let selected = record.attempt().selection().ok_or_else(unavailable)?;
        if original.target().database_id() != service.identity.database_id()
            || selected.published_lineage().history_incarnation()
                != service.identity.history_incarnation()
        {
            return Err(unavailable());
        }
        if original != request {
            return Err((
                ServiceAuditPhaseV1::Failed,
                if original.operation_id() == request.operation_id() {
                    PublicError::idempotency_key_reuse().into()
                } else {
                    PublicError::storage_unavailable().into()
                },
            ));
        }
        let result =
            PromoteFollowerResult::from_committed_record(&record, true).ok_or_else(unavailable)?;
        ensure_response_budget(&result)
            .map_err(|failure| (ServiceAuditPhaseV1::Failed, failure))?;
        Ok(result)
    });
    let (phase, link) = match &result {
        Ok(_) => (
            ServiceAuditPhaseV1::Succeeded,
            ServiceAuditLinkV1::ControlPlane {
                administration_sequence: record.administration_sequence(),
            },
        ),
        Err((phase, _)) => (*phase, ServiceAuditLinkV1::None),
    };
    if begun.finish(&service, &context, phase, link).await.is_err() {
        service.note_audit_failure(ServiceOperationV1::PromoteFollower);
        return Err(match result {
            Err((ServiceAuditPhaseV1::Denied, failure)) => failure,
            _ => PublicError::storage_unavailable().into(),
        });
    }
    result.map_err(|(_, failure)| failure)
}

fn unavailable() -> (ServiceAuditPhaseV1, ServiceFailure) {
    (
        ServiceAuditPhaseV1::Failed,
        PublicError::storage_unavailable().into(),
    )
}
