//! Intrinsically audited lifecycle preparation, retaining the exact request target.
use super::*;
use crate::ServiceAuditTargetMap;
use riffdb_auth::PrimaryFenceRequestV1;
use riffdb_policy::{AuthorizedPrimaryFencePreparation, PrimaryFenceDecision};
use riffdb_types::ServiceIngressKindV1;

pub(crate) fn operation(_: PrimaryFenceRequestV1) -> ServiceOperationV1 {
    ServiceOperationV1::FenceReplicationPrimary
}

impl RiffDbServiceInner {
    pub(crate) fn prepare_primary_fence(
        &self,
        context: &RequestContext,
        request: PrimaryFenceRequestV1,
    ) -> Result<Box<AuthorizedPrimaryFencePreparation>, (ServiceAuditPhaseV1, ServiceFailure)> {
        if context.ingress() == ServiceIngressKindV1::McpHttp {
            return Err((
                ServiceAuditPhaseV1::Denied,
                PublicError::authorization_denied().into(),
            ));
        }
        let preparation = match self
            .providers
            .policy
            .authorize_primary_fence(context.principal(), request)
        {
            Ok(PrimaryFenceDecision::Allow(preparation)) => preparation,
            Ok(PrimaryFenceDecision::Deny(_)) => {
                return Err((
                    ServiceAuditPhaseV1::Denied,
                    PublicError::authorization_denied().into(),
                ));
            }
            Err(_) => {
                return Err((
                    ServiceAuditPhaseV1::Failed,
                    PublicError::storage_unavailable().into(),
                ));
            }
        };
        if preparation.request() != request
            || request.request_id() != context.request_id()
            || preparation.principal() != context.principal()
            || preparation.environment() != self.identity.environment()
            || request.target().database_id() != self.identity.database_id()
        {
            return Err((
                ServiceAuditPhaseV1::Failed,
                self.internal_failure(operation(request), InternalDefect::ProofMismatch),
            ));
        }
        Ok(preparation)
    }

    pub(crate) async fn begin_primary_fence_invocation(
        &self,
        context: &RequestContext,
        request: PrimaryFenceRequestV1,
    ) -> ServiceResult<BegunInvocationCompletion> {
        let operation = operation(request);
        let targets = ServiceAuditTargetMap::replication_follower(request.target())
            .map_err(|_| self.internal_failure(operation, InternalDefect::ProofMismatch))?;
        self.classify_intrinsic_prestart(context, operation, targets.clone())?;
        if context.control().is_cancelled() || context.control().is_deadline_exceeded() {
            self.append_prestart_terminal_if_intrinsic(
                context,
                operation,
                targets,
                AuditScope::Intrinsic,
                ServiceAuditPhaseV1::Cancelled,
            )
            .await?;
            return Err(if context.control().is_cancelled() {
                ServiceFailure::Cancelled
            } else {
                ServiceFailure::DeadlineExceeded
            });
        }
        if let Err((phase, failure)) = self.prepare_primary_fence(context, request) {
            self.append_prestart_terminal_if_intrinsic(
                context,
                operation,
                targets,
                AuditScope::Intrinsic,
                phase,
            )
            .await?;
            return Err(failure);
        }
        let lifecycle = current_operation_audit_lifecycle(operation);
        let panic_terminal = PanicTerminalAudit::new(context, operation, targets.clone(), None)
            .map_err(|_| self.internal_failure(operation, InternalDefect::ProofMismatch))?;
        lifecycle
            .prepare_start(operation, panic_terminal)
            .map_err(|_| self.internal_failure(operation, InternalDefect::ProofMismatch))?;
        if let Err(failure) = self
            .append_audit(
                context,
                operation,
                ServiceAuditPhaseV1::Started,
                targets.clone(),
                None,
                ServiceAuditLinkV1::None,
                AuditAppendControl::Invocation,
            )
            .await
        {
            lifecycle.fail_start();
            self.note_audit_failure_with_cause(operation, failure.cause());
            return Err(PublicError::storage_unavailable().into());
        }
        lifecycle.mark_durable_start();
        lifecycle
            .confirm_start()
            .map_err(|_| self.internal_failure(operation, InternalDefect::ProofMismatch))?;
        Ok(BegunInvocationCompletion {
            operation,
            targets,
            approval_id: None,
            started: true,
            lifecycle,
        })
    }
}
