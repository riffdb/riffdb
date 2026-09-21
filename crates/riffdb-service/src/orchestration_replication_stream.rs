//! Stream establishment uses the ordinary sole-coordinator audit lifecycle.
use super::*;
use crate::{ReplicationFailure, replication};
use riffdb_types::DatabaseId;

impl RiffDbServiceInner {
    pub(crate) async fn begin_replication_stream(
        &self,
        context: &RequestContext,
        database: DatabaseId,
        selection: Result<ServiceAuditTargetsV1, ReplicationFailure>,
    ) -> Result<BegunInvocationCompletion, ReplicationFailure> {
        let operation = ServiceOperationV1::StreamChangelog;
        let targets = selection
            .clone()
            .unwrap_or_else(|_| ServiceAuditTargetsV1::empty());
        self.classify_intrinsic_prestart(context, operation, targets.clone())
            .map_err(replication::from_service_failure)?;
        let refusal =
            if context.control().is_cancelled() || context.control().is_deadline_exceeded() {
                Some((
                    ServiceAuditPhaseV1::Cancelled,
                    ReplicationFailure::Unavailable,
                ))
            } else if let Err(error) = selection {
                Some((ServiceAuditPhaseV1::Failed, error))
            } else {
                replication::authorize_context(self, context, database)
                    .err()
                    .map(|error| {
                        let phase = if error == ReplicationFailure::AuthorizationDenied {
                            ServiceAuditPhaseV1::Denied
                        } else {
                            ServiceAuditPhaseV1::Failed
                        };
                        (phase, error)
                    })
            };
        if let Some((phase, error)) = refusal {
            let appended = self
                .append_prestart_terminal_if_intrinsic(
                    context,
                    operation,
                    targets,
                    AuditScope::Intrinsic,
                    phase,
                )
                .await;
            // A denial must not disclose audit availability (ADR-0007 matrix).
            if phase != ServiceAuditPhaseV1::Denied {
                appended.map_err(replication::from_service_failure)?;
            }
            return Err(error);
        }
        let lifecycle = current_operation_audit_lifecycle(operation);
        let terminal = PanicTerminalAudit::new(context, operation, targets.clone(), None)
            .map_err(|_| ReplicationFailure::Unavailable)?;
        lifecycle
            .prepare_start(operation, terminal)
            .map_err(|_| ReplicationFailure::Unavailable)?;
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
            return Err(ReplicationFailure::Unavailable);
        }
        lifecycle.mark_durable_start();
        lifecycle
            .confirm_start()
            .map_err(|_| ReplicationFailure::Unavailable)?;
        Ok(BegunInvocationCompletion {
            operation,
            targets,
            approval_id: None,
            started: true,
            lifecycle,
        })
    }
}

impl BegunInvocationCompletion {
    /// Before calling a source that can change retention custody, a panic must
    /// not claim that no authoritative result exists. No caller cancellation
    /// can discard the supervised source operation after this point.
    pub(crate) fn arm_replication_custody(
        &self,
        context: &RequestContext,
    ) -> Result<(), ReplicationFailure> {
        if self.operation != ServiceOperationV1::StreamChangelog {
            return Err(ReplicationFailure::Unavailable);
        }
        let terminal = PanicTerminalAudit {
            input: ServiceAuditInput::new(
                context,
                self.operation,
                ServiceAuditPhaseV1::OutcomeUncertain,
                self.targets.clone(),
                None,
                ServiceAuditLinkV1::None,
            )
            .map_err(|_| ReplicationFailure::Unavailable)?,
            deadline: context.control().deadline(),
        };
        let mut state = self
            .lifecycle
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !matches!(*state, OperationAuditState::Started(_)) {
            return Err(ReplicationFailure::Unavailable);
        }
        *state = OperationAuditState::Started(PendingTerminalAudit::Authenticated(terminal));
        Ok(())
    }
}
