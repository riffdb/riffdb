//! Bounded, graph-independent handoff to the exclusive daemon promotion owner.
//!
//! Admission persists only external audit. It cannot drain a receiver, validate
//! TLS proof, mutate a database, or publish source readiness.

use std::sync::Arc;

use riffdb_policy::AuthorizedReplicationPromotionPreparation;
use riffdb_service::{
    FollowerPromotionAdmission, FollowerPromotionCoordinatorPort, FollowerPromotionPortError,
    FollowerPromotionSubmission, PortAdmissionError, PortCompletionSender, PortReceipt,
    PromoteFollowerResult, port_completion_channel,
};
use riffdb_storage_api::{
    AuditPrincipalV1, ReplicationPromotionFailureV1 as Failure,
    ReplicationPromotionReceiptV1 as Receipt, ReplicationPromotionStepV1 as Step, StorageError,
    StorageErrorKind,
};
use riffdb_storage_redb::RedbMaintenanceStorage;
use riffdb_types::{Environment, ReplicationFollowerAuditTargetV1, ServiceIngressKindV1};
use tokio::sync::mpsc;

type Completion = PortCompletionSender<PromoteFollowerResult, FollowerPromotionPortError>;

/// Contains only a bounded sender; it cannot keep a reader or old graph alive.
pub(crate) struct PromotionController {
    sender: mpsc::Sender<PromotionTrigger>,
}

impl PromotionController {
    pub(crate) fn channel() -> (Arc<Self>, mpsc::Receiver<PromotionTrigger>) {
        let (sender, receiver) = mpsc::channel(1);
        (Arc::new(Self { sender }), receiver)
    }
}

impl FollowerPromotionCoordinatorPort for PromotionController {
    fn submit(
        &self,
        submission: FollowerPromotionSubmission,
    ) -> Result<PortReceipt<PromoteFollowerResult, FollowerPromotionPortError>, PortAdmissionError>
    {
        let permit = self.sender.try_reserve().map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => PortAdmissionError::Unavailable,
            mpsc::error::TrySendError::Closed(_) => PortAdmissionError::Stopped,
        })?;
        let (completion, receipt) = port_completion_channel();
        permit.send(PromotionTrigger {
            submission,
            completion,
        });
        Ok(receipt)
    }
}

pub(crate) struct PromotionTrigger {
    submission: FollowerPromotionSubmission,
    completion: Completion,
}

/// Freshly allowed request plus durable external Attempted evidence. The daemon
/// must still obtain authenticated fence proof before drain, reauthorize from
/// drained current capability facts, and freeze the exact checked selection.
pub(crate) struct AuditedPromotion {
    pub(crate) attempt: Receipt,
    pub(crate) authorization: Box<AuthorizedReplicationPromotionPreparation>,
    pub(crate) ingress: ServiceIngressKindV1,
    pub(crate) completion: Completion,
}

impl PromotionTrigger {
    /// Called by the sole lifecycle owner before changing ordinary admission.
    /// A storage error requires quarantine: even an initial receipt publication
    /// may be uncertain. No receiver or reader has been changed by this method.
    pub(crate) fn audit(
        self,
        storage: &mut RedbMaintenanceStorage,
        expected_target: ReplicationFollowerAuditTargetV1,
        environment: &Environment,
    ) -> Result<Option<AuditedPromotion>, StorageError> {
        let Self {
            submission,
            completion,
        } = self;
        let (request, request_id, principal, ingress, admission) = submission.into_parts();
        let mut attempt = Receipt::attempted(
            request,
            request_id,
            AuditPrincipalV1::new(
                principal.principal_id().clone(),
                principal.actor_kind(),
                principal.capability_id(),
                principal.capability_revision(),
            ),
            // Required approval is refused by the accepted current policy model;
            // untrusted caller claims cannot create an approval identity here.
            None,
            principal.authenticated_at(),
        );
        let prepared = (|| {
            let inventory = storage.promotion_receipts()?;
            // A transport request ID cannot acquire a second actor or lifecycle.
            // Its retained row is not replaced by newly authenticated facts.
            if inventory
                .receipts()
                .iter()
                .any(|row| row.request_id() == request_id)
            {
                return Err(StorageError::new(
                    StorageErrorKind::InvariantViolation,
                    None,
                ));
            }
            let (authorization, terminal) = match admission {
                FollowerPromotionAdmission::Denied => (
                    None,
                    Some((
                        Step::Denied(Failure::AuthorizationDenied),
                        FollowerPromotionPortError::AuthorizationDenied,
                    )),
                ),
                FollowerPromotionAdmission::Unavailable => (
                    None,
                    Some((
                        if inventory
                            .request_for(request.operation_id())
                            .is_some_and(|original| original != request)
                        {
                            // Retain the conflicting authenticated attempt without
                            // exposing selection facts when current policy failed.
                            Step::Denied(Failure::SelectionConflict)
                        } else {
                            Step::FailedClosed(Failure::StorageUnavailable)
                        },
                        FollowerPromotionPortError::Unavailable,
                    )),
                ),
                FollowerPromotionAdmission::Authorized(authorization) => {
                    if authorization.request() != request
                        || authorization.principal() != &principal
                        || authorization.environment() != environment
                        || !matches!(
                            ingress,
                            ServiceIngressKindV1::Grpc
                                | ServiceIngressKindV1::InProcessTestComparison
                        )
                    {
                        (
                            None,
                            Some((
                                Step::FailedClosed(Failure::StorageUnavailable),
                                FollowerPromotionPortError::Unavailable,
                            )),
                        )
                    } else if inventory
                        .request_for(request.operation_id())
                        .is_some_and(|original| original != request)
                    {
                        (
                            None,
                            Some((
                                Step::Denied(Failure::SelectionConflict),
                                FollowerPromotionPortError::SelectionConflict,
                            )),
                        )
                    } else if request.target() != expected_target {
                        (
                            None,
                            Some((
                                Step::FailedClosed(Failure::FenceInvalid),
                                FollowerPromotionPortError::Unavailable,
                            )),
                        )
                    } else {
                        (Some(authorization), None)
                    }
                }
            };
            if let Some((step, _)) = terminal {
                if matches!(step, Step::FailedClosed(_)) {
                    // The existing ledger permits a new terminal denial in one
                    // publication. A failed attempt must first durably exist;
                    // terminal failure monotonically extends that original row.
                    storage.persist_promotion_receipt(&attempt)?;
                }
                attempt
                    .advance(step)
                    .map_err(|_| StorageError::new(StorageErrorKind::InvariantViolation, None))?;
            }
            // A conflicting request enters atomically with Denied. Its bare
            // Attempted phase must never replace the frozen operation request.
            storage.persist_promotion_receipt(&attempt)?;
            Ok((authorization, terminal))
        })();
        match prepared {
            Ok((Some(authorization), None)) => Ok(Some(AuditedPromotion {
                attempt,
                authorization,
                ingress,
                completion,
            })),
            Ok((None, Some((_, failure)))) => {
                completion.complete(Err(failure));
                Ok(None)
            }
            Ok(_) => {
                completion.complete(Err(FollowerPromotionPortError::Unavailable));
                Err(StorageError::new(
                    StorageErrorKind::InvariantViolation,
                    None,
                ))
            }
            Err(error) => {
                completion.complete(Err(FollowerPromotionPortError::Unavailable));
                Err(error)
            }
        }
    }
}

#[cfg(test)]
#[path = "promotion_admission_tests.rs"]
mod tests;
