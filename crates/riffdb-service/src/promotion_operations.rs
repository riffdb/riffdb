//! Promotion admission ends before its independently owned offline cutover.
//!
//! The returned completion owns no policy, reader, service graph or job-spawner
//! reference. Transports must also release their admitted service and security
//! handles before awaiting it, so the daemon can drain the old follower graph.

use std::sync::Arc;

use riffdb_auth::AuthenticatedPrincipal;
use riffdb_errors::PublicError;
use riffdb_policy::{AuthorizedReplicationPromotionPreparation, ReplicationPromotionDecision};
use riffdb_types::{DatabaseId, Environment, RequestId, ServiceIngressKindV1};

use crate::{
    CurrentPolicyPort, PortAdmissionError, PortReceipt, PromoteFollowerRequest,
    PromoteFollowerResult, RequestContext, ServiceFailure, ServiceFuture, ensure_response_budget,
};

/// Initial policy disposition, never authenticated source proof or a cutover permit.
pub enum FollowerPromotionAdmission {
    /// Exact current authority; the owner must reauthorize after draining.
    Authorized(Box<AuthorizedReplicationPromotionPreparation>),
    /// An explicit policy or ingress refusal which requires external denial audit.
    Denied,
    /// Current authority could not be established; no lifecycle work is allowed.
    Unavailable,
}

impl std::fmt::Debug for FollowerPromotionAdmission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FollowerPromotionAdmission([redacted])")
    }
}

/// Value-only authenticated attempt passed to the exclusive promotion owner.
///
/// The owner persists Attempted before lifecycle work, and persists a terminal
/// refusal before releasing a denied result. The authenticated principal's
/// timestamp records arrival; it cannot authorize a later cutover. Caller claims,
/// bearer credentials and access to the old service graph are deliberately absent.
pub struct FollowerPromotionSubmission {
    request: PromoteFollowerRequest,
    request_id: RequestId,
    principal: AuthenticatedPrincipal,
    ingress: ServiceIngressKindV1,
    admission: FollowerPromotionAdmission,
}

impl FollowerPromotionSubmission {
    /// Transfers bounded invocation facts and the initial decision to the owner.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        PromoteFollowerRequest,
        RequestId,
        AuthenticatedPrincipal,
        ServiceIngressKindV1,
        FollowerPromotionAdmission,
    ) {
        (
            self.request,
            self.request_id,
            self.principal,
            self.ingress,
            self.admission,
        )
    }
}

impl std::fmt::Debug for FollowerPromotionSubmission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FollowerPromotionSubmission([redacted])")
    }
}

/// Process-local completion failures; these introduce no durable outcome identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FollowerPromotionPortError {
    /// Durable external audit confirms an authorization refusal.
    AuthorizationDenied,
    /// Durable external audit confirms a conflicting operation selection.
    SelectionConflict,
    /// Required storage, source proof or validation is unavailable.
    Unavailable,
    /// Cutover may exist; exact committed reconciliation is required.
    OutcomeUnknown,
}

/// Narrow admission into the independently supervised promotion owner.
pub trait FollowerPromotionCoordinatorPort: Send + Sync {
    /// Synchronously accepts one bounded invocation, including denied attempts.
    ///
    /// Once accepted, the owner drives audit and any authorized lifecycle work
    /// independently of caller cancellation. It retains no follower service,
    /// authentication resolver or reader through the cutover. A success may be
    /// published only after complete source validation and receipt reconciliation.
    fn submit(
        &self,
        submission: FollowerPromotionSubmission,
    ) -> Result<PortReceipt<PromoteFollowerResult, FollowerPromotionPortError>, PortAdmissionError>;
}

/// Operator-only shared-service boundary for promotion, separate from ordinary
/// follower operations and their forbidden local database audit executor.
pub trait FollowerPromotionApplication: Send + Sync {
    /// Admits synchronously and returns an owned completion. The caller must drop
    /// its old route/service/security references before awaiting this completion.
    fn promote_follower(
        &self,
        context: RequestContext,
        request: PromoteFollowerRequest,
    ) -> ServiceFuture<'static, PromoteFollowerResult>;
}

/// API-neutral current-policy preparation with a graph-independent completion.
pub struct FollowerPromotionService {
    database_id: DatabaseId,
    environment: Environment,
    policy: Arc<dyn CurrentPolicyPort>,
    coordinator: Arc<dyn FollowerPromotionCoordinatorPort>,
}

impl FollowerPromotionService {
    /// Composes the existing policy layer with one exclusive lifecycle owner.
    /// Construction supplies no source proof, writer or readiness capability.
    #[must_use]
    pub fn new(
        database_id: DatabaseId,
        environment: Environment,
        policy: Arc<dyn CurrentPolicyPort>,
        coordinator: Arc<dyn FollowerPromotionCoordinatorPort>,
    ) -> Self {
        Self {
            database_id,
            environment,
            policy,
            coordinator,
        }
    }
}

impl std::fmt::Debug for FollowerPromotionService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FollowerPromotionService([redacted])")
    }
}

impl FollowerPromotionApplication for FollowerPromotionService {
    fn promote_follower(
        &self,
        context: RequestContext,
        request: PromoteFollowerRequest,
    ) -> ServiceFuture<'static, PromoteFollowerResult> {
        // Before protected submission, cancellation releases every non-durable
        // capability. After submit succeeds, only the exclusive owner may decide
        // the result; a disconnected caller cannot abandon its audit or cutover.
        if let Some(failure) = pre_submission_control(&context) {
            return Box::pin(async move { Err(failure) });
        }
        let admission = if context.ingress() == ServiceIngressKindV1::McpHttp
            || request.target().database_id() != self.database_id
        {
            FollowerPromotionAdmission::Denied
        } else {
            match self
                .policy
                .authorize_replication_promotion(context.principal(), request)
            {
                Ok(ReplicationPromotionDecision::Allow(preparation))
                    if preparation.request() == request
                        && preparation.principal() == context.principal()
                        && preparation.environment() == &self.environment =>
                {
                    FollowerPromotionAdmission::Authorized(preparation)
                }
                Ok(ReplicationPromotionDecision::Deny(_)) => FollowerPromotionAdmission::Denied,
                Ok(ReplicationPromotionDecision::Allow(_)) | Err(_) => {
                    FollowerPromotionAdmission::Unavailable
                }
            }
        };
        if let Some(failure) = pre_submission_control(&context) {
            return Box::pin(async move { Err(failure) });
        }
        let allowed = matches!(&admission, FollowerPromotionAdmission::Authorized(_));
        let submission = FollowerPromotionSubmission {
            request,
            request_id: context.request_id(),
            principal: context.principal().clone(),
            ingress: context.ingress(),
            admission,
        };
        let receipt = self.coordinator.submit(submission);
        // No self, context, policy, coordinator, authentication or old graph is
        // captured below. In particular, this is not an old-graph tracked job.
        Box::pin(async move {
            let receipt = receipt.map_err(|_| PublicError::storage_unavailable())?;
            let result = receipt
                .completion()
                .await
                .map_err(|_| PublicError::outcome_unknown())?
                .map_err(|error| match error {
                    FollowerPromotionPortError::AuthorizationDenied => {
                        PublicError::authorization_denied()
                    }
                    FollowerPromotionPortError::SelectionConflict => {
                        PublicError::idempotency_key_reuse()
                    }
                    FollowerPromotionPortError::Unavailable => PublicError::storage_unavailable(),
                    FollowerPromotionPortError::OutcomeUnknown => PublicError::outcome_unknown(),
                })?;
            if !allowed || !result.matches_request(request) {
                return Err(PublicError::outcome_unknown().into());
            }
            ensure_response_budget(&result)?;
            Ok(result)
        })
    }
}

fn pre_submission_control(context: &RequestContext) -> Option<ServiceFailure> {
    if context.control().is_cancelled() {
        Some(ServiceFailure::Cancelled)
    } else if context.control().is_deadline_exceeded() {
        Some(ServiceFailure::DeadlineExceeded)
    } else {
        None
    }
}

#[cfg(test)]
#[path = "promotion_operations_tests.rs"]
mod tests;
