//! Current administrative authority for an explicit follower promotion attempt.
use super::replication_administration::evaluate;
use super::*;
use riffdb_auth::ReplicationPromotionRequestV1;

/// Move-only initial authorization bound to the entire immutable request.
/// This is not fence proof or a cutover permit. The exclusive promotion owner
/// must recheck authority against its drained applied database before cutover.
pub struct AuthorizedReplicationPromotionPreparation {
    principal: AuthenticatedPrincipal,
    request: ReplicationPromotionRequestV1,
    environment: Environment,
    authorized_at: Timestamp,
}

impl AuthorizedReplicationPromotionPreparation {
    /// Consumes preparation against the drained database's current capability.
    /// The exclusive owner supplies those digest-free facts and fresh time.
    /// This pure check performs no resolver lookup, I/O or cached authorization.
    pub fn reauthorize(
        self,
        current: &TransactionCurrentCapabilityFacts,
        now: Timestamp,
    ) -> Result<AuthorizedReplicationPromotion, PolicyCode> {
        let facts = CurrentFacts {
            capability_id: current.capability_id(),
            revision: current.revision(),
            activity: match current.activity() {
                CapabilityActivity::Active => CurrentCapabilityActivity::Active,
                CapabilityActivity::Revoked => CurrentCapabilityActivity::Revoked,
            },
            database_id: current.database_id(),
            environment: current.environment().clone(),
            principal_id: current.principal_id().clone(),
            actor_kind: current.actor_kind(),
            audiences: current.audiences().to_vec(),
            issued_at: current.issued_at(),
            expires_at: current.expires_at(),
            grant: current.grant().clone(),
        };
        evaluate(
            &PrincipalFacts::from(&self.principal),
            &facts,
            self.request.target().database_id(),
            &self.environment,
            now,
            self.request.target().database_id(),
        )?;
        Ok(AuthorizedReplicationPromotion {
            preparation: self,
            timestamp: now,
        })
    }
    /// Exact request whose operation, target and generation were admitted.
    #[must_use]
    pub const fn request(&self) -> ReplicationPromotionRequestV1 {
        self.request
    }

    /// Digest-free principal to recheck from transaction-current capability state.
    #[must_use]
    pub const fn principal(&self) -> &AuthenticatedPrincipal {
        &self.principal
    }

    /// Exact environment checked for the preparation.
    #[must_use]
    pub const fn environment(&self) -> &Environment {
        &self.environment
    }

    /// Fresh sample used for this preparation, never authority for a later write.
    #[must_use]
    pub const fn authorized_at(&self) -> Timestamp {
        self.authorized_at
    }
}

/// Move-only successful final decision; only pure reauthorization constructs it.
#[derive(Debug)]
pub struct AuthorizedReplicationPromotion {
    preparation: AuthorizedReplicationPromotionPreparation,
    timestamp: Timestamp,
}
impl AuthorizedReplicationPromotion {
    /// Lowers the unchanged request/principal and the one final audit timestamp.
    #[must_use]
    pub fn into_parts(self) -> (AuthorizedReplicationPromotionPreparation, Timestamp) {
        (self.preparation, self.timestamp)
    }
}

impl fmt::Debug for AuthorizedReplicationPromotionPreparation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AuthorizedReplicationPromotionPreparation([REDACTED])")
    }
}

/// Closed preparation result; denial grants no receiver drain or cutover.
#[derive(Debug)]
pub enum ReplicationPromotionDecision {
    /// Exact request passed fresh initial authorization.
    Allow(Box<AuthorizedReplicationPromotionPreparation>),
    /// A closed internal policy refusal, safe to classify without request data.
    Deny(PolicyCode),
}

impl<R, C, T> CurrentAuthorizer<'_, R, C, T>
where
    R: CurrentCapabilityResolver + ?Sized,
    C: AuthorizationClock + ?Sized,
    T: AuthorizationTelemetry + ?Sized,
{
    /// Reloads current capability and fresh time for the complete explicit
    /// promotion request. Streaming or primary-fencing permission alone grants no promotion.
    pub fn authorize_replication_promotion(
        &self,
        principal: &AuthenticatedPrincipal,
        request: ReplicationPromotionRequestV1,
    ) -> Result<ReplicationPromotionDecision, AuthorizationError> {
        let current = self.resolver.resolve_current(principal).map_err(|_| {
            self.telemetry.record(AuthorizationTelemetryEvent::Defect(
                AuthorizationDefect::CurrentCapabilityUnavailable,
            ));
            AuthorizationError::CurrentCapabilityUnavailable
        })?;
        let now = self.clock.now().map_err(|_| {
            self.telemetry.record(AuthorizationTelemetryEvent::Defect(
                AuthorizationDefect::ClockUnavailable,
            ));
            AuthorizationError::ClockUnavailable
        })?;
        match evaluate(
            &PrincipalFacts::from(principal),
            &CurrentFacts::from(&current),
            self.expected_database_id,
            &self.expected_environment,
            now,
            request.target().database_id(),
        ) {
            Ok(()) => Ok(ReplicationPromotionDecision::Allow(Box::new(
                AuthorizedReplicationPromotionPreparation {
                    principal: principal.clone(),
                    request,
                    environment: self.expected_environment.clone(),
                    authorized_at: now,
                },
            ))),
            Err(code) => {
                self.telemetry
                    .record(AuthorizationTelemetryEvent::Denied(code.into()));
                Ok(ReplicationPromotionDecision::Deny(code))
            }
        }
    }
}
