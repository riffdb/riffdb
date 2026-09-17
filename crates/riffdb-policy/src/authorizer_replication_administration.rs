//! Fresh preparation for an explicit, audited follower lifecycle operation.
use super::*;
use riffdb_auth::ReplicationAdministrationRequestV1;

/// Move-only initial authorization bound to the entire immutable request.
/// This is not a write permit. The coordinator must recheck current authority
/// from its drained transaction, including on exact replay, before committing.
pub struct AuthorizedReplicationAdministrationPreparation {
    principal: AuthenticatedPrincipal,
    request: ReplicationAdministrationRequestV1,
    environment: Environment,
    authorized_at: Timestamp,
}

impl AuthorizedReplicationAdministrationPreparation {
    /// Consumes initial preparation at the drained transaction's safe point.
    /// Commit supplies digest-free facts from that transaction and samples its
    /// clock after opening it. No resolver, I/O or cache participates here.
    pub fn reauthorize(
        self,
        current: &TransactionCurrentCapabilityFacts,
        now: Timestamp,
    ) -> Result<AuthorizedReplicationAdministration, PolicyCode> {
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
        Ok(AuthorizedReplicationAdministration {
            preparation: self,
            timestamp: now,
        })
    }
    /// Exact request whose target and policy/generation were admitted.
    #[must_use]
    pub const fn request(&self) -> ReplicationAdministrationRequestV1 {
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
pub struct AuthorizedReplicationAdministration {
    preparation: AuthorizedReplicationAdministrationPreparation,
    timestamp: Timestamp,
}
impl AuthorizedReplicationAdministration {
    /// Lowers the unchanged request/principal and the one final audit timestamp.
    #[must_use]
    pub fn into_parts(self) -> (AuthorizedReplicationAdministrationPreparation, Timestamp) {
        (self.preparation, self.timestamp)
    }
}

impl fmt::Debug for AuthorizedReplicationAdministrationPreparation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AuthorizedReplicationAdministrationPreparation([REDACTED])")
    }
}

/// Closed preparation result; denial grants no hold mutation or release.
#[derive(Debug)]
pub enum ReplicationAdministrationDecision {
    /// Exact request passed fresh initial authorization.
    Allow(Box<AuthorizedReplicationAdministrationPreparation>),
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
    /// lifecycle request. Stream permission alone never grants fence release.
    pub fn authorize_replication_administration(
        &self,
        principal: &AuthenticatedPrincipal,
        request: ReplicationAdministrationRequestV1,
    ) -> Result<ReplicationAdministrationDecision, AuthorizationError> {
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
            Ok(()) => Ok(ReplicationAdministrationDecision::Allow(Box::new(
                AuthorizedReplicationAdministrationPreparation {
                    principal: principal.clone(),
                    request,
                    environment: self.expected_environment.clone(),
                    authorized_at: now,
                },
            ))),
            Err(code) => {
                self.telemetry
                    .record(AuthorizationTelemetryEvent::Denied(code.into()));
                Ok(ReplicationAdministrationDecision::Deny(code))
            }
        }
    }
}

pub(super) fn evaluate(
    principal: &PrincipalFacts,
    current: &CurrentFacts,
    database: DatabaseId,
    environment: &Environment,
    now: Timestamp,
    selected_database: DatabaseId,
) -> Result<(), PolicyCode> {
    validate_current(principal, current, database, environment, now)?;
    if selected_database != database {
        return Err(PolicyCode::MissingPermission);
    }
    let grant = &current.grant;
    if grant.tenant_scope() != &TenantScope::Global {
        return Err(PolicyCode::TenantScopeMismatch);
    }
    if grant.partition_scope() != &PartitionScopeV1::All {
        return Err(PolicyCode::PartitionScopeMismatch);
    }
    if grant.internal_row_policy().is_some()
        || grant
            .permissions()
            .as_slice()
            .iter()
            .any(|p| matches!(p, CapabilityPermissionV1::ApplicationRoleIdentity(_)))
    {
        return Err(PolicyCode::MissingPermission);
    }
    let permission = crate::operation::PermissionRequirement::Kind(
        CapabilityPermissionKindV1::AdministerCapabilities,
    );
    match check_permission(grant, &permission) {
        PermissionCheck::Allowed => Ok(()),
        PermissionCheck::Missing => Err(PolicyCode::MissingPermission),
        PermissionCheck::ApprovalRequired => Err(PolicyCode::ApprovalRequired),
    }
}
