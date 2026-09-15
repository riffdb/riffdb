//! Fresh administrative authorization for one unredacted replication release.

use super::*;

/// Move-only current-policy proof for one frame or bootstrap page release.
/// This is not continuing authority: each subsequent release requires a fresh
/// lookup, clock sample, and decision, including after any wait or backpressure.
pub struct AuthorizedReplicationRelease {
    database_id: DatabaseId,
}

impl AuthorizedReplicationRelease {
    /// Database to which this one release is restricted.
    #[must_use]
    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }
}

impl fmt::Debug for AuthorizedReplicationRelease {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AuthorizedReplicationRelease([redacted])")
    }
}

/// Closed decision for administrative replication, outside application roles.
#[derive(Debug)]
pub enum ReplicationDecision {
    /// One freshly checked release; transport confidentiality remains required.
    Allow(AuthorizedReplicationRelease),
    /// No source data may be released.
    Deny(PolicyCode),
}

impl<R, C, T> CurrentAuthorizer<'_, R, C, T>
where
    R: CurrentCapabilityResolver + ?Sized,
    C: AuthorizationClock + ?Sized,
    T: AuthorizationTelemetry + ?Sized,
{
    /// Rechecks capability identity, revision, activity, audience, validity,
    /// database and environment before authorizing a single unredacted release.
    pub fn authorize_replication(
        &self,
        principal: &AuthenticatedPrincipal,
    ) -> Result<ReplicationDecision, AuthorizationError> {
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
        match evaluate_replication(
            &PrincipalFacts::from(principal),
            &CurrentFacts::from(&current),
            self.expected_database_id,
            &self.expected_environment,
            now,
        ) {
            Ok(()) => Ok(ReplicationDecision::Allow(AuthorizedReplicationRelease {
                database_id: self.expected_database_id,
            })),
            Err(code) => {
                self.telemetry
                    .record(AuthorizationTelemetryEvent::Denied(code.into()));
                Ok(ReplicationDecision::Deny(code))
            }
        }
    }
}

pub(super) fn evaluate_replication(
    principal: &PrincipalFacts,
    current: &CurrentFacts,
    database: DatabaseId,
    environment: &Environment,
    now: Timestamp,
) -> Result<(), PolicyCode> {
    validate_current(principal, current, database, environment, now)?;
    let grant = &current.grant;
    if grant.tenant_scope() != &TenantScope::Global {
        return Err(PolicyCode::TenantScopeMismatch);
    }
    if grant.partition_scope() != &PartitionScopeV1::All {
        return Err(PolicyCode::PartitionScopeMismatch);
    }
    if grant.internal_row_policy().is_some()
        || grant.permissions().as_slice().iter().any(|permission| {
            matches!(
                permission,
                CapabilityPermissionV1::ApplicationRoleIdentity(_)
            )
        })
        || !grant
            .permissions()
            .as_slice()
            .iter()
            .any(|permission| permission.kind() == CapabilityPermissionKindV1::ReplicateChangelog)
    {
        return Err(PolicyCode::MissingPermission);
    }
    if grant
        .approval_required()
        .contains(&CapabilityPermissionKindV1::ReplicateChangelog)
    {
        return Err(PolicyCode::ApprovalRequired);
    }
    Ok(())
}
