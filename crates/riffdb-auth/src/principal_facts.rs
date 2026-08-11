//! Capability-bound principal facts for compiler-owned row policies.

use std::{error::Error, fmt, num::NonZeroU64};

use riffdb_types::{
    ActorId, ActorKind, Audience, CapabilityId, CapabilityPrincipalFactsV1, DatabaseId,
    Environment, MAX_CAPABILITY_AUDIENCES, TenantScope, Timestamp,
};

/// Closed failure to construct or delegate a principal-fact binding.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PrincipalFactBindingError {
    /// The issue/expiry interval is empty or outside the parent interval.
    InvalidLifetime,
    /// The audience set is empty, duplicated, excessive, or wider than the parent.
    InvalidAudience,
    /// The tenant scope widens or changes a tenant-bound parent.
    InvalidTenantScope,
    /// The delegated fact set adds or widens authority.
    FactWidening,
}

impl fmt::Display for PrincipalFactBindingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidLifetime => "principal fact binding lifetime is invalid",
            Self::InvalidAudience => "principal fact binding audience is invalid",
            Self::InvalidTenantScope => "principal fact binding tenant scope is invalid",
            Self::FactWidening => "principal fact delegation would widen authority",
        })
    }
}

impl Error for PrincipalFactBindingError {}

/// One immutable principal/fact snapshot bound to an exact capability revision.
///
/// This is an authorization value, never an application request value. Its
/// constructor is intentionally independent of transport and storage so WP-572
/// can persist and reload the same proof at every authorization safe point.
#[derive(Clone, Eq, PartialEq)]
pub struct PrincipalFactBindingV1 {
    capability_id: CapabilityId,
    revision: NonZeroU64,
    database_id: DatabaseId,
    environment: Environment,
    principal_id: ActorId,
    actor_kind: ActorKind,
    audiences: Vec<Audience>,
    tenant_scope: TenantScope,
    issued_at: Timestamp,
    expires_at: Timestamp,
    facts: CapabilityPrincipalFactsV1,
}

impl PrincipalFactBindingV1 {
    /// Constructs one trusted, revisioned, expiring authorization binding.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        capability_id: CapabilityId,
        revision: NonZeroU64,
        database_id: DatabaseId,
        environment: Environment,
        principal_id: ActorId,
        actor_kind: ActorKind,
        mut audiences: Vec<Audience>,
        tenant_scope: TenantScope,
        issued_at: Timestamp,
        expires_at: Timestamp,
        facts: CapabilityPrincipalFactsV1,
    ) -> Result<Self, PrincipalFactBindingError> {
        if issued_at >= expires_at {
            return Err(PrincipalFactBindingError::InvalidLifetime);
        }
        canonicalize_audiences(&mut audiences)?;
        Ok(Self {
            capability_id,
            revision,
            database_id,
            environment,
            principal_id,
            actor_kind,
            audiences,
            tenant_scope,
            issued_at,
            expires_at,
            facts,
        })
    }

    /// Constructs a child binding only when every inherited dimension narrows.
    ///
    /// Database and environment are inherited rather than caller-selectable.
    /// The child may name a distinct principal, but its audiences, tenant
    /// scope, lifetime, and facts cannot exceed the parent capability.
    #[allow(clippy::too_many_arguments)]
    pub fn delegate_narrowed(
        parent: &Self,
        capability_id: CapabilityId,
        revision: NonZeroU64,
        principal_id: ActorId,
        actor_kind: ActorKind,
        audiences: Vec<Audience>,
        tenant_scope: TenantScope,
        issued_at: Timestamp,
        expires_at: Timestamp,
        facts: CapabilityPrincipalFactsV1,
    ) -> Result<Self, PrincipalFactBindingError> {
        let child = Self::new(
            capability_id,
            revision,
            parent.database_id,
            parent.environment.clone(),
            principal_id,
            actor_kind,
            audiences,
            tenant_scope,
            issued_at,
            expires_at,
            facts,
        )?;
        if child.issued_at < parent.issued_at || child.expires_at > parent.expires_at {
            return Err(PrincipalFactBindingError::InvalidLifetime);
        }
        if !child
            .audiences
            .iter()
            .all(|audience| parent.audiences.binary_search(audience).is_ok())
        {
            return Err(PrincipalFactBindingError::InvalidAudience);
        }
        if !tenant_scope_narrows(&child.tenant_scope, &parent.tenant_scope) {
            return Err(PrincipalFactBindingError::InvalidTenantScope);
        }
        if !child.facts.is_narrowing_of(&parent.facts) {
            return Err(PrincipalFactBindingError::FactWidening);
        }
        Ok(child)
    }

    /// Stable capability identity.
    #[must_use]
    pub const fn capability_id(&self) -> CapabilityId {
        self.capability_id
    }

    /// Transaction-current capability revision covering this fact set.
    #[must_use]
    pub const fn revision(&self) -> NonZeroU64 {
        self.revision
    }

    /// Permanent database binding.
    #[must_use]
    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    /// Exact environment binding.
    #[must_use]
    pub const fn environment(&self) -> &Environment {
        &self.environment
    }

    /// Stable principal identity available to compiled policy.
    #[must_use]
    pub const fn principal_id(&self) -> &ActorId {
        &self.principal_id
    }

    /// Trusted principal kind available to compiled policy.
    #[must_use]
    pub const fn actor_kind(&self) -> ActorKind {
        self.actor_kind
    }

    /// Canonical audience boundary. Values remain authorization-only.
    #[doc(hidden)]
    #[must_use]
    pub fn internal_audiences(&self) -> &[Audience] {
        &self.audiences
    }

    /// Authorization-resolved tenant boundary.
    #[must_use]
    pub const fn tenant_scope(&self) -> &TenantScope {
        &self.tenant_scope
    }

    /// Inclusive issue time.
    #[must_use]
    pub const fn issued_at(&self) -> Timestamp {
        self.issued_at
    }

    /// Exclusive expiry time.
    #[must_use]
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }

    /// Current facts for trusted policy evaluation only.
    #[doc(hidden)]
    #[must_use]
    pub const fn internal_facts(&self) -> &CapabilityPrincipalFactsV1 {
        &self.facts
    }
}

impl fmt::Debug for PrincipalFactBindingV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PrincipalFactBindingV1([REDACTED])")
    }
}

fn canonicalize_audiences(audiences: &mut [Audience]) -> Result<(), PrincipalFactBindingError> {
    if audiences.is_empty() || audiences.len() > MAX_CAPABILITY_AUDIENCES {
        return Err(PrincipalFactBindingError::InvalidAudience);
    }
    audiences.sort_unstable();
    if audiences.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(PrincipalFactBindingError::InvalidAudience);
    }
    Ok(())
}

fn tenant_scope_narrows(child: &TenantScope, parent: &TenantScope) -> bool {
    match (child, parent) {
        (_, TenantScope::Global) => true,
        (TenantScope::Tenant(child), TenantScope::Tenant(parent)) => child == parent,
        (TenantScope::Global, TenantScope::Tenant(_)) => false,
    }
}

#[cfg(test)]
mod tests {
    use riffdb_types::{CanonicalValue, CapabilityPrincipalFactV1, TenantId};

    use super::*;

    fn capability(seed: u8) -> CapabilityId {
        CapabilityId::from_unix_milliseconds_and_random(1, [seed; 10]).expect("capability")
    }

    fn database() -> DatabaseId {
        DatabaseId::from_unix_milliseconds_and_random(1, [7; 10]).expect("database")
    }

    fn time(seconds: i64) -> Timestamp {
        Timestamp::new(seconds, 0).expect("time")
    }

    fn facts(groups: &[&str]) -> CapabilityPrincipalFactsV1 {
        CapabilityPrincipalFactsV1::new(vec![
            CapabilityPrincipalFactV1::new(
                "groups",
                CanonicalValue::list(
                    groups
                        .iter()
                        .map(|group| CanonicalValue::string(*group).expect("group"))
                        .collect(),
                )
                .expect("groups"),
            )
            .expect("fact"),
        ])
        .expect("facts")
    }

    fn parent() -> PrincipalFactBindingV1 {
        PrincipalFactBindingV1::new(
            capability(1),
            NonZeroU64::MIN,
            database(),
            Environment::new("production").expect("environment"),
            ActorId::new("service:issuer").expect("actor"),
            ActorKind::Service,
            vec![
                Audience::new("web").expect("audience"),
                Audience::new("worker").expect("audience"),
            ],
            TenantScope::Global,
            time(10),
            time(100),
            facts(&["authors", "operators"]),
        )
        .expect("parent")
    }

    #[test]
    fn delegation_is_revisioned_bound_and_narrowing_only() {
        let parent = parent();
        let child = PrincipalFactBindingV1::delegate_narrowed(
            &parent,
            capability(2),
            NonZeroU64::new(3).expect("revision"),
            ActorId::new("agent:writer").expect("actor"),
            ActorKind::Agent,
            vec![Audience::new("worker").expect("audience")],
            TenantScope::Tenant(TenantId::new("tenant-a").expect("tenant")),
            time(20),
            time(80),
            facts(&["authors"]),
        )
        .expect("narrow child");

        assert_eq!(child.database_id(), parent.database_id());
        assert_eq!(child.environment(), parent.environment());
        assert_eq!(child.revision().get(), 3);
        assert_eq!(format!("{child:?}"), "PrincipalFactBindingV1([REDACTED])");
        assert!(!format!("{child:?}").contains("authors"));
    }

    #[test]
    fn delegation_rejects_each_widening_dimension() {
        let parent = parent();
        let attempt = |audiences: Vec<Audience>,
                       tenant_scope: TenantScope,
                       expires_at: Timestamp,
                       facts: CapabilityPrincipalFactsV1| {
            PrincipalFactBindingV1::delegate_narrowed(
                &parent,
                capability(3),
                NonZeroU64::MIN,
                ActorId::new("agent:writer").expect("actor"),
                ActorKind::Agent,
                audiences,
                tenant_scope,
                time(20),
                expires_at,
                facts,
            )
        };
        assert_eq!(
            attempt(
                vec![Audience::new("admin").expect("audience")],
                TenantScope::Global,
                time(80),
                facts(&["authors"]),
            )
            .expect_err("audience widening"),
            PrincipalFactBindingError::InvalidAudience
        );
        assert_eq!(
            attempt(
                vec![Audience::new("worker").expect("audience")],
                TenantScope::Global,
                time(101),
                facts(&["authors"]),
            )
            .expect_err("lifetime widening"),
            PrincipalFactBindingError::InvalidLifetime
        );
        assert_eq!(
            attempt(
                vec![Audience::new("worker").expect("audience")],
                TenantScope::Global,
                time(80),
                facts(&["authors", "owners"]),
            )
            .expect_err("fact widening"),
            PrincipalFactBindingError::FactWidening
        );

        let tenant_parent = PrincipalFactBindingV1::new(
            capability(4),
            NonZeroU64::MIN,
            database(),
            Environment::new("production").expect("environment"),
            ActorId::new("service:issuer").expect("actor"),
            ActorKind::Service,
            vec![Audience::new("worker").expect("audience")],
            TenantScope::Tenant(TenantId::new("tenant-a").expect("tenant")),
            time(10),
            time(100),
            facts(&["authors"]),
        )
        .expect("tenant parent");
        assert_eq!(
            PrincipalFactBindingV1::delegate_narrowed(
                &tenant_parent,
                capability(5),
                NonZeroU64::MIN,
                ActorId::new("agent:writer").expect("actor"),
                ActorKind::Agent,
                vec![Audience::new("worker").expect("audience")],
                TenantScope::Tenant(TenantId::new("tenant-b").expect("tenant")),
                time(20),
                time(80),
                facts(&["authors"]),
            )
            .expect_err("tenant substitution"),
            PrincipalFactBindingError::InvalidTenantScope
        );
    }
}
