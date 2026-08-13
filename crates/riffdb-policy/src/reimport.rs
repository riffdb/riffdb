//! Closed, current-capability authorization for application reimport campaigns.

use std::fmt;
use std::num::NonZeroU64;

use riffdb_types::{
    ActorId, ActorKind, ApplicationInstallationCampaignId, ApplicationPortabilityManifestHash,
    ApplicationReimportAuthorityV1, CapabilityApplicationReimportScopeV1, CapabilityId,
    ContractLineage, DatabaseId, Environment,
};

use crate::{AuthorizedRowPolicyAuthority, Obligations, PolicyCode};

/// Closed safe point checked independently during one reimport campaign.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplicationReimportPolicyOperationV1 {
    /// Validate exact source artifacts and create a durable checkpoint.
    Start,
    /// Apply one bounded, hash-bound page.
    Page,
    /// Observe protected progress or a terminal receipt.
    Status,
    /// Close one nonterminal campaign without publishing readiness.
    Cancel,
}

/// Exact campaign, lineage, manifest, scope, and action supplied to policy.
#[derive(Clone, Eq, PartialEq)]
pub struct ApplicationReimportAuthorizationRequestV1 {
    campaign_id: ApplicationInstallationCampaignId,
    lineage: ContractLineage,
    portability_manifest_hash: ApplicationPortabilityManifestHash,
    scope: CapabilityApplicationReimportScopeV1,
    operation: ApplicationReimportPolicyOperationV1,
}

impl ApplicationReimportAuthorizationRequestV1 {
    /// Constructs one complete safe-point request.
    #[must_use]
    pub const fn new(
        campaign_id: ApplicationInstallationCampaignId,
        lineage: ContractLineage,
        portability_manifest_hash: ApplicationPortabilityManifestHash,
        scope: CapabilityApplicationReimportScopeV1,
        operation: ApplicationReimportPolicyOperationV1,
    ) -> Self {
        Self {
            campaign_id,
            lineage,
            portability_manifest_hash,
            scope,
            operation,
        }
    }

    /// Exact installation/reimport campaign identity.
    #[must_use]
    pub const fn campaign_id(&self) -> ApplicationInstallationCampaignId {
        self.campaign_id
    }

    /// Exact application lineage.
    #[must_use]
    pub const fn lineage(&self) -> &ContractLineage {
        &self.lineage
    }

    /// Exact adapter-owned portability manifest identity.
    #[must_use]
    pub const fn portability_manifest_hash(&self) -> ApplicationPortabilityManifestHash {
        self.portability_manifest_hash
    }

    /// Current-policy or whole-application scope.
    #[must_use]
    pub const fn scope(&self) -> CapabilityApplicationReimportScopeV1 {
        self.scope
    }

    /// Current operation safe point.
    #[must_use]
    pub const fn operation(&self) -> ApplicationReimportPolicyOperationV1 {
        self.operation
    }
}

impl fmt::Debug for ApplicationReimportAuthorizationRequestV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ApplicationReimportAuthorizationRequestV1([REDACTED])")
    }
}

/// Move-only proof that one exact safe point used current Capability V7 authority.
#[derive(Eq, PartialEq)]
pub struct AuthorizedApplicationReimportV1 {
    database_id: DatabaseId,
    environment: Environment,
    request: ApplicationReimportAuthorizationRequestV1,
    obligations: Obligations,
    authority: ApplicationReimportAuthorityV1,
    principal_id: ActorId,
    actor_kind: ActorKind,
    row_policy_authority: AuthorizedRowPolicyAuthority,
}

impl AuthorizedApplicationReimportV1 {
    #[allow(clippy::too_many_arguments)]
    pub(crate) const fn new(
        database_id: DatabaseId,
        environment: Environment,
        request: ApplicationReimportAuthorizationRequestV1,
        obligations: Obligations,
        capability_id: CapabilityId,
        capability_revision: NonZeroU64,
        principal_id: ActorId,
        actor_kind: ActorKind,
        row_policy_authority: AuthorizedRowPolicyAuthority,
    ) -> Self {
        Self {
            database_id,
            environment,
            request,
            obligations,
            authority: ApplicationReimportAuthorityV1::new(capability_id, capability_revision),
            principal_id,
            actor_kind,
            row_policy_authority,
        }
    }

    /// Exact database boundary checked by current policy.
    #[must_use]
    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    /// Exact configured environment checked by current policy.
    #[must_use]
    pub const fn environment(&self) -> &Environment {
        &self.environment
    }

    /// Exact request proven at this safe point.
    #[must_use]
    pub const fn request(&self) -> &ApplicationReimportAuthorizationRequestV1 {
        &self.request
    }

    /// Administrative, row-policy-aware obligations.
    #[must_use]
    pub const fn obligations(&self) -> &Obligations {
        &self.obligations
    }

    /// Current V7 authority identity and revision.
    #[must_use]
    pub const fn authority(&self) -> ApplicationReimportAuthorityV1 {
        self.authority
    }

    /// Admitted principal identity.
    #[must_use]
    pub const fn principal_id(&self) -> &ActorId {
        &self.principal_id
    }

    /// Admitted actor classification.
    #[must_use]
    pub const fn actor_kind(&self) -> ActorKind {
        self.actor_kind
    }

    /// Exact compiler-owned row-policy role required even for whole-application reimport.
    #[doc(hidden)]
    #[must_use]
    pub const fn internal_row_policy_authority(&self) -> &AuthorizedRowPolicyAuthority {
        &self.row_policy_authority
    }
}

impl fmt::Debug for AuthorizedApplicationReimportV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AuthorizedApplicationReimportV1([REDACTED])")
    }
}

/// Deny-by-default result of one transaction-current reimport safe point.
#[derive(Eq, PartialEq)]
pub enum ApplicationReimportDecisionV1 {
    /// Current V7 authority allows the exact request.
    Allow(Box<AuthorizedApplicationReimportV1>),
    /// Current authority denies the request with one closed policy code.
    Deny(PolicyCode),
}

impl fmt::Debug for ApplicationReimportDecisionV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Allow(_) => {
                formatter.write_str("ApplicationReimportDecisionV1::Allow([REDACTED])")
            }
            Self::Deny(code) => formatter
                .debug_tuple("ApplicationReimportDecisionV1::Deny")
                .field(code)
                .finish(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_is_closed_exact_and_redacted() {
        let campaign_id =
            ApplicationInstallationCampaignId::from_unix_milliseconds_and_random(1, [4; 10])
                .expect("campaign");
        let request = ApplicationReimportAuthorizationRequestV1::new(
            campaign_id,
            ContractLineage::new("TicketDesk").expect("lineage"),
            ApplicationPortabilityManifestHash::from_bytes([5; 32]),
            CapabilityApplicationReimportScopeV1::WholeApplication,
            ApplicationReimportPolicyOperationV1::Page,
        );
        assert_eq!(request.campaign_id(), campaign_id);
        assert_eq!(
            request.operation(),
            ApplicationReimportPolicyOperationV1::Page
        );
        assert_eq!(
            format!("{request:?}"),
            "ApplicationReimportAuthorizationRequestV1([REDACTED])"
        );
    }
}
