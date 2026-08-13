//! Closed, current-capability authorization for application reimport campaigns.

use std::fmt;
use std::num::NonZeroU64;

use riffdb_types::{
    ActorId, ActorKind, ApplicationInstallationCampaignId, ApplicationPortabilityManifestHash,
    ApplicationReimportAuthorityV1, CapabilityApplicationReimportScopeV1, CapabilityId, CommandId,
    ContractLineage, ContractVersion, DatabaseId, Environment, PartitionKey,
};

use crate::{
    ApplicationQueryTarget, AuthorizedApplicationQuery, AuthorizedCommandExecution,
    AuthorizedRowPolicyAuthority, CommandAuthorizationBindingError, Obligations, PolicyCode,
};

/// Safe failure to consume current reimport authority as one reconciliation query proof.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReimportQueryAuthorizationBindingError {
    /// The proof was not issued for page application/reconciliation.
    OperationMismatch,
    /// The compiled target is outside the exact reimport authority.
    ObligationMismatch,
}

impl fmt::Display for ReimportQueryAuthorizationBindingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::OperationMismatch => "reimport authorization is not for reconciliation",
            Self::ObligationMismatch => "reimport query authorization is inconsistent",
        })
    }
}

impl std::error::Error for ReimportQueryAuthorizationBindingError {}

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

    /// Consumes one current V7 page safe point into exact compiler-owned command authority.
    ///
    /// This is not an application-command authorization path. It succeeds only
    /// for the `Page` operation, retains the current reimport row-policy role,
    /// and checks a principal-filtered partition before producing a move-only
    /// command proof.
    #[doc(hidden)]
    pub fn into_command_execution(
        self,
        version: ContractVersion,
        command_id: CommandId,
        partition: PartitionKey,
    ) -> Result<AuthorizedCommandExecution, CommandAuthorizationBindingError> {
        AuthorizedCommandExecution::bind_reimport(self, version, command_id, partition)
    }

    /// Consumes one current V7 page safe point into a bounded reconciliation-query proof.
    ///
    /// This is not ordinary application-query authority. The compiler-derived
    /// target must remain inside the reimport grant's exact lineage, partition,
    /// row limit, and current row-policy role.
    #[doc(hidden)]
    pub fn into_query_execution(
        self,
        target: ApplicationQueryTarget,
    ) -> Result<AuthorizedApplicationQuery, ReimportQueryAuthorizationBindingError> {
        AuthorizedApplicationQuery::bind_reimport(self, target)
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
    use std::num::NonZeroU16;

    use riffdb_auth::PrincipalFactBindingV1;
    use riffdb_types::{
        ApplicationRoleHash, Audience, CapabilityPrincipalFactsV1, CapabilityRowPolicyBindingV1,
        CapabilityRowPolicyGrantV1, CapabilityRowPolicyOperationV1, ContractBundleHash,
        EntityTypeId, PartitionKeyBuilder, PartitionScopeV1, QueryCostVectorV1, QueryPlanHash,
        RowPolicyName, ServiceIngressKindV1, TenantScope, Timestamp,
    };

    use super::*;

    fn database() -> DatabaseId {
        DatabaseId::from_unix_milliseconds_and_random(1, [1; 10]).expect("database")
    }

    fn environment() -> Environment {
        Environment::new("reimport-test").expect("environment")
    }

    fn lineage() -> ContractLineage {
        ContractLineage::new("TicketDesk").expect("lineage")
    }

    fn campaign_id() -> ApplicationInstallationCampaignId {
        ApplicationInstallationCampaignId::from_unix_milliseconds_and_random(1, [4; 10])
            .expect("campaign")
    }

    fn capability_id() -> CapabilityId {
        CapabilityId::from_unix_milliseconds_and_random(1, [5; 10]).expect("capability")
    }

    fn partition(value: u64) -> PartitionKey {
        let mut builder = PartitionKeyBuilder::new(riffdb_types::AggregateTypeId::first());
        builder.push_u64(value).expect("component");
        builder.finish().expect("partition")
    }

    fn row_policy_authority() -> AuthorizedRowPolicyAuthority {
        let principal = ActorId::new("reimport-operator").expect("principal");
        let facts = CapabilityPrincipalFactsV1::empty();
        let binding = PrincipalFactBindingV1::new(
            capability_id(),
            NonZeroU64::MIN,
            database(),
            environment(),
            principal,
            ActorKind::Human,
            vec![Audience::new("reimport-test").expect("audience")],
            TenantScope::Global,
            Timestamp::new(1, 0).expect("issued"),
            Timestamp::new(100, 0).expect("expires"),
            facts.clone(),
        )
        .expect("principal facts");
        let role = ApplicationRoleHash::from_bytes([7; 32]);
        let grant = CapabilityRowPolicyGrantV1::new(
            role,
            facts,
            vec![
                CapabilityRowPolicyBindingV1::new(
                    lineage(),
                    RowPolicyName::new("TicketReimport").expect("policy"),
                    EntityTypeId::first(),
                    vec![CapabilityRowPolicyOperationV1::Create],
                )
                .expect("binding"),
            ],
        )
        .expect("grant");
        AuthorizedRowPolicyAuthority::new(binding, grant)
    }

    fn authorization(
        operation: ApplicationReimportPolicyOperationV1,
        constraint: Option<crate::PartitionConstraint>,
    ) -> AuthorizedApplicationReimportV1 {
        AuthorizedApplicationReimportV1::new(
            database(),
            environment(),
            ApplicationReimportAuthorizationRequestV1::new(
                campaign_id(),
                lineage(),
                ApplicationPortabilityManifestHash::from_bytes([6; 32]),
                CapabilityApplicationReimportScopeV1::WholeApplication,
                operation,
            ),
            Obligations::new(
                TenantScope::Global,
                constraint,
                None,
                Some(NonZeroU16::MIN),
                None,
                None,
                crate::OutputClassification::PolicyFilteredApplicationData,
            ),
            capability_id(),
            NonZeroU64::MIN,
            ActorId::new("reimport-operator").expect("principal"),
            ActorKind::Human,
            row_policy_authority(),
        )
    }

    fn query_target(value: u64, maximum_rows: u16) -> ApplicationQueryTarget {
        ApplicationQueryTarget::new(
            lineage(),
            ContractVersion::new(1).expect("version"),
            ContractBundleHash::from_bytes([8; 32]),
            QueryPlanHash::from_bytes([9; 32]),
            ServiceIngressKindV1::Grpc,
            crate::OperationTenantScope::global_only(),
            partition(value),
            vec![
                crate::ApplicationQueryAccessRequirement::new(
                    EntityTypeId::first(),
                    None,
                    Vec::new(),
                    NonZeroU16::new(maximum_rows).expect("rows"),
                )
                .expect("access"),
            ],
            QueryCostVectorV1::new(1, u64::from(maximum_rows), 0, 0, 1, 1, 16).expect("cost"),
        )
        .expect("target")
    }

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

    #[test]
    fn only_page_authority_can_be_consumed_as_reimport_command_authority() {
        let command = authorization(ApplicationReimportPolicyOperationV1::Page, None)
            .into_command_execution(
                ContractVersion::new(1).expect("version"),
                CommandId::first(),
                partition(7),
            )
            .expect("page authority");
        assert!(command.internal_is_reimport());
        assert_eq!(command.database_id(), database());
        assert_eq!(command.partition().partition_key(), &partition(7));

        assert_eq!(
            authorization(ApplicationReimportPolicyOperationV1::Start, None)
                .into_command_execution(
                    ContractVersion::new(1).expect("version"),
                    CommandId::first(),
                    partition(7),
                ),
            Err(CommandAuthorizationBindingError::ObligationMismatch)
        );
    }

    #[test]
    fn principal_filtered_reimport_cannot_escape_its_exact_partition_set() {
        let allowed = riffdb_types::ScopedPartitionV1::new(lineage(), partition(7));
        let filter = PartitionScopeV1::explicit(vec![allowed]).expect("explicit scope");
        let command = authorization(
            ApplicationReimportPolicyOperationV1::Page,
            Some(crate::PartitionConstraint::Filter(filter.clone())),
        )
        .into_command_execution(
            ContractVersion::new(1).expect("version"),
            CommandId::first(),
            partition(7),
        )
        .expect("in-scope partition");
        assert!(command.internal_is_reimport());

        assert_eq!(
            authorization(
                ApplicationReimportPolicyOperationV1::Page,
                Some(crate::PartitionConstraint::Filter(filter)),
            )
            .into_command_execution(
                ContractVersion::new(1).expect("version"),
                CommandId::first(),
                partition(8),
            ),
            Err(CommandAuthorizationBindingError::ObligationMismatch)
        );
    }

    #[test]
    fn reconciliation_query_is_bounded_by_current_page_authority() {
        let allowed = riffdb_types::ScopedPartitionV1::new(lineage(), partition(7));
        let filter = PartitionScopeV1::explicit(vec![allowed]).expect("explicit scope");
        let query = authorization(
            ApplicationReimportPolicyOperationV1::Page,
            Some(crate::PartitionConstraint::Filter(filter.clone())),
        )
        .into_query_execution(query_target(7, 1))
        .expect("in-scope bounded query");
        assert_eq!(query.target().partition().partition_key(), &partition(7));
        assert!(query.internal_row_policy_authority().is_some());

        assert_eq!(
            authorization(
                ApplicationReimportPolicyOperationV1::Page,
                Some(crate::PartitionConstraint::Filter(filter)),
            )
            .into_query_execution(query_target(8, 1)),
            Err(ReimportQueryAuthorizationBindingError::ObligationMismatch)
        );
        assert_eq!(
            authorization(ApplicationReimportPolicyOperationV1::Start, None)
                .into_query_execution(query_target(7, 1)),
            Err(ReimportQueryAuthorizationBindingError::OperationMismatch)
        );
    }
}
