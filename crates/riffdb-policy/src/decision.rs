//! Closed policy decisions and canonical authorization obligations.

use std::fmt;
use std::num::{NonZeroU16, NonZeroU64};

use riffdb_types::{
    ActorId, ActorKind, ApprovalId, CapabilityGrantV1, CapabilityId, CapabilityPermissionV1,
    ContractBundleHash, ContractLineage, ContractVersion, DatabaseId, EntityTypeId, Environment,
    FieldId, MAX_CAPABILITY_FIELD_VISIBILITY, PartitionScopeV1, ScopedPartitionV1,
    ServiceOperationV1, TenantScope, Timestamp,
};

use crate::operation::{
    ApplicationQueryTarget, FieldRequirement, PermissionRequirement, command_tool_permission,
    fixed_tool_permission_kind, named_query_tool_permission, resource_field_requirement,
    resource_permission,
};
use crate::{
    AgentSessionAdmissionPolicy, ApplicationCatalogQueryCandidate,
    ApplicationQueryAccessRequirement, AuthorizedCapabilityMutationPreparation,
    AuthorizedCommandExecution, CommandAuthorizationBindingError, CommandToolCandidate,
    DiscoveryResource, FixedToolCandidate, NamedQueryToolCandidate, OperationRequest,
    UntrustedInvocationClaims,
};

/// Maximum dynamic tool or resource candidates filtered by one safe-point proof.
pub const MAX_DISCOVERY_PAGE_ITEMS: usize = 500;

/// Stable deny-by-default policy reason retained only by trusted core code.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum PolicyCode {
    /// The current grant lacks the exact required permission atom.
    MissingPermission,
    /// The request is outside the capability's tenant authority.
    TenantScopeMismatch,
    /// The request is outside the capability's exact partition authority.
    PartitionScopeMismatch,
    /// At least one requested non-key field is not visible.
    FieldVisibilityDenied,
    /// The selected permission requires approval that has not been validated.
    ApprovalRequired,
    /// A capability mutation has not proven the required delegation relation.
    DelegationExceedsAuthority,
    /// Current capability state is inactive, expired, or stale.
    InactiveOrStaleCapability,
}

impl PolicyCode {
    /// Every accepted v1 policy code in stable tag order.
    pub const ALL: [Self; 7] = [
        Self::MissingPermission,
        Self::TenantScopeMismatch,
        Self::PartitionScopeMismatch,
        Self::FieldVisibilityDenied,
        Self::ApprovalRequired,
        Self::DelegationExceedsAuthority,
        Self::InactiveOrStaleCapability,
    ];

    /// Returns the stable v1 semantic tag.
    #[must_use]
    pub const fn tag(self) -> u8 {
        match self {
            Self::MissingPermission => 0x01,
            Self::TenantScopeMismatch => 0x02,
            Self::PartitionScopeMismatch => 0x03,
            Self::FieldVisibilityDenied => 0x04,
            Self::ApprovalRequired => 0x05,
            Self::DelegationExceedsAuthority => 0x06,
            Self::InactiveOrStaleCapability => 0x07,
        }
    }

    /// Decodes a stable tag and rejects zero or unknown values.
    #[must_use]
    pub const fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            0x01 => Some(Self::MissingPermission),
            0x02 => Some(Self::TenantScopeMismatch),
            0x03 => Some(Self::PartitionScopeMismatch),
            0x04 => Some(Self::FieldVisibilityDenied),
            0x05 => Some(Self::ApprovalRequired),
            0x06 => Some(Self::DelegationExceedsAuthority),
            0x07 => Some(Self::InactiveOrStaleCapability),
            _ => None,
        }
    }

    /// Returns static safe text for trusted diagnostics.
    #[must_use]
    pub const fn safe_text(self) -> &'static str {
        match self {
            Self::MissingPermission => "required permission is missing",
            Self::TenantScopeMismatch => "tenant scope does not authorize the request",
            Self::PartitionScopeMismatch => "partition scope does not authorize the request",
            Self::FieldVisibilityDenied => "requested fields are not visible",
            Self::ApprovalRequired => "validated approval is required",
            Self::DelegationExceedsAuthority => "delegation authority is insufficient",
            Self::InactiveOrStaleCapability => "capability state is inactive or stale",
        }
    }
}

/// Stable kind order for the closed obligation set.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ObligationKind {
    /// Effective authorization-resolved tenant scope.
    EffectiveTenantScope,
    /// Exact partition or bounded partition-filter constraint.
    PartitionConstraint,
    /// Exact non-key entity field mask.
    FieldMask,
    /// Maximum rows policy permits this request to return.
    RowLimit,
    /// Policy-validated approval identity.
    ValidatedApproval,
    /// Semantic audit classification.
    AuditClass,
    /// Required output handling classification.
    OutputClassification,
}

impl ObligationKind {
    /// Every obligation kind in required canonical order.
    pub const ALL: [Self; 7] = [
        Self::EffectiveTenantScope,
        Self::PartitionConstraint,
        Self::FieldMask,
        Self::RowLimit,
        Self::ValidatedApproval,
        Self::AuditClass,
        Self::OutputClassification,
    ];

    /// Returns the stable v1 semantic tag.
    #[must_use]
    pub const fn tag(self) -> u8 {
        match self {
            Self::EffectiveTenantScope => 0x01,
            Self::PartitionConstraint => 0x02,
            Self::FieldMask => 0x03,
            Self::RowLimit => 0x04,
            Self::ValidatedApproval => 0x05,
            Self::AuditClass => 0x06,
            Self::OutputClassification => 0x07,
        }
    }

    /// Decodes a stable tag and rejects zero or unknown values.
    #[must_use]
    pub const fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            0x01 => Some(Self::EffectiveTenantScope),
            0x02 => Some(Self::PartitionConstraint),
            0x03 => Some(Self::FieldMask),
            0x04 => Some(Self::RowLimit),
            0x05 => Some(Self::ValidatedApproval),
            0x06 => Some(Self::AuditClass),
            0x07 => Some(Self::OutputClassification),
            _ => None,
        }
    }
}

/// Exact partition handling the service must enforce before and after reads.
#[derive(Clone, Eq, PartialEq)]
pub enum PartitionConstraint {
    /// One exact lineage-scoped complete partition key.
    Exact(ScopedPartitionV1),
    /// The capability's complete all-or-explicit filter scope.
    Filter(PartitionScopeV1),
}

impl fmt::Debug for PartitionConstraint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PartitionConstraint([REDACTED])")
    }
}

/// Exact target-bound non-key fields that may remain visible.
///
/// An empty field list is meaningful: key identity may be returned, but every
/// non-key field must be removed.
#[derive(Clone, Eq, PartialEq)]
pub struct FieldMask {
    lineage: ContractLineage,
    entity_type_id: EntityTypeId,
    fields: Vec<FieldId>,
}

impl FieldMask {
    pub(crate) const fn new(
        lineage: ContractLineage,
        entity_type_id: EntityTypeId,
        fields: Vec<FieldId>,
    ) -> Self {
        Self {
            lineage,
            entity_type_id,
            fields,
        }
    }

    /// Returns the exact contract lineage owning the entity type.
    #[must_use]
    pub const fn lineage(&self) -> &ContractLineage {
        &self.lineage
    }

    /// Returns the exact stable entity type.
    #[must_use]
    pub const fn entity_type_id(&self) -> EntityTypeId {
        self.entity_type_id
    }

    /// Returns visible non-key fields in increasing stable-ID order.
    #[must_use]
    pub fn fields(&self) -> &[FieldId] {
        &self.fields
    }
}

impl fmt::Debug for FieldMask {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("FieldMask([REDACTED])")
    }
}

/// The semantic class used by service audit orchestration.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum AuditClass {
    /// A standard read or read-only computation.
    StandardRead,
    /// A command classified as mutating.
    CommandMutation,
    /// An administrative read or stream establishment.
    AdministrativeRead,
    /// A control-plane mutation.
    ControlPlaneMutation,
}

impl AuditClass {
    /// Every accepted audit class in stable tag order.
    pub const ALL: [Self; 4] = [
        Self::StandardRead,
        Self::CommandMutation,
        Self::AdministrativeRead,
        Self::ControlPlaneMutation,
    ];

    /// Returns the stable v1 semantic tag.
    #[must_use]
    pub const fn tag(self) -> u8 {
        match self {
            Self::StandardRead => 0x01,
            Self::CommandMutation => 0x02,
            Self::AdministrativeRead => 0x03,
            Self::ControlPlaneMutation => 0x04,
        }
    }

    /// Decodes a stable tag and rejects zero or unknown values.
    #[must_use]
    pub const fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            0x01 => Some(Self::StandardRead),
            0x02 => Some(Self::CommandMutation),
            0x03 => Some(Self::AdministrativeRead),
            0x04 => Some(Self::ControlPlaneMutation),
            _ => None,
        }
    }
}

/// Output handling required before any value reaches an adapter.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum OutputClassification {
    /// Bounded public metadata.
    PublicMetadata,
    /// Application data requiring complete policy filtering.
    PolicyFilteredApplicationData,
    /// Administrative data restricted to its public-redacted DTO.
    AdministrativeRedactedData,
}

impl OutputClassification {
    /// Every accepted output class in stable tag order.
    pub const ALL: [Self; 3] = [
        Self::PublicMetadata,
        Self::PolicyFilteredApplicationData,
        Self::AdministrativeRedactedData,
    ];

    /// Returns the stable v1 semantic tag.
    #[must_use]
    pub const fn tag(self) -> u8 {
        match self {
            Self::PublicMetadata => 0x01,
            Self::PolicyFilteredApplicationData => 0x02,
            Self::AdministrativeRedactedData => 0x03,
        }
    }

    /// Decodes a stable tag and rejects zero or unknown values.
    #[must_use]
    pub const fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            0x01 => Some(Self::PublicMetadata),
            0x02 => Some(Self::PolicyFilteredApplicationData),
            0x03 => Some(Self::AdministrativeRedactedData),
            _ => None,
        }
    }
}

/// Canonical, at-most-one-of-each obligation set produced only by policy.
#[derive(Clone, Eq, PartialEq)]
pub struct Obligations {
    effective_tenant_scope: TenantScope,
    partition_constraint: Option<PartitionConstraint>,
    field_mask: Option<FieldMask>,
    row_limit: Option<NonZeroU16>,
    validated_approval: Option<ApprovalId>,
    audit_class: Option<AuditClass>,
    output_classification: OutputClassification,
}

impl Obligations {
    #[allow(clippy::too_many_arguments)]
    pub(crate) const fn new(
        effective_tenant_scope: TenantScope,
        partition_constraint: Option<PartitionConstraint>,
        field_mask: Option<FieldMask>,
        row_limit: Option<NonZeroU16>,
        validated_approval: Option<ApprovalId>,
        audit_class: Option<AuditClass>,
        output_classification: OutputClassification,
    ) -> Self {
        Self {
            effective_tenant_scope,
            partition_constraint,
            field_mask,
            row_limit,
            validated_approval,
            audit_class,
            output_classification,
        }
    }

    /// Returns the authorization-resolved tenant scope.
    #[must_use]
    pub const fn effective_tenant_scope(&self) -> &TenantScope {
        &self.effective_tenant_scope
    }

    /// Returns the exact partition enforcement obligation, when applicable.
    #[must_use]
    pub const fn partition_constraint(&self) -> Option<&PartitionConstraint> {
        self.partition_constraint.as_ref()
    }

    /// Returns the exact visible non-key entity fields, when applicable.
    #[must_use]
    pub const fn field_mask(&self) -> Option<&FieldMask> {
        self.field_mask.as_ref()
    }

    /// Returns the policy-lowered row ceiling, when applicable.
    #[must_use]
    pub const fn row_limit(&self) -> Option<NonZeroU16> {
        self.row_limit
    }

    /// Returns the validated approval identity, when one was used.
    #[must_use]
    pub const fn validated_approval(&self) -> Option<&ApprovalId> {
        self.validated_approval.as_ref()
    }

    /// Returns the operation's semantic audit class.
    #[must_use]
    pub const fn audit_class(&self) -> Option<AuditClass> {
        self.audit_class
    }

    /// Returns the required output handling class.
    #[must_use]
    pub const fn output_classification(&self) -> OutputClassification {
        self.output_classification
    }

    /// Returns present obligation kinds in canonical order.
    pub fn kinds(&self) -> impl Iterator<Item = ObligationKind> + '_ {
        [
            Some(ObligationKind::EffectiveTenantScope),
            self.partition_constraint
                .as_ref()
                .map(|_| ObligationKind::PartitionConstraint),
            self.field_mask.as_ref().map(|_| ObligationKind::FieldMask),
            self.row_limit.map(|_| ObligationKind::RowLimit),
            self.validated_approval
                .as_ref()
                .map(|_| ObligationKind::ValidatedApproval),
            self.audit_class.map(|_| ObligationKind::AuditClass),
            Some(ObligationKind::OutputClassification),
        ]
        .into_iter()
        .flatten()
    }
}

impl fmt::Debug for Obligations {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Obligations([REDACTED])")
    }
}

/// Privately constructed proof that current policy allowed one exact boundary and request.
#[derive(Eq, PartialEq)]
pub struct AuthorizedOperation {
    database_id: DatabaseId,
    environment: Environment,
    request: OperationRequest,
    obligations: Obligations,
    identity: CurrentAuthorizationIdentity,
    discovery_authority: Option<CapabilityGrantV1>,
}

/// Move-only proof that current policy allowed one exact compiler-derived application plan.
///
/// This type deliberately implements neither `Clone` nor serialization. The
/// application service consumes it at the execution boundary, so raw kernel
/// read permissions cannot be substituted for application-query authority.
///
/// ```compile_fail
/// # use riffdb_policy::AuthorizedApplicationQuery;
/// fn duplicate(proof: &AuthorizedApplicationQuery) {
///     let _second: AuthorizedApplicationQuery = proof.clone();
/// }
/// ```
#[derive(Eq, PartialEq)]
pub struct AuthorizedApplicationQuery {
    database_id: DatabaseId,
    environment: Environment,
    target: ApplicationQueryTarget,
    identity: CurrentAuthorizationIdentity,
    obligations: Obligations,
}

impl AuthorizedApplicationQuery {
    /// Returns the exact database boundary checked by policy.
    #[must_use]
    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    /// Returns the exact environment checked by policy.
    #[must_use]
    pub const fn environment(&self) -> &Environment {
        &self.environment
    }

    /// Returns the exact compiler-derived target bound into this proof.
    #[must_use]
    pub const fn target(&self) -> &ApplicationQueryTarget {
        &self.target
    }

    /// Returns the capability identity revision checked at the safe point.
    #[must_use]
    pub const fn capability_revision(&self) -> NonZeroU64 {
        self.identity.capability_revision
    }

    /// Returns the exact policy obligations attached to execution and output.
    #[must_use]
    pub const fn obligations(&self) -> &Obligations {
        &self.obligations
    }
}

impl fmt::Debug for AuthorizedApplicationQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AuthorizedApplicationQuery([REDACTED])")
    }
}

/// The exact capability validity window compared by a current-policy safe point.
///
/// This is the sole definition of the time clause enforced by current
/// authorization. Full evaluation and revision-checked reauthorization both
/// call [`CheckedCapabilityValidity::admits`], so the two can never drift.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CheckedCapabilityValidity {
    issued_at: Timestamp,
    expires_at: Timestamp,
}

impl CheckedCapabilityValidity {
    /// Binds the exact issue and expiry instants recorded on a capability.
    #[must_use]
    pub const fn new(issued_at: Timestamp, expires_at: Timestamp) -> Self {
        Self {
            issued_at,
            expires_at,
        }
    }

    /// Returns whether `now` is inside the half-open validity window.
    #[must_use]
    pub fn admits(&self, now: Timestamp) -> bool {
        self.issued_at <= now && now < self.expires_at
    }

    /// Returns the exact instant the capability became valid.
    #[must_use]
    pub const fn issued_at(&self) -> Timestamp {
        self.issued_at
    }

    /// Returns the exact instant the capability stops being valid.
    #[must_use]
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }
}

/// One observation of live current-capability state taken at a safe point.
///
/// A checkpoint pairs the monotonic capability-view generation with the fresh
/// authorization time sampled from the same clock full evaluation uses. It is
/// the only input that permits [`AuthorizedOperation::reissue_for_unchanged_view`],
/// so a caller cannot reissue a proof without consulting live state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CapabilityViewCheckpoint {
    generation: u64,
    now: Timestamp,
}

impl CapabilityViewCheckpoint {
    /// Pairs a live capability-view generation with a fresh authorization time.
    #[must_use]
    pub const fn new(generation: u64, now: Timestamp) -> Self {
        Self { generation, now }
    }

    /// Returns the observed capability-view generation.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Returns the fresh authorization time observed with the generation.
    #[must_use]
    pub const fn now(&self) -> Timestamp {
        self.now
    }
}

#[derive(Clone, Eq, PartialEq)]
pub(crate) struct CurrentAuthorizationIdentity {
    capability_id: CapabilityId,
    capability_revision: NonZeroU64,
    principal_id: ActorId,
    actor_kind: ActorKind,
    validity: CheckedCapabilityValidity,
}

impl CurrentAuthorizationIdentity {
    pub(crate) const fn new(
        capability_id: CapabilityId,
        capability_revision: NonZeroU64,
        principal_id: ActorId,
        actor_kind: ActorKind,
        validity: CheckedCapabilityValidity,
    ) -> Self {
        Self {
            capability_id,
            capability_revision,
            principal_id,
            actor_kind,
            validity,
        }
    }
}

impl AuthorizedOperation {
    pub(crate) const fn new(
        database_id: DatabaseId,
        environment: Environment,
        request: OperationRequest,
        obligations: Obligations,
        identity: CurrentAuthorizationIdentity,
    ) -> Self {
        Self {
            database_id,
            environment,
            request,
            obligations,
            identity,
            discovery_authority: None,
        }
    }

    pub(crate) const fn new_discovery(
        database_id: DatabaseId,
        environment: Environment,
        request: OperationRequest,
        obligations: Obligations,
        grant: CapabilityGrantV1,
        identity: CurrentAuthorizationIdentity,
    ) -> Self {
        Self {
            database_id,
            environment,
            request,
            obligations,
            discovery_authority: Some(grant),
            identity,
        }
    }

    /// Returns the exact database boundary checked by the current authorizer.
    #[must_use]
    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    /// Borrows the exact environment checked by the current authorizer.
    #[must_use]
    pub const fn environment(&self) -> &Environment {
        &self.environment
    }

    /// Returns the exact request authorized by this proof.
    #[must_use]
    pub const fn request(&self) -> &OperationRequest {
        &self.request
    }

    /// Returns the exact canonical obligations paired with the proof.
    #[must_use]
    pub const fn obligations(&self) -> &Obligations {
        &self.obligations
    }

    /// Returns the closed service operation authorized by this proof.
    #[must_use]
    pub const fn operation(&self) -> ServiceOperationV1 {
        self.request.operation()
    }

    /// Re-issues this allow proof only when live state proves it still holds.
    ///
    /// `baseline_generation` is the capability-view generation observed
    /// immediately *before* the full evaluation that produced this proof.
    /// `observed` is a fresh checkpoint taken at the reauthorization safe
    /// point. `request` is the exact operation being reauthorized.
    ///
    /// Returns `None` — meaning the caller must perform a full evaluation —
    /// unless all three hold:
    ///
    /// 1. the generation is unchanged,
    /// 2. the checkpoint time is inside the capability validity window that
    ///    was checked when this proof was issued, and
    /// 3. the request is byte-identical to the authorized request.
    ///
    /// # Soundness
    ///
    /// Full evaluation is `validate_current` (identity, activity, revision,
    /// database, environment, principal, actor kind, audience, tenant scope,
    /// and `issued_at <= now < expires_at`) followed by the request-shaped
    /// permission, budget, tenant, partition, field, and row checks.
    ///
    /// * The capability-view generation is bumped on every publication that can
    ///   change what current-capability resolution returns. Generation unchanged
    ///   therefore implies the capability record is unchanged, so every fact
    ///   this proof was evaluated against still holds *except* facts that depend
    ///   on the passage of time.
    /// * The only time-dependent clause is the validity window. Because the
    ///   record is unchanged, the captured window **is** the current window, so
    ///   `self.identity.validity.admits(observed.now())` is exactly the clause a
    ///   full evaluation would apply — it calls the same
    ///   [`CheckedCapabilityValidity::admits`] predicate on the same values.
    /// * Every remaining clause is a pure function of (unchanged record,
    ///   unchanged static configuration, request). Requiring request equality
    ///   closes the last input.
    ///
    /// Hence the reissued proof is bit-identical to the proof a full evaluation
    /// would produce. The race window is no wider than a full evaluation's: a
    /// concurrent publication either has not been published (this checkpoint
    /// sees the old generation, exactly as a full evaluation reading the view
    /// before publication would) or has been published (generation moved, so
    /// this returns `None` and the caller fully re-evaluates and fails closed).
    #[must_use]
    pub fn reissue_for_unchanged_view(
        &self,
        baseline_generation: u64,
        observed: CapabilityViewCheckpoint,
        request: &OperationRequest,
    ) -> Option<Self> {
        if observed.generation() != baseline_generation
            || !self.identity.validity.admits(observed.now())
            || request != &self.request
        {
            return None;
        }
        Some(Self {
            database_id: self.database_id,
            environment: self.environment.clone(),
            request: self.request.clone(),
            obligations: self.obligations.clone(),
            identity: self.identity.clone(),
            discovery_authority: self.discovery_authority.clone(),
        })
    }

    /// Consumes this fresh allow proof into one exact application-query proof.
    pub fn into_application_query(self) -> Option<AuthorizedApplicationQuery> {
        let Self {
            database_id,
            environment,
            request,
            obligations,
            identity,
            discovery_authority,
        } = self;
        if discovery_authority.is_some() {
            return None;
        }
        let target = request.application_query_target()?.clone();
        Some(AuthorizedApplicationQuery {
            database_id,
            environment,
            target,
            identity,
            obligations,
        })
    }

    /// Consumes this fresh allow proof into an exact command-execution binding.
    ///
    /// This does not perform or replace a current-policy safe point. The proof
    /// must already have been returned by [`crate::CurrentAuthorizer`] for the
    /// exact command facts. Claim admission only binds bounded caller claims to
    /// the actor retained by that same authorization decision.
    pub fn into_command_execution(
        self,
        claims: UntrustedInvocationClaims,
        agent_session_policy: AgentSessionAdmissionPolicy,
    ) -> Result<AuthorizedCommandExecution, CommandAuthorizationBindingError> {
        let Self {
            database_id,
            environment,
            request,
            obligations,
            identity,
            discovery_authority,
        } = self;
        if discovery_authority.is_some() {
            return Err(CommandAuthorizationBindingError::OperationMismatch);
        }
        AuthorizedCommandExecution::bind(
            database_id,
            environment,
            request,
            obligations,
            identity.principal_id,
            identity.actor_kind,
            claims,
            agent_session_policy,
        )
    }

    /// Consumes this proof into the exact catalog-deployment authority it checked.
    ///
    /// The caller-supplied identity must be the same checked candidate that will
    /// be submitted downstream. A proof for another operation or candidate is
    /// consumed and cannot be reused.
    pub fn into_catalog_deployment(
        self,
        lineage: &ContractLineage,
        version: ContractVersion,
        bundle_hash: ContractBundleHash,
        expected_active_version: Option<ContractVersion>,
    ) -> Result<AuthorizedCatalogDeployment, CatalogDeploymentAuthorizationBindingError> {
        let Self {
            database_id,
            environment,
            request,
            obligations,
            identity,
            discovery_authority,
        } = self;
        if discovery_authority.is_some() {
            return Err(CatalogDeploymentAuthorizationBindingError::OperationMismatch);
        }
        let Some((
            authorized_lineage,
            authorized_version,
            authorized_bundle_hash,
            authorized_expected_active_version,
        )) = request.into_catalog_deployment_parts()
        else {
            return Err(CatalogDeploymentAuthorizationBindingError::OperationMismatch);
        };
        if &authorized_lineage != lineage
            || authorized_version != version
            || authorized_bundle_hash != bundle_hash
            || authorized_expected_active_version != expected_active_version
        {
            return Err(CatalogDeploymentAuthorizationBindingError::IdentityMismatch);
        }
        Ok(AuthorizedCatalogDeployment {
            database_id,
            environment,
            lineage: authorized_lineage,
            version: authorized_version,
            bundle_hash: authorized_bundle_hash,
            expected_active_version: authorized_expected_active_version,
            obligations,
            identity,
        })
    }

    /// Consumes a discovery allow proof into its candidate-filtering authority.
    ///
    /// A non-discovery proof is consumed and rejected so it cannot be reused as
    /// discovery authority.
    pub fn into_discovery(self) -> Result<AuthorizedDiscovery, DiscoveryFilterError> {
        if !matches!(
            self.request.operation(),
            ServiceOperationV1::DiscoverCommandTools
                | ServiceOperationV1::DiscoverResources
                | ServiceOperationV1::DescribeContract
        ) {
            return Err(DiscoveryFilterError::CatalogMismatch);
        }
        let Self {
            database_id: _,
            environment: _,
            request,
            obligations,
            identity: _,
            discovery_authority,
        } = self;
        match discovery_authority {
            Some(grant) => Ok(AuthorizedDiscovery {
                request,
                obligations,
                grant,
            }),
            None => {
                let _ = (request, obligations);
                Err(DiscoveryFilterError::CatalogMismatch)
            }
        }
    }
}

impl fmt::Debug for AuthorizedOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AuthorizedOperation([REDACTED])")
    }
}

/// Operation-specific proof for one exact authorized catalog deployment.
#[derive(Eq, PartialEq)]
pub struct AuthorizedCatalogDeployment {
    database_id: DatabaseId,
    environment: Environment,
    lineage: ContractLineage,
    version: ContractVersion,
    bundle_hash: ContractBundleHash,
    expected_active_version: Option<ContractVersion>,
    obligations: Obligations,
    identity: CurrentAuthorizationIdentity,
}

impl AuthorizedCatalogDeployment {
    /// Returns the exact database boundary checked by policy.
    #[must_use]
    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    /// Borrows the exact environment checked by policy.
    #[must_use]
    pub const fn environment(&self) -> &Environment {
        &self.environment
    }

    /// Borrows the exact contract lineage checked by policy.
    #[must_use]
    pub const fn lineage(&self) -> &ContractLineage {
        &self.lineage
    }

    /// Returns the exact contract version checked by policy.
    #[must_use]
    pub const fn version(&self) -> ContractVersion {
        self.version
    }

    /// Returns the exact immutable bundle hash checked by policy.
    #[must_use]
    pub const fn bundle_hash(&self) -> ContractBundleHash {
        self.bundle_hash
    }

    /// Returns the exact expected active version checked by policy.
    #[must_use]
    pub const fn expected_active_version(&self) -> Option<ContractVersion> {
        self.expected_active_version
    }

    /// Borrows the exact obligations paired with this authorization.
    #[must_use]
    pub const fn obligations(&self) -> &Obligations {
        &self.obligations
    }

    /// Returns the exact current capability identity that authorized deployment.
    #[must_use]
    pub const fn authorizing_capability_id(&self) -> CapabilityId {
        self.identity.capability_id
    }

    /// Returns the exact current capability revision that authorized deployment.
    #[must_use]
    pub const fn authorizing_revision(&self) -> NonZeroU64 {
        self.identity.capability_revision
    }

    /// Borrows the authenticated principal that authorized deployment.
    #[must_use]
    pub const fn principal_id(&self) -> &ActorId {
        &self.identity.principal_id
    }

    /// Returns the authenticated actor kind that authorized deployment.
    #[must_use]
    pub const fn actor_kind(&self) -> ActorKind {
        self.identity.actor_kind
    }
}

impl fmt::Debug for AuthorizedCatalogDeployment {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AuthorizedCatalogDeployment([REDACTED])")
    }
}

/// Safe failure to bind a generic allow proof to one catalog deployment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CatalogDeploymentAuthorizationBindingError {
    /// The proof authorized another closed service operation.
    OperationMismatch,
    /// The candidate identity differs from the deployment authorized by policy.
    IdentityMismatch,
}

impl fmt::Display for CatalogDeploymentAuthorizationBindingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::OperationMismatch => "authorization does not permit catalog deployment",
            Self::IdentityMismatch => "catalog deployment identity does not match authorization",
        })
    }
}

impl std::error::Error for CatalogDeploymentAuthorizationBindingError {}

/// Visibility of one candidate in a current-policy discovery snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiscoveryVisibility {
    /// The candidate may be presented, but invocation still reauthorizes.
    Visible,
    /// The candidate must be omitted without emitting a policy denial.
    Hidden,
}

/// One resource candidate's visibility and target-bound schema field mask.
#[derive(Eq, PartialEq)]
pub enum ResourceDiscoveryVisibility {
    /// The resource must be omitted without emitting a policy denial.
    Hidden,
    /// The resource may be presented with this optional entity-schema mask.
    Visible {
        /// Present only for an entity-schema resource, and valid even when empty.
        field_mask: Option<FieldMask>,
    },
}

impl fmt::Debug for ResourceDiscoveryVisibility {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Hidden => formatter.write_str("ResourceDiscoveryVisibility::Hidden"),
            Self::Visible { .. } => {
                formatter.write_str("ResourceDiscoveryVisibility::Visible([REDACTED])")
            }
        }
    }
}

/// Safe failure to consume one discovery proof for a bounded catalog.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiscoveryFilterError {
    /// The proof does not match the supplied candidate catalog class.
    CatalogMismatch,
    /// A dynamic candidate page exceeds the shared capability hard bound.
    TooManyCandidates,
    /// Aggregate entity-schema candidate fields exceed the capability field bound.
    CandidateFieldsLimitExceeded,
    /// The fixed-tool input is not the exact canonical SPEC POC inventory.
    FixedToolInventoryMismatch,
}

impl fmt::Display for DiscoveryFilterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::CatalogMismatch => "discovery proof does not match the candidate catalog",
            Self::TooManyCandidates => "discovery candidate page exceeds the hard limit",
            Self::CandidateFieldsLimitExceeded => {
                "discovery entity fields exceed the aggregate hard limit"
            }
            Self::FixedToolInventoryMismatch => {
                "fixed-tool candidates do not match the canonical inventory"
            }
        })
    }
}

impl std::error::Error for DiscoveryFilterError {}

/// Visibility masks for one consumed command-tool discovery snapshot.
#[derive(Eq, PartialEq)]
pub struct ToolCatalogVisibility {
    fixed_tools: Vec<DiscoveryVisibility>,
    command_tools: Vec<DiscoveryVisibility>,
    named_query_tools: Vec<DiscoveryVisibility>,
}

/// Visibility masks for one symbolic application-catalog authorization snapshot.
#[derive(Eq, PartialEq)]
pub struct ApplicationCatalogVisibility {
    command_operations: Vec<DiscoveryVisibility>,
    named_query_operations: Vec<DiscoveryVisibility>,
}

impl ApplicationCatalogVisibility {
    /// Positional command-operation visibility.
    #[must_use]
    pub fn command_operations(&self) -> &[DiscoveryVisibility] {
        &self.command_operations
    }

    /// Positional named-query-operation visibility.
    #[must_use]
    pub fn named_query_operations(&self) -> &[DiscoveryVisibility] {
        &self.named_query_operations
    }
}

impl fmt::Debug for ApplicationCatalogVisibility {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ApplicationCatalogVisibility([REDACTED])")
    }
}

impl ToolCatalogVisibility {
    /// Returns masks corresponding exactly to [`FixedToolCandidate::ALL`].
    #[must_use]
    pub fn fixed_tools(&self) -> &[DiscoveryVisibility] {
        &self.fixed_tools
    }

    /// Returns masks corresponding positionally to the supplied command candidates.
    #[must_use]
    pub fn command_tools(&self) -> &[DiscoveryVisibility] {
        &self.command_tools
    }

    /// Returns masks corresponding positionally to the supplied named-query candidates.
    #[must_use]
    pub fn named_query_tools(&self) -> &[DiscoveryVisibility] {
        &self.named_query_tools
    }
}

impl fmt::Debug for ToolCatalogVisibility {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ToolCatalogVisibility([REDACTED])")
    }
}

/// Move-only current-state proof used only to filter one discovery snapshot.
#[derive(Eq, PartialEq)]
pub struct AuthorizedDiscovery {
    request: OperationRequest,
    obligations: Obligations,
    grant: CapabilityGrantV1,
}

impl AuthorizedDiscovery {
    /// Returns the exact discovery request authorized at this safe point.
    #[must_use]
    pub const fn request(&self) -> &OperationRequest {
        &self.request
    }

    /// Returns the discovery-level obligations from this same safe point.
    #[must_use]
    pub const fn obligations(&self) -> &Obligations {
        &self.obligations
    }

    /// Consumes this safe-point proof to filter one complete tool catalog.
    pub fn tool_catalog(
        self,
        fixed_candidates: &[FixedToolCandidate],
        command_candidates: &[CommandToolCandidate],
        named_query_candidates: &[NamedQueryToolCandidate],
    ) -> Result<ToolCatalogVisibility, DiscoveryFilterError> {
        if self.request.operation() != ServiceOperationV1::DiscoverCommandTools {
            return Err(DiscoveryFilterError::CatalogMismatch);
        }
        if fixed_candidates != FixedToolCandidate::ALL.as_slice() {
            return Err(DiscoveryFilterError::FixedToolInventoryMismatch);
        }
        if command_candidates
            .len()
            .checked_add(named_query_candidates.len())
            .is_none_or(|count| count > MAX_DISCOVERY_PAGE_ITEMS)
        {
            return Err(DiscoveryFilterError::TooManyCandidates);
        }
        let fixed_tools = fixed_candidates
            .iter()
            .map(|candidate| self.fixed_tool_visibility(*candidate))
            .collect();
        let command_tools = command_candidates
            .iter()
            .map(|candidate| {
                if check_permission(&self.grant, &command_tool_permission(candidate))
                    == PermissionCheck::Allowed
                {
                    DiscoveryVisibility::Visible
                } else {
                    DiscoveryVisibility::Hidden
                }
            })
            .collect();
        let named_query_tools = named_query_candidates
            .iter()
            .map(|candidate| {
                if check_permission(&self.grant, &named_query_tool_permission(candidate))
                    == PermissionCheck::Allowed
                {
                    DiscoveryVisibility::Visible
                } else {
                    DiscoveryVisibility::Hidden
                }
            })
            .collect();
        Ok(ToolCatalogVisibility {
            fixed_tools,
            command_tools,
            named_query_tools,
        })
    }

    /// Consumes one `DescribeContract` safe-point proof to filter exact
    /// application operations before symbolic catalog assembly.
    pub fn application_catalog(
        self,
        command_candidates: &[CommandToolCandidate],
        named_query_candidates: &[ApplicationCatalogQueryCandidate],
    ) -> Result<ApplicationCatalogVisibility, DiscoveryFilterError> {
        if self.request.operation() != ServiceOperationV1::DescribeContract {
            return Err(DiscoveryFilterError::CatalogMismatch);
        }
        if command_candidates
            .len()
            .checked_add(named_query_candidates.len())
            .is_none_or(|count| count > MAX_DISCOVERY_PAGE_ITEMS)
        {
            return Err(DiscoveryFilterError::TooManyCandidates);
        }
        let command_operations = command_candidates
            .iter()
            .map(|candidate| {
                if check_permission(&self.grant, &command_tool_permission(candidate))
                    == PermissionCheck::Allowed
                {
                    DiscoveryVisibility::Visible
                } else {
                    DiscoveryVisibility::Hidden
                }
            })
            .collect();
        let named_query_operations = named_query_candidates
            .iter()
            .map(|candidate| {
                if check_permission(
                    &self.grant,
                    &named_query_tool_permission(candidate.operation()),
                ) == PermissionCheck::Allowed
                    && application_query_accesses_visible(
                        &self.grant,
                        candidate.operation().lineage(),
                        candidate.accesses(),
                    )
                {
                    DiscoveryVisibility::Visible
                } else {
                    DiscoveryVisibility::Hidden
                }
            })
            .collect();
        Ok(ApplicationCatalogVisibility {
            command_operations,
            named_query_operations,
        })
    }

    /// Consumes this safe-point proof to filter one bounded resource catalog.
    pub fn resource_catalog(
        self,
        candidates: &[DiscoveryResource],
    ) -> Result<Vec<ResourceDiscoveryVisibility>, DiscoveryFilterError> {
        if self.request.operation() != ServiceOperationV1::DiscoverResources {
            return Err(DiscoveryFilterError::CatalogMismatch);
        }
        if candidates.len() > MAX_DISCOVERY_PAGE_ITEMS {
            return Err(DiscoveryFilterError::TooManyCandidates);
        }
        let candidate_fields = candidates.iter().try_fold(0usize, |total, candidate| {
            let fields = resource_field_requirement(candidate)
                .map_or(0, |requirement| requirement.non_key_fields.len());
            total.checked_add(fields)
        });
        if candidate_fields.is_none_or(|total| total > MAX_CAPABILITY_FIELD_VISIBILITY) {
            return Err(DiscoveryFilterError::CandidateFieldsLimitExceeded);
        }
        Ok(candidates
            .iter()
            .map(|candidate| {
                let permission = resource_permission(candidate);
                if check_permission(&self.grant, &permission) != PermissionCheck::Allowed
                    || !resource_scope_is_discoverable(&self.grant, candidate)
                {
                    return ResourceDiscoveryVisibility::Hidden;
                }
                let field_mask = resource_field_requirement(candidate)
                    .map(|requirement| derive_field_mask(&self.grant, requirement));
                ResourceDiscoveryVisibility::Visible { field_mask }
            })
            .collect())
    }

    fn fixed_tool_visibility(&self, candidate: FixedToolCandidate) -> DiscoveryVisibility {
        let kind = fixed_tool_permission_kind(candidate);
        let has_permission = self.grant.permissions().contains_kind(kind);
        let needs_approval = self.grant.approval_required().binary_search(&kind).is_ok();
        let global_required = matches!(
            candidate,
            FixedToolCandidate::ResolveCommandOutcome
                | FixedToolCandidate::GetEntity
                | FixedToolCandidate::ScanIndex
                | FixedToolCandidate::QueryProjection
                | FixedToolCandidate::GetCommit
                | FixedToolCandidate::ScanCommits
                | FixedToolCandidate::TraceProvenance
                | FixedToolCandidate::ListPendingOutboxDeliveries
        );
        let all_partitions_required = matches!(
            candidate,
            FixedToolCandidate::QueryProjection
                | FixedToolCandidate::GetCommit
                | FixedToolCandidate::ScanCommits
                | FixedToolCandidate::TraceProvenance
                | FixedToolCandidate::ListPendingOutboxDeliveries
        );
        let global_allowed =
            !global_required || matches!(self.grant.tenant_scope(), TenantScope::Global);
        let partitions_allowed = !all_partitions_required
            || matches!(self.grant.partition_scope(), PartitionScopeV1::All);
        if has_permission && !needs_approval && global_allowed && partitions_allowed {
            DiscoveryVisibility::Visible
        } else {
            DiscoveryVisibility::Hidden
        }
    }
}

pub(crate) fn application_query_accesses_visible(
    grant: &CapabilityGrantV1,
    lineage: &ContractLineage,
    accesses: &[ApplicationQueryAccessRequirement],
) -> bool {
    accesses.iter().all(|access| {
        if access.maximum_rows() > grant.max_scan_rows() {
            return false;
        }
        if access.non_key_fields().is_empty() {
            return true;
        }
        grant.field_visibility().iter().any(|visibility| {
            visibility.lineage() == lineage
                && visibility.entity_type() == access.entity_type_id()
                && access
                    .non_key_fields()
                    .iter()
                    .all(|field| visibility.fields().binary_search(field).is_ok())
        })
    })
}

impl fmt::Debug for AuthorizedDiscovery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AuthorizedDiscovery([REDACTED])")
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PermissionCheck {
    Missing,
    ApprovalRequired,
    Allowed,
}

pub(crate) fn check_permission(
    grant: &CapabilityGrantV1,
    requirement: &PermissionRequirement,
) -> PermissionCheck {
    let mut present_requiring_approval = false;
    let mut inspect = |permission: &CapabilityPermissionV1| {
        if !grant.permissions().contains_exact(permission) {
            return None;
        }
        let kind = permission.kind();
        if grant.approval_required().binary_search(&kind).is_ok() {
            present_requiring_approval = true;
            None
        } else {
            Some(PermissionCheck::Allowed)
        }
    };

    let allowed = match requirement {
        PermissionRequirement::Kind(kind) => CapabilityPermissionV1::unparameterized(*kind)
            .ok()
            .and_then(|permission| inspect(&permission)),
        PermissionRequirement::Exact(permission) => inspect(permission),
        PermissionRequirement::Either(left, right) => {
            [*left, *right].into_iter().find_map(|kind| {
                CapabilityPermissionV1::unparameterized(kind)
                    .ok()
                    .and_then(|permission| inspect(&permission))
            })
        }
        PermissionRequirement::AnyKind(kinds) => kinds.iter().find_map(|kind| {
            if !grant.permissions().contains_kind(*kind) {
                return None;
            }
            if grant.approval_required().binary_search(kind).is_ok() {
                present_requiring_approval = true;
                None
            } else {
                Some(PermissionCheck::Allowed)
            }
        }),
    };
    allowed.unwrap_or(if present_requiring_approval {
        PermissionCheck::ApprovalRequired
    } else {
        PermissionCheck::Missing
    })
}

pub(crate) fn derive_field_mask(
    grant: &CapabilityGrantV1,
    requirement: FieldRequirement<'_>,
) -> FieldMask {
    let visible = grant
        .field_visibility()
        .iter()
        .find(|entry| {
            entry.lineage() == requirement.lineage
                && entry.entity_type() == requirement.entity_type_id
        })
        .map_or(&[][..], |entry| entry.fields());
    let fields = requirement
        .non_key_fields
        .iter()
        .copied()
        .filter(|field| visible.binary_search(field).is_ok())
        .collect();
    FieldMask::new(
        requirement.lineage.clone(),
        requirement.entity_type_id,
        fields,
    )
}

fn resource_scope_is_discoverable(
    grant: &CapabilityGrantV1,
    candidate: &DiscoveryResource,
) -> bool {
    match candidate {
        DiscoveryResource::CommandOutcome { .. }
        | DiscoveryResource::Commit
        | DiscoveryResource::Provenance => {
            matches!(grant.tenant_scope(), TenantScope::Global)
                && matches!(grant.partition_scope(), PartitionScopeV1::All)
        }
        DiscoveryResource::EntitySchema(_) => {
            matches!(grant.tenant_scope(), TenantScope::Global)
        }
        _ => true,
    }
}

/// Structured deny-by-default result of current policy evaluation.
#[derive(Eq, PartialEq)]
pub enum Decision {
    /// Current policy allowed this exact request and produced obligations.
    Allow(Box<AuthorizedOperation>),
    /// Initial policy authorized transaction-current preparation of one mutation.
    PrepareCapabilityMutation(Box<AuthorizedCapabilityMutationPreparation>),
    /// Current policy denied the request with a closed internal code.
    Deny(PolicyCode),
}

/// Why a test-only discovery audit obligation could not be attached.
#[cfg(feature = "test-fixtures")]
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiscoveryAuditFixtureError {
    /// A denied decision cannot be converted into an allow proof.
    Denied,
    /// A capability-mutation preparation is not a discovery allow proof.
    CapabilityMutationPreparation,
    /// The allow proof belongs to an operation other than discovery.
    NonDiscoveryOperation,
    /// The allow proof does not carry the capability grant needed for filtering.
    MissingDiscoveryAuthority,
    /// The allow proof already carries an audit obligation.
    ExistingAuditObligation,
}

#[cfg(feature = "test-fixtures")]
impl fmt::Display for DiscoveryAuditFixtureError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Denied => "the policy decision denied the request",
            Self::CapabilityMutationPreparation => {
                "the policy decision prepared a capability mutation"
            }
            Self::NonDiscoveryOperation => "the allow proof is not for discovery",
            Self::MissingDiscoveryAuthority => "the allow proof lacks discovery authority",
            Self::ExistingAuditObligation => "the allow proof already has an audit obligation",
        })
    }
}

#[cfg(feature = "test-fixtures")]
impl std::error::Error for DiscoveryAuditFixtureError {}

impl Decision {
    /// Adds the standard-read audit obligation to one valid discovery allow proof.
    ///
    /// This transformation exists only for service audit fixtures. It cannot
    /// create authority, replace another audit classification, or convert any
    /// non-allow decision.
    #[cfg(feature = "test-fixtures")]
    #[doc(hidden)]
    pub fn with_test_standard_read_discovery_audit(
        self,
    ) -> Result<Self, DiscoveryAuditFixtureError> {
        let mut authorized = match self {
            Self::Allow(authorized) => authorized,
            Self::PrepareCapabilityMutation(_) => {
                return Err(DiscoveryAuditFixtureError::CapabilityMutationPreparation);
            }
            Self::Deny(_) => return Err(DiscoveryAuditFixtureError::Denied),
        };
        if !matches!(
            authorized.request.operation(),
            ServiceOperationV1::DiscoverCommandTools | ServiceOperationV1::DiscoverResources
        ) {
            return Err(DiscoveryAuditFixtureError::NonDiscoveryOperation);
        }
        if authorized.discovery_authority.is_none() {
            return Err(DiscoveryAuditFixtureError::MissingDiscoveryAuthority);
        }
        if authorized.obligations.audit_class.is_some() {
            return Err(DiscoveryAuditFixtureError::ExistingAuditObligation);
        }
        authorized.obligations.audit_class = Some(AuditClass::StandardRead);
        Ok(Self::Allow(authorized))
    }
}

impl fmt::Debug for Decision {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Allow(_) => formatter.write_str("Decision::Allow([REDACTED])"),
            Self::PrepareCapabilityMutation(_) => {
                formatter.write_str("Decision::PrepareCapabilityMutation([REDACTED])")
            }
            Self::Deny(code) => formatter.debug_tuple("Decision::Deny").field(code).finish(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU16;

    #[cfg(feature = "test-fixtures")]
    use riffdb_testkit::authorization::{
        AuthorizationFixture, AuthorizationFixtureConfig, AuthorizationFixtureTimes,
    };
    use riffdb_types::{
        AggregateTypeId, CapabilityPermissionKindV1, CapabilityPermissionsV1, CommandId,
        EntityFieldVisibilityV1, PartitionKeyBuilder, ReactiveModuleHash, ReactiveOperationName,
    };
    #[cfg(feature = "test-fixtures")]
    use riffdb_types::{Audience, RequestId, Timestamp};
    #[cfg(feature = "test-fixtures")]
    use std::num::NonZeroU32;

    use super::*;

    fn lineage() -> ContractLineage {
        ContractLineage::new("example.contract").expect("valid lineage")
    }

    fn grant(
        tenant_scope: TenantScope,
        partition_scope: PartitionScopeV1,
        permissions: Vec<CapabilityPermissionV1>,
        field_visibility: Vec<EntityFieldVisibilityV1>,
        approval_required: Vec<CapabilityPermissionKindV1>,
    ) -> CapabilityGrantV1 {
        CapabilityGrantV1::new(
            tenant_scope,
            partition_scope,
            CapabilityPermissionsV1::new(permissions).expect("valid permissions"),
            field_visibility,
            NonZeroU16::new(10).expect("nonzero rows"),
            approval_required,
        )
        .expect("valid grant")
    }

    fn obligations() -> Obligations {
        obligations_with_audit(None)
    }

    fn obligations_with_audit(audit_class: Option<AuditClass>) -> Obligations {
        Obligations::new(
            TenantScope::Global,
            None,
            None,
            None,
            None,
            audit_class,
            OutputClassification::PublicMetadata,
        )
    }

    fn authorization_identity() -> CurrentAuthorizationIdentity {
        CurrentAuthorizationIdentity::new(
            CapabilityId::from_unix_milliseconds_and_random(2, [0x52; 10]).expect("valid UUIDv7"),
            NonZeroU64::MIN,
            ActorId::new("discovery-principal").expect("bounded principal"),
            ActorKind::Service,
            CheckedCapabilityValidity::new(
                riffdb_types::Timestamp::new(0, 0).expect("issued at"),
                riffdb_types::Timestamp::new(1_000, 0).expect("expires at"),
            ),
        )
    }

    fn authorized_operation(
        request: OperationRequest,
        obligations: Obligations,
        discovery_grant: Option<CapabilityGrantV1>,
    ) -> AuthorizedOperation {
        let database_id =
            DatabaseId::from_unix_milliseconds_and_random(1, [0x51; 10]).expect("valid UUIDv7");
        let environment = Environment::new("discovery-test").expect("bounded environment");
        match discovery_grant {
            Some(grant) => AuthorizedOperation::new_discovery(
                database_id,
                environment,
                request,
                obligations,
                grant,
                authorization_identity(),
            ),
            None => AuthorizedOperation::new(
                database_id,
                environment,
                request,
                obligations,
                authorization_identity(),
            ),
        }
    }

    fn discovery(request: OperationRequest, grant: CapabilityGrantV1) -> AuthorizedDiscovery {
        authorized_operation(request, obligations(), Some(grant))
            .into_discovery()
            .expect("discovery proof")
    }

    #[cfg(feature = "test-fixtures")]
    #[test]
    fn test_discovery_audit_fixture_only_transforms_fresh_discovery_allows() {
        let discovery_grant = || {
            grant(
                TenantScope::Global,
                PartitionScopeV1::All,
                Vec::new(),
                Vec::new(),
                Vec::new(),
            )
        };
        for request in [
            OperationRequest::discover_command_tools(),
            OperationRequest::discover_resources(),
        ] {
            let decision = Decision::Allow(Box::new(authorized_operation(
                request,
                obligations(),
                Some(discovery_grant()),
            )))
            .with_test_standard_read_discovery_audit()
            .expect("fresh discovery allow accepts the fixture audit class");
            let Decision::Allow(authorized) = decision else {
                panic!("the fixture preserves the allow decision");
            };
            assert_eq!(
                authorized.obligations().audit_class(),
                Some(AuditClass::StandardRead)
            );
            assert_eq!(
                Decision::Allow(authorized).with_test_standard_read_discovery_audit(),
                Err(DiscoveryAuditFixtureError::ExistingAuditObligation)
            );
        }

        assert_eq!(
            Decision::Deny(PolicyCode::MissingPermission).with_test_standard_read_discovery_audit(),
            Err(DiscoveryAuditFixtureError::Denied)
        );
        assert_eq!(
            Decision::Allow(Box::new(authorized_operation(
                OperationRequest::get_health(),
                obligations(),
                Some(discovery_grant()),
            )))
            .with_test_standard_read_discovery_audit(),
            Err(DiscoveryAuditFixtureError::NonDiscoveryOperation)
        );
        assert_eq!(
            Decision::Allow(Box::new(authorized_operation(
                OperationRequest::discover_command_tools(),
                obligations(),
                None,
            )))
            .with_test_standard_read_discovery_audit(),
            Err(DiscoveryAuditFixtureError::MissingDiscoveryAuthority)
        );
        assert_eq!(
            Decision::Allow(Box::new(authorized_operation(
                OperationRequest::discover_resources(),
                obligations_with_audit(Some(AuditClass::AdministrativeRead)),
                Some(discovery_grant()),
            )))
            .with_test_standard_read_discovery_audit(),
            Err(DiscoveryAuditFixtureError::ExistingAuditObligation)
        );
    }

    fn explicit_partition() -> PartitionScopeV1 {
        let mut builder = PartitionKeyBuilder::new(AggregateTypeId::first());
        builder.push_u64(1).expect("bounded component");
        let partition = builder.finish().expect("valid partition");
        PartitionScopeV1::explicit(vec![ScopedPartitionV1::new(lineage(), partition)])
            .expect("explicit scope")
    }

    #[cfg(feature = "test-fixtures")]
    #[test]
    fn test_discovery_audit_fixture_rejects_capability_mutation_preparation() {
        struct FixedClock(Timestamp);

        impl crate::AuthorizationClock for FixedClock {
            fn now(&self) -> Result<Timestamp, crate::AuthorizationClockError> {
                Ok(self.0)
            }
        }

        let timestamp = |seconds| Timestamp::new(seconds, 0).expect("valid timestamp");
        let database_id =
            DatabaseId::from_unix_milliseconds_and_random(3, [0x53; 10]).expect("valid UUIDv7");
        let environment = Environment::new("fixture-test").expect("bounded environment");
        let audience = Audience::new("fixture-grpc").expect("bounded audience");
        let parent_grant = grant(
            TenantScope::Global,
            PartitionScopeV1::All,
            vec![
                CapabilityPermissionV1::unparameterized(
                    CapabilityPermissionKindV1::CreateCapability,
                )
                .expect("unparameterized create permission"),
                CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::ReadHealth)
                    .expect("unparameterized health permission"),
            ],
            Vec::new(),
            Vec::new(),
        );
        let fixture = AuthorizationFixture::new(AuthorizationFixtureConfig::new(
            database_id,
            environment.clone(),
            ActorId::new("fixture-parent").expect("bounded principal"),
            ActorKind::Service,
            audience.clone(),
            AuthorizationFixtureTimes::new(timestamp(100), timestamp(1_000), timestamp(150)),
            parent_grant,
        ))
        .expect("valid authorization fixture");
        let target_grant = grant(
            TenantScope::Global,
            PartitionScopeV1::All,
            vec![
                CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::ReadHealth)
                    .expect("unparameterized health permission"),
            ],
            Vec::new(),
            Vec::new(),
        );
        let requested_record = crate::NormalizedCapabilityCreateRecord::new(
            database_id,
            environment.clone(),
            ActorId::new("fixture-child").expect("bounded principal"),
            ActorKind::Agent,
            NonZeroU32::new(60).expect("nonzero lifetime"),
            vec![audience.clone()],
            target_grant,
        )
        .expect("valid requested capability");
        let target = crate::CapabilityCreateTargetFacts::new(
            RequestId::from_unix_milliseconds_and_random(4, [0x54; 10]).expect("valid UUIDv7"),
            CapabilityId::from_unix_milliseconds_and_random(5, [0x55; 10]).expect("valid UUIDv7"),
            requested_record,
        );
        let trusted_audiences =
            crate::TrustedAudienceCatalog::new(vec![audience]).expect("one trusted audience");
        let resolver = fixture.current_capability_resolver();
        let decision = crate::CurrentAuthorizer::new(
            &resolver,
            &FixedClock(timestamp(200)),
            &crate::NoopAuthorizationTelemetry,
            database_id,
            environment,
        )
        .with_trusted_audience_catalog(&trusted_audiences)
        .authorize(
            fixture.authenticated_principal(),
            OperationRequest::create_capability(target),
        )
        .expect("policy decision");
        assert!(matches!(&decision, Decision::PrepareCapabilityMutation(_)));
        assert_eq!(
            decision.with_test_standard_read_discovery_audit(),
            Err(DiscoveryAuditFixtureError::CapabilityMutationPreparation)
        );
    }

    #[test]
    fn closed_tags_are_exact_and_reject_unknown_values() {
        assert_eq!(PolicyCode::ALL.map(PolicyCode::tag), [1, 2, 3, 4, 5, 6, 7]);
        assert_eq!(
            ObligationKind::ALL.map(ObligationKind::tag),
            [1, 2, 3, 4, 5, 6, 7]
        );
        assert_eq!(AuditClass::ALL.map(AuditClass::tag), [1, 2, 3, 4]);
        assert_eq!(
            OutputClassification::ALL.map(OutputClassification::tag),
            [1, 2, 3]
        );

        for code in PolicyCode::ALL {
            assert_eq!(PolicyCode::from_tag(code.tag()), Some(code));
        }
        for kind in ObligationKind::ALL {
            assert_eq!(ObligationKind::from_tag(kind.tag()), Some(kind));
        }
        for class in AuditClass::ALL {
            assert_eq!(AuditClass::from_tag(class.tag()), Some(class));
        }
        for class in OutputClassification::ALL {
            assert_eq!(OutputClassification::from_tag(class.tag()), Some(class));
        }
        assert_eq!(PolicyCode::from_tag(0), None);
        assert_eq!(PolicyCode::from_tag(8), None);
        assert_eq!(ObligationKind::from_tag(0), None);
        assert_eq!(ObligationKind::from_tag(8), None);
        assert_eq!(AuditClass::from_tag(0), None);
        assert_eq!(AuditClass::from_tag(5), None);
        assert_eq!(OutputClassification::from_tag(0), None);
        assert_eq!(OutputClassification::from_tag(4), None);
    }

    #[test]
    fn policy_codes_expose_only_static_safe_text() {
        for code in PolicyCode::ALL {
            assert!(!code.safe_text().is_empty());
            assert!(!code.safe_text().contains("secret"));
        }
    }

    #[test]
    fn application_tool_discovery_is_exact_and_invocation_scope_neutral() {
        let candidate = CommandToolCandidate::new(lineage(), CommandId::first());
        let permission = CapabilityPermissionV1::InvokeCommand(lineage(), CommandId::first());
        let visible = discovery(
            OperationRequest::discover_command_tools(),
            grant(
                TenantScope::Global,
                PartitionScopeV1::All,
                vec![permission.clone()],
                Vec::new(),
                Vec::new(),
            ),
        )
        .tool_catalog(
            FixedToolCandidate::ALL.as_slice(),
            std::slice::from_ref(&candidate),
            &[],
        )
        .expect("bounded catalog");
        assert_eq!(visible.command_tools(), &[DiscoveryVisibility::Visible]);

        let approval_hidden = grant(
            TenantScope::Global,
            PartitionScopeV1::All,
            vec![permission.clone()],
            Vec::new(),
            vec![CapabilityPermissionKindV1::InvokeCommand],
        );
        let hidden = discovery(OperationRequest::discover_command_tools(), approval_hidden)
            .tool_catalog(
                FixedToolCandidate::ALL.as_slice(),
                std::slice::from_ref(&candidate),
                &[],
            )
            .expect("bounded catalog");
        assert_eq!(hidden.command_tools(), &[DiscoveryVisibility::Hidden]);

        for (tenant_scope, partition_scope) in [
            (TenantScope::Global, explicit_partition()),
            (
                TenantScope::Tenant(riffdb_types::TenantId::new("tenant-a").expect("valid tenant")),
                PartitionScopeV1::All,
            ),
        ] {
            let scoped = discovery(
                OperationRequest::discover_command_tools(),
                grant(
                    tenant_scope,
                    partition_scope,
                    vec![permission.clone()],
                    Vec::new(),
                    Vec::new(),
                ),
            )
            .tool_catalog(
                FixedToolCandidate::ALL.as_slice(),
                std::slice::from_ref(&candidate),
                &[],
            )
            .expect("bounded catalog");
            assert_eq!(scoped.command_tools(), &[DiscoveryVisibility::Visible]);
        }
    }

    #[test]
    fn named_query_discovery_is_exact_module_and_name_filtered() {
        let module_hash = riffdb_types::QueryModuleHash::from_bytes([0x71; 32]);
        let query_name = riffdb_types::QueryOperationName::new("TicketPage")
            .expect("valid query operation name");
        let candidate =
            crate::NamedQueryToolCandidate::new(lineage(), module_hash, query_name.clone());
        let permission =
            CapabilityPermissionV1::ExecuteNamedQuery(lineage(), module_hash, query_name);

        let visible = discovery(
            OperationRequest::discover_command_tools(),
            grant(
                TenantScope::Global,
                PartitionScopeV1::All,
                vec![permission],
                Vec::new(),
                Vec::new(),
            ),
        )
        .tool_catalog(
            FixedToolCandidate::ALL.as_slice(),
            &[],
            std::slice::from_ref(&candidate),
        )
        .expect("bounded catalog");
        assert_eq!(visible.named_query_tools(), &[DiscoveryVisibility::Visible]);

        let tenant_visible = discovery(
            OperationRequest::discover_command_tools(),
            grant(
                TenantScope::Tenant(riffdb_types::TenantId::new("tenant-a").expect("valid tenant")),
                explicit_partition(),
                vec![CapabilityPermissionV1::ExecuteNamedQuery(
                    lineage(),
                    module_hash,
                    riffdb_types::QueryOperationName::new("TicketPage")
                        .expect("valid query operation name"),
                )],
                Vec::new(),
                Vec::new(),
            ),
        )
        .tool_catalog(
            FixedToolCandidate::ALL.as_slice(),
            &[],
            std::slice::from_ref(&candidate),
        )
        .expect("bounded catalog");
        assert_eq!(
            tenant_visible.named_query_tools(),
            &[DiscoveryVisibility::Visible]
        );

        let hidden = discovery(
            OperationRequest::discover_command_tools(),
            grant(
                TenantScope::Global,
                PartitionScopeV1::All,
                vec![CapabilityPermissionV1::ExecuteNamedQuery(
                    lineage(),
                    riffdb_types::QueryModuleHash::from_bytes([0x72; 32]),
                    riffdb_types::QueryOperationName::new("TicketPage")
                        .expect("valid query operation name"),
                )],
                Vec::new(),
                Vec::new(),
            ),
        )
        .tool_catalog(FixedToolCandidate::ALL.as_slice(), &[], &[candidate])
        .expect("bounded catalog");
        assert_eq!(hidden.named_query_tools(), &[DiscoveryVisibility::Hidden]);
    }

    #[test]
    fn application_catalog_filters_exact_operations_under_describe_authority() {
        let module_hash = riffdb_types::QueryModuleHash::from_bytes([0x73; 32]);
        let query_name = riffdb_types::QueryOperationName::new("TicketPage")
            .expect("valid query operation name");
        let command = CommandToolCandidate::new(lineage(), CommandId::first());
        let query = ApplicationCatalogQueryCandidate::new(
            crate::NamedQueryToolCandidate::new(lineage(), module_hash, query_name.clone()),
            vec![
                ApplicationQueryAccessRequirement::new(
                    EntityTypeId::first(),
                    None,
                    vec![FieldId::first()],
                    NonZeroU16::new(10).expect("nonzero rows"),
                )
                .expect("query access"),
            ],
        )
        .expect("catalog query candidate");
        let permissions = vec![
            CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::ReadContract)
                .expect("read-contract permission"),
            CapabilityPermissionV1::ExecuteNamedQuery(lineage(), module_hash, query_name),
        ];
        let visibility = discovery(
            OperationRequest::describe_contract(),
            grant(
                TenantScope::Global,
                PartitionScopeV1::All,
                permissions.clone(),
                vec![
                    EntityFieldVisibilityV1::new(
                        lineage(),
                        EntityTypeId::first(),
                        vec![FieldId::first()],
                    )
                    .expect("field visibility"),
                ],
                Vec::new(),
            ),
        )
        .application_catalog(std::slice::from_ref(&command), std::slice::from_ref(&query))
        .expect("bounded application catalog");

        assert_eq!(
            visibility.command_operations(),
            &[DiscoveryVisibility::Hidden]
        );
        assert_eq!(
            visibility.named_query_operations(),
            &[DiscoveryVisibility::Visible]
        );
        let hidden_fields = discovery(
            OperationRequest::describe_contract(),
            grant(
                TenantScope::Global,
                PartitionScopeV1::All,
                permissions,
                Vec::new(),
                Vec::new(),
            ),
        )
        .application_catalog(&[], std::slice::from_ref(&query))
        .expect("bounded application catalog");
        assert_eq!(
            hidden_fields.named_query_operations(),
            &[DiscoveryVisibility::Hidden],
            "a named-query grant cannot disclose fields it cannot read"
        );
        assert_eq!(
            discovery(
                OperationRequest::discover_resources(),
                grant(
                    TenantScope::Global,
                    PartitionScopeV1::All,
                    vec![],
                    Vec::new(),
                    Vec::new(),
                ),
            )
            .application_catalog(&[], &[]),
            Err(DiscoveryFilterError::CatalogMismatch)
        );
    }

    #[test]
    fn reactive_fixed_tool_discovery_follows_permission_families() {
        let module = ReactiveModuleHash::from_bytes([0x71; 32]);
        let stream = ReactiveOperationName::new("TicketEvents").expect("operation name");
        let watch = ReactiveOperationName::new("TicketQueueWatch").expect("operation name");
        let contextual = ReactiveOperationName::new("TriageTicket").expect("operation name");
        let cases = [
            (
                CapabilityPermissionV1::ConsumeEventStream(lineage(), module, stream.clone()),
                &[
                    FixedToolCandidate::EventNext,
                    FixedToolCandidate::EventAck,
                    FixedToolCandidate::EventNack,
                    FixedToolCandidate::EventStatus,
                ][..],
            ),
            (
                CapabilityPermissionV1::SeekEventStreamConsumer(lineage(), module, stream),
                &[FixedToolCandidate::EventSeek][..],
            ),
            (
                CapabilityPermissionV1::WatchNamedQuery(lineage(), module, watch),
                &[FixedToolCandidate::QueryWatch][..],
            ),
            (
                CapabilityPermissionV1::ConsumeContextualSubscription(
                    lineage(),
                    module,
                    contextual,
                ),
                &[
                    FixedToolCandidate::ContextualNext,
                    FixedToolCandidate::ContextualAck,
                    FixedToolCandidate::ContextualNack,
                    FixedToolCandidate::ContextualStatus,
                    FixedToolCandidate::ContextualReact,
                ][..],
            ),
        ];

        for (permission, expected) in cases {
            let visibility = discovery(
                OperationRequest::discover_command_tools(),
                grant(
                    TenantScope::Global,
                    PartitionScopeV1::All,
                    vec![permission],
                    Vec::new(),
                    Vec::new(),
                ),
            )
            .tool_catalog(FixedToolCandidate::ALL.as_slice(), &[], &[])
            .expect("bounded catalog");
            let visible = FixedToolCandidate::ALL
                .iter()
                .zip(visibility.fixed_tools())
                .filter_map(|(candidate, visibility)| {
                    (*visibility == DiscoveryVisibility::Visible).then_some(*candidate)
                })
                .collect::<Vec<_>>();
            assert_eq!(visible, expected);
        }
    }

    #[test]
    fn reactive_wakeup_resource_requires_one_unapproved_reactive_permission() {
        let module = ReactiveModuleHash::from_bytes([0x73; 32]);
        let operation = ReactiveOperationName::new("ReactiveOperation").expect("operation name");
        let permissions = [
            CapabilityPermissionV1::WatchNamedQuery(lineage(), module, operation.clone()),
            CapabilityPermissionV1::ConsumeEventStream(lineage(), module, operation.clone()),
            CapabilityPermissionV1::ConsumeContextualSubscription(lineage(), module, operation),
        ];

        for permission in permissions {
            let kind = permission.kind();
            let visible = |approval_required| {
                discovery(
                    OperationRequest::discover_resources(),
                    grant(
                        TenantScope::Global,
                        PartitionScopeV1::All,
                        vec![permission.clone()],
                        Vec::new(),
                        approval_required,
                    ),
                )
                .resource_catalog(&[DiscoveryResource::ReactiveWakeup])
                .expect("bounded resource catalog")
            };
            assert!(matches!(
                visible(Vec::new()).as_slice(),
                [ResourceDiscoveryVisibility::Visible { field_mask: None }]
            ));
            assert_eq!(visible(vec![kind]), [ResourceDiscoveryVisibility::Hidden]);
        }

        let hidden = discovery(
            OperationRequest::discover_resources(),
            grant(
                TenantScope::Global,
                PartitionScopeV1::All,
                Vec::new(),
                Vec::new(),
                Vec::new(),
            ),
        )
        .resource_catalog(&[DiscoveryResource::ReactiveWakeup])
        .expect("bounded resource catalog");
        assert_eq!(hidden, [ResourceDiscoveryVisibility::Hidden]);
    }

    #[test]
    fn unfiltered_administrative_discovery_matches_global_all_invocation_scope() {
        let tool_cases = [
            (
                FixedToolCandidate::GetCommit,
                CapabilityPermissionKindV1::ReadCommit,
            ),
            (
                FixedToolCandidate::ScanCommits,
                CapabilityPermissionKindV1::ScanCommits,
            ),
            (
                FixedToolCandidate::TraceProvenance,
                CapabilityPermissionKindV1::ReadProvenance,
            ),
            (
                FixedToolCandidate::ListPendingOutboxDeliveries,
                CapabilityPermissionKindV1::InspectOutbox,
            ),
        ];
        for (candidate, permission_kind) in tool_cases {
            let permission = CapabilityPermissionV1::unparameterized(permission_kind)
                .expect("administrative permission");
            let visibility = |tenant_scope, partition_scope| {
                discovery(
                    OperationRequest::discover_command_tools(),
                    grant(
                        tenant_scope,
                        partition_scope,
                        vec![permission.clone()],
                        Vec::new(),
                        Vec::new(),
                    ),
                )
                .tool_catalog(FixedToolCandidate::ALL.as_slice(), &[], &[])
                .expect("bounded catalog")
                .fixed_tools()[FixedToolCandidate::ALL
                    .iter()
                    .position(|fixed| *fixed == candidate)
                    .expect("candidate is in the fixed inventory")]
            };

            assert_eq!(
                visibility(TenantScope::Global, PartitionScopeV1::All),
                DiscoveryVisibility::Visible
            );
            assert_eq!(
                visibility(TenantScope::Global, explicit_partition()),
                DiscoveryVisibility::Hidden
            );
            assert_eq!(
                visibility(
                    TenantScope::Tenant(
                        riffdb_types::TenantId::new("tenant-a").expect("valid tenant")
                    ),
                    PartitionScopeV1::All,
                ),
                DiscoveryVisibility::Hidden
            );
        }

        for (candidate, permission_kind) in [
            (
                DiscoveryResource::Commit,
                CapabilityPermissionKindV1::ReadCommit,
            ),
            (
                DiscoveryResource::Provenance,
                CapabilityPermissionKindV1::ReadProvenance,
            ),
        ] {
            let permission = CapabilityPermissionV1::unparameterized(permission_kind)
                .expect("administrative permission");
            let visibility = |tenant_scope, partition_scope| {
                discovery(
                    OperationRequest::discover_resources(),
                    grant(
                        tenant_scope,
                        partition_scope,
                        vec![permission.clone()],
                        Vec::new(),
                        Vec::new(),
                    ),
                )
                .resource_catalog(std::slice::from_ref(&candidate))
                .expect("bounded catalog")
                .remove(0)
            };
            assert!(matches!(
                visibility(TenantScope::Global, PartitionScopeV1::All),
                ResourceDiscoveryVisibility::Visible { .. }
            ));
            assert_eq!(
                visibility(TenantScope::Global, explicit_partition()),
                ResourceDiscoveryVisibility::Hidden
            );
            assert_eq!(
                visibility(
                    TenantScope::Tenant(
                        riffdb_types::TenantId::new("tenant-a").expect("valid tenant")
                    ),
                    PartitionScopeV1::All,
                ),
                ResourceDiscoveryVisibility::Hidden
            );
        }
    }

    #[test]
    fn entity_schema_discovery_derives_an_empty_capable_target_mask() {
        let candidate = DiscoveryResource::EntitySchema(
            crate::EntitySchemaCandidate::new(
                lineage(),
                EntityTypeId::first(),
                vec![FieldId::first(), FieldId::new(2).expect("nonzero field")],
            )
            .expect("valid candidate"),
        );
        let permission = CapabilityPermissionV1::ReadEntity(lineage(), EntityTypeId::first());
        let visible = discovery(
            OperationRequest::discover_resources(),
            grant(
                TenantScope::Global,
                PartitionScopeV1::All,
                vec![permission.clone()],
                vec![
                    EntityFieldVisibilityV1::new(
                        lineage(),
                        EntityTypeId::first(),
                        vec![FieldId::first()],
                    )
                    .expect("valid visibility"),
                ],
                Vec::new(),
            ),
        )
        .resource_catalog(std::slice::from_ref(&candidate))
        .expect("bounded catalog");
        let ResourceDiscoveryVisibility::Visible {
            field_mask: Some(mask),
        } = &visible[0]
        else {
            panic!("entity schema should be visible with a mask");
        };
        assert_eq!(mask.fields(), &[FieldId::first()]);

        let key_only = discovery(
            OperationRequest::discover_resources(),
            grant(
                TenantScope::Global,
                PartitionScopeV1::All,
                vec![permission],
                Vec::new(),
                Vec::new(),
            ),
        )
        .resource_catalog(std::slice::from_ref(&candidate))
        .expect("bounded catalog");
        let ResourceDiscoveryVisibility::Visible {
            field_mask: Some(mask),
        } = &key_only[0]
        else {
            panic!("entity schema should allow a key-only mask");
        };
        assert!(mask.fields().is_empty());
    }

    #[test]
    fn discovery_rejects_over_limit_candidates_before_filtering() {
        let candidate = CommandToolCandidate::new(lineage(), CommandId::first());
        let candidates = vec![candidate; MAX_DISCOVERY_PAGE_ITEMS + 1];
        let result = discovery(
            OperationRequest::discover_command_tools(),
            grant(
                TenantScope::Global,
                PartitionScopeV1::All,
                Vec::new(),
                Vec::new(),
                Vec::new(),
            ),
        )
        .tool_catalog(FixedToolCandidate::ALL.as_slice(), &candidates, &[]);
        assert_eq!(result, Err(DiscoveryFilterError::TooManyCandidates));
    }

    #[test]
    fn resource_discovery_rejects_aggregate_field_amplification() {
        let fields = (1..=32_768)
            .map(|value| FieldId::new(value).expect("nonzero field"))
            .collect();
        let candidate = DiscoveryResource::EntitySchema(
            crate::EntitySchemaCandidate::new(lineage(), EntityTypeId::first(), fields)
                .expect("individually bounded candidate"),
        );
        let candidates = vec![candidate.clone(), candidate];
        let result = discovery(
            OperationRequest::discover_resources(),
            grant(
                TenantScope::Global,
                PartitionScopeV1::All,
                vec![CapabilityPermissionV1::ReadEntity(
                    lineage(),
                    EntityTypeId::first(),
                )],
                Vec::new(),
                Vec::new(),
            ),
        )
        .resource_catalog(&candidates);
        assert_eq!(
            result,
            Err(DiscoveryFilterError::CandidateFieldsLimitExceeded)
        );
    }

    #[test]
    fn obligation_kinds_are_emitted_once_in_canonical_order() {
        let PartitionScopeV1::Explicit(mut entries) = explicit_partition() else {
            panic!("test scope must be explicit");
        };
        let obligations = Obligations::new(
            TenantScope::Global,
            Some(PartitionConstraint::Exact(entries.remove(0))),
            Some(FieldMask::new(lineage(), EntityTypeId::first(), Vec::new())),
            NonZeroU16::new(5),
            Some(ApprovalId::new("approval-1").expect("valid approval")),
            Some(AuditClass::CommandMutation),
            OutputClassification::PolicyFilteredApplicationData,
        );
        assert_eq!(obligations.kinds().collect::<Vec<_>>(), ObligationKind::ALL);
    }
}
