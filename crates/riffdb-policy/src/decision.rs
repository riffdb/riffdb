//! Closed policy decisions and canonical authorization obligations.

use std::fmt;
use std::num::NonZeroU16;

use riffdb_types::{
    ApprovalId, CapabilityGrantV1, CapabilityPermissionV1, ContractLineage, EntityTypeId, FieldId,
    MAX_CAPABILITY_FIELD_VISIBILITY, PartitionScopeV1, ScopedPartitionV1, ServiceOperationV1,
    TenantScope,
};

use crate::operation::{
    FieldRequirement, PermissionRequirement, command_tool_permission, fixed_tool_permission_kind,
    resource_field_requirement, resource_permission,
};
use crate::{
    AuthorizedCapabilityMutationPreparation, CommandToolCandidate, DiscoveryResource,
    FixedToolCandidate, OperationRequest,
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

/// Privately constructed proof that current policy allowed one exact request.
#[derive(Eq, PartialEq)]
pub struct AuthorizedOperation {
    request: OperationRequest,
    obligations: Obligations,
    discovery_authority: Option<CapabilityGrantV1>,
}

impl AuthorizedOperation {
    pub(crate) const fn new(request: OperationRequest, obligations: Obligations) -> Self {
        Self {
            request,
            obligations,
            discovery_authority: None,
        }
    }

    pub(crate) const fn new_discovery(
        request: OperationRequest,
        obligations: Obligations,
        grant: CapabilityGrantV1,
    ) -> Self {
        Self {
            request,
            obligations,
            discovery_authority: Some(grant),
        }
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

    /// Consumes a discovery allow proof into its candidate-filtering authority.
    ///
    /// A non-discovery proof is consumed and rejected so it cannot be reused as
    /// discovery authority.
    pub fn into_discovery(self) -> Result<AuthorizedDiscovery, DiscoveryFilterError> {
        if !matches!(
            self.request.operation(),
            ServiceOperationV1::DiscoverCommandTools | ServiceOperationV1::DiscoverResources
        ) {
            return Err(DiscoveryFilterError::CatalogMismatch);
        }
        let Self {
            request,
            obligations,
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
        candidates: &[CommandToolCandidate],
    ) -> Result<ToolCatalogVisibility, DiscoveryFilterError> {
        if self.request.operation() != ServiceOperationV1::DiscoverCommandTools {
            return Err(DiscoveryFilterError::CatalogMismatch);
        }
        if fixed_candidates != FixedToolCandidate::ALL.as_slice() {
            return Err(DiscoveryFilterError::FixedToolInventoryMismatch);
        }
        if candidates.len() > MAX_DISCOVERY_PAGE_ITEMS {
            return Err(DiscoveryFilterError::TooManyCandidates);
        }
        let fixed_tools = fixed_candidates
            .iter()
            .map(|candidate| self.fixed_tool_visibility(*candidate))
            .collect();
        let command_tools = candidates
            .iter()
            .map(|candidate| {
                let globally_scoped = matches!(self.grant.tenant_scope(), TenantScope::Global);
                let all_partitions = matches!(self.grant.partition_scope(), PartitionScopeV1::All);
                if globally_scoped
                    && all_partitions
                    && check_permission(&self.grant, &command_tool_permission(candidate))
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
        );
        let all_partitions_required = matches!(candidate, FixedToolCandidate::QueryProjection);
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
        let target = permission.canonical_key();
        if grant
            .permissions()
            .as_slice()
            .binary_search_by(|candidate| candidate.canonical_key().cmp(&target))
            .is_err()
        {
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
        DiscoveryResource::CommandOutcome { .. } => {
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

    use riffdb_types::{
        AggregateTypeId, CapabilityPermissionKindV1, CapabilityPermissionsV1, CommandId,
        EntityFieldVisibilityV1, PartitionKeyBuilder,
    };

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
        Obligations::new(
            TenantScope::Global,
            None,
            None,
            None,
            None,
            None,
            OutputClassification::PublicMetadata,
        )
    }

    fn discovery(request: OperationRequest, grant: CapabilityGrantV1) -> AuthorizedDiscovery {
        AuthorizedOperation::new_discovery(request, obligations(), grant)
            .into_discovery()
            .expect("discovery proof")
    }

    fn explicit_partition() -> PartitionScopeV1 {
        let mut builder = PartitionKeyBuilder::new(AggregateTypeId::first());
        builder.push_u64(1).expect("bounded component");
        let partition = builder.finish().expect("valid partition");
        PartitionScopeV1::explicit(vec![ScopedPartitionV1::new(lineage(), partition)])
            .expect("explicit scope")
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
    fn command_discovery_is_exact_approval_and_scope_filtered() {
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
        )
        .expect("bounded catalog");
        assert_eq!(visible.command_tools(), &[DiscoveryVisibility::Visible]);

        for denied_grant in [
            grant(
                TenantScope::Global,
                explicit_partition(),
                vec![permission.clone()],
                Vec::new(),
                Vec::new(),
            ),
            grant(
                TenantScope::Global,
                PartitionScopeV1::All,
                vec![permission.clone()],
                Vec::new(),
                vec![CapabilityPermissionKindV1::InvokeCommand],
            ),
            grant(
                TenantScope::Tenant(riffdb_types::TenantId::new("tenant-a").expect("valid tenant")),
                PartitionScopeV1::All,
                vec![permission.clone()],
                Vec::new(),
                Vec::new(),
            ),
        ] {
            let hidden = discovery(OperationRequest::discover_command_tools(), denied_grant)
                .tool_catalog(
                    FixedToolCandidate::ALL.as_slice(),
                    std::slice::from_ref(&candidate),
                )
                .expect("bounded catalog");
            assert_eq!(hidden.command_tools(), &[DiscoveryVisibility::Hidden]);
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
        .tool_catalog(FixedToolCandidate::ALL.as_slice(), &candidates);
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
