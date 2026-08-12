//! Capability-mutation preparation and transaction-current reauthorization.

use std::{error::Error, fmt, num::NonZeroU32, num::NonZeroU64};

use riffdb_types::{
    ActorId, ActorKind, ApprovalId, Audience, CapabilityApplicationExportScopeV1,
    CapabilityGrantV1, CapabilityId, CapabilityPermissionKindV1, CapabilityPermissionV1,
    DatabaseId, Environment, PartitionScopeV1, RequestId, TenantScope, Timestamp,
};
pub use riffdb_types::{
    MAX_CAPABILITY_AUDIENCES, MAX_CAPABILITY_LIFETIME_SECONDS, RevocationReasonCodeV1,
};

use crate::PolicyCode;

/// Maximum audiences in the trusted server configuration.
pub const MAX_CONFIGURED_AUDIENCES: usize = 32;
/// Maximum aggregate audience bytes in the trusted server configuration.
pub const MAX_CONFIGURED_AUDIENCE_BYTES: usize = 16_384;

/// Safe failure to construct bounded capability-mutation facts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CapabilityMutationFactsError {
    /// At least one audience is required.
    EmptyAudiences,
    /// The supplied audience count exceeds its hard bound.
    TooManyAudiences,
    /// The same audience was supplied more than once.
    DuplicateAudience,
    /// Transaction-current audiences were not supplied in canonical order.
    NonCanonicalAudiences,
    /// Configured audience bytes exceed their aggregate hard bound.
    ConfiguredAudienceBytesExceeded,
    /// The requested lifetime exceeds the process hard bound.
    LifetimeLimitExceeded,
}

impl fmt::Display for CapabilityMutationFactsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::EmptyAudiences => "at least one capability audience is required",
            Self::TooManyAudiences => "capability audiences exceed the hard limit",
            Self::DuplicateAudience => "capability audiences contain a duplicate",
            Self::NonCanonicalAudiences => "capability audiences are not canonical",
            Self::ConfiguredAudienceBytesExceeded => {
                "configured audience bytes exceed the hard limit"
            }
            Self::LifetimeLimitExceeded => "capability lifetime exceeds the hard limit",
        })
    }
}

impl Error for CapabilityMutationFactsError {}

/// Canonical, bounded audiences trusted by this server configuration.
#[derive(Clone, Eq, PartialEq)]
pub struct TrustedAudienceCatalog(Vec<Audience>);

impl TrustedAudienceCatalog {
    /// Checks, sorts, and retains the complete trusted audience catalog.
    pub fn new(mut audiences: Vec<Audience>) -> Result<Self, CapabilityMutationFactsError> {
        canonicalize_audiences(&mut audiences, MAX_CONFIGURED_AUDIENCES)?;
        let bytes = audiences.iter().try_fold(0usize, |total, audience| {
            total.checked_add(audience.as_bytes().len())
        });
        if bytes.is_none_or(|bytes| bytes > MAX_CONFIGURED_AUDIENCE_BYTES) {
            return Err(CapabilityMutationFactsError::ConfiguredAudienceBytesExceeded);
        }
        Ok(Self(audiences))
    }

    /// Returns the complete canonical configured catalog.
    #[must_use]
    pub fn audiences(&self) -> &[Audience] {
        &self.0
    }

    fn contains_all(&self, audiences: &[Audience]) -> bool {
        audiences
            .iter()
            .all(|audience| self.0.binary_search(audience).is_ok())
    }
}

impl fmt::Debug for TrustedAudienceCatalog {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("TrustedAudienceCatalog([REDACTED])")
    }
}

/// Closed policy view of a capability lifecycle.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CapabilityActivity {
    /// The capability may authorize operations subject to all other checks.
    Active,
    /// The capability has been revoked.
    Revoked,
}

/// The normalized requested record used for capability-create replay identity.
///
/// This value intentionally excludes the outer [`RequestId`], target
/// [`CapabilityId`], token material, digest, issued/expiry timestamps, revision,
/// and assigned sequence. Replay identity is the target `CapabilityId` paired
/// with this record; a retry uses a fresh `RequestId`.
#[derive(Clone, Eq, PartialEq)]
pub struct NormalizedCapabilityCreateRecord {
    database_id: DatabaseId,
    environment: Environment,
    principal_id: ActorId,
    actor_kind: ActorKind,
    requested_lifetime_seconds: NonZeroU32,
    audiences: Vec<Audience>,
    grant: CapabilityGrantV1,
}

impl NormalizedCapabilityCreateRecord {
    /// Constructs the complete bounded and canonical requested record.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        database_id: DatabaseId,
        environment: Environment,
        principal_id: ActorId,
        actor_kind: ActorKind,
        requested_lifetime_seconds: NonZeroU32,
        mut audiences: Vec<Audience>,
        grant: CapabilityGrantV1,
    ) -> Result<Self, CapabilityMutationFactsError> {
        if requested_lifetime_seconds.get() > MAX_CAPABILITY_LIFETIME_SECONDS {
            return Err(CapabilityMutationFactsError::LifetimeLimitExceeded);
        }
        canonicalize_audiences(&mut audiences, MAX_CAPABILITY_AUDIENCES)?;
        Ok(Self {
            database_id,
            environment,
            principal_id,
            actor_kind,
            requested_lifetime_seconds,
            audiences,
            grant,
        })
    }

    /// Returns the database component of replay identity.
    #[must_use]
    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    /// Returns the environment component of replay identity.
    #[must_use]
    pub const fn environment(&self) -> &Environment {
        &self.environment
    }

    /// Returns the target-principal component of replay identity.
    #[must_use]
    pub const fn principal_id(&self) -> &ActorId {
        &self.principal_id
    }

    /// Returns the actor-kind component of replay identity.
    #[must_use]
    pub const fn actor_kind(&self) -> ActorKind {
        self.actor_kind
    }

    /// Returns the duration component of replay identity.
    #[must_use]
    pub const fn requested_lifetime_seconds(&self) -> NonZeroU32 {
        self.requested_lifetime_seconds
    }

    /// Returns the canonical audience component of replay identity.
    #[must_use]
    pub fn audiences(&self) -> &[Audience] {
        &self.audiences
    }

    /// Returns the complete checked grant component of replay identity.
    #[must_use]
    pub const fn grant(&self) -> &CapabilityGrantV1 {
        &self.grant
    }
}

impl fmt::Debug for NormalizedCapabilityCreateRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("NormalizedCapabilityCreateRecord([REDACTED])")
    }
}

/// Exact invocation and operation identity for one capability creation.
///
/// The request ID is tracing/audit identity only. The capability ID plus
/// [`Self::normalized_requested_record`] form replay identity.
#[derive(Clone, Eq, PartialEq)]
pub struct CapabilityCreateTargetFacts {
    request_id: RequestId,
    capability_id: CapabilityId,
    normalized_requested_record: NormalizedCapabilityCreateRecord,
}

impl CapabilityCreateTargetFacts {
    /// Binds one invocation ID and target ID to a checked requested record.
    #[must_use]
    pub const fn new(
        request_id: RequestId,
        capability_id: CapabilityId,
        normalized_requested_record: NormalizedCapabilityCreateRecord,
    ) -> Self {
        Self {
            request_id,
            capability_id,
            normalized_requested_record,
        }
    }

    /// Returns the tracing/audit identity of this transport invocation.
    #[must_use]
    pub const fn request_id(&self) -> RequestId {
        self.request_id
    }

    /// Returns the target capability component of create replay identity.
    #[must_use]
    pub const fn capability_id(&self) -> CapabilityId {
        self.capability_id
    }

    /// Returns the normalized requested-record component of create replay identity.
    #[must_use]
    pub const fn normalized_requested_record(&self) -> &NormalizedCapabilityCreateRecord {
        &self.normalized_requested_record
    }
}

impl fmt::Debug for CapabilityCreateTargetFacts {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CapabilityCreateTargetFacts([REDACTED])")
    }
}

/// Complete checked target facts bound to one capability revocation.
///
/// These facts are resolved by the trusted application service and later
/// matched exactly against the coordinator's proposed target transition. The
/// target's lifecycle and expected revision remain coordinator transition
/// checks; policy binds them so a different target record cannot be substituted.
#[derive(Clone, Eq, PartialEq)]
pub struct CapabilityRevokeTargetFacts {
    request_id: RequestId,
    capability_id: CapabilityId,
    revision: NonZeroU64,
    activity: CapabilityActivity,
    database_id: DatabaseId,
    environment: Environment,
    principal_id: ActorId,
    actor_kind: ActorKind,
    audiences: Vec<Audience>,
    issued_at: Timestamp,
    expires_at: Timestamp,
    grant: CapabilityGrantV1,
}

impl CapabilityRevokeTargetFacts {
    /// Binds a revoke invocation to complete already-canonical target facts.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        request_id: RequestId,
        capability_id: CapabilityId,
        revision: NonZeroU64,
        activity: CapabilityActivity,
        database_id: DatabaseId,
        environment: Environment,
        principal_id: ActorId,
        actor_kind: ActorKind,
        audiences: Vec<Audience>,
        issued_at: Timestamp,
        expires_at: Timestamp,
        grant: CapabilityGrantV1,
    ) -> Result<Self, CapabilityMutationFactsError> {
        validate_canonical_audiences(&audiences, MAX_CAPABILITY_AUDIENCES)?;
        Ok(Self {
            request_id,
            capability_id,
            revision,
            activity,
            database_id,
            environment,
            principal_id,
            actor_kind,
            audiences,
            issued_at,
            expires_at,
            grant,
        })
    }

    /// Returns the tracing/audit identity of this transport invocation.
    #[must_use]
    pub const fn request_id(&self) -> RequestId {
        self.request_id
    }

    /// Returns the exact target capability identity.
    #[must_use]
    pub const fn capability_id(&self) -> CapabilityId {
        self.capability_id
    }

    /// Returns the exact expected target revision bound to the transition.
    #[must_use]
    pub const fn revision(&self) -> NonZeroU64 {
        self.revision
    }

    /// Returns the bound target lifecycle.
    #[must_use]
    pub const fn activity(&self) -> CapabilityActivity {
        self.activity
    }

    /// Returns the target database.
    #[must_use]
    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    /// Returns the target environment.
    #[must_use]
    pub const fn environment(&self) -> &Environment {
        &self.environment
    }

    /// Returns the target stable principal.
    #[must_use]
    pub const fn principal_id(&self) -> &ActorId {
        &self.principal_id
    }

    /// Returns the target actor kind.
    #[must_use]
    pub const fn actor_kind(&self) -> ActorKind {
        self.actor_kind
    }

    /// Returns the complete canonical target audiences.
    #[must_use]
    pub fn audiences(&self) -> &[Audience] {
        &self.audiences
    }

    /// Returns the bound target issue time.
    #[must_use]
    pub const fn issued_at(&self) -> Timestamp {
        self.issued_at
    }

    /// Returns the bound target exclusive expiry time.
    #[must_use]
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }

    /// Returns the complete checked target grant.
    #[must_use]
    pub const fn grant(&self) -> &CapabilityGrantV1 {
        &self.grant
    }
}

impl fmt::Debug for CapabilityRevokeTargetFacts {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CapabilityRevokeTargetFacts([REDACTED])")
    }
}

/// Checked request and server-scope facts for an absent revoke target.
///
/// Absence cannot supply a revision, principal, lifecycle, audience, grant, or
/// validity interval. This type deliberately has no fields for those facts.
#[derive(Clone, Eq, PartialEq)]
pub struct AbsentCapabilityRevokeTargetFacts {
    request_id: RequestId,
    capability_id: CapabilityId,
    database_id: DatabaseId,
    environment: Environment,
}

impl AbsentCapabilityRevokeTargetFacts {
    /// Binds an absent-target request to the trusted database and environment.
    #[must_use]
    pub const fn new(
        request_id: RequestId,
        capability_id: CapabilityId,
        database_id: DatabaseId,
        environment: Environment,
    ) -> Self {
        Self {
            request_id,
            capability_id,
            database_id,
            environment,
        }
    }

    /// Returns the tracing/audit identity of this transport invocation.
    #[must_use]
    pub const fn request_id(&self) -> RequestId {
        self.request_id
    }

    /// Returns the exact requested capability identity.
    #[must_use]
    pub const fn capability_id(&self) -> CapabilityId {
        self.capability_id
    }

    /// Returns the trusted request database.
    #[must_use]
    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    /// Returns the trusted request environment.
    #[must_use]
    pub const fn environment(&self) -> &Environment {
        &self.environment
    }
}

impl fmt::Debug for AbsentCapabilityRevokeTargetFacts {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AbsentCapabilityRevokeTargetFacts([REDACTED])")
    }
}

/// Transaction-current existence of one exact capability target.
///
/// The coordinator lowers this fact mechanically from its transaction. A
/// present observation carries no target record because an absence-authorized
/// preparation must be abandoned rather than promoted to a present revoke.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct TransactionCurrentCapabilityExistence {
    capability_id: CapabilityId,
    exists: bool,
}

impl TransactionCurrentCapabilityExistence {
    /// Records that the exact target remains absent in the current transaction.
    #[must_use]
    pub const fn absent(capability_id: CapabilityId) -> Self {
        Self {
            capability_id,
            exists: false,
        }
    }

    /// Records that the exact target appeared before the current transaction.
    #[must_use]
    pub const fn present(capability_id: CapabilityId) -> Self {
        Self {
            capability_id,
            exists: true,
        }
    }

    /// Returns the exact target identity observed by the transaction.
    #[must_use]
    pub const fn capability_id(self) -> CapabilityId {
        self.capability_id
    }

    /// Reports whether the exact target is transaction-current present.
    #[must_use]
    pub const fn is_present(self) -> bool {
        self.exists
    }
}

impl fmt::Debug for TransactionCurrentCapabilityExistence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("TransactionCurrentCapabilityExistence([REDACTED])")
    }
}

/// Complete policy-relevant current capability facts supplied by commit.
///
/// This is a trusted commit-coordinator lowering boundary, not an unforgeable
/// authorization token and not a DTO accepted from a service or transport.
/// Construction checks only value shape and canonical bounds. Authorization is
/// deliberately performed by [`TransactionCurrentCapabilityVerifier`].
#[derive(Clone, Eq, PartialEq)]
pub struct TransactionCurrentCapabilityFacts {
    pub(crate) capability_id: CapabilityId,
    pub(crate) revision: NonZeroU64,
    pub(crate) activity: CapabilityActivity,
    pub(crate) database_id: DatabaseId,
    pub(crate) environment: Environment,
    pub(crate) principal_id: ActorId,
    pub(crate) actor_kind: ActorKind,
    pub(crate) audiences: Vec<Audience>,
    pub(crate) issued_at: Timestamp,
    pub(crate) expires_at: Timestamp,
    pub(crate) grant: CapabilityGrantV1,
}

impl TransactionCurrentCapabilityFacts {
    /// Mechanically copies complete bounded current facts without deciding policy.
    ///
    /// Commit must copy already-canonical transaction-current values. Callers
    /// must not use this constructor to repair service- or transport-supplied
    /// values before verification.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        capability_id: CapabilityId,
        revision: NonZeroU64,
        activity: CapabilityActivity,
        database_id: DatabaseId,
        environment: Environment,
        principal_id: ActorId,
        actor_kind: ActorKind,
        audiences: Vec<Audience>,
        issued_at: Timestamp,
        expires_at: Timestamp,
        grant: CapabilityGrantV1,
    ) -> Result<Self, CapabilityMutationFactsError> {
        validate_canonical_audiences(&audiences, MAX_CAPABILITY_AUDIENCES)?;
        Ok(Self {
            capability_id,
            revision,
            activity,
            database_id,
            environment,
            principal_id,
            actor_kind,
            audiences,
            issued_at,
            expires_at,
            grant,
        })
    }

    /// Returns the exact current capability identity.
    #[must_use]
    pub const fn capability_id(&self) -> CapabilityId {
        self.capability_id
    }

    /// Returns the exact current revision.
    #[must_use]
    pub const fn revision(&self) -> NonZeroU64 {
        self.revision
    }

    /// Returns the current lifecycle supplied by the commit coordinator.
    #[must_use]
    pub const fn activity(&self) -> CapabilityActivity {
        self.activity
    }

    /// Returns the exact current database.
    #[must_use]
    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    /// Returns the exact current environment.
    #[must_use]
    pub const fn environment(&self) -> &Environment {
        &self.environment
    }

    /// Returns the stable current principal.
    #[must_use]
    pub const fn principal_id(&self) -> &ActorId {
        &self.principal_id
    }

    /// Returns the trusted current actor kind.
    #[must_use]
    pub const fn actor_kind(&self) -> ActorKind {
        self.actor_kind
    }

    /// Returns the complete canonical current audiences.
    #[must_use]
    pub fn audiences(&self) -> &[Audience] {
        &self.audiences
    }

    /// Returns the inclusive beginning of the current validity interval.
    #[must_use]
    pub const fn issued_at(&self) -> Timestamp {
        self.issued_at
    }

    /// Returns the exclusive end of the current validity interval.
    #[must_use]
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }

    /// Returns the complete checked current grant.
    #[must_use]
    pub const fn grant(&self) -> &CapabilityGrantV1 {
        &self.grant
    }
}

impl fmt::Debug for TransactionCurrentCapabilityFacts {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("TransactionCurrentCapabilityFacts([REDACTED])")
    }
}

/// Exact delegation rule selected during initial authorization.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CapabilityDelegationMode {
    /// The requested authority must remain a complete subset of the caller.
    Subset,
    /// Administration authority permits any checked v1 grant in the same database/environment.
    Administrator,
}

#[derive(Eq, PartialEq)]
enum PreparedCapabilityMutation {
    Create {
        target: CapabilityCreateTargetFacts,
        trusted_audiences: TrustedAudienceCatalog,
    },
    Revoke {
        target: CapabilityRevokeTargetFacts,
        reason: RevocationReasonCodeV1,
    },
    RevokeAbsent {
        target: AbsentCapabilityRevokeTargetFacts,
        reason: RevocationReasonCodeV1,
    },
}

pub(crate) enum CapabilityMutationRequest {
    Create(CapabilityCreateTargetFacts),
    Revoke {
        target: CapabilityRevokeTargetFacts,
        reason: RevocationReasonCodeV1,
    },
    RevokeAbsent {
        target: AbsentCapabilityRevokeTargetFacts,
        reason: RevocationReasonCodeV1,
    },
}

/// Move-only initial authorization for one exact capability mutation.
#[derive(Eq, PartialEq)]
pub struct AuthorizedCapabilityMutationPreparation {
    authorizing_capability_id: CapabilityId,
    authorizing_revision: NonZeroU64,
    authorizing_principal_id: ActorId,
    authorizing_actor_kind: ActorKind,
    authenticated_audience: Audience,
    authenticated_tenant_scope: TenantScope,
    validated_approval: Option<ApprovalId>,
    mode: CapabilityDelegationMode,
    mutation: PreparedCapabilityMutation,
}

impl AuthorizedCapabilityMutationPreparation {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn create(
        authorizing: &TransactionCurrentCapabilityFacts,
        authenticated_principal_id: &ActorId,
        authenticated_actor_kind: ActorKind,
        authenticated_audience: &Audience,
        authenticated_tenant_scope: &TenantScope,
        trusted_audiences: &TrustedAudienceCatalog,
        now: Timestamp,
        target: CapabilityCreateTargetFacts,
    ) -> Result<Self, PolicyCode> {
        if authorizing.principal_id != *authenticated_principal_id
            || authorizing.actor_kind != authenticated_actor_kind
            || authorizing
                .audiences
                .binary_search(authenticated_audience)
                .is_err()
            || authorizing.grant.tenant_scope() != authenticated_tenant_scope
        {
            return Err(PolicyCode::InactiveOrStaleCapability);
        }
        let requested = target.normalized_requested_record();
        if !trusted_audiences.contains_all(requested.audiences()) {
            return Err(PolicyCode::DelegationExceedsAuthority);
        }
        let expected_expiry = checked_expiry(now, requested.requested_lifetime_seconds())
            .ok_or(PolicyCode::DelegationExceedsAuthority)?;
        let mode = select_create_mode(authorizing, &target, expected_expiry)?;
        Ok(Self {
            authorizing_capability_id: authorizing.capability_id,
            authorizing_revision: authorizing.revision,
            authorizing_principal_id: authenticated_principal_id.clone(),
            authorizing_actor_kind: authenticated_actor_kind,
            authenticated_audience: authenticated_audience.clone(),
            authenticated_tenant_scope: authenticated_tenant_scope.clone(),
            validated_approval: None,
            mode,
            mutation: PreparedCapabilityMutation::Create {
                target,
                trusted_audiences: trusted_audiences.clone(),
            },
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn revoke(
        authorizing: &TransactionCurrentCapabilityFacts,
        authenticated_principal_id: &ActorId,
        authenticated_actor_kind: ActorKind,
        authenticated_audience: &Audience,
        authenticated_tenant_scope: &TenantScope,
        target: CapabilityRevokeTargetFacts,
        reason: RevocationReasonCodeV1,
    ) -> Result<Self, PolicyCode> {
        if authorizing.principal_id != *authenticated_principal_id
            || authorizing.actor_kind != authenticated_actor_kind
            || authorizing
                .audiences
                .binary_search(authenticated_audience)
                .is_err()
            || authorizing.grant.tenant_scope() != authenticated_tenant_scope
        {
            return Err(PolicyCode::InactiveOrStaleCapability);
        }
        let mode = select_revoke_mode(authorizing, &target)?;
        Ok(Self {
            authorizing_capability_id: authorizing.capability_id,
            authorizing_revision: authorizing.revision,
            authorizing_principal_id: authenticated_principal_id.clone(),
            authorizing_actor_kind: authenticated_actor_kind,
            authenticated_audience: authenticated_audience.clone(),
            authenticated_tenant_scope: authenticated_tenant_scope.clone(),
            validated_approval: None,
            mode,
            mutation: PreparedCapabilityMutation::Revoke { target, reason },
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn revoke_absent(
        authorizing: &TransactionCurrentCapabilityFacts,
        authenticated_principal_id: &ActorId,
        authenticated_actor_kind: ActorKind,
        authenticated_audience: &Audience,
        authenticated_tenant_scope: &TenantScope,
        target: AbsentCapabilityRevokeTargetFacts,
        reason: RevocationReasonCodeV1,
    ) -> Result<Self, PolicyCode> {
        if authorizing.principal_id != *authenticated_principal_id
            || authorizing.actor_kind != authenticated_actor_kind
            || authorizing
                .audiences
                .binary_search(authenticated_audience)
                .is_err()
            || authorizing.grant.tenant_scope() != authenticated_tenant_scope
        {
            return Err(PolicyCode::InactiveOrStaleCapability);
        }
        verify_permission_path(
            &authorizing.grant,
            CapabilityPermissionKindV1::AdministerCapabilities,
            target.database_id == authorizing.database_id
                && target.environment == authorizing.environment,
        )?;
        Ok(Self {
            authorizing_capability_id: authorizing.capability_id,
            authorizing_revision: authorizing.revision,
            authorizing_principal_id: authenticated_principal_id.clone(),
            authorizing_actor_kind: authenticated_actor_kind,
            authenticated_audience: authenticated_audience.clone(),
            authenticated_tenant_scope: authenticated_tenant_scope.clone(),
            validated_approval: None,
            mode: CapabilityDelegationMode::Administrator,
            mutation: PreparedCapabilityMutation::RevokeAbsent { target, reason },
        })
    }

    /// Returns the exact authorizing capability identity.
    #[must_use]
    pub const fn authorizing_capability_id(&self) -> CapabilityId {
        self.authorizing_capability_id
    }

    /// Returns the exact authorizing capability revision.
    #[must_use]
    pub const fn authorizing_revision(&self) -> NonZeroU64 {
        self.authorizing_revision
    }

    /// Returns the stable authenticated principal bound to this preparation.
    #[must_use]
    pub const fn authorizing_principal_id(&self) -> &ActorId {
        &self.authorizing_principal_id
    }

    /// Returns the trusted authenticated actor kind.
    #[must_use]
    pub const fn authorizing_actor_kind(&self) -> ActorKind {
        self.authorizing_actor_kind
    }

    /// Returns the exact authenticated audience.
    #[must_use]
    pub const fn authenticated_audience(&self) -> &Audience {
        &self.authenticated_audience
    }

    /// Returns the authenticated tenant scope.
    #[must_use]
    pub const fn authenticated_tenant_scope(&self) -> &TenantScope {
        &self.authenticated_tenant_scope
    }

    /// Borrows the exact validated approval retained for this mutation.
    ///
    /// The POC default has no approval provider and therefore always returns
    /// `None`; approval-required permission paths fail closed before a
    /// preparation is constructed.
    #[must_use]
    pub const fn validated_approval(&self) -> Option<&ApprovalId> {
        self.validated_approval.as_ref()
    }

    /// Returns the exact selected delegation mode.
    #[must_use]
    pub const fn delegation_mode(&self) -> CapabilityDelegationMode {
        self.mode
    }

    /// Returns the exact normalized create target, when this prepares creation.
    #[must_use]
    pub const fn create_target(&self) -> Option<&CapabilityCreateTargetFacts> {
        match &self.mutation {
            PreparedCapabilityMutation::Create { target, .. } => Some(target),
            PreparedCapabilityMutation::Revoke { .. }
            | PreparedCapabilityMutation::RevokeAbsent { .. } => None,
        }
    }

    /// Returns the exact checked revoke target, when this prepares revocation.
    #[must_use]
    pub const fn revoke_target(&self) -> Option<&CapabilityRevokeTargetFacts> {
        match &self.mutation {
            PreparedCapabilityMutation::Create { .. } => None,
            PreparedCapabilityMutation::Revoke { target, .. } => Some(target),
            PreparedCapabilityMutation::RevokeAbsent { .. } => None,
        }
    }

    /// Returns the exact checked absent revoke target, when one was authorized.
    #[must_use]
    pub const fn absent_revoke_target(&self) -> Option<&AbsentCapabilityRevokeTargetFacts> {
        match &self.mutation {
            PreparedCapabilityMutation::RevokeAbsent { target, .. } => Some(target),
            PreparedCapabilityMutation::Create { .. }
            | PreparedCapabilityMutation::Revoke { .. } => None,
        }
    }

    /// Returns the exact closed reason, when this prepares revocation.
    #[must_use]
    pub const fn revoke_reason(&self) -> Option<RevocationReasonCodeV1> {
        match &self.mutation {
            PreparedCapabilityMutation::Create { .. } => None,
            PreparedCapabilityMutation::Revoke { reason, .. }
            | PreparedCapabilityMutation::RevokeAbsent { reason, .. } => Some(*reason),
        }
    }
}

impl fmt::Debug for AuthorizedCapabilityMutationPreparation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AuthorizedCapabilityMutationPreparation([REDACTED])")
    }
}

/// Exact coordinator-proposed create transition checked by policy at commit time.
#[derive(Eq, PartialEq)]
pub struct ProposedCapabilityCreate {
    target: CapabilityCreateTargetFacts,
    issued_at: Timestamp,
    expires_at: Timestamp,
}

impl ProposedCapabilityCreate {
    /// Constructs the value-only proposed create transition.
    #[must_use]
    pub const fn new(
        target: CapabilityCreateTargetFacts,
        issued_at: Timestamp,
        expires_at: Timestamp,
    ) -> Self {
        Self {
            target,
            issued_at,
            expires_at,
        }
    }

    /// Returns the exact normalized target.
    #[must_use]
    pub const fn target(&self) -> &CapabilityCreateTargetFacts {
        &self.target
    }

    /// Returns the proposed inclusive issue time.
    #[must_use]
    pub const fn issued_at(&self) -> Timestamp {
        self.issued_at
    }

    /// Returns the proposed exclusive expiry time.
    #[must_use]
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }
}

impl fmt::Debug for ProposedCapabilityCreate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProposedCapabilityCreate([REDACTED])")
    }
}

/// Exact coordinator-proposed revoke transition checked by policy at commit time.
#[derive(Eq, PartialEq)]
pub struct ProposedCapabilityRevoke {
    target: CapabilityRevokeTargetFacts,
    reason: RevocationReasonCodeV1,
    revoked_at: Timestamp,
}

impl ProposedCapabilityRevoke {
    /// Constructs the value-only proposed revoke transition.
    #[must_use]
    pub const fn new(
        target: CapabilityRevokeTargetFacts,
        reason: RevocationReasonCodeV1,
        revoked_at: Timestamp,
    ) -> Self {
        Self {
            target,
            reason,
            revoked_at,
        }
    }

    /// Returns the exact checked target facts.
    #[must_use]
    pub const fn target(&self) -> &CapabilityRevokeTargetFacts {
        &self.target
    }

    /// Returns the exact closed revocation reason.
    #[must_use]
    pub const fn reason(&self) -> RevocationReasonCodeV1 {
        self.reason
    }

    /// Returns the proposed authoritative revocation time.
    #[must_use]
    pub const fn revoked_at(&self) -> Timestamp {
        self.revoked_at
    }
}

impl fmt::Debug for ProposedCapabilityRevoke {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProposedCapabilityRevoke([REDACTED])")
    }
}

/// Exact capability transition proposed to the transaction-current verifier.
#[derive(Eq, PartialEq)]
pub enum ProposedCapabilityMutation {
    /// Create one exact capability and validity interval.
    Create(ProposedCapabilityCreate),
    /// Revoke one exact checked target for one closed reason.
    Revoke(ProposedCapabilityRevoke),
}

impl fmt::Debug for ProposedCapabilityMutation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProposedCapabilityMutation([REDACTED])")
    }
}

/// Move-only proof that transaction-current policy allowed one exact transition.
///
/// This proof is privately constructed for consumption inside the trusted
/// commit coordinator. It is not a credential or a service/transport token and
/// must not be accepted back across a public adapter boundary.
#[derive(Eq, PartialEq)]
pub struct AuthorizedTransactionCapabilityMutation {
    authoritative_time: Timestamp,
    preparation: AuthorizedCapabilityMutationPreparation,
    proposed: ProposedCapabilityMutation,
}

impl AuthorizedTransactionCapabilityMutation {
    /// Returns the single authoritative clock sample used for this decision.
    #[must_use]
    pub const fn authoritative_time(&self) -> Timestamp {
        self.authoritative_time
    }

    /// Returns the exact initially authorized preparation.
    #[must_use]
    pub const fn preparation(&self) -> &AuthorizedCapabilityMutationPreparation {
        &self.preparation
    }

    /// Returns the exact coordinator transition authorized at commit time.
    #[must_use]
    pub const fn proposed(&self) -> &ProposedCapabilityMutation {
        &self.proposed
    }

    /// Consumes the proof into its exact value-only parts.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        Timestamp,
        AuthorizedCapabilityMutationPreparation,
        ProposedCapabilityMutation,
    ) {
        (self.authoritative_time, self.preparation, self.proposed)
    }
}

impl fmt::Debug for AuthorizedTransactionCapabilityMutation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AuthorizedTransactionCapabilityMutation([REDACTED])")
    }
}

/// Move-only proof that an administrator reauthorized one still-absent target.
///
/// This is a proof for returning a typed no-transition result. It is not an
/// authorization to create a durable capability transition or assign its
/// sequence.
#[derive(Eq, PartialEq)]
pub struct AuthorizedTransactionAbsentCapabilityRevoke {
    authoritative_time: Timestamp,
    preparation: AuthorizedCapabilityMutationPreparation,
}

impl AuthorizedTransactionAbsentCapabilityRevoke {
    /// Returns the single authoritative clock sample used for this decision.
    #[must_use]
    pub const fn authoritative_time(&self) -> Timestamp {
        self.authoritative_time
    }

    /// Returns the exact absence-bound initial authorization.
    #[must_use]
    pub const fn preparation(&self) -> &AuthorizedCapabilityMutationPreparation {
        &self.preparation
    }

    /// Consumes the proof into its exact value-only parts.
    #[must_use]
    pub fn into_parts(self) -> (Timestamp, AuthorizedCapabilityMutationPreparation) {
        (self.authoritative_time, self.preparation)
    }
}

impl fmt::Debug for AuthorizedTransactionAbsentCapabilityRevoke {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AuthorizedTransactionAbsentCapabilityRevoke([REDACTED])")
    }
}

/// Closed policy reason an absent-revoke preparation must be rebuilt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AbsentCapabilityRevokePreparationChange {
    /// A target proven absent during initial policy evaluation is now present.
    TargetAppeared,
}

/// Deny-by-default result for a transaction-current absent-target recheck.
#[derive(Eq, PartialEq)]
pub enum TransactionAbsentCapabilityRevokeDecision {
    /// Current policy allowed returning the exact no-transition result.
    Allow(Box<AuthorizedTransactionAbsentCapabilityRevoke>),
    /// Target existence changed and complete present-target facts are required.
    PreparationChanged(AbsentCapabilityRevokePreparationChange),
    /// Current authorizing policy denied the operation with a closed code.
    Deny(PolicyCode),
}

impl fmt::Debug for TransactionAbsentCapabilityRevokeDecision {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Allow(_) => {
                formatter.write_str("TransactionAbsentCapabilityRevokeDecision::Allow([REDACTED])")
            }
            Self::PreparationChanged(change) => formatter
                .debug_tuple("TransactionAbsentCapabilityRevokeDecision::PreparationChanged")
                .field(change)
                .finish(),
            Self::Deny(code) => formatter
                .debug_tuple("TransactionAbsentCapabilityRevokeDecision::Deny")
                .field(code)
                .finish(),
        }
    }
}

/// Closed reason an initially authorized present-target revoke must be rebuilt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CapabilityRevokePreparationChange {
    /// The complete transaction-current target no longer matches initial authorization.
    TargetChanged,
}

/// Deny-by-default result of the transaction-current policy check.
#[derive(Eq, PartialEq)]
pub enum TransactionCapabilityMutationDecision {
    /// Current policy allowed the exact proposed transition.
    Allow(Box<AuthorizedTransactionCapabilityMutation>),
    /// Initial target facts changed and require fresh service authorization.
    PreparationChanged(CapabilityRevokePreparationChange),
    /// Current policy denied the transition with a closed internal code.
    Deny(PolicyCode),
}

impl fmt::Debug for TransactionCapabilityMutationDecision {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Allow(_) => {
                formatter.write_str("TransactionCapabilityMutationDecision::Allow([REDACTED])")
            }
            Self::PreparationChanged(change) => formatter
                .debug_tuple("TransactionCapabilityMutationDecision::PreparationChanged")
                .field(change)
                .finish(),
            Self::Deny(code) => formatter
                .debug_tuple("TransactionCapabilityMutationDecision::Deny")
                .field(code)
                .finish(),
        }
    }
}

/// Pure transaction-current capability-mutation verifier.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TransactionCurrentCapabilityVerifier;

impl TransactionCurrentCapabilityVerifier {
    /// Rechecks one create preparation using one fresh authoritative clock sample.
    #[must_use]
    pub fn verify_create(
        current: &TransactionCurrentCapabilityFacts,
        authorization_time: Timestamp,
        preparation: AuthorizedCapabilityMutationPreparation,
        proposed: ProposedCapabilityCreate,
    ) -> TransactionCapabilityMutationDecision {
        let result =
            verify_current_binding(current, authorization_time, &preparation).and_then(|()| {
                match &preparation.mutation {
                    PreparedCapabilityMutation::Create {
                        target,
                        trusted_audiences,
                    } => verify_create_proposal(
                        current,
                        authorization_time,
                        target,
                        trusted_audiences,
                        preparation.mode,
                        &proposed,
                    ),
                    PreparedCapabilityMutation::Revoke { .. }
                    | PreparedCapabilityMutation::RevokeAbsent { .. } => {
                        Err(PolicyCode::DelegationExceedsAuthority)
                    }
                }
            });
        match result {
            Ok(()) => TransactionCapabilityMutationDecision::Allow(Box::new(
                AuthorizedTransactionCapabilityMutation {
                    authoritative_time: authorization_time,
                    preparation,
                    proposed: ProposedCapabilityMutation::Create(proposed),
                },
            )),
            Err(code) => TransactionCapabilityMutationDecision::Deny(code),
        }
    }

    /// Rechecks one present-target revoke at the transaction's final safe point.
    ///
    /// Authorizer changes deny before target drift is classified. A changed target
    /// never inherits the initial authorization; callers must reload complete
    /// facts and perform initial policy evaluation again.
    #[must_use]
    pub fn verify_revoke(
        current_authorizer: &TransactionCurrentCapabilityFacts,
        current_target: &TransactionCurrentCapabilityFacts,
        authorization_time: Timestamp,
        preparation: AuthorizedCapabilityMutationPreparation,
        proposed: ProposedCapabilityRevoke,
    ) -> TransactionCapabilityMutationDecision {
        if let Err(code) =
            verify_current_binding(current_authorizer, authorization_time, &preparation)
        {
            return TransactionCapabilityMutationDecision::Deny(code);
        }
        let PreparedCapabilityMutation::Revoke { target, reason } = &preparation.mutation else {
            return TransactionCapabilityMutationDecision::Deny(
                PolicyCode::DelegationExceedsAuthority,
            );
        };
        if !current_target_matches_revoke_target(current_target, target) {
            return TransactionCapabilityMutationDecision::PreparationChanged(
                CapabilityRevokePreparationChange::TargetChanged,
            );
        }
        if let Err(code) = verify_revoke_proposal(
            current_authorizer,
            authorization_time,
            target,
            *reason,
            preparation.mode,
            &proposed,
        ) {
            return TransactionCapabilityMutationDecision::Deny(code);
        }
        TransactionCapabilityMutationDecision::Allow(Box::new(
            AuthorizedTransactionCapabilityMutation {
                authoritative_time: authorization_time,
                preparation,
                proposed: ProposedCapabilityMutation::Revoke(proposed),
            },
        ))
    }

    /// Rechecks an absence-authorized revoke against exact transaction state.
    ///
    /// Authorizer changes deny before target appearance is classified. A
    /// present target never inherits the absence authorization; callers must
    /// rebuild complete present-target facts and run initial policy again.
    #[must_use]
    pub fn verify_absent_revoke(
        current: &TransactionCurrentCapabilityFacts,
        target_existence: TransactionCurrentCapabilityExistence,
        authorization_time: Timestamp,
        preparation: AuthorizedCapabilityMutationPreparation,
    ) -> TransactionAbsentCapabilityRevokeDecision {
        let current_result = verify_current_binding(current, authorization_time, &preparation)
            .and_then(|()| match &preparation.mutation {
                PreparedCapabilityMutation::RevokeAbsent { target, .. } => {
                    verify_permission_path(
                        &current.grant,
                        CapabilityPermissionKindV1::AdministerCapabilities,
                        target.database_id == current.database_id
                            && target.environment == current.environment,
                    )?;
                    if target.capability_id != target_existence.capability_id {
                        return Err(PolicyCode::DelegationExceedsAuthority);
                    }
                    Ok(())
                }
                PreparedCapabilityMutation::Create { .. }
                | PreparedCapabilityMutation::Revoke { .. } => {
                    Err(PolicyCode::DelegationExceedsAuthority)
                }
            });
        if let Err(code) = current_result {
            return TransactionAbsentCapabilityRevokeDecision::Deny(code);
        }
        if target_existence.is_present() {
            return TransactionAbsentCapabilityRevokeDecision::PreparationChanged(
                AbsentCapabilityRevokePreparationChange::TargetAppeared,
            );
        }
        TransactionAbsentCapabilityRevokeDecision::Allow(Box::new(
            AuthorizedTransactionAbsentCapabilityRevoke {
                authoritative_time: authorization_time,
                preparation,
            },
        ))
    }
}

fn verify_current_binding(
    current: &TransactionCurrentCapabilityFacts,
    now: Timestamp,
    preparation: &AuthorizedCapabilityMutationPreparation,
) -> Result<(), PolicyCode> {
    let valid = current.activity == CapabilityActivity::Active
        && current.capability_id == preparation.authorizing_capability_id
        && current.revision == preparation.authorizing_revision
        && current.principal_id == preparation.authorizing_principal_id
        && current.actor_kind == preparation.authorizing_actor_kind
        && current
            .audiences
            .binary_search(&preparation.authenticated_audience)
            .is_ok()
        && current.grant.tenant_scope() == &preparation.authenticated_tenant_scope
        && current.issued_at <= now
        && now < current.expires_at;
    if valid {
        Ok(())
    } else {
        Err(PolicyCode::InactiveOrStaleCapability)
    }
}

fn verify_create_proposal(
    current: &TransactionCurrentCapabilityFacts,
    now: Timestamp,
    target: &CapabilityCreateTargetFacts,
    trusted_audiences: &TrustedAudienceCatalog,
    mode: CapabilityDelegationMode,
    proposed: &ProposedCapabilityCreate,
) -> Result<(), PolicyCode> {
    let requested = target.normalized_requested_record();
    let expected_expires = checked_expiry(now, requested.requested_lifetime_seconds())
        .ok_or(PolicyCode::DelegationExceedsAuthority)?;
    if proposed.target != *target
        || proposed.issued_at != now
        || proposed.expires_at != expected_expires
        || !trusted_audiences.contains_all(requested.audiences())
    {
        return Err(PolicyCode::DelegationExceedsAuthority);
    }
    verify_selected_create_mode(current, target, expected_expires, mode)
}

fn verify_revoke_proposal(
    current: &TransactionCurrentCapabilityFacts,
    now: Timestamp,
    target: &CapabilityRevokeTargetFacts,
    reason: RevocationReasonCodeV1,
    mode: CapabilityDelegationMode,
    proposed: &ProposedCapabilityRevoke,
) -> Result<(), PolicyCode> {
    if proposed.target != *target || proposed.reason != reason || proposed.revoked_at != now {
        return Err(PolicyCode::DelegationExceedsAuthority);
    }
    verify_selected_revoke_mode(current, target, mode)
}

fn current_target_matches_revoke_target(
    current: &TransactionCurrentCapabilityFacts,
    target: &CapabilityRevokeTargetFacts,
) -> bool {
    current.capability_id == target.capability_id
        && current.revision == target.revision
        && current.activity == target.activity
        && current.database_id == target.database_id
        && current.environment == target.environment
        && current.principal_id == target.principal_id
        && current.actor_kind == target.actor_kind
        && current.audiences == target.audiences
        && current.issued_at == target.issued_at
        && current.expires_at == target.expires_at
        && current.grant == target.grant
}

fn select_create_mode(
    current: &TransactionCurrentCapabilityFacts,
    target: &CapabilityCreateTargetFacts,
    expected_expires: Timestamp,
) -> Result<CapabilityDelegationMode, PolicyCode> {
    select_mode(
        &current.grant,
        CapabilityPermissionKindV1::CreateCapability,
        || create_subset(current, target, expected_expires),
        || {
            let requested = target.normalized_requested_record();
            requested.database_id() == current.database_id
                && requested.environment() == &current.environment
        },
    )
}

fn verify_selected_create_mode(
    current: &TransactionCurrentCapabilityFacts,
    target: &CapabilityCreateTargetFacts,
    expected_expires: Timestamp,
    mode: CapabilityDelegationMode,
) -> Result<(), PolicyCode> {
    let (permission, authority) = match mode {
        CapabilityDelegationMode::Subset => (
            CapabilityPermissionKindV1::CreateCapability,
            create_subset(current, target, expected_expires),
        ),
        CapabilityDelegationMode::Administrator => (
            CapabilityPermissionKindV1::AdministerCapabilities,
            target.normalized_requested_record().database_id() == current.database_id
                && target.normalized_requested_record().environment() == &current.environment,
        ),
    };
    verify_permission_path(&current.grant, permission, authority)
}

fn select_revoke_mode(
    current: &TransactionCurrentCapabilityFacts,
    target: &CapabilityRevokeTargetFacts,
) -> Result<CapabilityDelegationMode, PolicyCode> {
    select_mode(
        &current.grant,
        CapabilityPermissionKindV1::RevokeCapability,
        || revoke_subset(current, target),
        || target.database_id == current.database_id && target.environment == current.environment,
    )
}

fn verify_selected_revoke_mode(
    current: &TransactionCurrentCapabilityFacts,
    target: &CapabilityRevokeTargetFacts,
    mode: CapabilityDelegationMode,
) -> Result<(), PolicyCode> {
    let (permission, authority) = match mode {
        CapabilityDelegationMode::Subset => (
            CapabilityPermissionKindV1::RevokeCapability,
            revoke_subset(current, target),
        ),
        CapabilityDelegationMode::Administrator => (
            CapabilityPermissionKindV1::AdministerCapabilities,
            target.database_id == current.database_id && target.environment == current.environment,
        ),
    };
    verify_permission_path(&current.grant, permission, authority)
}

fn select_mode<OrdinaryAuthority, AdministratorAuthority>(
    grant: &CapabilityGrantV1,
    ordinary_permission: CapabilityPermissionKindV1,
    ordinary_authority: OrdinaryAuthority,
    admin_authority: AdministratorAuthority,
) -> Result<CapabilityDelegationMode, PolicyCode>
where
    OrdinaryAuthority: FnOnce() -> bool,
    AdministratorAuthority: FnOnce() -> bool,
{
    let ordinary = inspect_permission_path_lazy(grant, ordinary_permission, ordinary_authority);
    if ordinary == PermissionPath::Allowed {
        return Ok(CapabilityDelegationMode::Subset);
    }
    let administrator = inspect_permission_path_lazy(
        grant,
        CapabilityPermissionKindV1::AdministerCapabilities,
        admin_authority,
    );
    if administrator == PermissionPath::Allowed {
        return Ok(CapabilityDelegationMode::Administrator);
    }
    if ordinary == PermissionPath::ApprovalRequired
        || administrator == PermissionPath::ApprovalRequired
    {
        Err(PolicyCode::ApprovalRequired)
    } else if ordinary == PermissionPath::OutsideAuthority
        || administrator == PermissionPath::OutsideAuthority
    {
        Err(PolicyCode::DelegationExceedsAuthority)
    } else {
        Err(PolicyCode::MissingPermission)
    }
}

fn verify_permission_path(
    grant: &CapabilityGrantV1,
    permission: CapabilityPermissionKindV1,
    authority: bool,
) -> Result<(), PolicyCode> {
    match inspect_permission_path(grant, permission, authority) {
        PermissionPath::Allowed => Ok(()),
        PermissionPath::Missing => Err(PolicyCode::MissingPermission),
        PermissionPath::ApprovalRequired => Err(PolicyCode::ApprovalRequired),
        PermissionPath::OutsideAuthority => Err(PolicyCode::DelegationExceedsAuthority),
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum PermissionPath {
    Missing,
    ApprovalRequired,
    OutsideAuthority,
    Allowed,
}

fn inspect_permission_path(
    grant: &CapabilityGrantV1,
    permission: CapabilityPermissionKindV1,
    authority: bool,
) -> PermissionPath {
    inspect_permission_path_lazy(grant, permission, || authority)
}

fn inspect_permission_path_lazy(
    grant: &CapabilityGrantV1,
    permission: CapabilityPermissionKindV1,
    authority: impl FnOnce() -> bool,
) -> PermissionPath {
    if !grant.permissions().contains_kind(permission) {
        return PermissionPath::Missing;
    }
    if !authority() {
        return PermissionPath::OutsideAuthority;
    }
    if grant.approval_required().binary_search(&permission).is_ok() {
        PermissionPath::ApprovalRequired
    } else {
        PermissionPath::Allowed
    }
}

fn create_subset(
    current: &TransactionCurrentCapabilityFacts,
    target: &CapabilityCreateTargetFacts,
    expected_expires: Timestamp,
) -> bool {
    let requested = target.normalized_requested_record();
    requested.database_id() == current.database_id
        && requested.environment() == &current.environment
        && audiences_subset(requested.audiences(), &current.audiences)
        && grant_subset(requested.grant(), &current.grant)
        && expected_expires <= current.expires_at
}

fn revoke_subset(
    current: &TransactionCurrentCapabilityFacts,
    target: &CapabilityRevokeTargetFacts,
) -> bool {
    target.database_id == current.database_id
        && target.environment == current.environment
        && audiences_subset(&target.audiences, &current.audiences)
        && grant_subset(&target.grant, &current.grant)
        && target.expires_at <= current.expires_at
}

pub(crate) fn grant_subset(child: &CapabilityGrantV1, parent: &CapabilityGrantV1) -> bool {
    tenant_subset(child.tenant_scope(), parent.tenant_scope())
        && partition_subset(child.partition_scope(), parent.partition_scope())
        && permissions_subset(child, parent)
        && field_visibility_subset(child, parent)
        && row_policy_subset(child, parent)
        && export_subset(child, parent)
        && child.max_scan_rows() <= parent.max_scan_rows()
        && inherited_approvals_preserved(child, parent)
}

fn row_policy_subset(child: &CapabilityGrantV1, parent: &CapabilityGrantV1) -> bool {
    match (child.internal_row_policy(), parent.internal_row_policy()) {
        (None, _) => true,
        (Some(_), None) => export_scope_narrows_to_principal(child, parent),
        (Some(child), Some(parent)) => child.is_narrowing_of(parent),
    }
}

fn export_scope_narrows_to_principal(
    child: &CapabilityGrantV1,
    parent: &CapabilityGrantV1,
) -> bool {
    let (Some(child_export), Some(parent_export)) =
        (child.internal_export(), parent.internal_export())
    else {
        return false;
    };
    child_export.applications().iter().all(|child_application| {
        child_application.scope() == CapabilityApplicationExportScopeV1::PrincipalFiltered
            && parent_export
                .applications()
                .binary_search_by(|candidate| {
                    candidate
                        .lineage()
                        .as_bytes()
                        .cmp(child_application.lineage().as_bytes())
                })
                .ok()
                .is_some_and(|index| {
                    parent_export.applications()[index].scope()
                        == CapabilityApplicationExportScopeV1::WholeApplication
                })
    })
}

fn export_subset(child: &CapabilityGrantV1, parent: &CapabilityGrantV1) -> bool {
    match (child.internal_export(), parent.internal_export()) {
        (None, _) => true,
        (Some(_), None) => false,
        (Some(child_export), Some(parent_export)) => {
            child_export.is_narrowing_of(parent_export, child.internal_row_policy().is_some())
        }
    }
}

fn tenant_subset(child: &TenantScope, parent: &TenantScope) -> bool {
    match (child, parent) {
        (_, TenantScope::Global) => true,
        (TenantScope::Tenant(child), TenantScope::Tenant(parent)) => child == parent,
        (TenantScope::Global, TenantScope::Tenant(_)) => false,
    }
}

fn partition_subset(child: &PartitionScopeV1, parent: &PartitionScopeV1) -> bool {
    match (child, parent) {
        (_, PartitionScopeV1::All) => true,
        (PartitionScopeV1::All, PartitionScopeV1::Explicit(_)) => false,
        (PartitionScopeV1::Explicit(child), PartitionScopeV1::Explicit(parent)) => {
            canonical_key_subset(
                child,
                parent,
                riffdb_types::ScopedPartitionV1::canonical_key,
            )
        }
    }
}

fn permissions_subset(child: &CapabilityGrantV1, parent: &CapabilityGrantV1) -> bool {
    canonical_key_subset(
        child.permissions().as_slice(),
        parent.permissions().as_slice(),
        CapabilityPermissionV1::canonical_key,
    )
}

fn canonical_key_subset<T>(child: &[T], parent: &[T], key: impl Fn(&T) -> Vec<u8>) -> bool {
    let child_keys: Vec<_> = child.iter().map(&key).collect();
    let parent_keys: Vec<_> = parent.iter().map(key).collect();
    let mut parent_index = 0;
    for child_key in child_keys {
        while parent_index < parent_keys.len() && parent_keys[parent_index] < child_key {
            parent_index += 1;
        }
        if parent_keys.get(parent_index) != Some(&child_key) {
            return false;
        }
        parent_index += 1;
    }
    true
}

fn field_visibility_subset(child: &CapabilityGrantV1, parent: &CapabilityGrantV1) -> bool {
    child.field_visibility().iter().all(|child_entry| {
        parent
            .field_visibility()
            .binary_search_by(|parent_entry| {
                parent_entry
                    .lineage()
                    .as_bytes()
                    .len()
                    .cmp(&child_entry.lineage().as_bytes().len())
                    .then_with(|| {
                        parent_entry
                            .lineage()
                            .as_bytes()
                            .cmp(child_entry.lineage().as_bytes())
                    })
                    .then_with(|| parent_entry.entity_type().cmp(&child_entry.entity_type()))
            })
            .ok()
            .map(|index| &parent.field_visibility()[index])
            .is_some_and(|parent_entry| {
                child_entry
                    .fields()
                    .iter()
                    .all(|field| parent_entry.fields().binary_search(field).is_ok())
                    // Secret reveal authority attenuates like any other
                    // visibility: a child may only name secrets its parent
                    // explicitly names (ADR-0118).
                    && child_entry.secret_fields().iter().all(|field| {
                        parent_entry.secret_fields().binary_search(field).is_ok()
                    })
            })
    })
}

fn inherited_approvals_preserved(child: &CapabilityGrantV1, parent: &CapabilityGrantV1) -> bool {
    parent.approval_required().iter().all(|required| {
        !child.permissions().contains_kind(*required)
            || child.approval_required().binary_search(required).is_ok()
    })
}

fn audiences_subset(child: &[Audience], parent: &[Audience]) -> bool {
    child
        .iter()
        .all(|audience| parent.binary_search(audience).is_ok())
}

fn canonicalize_audiences(
    audiences: &mut [Audience],
    maximum: usize,
) -> Result<(), CapabilityMutationFactsError> {
    if audiences.is_empty() {
        return Err(CapabilityMutationFactsError::EmptyAudiences);
    }
    if audiences.len() > maximum {
        return Err(CapabilityMutationFactsError::TooManyAudiences);
    }
    audiences.sort_unstable();
    if audiences.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(CapabilityMutationFactsError::DuplicateAudience);
    }
    Ok(())
}

fn validate_canonical_audiences(
    audiences: &[Audience],
    maximum: usize,
) -> Result<(), CapabilityMutationFactsError> {
    if audiences.is_empty() {
        return Err(CapabilityMutationFactsError::EmptyAudiences);
    }
    if audiences.len() > maximum {
        return Err(CapabilityMutationFactsError::TooManyAudiences);
    }
    if audiences.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(CapabilityMutationFactsError::DuplicateAudience);
    }
    if audiences.windows(2).any(|pair| pair[0] > pair[1]) {
        return Err(CapabilityMutationFactsError::NonCanonicalAudiences);
    }
    Ok(())
}

fn checked_expiry(now: Timestamp, duration: NonZeroU32) -> Option<Timestamp> {
    let seconds = now.seconds().checked_add(i64::from(duration.get()))?;
    Timestamp::new(seconds, now.nanoseconds()).ok()
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::num::NonZeroU16;

    use riffdb_types::{
        AggregateTypeId, ApplicationRoleHash, CapabilityApplicationExportGrantV1,
        CapabilityApplicationExportScopeV1, CapabilityExportGrantV1, CapabilityPermissionsV1,
        CapabilityPrincipalFactsV1, CapabilityRowPolicyBindingV1, CapabilityRowPolicyGrantV1,
        CapabilityRowPolicyOperationV1, CommandId, ContractLineage, EntityFieldVisibilityV1,
        EntityTypeId, FieldId, PartitionKeyBuilder, PartitionScopeV1, RowPolicyName,
        ScopedPartitionV1, TenantId,
    };

    use super::*;

    fn timestamp(seconds: i64) -> Timestamp {
        Timestamp::new(seconds, 17).expect("valid timestamp")
    }

    fn database_id() -> DatabaseId {
        DatabaseId::from_unix_milliseconds_and_random(1, [0x11; 10]).expect("valid UUIDv7")
    }

    fn authorizing_capability_id() -> CapabilityId {
        CapabilityId::from_unix_milliseconds_and_random(2, [0x22; 10]).expect("valid UUIDv7")
    }

    fn target_capability_id(seed: u8) -> CapabilityId {
        CapabilityId::from_unix_milliseconds_and_random(3, [seed; 10]).expect("valid UUIDv7")
    }

    fn request_id(seed: u8) -> RequestId {
        RequestId::from_unix_milliseconds_and_random(4, [seed; 10]).expect("valid UUIDv7")
    }

    fn environment() -> Environment {
        Environment::new("test").expect("valid environment")
    }

    fn principal_id() -> ActorId {
        ActorId::new("authorizing-principal").expect("valid actor")
    }

    fn target_principal_id() -> ActorId {
        ActorId::new("target-principal").expect("valid actor")
    }

    fn audience(value: &str) -> Audience {
        Audience::new(value).expect("valid audience")
    }

    fn permission(kind: CapabilityPermissionKindV1) -> CapabilityPermissionV1 {
        CapabilityPermissionV1::unparameterized(kind).expect("unparameterized permission")
    }

    fn grant(
        tenant_scope: TenantScope,
        permissions: Vec<CapabilityPermissionV1>,
        max_rows: u16,
        approval_required: Vec<CapabilityPermissionKindV1>,
    ) -> CapabilityGrantV1 {
        CapabilityGrantV1::new(
            tenant_scope,
            PartitionScopeV1::All,
            CapabilityPermissionsV1::new(permissions).expect("valid permissions"),
            Vec::new(),
            NonZeroU16::new(max_rows).expect("nonzero rows"),
            approval_required,
        )
        .expect("valid grant")
    }

    fn export_grant(
        role: ApplicationRoleHash,
        scope: CapabilityApplicationExportScopeV1,
        entities: bool,
        events: bool,
        protected: bool,
    ) -> CapabilityGrantV1 {
        let lineage = ContractLineage::new("ticketdesk").expect("lineage");
        let base = grant(
            TenantScope::Global,
            vec![CapabilityPermissionV1::ApplicationRoleIdentity(role)],
            1,
            Vec::new(),
        );
        let base = if protected {
            base.with_row_policy(
                CapabilityRowPolicyGrantV1::new(
                    role,
                    CapabilityPrincipalFactsV1::empty(),
                    vec![
                        CapabilityRowPolicyBindingV1::new(
                            lineage.clone(),
                            RowPolicyName::new("TicketVisible").expect("policy"),
                            EntityTypeId::first(),
                            vec![CapabilityRowPolicyOperationV1::Read],
                        )
                        .expect("binding"),
                    ],
                )
                .expect("row policy"),
            )
            .expect("protected grant")
        } else {
            base
        };
        base.with_export(
            CapabilityExportGrantV1::new(vec![
                CapabilityApplicationExportGrantV1::new(
                    lineage, scope, entities, events, true, true,
                )
                .expect("application export"),
            ])
            .expect("export grant"),
        )
        .expect("grant with export")
    }

    #[test]
    fn export_delegation_is_narrowing_and_policy_bound() {
        let role = ApplicationRoleHash::from_bytes([0x81; 32]);
        let parent = export_grant(
            role,
            CapabilityApplicationExportScopeV1::WholeApplication,
            true,
            true,
            false,
        );
        let principal_child = export_grant(
            role,
            CapabilityApplicationExportScopeV1::PrincipalFiltered,
            true,
            false,
            true,
        );
        assert!(grant_subset(&principal_child, &parent));
        assert!(!grant_subset(&parent, &principal_child));

        let entities_only_parent = export_grant(
            role,
            CapabilityApplicationExportScopeV1::WholeApplication,
            true,
            false,
            false,
        );
        let event_child = export_grant(
            role,
            CapabilityApplicationExportScopeV1::PrincipalFiltered,
            false,
            true,
            true,
        );
        assert!(!grant_subset(&event_child, &entities_only_parent));
    }

    fn current(
        grant: CapabilityGrantV1,
        audiences: Vec<Audience>,
        expires_at: Timestamp,
    ) -> TransactionCurrentCapabilityFacts {
        TransactionCurrentCapabilityFacts::new(
            authorizing_capability_id(),
            NonZeroU64::new(7).expect("nonzero revision"),
            CapabilityActivity::Active,
            database_id(),
            environment(),
            principal_id(),
            ActorKind::Service,
            audiences,
            timestamp(100),
            expires_at,
            grant,
        )
        .expect("valid current facts")
    }

    fn create_target(
        seed: u8,
        grant: CapabilityGrantV1,
        audiences: Vec<Audience>,
        lifetime_seconds: u32,
    ) -> CapabilityCreateTargetFacts {
        let requested_record = NormalizedCapabilityCreateRecord::new(
            database_id(),
            environment(),
            target_principal_id(),
            ActorKind::Agent,
            NonZeroU32::new(lifetime_seconds).expect("nonzero duration"),
            audiences,
            grant,
        )
        .expect("valid requested record");
        CapabilityCreateTargetFacts::new(
            request_id(seed),
            target_capability_id(seed),
            requested_record,
        )
    }

    fn revoke_target(
        seed: u8,
        grant: CapabilityGrantV1,
        audiences: Vec<Audience>,
        expires_at: Timestamp,
    ) -> CapabilityRevokeTargetFacts {
        CapabilityRevokeTargetFacts::new(
            request_id(seed),
            target_capability_id(seed),
            NonZeroU64::new(3).expect("nonzero revision"),
            CapabilityActivity::Active,
            database_id(),
            environment(),
            target_principal_id(),
            ActorKind::Agent,
            audiences,
            timestamp(100),
            expires_at,
            grant,
        )
        .expect("valid revoke target")
    }

    fn current_revoke_target(
        target: &CapabilityRevokeTargetFacts,
    ) -> TransactionCurrentCapabilityFacts {
        TransactionCurrentCapabilityFacts::new(
            target.capability_id,
            target.revision,
            target.activity,
            target.database_id,
            target.environment.clone(),
            target.principal_id.clone(),
            target.actor_kind,
            target.audiences.clone(),
            target.issued_at,
            target.expires_at,
            target.grant.clone(),
        )
        .expect("valid transaction-current revoke target")
    }

    fn absent_revoke_target(seed: u8) -> AbsentCapabilityRevokeTargetFacts {
        AbsentCapabilityRevokeTargetFacts::new(
            request_id(seed),
            target_capability_id(seed),
            database_id(),
            environment(),
        )
    }

    fn catalog() -> TrustedAudienceCatalog {
        TrustedAudienceCatalog::new(vec![audience("grpc"), audience("mcp")]).expect("valid catalog")
    }

    fn prepare(
        current: &TransactionCurrentCapabilityFacts,
        target: CapabilityCreateTargetFacts,
        now: Timestamp,
    ) -> Result<AuthorizedCapabilityMutationPreparation, PolicyCode> {
        AuthorizedCapabilityMutationPreparation::create(
            current,
            &principal_id(),
            ActorKind::Service,
            &audience("grpc"),
            current.grant().tenant_scope(),
            &catalog(),
            now,
            target,
        )
    }

    fn prepare_revoke(
        current: &TransactionCurrentCapabilityFacts,
        target: CapabilityRevokeTargetFacts,
        reason: RevocationReasonCodeV1,
    ) -> Result<AuthorizedCapabilityMutationPreparation, PolicyCode> {
        AuthorizedCapabilityMutationPreparation::revoke(
            current,
            &principal_id(),
            ActorKind::Service,
            &audience("grpc"),
            current.grant().tenant_scope(),
            target,
            reason,
        )
    }

    fn prepare_absent_revoke(
        current: &TransactionCurrentCapabilityFacts,
        target: AbsentCapabilityRevokeTargetFacts,
        reason: RevocationReasonCodeV1,
    ) -> Result<AuthorizedCapabilityMutationPreparation, PolicyCode> {
        AuthorizedCapabilityMutationPreparation::revoke_absent(
            current,
            &principal_id(),
            ActorKind::Service,
            &audience("grpc"),
            current.grant().tenant_scope(),
            target,
            reason,
        )
    }

    fn ordinary_grant() -> CapabilityGrantV1 {
        grant(
            TenantScope::Global,
            vec![
                permission(CapabilityPermissionKindV1::ReadHealth),
                permission(CapabilityPermissionKindV1::CreateCapability),
            ],
            100,
            Vec::new(),
        )
    }

    fn ordinary_revoke_grant() -> CapabilityGrantV1 {
        grant(
            TenantScope::Global,
            vec![
                permission(CapabilityPermissionKindV1::ReadHealth),
                permission(CapabilityPermissionKindV1::RevokeCapability),
            ],
            100,
            Vec::new(),
        )
    }

    #[test]
    fn every_mutation_preparation_retains_exact_authority_without_fabricated_approval() {
        let create_current = current(ordinary_grant(), vec![audience("grpc")], timestamp(1_000));
        let create = prepare(
            &create_current,
            create_target(0x37, child_grant(), vec![audience("grpc")], 60),
            timestamp(200),
        )
        .expect("create preparation");
        assert_eq!(
            create.authorizing_capability_id(),
            create_current.capability_id()
        );
        assert_eq!(create.authorizing_revision(), create_current.revision());
        assert_eq!(create.validated_approval(), None);

        let revoke_current = current(
            ordinary_revoke_grant(),
            vec![audience("grpc")],
            timestamp(1_000),
        );
        let revoke = prepare_revoke(
            &revoke_current,
            revoke_target(0x38, child_grant(), vec![audience("grpc")], timestamp(900)),
            RevocationReasonCodeV1::Requested,
        )
        .expect("revoke preparation");
        assert_eq!(
            revoke.authorizing_capability_id(),
            revoke_current.capability_id()
        );
        assert_eq!(revoke.authorizing_revision(), revoke_current.revision());
        assert_eq!(revoke.validated_approval(), None);

        let administrator = current(
            administrator_grant(),
            vec![audience("grpc")],
            timestamp(1_000),
        );
        let absent = prepare_absent_revoke(
            &administrator,
            absent_revoke_target(0x39),
            RevocationReasonCodeV1::Requested,
        )
        .expect("absent revoke preparation");
        assert_eq!(
            absent.authorizing_capability_id(),
            administrator.capability_id()
        );
        assert_eq!(absent.authorizing_revision(), administrator.revision());
        assert_eq!(absent.validated_approval(), None);
    }

    fn administrator_grant() -> CapabilityGrantV1 {
        grant(
            TenantScope::Global,
            vec![permission(
                CapabilityPermissionKindV1::AdministerCapabilities,
            )],
            1,
            Vec::new(),
        )
    }

    fn child_grant() -> CapabilityGrantV1 {
        grant(
            TenantScope::Tenant(TenantId::new("tenant-a").expect("valid tenant")),
            vec![permission(CapabilityPermissionKindV1::ReadHealth)],
            20,
            Vec::new(),
        )
    }

    fn scoped_partition(value: u64) -> ScopedPartitionV1 {
        let mut builder = PartitionKeyBuilder::new(AggregateTypeId::first());
        builder.push_u64(value).expect("bounded component");
        ScopedPartitionV1::new(
            ContractLineage::new("example.contract").expect("valid lineage"),
            builder.finish().expect("valid partition"),
        )
    }

    fn grant_with_partition(
        tenant_scope: TenantScope,
        partition_scope: PartitionScopeV1,
        permissions: Vec<CapabilityPermissionV1>,
        max_rows: u16,
        approval_required: Vec<CapabilityPermissionKindV1>,
    ) -> CapabilityGrantV1 {
        CapabilityGrantV1::new(
            tenant_scope,
            partition_scope,
            CapabilityPermissionsV1::new(permissions).expect("valid permissions"),
            Vec::new(),
            NonZeroU16::new(max_rows).expect("nonzero rows"),
            approval_required,
        )
        .expect("valid grant")
    }

    #[test]
    fn checked_audience_boundaries_canonicalize_only_configuration_and_create_input() {
        let configured = TrustedAudienceCatalog::new(vec![audience("mcp"), audience("grpc")])
            .expect("valid catalog");
        assert_eq!(configured.audiences(), &[audience("grpc"), audience("mcp")]);
        assert_eq!(
            TrustedAudienceCatalog::new(vec![audience("grpc"), audience("grpc")]),
            Err(CapabilityMutationFactsError::DuplicateAudience)
        );

        let target = create_target(
            0x31,
            child_grant(),
            vec![audience("mcp"), audience("grpc")],
            60,
        );
        assert_eq!(
            target.normalized_requested_record().audiences(),
            &[audience("grpc"), audience("mcp")]
        );

        let unsorted = TransactionCurrentCapabilityFacts::new(
            authorizing_capability_id(),
            NonZeroU64::new(1).expect("nonzero revision"),
            CapabilityActivity::Active,
            database_id(),
            environment(),
            principal_id(),
            ActorKind::Service,
            vec![audience("mcp"), audience("grpc")],
            timestamp(1),
            timestamp(2),
            ordinary_grant(),
        );
        assert_eq!(
            unsorted,
            Err(CapabilityMutationFactsError::NonCanonicalAudiences)
        );
    }

    #[test]
    fn create_target_enforces_audience_and_lifetime_hard_bounds() {
        let too_many = (0..=MAX_CAPABILITY_AUDIENCES)
            .map(|index| audience(&format!("audience-{index}")))
            .collect();
        let result = NormalizedCapabilityCreateRecord::new(
            database_id(),
            environment(),
            target_principal_id(),
            ActorKind::Agent,
            NonZeroU32::new(1).expect("nonzero duration"),
            too_many,
            child_grant(),
        );
        assert_eq!(result, Err(CapabilityMutationFactsError::TooManyAudiences));

        let result = NormalizedCapabilityCreateRecord::new(
            database_id(),
            environment(),
            target_principal_id(),
            ActorKind::Agent,
            NonZeroU32::new(MAX_CAPABILITY_LIFETIME_SECONDS + 1).expect("nonzero duration"),
            vec![audience("grpc")],
            child_grant(),
        );
        assert_eq!(
            result,
            Err(CapabilityMutationFactsError::LifetimeLimitExceeded)
        );

        assert!(
            NormalizedCapabilityCreateRecord::new(
                database_id(),
                environment(),
                target_principal_id(),
                ActorKind::Agent,
                NonZeroU32::new(MAX_CAPABILITY_LIFETIME_SECONDS).expect("nonzero duration"),
                vec![audience("grpc")],
                child_grant(),
            )
            .is_ok()
        );
    }

    #[test]
    fn complete_subset_checks_tenant_partition_permission_rows_and_child_approval() {
        let tenant_a = TenantId::new("tenant-a").expect("valid tenant");
        let tenant_b = TenantId::new("tenant-b").expect("valid tenant");
        let lineage = ContractLineage::new("example.contract").expect("valid lineage");
        let invoke_one = CapabilityPermissionV1::InvokeCommand(lineage.clone(), CommandId::first());
        let invoke_two = CapabilityPermissionV1::InvokeCommand(
            lineage,
            CommandId::new(2).expect("nonzero command"),
        );
        let partition_one = scoped_partition(1);
        let partition_two = scoped_partition(2);
        let parent_partition =
            PartitionScopeV1::explicit(vec![partition_one.clone(), partition_two])
                .expect("valid partitions");
        let parent = grant_with_partition(
            TenantScope::Tenant(tenant_a.clone()),
            parent_partition,
            vec![invoke_one.clone()],
            100,
            Vec::new(),
        );

        let same_tenant_narrow_partition = grant_with_partition(
            TenantScope::Tenant(tenant_a.clone()),
            PartitionScopeV1::explicit(vec![partition_one.clone()]).expect("valid partition"),
            vec![invoke_one.clone()],
            50,
            vec![CapabilityPermissionKindV1::InvokeCommand],
        );
        assert!(grant_subset(&same_tenant_narrow_partition, &parent));

        let different_tenant = grant_with_partition(
            TenantScope::Tenant(tenant_b),
            PartitionScopeV1::explicit(vec![partition_one.clone()]).expect("valid partition"),
            vec![invoke_one.clone()],
            50,
            Vec::new(),
        );
        assert!(!grant_subset(&different_tenant, &parent));

        let global_child = grant_with_partition(
            TenantScope::Global,
            PartitionScopeV1::explicit(vec![partition_one.clone()]).expect("valid partition"),
            vec![invoke_one.clone()],
            50,
            Vec::new(),
        );
        assert!(!grant_subset(&global_child, &parent));

        let all_partitions = grant_with_partition(
            TenantScope::Tenant(tenant_a.clone()),
            PartitionScopeV1::All,
            vec![invoke_one.clone()],
            50,
            Vec::new(),
        );
        assert!(!grant_subset(&all_partitions, &parent));

        let unknown_partition = grant_with_partition(
            TenantScope::Tenant(tenant_a.clone()),
            PartitionScopeV1::explicit(vec![scoped_partition(3)]).expect("valid partition"),
            vec![invoke_one.clone()],
            50,
            Vec::new(),
        );
        assert!(!grant_subset(&unknown_partition, &parent));

        let different_exact_atom = grant_with_partition(
            TenantScope::Tenant(tenant_a.clone()),
            PartitionScopeV1::explicit(vec![partition_one.clone()]).expect("valid partition"),
            vec![invoke_two],
            50,
            Vec::new(),
        );
        assert!(!grant_subset(&different_exact_atom, &parent));

        let too_many_rows = grant_with_partition(
            TenantScope::Tenant(tenant_a),
            PartitionScopeV1::explicit(vec![partition_one]).expect("valid partition"),
            vec![invoke_one],
            101,
            Vec::new(),
        );
        assert!(!grant_subset(&too_many_rows, &parent));

        let global_parent = ordinary_grant();
        assert!(grant_subset(&child_grant(), &global_parent));
    }

    #[test]
    fn ordinary_create_is_reauthorized_against_exact_current_state_and_interval() {
        let current = current(
            ordinary_grant(),
            vec![audience("grpc"), audience("mcp")],
            timestamp(1_000),
        );
        let target = create_target(0x41, child_grant(), vec![audience("grpc")], 60);
        let preparation = prepare(&current, target.clone(), timestamp(200)).expect("prepared");
        assert_eq!(
            preparation.delegation_mode(),
            CapabilityDelegationMode::Subset
        );

        let commit_time = timestamp(250);
        let proposed = ProposedCapabilityCreate::new(target, commit_time, timestamp(310));
        let TransactionCapabilityMutationDecision::Allow(proof) =
            TransactionCurrentCapabilityVerifier::verify_create(
                &current,
                commit_time,
                preparation,
                proposed,
            )
        else {
            panic!("expected transaction-current allow");
        };
        assert_eq!(proof.authoritative_time(), commit_time);
    }

    #[test]
    fn ordinary_create_rejects_every_create_scope_escalation() {
        let current = current(ordinary_grant(), vec![audience("grpc")], timestamp(300));

        let outside_audience = create_target(0x45, child_grant(), vec![audience("mcp")], 60);
        assert_eq!(
            prepare(&current, outside_audience, timestamp(200)),
            Err(PolicyCode::DelegationExceedsAuthority)
        );

        let unconfigured = create_target(0x46, child_grant(), vec![audience("unconfigured")], 60);
        assert_eq!(
            prepare(&current, unconfigured, timestamp(200)),
            Err(PolicyCode::DelegationExceedsAuthority)
        );

        let too_long = create_target(0x47, child_grant(), vec![audience("grpc")], 101);
        assert_eq!(
            prepare(&current, too_long, timestamp(200)),
            Err(PolicyCode::DelegationExceedsAuthority)
        );

        let mut wrong_database = create_target(0x48, child_grant(), vec![audience("grpc")], 60);
        wrong_database.normalized_requested_record.database_id =
            DatabaseId::from_unix_milliseconds_and_random(9, [0x48; 10]).expect("valid UUIDv7");
        assert_eq!(
            prepare(&current, wrong_database, timestamp(200)),
            Err(PolicyCode::DelegationExceedsAuthority)
        );

        let mut wrong_environment = create_target(0x49, child_grant(), vec![audience("grpc")], 60);
        wrong_environment.normalized_requested_record.environment =
            Environment::new("other").expect("valid environment");
        assert_eq!(
            prepare(&current, wrong_environment, timestamp(200)),
            Err(PolicyCode::DelegationExceedsAuthority)
        );

        let rows_escalated = grant(
            TenantScope::Global,
            vec![permission(CapabilityPermissionKindV1::ReadHealth)],
            101,
            Vec::new(),
        );
        let rows_target = create_target(0x4a, rows_escalated, vec![audience("grpc")], 60);
        assert_eq!(
            prepare(&current, rows_target, timestamp(200)),
            Err(PolicyCode::DelegationExceedsAuthority)
        );
    }

    #[test]
    fn administrator_can_create_broader_authority_and_outlive_the_caller() {
        let administrator = grant(
            TenantScope::Tenant(TenantId::new("tenant-a").expect("valid tenant")),
            vec![permission(
                CapabilityPermissionKindV1::AdministerCapabilities,
            )],
            1,
            Vec::new(),
        );
        let current = current(administrator, vec![audience("grpc")], timestamp(300));
        let broader = grant(
            TenantScope::Global,
            vec![permission(CapabilityPermissionKindV1::ReadStatistics)],
            500,
            Vec::new(),
        );
        let target = create_target(0x42, broader, vec![audience("mcp")], 100);
        let preparation = prepare(&current, target.clone(), timestamp(250)).expect("prepared");
        assert_eq!(
            preparation.delegation_mode(),
            CapabilityDelegationMode::Administrator
        );

        let commit_time = timestamp(260);
        assert!(matches!(
            TransactionCurrentCapabilityVerifier::verify_create(
                &current,
                commit_time,
                preparation,
                ProposedCapabilityCreate::new(target, commit_time, timestamp(360),),
            ),
            TransactionCapabilityMutationDecision::Allow(_)
        ));
    }

    #[test]
    fn ordinary_revoke_is_reauthorized_with_complete_subset_and_exact_time() {
        let current = current(
            ordinary_revoke_grant(),
            vec![audience("grpc"), audience("mcp")],
            timestamp(1_000),
        );
        let target = revoke_target(0x71, child_grant(), vec![audience("mcp")], timestamp(900));
        let preparation =
            prepare_revoke(&current, target.clone(), RevocationReasonCodeV1::Requested)
                .expect("prepared");
        assert_eq!(
            preparation.delegation_mode(),
            CapabilityDelegationMode::Subset
        );
        assert_eq!(preparation.revoke_target().expect("revoke target"), &target);
        assert_eq!(
            preparation.revoke_reason(),
            Some(RevocationReasonCodeV1::Requested)
        );

        let commit_time = timestamp(250);
        let current_target = current_revoke_target(&target);
        let TransactionCapabilityMutationDecision::Allow(proof) =
            TransactionCurrentCapabilityVerifier::verify_revoke(
                &current,
                &current_target,
                commit_time,
                preparation,
                ProposedCapabilityRevoke::new(
                    target,
                    RevocationReasonCodeV1::Requested,
                    commit_time,
                ),
            )
        else {
            panic!("expected transaction-current allow");
        };
        assert_eq!(proof.authoritative_time(), commit_time);
    }

    #[test]
    fn present_revoke_requires_exact_transaction_current_target_facts() {
        let authorizer = current(
            ordinary_revoke_grant(),
            vec![audience("grpc"), audience("mcp")],
            timestamp(1_000),
        );
        let target = revoke_target(0x7d, child_grant(), vec![audience("mcp")], timestamp(900));
        let exact_current_target = current_revoke_target(&target);
        let mut changes = Vec::new();

        let mut changed = exact_current_target.clone();
        changed.capability_id = target_capability_id(0x7e);
        changes.push(changed);
        let mut changed = exact_current_target.clone();
        changed.revision = NonZeroU64::new(4).expect("nonzero revision");
        changes.push(changed);
        let mut changed = exact_current_target.clone();
        changed.activity = CapabilityActivity::Revoked;
        changes.push(changed);
        let mut changed = exact_current_target.clone();
        changed.database_id =
            DatabaseId::from_unix_milliseconds_and_random(9, [0x7e; 10]).expect("valid UUIDv7");
        changes.push(changed);
        let mut changed = exact_current_target.clone();
        changed.environment = Environment::new("other").expect("valid environment");
        changes.push(changed);
        let mut changed = exact_current_target.clone();
        changed.principal_id = ActorId::new("changed-principal").expect("valid actor");
        changes.push(changed);
        let mut changed = exact_current_target.clone();
        changed.actor_kind = ActorKind::Human;
        changes.push(changed);
        let mut changed = exact_current_target.clone();
        changed.audiences = vec![audience("grpc")];
        changes.push(changed);
        let mut changed = exact_current_target.clone();
        changed.issued_at = timestamp(101);
        changes.push(changed);
        let mut changed = exact_current_target.clone();
        changed.expires_at = timestamp(901);
        changes.push(changed);
        let mut changed = exact_current_target;
        changed.grant = grant(
            TenantScope::Global,
            vec![permission(CapabilityPermissionKindV1::ReadHealth)],
            1,
            Vec::new(),
        );
        changes.push(changed);

        for changed_target in changes {
            let preparation = prepare_revoke(
                &authorizer,
                target.clone(),
                RevocationReasonCodeV1::Requested,
            )
            .expect("prepared");
            assert_eq!(
                TransactionCurrentCapabilityVerifier::verify_revoke(
                    &authorizer,
                    &changed_target,
                    timestamp(250),
                    preparation,
                    ProposedCapabilityRevoke::new(
                        target.clone(),
                        RevocationReasonCodeV1::Requested,
                        timestamp(250),
                    ),
                ),
                TransactionCapabilityMutationDecision::PreparationChanged(
                    CapabilityRevokePreparationChange::TargetChanged,
                )
            );
        }
    }

    #[test]
    fn present_revoke_denies_authorizer_drift_before_classifying_target_drift() {
        let authorizer = current(
            ordinary_revoke_grant(),
            vec![audience("grpc")],
            timestamp(1_000),
        );
        let target = revoke_target(0x7f, child_grant(), vec![audience("grpc")], timestamp(900));
        let preparation = prepare_revoke(
            &authorizer,
            target.clone(),
            RevocationReasonCodeV1::Requested,
        )
        .expect("prepared");
        let mut changed_authorizer = authorizer;
        changed_authorizer.activity = CapabilityActivity::Revoked;
        let mut changed_target = current_revoke_target(&target);
        changed_target.revision = NonZeroU64::new(4).expect("nonzero revision");

        assert_eq!(
            TransactionCurrentCapabilityVerifier::verify_revoke(
                &changed_authorizer,
                &changed_target,
                timestamp(250),
                preparation,
                ProposedCapabilityRevoke::new(
                    target,
                    RevocationReasonCodeV1::Requested,
                    timestamp(250),
                ),
            ),
            TransactionCapabilityMutationDecision::Deny(PolicyCode::InactiveOrStaleCapability,)
        );
    }

    #[test]
    fn freshly_prepared_revoked_target_remains_eligible_for_idempotent_revoke() {
        let authorizer = current(
            ordinary_revoke_grant(),
            vec![audience("grpc")],
            timestamp(1_000),
        );
        let mut target = revoke_target(0x80, child_grant(), vec![audience("grpc")], timestamp(900));
        target.revision = NonZeroU64::new(4).expect("nonzero revision");
        target.activity = CapabilityActivity::Revoked;
        let preparation = prepare_revoke(
            &authorizer,
            target.clone(),
            RevocationReasonCodeV1::Requested,
        )
        .expect("prepared");
        let current_target = current_revoke_target(&target);

        assert!(matches!(
            TransactionCurrentCapabilityVerifier::verify_revoke(
                &authorizer,
                &current_target,
                timestamp(250),
                preparation,
                ProposedCapabilityRevoke::new(
                    target,
                    RevocationReasonCodeV1::Requested,
                    timestamp(250),
                ),
            ),
            TransactionCapabilityMutationDecision::Allow(_)
        ));
    }

    #[test]
    fn administrator_revoke_bypasses_subset_only_for_exact_database_and_environment() {
        let administrator = grant(
            TenantScope::Tenant(TenantId::new("tenant-a").expect("valid tenant")),
            vec![permission(
                CapabilityPermissionKindV1::AdministerCapabilities,
            )],
            1,
            Vec::new(),
        );
        let current = current(administrator, vec![audience("grpc")], timestamp(300));
        let broad_target_grant = grant(
            TenantScope::Global,
            vec![permission(CapabilityPermissionKindV1::ReadStatistics)],
            500,
            Vec::new(),
        );
        let target = revoke_target(
            0x72,
            broad_target_grant,
            vec![audience("mcp")],
            timestamp(2_000),
        );
        let preparation = prepare_revoke(
            &current,
            target.clone(),
            RevocationReasonCodeV1::PolicyChange,
        )
        .expect("prepared");
        assert_eq!(
            preparation.delegation_mode(),
            CapabilityDelegationMode::Administrator
        );
        let current_target = current_revoke_target(&target);
        assert!(matches!(
            TransactionCurrentCapabilityVerifier::verify_revoke(
                &current,
                &current_target,
                timestamp(250),
                preparation,
                ProposedCapabilityRevoke::new(
                    target,
                    RevocationReasonCodeV1::PolicyChange,
                    timestamp(250),
                ),
            ),
            TransactionCapabilityMutationDecision::Allow(_)
        ));

        let mut wrong_database =
            revoke_target(0x73, child_grant(), vec![audience("grpc")], timestamp(290));
        wrong_database.database_id =
            DatabaseId::from_unix_milliseconds_and_random(9, [0x73; 10]).expect("valid UUIDv7");
        assert_eq!(
            prepare_revoke(&current, wrong_database, RevocationReasonCodeV1::Requested,),
            Err(PolicyCode::DelegationExceedsAuthority)
        );

        let mut wrong_environment =
            revoke_target(0x74, child_grant(), vec![audience("grpc")], timestamp(290));
        wrong_environment.environment = Environment::new("other").expect("valid environment");
        assert_eq!(
            prepare_revoke(
                &current,
                wrong_environment,
                RevocationReasonCodeV1::Requested,
            ),
            Err(PolicyCode::DelegationExceedsAuthority)
        );
    }

    #[test]
    fn absent_revoke_is_admin_only_and_preserves_the_exact_absence() {
        let ordinary = current(
            ordinary_revoke_grant(),
            vec![audience("grpc")],
            timestamp(1_000),
        );
        let target = absent_revoke_target(0x81);
        assert_eq!(
            prepare_absent_revoke(&ordinary, target.clone(), RevocationReasonCodeV1::Requested,),
            Err(PolicyCode::MissingPermission)
        );

        let administrator = current(
            administrator_grant(),
            vec![audience("grpc")],
            timestamp(1_000),
        );
        let preparation = prepare_absent_revoke(
            &administrator,
            target.clone(),
            RevocationReasonCodeV1::PolicyChange,
        )
        .expect("administrator prepares exact absence");
        assert_eq!(
            preparation.delegation_mode(),
            CapabilityDelegationMode::Administrator
        );
        assert_eq!(preparation.create_target(), None);
        assert_eq!(preparation.revoke_target(), None);
        assert_eq!(preparation.absent_revoke_target(), Some(&target));
        assert_eq!(
            preparation.revoke_reason(),
            Some(RevocationReasonCodeV1::PolicyChange)
        );

        let TransactionAbsentCapabilityRevokeDecision::Allow(proof) =
            TransactionCurrentCapabilityVerifier::verify_absent_revoke(
                &administrator,
                TransactionCurrentCapabilityExistence::absent(target.capability_id()),
                timestamp(250),
                preparation,
            )
        else {
            panic!("expected exact absent-target authorization");
        };
        assert_eq!(proof.authoritative_time(), timestamp(250));
        assert_eq!(
            proof
                .preparation()
                .absent_revoke_target()
                .expect("absent target"),
            &target
        );
    }

    #[test]
    fn absent_revoke_rejects_database_environment_and_target_substitution() {
        let administrator = current(
            administrator_grant(),
            vec![audience("grpc")],
            timestamp(1_000),
        );
        let target = absent_revoke_target(0x82);
        let mut wrong_database = target.clone();
        wrong_database.database_id =
            DatabaseId::from_unix_milliseconds_and_random(9, [0x82; 10]).expect("valid UUIDv7");
        assert_eq!(
            prepare_absent_revoke(
                &administrator,
                wrong_database,
                RevocationReasonCodeV1::Requested,
            ),
            Err(PolicyCode::DelegationExceedsAuthority)
        );

        let mut wrong_environment = target.clone();
        wrong_environment.environment = Environment::new("other").expect("valid environment");
        assert_eq!(
            prepare_absent_revoke(
                &administrator,
                wrong_environment,
                RevocationReasonCodeV1::Requested,
            ),
            Err(PolicyCode::DelegationExceedsAuthority)
        );

        let preparation =
            prepare_absent_revoke(&administrator, target, RevocationReasonCodeV1::Requested)
                .expect("prepared");
        assert_eq!(
            TransactionCurrentCapabilityVerifier::verify_absent_revoke(
                &administrator,
                TransactionCurrentCapabilityExistence::absent(target_capability_id(0x83)),
                timestamp(250),
                preparation,
            ),
            TransactionAbsentCapabilityRevokeDecision::Deny(PolicyCode::DelegationExceedsAuthority)
        );
    }

    #[test]
    fn appeared_absent_revoke_target_requires_a_fresh_present_preparation() {
        let administrator = current(
            administrator_grant(),
            vec![audience("grpc")],
            timestamp(1_000),
        );
        let target = absent_revoke_target(0x84);
        let preparation = prepare_absent_revoke(
            &administrator,
            target.clone(),
            RevocationReasonCodeV1::Requested,
        )
        .expect("prepared");

        assert_eq!(
            TransactionCurrentCapabilityVerifier::verify_absent_revoke(
                &administrator,
                TransactionCurrentCapabilityExistence::present(target.capability_id()),
                timestamp(250),
                preparation,
            ),
            TransactionAbsentCapabilityRevokeDecision::PreparationChanged(
                AbsentCapabilityRevokePreparationChange::TargetAppeared
            )
        );
    }

    #[test]
    fn absent_revoke_rechecks_authorizer_before_classifying_target_appearance() {
        let initial = current(
            administrator_grant(),
            vec![audience("grpc")],
            timestamp(1_000),
        );
        let target = absent_revoke_target(0x85);
        let verify = |transaction_current: &TransactionCurrentCapabilityFacts| {
            let preparation =
                prepare_absent_revoke(&initial, target.clone(), RevocationReasonCodeV1::Requested)
                    .expect("prepared");
            TransactionCurrentCapabilityVerifier::verify_absent_revoke(
                transaction_current,
                TransactionCurrentCapabilityExistence::present(target.capability_id()),
                timestamp(250),
                preparation,
            )
        };

        let mut stale = initial.clone();
        stale.revision = NonZeroU64::new(8).expect("nonzero revision");
        assert_eq!(
            verify(&stale),
            TransactionAbsentCapabilityRevokeDecision::Deny(PolicyCode::InactiveOrStaleCapability)
        );

        let mut revoked = initial.clone();
        revoked.activity = CapabilityActivity::Revoked;
        assert_eq!(
            verify(&revoked),
            TransactionAbsentCapabilityRevokeDecision::Deny(PolicyCode::InactiveOrStaleCapability)
        );

        let mut expired = initial.clone();
        expired.expires_at = timestamp(250);
        assert_eq!(
            verify(&expired),
            TransactionAbsentCapabilityRevokeDecision::Deny(PolicyCode::InactiveOrStaleCapability)
        );

        let mut missing = initial.clone();
        missing.grant = grant(TenantScope::Global, Vec::new(), 1, Vec::new());
        assert_eq!(
            verify(&missing),
            TransactionAbsentCapabilityRevokeDecision::Deny(PolicyCode::MissingPermission)
        );

        let mut approval = initial.clone();
        approval.grant = grant(
            TenantScope::Global,
            vec![permission(
                CapabilityPermissionKindV1::AdministerCapabilities,
            )],
            1,
            vec![CapabilityPermissionKindV1::AdministerCapabilities],
        );
        assert_eq!(
            verify(&approval),
            TransactionAbsentCapabilityRevokeDecision::Deny(PolicyCode::ApprovalRequired)
        );

        let mut wrong_environment = initial.clone();
        wrong_environment.environment = Environment::new("other").expect("valid environment");
        assert_eq!(
            verify(&wrong_environment),
            TransactionAbsentCapabilityRevokeDecision::Deny(PolicyCode::DelegationExceedsAuthority)
        );
    }

    #[test]
    fn ordinary_revoke_rejects_every_complete_subset_escalation() {
        let current = current(
            ordinary_revoke_grant(),
            vec![audience("grpc"), audience("mcp")],
            timestamp(1_000),
        );
        let base = revoke_target(0x75, child_grant(), vec![audience("mcp")], timestamp(900));
        let mut outside_audience = base.clone();
        outside_audience.audiences = vec![audience("unconfigured")];
        let mut outliving = base.clone();
        outliving.expires_at = timestamp(1_001);
        let mut wrong_database = base.clone();
        wrong_database.database_id =
            DatabaseId::from_unix_milliseconds_and_random(9, [0x75; 10]).expect("valid UUIDv7");
        let mut wrong_environment = base.clone();
        wrong_environment.environment = Environment::new("other").expect("valid environment");
        let mut rows_escalated = base;
        rows_escalated.grant = grant(
            TenantScope::Global,
            vec![permission(CapabilityPermissionKindV1::ReadHealth)],
            101,
            Vec::new(),
        );

        for target in [
            outside_audience,
            outliving,
            wrong_database,
            wrong_environment,
            rows_escalated,
        ] {
            assert_eq!(
                prepare_revoke(&current, target, RevocationReasonCodeV1::Requested),
                Err(PolicyCode::DelegationExceedsAuthority)
            );
        }
    }

    #[test]
    fn revoke_approval_is_fail_closed_and_selected_path_cannot_switch() {
        let approval_only = grant(
            TenantScope::Global,
            vec![
                permission(CapabilityPermissionKindV1::ReadHealth),
                permission(CapabilityPermissionKindV1::RevokeCapability),
            ],
            100,
            vec![CapabilityPermissionKindV1::RevokeCapability],
        );
        let approval_current = current(approval_only, vec![audience("grpc")], timestamp(1_000));
        let target = revoke_target(0x76, child_grant(), vec![audience("grpc")], timestamp(900));
        assert_eq!(
            prepare_revoke(
                &approval_current,
                target.clone(),
                RevocationReasonCodeV1::Requested,
            ),
            Err(PolicyCode::ApprovalRequired)
        );

        let admin_approval = grant(
            TenantScope::Global,
            vec![permission(
                CapabilityPermissionKindV1::AdministerCapabilities,
            )],
            1,
            vec![CapabilityPermissionKindV1::AdministerCapabilities],
        );
        let admin_approval_current =
            current(admin_approval, vec![audience("grpc")], timestamp(1_000));
        assert_eq!(
            prepare_revoke(
                &admin_approval_current,
                target.clone(),
                RevocationReasonCodeV1::Requested,
            ),
            Err(PolicyCode::ApprovalRequired)
        );

        let both = grant(
            TenantScope::Global,
            vec![
                permission(CapabilityPermissionKindV1::ReadHealth),
                permission(CapabilityPermissionKindV1::RevokeCapability),
                permission(CapabilityPermissionKindV1::AdministerCapabilities),
            ],
            100,
            Vec::new(),
        );
        let initial = current(both, vec![audience("grpc")], timestamp(1_000));
        let preparation =
            prepare_revoke(&initial, target.clone(), RevocationReasonCodeV1::Requested)
                .expect("prepared");
        assert_eq!(
            preparation.delegation_mode(),
            CapabilityDelegationMode::Subset
        );
        let mut changed = initial.clone();
        changed.grant = grant(
            TenantScope::Global,
            vec![
                permission(CapabilityPermissionKindV1::ReadHealth),
                permission(CapabilityPermissionKindV1::AdministerCapabilities),
            ],
            100,
            Vec::new(),
        );
        let current_target = current_revoke_target(&target);
        assert_eq!(
            TransactionCurrentCapabilityVerifier::verify_revoke(
                &changed,
                &current_target,
                timestamp(250),
                preparation,
                ProposedCapabilityRevoke::new(
                    target,
                    RevocationReasonCodeV1::Requested,
                    timestamp(250),
                ),
            ),
            TransactionCapabilityMutationDecision::Deny(PolicyCode::MissingPermission)
        );
    }

    #[test]
    fn revoke_proposal_binds_target_reason_and_authoritative_time_exactly() {
        let current = current(
            ordinary_revoke_grant(),
            vec![audience("grpc")],
            timestamp(1_000),
        );
        let target = revoke_target(0x77, child_grant(), vec![audience("grpc")], timestamp(900));
        let mut request_changed = target.clone();
        request_changed.request_id = request_id(0x78);
        let mut revision_changed = target.clone();
        revision_changed.revision = NonZeroU64::new(4).expect("nonzero revision");

        let cases = [
            (
                request_changed,
                RevocationReasonCodeV1::Requested,
                timestamp(250),
            ),
            (
                revision_changed,
                RevocationReasonCodeV1::Requested,
                timestamp(250),
            ),
            (
                target.clone(),
                RevocationReasonCodeV1::Replaced,
                timestamp(250),
            ),
            (
                target.clone(),
                RevocationReasonCodeV1::Requested,
                timestamp(251),
            ),
        ];
        let current_target = current_revoke_target(&target);
        for (proposed_target, proposed_reason, proposed_time) in cases {
            let preparation =
                prepare_revoke(&current, target.clone(), RevocationReasonCodeV1::Requested)
                    .expect("prepared");
            assert_eq!(
                TransactionCurrentCapabilityVerifier::verify_revoke(
                    &current,
                    &current_target,
                    timestamp(250),
                    preparation,
                    ProposedCapabilityRevoke::new(proposed_target, proposed_reason, proposed_time,),
                ),
                TransactionCapabilityMutationDecision::Deny(PolicyCode::DelegationExceedsAuthority)
            );
        }
    }

    #[test]
    fn revoke_target_lowering_rejects_noncanonical_audiences() {
        let result = CapabilityRevokeTargetFacts::new(
            request_id(0x79),
            target_capability_id(0x79),
            NonZeroU64::MIN,
            CapabilityActivity::Active,
            database_id(),
            environment(),
            target_principal_id(),
            ActorKind::Agent,
            vec![audience("mcp"), audience("grpc")],
            timestamp(100),
            timestamp(900),
            child_grant(),
        );
        assert_eq!(
            result,
            Err(CapabilityMutationFactsError::NonCanonicalAudiences)
        );
    }

    #[test]
    fn preparation_cannot_be_paired_with_the_other_mutation_operation() {
        let authority = grant(
            TenantScope::Global,
            vec![
                permission(CapabilityPermissionKindV1::ReadHealth),
                permission(CapabilityPermissionKindV1::CreateCapability),
                permission(CapabilityPermissionKindV1::RevokeCapability),
            ],
            100,
            Vec::new(),
        );
        let current = current(authority, vec![audience("grpc")], timestamp(1_000));
        let create_target = create_target(0x7a, child_grant(), vec![audience("grpc")], 60);
        let revoke_target =
            revoke_target(0x7b, child_grant(), vec![audience("grpc")], timestamp(900));
        let create_preparation =
            prepare(&current, create_target.clone(), timestamp(200)).expect("create prepared");
        let current_revoke_target = current_revoke_target(&revoke_target);
        assert_eq!(
            TransactionCurrentCapabilityVerifier::verify_revoke(
                &current,
                &current_revoke_target,
                timestamp(250),
                create_preparation,
                ProposedCapabilityRevoke::new(
                    revoke_target.clone(),
                    RevocationReasonCodeV1::Requested,
                    timestamp(250),
                ),
            ),
            TransactionCapabilityMutationDecision::Deny(PolicyCode::DelegationExceedsAuthority)
        );

        let revoke_preparation =
            prepare_revoke(&current, revoke_target, RevocationReasonCodeV1::Requested)
                .expect("revoke prepared");
        assert_eq!(
            TransactionCurrentCapabilityVerifier::verify_create(
                &current,
                timestamp(250),
                revoke_preparation,
                ProposedCapabilityCreate::new(create_target, timestamp(250), timestamp(310),),
            ),
            TransactionCapabilityMutationDecision::Deny(PolicyCode::DelegationExceedsAuthority)
        );
    }

    #[test]
    fn mutation_debug_output_redacts_every_bound_value() {
        let current = current(ordinary_grant(), vec![audience("grpc")], timestamp(1_000));
        let target = create_target(0x7c, child_grant(), vec![audience("grpc")], 60);
        let preparation = prepare(&current, target.clone(), timestamp(200)).expect("prepared");
        let proposed = ProposedCapabilityMutation::Create(ProposedCapabilityCreate::new(
            target,
            timestamp(250),
            timestamp(310),
        ));
        for rendered in [
            format!("{current:?}"),
            format!("{preparation:?}"),
            format!("{proposed:?}"),
        ] {
            assert!(rendered.contains("[REDACTED]"));
            assert!(!rendered.contains("authorizing-principal"));
            assert!(!rendered.contains("target-principal"));
            assert!(!rendered.contains("grpc"));
        }
    }

    #[test]
    fn absent_revoke_debug_output_redacts_scope_and_identity_facts() {
        let secret_environment = Environment::new("secret-environment").expect("valid environment");
        let mut administrator = current(
            administrator_grant(),
            vec![audience("grpc")],
            timestamp(1_000),
        );
        administrator.environment = secret_environment.clone();
        let target = AbsentCapabilityRevokeTargetFacts::new(
            request_id(0x86),
            target_capability_id(0x86),
            database_id(),
            secret_environment,
        );
        let existence = TransactionCurrentCapabilityExistence::absent(target.capability_id());
        let preparation = prepare_absent_revoke(
            &administrator,
            target.clone(),
            RevocationReasonCodeV1::Requested,
        )
        .expect("prepared");
        let preparation_debug = format!("{preparation:?}");
        let decision = TransactionCurrentCapabilityVerifier::verify_absent_revoke(
            &administrator,
            existence,
            timestamp(250),
            preparation,
        );

        for rendered in [
            format!("{target:?}"),
            format!("{existence:?}"),
            preparation_debug,
            format!("{decision:?}"),
        ] {
            assert!(rendered.contains("[REDACTED]"));
            assert!(!rendered.contains("secret-environment"));
        }
    }

    #[test]
    fn path_selection_is_per_authority_and_fails_closed_for_approval() {
        let both = grant(
            TenantScope::Global,
            vec![
                permission(CapabilityPermissionKindV1::CreateCapability),
                permission(CapabilityPermissionKindV1::AdministerCapabilities),
            ],
            100,
            vec![CapabilityPermissionKindV1::CreateCapability],
        );
        let both_current = current(both, vec![audience("grpc")], timestamp(1_000));
        let target = create_target(0x43, child_grant(), vec![audience("grpc")], 60);
        let preparation =
            prepare(&both_current, target, timestamp(200)).expect("admin path is viable");
        assert_eq!(
            preparation.delegation_mode(),
            CapabilityDelegationMode::Administrator
        );

        let approval_only = grant(
            TenantScope::Global,
            vec![
                permission(CapabilityPermissionKindV1::ReadHealth),
                permission(CapabilityPermissionKindV1::CreateCapability),
            ],
            100,
            vec![CapabilityPermissionKindV1::CreateCapability],
        );
        let current = current(approval_only, vec![audience("grpc")], timestamp(1_000));
        let target = create_target(0x44, child_grant(), vec![audience("grpc")], 60);
        assert_eq!(
            prepare(&current, target, timestamp(200)),
            Err(PolicyCode::ApprovalRequired)
        );
    }

    #[test]
    fn normalized_record_is_separate_from_fresh_invocation_identity() {
        let requested_record = NormalizedCapabilityCreateRecord::new(
            database_id(),
            environment(),
            target_principal_id(),
            ActorKind::Agent,
            NonZeroU32::new(60).expect("nonzero duration"),
            vec![audience("grpc")],
            child_grant(),
        )
        .expect("valid requested record");
        let capability_id = target_capability_id(0x54);
        let first = CapabilityCreateTargetFacts::new(
            request_id(0x54),
            capability_id,
            requested_record.clone(),
        );
        let retry = CapabilityCreateTargetFacts::new(
            request_id(0x55),
            capability_id,
            requested_record.clone(),
        );

        assert_ne!(first, retry);
        assert_eq!(first.capability_id(), retry.capability_id());
        assert_eq!(first.normalized_requested_record(), &requested_record);
        assert_eq!(
            first.normalized_requested_record(),
            retry.normalized_requested_record()
        );
    }

    #[test]
    fn administrator_still_rejects_an_unconfigured_target_audience() {
        let administrator = grant(
            TenantScope::Global,
            vec![permission(
                CapabilityPermissionKindV1::AdministerCapabilities,
            )],
            1,
            Vec::new(),
        );
        let current = current(administrator, vec![audience("grpc")], timestamp(1_000));
        let target = create_target(0x56, child_grant(), vec![audience("unconfigured")], 60);
        assert_eq!(
            prepare(&current, target, timestamp(200)),
            Err(PolicyCode::DelegationExceedsAuthority)
        );
    }

    #[test]
    fn administrator_only_selection_does_not_evaluate_subset_authority() {
        let administrator = grant(
            TenantScope::Global,
            vec![permission(
                CapabilityPermissionKindV1::AdministerCapabilities,
            )],
            1,
            Vec::new(),
        );
        let subset_evaluated = Cell::new(false);
        let mode = select_mode(
            &administrator,
            CapabilityPermissionKindV1::CreateCapability,
            || {
                subset_evaluated.set(true);
                false
            },
            || true,
        )
        .expect("administrator path");
        assert_eq!(mode, CapabilityDelegationMode::Administrator);
        assert!(!subset_evaluated.get());
    }

    #[test]
    fn selected_delegation_path_cannot_switch_at_the_final_safe_point() {
        let initial_grant = grant(
            TenantScope::Global,
            vec![
                permission(CapabilityPermissionKindV1::ReadHealth),
                permission(CapabilityPermissionKindV1::CreateCapability),
                permission(CapabilityPermissionKindV1::AdministerCapabilities),
            ],
            100,
            Vec::new(),
        );
        let initial = current(initial_grant, vec![audience("grpc")], timestamp(1_000));
        let target = create_target(0x57, child_grant(), vec![audience("grpc")], 60);
        let preparation = prepare(&initial, target.clone(), timestamp(200)).expect("prepared");
        assert_eq!(
            preparation.delegation_mode(),
            CapabilityDelegationMode::Subset
        );

        let mut transaction_current = initial.clone();
        transaction_current.grant = grant(
            TenantScope::Global,
            vec![
                permission(CapabilityPermissionKindV1::ReadHealth),
                permission(CapabilityPermissionKindV1::AdministerCapabilities),
            ],
            100,
            Vec::new(),
        );
        assert_eq!(
            TransactionCurrentCapabilityVerifier::verify_create(
                &transaction_current,
                timestamp(250),
                preparation,
                ProposedCapabilityCreate::new(target, timestamp(250), timestamp(310),),
            ),
            TransactionCapabilityMutationDecision::Deny(PolicyCode::MissingPermission)
        );
    }

    #[test]
    fn proposal_substitution_rejects_each_create_identity_component() {
        let current = current(ordinary_grant(), vec![audience("grpc")], timestamp(1_000));
        let target = create_target(0x58, child_grant(), vec![audience("grpc")], 60);

        let mut request_changed = target.clone();
        request_changed.request_id = request_id(0x59);
        let mut grant_changed = target.clone();
        grant_changed.normalized_requested_record.grant = grant(
            TenantScope::Tenant(TenantId::new("tenant-a").expect("valid tenant")),
            vec![permission(CapabilityPermissionKindV1::ReadHealth)],
            19,
            Vec::new(),
        );
        let mut duration_changed = target.clone();
        duration_changed
            .normalized_requested_record
            .requested_lifetime_seconds = NonZeroU32::new(61).expect("nonzero duration");
        let mut principal_changed = target.clone();
        principal_changed.normalized_requested_record.principal_id =
            ActorId::new("substituted-principal").expect("valid actor");

        for substituted in [
            request_changed,
            grant_changed,
            duration_changed,
            principal_changed,
        ] {
            let preparation = prepare(&current, target.clone(), timestamp(200)).expect("prepared");
            assert_eq!(
                TransactionCurrentCapabilityVerifier::verify_create(
                    &current,
                    timestamp(250),
                    preparation,
                    ProposedCapabilityCreate::new(substituted, timestamp(250), timestamp(310),),
                ),
                TransactionCapabilityMutationDecision::Deny(PolicyCode::DelegationExceedsAuthority)
            );
        }
    }

    #[test]
    fn valid_create_preparations_cannot_be_cross_paired_with_proposals() {
        let current = current(ordinary_grant(), vec![audience("grpc")], timestamp(1_000));
        let first = create_target(0x5a, child_grant(), vec![audience("grpc")], 60);
        let second = create_target(0x5b, child_grant(), vec![audience("grpc")], 60);
        let first_preparation =
            prepare(&current, first.clone(), timestamp(200)).expect("first prepared");
        let second_preparation =
            prepare(&current, second.clone(), timestamp(200)).expect("second prepared");

        for (preparation, proposed_target) in
            [(first_preparation, second), (second_preparation, first)]
        {
            assert_eq!(
                TransactionCurrentCapabilityVerifier::verify_create(
                    &current,
                    timestamp(250),
                    preparation,
                    ProposedCapabilityCreate::new(proposed_target, timestamp(250), timestamp(310),),
                ),
                TransactionCapabilityMutationDecision::Deny(PolicyCode::DelegationExceedsAuthority)
            );
        }
    }

    #[test]
    fn transaction_current_verifier_rejects_substitution_staleness_and_wrong_time() {
        let current = current(
            ordinary_grant(),
            vec![audience("grpc"), audience("mcp")],
            timestamp(1_000),
        );
        let target = create_target(0x51, child_grant(), vec![audience("grpc")], 60);
        let substituted = create_target(0x52, child_grant(), vec![audience("grpc")], 60);
        let commit_time = timestamp(250);
        let preparation = prepare(&current, target.clone(), timestamp(200)).expect("prepared");
        assert_eq!(
            TransactionCurrentCapabilityVerifier::verify_create(
                &current,
                commit_time,
                preparation,
                ProposedCapabilityCreate::new(substituted, commit_time, timestamp(310),),
            ),
            TransactionCapabilityMutationDecision::Deny(PolicyCode::DelegationExceedsAuthority)
        );

        let preparation = prepare(&current, target.clone(), timestamp(200)).expect("prepared");
        assert_eq!(
            TransactionCurrentCapabilityVerifier::verify_create(
                &current,
                commit_time,
                preparation,
                ProposedCapabilityCreate::new(target.clone(), timestamp(249), timestamp(310),),
            ),
            TransactionCapabilityMutationDecision::Deny(PolicyCode::DelegationExceedsAuthority)
        );

        let preparation = prepare(&current, target.clone(), timestamp(200)).expect("prepared");
        assert_eq!(
            TransactionCurrentCapabilityVerifier::verify_create(
                &current,
                commit_time,
                preparation,
                ProposedCapabilityCreate::new(target.clone(), commit_time, timestamp(311),),
            ),
            TransactionCapabilityMutationDecision::Deny(PolicyCode::DelegationExceedsAuthority)
        );

        let mut stale = current.clone();
        stale.revision = NonZeroU64::new(8).expect("nonzero revision");
        let preparation = prepare(&current, target.clone(), timestamp(200)).expect("prepared");
        assert_eq!(
            TransactionCurrentCapabilityVerifier::verify_create(
                &stale,
                commit_time,
                preparation,
                ProposedCapabilityCreate::new(target, commit_time, timestamp(310),),
            ),
            TransactionCapabilityMutationDecision::Deny(PolicyCode::InactiveOrStaleCapability)
        );
    }

    #[test]
    fn final_safe_point_rechecks_all_current_bindings_permission_and_delegation() {
        let initial = current(
            ordinary_grant(),
            vec![audience("grpc"), audience("mcp")],
            timestamp(1_000),
        );
        let target = create_target(0x53, child_grant(), vec![audience("mcp")], 60);
        let commit_time = timestamp(250);
        let verify = |transaction_current: &TransactionCurrentCapabilityFacts| {
            let preparation = prepare(&initial, target.clone(), timestamp(200)).expect("prepared");
            TransactionCurrentCapabilityVerifier::verify_create(
                transaction_current,
                commit_time,
                preparation,
                ProposedCapabilityCreate::new(target.clone(), commit_time, timestamp(310)),
            )
        };

        let mut inactive_changes = Vec::new();
        let mut changed = initial.clone();
        changed.activity = CapabilityActivity::Revoked;
        inactive_changes.push(changed);
        let mut changed = initial.clone();
        changed.capability_id = target_capability_id(0x61);
        inactive_changes.push(changed);
        let mut changed = initial.clone();
        changed.revision = NonZeroU64::new(8).expect("nonzero revision");
        inactive_changes.push(changed);
        let mut changed = initial.clone();
        changed.principal_id = ActorId::new("different-principal").expect("valid actor");
        inactive_changes.push(changed);
        let mut changed = initial.clone();
        changed.actor_kind = ActorKind::Human;
        inactive_changes.push(changed);
        let mut changed = initial.clone();
        changed.audiences = vec![audience("mcp")];
        inactive_changes.push(changed);
        let mut changed = initial.clone();
        changed.issued_at = timestamp(251);
        inactive_changes.push(changed);
        let mut changed = initial.clone();
        changed.expires_at = commit_time;
        inactive_changes.push(changed);
        let mut changed = initial.clone();
        changed.grant = grant(
            TenantScope::Tenant(TenantId::new("tenant-a").expect("valid tenant")),
            vec![
                permission(CapabilityPermissionKindV1::ReadHealth),
                permission(CapabilityPermissionKindV1::CreateCapability),
            ],
            100,
            Vec::new(),
        );
        inactive_changes.push(changed);

        for changed in inactive_changes {
            assert_eq!(
                verify(&changed),
                TransactionCapabilityMutationDecision::Deny(PolicyCode::InactiveOrStaleCapability)
            );
        }

        let mut missing_permission = initial.clone();
        missing_permission.grant = grant(
            TenantScope::Global,
            vec![permission(CapabilityPermissionKindV1::ReadHealth)],
            100,
            Vec::new(),
        );
        assert_eq!(
            verify(&missing_permission),
            TransactionCapabilityMutationDecision::Deny(PolicyCode::MissingPermission)
        );

        let mut approval_changed = initial.clone();
        approval_changed.grant = grant(
            TenantScope::Global,
            vec![
                permission(CapabilityPermissionKindV1::ReadHealth),
                permission(CapabilityPermissionKindV1::CreateCapability),
            ],
            100,
            vec![CapabilityPermissionKindV1::CreateCapability],
        );
        assert_eq!(
            verify(&approval_changed),
            TransactionCapabilityMutationDecision::Deny(PolicyCode::ApprovalRequired)
        );

        let mut delegation_changes = Vec::new();
        let mut changed = initial.clone();
        changed.database_id =
            DatabaseId::from_unix_milliseconds_and_random(9, [0x62; 10]).expect("valid UUIDv7");
        delegation_changes.push(changed);
        let mut changed = initial.clone();
        changed.environment = Environment::new("other").expect("valid environment");
        delegation_changes.push(changed);
        let mut changed = initial.clone();
        changed.audiences = vec![audience("grpc")];
        delegation_changes.push(changed);
        let mut changed = initial.clone();
        changed.expires_at = timestamp(300);
        delegation_changes.push(changed);
        let mut changed = initial.clone();
        changed.grant = grant(
            TenantScope::Global,
            vec![
                permission(CapabilityPermissionKindV1::ReadHealth),
                permission(CapabilityPermissionKindV1::CreateCapability),
            ],
            10,
            Vec::new(),
        );
        delegation_changes.push(changed);

        for changed in delegation_changes {
            assert_eq!(
                verify(&changed),
                TransactionCapabilityMutationDecision::Deny(PolicyCode::DelegationExceedsAuthority)
            );
        }
    }

    #[test]
    fn complete_grant_subset_preserves_fields_rows_and_inherited_approvals() {
        let lineage = ContractLineage::new("example.contract").expect("valid lineage");
        let read = CapabilityPermissionV1::ReadEntity(lineage.clone(), EntityTypeId::first());
        let parent = CapabilityGrantV1::new(
            TenantScope::Global,
            PartitionScopeV1::All,
            CapabilityPermissionsV1::new(vec![read.clone()]).expect("valid permissions"),
            vec![
                EntityFieldVisibilityV1::new(
                    lineage.clone(),
                    EntityTypeId::first(),
                    vec![FieldId::first(), FieldId::new(2).expect("nonzero field")],
                )
                .expect("valid fields"),
            ],
            NonZeroU16::new(100).expect("nonzero rows"),
            vec![CapabilityPermissionKindV1::ReadEntity],
        )
        .expect("valid grant");
        let child = CapabilityGrantV1::new(
            TenantScope::Tenant(TenantId::new("tenant-a").expect("valid tenant")),
            PartitionScopeV1::All,
            CapabilityPermissionsV1::new(vec![read.clone()]).expect("valid permissions"),
            vec![
                EntityFieldVisibilityV1::new(
                    lineage.clone(),
                    EntityTypeId::first(),
                    vec![FieldId::first()],
                )
                .expect("valid fields"),
            ],
            NonZeroU16::new(50).expect("nonzero rows"),
            vec![CapabilityPermissionKindV1::ReadEntity],
        )
        .expect("valid grant");
        assert!(grant_subset(&child, &parent));

        let missing_inherited_approval = CapabilityGrantV1::new(
            TenantScope::Global,
            PartitionScopeV1::All,
            CapabilityPermissionsV1::new(vec![read.clone()]).expect("valid permissions"),
            Vec::new(),
            NonZeroU16::new(50).expect("nonzero rows"),
            Vec::new(),
        )
        .expect("valid grant");
        assert!(!grant_subset(&missing_inherited_approval, &parent));

        let invisible_field = CapabilityGrantV1::new(
            TenantScope::Global,
            PartitionScopeV1::All,
            CapabilityPermissionsV1::new(vec![read]).expect("valid permissions"),
            vec![
                EntityFieldVisibilityV1::new(
                    lineage,
                    EntityTypeId::first(),
                    vec![FieldId::new(3).expect("nonzero field")],
                )
                .expect("valid fields"),
            ],
            NonZeroU16::new(50).expect("nonzero rows"),
            vec![CapabilityPermissionKindV1::ReadEntity],
        )
        .expect("valid grant");
        assert!(!grant_subset(&invisible_field, &parent));
    }

    /// Secret reveal authority attenuates (ADR-0118): a child may name a
    /// secret field only when its parent's dedicated secret list names it —
    /// the parent's ordinary field list, however complete, is not enough.
    #[test]
    fn delegation_cannot_mint_secret_reveal_authority() {
        let lineage = ContractLineage::new("example.contract").expect("valid lineage");
        let read = CapabilityPermissionV1::ReadEntity(lineage.clone(), EntityTypeId::first());
        let secret = FieldId::new(7).expect("nonzero field");
        let entry = |secret_fields: Vec<FieldId>| {
            EntityFieldVisibilityV1::with_secret_fields(
                lineage.clone(),
                EntityTypeId::first(),
                vec![FieldId::first(), secret],
                secret_fields,
            )
            .expect("valid entry")
        };
        let make = |secret_fields: Vec<FieldId>| {
            CapabilityGrantV1::new(
                TenantScope::Global,
                PartitionScopeV1::All,
                CapabilityPermissionsV1::new(vec![read.clone()]).expect("valid permissions"),
                vec![entry(secret_fields)],
                NonZeroU16::new(50).expect("nonzero rows"),
                Vec::new(),
            )
            .expect("valid grant")
        };
        let parent_without_naming = make(Vec::new());
        let child_naming_secret = make(vec![secret]);
        assert!(
            !grant_subset(&child_naming_secret, &parent_without_naming),
            "a delegation must not mint reveal authority its parent lacks"
        );
        let parent_with_naming = make(vec![secret]);
        assert!(grant_subset(&child_naming_secret, &parent_with_naming));
        assert!(grant_subset(&parent_without_naming, &parent_with_naming));
    }

    #[test]
    fn checked_expiry_preserves_fraction_and_rejects_overflow() {
        let duration = NonZeroU32::new(60).expect("nonzero duration");
        assert_eq!(checked_expiry(timestamp(10), duration), Some(timestamp(70)));
        let maximum = Timestamp::new(i64::MAX, 17).expect("valid timestamp");
        assert_eq!(checked_expiry(maximum, duration), None);
    }
}
