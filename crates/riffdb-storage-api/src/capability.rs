//! Bounded capability records, lookup cross-links, and typed transitions.

use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};

use riffdb_types::{
    ActorId, ActorKind, AdministrationSequence, ApprovalId, Audience, CapabilityId,
    CapabilityTokenDigest, CommandId, ContractLineage, DatabaseId, EntityTypeId, Environment,
    FieldId, IndexId, PartitionKey, ProjectionId, RequestId, TenantScope, Timestamp,
};

use crate::{AuditPrincipalV1, BootstrapServiceAuditStartV1, StorageError, StorageValueError};

/// Maximum audiences retained by one capability.
pub const MAX_CAPABILITY_AUDIENCES: usize = 8;
/// Maximum explicit partition entries retained by one grant.
pub const MAX_CAPABILITY_PARTITIONS: usize = 1_024;
/// Maximum permission atoms retained by one grant.
pub const MAX_CAPABILITY_PERMISSIONS: usize = 8_192;
/// Maximum field-visibility entries and total listed fields.
pub const MAX_CAPABILITY_FIELD_VISIBILITY: usize = 65_535;
/// Maximum requested capability lifetime in seconds.
pub const MAX_CAPABILITY_LIFETIME_SECONDS: u32 = 2_592_000;
/// Maximum semantic bytes in one complete durable capability payload.
pub const MAX_CAPABILITY_PAYLOAD_BYTES: usize = 1024 * 1024;

/// The closed v1 permission registry without its optional stable parameter.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CapabilityPermissionKindV1 {
    /// Validate contract source.
    ValidateContract,
    /// Read active or historical contract metadata.
    ReadContract,
    /// Explain a lineage-scoped command.
    ExplainCommand,
    /// Deploy a contract.
    DeployContract,
    /// Invoke and resolve a lineage-scoped command.
    InvokeCommand,
    /// Read a lineage-scoped entity type.
    ReadEntity,
    /// Scan a lineage-scoped index.
    ScanIndex,
    /// Query a lineage-scoped projection.
    QueryProjection,
    /// Read a lineage-scoped projection status.
    ReadProjectionStatus,
    /// Read one commit.
    ReadCommit,
    /// Scan commits.
    ScanCommits,
    /// Subscribe to commits.
    SubscribeCommits,
    /// Read provenance.
    ReadProvenance,
    /// Inspect outbox status.
    InspectOutbox,
    /// Read server health.
    ReadHealth,
    /// Read server statistics.
    ReadStatistics,
    /// Create a subset capability.
    CreateCapability,
    /// Revoke a subset capability.
    RevokeCapability,
    /// Administer capabilities in this database and environment.
    AdministerCapabilities,
}

impl CapabilityPermissionKindV1 {
    /// Returns the immutable v1 permission tag.
    #[must_use]
    pub const fn tag(self) -> u8 {
        match self {
            Self::ValidateContract => 0x01,
            Self::ReadContract => 0x02,
            Self::ExplainCommand => 0x03,
            Self::DeployContract => 0x04,
            Self::InvokeCommand => 0x05,
            Self::ReadEntity => 0x06,
            Self::ScanIndex => 0x07,
            Self::QueryProjection => 0x08,
            Self::ReadProjectionStatus => 0x09,
            Self::ReadCommit => 0x0a,
            Self::ScanCommits => 0x0b,
            Self::SubscribeCommits => 0x0c,
            Self::ReadProvenance => 0x0d,
            Self::InspectOutbox => 0x0e,
            Self::ReadHealth => 0x0f,
            Self::ReadStatistics => 0x10,
            Self::CreateCapability => 0x11,
            Self::RevokeCapability => 0x12,
            Self::AdministerCapabilities => 0x13,
        }
    }

    /// Decodes a stable v1 permission tag.
    #[must_use]
    pub const fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            0x01 => Some(Self::ValidateContract),
            0x02 => Some(Self::ReadContract),
            0x03 => Some(Self::ExplainCommand),
            0x04 => Some(Self::DeployContract),
            0x05 => Some(Self::InvokeCommand),
            0x06 => Some(Self::ReadEntity),
            0x07 => Some(Self::ScanIndex),
            0x08 => Some(Self::QueryProjection),
            0x09 => Some(Self::ReadProjectionStatus),
            0x0a => Some(Self::ReadCommit),
            0x0b => Some(Self::ScanCommits),
            0x0c => Some(Self::SubscribeCommits),
            0x0d => Some(Self::ReadProvenance),
            0x0e => Some(Self::InspectOutbox),
            0x0f => Some(Self::ReadHealth),
            0x10 => Some(Self::ReadStatistics),
            0x11 => Some(Self::CreateCapability),
            0x12 => Some(Self::RevokeCapability),
            0x13 => Some(Self::AdministerCapabilities),
            _ => None,
        }
    }

    const fn requires_parameter(self) -> bool {
        matches!(
            self,
            Self::ExplainCommand
                | Self::InvokeCommand
                | Self::ReadEntity
                | Self::ScanIndex
                | Self::QueryProjection
                | Self::ReadProjectionStatus
        )
    }
}

/// One canonical permission atom with any required lineage-scoped ID.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CapabilityPermissionV1 {
    /// An unparameterized permission.
    Unparameterized(CapabilityPermissionKindV1),
    /// Explain a lineage-scoped command.
    ExplainCommand(ContractLineage, CommandId),
    /// Invoke a lineage-scoped command.
    InvokeCommand(ContractLineage, CommandId),
    /// Read a lineage-scoped entity type.
    ReadEntity(ContractLineage, EntityTypeId),
    /// Scan a lineage-scoped index.
    ScanIndex(ContractLineage, IndexId),
    /// Query a lineage-scoped projection.
    QueryProjection(ContractLineage, ProjectionId),
    /// Read status for a lineage-scoped projection.
    ReadProjectionStatus(ContractLineage, ProjectionId),
}

impl CapabilityPermissionV1 {
    /// Constructs an unparameterized atom, rejecting parameterized kinds.
    pub fn unparameterized(kind: CapabilityPermissionKindV1) -> Result<Self, StorageValueError> {
        if kind.requires_parameter() {
            return Err(StorageValueError::InvalidShape);
        }
        Ok(Self::Unparameterized(kind))
    }

    /// Returns the stable permission kind.
    #[must_use]
    pub const fn kind(&self) -> CapabilityPermissionKindV1 {
        match self {
            Self::Unparameterized(kind) => *kind,
            Self::ExplainCommand(..) => CapabilityPermissionKindV1::ExplainCommand,
            Self::InvokeCommand(..) => CapabilityPermissionKindV1::InvokeCommand,
            Self::ReadEntity(..) => CapabilityPermissionKindV1::ReadEntity,
            Self::ScanIndex(..) => CapabilityPermissionKindV1::ScanIndex,
            Self::QueryProjection(..) => CapabilityPermissionKindV1::QueryProjection,
            Self::ReadProjectionStatus(..) => CapabilityPermissionKindV1::ReadProjectionStatus,
        }
    }

    /// Returns the canonical comparison key.
    #[must_use]
    pub fn canonical_key(&self) -> Vec<u8> {
        let mut bytes = vec![self.kind().tag()];
        match self {
            Self::Unparameterized(_) => {}
            Self::ExplainCommand(lineage, id) | Self::InvokeCommand(lineage, id) => {
                append_lineage(&mut bytes, lineage);
                bytes.extend_from_slice(&id.to_be_bytes());
            }
            Self::ReadEntity(lineage, id) => {
                append_lineage(&mut bytes, lineage);
                bytes.extend_from_slice(&id.to_be_bytes());
            }
            Self::ScanIndex(lineage, id) => {
                append_lineage(&mut bytes, lineage);
                bytes.extend_from_slice(&id.to_be_bytes());
            }
            Self::QueryProjection(lineage, id) | Self::ReadProjectionStatus(lineage, id) => {
                append_lineage(&mut bytes, lineage);
                bytes.extend_from_slice(&id.to_be_bytes());
            }
        }
        bytes
    }
}

/// Canonical permission set.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapabilityPermissionsV1(Vec<CapabilityPermissionV1>);

impl CapabilityPermissionsV1 {
    /// Sorts by canonical bytes and rejects duplicates or excessive counts.
    ///
    /// An empty set is a valid grant with no operation permission. Bootstrap
    /// separately requires the administration permission.
    pub fn new(mut values: Vec<CapabilityPermissionV1>) -> Result<Self, StorageValueError> {
        if values.len() > MAX_CAPABILITY_PERMISSIONS {
            return Err(StorageValueError::LimitExceeded);
        }
        validate_capability_payload_bytes(permission_set_semantic_bytes(&values)?)?;
        values.sort_by_key(CapabilityPermissionV1::canonical_key);
        if values
            .windows(2)
            .any(|pair| pair[0].canonical_key() == pair[1].canonical_key())
        {
            return Err(StorageValueError::Duplicate);
        }
        Ok(Self(values))
    }

    /// Returns atoms in canonical order.
    #[must_use]
    pub fn as_slice(&self) -> &[CapabilityPermissionV1] {
        &self.0
    }

    /// Returns whether any atom has the supplied permission kind.
    #[must_use]
    pub fn contains_kind(&self, kind: CapabilityPermissionKindV1) -> bool {
        self.0.iter().any(|permission| permission.kind() == kind)
    }
}

/// One lineage-scoped complete partition key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScopedPartitionV1 {
    lineage: ContractLineage,
    partition_key: PartitionKey,
}

impl ScopedPartitionV1 {
    /// Constructs a scoped partition identity.
    #[must_use]
    pub const fn new(lineage: ContractLineage, partition_key: PartitionKey) -> Self {
        Self {
            lineage,
            partition_key,
        }
    }

    /// Returns the canonical comparison key.
    #[must_use]
    pub fn canonical_key(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        append_lineage(&mut bytes, &self.lineage);
        append_bytes(&mut bytes, self.partition_key.as_bytes());
        bytes
    }

    /// Returns the exact contract lineage.
    #[must_use]
    pub const fn lineage(&self) -> &ContractLineage {
        &self.lineage
    }

    /// Returns the complete partition key.
    #[must_use]
    pub const fn partition_key(&self) -> &PartitionKey {
        &self.partition_key
    }
}

/// Closed partition scope in one capability grant.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PartitionScopeV1 {
    /// Every partition within otherwise authorized contract operations.
    All,
    /// A nonempty canonical set of lineage-scoped complete keys.
    Explicit(Vec<ScopedPartitionV1>),
}

impl PartitionScopeV1 {
    /// Returns the stable v1 semantic tag.
    #[must_use]
    pub const fn tag(&self) -> u8 {
        match self {
            Self::All => 0x01,
            Self::Explicit(_) => 0x02,
        }
    }

    /// Constructs a canonical explicit scope.
    pub fn explicit(mut values: Vec<ScopedPartitionV1>) -> Result<Self, StorageValueError> {
        if values.is_empty() {
            return Err(StorageValueError::Empty);
        }
        if values.len() > MAX_CAPABILITY_PARTITIONS {
            return Err(StorageValueError::LimitExceeded);
        }
        validate_capability_payload_bytes(explicit_partitions_semantic_bytes(&values)?)?;
        values.sort_by_key(ScopedPartitionV1::canonical_key);
        if values
            .windows(2)
            .any(|pair| pair[0].canonical_key() == pair[1].canonical_key())
        {
            return Err(StorageValueError::Duplicate);
        }
        Ok(Self::Explicit(values))
    }

    /// Returns canonical explicit entries, or `None` for all partitions.
    #[must_use]
    pub fn explicit_entries(&self) -> Option<&[ScopedPartitionV1]> {
        match self {
            Self::All => None,
            Self::Explicit(entries) => Some(entries),
        }
    }
}

/// One canonical entity-field visibility entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EntityFieldVisibilityV1 {
    lineage: ContractLineage,
    entity_type: EntityTypeId,
    fields: Vec<FieldId>,
}

impl EntityFieldVisibilityV1 {
    /// Sorts and validates a nonempty field set.
    pub fn new(
        lineage: ContractLineage,
        entity_type: EntityTypeId,
        mut fields: Vec<FieldId>,
    ) -> Result<Self, StorageValueError> {
        if fields.is_empty() {
            return Err(StorageValueError::Empty);
        }
        if fields.len() > MAX_CAPABILITY_FIELD_VISIBILITY {
            return Err(StorageValueError::LimitExceeded);
        }
        fields.sort_unstable();
        if fields.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(StorageValueError::Duplicate);
        }
        Ok(Self {
            lineage,
            entity_type,
            fields,
        })
    }

    fn canonical_key(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        append_lineage(&mut bytes, &self.lineage);
        bytes.extend_from_slice(&self.entity_type.to_be_bytes());
        bytes
    }

    /// Returns the exact contract lineage.
    #[must_use]
    pub const fn lineage(&self) -> &ContractLineage {
        &self.lineage
    }

    /// Returns the stable entity type identity.
    #[must_use]
    pub const fn entity_type(&self) -> EntityTypeId {
        self.entity_type
    }

    /// Returns visible stable fields in increasing order.
    #[must_use]
    pub fn fields(&self) -> &[FieldId] {
        &self.fields
    }
}

/// Complete bounded v1 grant persisted with a capability.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapabilityGrantV1 {
    tenant_scope: TenantScope,
    partition_scope: PartitionScopeV1,
    permissions: CapabilityPermissionsV1,
    field_visibility: Vec<EntityFieldVisibilityV1>,
    max_scan_rows: NonZeroU16,
    approval_required: Vec<CapabilityPermissionKindV1>,
}

impl CapabilityGrantV1 {
    /// Constructs a canonically ordered complete grant.
    pub fn new(
        tenant_scope: TenantScope,
        partition_scope: PartitionScopeV1,
        permissions: CapabilityPermissionsV1,
        mut field_visibility: Vec<EntityFieldVisibilityV1>,
        max_scan_rows: NonZeroU16,
        mut approval_required: Vec<CapabilityPermissionKindV1>,
    ) -> Result<Self, StorageValueError> {
        if usize::from(max_scan_rows.get()) > 500
            || field_visibility.len() > MAX_CAPABILITY_FIELD_VISIBILITY
            || approval_required.len() > 19
        {
            return Err(StorageValueError::LimitExceeded);
        }
        let unchecked_grant_size = capability_grant_semantic_bytes_parts(
            &tenant_scope,
            &partition_scope,
            &permissions,
            &field_visibility,
            &approval_required,
        )?;
        validate_capability_payload_bytes(unchecked_grant_size)?;
        field_visibility.sort_by_key(EntityFieldVisibilityV1::canonical_key);
        if field_visibility
            .windows(2)
            .any(|pair| pair[0].canonical_key() == pair[1].canonical_key())
        {
            return Err(StorageValueError::Duplicate);
        }
        let total_fields = field_visibility.iter().try_fold(0usize, |count, entry| {
            count
                .checked_add(entry.fields.len())
                .ok_or(StorageValueError::SizeOverflow)
        })?;
        if total_fields > MAX_CAPABILITY_FIELD_VISIBILITY {
            return Err(StorageValueError::LimitExceeded);
        }
        approval_required.sort_unstable();
        if approval_required.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(StorageValueError::Duplicate);
        }
        let value = Self {
            tenant_scope,
            partition_scope,
            permissions,
            field_visibility,
            max_scan_rows,
            approval_required,
        };
        validate_capability_payload_bytes(capability_grant_semantic_bytes(&value)?)?;
        Ok(value)
    }

    /// Returns the authorization-resolved tenant constraint.
    #[must_use]
    pub const fn tenant_scope(&self) -> &TenantScope {
        &self.tenant_scope
    }

    /// Returns the complete partition constraint.
    #[must_use]
    pub const fn partition_scope(&self) -> &PartitionScopeV1 {
        &self.partition_scope
    }

    /// Returns the canonical permission set.
    #[must_use]
    pub const fn permissions(&self) -> &CapabilityPermissionsV1 {
        &self.permissions
    }

    /// Returns canonical entity-field visibility entries.
    #[must_use]
    pub fn field_visibility(&self) -> &[EntityFieldVisibilityV1] {
        &self.field_visibility
    }

    /// Returns the bounded maximum scan rows.
    #[must_use]
    pub const fn max_scan_rows(&self) -> NonZeroU16 {
        self.max_scan_rows
    }

    /// Returns canonical permission kinds requiring validated approval.
    #[must_use]
    pub fn approval_required(&self) -> &[CapabilityPermissionKindV1] {
        &self.approval_required
    }
}

/// Canonical request-owned record used to detect create/bootstrap replay.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapabilityRequestedRecordV1 {
    database_id: DatabaseId,
    environment: Environment,
    principal_id: ActorId,
    actor_kind: ActorKind,
    duration_seconds: NonZeroU32,
    audiences: Vec<Audience>,
    grant: CapabilityGrantV1,
}

impl CapabilityRequestedRecordV1 {
    /// Constructs and canonically orders the normalized requested record.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        database_id: DatabaseId,
        environment: Environment,
        principal_id: ActorId,
        actor_kind: ActorKind,
        duration_seconds: NonZeroU32,
        mut audiences: Vec<Audience>,
        grant: CapabilityGrantV1,
    ) -> Result<Self, StorageValueError> {
        if audiences.is_empty() {
            return Err(StorageValueError::Empty);
        }
        if duration_seconds.get() > MAX_CAPABILITY_LIFETIME_SECONDS
            || audiences.len() > MAX_CAPABILITY_AUDIENCES
        {
            return Err(StorageValueError::LimitExceeded);
        }
        audiences.sort_unstable();
        if audiences.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(StorageValueError::Duplicate);
        }
        let value = Self {
            database_id,
            environment,
            principal_id,
            actor_kind,
            duration_seconds,
            audiences,
            grant,
        };
        validate_capability_payload_bytes(
            maximum_stored_capability_semantic_bytes_from_requested(&value)?,
        )?;
        Ok(value)
    }

    /// Returns the permanent database identity.
    #[must_use]
    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    /// Returns the exact environment.
    #[must_use]
    pub const fn environment(&self) -> &Environment {
        &self.environment
    }

    /// Returns the target stable principal.
    #[must_use]
    pub const fn principal_id(&self) -> &ActorId {
        &self.principal_id
    }

    /// Returns the target actor class.
    #[must_use]
    pub const fn actor_kind(&self) -> ActorKind {
        self.actor_kind
    }

    /// Returns the requested exact duration.
    #[must_use]
    pub const fn duration_seconds(&self) -> NonZeroU32 {
        self.duration_seconds
    }

    /// Returns configured audience selections in canonical order.
    #[must_use]
    pub fn audiences(&self) -> &[Audience] {
        &self.audiences
    }

    /// Returns the complete requested grant.
    #[must_use]
    pub const fn grant(&self) -> &CapabilityGrantV1 {
        &self.grant
    }

    /// Returns the conservative durable payload size including revoked shape.
    pub fn maximum_stored_semantic_bytes(&self) -> Result<usize, StorageValueError> {
        maximum_stored_capability_semantic_bytes_from_requested(self)
    }
}

/// Closed irreversible capability lifecycle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CapabilityLifecycleV1 {
    /// Capability may authenticate subject to time and current policy.
    Active,
    /// Capability was irreversibly revoked.
    Revoked {
        /// Exact transaction-current revocation time.
        revoked_at: Timestamp,
        /// Administration sequence of the revoke transition.
        administration_sequence: AdministrationSequence,
        /// Closed safe reason.
        reason: RevocationReasonCodeV1,
    },
}

impl CapabilityLifecycleV1 {
    /// Returns the stable v1 lifecycle tag.
    #[must_use]
    pub const fn tag(&self) -> u8 {
        match self {
            Self::Active => 0x01,
            Self::Revoked { .. } => 0x02,
        }
    }
}

/// Closed revocation reason registry.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum RevocationReasonCodeV1 {
    /// Explicit operator request.
    Requested,
    /// Capability was replaced.
    Replaced,
    /// Suspected credential compromise.
    SuspectedCompromise,
    /// Current policy changed.
    PolicyChange,
}

impl RevocationReasonCodeV1 {
    /// Returns the immutable v1 tag.
    #[must_use]
    pub const fn tag(self) -> u8 {
        match self {
            Self::Requested => 0x01,
            Self::Replaced => 0x02,
            Self::SuspectedCompromise => 0x03,
            Self::PolicyChange => 0x04,
        }
    }

    /// Decodes a stable v1 revocation-reason tag.
    #[must_use]
    pub const fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            0x01 => Some(Self::Requested),
            0x02 => Some(Self::Replaced),
            0x03 => Some(Self::SuspectedCompromise),
            0x04 => Some(Self::PolicyChange),
            _ => None,
        }
    }
}

/// Complete durable capability record keyed by stable capability ID.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredCapabilityRecordV1 {
    capability_id: CapabilityId,
    revision: NonZeroU64,
    token_digest: CapabilityTokenDigest,
    database_id: DatabaseId,
    environment: Environment,
    principal_id: ActorId,
    actor_kind: ActorKind,
    audiences: Vec<Audience>,
    issued_at: Timestamp,
    expires_at: Timestamp,
    creation_sequence: AdministrationSequence,
    creation_request_id: RequestId,
    grant: CapabilityGrantV1,
    lifecycle: CapabilityLifecycleV1,
}

impl StoredCapabilityRecordV1 {
    /// Reconstructs and validates the exact durable semantic record.
    #[allow(clippy::too_many_arguments)]
    pub fn from_stored_parts(
        capability_id: CapabilityId,
        revision: NonZeroU64,
        token_digest: CapabilityTokenDigest,
        database_id: DatabaseId,
        environment: Environment,
        principal_id: ActorId,
        actor_kind: ActorKind,
        audiences: Vec<Audience>,
        issued_at: Timestamp,
        expires_at: Timestamp,
        creation_sequence: AdministrationSequence,
        creation_request_id: RequestId,
        grant: CapabilityGrantV1,
        lifecycle: CapabilityLifecycleV1,
    ) -> Result<Self, StorageValueError> {
        validate_capability_audiences(&audiences)?;
        validate_stored_capability_interval(issued_at, expires_at)?;
        let valid_lifecycle = match lifecycle {
            CapabilityLifecycleV1::Active => revision.get() == 1,
            CapabilityLifecycleV1::Revoked {
                administration_sequence,
                ..
            } => revision.get() == 2 && administration_sequence > creation_sequence,
        };
        if !valid_lifecycle {
            return Err(StorageValueError::InvalidShape);
        }
        validate_capability_payload_bytes(stored_capability_semantic_bytes(
            &environment,
            &principal_id,
            &audiences,
            &grant,
            &lifecycle,
        )?)?;
        Ok(Self {
            capability_id,
            revision,
            token_digest,
            database_id,
            environment,
            principal_id,
            actor_kind,
            audiences,
            issued_at,
            expires_at,
            creation_sequence,
            creation_request_id,
            grant,
            lifecycle,
        })
    }

    /// Constructs a newly active revision-one record.
    #[allow(clippy::too_many_arguments)]
    pub fn active(
        capability_id: CapabilityId,
        token_digest: CapabilityTokenDigest,
        requested: CapabilityRequestedRecordV1,
        issued_at: Timestamp,
        expires_at: Timestamp,
        creation_sequence: AdministrationSequence,
        creation_request_id: RequestId,
    ) -> Result<Self, StorageValueError> {
        if !timestamp_interval_matches(issued_at, expires_at, requested.duration_seconds.get()) {
            return Err(StorageValueError::InvalidShape);
        }
        let CapabilityRequestedRecordV1 {
            database_id,
            environment,
            principal_id,
            actor_kind,
            duration_seconds: _,
            audiences,
            grant,
        } = requested;
        Self::from_stored_parts(
            capability_id,
            NonZeroU64::MIN,
            token_digest,
            database_id,
            environment,
            principal_id,
            actor_kind,
            audiences,
            issued_at,
            expires_at,
            creation_sequence,
            creation_request_id,
            grant,
            CapabilityLifecycleV1::Active,
        )
    }

    /// Returns the stable capability identity.
    #[must_use]
    pub const fn capability_id(&self) -> CapabilityId {
        self.capability_id
    }

    /// Returns the nonzero lifecycle revision.
    #[must_use]
    pub const fn revision(&self) -> NonZeroU64 {
        self.revision
    }

    /// Returns the nonsecret typed digest reference.
    #[must_use]
    pub const fn token_digest(&self) -> CapabilityTokenDigest {
        self.token_digest
    }

    /// Returns the permanent database identity.
    #[must_use]
    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    /// Returns the exact environment.
    #[must_use]
    pub const fn environment(&self) -> &Environment {
        &self.environment
    }

    /// Returns the stable target principal.
    #[must_use]
    pub const fn principal_id(&self) -> &ActorId {
        &self.principal_id
    }

    /// Returns the target actor class.
    #[must_use]
    pub const fn actor_kind(&self) -> ActorKind {
        self.actor_kind
    }

    /// Returns configured audiences in canonical order.
    #[must_use]
    pub fn audiences(&self) -> &[Audience] {
        &self.audiences
    }

    /// Returns issue time.
    #[must_use]
    pub const fn issued_at(&self) -> Timestamp {
        self.issued_at
    }

    /// Returns exclusive expiry time.
    #[must_use]
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }

    /// Returns the creating administration sequence.
    #[must_use]
    pub const fn creation_sequence(&self) -> AdministrationSequence {
        self.creation_sequence
    }

    /// Returns the original create invocation request ID.
    #[must_use]
    pub const fn creation_request_id(&self) -> RequestId {
        self.creation_request_id
    }

    /// Returns the complete durable grant.
    #[must_use]
    pub const fn grant(&self) -> &CapabilityGrantV1 {
        &self.grant
    }

    /// Returns the irreversible lifecycle.
    #[must_use]
    pub const fn lifecycle(&self) -> &CapabilityLifecycleV1 {
        &self.lifecycle
    }

    /// Returns the complete checked durable semantic payload size.
    pub fn semantic_bytes(&self) -> Result<usize, StorageValueError> {
        stored_capability_semantic_bytes(
            &self.environment,
            &self.principal_id,
            &self.audiences,
            &self.grant,
            &self.lifecycle,
        )
    }

    /// Produces the sole irreversible active-to-revoked durable post-image.
    pub fn revoked(
        &self,
        expected_revision: NonZeroU64,
        revoked_at: Timestamp,
        administration_sequence: AdministrationSequence,
        reason: RevocationReasonCodeV1,
    ) -> Result<Self, StorageValueError> {
        if self.revision != expected_revision
            || !matches!(self.lifecycle, CapabilityLifecycleV1::Active)
        {
            return Err(StorageValueError::InvalidShape);
        }
        let revision = self
            .revision
            .get()
            .checked_add(1)
            .and_then(NonZeroU64::new)
            .ok_or(StorageValueError::SizeOverflow)?;
        Self::from_stored_parts(
            self.capability_id,
            revision,
            self.token_digest,
            self.database_id,
            self.environment.clone(),
            self.principal_id.clone(),
            self.actor_kind,
            self.audiences.clone(),
            self.issued_at,
            self.expires_at,
            self.creation_sequence,
            self.creation_request_id,
            self.grant.clone(),
            CapabilityLifecycleV1::Revoked {
                revoked_at,
                administration_sequence,
                reason,
            },
        )
    }

    /// Compares this exact durable record with a normalized create identity.
    ///
    /// Requested duration is not persisted as a duplicate durable field; it is
    /// recovered exactly from the checked issue/expiry interval.
    #[must_use]
    pub fn matches_requested(&self, requested: &CapabilityRequestedRecordV1) -> bool {
        self.database_id == requested.database_id
            && self.environment == requested.environment
            && self.principal_id == requested.principal_id
            && self.actor_kind == requested.actor_kind
            && self.audiences == requested.audiences
            && self.grant == requested.grant
            && timestamp_interval_matches(
                self.issued_at,
                self.expires_at,
                requested.duration_seconds.get(),
            )
    }
}

/// Digest-index value containing exactly one stable capability ID.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CapabilityTokenLookupV1 {
    capability_id: CapabilityId,
}

impl CapabilityTokenLookupV1 {
    /// Constructs the reciprocal lookup value.
    #[must_use]
    pub const fn new(capability_id: CapabilityId) -> Self {
        Self { capability_id }
    }

    /// Returns the referenced capability identity.
    #[must_use]
    pub const fn capability_id(self) -> CapabilityId {
        self.capability_id
    }
}

/// Singleton durable marker retained after bootstrap capability revocation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CapabilityBootstrapMarkerV1 {
    database_id: DatabaseId,
    capability_id: CapabilityId,
    administration_sequence: AdministrationSequence,
}

impl CapabilityBootstrapMarkerV1 {
    /// Constructs the exact bootstrap transition cross-link.
    #[must_use]
    pub const fn new(
        database_id: DatabaseId,
        capability_id: CapabilityId,
        administration_sequence: AdministrationSequence,
    ) -> Self {
        Self {
            database_id,
            capability_id,
            administration_sequence,
        }
    }

    /// Returns the permanent database identity.
    #[must_use]
    pub const fn database_id(self) -> DatabaseId {
        self.database_id
    }

    /// Returns the bootstrap capability identity.
    #[must_use]
    pub const fn capability_id(self) -> CapabilityId {
        self.capability_id
    }

    /// Returns the authoritative bootstrap transition sequence.
    #[must_use]
    pub const fn administration_sequence(self) -> AdministrationSequence {
        self.administration_sequence
    }
}

/// Capability administration operation retained in the shared audit stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CapabilityAdministrationOperationV1 {
    /// First database capability.
    Bootstrap,
    /// Normal delegated creation.
    Create,
    /// Irreversible revocation.
    Revoke,
}

impl CapabilityAdministrationOperationV1 {
    /// Returns the stable capability-administration operation tag.
    #[must_use]
    pub const fn tag(self) -> u8 {
        match self {
            Self::Bootstrap => 0x01,
            Self::Create => 0x02,
            Self::Revoke => 0x03,
        }
    }

    /// Decodes a stable operation tag.
    #[must_use]
    pub const fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            0x01 => Some(Self::Bootstrap),
            0x02 => Some(Self::Create),
            0x03 => Some(Self::Revoke),
            _ => None,
        }
    }
}

/// Durable capability-transition audit record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredCapabilityAdministrationV1 {
    administration_sequence: AdministrationSequence,
    request_id: RequestId,
    operation: CapabilityAdministrationOperationV1,
    timestamp: Timestamp,
    initiator: Option<AuditPrincipalV1>,
    target_capability_id: CapabilityId,
    resulting_revision: NonZeroU64,
    approval_id: Option<ApprovalId>,
    revocation_reason: Option<RevocationReasonCodeV1>,
}

impl StoredCapabilityAdministrationV1 {
    /// Constructs a checked closed transition record.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        administration_sequence: AdministrationSequence,
        request_id: RequestId,
        operation: CapabilityAdministrationOperationV1,
        timestamp: Timestamp,
        initiator: Option<AuditPrincipalV1>,
        target_capability_id: CapabilityId,
        resulting_revision: NonZeroU64,
        approval_id: Option<ApprovalId>,
        revocation_reason: Option<RevocationReasonCodeV1>,
    ) -> Result<Self, StorageValueError> {
        let valid = match operation {
            CapabilityAdministrationOperationV1::Bootstrap => {
                initiator.is_none() && revocation_reason.is_none() && resulting_revision.get() == 1
            }
            CapabilityAdministrationOperationV1::Create => {
                initiator.is_some() && revocation_reason.is_none() && resulting_revision.get() == 1
            }
            CapabilityAdministrationOperationV1::Revoke => {
                initiator.is_some() && revocation_reason.is_some() && resulting_revision.get() == 2
            }
        };
        if !valid {
            return Err(StorageValueError::InvalidShape);
        }
        Ok(Self {
            administration_sequence,
            request_id,
            operation,
            timestamp,
            initiator,
            target_capability_id,
            resulting_revision,
            approval_id,
            revocation_reason,
        })
    }

    /// Returns the assigned shared administration sequence.
    #[must_use]
    pub const fn administration_sequence(&self) -> AdministrationSequence {
        self.administration_sequence
    }

    /// Returns the invocation request identity.
    #[must_use]
    pub const fn request_id(&self) -> RequestId {
        self.request_id
    }

    /// Returns the closed capability transition operation.
    #[must_use]
    pub const fn operation(&self) -> CapabilityAdministrationOperationV1 {
        self.operation
    }

    /// Returns the coordinator-observed transition timestamp.
    #[must_use]
    pub const fn timestamp(&self) -> Timestamp {
        self.timestamp
    }

    /// Returns the authenticated initiator, absent only for bootstrap.
    #[must_use]
    pub const fn initiator(&self) -> Option<&AuditPrincipalV1> {
        self.initiator.as_ref()
    }

    /// Returns the target capability.
    #[must_use]
    pub const fn target_capability_id(&self) -> CapabilityId {
        self.target_capability_id
    }

    /// Returns the resulting capability revision.
    #[must_use]
    pub const fn resulting_revision(&self) -> NonZeroU64 {
        self.resulting_revision
    }

    /// Returns optional policy-validated approval identity.
    #[must_use]
    pub const fn approval_id(&self) -> Option<&ApprovalId> {
        self.approval_id.as_ref()
    }

    /// Returns the closed reason, present only for revoke.
    #[must_use]
    pub const fn revocation_reason(&self) -> Option<RevocationReasonCodeV1> {
        self.revocation_reason
    }
}

/// Normal capability create request after policy and time verification.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapabilityCreateIntentV1 {
    capability_id: CapabilityId,
    request_id: RequestId,
    requested: CapabilityRequestedRecordV1,
    token_digest: CapabilityTokenDigest,
    issued_at: Timestamp,
    expires_at: Timestamp,
    initiator: AuditPrincipalV1,
    approval_id: Option<ApprovalId>,
}

impl CapabilityCreateIntentV1 {
    /// Constructs an exact transaction-current create interval.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        capability_id: CapabilityId,
        request_id: RequestId,
        requested: CapabilityRequestedRecordV1,
        token_digest: CapabilityTokenDigest,
        issued_at: Timestamp,
        expires_at: Timestamp,
        initiator: AuditPrincipalV1,
        approval_id: Option<ApprovalId>,
    ) -> Result<Self, StorageValueError> {
        if !timestamp_interval_matches(issued_at, expires_at, requested.duration_seconds.get()) {
            return Err(StorageValueError::InvalidShape);
        }
        Ok(Self {
            capability_id,
            request_id,
            requested,
            token_digest,
            issued_at,
            expires_at,
            initiator,
            approval_id,
        })
    }

    /// Returns the caller-selected stable capability ID.
    #[must_use]
    pub const fn capability_id(&self) -> CapabilityId {
        self.capability_id
    }

    /// Returns the invocation request ID.
    #[must_use]
    pub const fn request_id(&self) -> RequestId {
        self.request_id
    }

    /// Returns the normalized requested record.
    #[must_use]
    pub const fn requested(&self) -> &CapabilityRequestedRecordV1 {
        &self.requested
    }

    /// Returns the candidate's typed digest reference.
    #[must_use]
    pub const fn token_digest(&self) -> CapabilityTokenDigest {
        self.token_digest
    }

    /// Returns transaction-current issue time.
    #[must_use]
    pub const fn issued_at(&self) -> Timestamp {
        self.issued_at
    }

    /// Returns checked exclusive expiry time.
    #[must_use]
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }

    /// Returns the transaction-current authorizing principal.
    #[must_use]
    pub const fn initiator(&self) -> &AuditPrincipalV1 {
        &self.initiator
    }

    /// Returns optional validated approval.
    #[must_use]
    pub const fn approval_id(&self) -> Option<&ApprovalId> {
        self.approval_id.as_ref()
    }

    /// Returns whether every pre-time field equals the opened candidate.
    #[must_use]
    pub fn matches_candidate(&self, candidate: &CapabilityCreateCandidateV1) -> bool {
        self.capability_id == candidate.capability_id
            && self.request_id == candidate.request_id
            && self.requested == candidate.requested
            && self.token_digest == candidate.token_digest
            && self.initiator == candidate.initiator
            && self.approval_id == candidate.approval_id
    }
}

/// Normal irreversible revocation request after transaction-current policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapabilityRevokeIntentV1 {
    capability_id: CapabilityId,
    expected_revision: NonZeroU64,
    request_id: RequestId,
    revoked_at: Timestamp,
    initiator: AuditPrincipalV1,
    approval_id: Option<ApprovalId>,
    reason: RevocationReasonCodeV1,
}

impl CapabilityRevokeIntentV1 {
    /// Constructs a complete checked revocation request.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub const fn new(
        capability_id: CapabilityId,
        expected_revision: NonZeroU64,
        request_id: RequestId,
        revoked_at: Timestamp,
        initiator: AuditPrincipalV1,
        approval_id: Option<ApprovalId>,
        reason: RevocationReasonCodeV1,
    ) -> Self {
        Self {
            capability_id,
            expected_revision,
            request_id,
            revoked_at,
            initiator,
            approval_id,
            reason,
        }
    }

    /// Returns the target capability identity.
    #[must_use]
    pub const fn capability_id(&self) -> CapabilityId {
        self.capability_id
    }

    /// Returns the expected current revision.
    #[must_use]
    pub const fn expected_revision(&self) -> NonZeroU64 {
        self.expected_revision
    }

    /// Returns revocation request ID.
    #[must_use]
    pub const fn request_id(&self) -> RequestId {
        self.request_id
    }

    /// Returns the exact verifier time reused for the transition.
    #[must_use]
    pub const fn revoked_at(&self) -> Timestamp {
        self.revoked_at
    }

    /// Returns the authorizing principal.
    #[must_use]
    pub const fn initiator(&self) -> &AuditPrincipalV1 {
        &self.initiator
    }

    /// Returns optional validated approval.
    #[must_use]
    pub const fn approval_id(&self) -> Option<&ApprovalId> {
        self.approval_id.as_ref()
    }

    /// Returns the closed revocation reason.
    #[must_use]
    pub const fn reason(&self) -> RevocationReasonCodeV1 {
        self.reason
    }

    /// Returns whether every pre-time field equals the opened candidate.
    #[must_use]
    pub fn matches_candidate(&self, candidate: &CapabilityRevokeCandidateV1) -> bool {
        self.capability_id == candidate.capability_id
            && self.request_id == candidate.request_id
            && self.initiator == candidate.initiator
            && self.approval_id == candidate.approval_id
            && self.reason == candidate.reason
    }
}

/// Pre-time normal-create candidate used to open a short transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapabilityCreateCandidateV1 {
    capability_id: CapabilityId,
    request_id: RequestId,
    requested: CapabilityRequestedRecordV1,
    token_digest: CapabilityTokenDigest,
    initiator: AuditPrincipalV1,
    approval_id: Option<ApprovalId>,
}

impl CapabilityCreateCandidateV1 {
    /// Constructs the value-only candidate before the final authorization clock.
    #[must_use]
    pub const fn new(
        capability_id: CapabilityId,
        request_id: RequestId,
        requested: CapabilityRequestedRecordV1,
        token_digest: CapabilityTokenDigest,
        initiator: AuditPrincipalV1,
        approval_id: Option<ApprovalId>,
    ) -> Self {
        Self {
            capability_id,
            request_id,
            requested,
            token_digest,
            initiator,
            approval_id,
        }
    }

    /// Returns the caller-selected target identity.
    #[must_use]
    pub const fn capability_id(&self) -> CapabilityId {
        self.capability_id
    }

    /// Returns the fresh invocation request identity.
    #[must_use]
    pub const fn request_id(&self) -> RequestId {
        self.request_id
    }

    /// Borrows the normalized requested record.
    #[must_use]
    pub const fn requested(&self) -> &CapabilityRequestedRecordV1 {
        &self.requested
    }

    /// Returns the nonsecret candidate digest reference.
    #[must_use]
    pub const fn token_digest(&self) -> CapabilityTokenDigest {
        self.token_digest
    }

    /// Borrows the initially authorizing principal identity and revision.
    #[must_use]
    pub const fn initiator(&self) -> &AuditPrincipalV1 {
        &self.initiator
    }

    /// Borrows the optional policy-validated approval identity.
    #[must_use]
    pub const fn approval_id(&self) -> Option<&ApprovalId> {
        self.approval_id.as_ref()
    }
}

/// Pre-time normal-revoke candidate used to open a short transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapabilityRevokeCandidateV1 {
    capability_id: CapabilityId,
    request_id: RequestId,
    initiator: AuditPrincipalV1,
    approval_id: Option<ApprovalId>,
    reason: RevocationReasonCodeV1,
}

impl CapabilityRevokeCandidateV1 {
    /// Constructs the value-only candidate before the final authorization clock.
    #[must_use]
    pub const fn new(
        capability_id: CapabilityId,
        request_id: RequestId,
        initiator: AuditPrincipalV1,
        approval_id: Option<ApprovalId>,
        reason: RevocationReasonCodeV1,
    ) -> Self {
        Self {
            capability_id,
            request_id,
            initiator,
            approval_id,
            reason,
        }
    }

    /// Returns the target stable capability identity.
    #[must_use]
    pub const fn capability_id(&self) -> CapabilityId {
        self.capability_id
    }

    /// Returns the fresh invocation request identity.
    #[must_use]
    pub const fn request_id(&self) -> RequestId {
        self.request_id
    }

    /// Borrows the initially authorizing principal identity and revision.
    #[must_use]
    pub const fn initiator(&self) -> &AuditPrincipalV1 {
        &self.initiator
    }

    /// Borrows the optional policy-validated approval identity.
    #[must_use]
    pub const fn approval_id(&self) -> Option<&ApprovalId> {
        self.approval_id.as_ref()
    }

    /// Returns the closed requested revocation reason.
    #[must_use]
    pub const fn reason(&self) -> RevocationReasonCodeV1 {
        self.reason
    }
}

/// Digest-free transaction-current capability observation for policy lowering.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionCurrentCapabilityObservationV1 {
    capability_id: CapabilityId,
    revision: NonZeroU64,
    database_id: DatabaseId,
    environment: Environment,
    principal_id: ActorId,
    actor_kind: ActorKind,
    audiences: Vec<Audience>,
    issued_at: Timestamp,
    expires_at: Timestamp,
    grant: CapabilityGrantV1,
    lifecycle: CapabilityLifecycleV1,
}

impl TransactionCurrentCapabilityObservationV1 {
    /// Mechanically copies every policy-relevant checked field and no digest.
    #[must_use]
    pub fn from_record(record: &StoredCapabilityRecordV1) -> Self {
        Self {
            capability_id: record.capability_id,
            revision: record.revision,
            database_id: record.database_id,
            environment: record.environment.clone(),
            principal_id: record.principal_id.clone(),
            actor_kind: record.actor_kind,
            audiences: record.audiences.clone(),
            issued_at: record.issued_at,
            expires_at: record.expires_at,
            grant: record.grant.clone(),
            lifecycle: record.lifecycle.clone(),
        }
    }

    /// Returns the stable capability identity.
    #[must_use]
    pub const fn capability_id(&self) -> CapabilityId {
        self.capability_id
    }

    /// Returns the transaction-current lifecycle revision.
    #[must_use]
    pub const fn revision(&self) -> NonZeroU64 {
        self.revision
    }

    /// Returns the permanent database identity.
    #[must_use]
    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    /// Returns the exact environment.
    #[must_use]
    pub const fn environment(&self) -> &Environment {
        &self.environment
    }

    /// Returns the stable principal identity.
    #[must_use]
    pub const fn principal_id(&self) -> &ActorId {
        &self.principal_id
    }

    /// Returns the trusted actor kind.
    #[must_use]
    pub const fn actor_kind(&self) -> ActorKind {
        self.actor_kind
    }

    /// Returns canonical configured audiences.
    #[must_use]
    pub fn audiences(&self) -> &[Audience] {
        &self.audiences
    }

    /// Returns the exact issue time.
    #[must_use]
    pub const fn issued_at(&self) -> Timestamp {
        self.issued_at
    }

    /// Returns the exclusive expiration time.
    #[must_use]
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }

    /// Returns the complete checked grant.
    #[must_use]
    pub const fn grant(&self) -> &CapabilityGrantV1 {
        &self.grant
    }

    /// Returns the irreversible lifecycle observation.
    #[must_use]
    pub const fn lifecycle(&self) -> &CapabilityLifecycleV1 {
        &self.lifecycle
    }
}

/// Exact transaction-current observations for one normal mutation candidate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapabilityMutationCurrentStateV1 {
    authorizing: Option<TransactionCurrentCapabilityObservationV1>,
    target: Option<TransactionCurrentCapabilityObservationV1>,
}

impl CapabilityMutationCurrentStateV1 {
    /// Constructs observations for one create candidate from the held transaction.
    pub fn for_create(
        candidate: &CapabilityCreateCandidateV1,
        authorizing: Option<TransactionCurrentCapabilityObservationV1>,
        target: Option<TransactionCurrentCapabilityObservationV1>,
    ) -> Result<Self, StorageValueError> {
        Self::for_candidate_ids(
            candidate.initiator.capability_id(),
            candidate.capability_id,
            authorizing,
            target,
        )
    }

    /// Constructs observations for one revoke candidate from the held transaction.
    pub fn for_revoke(
        candidate: &CapabilityRevokeCandidateV1,
        authorizing: Option<TransactionCurrentCapabilityObservationV1>,
        target: Option<TransactionCurrentCapabilityObservationV1>,
    ) -> Result<Self, StorageValueError> {
        Self::for_candidate_ids(
            candidate.initiator.capability_id(),
            candidate.capability_id,
            authorizing,
            target,
        )
    }

    fn for_candidate_ids(
        authorizing_id: CapabilityId,
        target_id: CapabilityId,
        authorizing: Option<TransactionCurrentCapabilityObservationV1>,
        target: Option<TransactionCurrentCapabilityObservationV1>,
    ) -> Result<Self, StorageValueError> {
        if authorizing
            .as_ref()
            .is_some_and(|record| record.capability_id != authorizing_id)
            || target
                .as_ref()
                .is_some_and(|record| record.capability_id != target_id)
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        Ok(Self {
            authorizing,
            target,
        })
    }

    /// Returns the transaction-current authorizing capability, if still present.
    #[must_use]
    pub const fn authorizing(&self) -> Option<&TransactionCurrentCapabilityObservationV1> {
        self.authorizing.as_ref()
    }

    /// Returns the transaction-current target capability, if present.
    #[must_use]
    pub const fn target(&self) -> Option<&TransactionCurrentCapabilityObservationV1> {
        self.target.as_ref()
    }
}

/// Checked digest candidates supplied to the principal-less bootstrap path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BootstrapDigestCandidatesV1 {
    candidates: Vec<CapabilityTokenDigest>,
    current_write: CapabilityTokenDigest,
}

impl BootstrapDigestCandidatesV1 {
    /// Constructs a one-to-eight candidate set and identifies the write digest.
    pub fn new(
        candidates: Vec<CapabilityTokenDigest>,
        current_write: CapabilityTokenDigest,
    ) -> Result<Self, StorageValueError> {
        if candidates.is_empty() {
            return Err(StorageValueError::Empty);
        }
        if candidates.len() > 8 {
            return Err(StorageValueError::LimitExceeded);
        }
        if !candidates.contains(&current_write) {
            return Err(StorageValueError::InvalidShape);
        }
        let mut sorted = candidates.clone();
        sorted.sort_unstable();
        if sorted.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(StorageValueError::Duplicate);
        }
        Ok(Self {
            candidates,
            current_write,
        })
    }

    /// Returns lookup candidates in provider order.
    #[must_use]
    pub fn candidates(&self) -> &[CapabilityTokenDigest] {
        &self.candidates
    }

    /// Returns the current-key digest persisted by a new bootstrap.
    #[must_use]
    pub const fn current_write(&self) -> CapabilityTokenDigest {
        self.current_write
    }
}

/// Compound new-or-replay bootstrap transition input.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapabilityBootstrapIntentV1 {
    capability_id: CapabilityId,
    requested: CapabilityRequestedRecordV1,
    digests: BootstrapDigestCandidatesV1,
    issued_at: Timestamp,
    expires_at: Timestamp,
    start: BootstrapServiceAuditStartV1,
}

impl CapabilityBootstrapIntentV1 {
    /// Constructs a human-target bootstrap request sharing one start timestamp.
    pub fn new(
        capability_id: CapabilityId,
        requested: CapabilityRequestedRecordV1,
        digests: BootstrapDigestCandidatesV1,
        issued_at: Timestamp,
        expires_at: Timestamp,
        start: BootstrapServiceAuditStartV1,
    ) -> Result<Self, StorageValueError> {
        if requested.actor_kind != ActorKind::Human
            || !requested
                .grant
                .permissions
                .contains_kind(CapabilityPermissionKindV1::AdministerCapabilities)
            || issued_at != start.timestamp()
            || !timestamp_interval_matches(issued_at, expires_at, requested.duration_seconds.get())
        {
            return Err(StorageValueError::InvalidShape);
        }
        Ok(Self {
            capability_id,
            requested,
            digests,
            issued_at,
            expires_at,
            start,
        })
    }

    /// Returns the retained caller-selected capability ID.
    #[must_use]
    pub const fn capability_id(&self) -> CapabilityId {
        self.capability_id
    }

    /// Returns the normalized requested record.
    #[must_use]
    pub const fn requested(&self) -> &CapabilityRequestedRecordV1 {
        &self.requested
    }

    /// Returns checked readable digest candidates.
    #[must_use]
    pub const fn digests(&self) -> &BootstrapDigestCandidatesV1 {
        &self.digests
    }

    /// Returns issue time shared with the compound records.
    #[must_use]
    pub const fn issued_at(&self) -> Timestamp {
        self.issued_at
    }

    /// Returns checked exclusive expiry time.
    #[must_use]
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }

    /// Returns the principal-less checked start input.
    #[must_use]
    pub const fn start(&self) -> &BootstrapServiceAuditStartV1 {
        &self.start
    }
}

/// Closed result of resolving bounded digest candidates.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CapabilityLookupResult {
    /// No candidate lookup exists.
    NotFound,
    /// Exactly one reciprocal capability record matches.
    Found(Box<StoredCapabilityRecordV1>),
    /// More than one candidate matched; callers must fail closed.
    MultipleMatches,
}

/// Closed normal create transition result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CapabilityCreateResult {
    /// Capability, lookup, audit, and allocator advance committed.
    Created {
        /// Stable capability identity.
        capability_id: CapabilityId,
        /// Initial revision one.
        revision: NonZeroU64,
        /// Assigned administration sequence.
        administration_sequence: AdministrationSequence,
    },
    /// The same normalized record already exists; token is unrecoverable.
    AlreadyCreated {
        /// Stable capability identity.
        capability_id: CapabilityId,
        /// Existing revision.
        revision: NonZeroU64,
        /// Original administration sequence that created the capability.
        administration_sequence: AdministrationSequence,
    },
    /// The stable ID exists with different normalized requested content.
    CapabilityIdConflict,
    /// The proposed token digest already maps to another capability.
    TokenDigestCollision,
}

/// Closed revocation transition result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CapabilityRevokeResult {
    /// Revocation, record revision, audit, and sequence committed.
    Revoked {
        /// Stable target identity.
        capability_id: CapabilityId,
        /// New revision.
        revision: NonZeroU64,
        /// Assigned revoke sequence.
        administration_sequence: AdministrationSequence,
    },
    /// Target was already irreversibly revoked; no write occurred.
    AlreadyRevoked {
        /// Stable target identity.
        capability_id: CapabilityId,
        /// Existing revoked revision.
        revision: NonZeroU64,
        /// Original revoking sequence.
        administration_sequence: AdministrationSequence,
    },
    /// No capability exists for the stable ID.
    CapabilityNotFound,
}

/// Closed compound bootstrap transition result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CapabilityBootstrapResult {
    /// First capability and its linked records committed.
    BootstrapCreated {
        /// Stable bootstrap capability ID.
        capability_id: CapabilityId,
        /// Initial revision one.
        revision: NonZeroU64,
        /// Original authoritative transition sequence.
        administration_sequence: AdministrationSequence,
        /// This invocation's preceding principal-less start sequence.
        invocation_started_sequence: AdministrationSequence,
    },
    /// Exact replay appended only a new linked start record.
    BootstrapReplayed {
        /// Stable bootstrap capability ID.
        capability_id: CapabilityId,
        /// Existing revision.
        revision: NonZeroU64,
        /// Original authoritative transition sequence.
        administration_sequence: AdministrationSequence,
        /// This invocation's new start sequence.
        invocation_started_sequence: AdministrationSequence,
    },
    /// Marker, ID, requested record, or digest candidates do not match.
    BootstrapConflict,
}

/// Least-authority synchronous capability lookup port used by authentication.
pub trait CapabilityReader {
    /// Reads one capability by stable ID.
    fn read_capability(
        &self,
        capability_id: CapabilityId,
    ) -> Result<Option<StoredCapabilityRecordV1>, StorageError>;

    /// Resolves all bounded digest candidates before selecting a unique match.
    fn resolve_capability_digests(
        &self,
        candidates: &[CapabilityTokenDigest],
    ) -> Result<CapabilityLookupResult, StorageError>;
}

/// Opens short consuming normal capability-administration transactions.
pub trait CapabilityAdministrationTransactionPort {
    /// Backend-private create candidate state.
    type CreateCandidate: CapabilityCreateCandidateTransaction;
    /// Backend-private revoke candidate state.
    type RevokeCandidate: CapabilityRevokeCandidateTransaction;

    /// Opens a create candidate before the final authorization-clock sample.
    fn begin_capability_create(
        &self,
        candidate: CapabilityCreateCandidateV1,
    ) -> Result<Self::CreateCandidate, StorageError>;

    /// Opens a revoke candidate before the final authorization-clock sample.
    fn begin_capability_revoke(
        &self,
        candidate: CapabilityRevokeCandidateV1,
    ) -> Result<Self::RevokeCandidate, StorageError>;
}

/// Consuming create state permitted only to read transaction-current facts.
pub trait CapabilityCreateCandidateTransaction: Sized {
    /// Backend-private state held while policy makes its pure final decision.
    type AwaitingDecision: CapabilityCreateAwaitingDecision;

    /// Reads digest-free authorizer and target observations from this transaction.
    fn read_transaction_current(
        self,
    ) -> Result<(Self::AwaitingDecision, CapabilityMutationCurrentStateV1), StorageError>;

    /// Rolls back without sampling a clock, assigning a sequence, or writing.
    fn abandon(self) -> CapabilityCreateCandidateV1;
}

/// Create state held synchronously across the pure policy verifier call.
pub trait CapabilityCreateAwaitingDecision: Sized {
    /// Commits only a final intent matching the opened candidate and current
    /// authorization decision, reusing its one checked clock timestamp.
    fn commit_create(
        self,
        intent: CapabilityCreateIntentV1,
    ) -> Result<CapabilityCreateResult, StorageError>;

    /// Rolls back a denied or stale candidate with no sequence or audit record.
    fn abandon(self) -> CapabilityCreateCandidateV1;
}

/// Consuming revoke state permitted only to read transaction-current facts.
pub trait CapabilityRevokeCandidateTransaction: Sized {
    /// Backend-private state held while policy makes its pure final decision.
    type AwaitingDecision: CapabilityRevokeAwaitingDecision;

    /// Reads digest-free authorizer and target observations from this transaction.
    fn read_transaction_current(
        self,
    ) -> Result<(Self::AwaitingDecision, CapabilityMutationCurrentStateV1), StorageError>;

    /// Rolls back without sampling a clock, assigning a sequence, or writing.
    fn abandon(self) -> CapabilityRevokeCandidateV1;
}

/// Revoke state held synchronously across the pure policy verifier call.
pub trait CapabilityRevokeAwaitingDecision: Sized {
    /// Commits only a final intent matching the opened candidate/current target,
    /// reusing its one checked authorization timestamp for revoke and audit.
    fn commit_revoke(
        self,
        intent: CapabilityRevokeIntentV1,
    ) -> Result<CapabilityRevokeResult, StorageError>;

    /// Rolls back a denied or stale candidate with no sequence or audit record.
    fn abandon(self) -> CapabilityRevokeCandidateV1;
}

/// Coordinator-only dedicated compound bootstrap transition.
pub trait CapabilityBootstrapAdministrationRepository {
    /// Atomically performs new bootstrap or exact replay start linkage.
    fn bootstrap_capability(
        &mut self,
        intent: &CapabilityBootstrapIntentV1,
    ) -> Result<CapabilityBootstrapResult, StorageError>;
}

fn permission_set_semantic_bytes(
    permissions: &[CapabilityPermissionV1],
) -> Result<usize, StorageValueError> {
    permissions.iter().try_fold(4usize, |total, permission| {
        total
            .checked_add(framed_capability_bytes(
                capability_permission_semantic_bytes(permission),
            )?)
            .ok_or(StorageValueError::SizeOverflow)
    })
}

fn capability_permission_semantic_bytes(permission: &CapabilityPermissionV1) -> usize {
    match permission {
        CapabilityPermissionV1::Unparameterized(_) => 1,
        CapabilityPermissionV1::ExplainCommand(lineage, _)
        | CapabilityPermissionV1::InvokeCommand(lineage, _)
        | CapabilityPermissionV1::ReadEntity(lineage, _)
        | CapabilityPermissionV1::ScanIndex(lineage, _)
        | CapabilityPermissionV1::QueryProjection(lineage, _)
        | CapabilityPermissionV1::ReadProjectionStatus(lineage, _) => {
            1 + 4 + lineage.as_bytes().len() + 4
        }
    }
}

fn explicit_partitions_semantic_bytes(
    values: &[ScopedPartitionV1],
) -> Result<usize, StorageValueError> {
    values.iter().try_fold(5usize, |total, value| {
        let entry = checked_capability_sum([
            framed_capability_bytes(value.lineage.as_bytes().len())?,
            framed_capability_bytes(value.partition_key.as_bytes().len())?,
        ])?;
        total
            .checked_add(framed_capability_bytes(entry)?)
            .ok_or(StorageValueError::SizeOverflow)
    })
}

fn partition_scope_semantic_bytes(scope: &PartitionScopeV1) -> Result<usize, StorageValueError> {
    match scope {
        PartitionScopeV1::All => Ok(1),
        PartitionScopeV1::Explicit(values) => explicit_partitions_semantic_bytes(values),
    }
}

fn field_visibility_semantic_bytes(
    entries: &[EntityFieldVisibilityV1],
) -> Result<usize, StorageValueError> {
    entries.iter().try_fold(4usize, |total, entry| {
        let fields = entry
            .fields
            .len()
            .checked_mul(4)
            .ok_or(StorageValueError::SizeOverflow)?;
        let value = checked_capability_sum([
            framed_capability_bytes(entry.lineage.as_bytes().len())?,
            4,
            4,
            fields,
        ])?;
        total
            .checked_add(framed_capability_bytes(value)?)
            .ok_or(StorageValueError::SizeOverflow)
    })
}

fn capability_grant_semantic_bytes_parts(
    tenant_scope: &TenantScope,
    partition_scope: &PartitionScopeV1,
    permissions: &CapabilityPermissionsV1,
    field_visibility: &[EntityFieldVisibilityV1],
    approval_required: &[CapabilityPermissionKindV1],
) -> Result<usize, StorageValueError> {
    checked_capability_sum([
        framed_capability_bytes(tenant_scope.to_canonical_bytes().len())?,
        partition_scope_semantic_bytes(partition_scope)?,
        permission_set_semantic_bytes(permissions.as_slice())?,
        field_visibility_semantic_bytes(field_visibility)?,
        2,
        4usize
            .checked_add(approval_required.len())
            .ok_or(StorageValueError::SizeOverflow)?,
    ])
}

fn capability_grant_semantic_bytes(grant: &CapabilityGrantV1) -> Result<usize, StorageValueError> {
    capability_grant_semantic_bytes_parts(
        &grant.tenant_scope,
        &grant.partition_scope,
        &grant.permissions,
        &grant.field_visibility,
        &grant.approval_required,
    )
}

fn audience_set_semantic_bytes(audiences: &[Audience]) -> Result<usize, StorageValueError> {
    audiences.iter().try_fold(4usize, |total, audience| {
        total
            .checked_add(framed_capability_bytes(audience.as_bytes().len())?)
            .ok_or(StorageValueError::SizeOverflow)
    })
}

fn maximum_stored_capability_semantic_bytes_from_requested(
    requested: &CapabilityRequestedRecordV1,
) -> Result<usize, StorageValueError> {
    stored_capability_semantic_bytes_parts(
        &requested.environment,
        &requested.principal_id,
        &requested.audiences,
        &requested.grant,
        1 + 12 + 8 + 1,
    )
}

fn stored_capability_semantic_bytes(
    environment: &Environment,
    principal_id: &ActorId,
    audiences: &[Audience],
    grant: &CapabilityGrantV1,
    lifecycle: &CapabilityLifecycleV1,
) -> Result<usize, StorageValueError> {
    let lifecycle_bytes = match lifecycle {
        CapabilityLifecycleV1::Active => 1,
        CapabilityLifecycleV1::Revoked { .. } => 1 + 12 + 8 + 1,
    };
    stored_capability_semantic_bytes_parts(
        environment,
        principal_id,
        audiences,
        grant,
        lifecycle_bytes,
    )
}

fn stored_capability_semantic_bytes_parts(
    environment: &Environment,
    principal_id: &ActorId,
    audiences: &[Audience],
    grant: &CapabilityGrantV1,
    lifecycle_bytes: usize,
) -> Result<usize, StorageValueError> {
    checked_capability_sum([
        16,         // capability ID
        8,          // revision
        1 + 4 + 32, // digest scheme, key ID, digest
        16,         // database ID
        framed_capability_bytes(environment.as_bytes().len())?,
        framed_capability_bytes(principal_id.as_str().len())?,
        1, // actor kind
        audience_set_semantic_bytes(audiences)?,
        12,
        12,
        8,  // creation administration sequence
        16, // creation request ID
        framed_capability_bytes(capability_grant_semantic_bytes(grant)?)?,
        lifecycle_bytes,
    ])
}

fn framed_capability_bytes(content_bytes: usize) -> Result<usize, StorageValueError> {
    4usize
        .checked_add(content_bytes)
        .ok_or(StorageValueError::SizeOverflow)
}

fn checked_capability_sum(
    parts: impl IntoIterator<Item = usize>,
) -> Result<usize, StorageValueError> {
    parts.into_iter().try_fold(0usize, |total, part| {
        total
            .checked_add(part)
            .ok_or(StorageValueError::SizeOverflow)
    })
}

fn validate_capability_payload_bytes(bytes: usize) -> Result<(), StorageValueError> {
    if bytes > MAX_CAPABILITY_PAYLOAD_BYTES {
        return Err(StorageValueError::LimitExceeded);
    }
    Ok(())
}

fn validate_capability_audiences(audiences: &[Audience]) -> Result<(), StorageValueError> {
    if audiences.is_empty() {
        return Err(StorageValueError::Empty);
    }
    if audiences.len() > MAX_CAPABILITY_AUDIENCES {
        return Err(StorageValueError::LimitExceeded);
    }
    if audiences.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(StorageValueError::NonCanonicalOrder);
    }
    Ok(())
}

fn validate_stored_capability_interval(
    issued: Timestamp,
    expires: Timestamp,
) -> Result<(), StorageValueError> {
    if issued.nanoseconds() != expires.nanoseconds() {
        return Err(StorageValueError::InvalidShape);
    }
    let duration = expires
        .seconds()
        .checked_sub(issued.seconds())
        .ok_or(StorageValueError::InvalidShape)?;
    if !(1..=i64::from(MAX_CAPABILITY_LIFETIME_SECONDS)).contains(&duration) {
        return Err(StorageValueError::InvalidShape);
    }
    Ok(())
}

fn append_lineage(bytes: &mut Vec<u8>, lineage: &ContractLineage) {
    append_bytes(bytes, lineage.as_bytes());
}

fn append_bytes(output: &mut Vec<u8>, value: &[u8]) {
    output.extend_from_slice(&(value.len() as u32).to_be_bytes());
    output.extend_from_slice(value);
}

fn timestamp_interval_matches(issued: Timestamp, expires: Timestamp, seconds: u32) -> bool {
    issued.nanoseconds() == expires.nanoseconds()
        && issued
            .seconds()
            .checked_add(i64::from(seconds))
            .is_some_and(|expected| expected == expires.seconds())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permission_tags_are_closed_and_stable() {
        for tag in 1..=19 {
            let kind = CapabilityPermissionKindV1::from_tag(tag).expect("known tag");
            assert_eq!(kind.tag(), tag);
        }
        assert_eq!(CapabilityPermissionKindV1::from_tag(0), None);
        assert_eq!(CapabilityPermissionKindV1::from_tag(20), None);
    }

    #[test]
    fn parameterized_permission_cannot_be_bare() {
        assert_eq!(
            CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::InvokeCommand),
            Err(StorageValueError::InvalidShape)
        );
        assert!(
            CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::ReadHealth).is_ok()
        );
    }

    #[test]
    fn capability_payload_bound_accepts_exact_limit_only() {
        assert_eq!(
            validate_capability_payload_bytes(MAX_CAPABILITY_PAYLOAD_BYTES),
            Ok(())
        );
        assert_eq!(
            validate_capability_payload_bytes(MAX_CAPABILITY_PAYLOAD_BYTES + 1),
            Err(StorageValueError::LimitExceeded)
        );
    }
}
