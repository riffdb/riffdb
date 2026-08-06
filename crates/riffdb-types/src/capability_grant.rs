//! Canonical bounded capability grants shared by authentication and policy.

use std::{error::Error, fmt, num::NonZeroU16, sync::Arc};

use crate::{
    ApplicationRoleHash, CommandId, ContractLineage, EntityTypeId, FieldId, IndexId, PartitionKey,
    ProjectionId, QueryModuleHash, QueryOperationName, ReactiveModuleHash, ReactiveOperationName,
    TenantScope,
};

/// Maximum explicit partition entries retained by one grant.
pub const MAX_CAPABILITY_PARTITIONS: usize = 1_024;
/// Maximum permission atoms retained by one grant.
pub const MAX_CAPABILITY_PERMISSIONS: usize = 8_192;
/// Maximum field-visibility entries and total listed fields.
pub const MAX_CAPABILITY_FIELD_VISIBILITY: usize = 65_535;
/// Maximum semantic bytes in one complete durable capability payload.
pub const MAX_CAPABILITY_PAYLOAD_BYTES: usize = 1024 * 1024;

/// A safe failure to construct or account for a bounded capability grant.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CapabilityGrantError {
    /// A supplied collection is empty when at least one item is required.
    Empty,
    /// A supplied count or semantic size exceeds its accepted bound.
    LimitExceeded,
    /// The same semantic identity occurs more than once.
    Duplicate,
    /// A permission has an invalid closed shape.
    InvalidShape,
    /// Checked semantic byte accounting overflowed.
    SizeOverflow,
}

impl fmt::Display for CapabilityGrantError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Empty => "required capability grant value is empty",
            Self::LimitExceeded => "capability grant value exceeds a hard limit",
            Self::Duplicate => "capability grant value contains a duplicate identity",
            Self::InvalidShape => "capability grant value has an invalid semantic shape",
            Self::SizeOverflow => "capability grant size calculation overflowed",
        })
    }
}

impl Error for CapabilityGrantError {}

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
    /// Check ad-hoc RiffQL source.
    CheckAdHocQuery,
    /// Explain ad-hoc RiffQL source.
    ExplainAdHocQuery,
    /// Execute ad-hoc RiffQL source.
    ExecuteAdHocQuery,
    /// Explain one exact named deployed query.
    ExplainNamedQuery,
    /// Execute one exact named deployed query.
    ExecuteNamedQuery,
    /// Non-authorizing identity of the compiled application role that produced the grant.
    ApplicationRoleIdentity,
    /// Migrate one exact contract lineage.
    MigrateContract,
    /// Consume one exact immutable event stream.
    ConsumeEventStream,
    /// Administratively seek one exact immutable event stream consumer.
    SeekEventStreamConsumer,
    /// Watch one exact immutable named-query projection.
    WatchNamedQuery,
    /// Consume one exact immutable contextual subscription.
    ConsumeContextualSubscription,
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
            Self::CheckAdHocQuery => 0x14,
            Self::ExplainAdHocQuery => 0x15,
            Self::ExecuteAdHocQuery => 0x16,
            Self::ExplainNamedQuery => 0x17,
            Self::ExecuteNamedQuery => 0x18,
            Self::ApplicationRoleIdentity => 0x19,
            Self::MigrateContract => 0x1a,
            Self::ConsumeEventStream => 0x1b,
            Self::SeekEventStreamConsumer => 0x1c,
            Self::WatchNamedQuery => 0x1d,
            Self::ConsumeContextualSubscription => 0x1e,
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
            0x14 => Some(Self::CheckAdHocQuery),
            0x15 => Some(Self::ExplainAdHocQuery),
            0x16 => Some(Self::ExecuteAdHocQuery),
            0x17 => Some(Self::ExplainNamedQuery),
            0x18 => Some(Self::ExecuteNamedQuery),
            0x19 => Some(Self::ApplicationRoleIdentity),
            0x1a => Some(Self::MigrateContract),
            0x1b => Some(Self::ConsumeEventStream),
            0x1c => Some(Self::SeekEventStreamConsumer),
            0x1d => Some(Self::WatchNamedQuery),
            0x1e => Some(Self::ConsumeContextualSubscription),
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
                | Self::ExplainNamedQuery
                | Self::ExecuteNamedQuery
                | Self::ApplicationRoleIdentity
                | Self::MigrateContract
                | Self::ConsumeEventStream
                | Self::SeekEventStreamConsumer
                | Self::WatchNamedQuery
                | Self::ConsumeContextualSubscription
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
    /// Explain one exact named query in one immutable module.
    ExplainNamedQuery(ContractLineage, QueryModuleHash, QueryOperationName),
    /// Execute one exact named query in one immutable module.
    ExecuteNamedQuery(ContractLineage, QueryModuleHash, QueryOperationName),
    /// Audit-only identity of the exact compiled application role.
    ApplicationRoleIdentity(ApplicationRoleHash),
    /// Migrate one exact contract lineage.
    MigrateContract(ContractLineage),
    /// Consume an exact stream from one immutable reactive module.
    ConsumeEventStream(ContractLineage, ReactiveModuleHash, ReactiveOperationName),
    /// Seek a consumer of an exact stream from one immutable reactive module.
    SeekEventStreamConsumer(ContractLineage, ReactiveModuleHash, ReactiveOperationName),
    /// Watch an exact query watch from one immutable reactive module.
    WatchNamedQuery(ContractLineage, ReactiveModuleHash, ReactiveOperationName),
    /// Consume an exact contextual subscription from one immutable reactive module.
    ConsumeContextualSubscription(ContractLineage, ReactiveModuleHash, ReactiveOperationName),
}

impl CapabilityPermissionV1 {
    /// Constructs an unparameterized atom, rejecting parameterized kinds.
    pub fn unparameterized(kind: CapabilityPermissionKindV1) -> Result<Self, CapabilityGrantError> {
        if kind.requires_parameter() {
            return Err(CapabilityGrantError::InvalidShape);
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
            Self::ExplainNamedQuery(..) => CapabilityPermissionKindV1::ExplainNamedQuery,
            Self::ExecuteNamedQuery(..) => CapabilityPermissionKindV1::ExecuteNamedQuery,
            Self::ApplicationRoleIdentity(..) => {
                CapabilityPermissionKindV1::ApplicationRoleIdentity
            }
            Self::MigrateContract(..) => CapabilityPermissionKindV1::MigrateContract,
            Self::ConsumeEventStream(..) => CapabilityPermissionKindV1::ConsumeEventStream,
            Self::SeekEventStreamConsumer(..) => {
                CapabilityPermissionKindV1::SeekEventStreamConsumer
            }
            Self::WatchNamedQuery(..) => CapabilityPermissionKindV1::WatchNamedQuery,
            Self::ConsumeContextualSubscription(..) => {
                CapabilityPermissionKindV1::ConsumeContextualSubscription
            }
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
            Self::ExplainNamedQuery(lineage, module_hash, query_name)
            | Self::ExecuteNamedQuery(lineage, module_hash, query_name) => {
                append_lineage(&mut bytes, lineage);
                bytes.extend_from_slice(module_hash.as_bytes());
                append_bytes(&mut bytes, query_name.as_str().as_bytes());
            }
            Self::ApplicationRoleIdentity(role_hash) => {
                bytes.extend_from_slice(role_hash.as_bytes());
            }
            Self::MigrateContract(lineage) => append_lineage(&mut bytes, lineage),
            Self::ConsumeEventStream(lineage, module_hash, operation_name)
            | Self::SeekEventStreamConsumer(lineage, module_hash, operation_name)
            | Self::WatchNamedQuery(lineage, module_hash, operation_name)
            | Self::ConsumeContextualSubscription(lineage, module_hash, operation_name) => {
                append_lineage(&mut bytes, lineage);
                bytes.extend_from_slice(module_hash.as_bytes());
                append_bytes(&mut bytes, operation_name.as_str().as_bytes());
            }
        }
        bytes
    }
}

/// Canonical permission set.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapabilityPermissionsV1(Arc<[CapabilityPermissionV1]>);

impl CapabilityPermissionsV1 {
    /// Sorts by canonical bytes and rejects duplicates or excessive counts.
    ///
    /// An empty set is a valid grant with no operation permission. Bootstrap
    /// separately requires the administration permission.
    pub fn new(mut values: Vec<CapabilityPermissionV1>) -> Result<Self, CapabilityGrantError> {
        if values.len() > MAX_CAPABILITY_PERMISSIONS {
            return Err(CapabilityGrantError::LimitExceeded);
        }
        validate_capability_payload_bytes(permission_set_semantic_bytes(&values)?)?;
        values.sort_by_key(CapabilityPermissionV1::canonical_key);
        if values
            .windows(2)
            .any(|pair| pair[0].canonical_key() == pair[1].canonical_key())
        {
            return Err(CapabilityGrantError::Duplicate);
        }
        Ok(Self(values.into()))
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
    pub fn explicit(mut values: Vec<ScopedPartitionV1>) -> Result<Self, CapabilityGrantError> {
        if values.is_empty() {
            return Err(CapabilityGrantError::Empty);
        }
        if values.len() > MAX_CAPABILITY_PARTITIONS {
            return Err(CapabilityGrantError::LimitExceeded);
        }
        validate_capability_payload_bytes(explicit_partitions_semantic_bytes(&values)?)?;
        values.sort_by_key(ScopedPartitionV1::canonical_key);
        if values
            .windows(2)
            .any(|pair| pair[0].canonical_key() == pair[1].canonical_key())
        {
            return Err(CapabilityGrantError::Duplicate);
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
    fields: Arc<[FieldId]>,
}

impl EntityFieldVisibilityV1 {
    /// Sorts and validates a nonempty field set.
    pub fn new(
        lineage: ContractLineage,
        entity_type: EntityTypeId,
        mut fields: Vec<FieldId>,
    ) -> Result<Self, CapabilityGrantError> {
        if fields.is_empty() {
            return Err(CapabilityGrantError::Empty);
        }
        if fields.len() > MAX_CAPABILITY_FIELD_VISIBILITY {
            return Err(CapabilityGrantError::LimitExceeded);
        }
        fields.sort_unstable();
        if fields.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(CapabilityGrantError::Duplicate);
        }
        Ok(Self {
            lineage,
            entity_type,
            fields: fields.into(),
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
    field_visibility: Arc<[EntityFieldVisibilityV1]>,
    max_scan_rows: NonZeroU16,
    approval_required: Arc<[CapabilityPermissionKindV1]>,
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
    ) -> Result<Self, CapabilityGrantError> {
        if usize::from(max_scan_rows.get()) > 500
            || field_visibility.len() > MAX_CAPABILITY_FIELD_VISIBILITY
            || approval_required.len() > 30
        {
            return Err(CapabilityGrantError::LimitExceeded);
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
            return Err(CapabilityGrantError::Duplicate);
        }
        let total_fields = field_visibility.iter().try_fold(0usize, |count, entry| {
            count
                .checked_add(entry.fields.len())
                .ok_or(CapabilityGrantError::SizeOverflow)
        })?;
        if total_fields > MAX_CAPABILITY_FIELD_VISIBILITY {
            return Err(CapabilityGrantError::LimitExceeded);
        }
        approval_required.sort_unstable();
        if approval_required.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(CapabilityGrantError::Duplicate);
        }
        let value = Self {
            tenant_scope,
            partition_scope,
            permissions,
            field_visibility: field_visibility.into(),
            max_scan_rows,
            approval_required: approval_required.into(),
        };
        validate_capability_payload_bytes(value.semantic_bytes()?)?;
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

    /// Returns the complete checked v1 semantic grant byte count.
    pub fn semantic_bytes(&self) -> Result<usize, CapabilityGrantError> {
        capability_grant_semantic_bytes_parts(
            &self.tenant_scope,
            &self.partition_scope,
            &self.permissions,
            &self.field_visibility,
            &self.approval_required,
        )
    }
}

fn permission_set_semantic_bytes(
    permissions: &[CapabilityPermissionV1],
) -> Result<usize, CapabilityGrantError> {
    permissions.iter().try_fold(4usize, |total, permission| {
        total
            .checked_add(framed_capability_bytes(
                capability_permission_semantic_bytes(permission),
            )?)
            .ok_or(CapabilityGrantError::SizeOverflow)
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
        CapabilityPermissionV1::ExplainNamedQuery(lineage, _, name)
        | CapabilityPermissionV1::ExecuteNamedQuery(lineage, _, name) => {
            1 + 4 + lineage.as_bytes().len() + 32 + 4 + name.as_str().len()
        }
        CapabilityPermissionV1::ApplicationRoleIdentity(_) => 1 + 32,
        CapabilityPermissionV1::MigrateContract(lineage) => 1 + 4 + lineage.as_bytes().len(),
        CapabilityPermissionV1::ConsumeEventStream(lineage, _, name)
        | CapabilityPermissionV1::SeekEventStreamConsumer(lineage, _, name)
        | CapabilityPermissionV1::WatchNamedQuery(lineage, _, name)
        | CapabilityPermissionV1::ConsumeContextualSubscription(lineage, _, name) => {
            1 + 4 + lineage.as_bytes().len() + 32 + 4 + name.as_str().len()
        }
    }
}

fn explicit_partitions_semantic_bytes(
    values: &[ScopedPartitionV1],
) -> Result<usize, CapabilityGrantError> {
    values.iter().try_fold(5usize, |total, value| {
        let entry = checked_capability_sum([
            framed_capability_bytes(value.lineage.as_bytes().len())?,
            framed_capability_bytes(value.partition_key.as_bytes().len())?,
        ])?;
        total
            .checked_add(framed_capability_bytes(entry)?)
            .ok_or(CapabilityGrantError::SizeOverflow)
    })
}

fn partition_scope_semantic_bytes(scope: &PartitionScopeV1) -> Result<usize, CapabilityGrantError> {
    match scope {
        PartitionScopeV1::All => Ok(1),
        PartitionScopeV1::Explicit(values) => explicit_partitions_semantic_bytes(values),
    }
}

fn field_visibility_semantic_bytes(
    entries: &[EntityFieldVisibilityV1],
) -> Result<usize, CapabilityGrantError> {
    entries.iter().try_fold(4usize, |total, entry| {
        let fields = entry
            .fields
            .len()
            .checked_mul(4)
            .ok_or(CapabilityGrantError::SizeOverflow)?;
        let value = checked_capability_sum([
            framed_capability_bytes(entry.lineage.as_bytes().len())?,
            4,
            4,
            fields,
        ])?;
        total
            .checked_add(framed_capability_bytes(value)?)
            .ok_or(CapabilityGrantError::SizeOverflow)
    })
}

fn capability_grant_semantic_bytes_parts(
    tenant_scope: &TenantScope,
    partition_scope: &PartitionScopeV1,
    permissions: &CapabilityPermissionsV1,
    field_visibility: &[EntityFieldVisibilityV1],
    approval_required: &[CapabilityPermissionKindV1],
) -> Result<usize, CapabilityGrantError> {
    checked_capability_sum([
        framed_capability_bytes(tenant_scope.to_canonical_bytes().len())?,
        partition_scope_semantic_bytes(partition_scope)?,
        permission_set_semantic_bytes(permissions.as_slice())?,
        field_visibility_semantic_bytes(field_visibility)?,
        2,
        4usize
            .checked_add(approval_required.len())
            .ok_or(CapabilityGrantError::SizeOverflow)?,
    ])
}

fn framed_capability_bytes(content_bytes: usize) -> Result<usize, CapabilityGrantError> {
    4usize
        .checked_add(content_bytes)
        .ok_or(CapabilityGrantError::SizeOverflow)
}

fn checked_capability_sum(
    parts: impl IntoIterator<Item = usize>,
) -> Result<usize, CapabilityGrantError> {
    parts.into_iter().try_fold(0usize, |total, part| {
        total
            .checked_add(part)
            .ok_or(CapabilityGrantError::SizeOverflow)
    })
}

fn validate_capability_payload_bytes(bytes: usize) -> Result<(), CapabilityGrantError> {
    if bytes > MAX_CAPABILITY_PAYLOAD_BYTES {
        return Err(CapabilityGrantError::LimitExceeded);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permission_tags_are_closed_and_stable() {
        for tag in 1..=30 {
            let kind = CapabilityPermissionKindV1::from_tag(tag).expect("known tag");
            assert_eq!(kind.tag(), tag);
        }
        assert_eq!(CapabilityPermissionKindV1::from_tag(0), None);
        assert_eq!(CapabilityPermissionKindV1::from_tag(31), None);
    }

    #[test]
    fn parameterized_permission_cannot_be_bare() {
        assert_eq!(
            CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::InvokeCommand),
            Err(CapabilityGrantError::InvalidShape)
        );
        assert!(
            CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::ReadHealth).is_ok()
        );
        assert_eq!(
            CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::ExecuteNamedQuery),
            Err(CapabilityGrantError::InvalidShape)
        );
        assert_eq!(
            CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::MigrateContract),
            Err(CapabilityGrantError::InvalidShape)
        );
        assert_eq!(
            CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::ConsumeEventStream),
            Err(CapabilityGrantError::InvalidShape)
        );
        assert!(
            CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::ExecuteAdHocQuery)
                .is_ok()
        );
    }

    #[test]
    fn migration_permission_binds_the_exact_lineage() {
        let permission = CapabilityPermissionV1::MigrateContract(
            ContractLineage::new("ticketdesk").expect("lineage"),
        );
        let mut expected = vec![0x1a, 0, 0, 0, 10];
        expected.extend_from_slice(b"ticketdesk");
        assert_eq!(permission.canonical_key(), expected);
    }

    #[test]
    fn reactive_permissions_bind_lineage_module_and_operation() {
        let lineage = ContractLineage::new("ticketdesk").expect("lineage");
        let module = ReactiveModuleHash::from_bytes([7; 32]);
        let operation = ReactiveOperationName::new("TicketActivity").expect("operation");
        let consume =
            CapabilityPermissionV1::ConsumeEventStream(lineage.clone(), module, operation.clone());
        let seek = CapabilityPermissionV1::SeekEventStreamConsumer(lineage, module, operation);
        assert_ne!(consume.canonical_key(), seek.canonical_key());
        assert_eq!(consume.canonical_key()[0], 0x1b);
        assert_eq!(seek.canonical_key()[0], 0x1c);
        assert!(
            consume
                .canonical_key()
                .windows(32)
                .any(|window| window == [7; 32])
        );
    }

    #[test]
    fn permission_sets_are_canonical_and_duplicate_free() {
        let values = vec![
            CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::ReadHealth)
                .expect("permission"),
            CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::ReadContract)
                .expect("permission"),
        ];
        let permissions = CapabilityPermissionsV1::new(values).expect("permissions");
        assert_eq!(
            permissions
                .as_slice()
                .iter()
                .map(CapabilityPermissionV1::kind)
                .collect::<Vec<_>>(),
            vec![
                CapabilityPermissionKindV1::ReadContract,
                CapabilityPermissionKindV1::ReadHealth,
            ]
        );
        let duplicate =
            CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::ReadHealth)
                .expect("permission");
        assert_eq!(
            CapabilityPermissionsV1::new(vec![duplicate.clone(), duplicate]),
            Err(CapabilityGrantError::Duplicate)
        );
    }

    #[test]
    fn capability_grant_clones_share_only_checked_immutable_collections() {
        fn grant() -> CapabilityGrantV1 {
            let lineage = ContractLineage::new("ticketdesk").expect("lineage");
            let permissions = CapabilityPermissionsV1::new(vec![
                CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::ReadContract)
                    .expect("permission"),
                CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::ReadHealth)
                    .expect("permission"),
            ])
            .expect("permissions");
            let visibility = EntityFieldVisibilityV1::new(
                lineage,
                EntityTypeId::first(),
                vec![FieldId::first()],
            )
            .expect("visibility");
            CapabilityGrantV1::new(
                TenantScope::Global,
                PartitionScopeV1::All,
                permissions,
                vec![visibility],
                NonZeroU16::new(10).expect("nonzero"),
                vec![CapabilityPermissionKindV1::ReadContract],
            )
            .expect("grant")
        }

        let original = grant();
        let cloned = original.clone();
        assert!(Arc::ptr_eq(&original.permissions.0, &cloned.permissions.0));
        assert!(Arc::ptr_eq(
            &original.field_visibility,
            &cloned.field_visibility
        ));
        assert!(Arc::ptr_eq(
            &original.field_visibility[0].fields,
            &cloned.field_visibility[0].fields
        ));
        assert!(Arc::ptr_eq(
            &original.approval_required,
            &cloned.approval_required
        ));

        let independently_built = grant();
        assert_eq!(original, independently_built);
        assert_eq!(
            original.semantic_bytes(),
            independently_built.semantic_bytes()
        );
        assert!(!Arc::ptr_eq(
            &original.permissions.0,
            &independently_built.permissions.0
        ));
    }

    #[test]
    fn capability_payload_bound_accepts_exact_limit_only() {
        assert_eq!(
            validate_capability_payload_bytes(MAX_CAPABILITY_PAYLOAD_BYTES),
            Ok(())
        );
        assert_eq!(
            validate_capability_payload_bytes(MAX_CAPABILITY_PAYLOAD_BYTES + 1),
            Err(CapabilityGrantError::LimitExceeded)
        );
    }
}
