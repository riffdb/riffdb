//! Canonical bounded capability grants shared by authentication and policy.

use std::{error::Error, fmt, num::NonZeroU16, sync::Arc};

use crate::{
    ApplicationInstallationCampaignId, ApplicationPortabilityManifestHash, ApplicationRoleHash,
    CapabilityPrincipalFactsV1, CommandId, ContractLineage, EntityTypeId, FieldId, IndexId,
    PartitionKey, ProjectionId, QueryModuleHash, QueryOperationName, ReactiveModuleHash,
    ReactiveOperationName, RowPolicyName, TenantScope,
};

/// Maximum explicit partition entries retained by one grant.
pub const MAX_CAPABILITY_PARTITIONS: usize = 1_024;
/// Maximum permission atoms retained by one grant.
pub const MAX_CAPABILITY_PERMISSIONS: usize = 8_192;
/// Maximum field-visibility entries and total listed fields.
pub const MAX_CAPABILITY_FIELD_VISIBILITY: usize = 65_535;
/// Maximum compiler-selected row-policy bindings carried by one capability.
pub const MAX_CAPABILITY_ROW_POLICY_BINDINGS: usize = 1_024;
/// Maximum lineage-scoped application export grants carried by one capability.
pub const MAX_CAPABILITY_APPLICATION_EXPORT_GRANTS: usize = 256;
/// Maximum exact vector-inspection targets carried by one application role.
pub const MAX_CAPABILITY_VECTOR_INSPECTION_TARGETS: usize = 256;
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
    /// Install or upgrade one exact application lineage.
    InstallApplication,
    /// Inspect one exact compiler-declared vector field.
    InspectVectorState,
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
            Self::InstallApplication => 0x1f,
            Self::InspectVectorState => 0x20,
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
            0x1f => Some(Self::InstallApplication),
            0x20 => Some(Self::InspectVectorState),
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
                | Self::InstallApplication
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
    /// Install or upgrade one exact application lineage.
    InstallApplication(ContractLineage),
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
            Self::InstallApplication(..) => CapabilityPermissionKindV1::InstallApplication,
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
            Self::InstallApplication(lineage) => append_lineage(&mut bytes, lineage),
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
#[derive(Clone)]
pub struct CapabilityPermissionsV1(Arc<[CapabilityPermissionV1]>, Arc<[Vec<u8>]>);

impl fmt::Debug for CapabilityPermissionsV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("CapabilityPermissionsV1")
            .field(&self.0)
            .finish()
    }
}

impl PartialEq for CapabilityPermissionsV1 {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for CapabilityPermissionsV1 {}

impl CapabilityPermissionsV1 {
    /// Sorts by canonical bytes and rejects duplicates or excessive counts.
    ///
    /// An empty set is a valid grant with no operation permission. Bootstrap
    /// separately requires the administration permission.
    pub fn new(values: Vec<CapabilityPermissionV1>) -> Result<Self, CapabilityGrantError> {
        if values.len() > MAX_CAPABILITY_PERMISSIONS {
            return Err(CapabilityGrantError::LimitExceeded);
        }
        validate_capability_payload_bytes(permission_set_semantic_bytes(&values)?)?;
        let mut keyed = values
            .into_iter()
            .map(|permission| (permission.canonical_key(), permission))
            .collect::<Vec<_>>();
        keyed.sort_by(|left, right| left.0.cmp(&right.0));
        if keyed.windows(2).any(|pair| pair[0].0 == pair[1].0) {
            return Err(CapabilityGrantError::Duplicate);
        }
        let mut values = Vec::with_capacity(keyed.len());
        let mut canonical_keys = Vec::with_capacity(keyed.len());
        for (canonical_key, permission) in keyed {
            canonical_keys.push(canonical_key);
            values.push(permission);
        }
        Ok(Self(values.into(), canonical_keys.into()))
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

    /// Returns whether the exact canonical permission atom is present.
    #[must_use]
    pub fn contains_exact(&self, permission: &CapabilityPermissionV1) -> bool {
        let target = permission.canonical_key();
        self.1
            .binary_search_by(|candidate| candidate.as_slice().cmp(&target))
            .is_ok()
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
///
/// Ordinary `fields` never reveal a secret-classified field (ADR-0118):
/// enumerating every field — the de-facto wildcard and the shape every role
/// default produces — is inert for secrets. Revealing one requires naming it
/// in the separate `secret_fields` list, which no derivation path populates
/// implicitly.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EntityFieldVisibilityV1 {
    lineage: ContractLineage,
    entity_type: EntityTypeId,
    fields: Arc<[FieldId]>,
    secret_fields: Arc<[FieldId]>,
}

impl EntityFieldVisibilityV1 {
    /// Sorts and validates a nonempty field set with no secret-field naming.
    pub fn new(
        lineage: ContractLineage,
        entity_type: EntityTypeId,
        fields: Vec<FieldId>,
    ) -> Result<Self, CapabilityGrantError> {
        Self::with_secret_fields(lineage, entity_type, fields, Vec::new())
    }

    /// Sorts and validates a visibility entry that explicitly names
    /// secret-classified fields for reveal (ADR-0118 item 3).
    ///
    /// The named secret fields count against the same bounds as ordinary
    /// visibility. At least one of the ordinary or secret lists must be
    /// nonempty.
    pub fn with_secret_fields(
        lineage: ContractLineage,
        entity_type: EntityTypeId,
        mut fields: Vec<FieldId>,
        mut secret_fields: Vec<FieldId>,
    ) -> Result<Self, CapabilityGrantError> {
        if fields.is_empty() && secret_fields.is_empty() {
            return Err(CapabilityGrantError::Empty);
        }
        if fields.len().saturating_add(secret_fields.len()) > MAX_CAPABILITY_FIELD_VISIBILITY {
            return Err(CapabilityGrantError::LimitExceeded);
        }
        fields.sort_unstable();
        if fields.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(CapabilityGrantError::Duplicate);
        }
        secret_fields.sort_unstable();
        if secret_fields.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(CapabilityGrantError::Duplicate);
        }
        Ok(Self {
            lineage,
            entity_type,
            fields: fields.into(),
            secret_fields: secret_fields.into(),
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

    /// Returns explicitly named secret-classified fields in increasing order
    /// (ADR-0118): the only naming surface that reveals a secret field.
    #[must_use]
    pub fn secret_fields(&self) -> &[FieldId] {
        &self.secret_fields
    }
}

/// Closed operation classes that one durable row-policy binding may authorize.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum CapabilityRowPolicyOperationV1 {
    /// Read one protected row.
    Read = 1,
    /// Create one protected row from proposed state.
    Create = 2,
    /// Update one protected row after checking current and successor state.
    Update = 3,
    /// Delete one protected transaction-current row.
    Delete = 4,
}

impl CapabilityRowPolicyOperationV1 {
    /// Decodes one closed durable operation tag.
    #[must_use]
    pub const fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            1 => Some(Self::Read),
            2 => Some(Self::Create),
            3 => Some(Self::Update),
            4 => Some(Self::Delete),
            _ => None,
        }
    }

    /// Returns the stable durable operation tag.
    #[must_use]
    pub const fn tag(self) -> u8 {
        self as u8
    }
}

/// One exact compiler-selected row policy for one protected entity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapabilityRowPolicyBindingV1 {
    lineage: ContractLineage,
    policy_name: RowPolicyName,
    entity_type: EntityTypeId,
    operations: Arc<[CapabilityRowPolicyOperationV1]>,
}

impl CapabilityRowPolicyBindingV1 {
    /// Constructs one canonical nonempty operation binding.
    pub fn new(
        lineage: ContractLineage,
        policy_name: RowPolicyName,
        entity_type: EntityTypeId,
        mut operations: Vec<CapabilityRowPolicyOperationV1>,
    ) -> Result<Self, CapabilityGrantError> {
        if operations.is_empty() {
            return Err(CapabilityGrantError::Empty);
        }
        if operations.len() > 4 {
            return Err(CapabilityGrantError::LimitExceeded);
        }
        operations.sort_unstable();
        if operations.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(CapabilityGrantError::Duplicate);
        }
        Ok(Self {
            lineage,
            policy_name,
            entity_type,
            operations: operations.into(),
        })
    }

    fn canonical_key(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        append_lineage(&mut bytes, &self.lineage);
        bytes.extend_from_slice(&self.entity_type.to_be_bytes());
        bytes.extend_from_slice(self.policy_name.as_str().as_bytes());
        bytes
    }

    fn entity_key(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        append_lineage(&mut bytes, &self.lineage);
        bytes.extend_from_slice(&self.entity_type.to_be_bytes());
        bytes
    }

    /// Exact contract lineage.
    #[must_use]
    pub const fn lineage(&self) -> &ContractLineage {
        &self.lineage
    }

    /// Exact symbolic policy name.
    #[must_use]
    pub const fn policy_name(&self) -> &RowPolicyName {
        &self.policy_name
    }

    /// Protected stable entity identity.
    #[must_use]
    pub const fn entity_type(&self) -> EntityTypeId {
        self.entity_type
    }

    /// Strictly increasing nonempty operation set.
    #[must_use]
    pub fn operations(&self) -> &[CapabilityRowPolicyOperationV1] {
        &self.operations
    }
}

/// Complete V1 row-policy authority extension carried only by CapabilityRecordV4.
#[derive(Clone, Eq, PartialEq)]
pub struct CapabilityRowPolicyGrantV1 {
    application_role_hash: ApplicationRoleHash,
    principal_facts: CapabilityPrincipalFactsV1,
    bindings: Arc<[CapabilityRowPolicyBindingV1]>,
}

impl CapabilityRowPolicyGrantV1 {
    /// Constructs a canonical complete role/fact/policy selection.
    pub fn new(
        application_role_hash: ApplicationRoleHash,
        principal_facts: CapabilityPrincipalFactsV1,
        mut bindings: Vec<CapabilityRowPolicyBindingV1>,
    ) -> Result<Self, CapabilityGrantError> {
        if bindings.is_empty() {
            return Err(CapabilityGrantError::Empty);
        }
        if bindings.len() > MAX_CAPABILITY_ROW_POLICY_BINDINGS {
            return Err(CapabilityGrantError::LimitExceeded);
        }
        bindings.sort_by_key(CapabilityRowPolicyBindingV1::canonical_key);
        if bindings
            .windows(2)
            .any(|pair| pair[0].entity_key() == pair[1].entity_key())
        {
            return Err(CapabilityGrantError::Duplicate);
        }
        Ok(Self {
            application_role_hash,
            principal_facts,
            bindings: bindings.into(),
        })
    }

    /// Exact compiled application-role identity.
    #[must_use]
    pub const fn application_role_hash(&self) -> ApplicationRoleHash {
        self.application_role_hash
    }

    /// Complete canonical current principal facts. Formatting remains redacted.
    #[doc(hidden)]
    #[must_use]
    pub const fn internal_principal_facts(&self) -> &CapabilityPrincipalFactsV1 {
        &self.principal_facts
    }

    /// Canonical compiler-selected policy bindings.
    #[must_use]
    pub fn bindings(&self) -> &[CapabilityRowPolicyBindingV1] {
        &self.bindings
    }

    /// True only when facts, policies, and operation classes hold or narrow.
    #[must_use]
    pub fn is_narrowing_of(&self, parent: &Self) -> bool {
        self.application_role_hash == parent.application_role_hash
            && self
                .principal_facts
                .is_narrowing_of(&parent.principal_facts)
            && self.bindings.iter().all(|child| {
                parent
                    .bindings
                    .binary_search_by_key(&child.canonical_key(), |binding| binding.canonical_key())
                    .ok()
                    .is_some_and(|index| {
                        child.operations.iter().all(|operation| {
                            parent.bindings[index]
                                .operations
                                .binary_search(operation)
                                .is_ok()
                        })
                    })
            })
    }
}

impl fmt::Debug for CapabilityRowPolicyGrantV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CapabilityRowPolicyGrantV1")
            .field("application_role_hash", &self.application_role_hash)
            .field("principal_facts", &"[REDACTED]")
            .field("binding_count", &self.bindings.len())
            .finish()
    }
}

/// Closed V1 scope for one lineage-scoped application export grant.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CapabilityApplicationExportScopeV1 {
    /// Apply current role, field-visibility, principal-fact, and row-policy authority.
    PrincipalFiltered,
    /// Explicit operator authority over the complete named application lineage.
    WholeApplication,
}

impl CapabilityApplicationExportScopeV1 {
    /// Returns the stable V1 semantic tag.
    #[must_use]
    pub const fn tag(self) -> u8 {
        match self {
            Self::PrincipalFiltered => 1,
            Self::WholeApplication => 2,
        }
    }

    /// Decodes a stable V1 semantic tag, rejecting unspecified and unknown values.
    #[must_use]
    pub const fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            1 => Some(Self::PrincipalFiltered),
            2 => Some(Self::WholeApplication),
            _ => None,
        }
    }
}

/// One explicit lineage, scope, and portable record-class export authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapabilityApplicationExportGrantV1 {
    lineage: ContractLineage,
    scope: CapabilityApplicationExportScopeV1,
    entities: bool,
    events: bool,
    provenance: bool,
    public_audit: bool,
}

impl CapabilityApplicationExportGrantV1 {
    /// Constructs one checked grant. Supporting records cannot be granted
    /// without at least one portable application-data class.
    #[allow(clippy::fn_params_excessive_bools)]
    pub fn new(
        lineage: ContractLineage,
        scope: CapabilityApplicationExportScopeV1,
        entities: bool,
        events: bool,
        provenance: bool,
        public_audit: bool,
    ) -> Result<Self, CapabilityGrantError> {
        if !entities && !events {
            return Err(CapabilityGrantError::InvalidShape);
        }
        Ok(Self {
            lineage,
            scope,
            entities,
            events,
            provenance,
            public_audit,
        })
    }

    /// Exact symbolic application lineage.
    #[must_use]
    pub const fn lineage(&self) -> &ContractLineage {
        &self.lineage
    }

    /// Selected current-policy or whole-application authority class.
    #[must_use]
    pub const fn scope(&self) -> CapabilityApplicationExportScopeV1 {
        self.scope
    }

    /// Whether symbolic entity records may be exported.
    #[must_use]
    pub const fn entities(&self) -> bool {
        self.entities
    }

    /// Whether symbolic durable events may be exported.
    #[must_use]
    pub const fn events(&self) -> bool {
        self.events
    }

    /// Whether separately protected provenance records may accompany the data.
    #[must_use]
    pub const fn provenance(&self) -> bool {
        self.provenance
    }

    /// Whether separately protected public-audit records may accompany the data.
    #[must_use]
    pub const fn public_audit(&self) -> bool {
        self.public_audit
    }

    fn narrows(&self, parent: &Self, child_has_row_policy: bool) -> bool {
        self.lineage == parent.lineage
            && match (self.scope, parent.scope) {
                (
                    CapabilityApplicationExportScopeV1::PrincipalFiltered,
                    CapabilityApplicationExportScopeV1::WholeApplication,
                ) => child_has_row_policy,
                (child, parent) => child == parent,
            }
            && (!self.entities || parent.entities)
            && (!self.events || parent.events)
            && (!self.provenance || parent.provenance)
            && (!self.public_audit || parent.public_audit)
    }
}

/// Complete canonical V1 application-export authority extension.
#[derive(Clone, Eq, PartialEq)]
pub struct CapabilityExportGrantV1 {
    applications: Arc<[CapabilityApplicationExportGrantV1]>,
}

impl CapabilityExportGrantV1 {
    /// Constructs one bounded lineage-ordered extension.
    pub fn new(
        mut applications: Vec<CapabilityApplicationExportGrantV1>,
    ) -> Result<Self, CapabilityGrantError> {
        if applications.is_empty() {
            return Err(CapabilityGrantError::Empty);
        }
        if applications.len() > MAX_CAPABILITY_APPLICATION_EXPORT_GRANTS {
            return Err(CapabilityGrantError::LimitExceeded);
        }
        applications.sort_by(|left, right| left.lineage.as_bytes().cmp(right.lineage.as_bytes()));
        if applications
            .windows(2)
            .any(|pair| pair[0].lineage == pair[1].lineage)
        {
            return Err(CapabilityGrantError::Duplicate);
        }
        Ok(Self {
            applications: applications.into(),
        })
    }

    /// Canonical lineage-ordered export grants.
    #[must_use]
    pub fn applications(&self) -> &[CapabilityApplicationExportGrantV1] {
        &self.applications
    }

    /// True only when every child lineage, scope, and class is equal or narrower.
    #[must_use]
    pub fn is_narrowing_of(&self, parent: &Self, child_has_row_policy: bool) -> bool {
        self.applications.iter().all(|child| {
            parent
                .applications
                .binary_search_by(|candidate| {
                    candidate.lineage.as_bytes().cmp(child.lineage.as_bytes())
                })
                .ok()
                .is_some_and(|index| {
                    child.narrows(&parent.applications[index], child_has_row_policy)
                })
        })
    }
}

impl fmt::Debug for CapabilityExportGrantV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CapabilityExportGrantV1")
            .field("application_count", &self.applications.len())
            .finish()
    }
}

/// Closed V1 scope for one exact application-reimport campaign grant.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CapabilityApplicationReimportScopeV1 {
    /// Apply the exact compiled application-role row policy during reconstitution.
    PrincipalFiltered,
    /// Operator authority over the complete application, still under the declared role policy.
    WholeApplication,
}

impl CapabilityApplicationReimportScopeV1 {
    /// Returns the stable V1 semantic tag.
    #[must_use]
    pub const fn tag(self) -> u8 {
        match self {
            Self::PrincipalFiltered => 1,
            Self::WholeApplication => 2,
        }
    }

    /// Decodes a stable V1 semantic tag.
    #[must_use]
    pub const fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            1 => Some(Self::PrincipalFiltered),
            2 => Some(Self::WholeApplication),
            _ => None,
        }
    }
}

/// Distinct authority for one exact not-ready reimport campaign and manifest.
#[derive(Clone, Eq, PartialEq)]
pub struct CapabilityApplicationReimportGrantV1 {
    lineage: ContractLineage,
    campaign_id: ApplicationInstallationCampaignId,
    portability_manifest_hash: ApplicationPortabilityManifestHash,
    scope: CapabilityApplicationReimportScopeV1,
}

impl CapabilityApplicationReimportGrantV1 {
    /// Constructs an exact campaign-bound reimport grant.
    #[must_use]
    pub const fn new(
        lineage: ContractLineage,
        campaign_id: ApplicationInstallationCampaignId,
        portability_manifest_hash: ApplicationPortabilityManifestHash,
        scope: CapabilityApplicationReimportScopeV1,
    ) -> Self {
        Self {
            lineage,
            campaign_id,
            portability_manifest_hash,
            scope,
        }
    }

    /// Exact application lineage.
    #[must_use]
    pub const fn lineage(&self) -> &ContractLineage {
        &self.lineage
    }

    /// Exact destination installation/reimport campaign.
    #[must_use]
    pub const fn campaign_id(&self) -> ApplicationInstallationCampaignId {
        self.campaign_id
    }

    /// Exact adapter-owned portability manifest.
    #[must_use]
    pub const fn portability_manifest_hash(&self) -> ApplicationPortabilityManifestHash {
        self.portability_manifest_hash
    }

    /// Current-policy or whole-application scope.
    #[must_use]
    pub const fn scope(&self) -> CapabilityApplicationReimportScopeV1 {
        self.scope
    }

    /// True only for the exact same campaign and manifest with equal or narrower scope.
    #[must_use]
    pub fn is_narrowing_of(&self, parent: &Self, child_has_row_policy: bool) -> bool {
        self.lineage == parent.lineage
            && self.campaign_id == parent.campaign_id
            && self.portability_manifest_hash == parent.portability_manifest_hash
            && match (self.scope, parent.scope) {
                (
                    CapabilityApplicationReimportScopeV1::PrincipalFiltered,
                    CapabilityApplicationReimportScopeV1::WholeApplication,
                ) => child_has_row_policy,
                (child, parent) => child == parent,
            }
    }
}

impl fmt::Debug for CapabilityApplicationReimportGrantV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CapabilityApplicationReimportGrantV1")
            .field("lineage", &self.lineage)
            .field("campaign_id", &self.campaign_id)
            .field("portability_manifest_hash", &self.portability_manifest_hash)
            .field("scope", &self.scope)
            .finish()
    }
}

/// One exact compiler-selected vector field and its count-disclosure posture.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapabilityVectorInspectionTargetV1 {
    lineage: ContractLineage,
    entity_type: EntityTypeId,
    field: FieldId,
    allow_counts: bool,
}

impl CapabilityVectorInspectionTargetV1 {
    /// Constructs an exact symbolic vector target lowered to stable contract IDs.
    #[must_use]
    pub const fn new(
        lineage: ContractLineage,
        entity_type: EntityTypeId,
        field: FieldId,
        allow_counts: bool,
    ) -> Self {
        Self {
            lineage,
            entity_type,
            field,
            allow_counts,
        }
    }

    fn canonical_key(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        append_lineage(&mut bytes, &self.lineage);
        bytes.extend_from_slice(&self.entity_type.to_be_bytes());
        bytes.extend_from_slice(&self.field.to_be_bytes());
        bytes
    }

    /// Exact contract lineage.
    #[must_use]
    pub const fn lineage(&self) -> &ContractLineage {
        &self.lineage
    }

    /// Exact stable entity identity.
    #[must_use]
    pub const fn entity_type(&self) -> EntityTypeId {
        self.entity_type
    }

    /// Exact stable vector-field identity.
    #[must_use]
    pub const fn field(&self) -> FieldId {
        self.field
    }

    /// Whether whole-partition maintained counts may be disclosed.
    #[must_use]
    pub const fn allow_counts(&self) -> bool {
        self.allow_counts
    }

    fn narrows(&self, parent: &Self) -> bool {
        self.lineage == parent.lineage
            && self.entity_type == parent.entity_type
            && self.field == parent.field
            && (!self.allow_counts || parent.allow_counts)
    }
}

/// Complete exact vector-inspection authority carried only by CapabilityRecordV8.
#[derive(Clone, Eq, PartialEq)]
pub struct CapabilityVectorInspectionGrantV1 {
    application_role_hash: ApplicationRoleHash,
    targets: Arc<[CapabilityVectorInspectionTargetV1]>,
}

impl CapabilityVectorInspectionGrantV1 {
    /// Constructs one role-bound, canonical, nonempty target set.
    pub fn new(
        application_role_hash: ApplicationRoleHash,
        mut targets: Vec<CapabilityVectorInspectionTargetV1>,
    ) -> Result<Self, CapabilityGrantError> {
        if targets.is_empty() {
            return Err(CapabilityGrantError::Empty);
        }
        if targets.len() > MAX_CAPABILITY_VECTOR_INSPECTION_TARGETS {
            return Err(CapabilityGrantError::LimitExceeded);
        }
        targets.sort_by_key(CapabilityVectorInspectionTargetV1::canonical_key);
        if targets
            .windows(2)
            .any(|pair| pair[0].canonical_key() == pair[1].canonical_key())
        {
            return Err(CapabilityGrantError::Duplicate);
        }
        Ok(Self {
            application_role_hash,
            targets: targets.into(),
        })
    }

    /// Exact compiled application-role identity.
    #[must_use]
    pub const fn application_role_hash(&self) -> ApplicationRoleHash {
        self.application_role_hash
    }

    /// Canonical exact vector targets.
    #[must_use]
    pub fn targets(&self) -> &[CapabilityVectorInspectionTargetV1] {
        &self.targets
    }

    /// Finds one exact target without exposing a broader wildcard.
    #[must_use]
    pub fn target(
        &self,
        lineage: &ContractLineage,
        entity_type: EntityTypeId,
        field: FieldId,
    ) -> Option<&CapabilityVectorInspectionTargetV1> {
        self.targets.iter().find(|target| {
            target.lineage() == lineage
                && target.entity_type() == entity_type
                && target.field() == field
        })
    }

    /// True only when every child target and count posture is equal or narrower.
    #[must_use]
    pub fn is_narrowing_of(&self, parent: &Self) -> bool {
        self.application_role_hash == parent.application_role_hash
            && self.targets.iter().all(|child| {
                parent
                    .targets
                    .binary_search_by_key(&child.canonical_key(), |target| target.canonical_key())
                    .ok()
                    .is_some_and(|index| child.narrows(&parent.targets[index]))
            })
    }
}

impl fmt::Debug for CapabilityVectorInspectionGrantV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CapabilityVectorInspectionGrantV1")
            .field("application_role_hash", &self.application_role_hash)
            .field("target_count", &self.targets.len())
            .finish()
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
    row_policy: Option<CapabilityRowPolicyGrantV1>,
    export: Option<CapabilityExportGrantV1>,
    reimport: Option<CapabilityApplicationReimportGrantV1>,
    vector_inspection: Option<CapabilityVectorInspectionGrantV1>,
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
            || approval_required.len() > 31
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
                .and_then(|count| count.checked_add(entry.secret_fields.len()))
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
            row_policy: None,
            export: None,
            reimport: None,
            vector_inspection: None,
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

    /// Adds the exact V4 row-policy extension after checking the matching role identity.
    pub fn with_row_policy(
        mut self,
        row_policy: CapabilityRowPolicyGrantV1,
    ) -> Result<Self, CapabilityGrantError> {
        if self.row_policy.is_some() {
            return Err(CapabilityGrantError::Duplicate);
        }
        let matching_roles = self
            .permissions
            .as_slice()
            .iter()
            .filter(|permission| {
                matches!(
                    permission,
                    CapabilityPermissionV1::ApplicationRoleIdentity(hash)
                        if *hash == row_policy.application_role_hash
                )
            })
            .count();
        let all_roles = self
            .permissions
            .as_slice()
            .iter()
            .filter(|permission| {
                matches!(
                    permission,
                    CapabilityPermissionV1::ApplicationRoleIdentity(_)
                )
            })
            .count();
        if matching_roles != 1 || all_roles != 1 {
            return Err(CapabilityGrantError::InvalidShape);
        }
        if self.vector_inspection.as_ref().is_some_and(|inspection| {
            inspection.targets().iter().any(|target| {
                target.allow_counts()
                    && row_policy.bindings().iter().any(|binding| {
                        binding.lineage() == target.lineage()
                            && binding.entity_type() == target.entity_type()
                    })
            })
        }) {
            return Err(CapabilityGrantError::InvalidShape);
        }
        self.row_policy = Some(row_policy);
        validate_capability_payload_bytes(self.semantic_bytes()?)?;
        Ok(self)
    }

    /// Exact trusted row-policy extension, absent for V1/V2/V3 capabilities.
    #[doc(hidden)]
    #[must_use]
    pub const fn internal_row_policy(&self) -> Option<&CapabilityRowPolicyGrantV1> {
        self.row_policy.as_ref()
    }

    /// Adds the exact V5 export extension after checking scope prerequisites.
    pub fn with_export(
        mut self,
        export: CapabilityExportGrantV1,
    ) -> Result<Self, CapabilityGrantError> {
        if self.export.is_some() {
            return Err(CapabilityGrantError::Duplicate);
        }
        for application in export.applications() {
            match application.scope() {
                CapabilityApplicationExportScopeV1::PrincipalFiltered
                    if self.row_policy.is_none() =>
                {
                    return Err(CapabilityGrantError::InvalidShape);
                }
                CapabilityApplicationExportScopeV1::WholeApplication
                    if self.tenant_scope != TenantScope::Global
                        || self.partition_scope != PartitionScopeV1::All =>
                {
                    return Err(CapabilityGrantError::InvalidShape);
                }
                CapabilityApplicationExportScopeV1::PrincipalFiltered
                | CapabilityApplicationExportScopeV1::WholeApplication => {}
            }
        }
        self.export = Some(export);
        validate_capability_payload_bytes(self.semantic_bytes()?)?;
        Ok(self)
    }

    /// Exact trusted export extension, absent for V1 through V4 capabilities.
    #[doc(hidden)]
    #[must_use]
    pub const fn internal_export(&self) -> Option<&CapabilityExportGrantV1> {
        self.export.as_ref()
    }

    /// Adds distinct V7 reimport authority after checking its role and scope prerequisites.
    pub fn with_reimport(
        mut self,
        reimport: CapabilityApplicationReimportGrantV1,
    ) -> Result<Self, CapabilityGrantError> {
        if self.reimport.is_some() || self.row_policy.is_none() {
            return Err(CapabilityGrantError::InvalidShape);
        }
        if reimport.scope() == CapabilityApplicationReimportScopeV1::WholeApplication
            && (self.tenant_scope != TenantScope::Global
                || self.partition_scope != PartitionScopeV1::All)
        {
            return Err(CapabilityGrantError::InvalidShape);
        }
        self.reimport = Some(reimport);
        validate_capability_payload_bytes(self.semantic_bytes()?)?;
        Ok(self)
    }

    /// Exact trusted reimport extension, absent for V1 through V6 capabilities.
    #[doc(hidden)]
    #[must_use]
    pub const fn internal_reimport(&self) -> Option<&CapabilityApplicationReimportGrantV1> {
        self.reimport.as_ref()
    }

    /// Adds exact V8 vector-inspection authority after checking role, permission,
    /// field visibility, and row-policy count-disclosure prerequisites.
    pub fn with_vector_inspection(
        mut self,
        inspection: CapabilityVectorInspectionGrantV1,
    ) -> Result<Self, CapabilityGrantError> {
        if self.vector_inspection.is_some()
            || !self
                .permissions
                .contains_exact(&CapabilityPermissionV1::unparameterized(
                    CapabilityPermissionKindV1::InspectVectorState,
                )?)
        {
            return Err(CapabilityGrantError::InvalidShape);
        }
        let matching_roles = self
            .permissions
            .as_slice()
            .iter()
            .filter(|permission| {
                matches!(
                    permission,
                    CapabilityPermissionV1::ApplicationRoleIdentity(hash)
                        if *hash == inspection.application_role_hash()
                )
            })
            .count();
        let all_roles = self
            .permissions
            .as_slice()
            .iter()
            .filter(|permission| {
                matches!(
                    permission,
                    CapabilityPermissionV1::ApplicationRoleIdentity(_)
                )
            })
            .count();
        if matching_roles != 1 || all_roles != 1 {
            return Err(CapabilityGrantError::InvalidShape);
        }
        for target in inspection.targets() {
            let visible = self.field_visibility.iter().any(|entry| {
                entry.lineage() == target.lineage()
                    && entry.entity_type() == target.entity_type()
                    && (entry.fields().binary_search(&target.field()).is_ok()
                        || entry.secret_fields().binary_search(&target.field()).is_ok())
            });
            let row_limited = self.row_policy.as_ref().is_some_and(|row_policy| {
                row_policy.bindings().iter().any(|binding| {
                    binding.lineage() == target.lineage()
                        && binding.entity_type() == target.entity_type()
                })
            });
            if !visible || (target.allow_counts() && row_limited) {
                return Err(CapabilityGrantError::InvalidShape);
            }
        }
        self.vector_inspection = Some(inspection);
        validate_capability_payload_bytes(self.semantic_bytes()?)?;
        Ok(self)
    }

    /// Exact trusted vector-inspection extension, absent for V1 through V7 capabilities.
    #[doc(hidden)]
    #[must_use]
    pub const fn internal_vector_inspection(&self) -> Option<&CapabilityVectorInspectionGrantV1> {
        self.vector_inspection.as_ref()
    }

    /// Returns the complete checked v1 semantic grant byte count.
    pub fn semantic_bytes(&self) -> Result<usize, CapabilityGrantError> {
        let base = capability_grant_semantic_bytes_parts(
            &self.tenant_scope,
            &self.partition_scope,
            &self.permissions,
            &self.field_visibility,
            &self.approval_required,
        )?;
        let with_row_policy = match &self.row_policy {
            Some(extension) => base
                .checked_add(row_policy_semantic_bytes(extension)?)
                .ok_or(CapabilityGrantError::SizeOverflow)?,
            None => base,
        };
        let with_export = match &self.export {
            Some(extension) => with_row_policy
                .checked_add(export_semantic_bytes(extension)?)
                .ok_or(CapabilityGrantError::SizeOverflow)?,
            None => with_row_policy,
        };
        let with_reimport = match &self.reimport {
            Some(extension) => with_export
                .checked_add(reimport_semantic_bytes(extension)?)
                .ok_or(CapabilityGrantError::SizeOverflow)?,
            None => with_export,
        };
        match &self.vector_inspection {
            Some(extension) => with_reimport
                .checked_add(vector_inspection_semantic_bytes(extension)?)
                .ok_or(CapabilityGrantError::SizeOverflow),
            None => Ok(with_reimport),
        }
    }
}

fn vector_inspection_semantic_bytes(
    extension: &CapabilityVectorInspectionGrantV1,
) -> Result<usize, CapabilityGrantError> {
    extension
        .targets()
        .iter()
        .try_fold(36usize, |total, target| {
            total
                .checked_add(framed_capability_bytes(target.lineage().as_bytes().len())?)
                .and_then(|value| value.checked_add(9))
                .ok_or(CapabilityGrantError::SizeOverflow)
        })
}

fn reimport_semantic_bytes(
    extension: &CapabilityApplicationReimportGrantV1,
) -> Result<usize, CapabilityGrantError> {
    checked_capability_sum([
        framed_capability_bytes(extension.lineage.as_bytes().len())?,
        16,
        32,
        1,
    ])
}

fn export_semantic_bytes(
    extension: &CapabilityExportGrantV1,
) -> Result<usize, CapabilityGrantError> {
    extension
        .applications
        .iter()
        .try_fold(4usize, |total, grant| {
            total
                .checked_add(framed_capability_bytes(grant.lineage.as_bytes().len())?)
                .and_then(|value| value.checked_add(5))
                .ok_or(CapabilityGrantError::SizeOverflow)
        })
}

fn row_policy_semantic_bytes(
    extension: &CapabilityRowPolicyGrantV1,
) -> Result<usize, CapabilityGrantError> {
    extension.bindings.iter().try_fold(
        32usize
            .checked_add(extension.principal_facts.internal_canonical_bytes().len())
            .and_then(|value| value.checked_add(4))
            .ok_or(CapabilityGrantError::SizeOverflow)?,
        |total, binding| {
            total
                .checked_add(binding.lineage.as_bytes().len())
                .and_then(|value| value.checked_add(binding.policy_name.as_str().len()))
                .and_then(|value| value.checked_add(4 + 4 + binding.operations.len()))
                .ok_or(CapabilityGrantError::SizeOverflow)
        },
    )
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
        CapabilityPermissionV1::InstallApplication(lineage) => 1 + 4 + lineage.as_bytes().len(),
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
            .checked_add(entry.secret_fields.len())
            .and_then(|count| count.checked_mul(4))
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
        for tag in 1..=32 {
            let kind = CapabilityPermissionKindV1::from_tag(tag).expect("known tag");
            assert_eq!(kind.tag(), tag);
        }
        assert_eq!(CapabilityPermissionKindV1::from_tag(0), None);
        assert_eq!(CapabilityPermissionKindV1::from_tag(33), None);
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
            CapabilityPermissionV1::unparameterized(
                CapabilityPermissionKindV1::InspectVectorState,
            )
            .is_ok()
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
    fn vector_inspection_is_role_field_and_count_bound() {
        let lineage = ContractLineage::new("ticketdesk").expect("lineage");
        let role = ApplicationRoleHash::from_bytes([9; 32]);
        let entity = EntityTypeId::first();
        let field = FieldId::first();
        let permissions = CapabilityPermissionsV1::new(vec![
            CapabilityPermissionV1::ApplicationRoleIdentity(role),
            CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::InspectVectorState)
                .expect("inspection permission"),
        ])
        .expect("permissions");
        let grant = CapabilityGrantV1::new(
            TenantScope::Global,
            PartitionScopeV1::All,
            permissions,
            vec![
                EntityFieldVisibilityV1::new(lineage.clone(), entity, vec![field])
                    .expect("visibility"),
            ],
            NonZeroU16::new(50).expect("limit"),
            Vec::new(),
        )
        .expect("grant");
        let counts = CapabilityVectorInspectionTargetV1::new(lineage.clone(), entity, field, true);
        let inspection =
            CapabilityVectorInspectionGrantV1::new(role, vec![counts.clone()]).expect("inspection");
        let granted = grant
            .clone()
            .with_vector_inspection(inspection)
            .expect("vector inspection grant");
        assert!(
            granted
                .internal_vector_inspection()
                .and_then(|value| value.target(&lineage, entity, field))
                .is_some_and(CapabilityVectorInspectionTargetV1::allow_counts)
        );

        let narrowed = CapabilityVectorInspectionGrantV1::new(
            role,
            vec![CapabilityVectorInspectionTargetV1::new(
                lineage.clone(),
                entity,
                field,
                false,
            )],
        )
        .expect("narrowed");
        assert!(
            narrowed.is_narrowing_of(
                granted
                    .internal_vector_inspection()
                    .expect("parent inspection")
            )
        );

        let row_policy = CapabilityRowPolicyGrantV1::new(
            role,
            CapabilityPrincipalFactsV1::empty(),
            vec![
                CapabilityRowPolicyBindingV1::new(
                    lineage,
                    RowPolicyName::new("ticket_visibility").expect("policy"),
                    entity,
                    vec![CapabilityRowPolicyOperationV1::Read],
                )
                .expect("binding"),
            ],
        )
        .expect("row policy");
        assert_eq!(
            granted.with_row_policy(row_policy),
            Err(CapabilityGrantError::InvalidShape)
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

        let lineage = ContractLineage::new("ticketdesk").expect("lineage");
        let command = CapabilityPermissionV1::InvokeCommand(lineage, CommandId::first());
        let exact = CapabilityPermissionsV1::new(vec![command.clone()]).expect("permissions");
        assert!(exact.contains_exact(&command));
        assert!(
            !exact.contains_exact(
                &CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::ReadHealth)
                    .expect("permission")
            )
        );
    }

    #[test]
    fn retained_permission_keys_cover_the_closed_registry_exactly() {
        let lineage = ContractLineage::new("ticketdesk").expect("lineage");
        let query_module = QueryModuleHash::from_bytes([3; 32]);
        let query_name = QueryOperationName::new("TicketPage").expect("query name");
        let reactive_module = ReactiveModuleHash::from_bytes([4; 32]);
        let reactive_name = ReactiveOperationName::new("TicketActivity").expect("reactive name");
        let mut values = (1..=32)
            .filter_map(CapabilityPermissionKindV1::from_tag)
            .filter_map(|kind| CapabilityPermissionV1::unparameterized(kind).ok())
            .collect::<Vec<_>>();
        values.extend([
            CapabilityPermissionV1::ExplainCommand(lineage.clone(), CommandId::first()),
            CapabilityPermissionV1::InvokeCommand(lineage.clone(), CommandId::first()),
            CapabilityPermissionV1::ReadEntity(lineage.clone(), EntityTypeId::first()),
            CapabilityPermissionV1::ScanIndex(lineage.clone(), IndexId::first()),
            CapabilityPermissionV1::QueryProjection(lineage.clone(), ProjectionId::first()),
            CapabilityPermissionV1::ReadProjectionStatus(lineage.clone(), ProjectionId::first()),
            CapabilityPermissionV1::ExplainNamedQuery(
                lineage.clone(),
                query_module,
                query_name.clone(),
            ),
            CapabilityPermissionV1::ExecuteNamedQuery(lineage.clone(), query_module, query_name),
            CapabilityPermissionV1::ApplicationRoleIdentity(ApplicationRoleHash::from_bytes(
                [5; 32],
            )),
            CapabilityPermissionV1::MigrateContract(lineage.clone()),
            CapabilityPermissionV1::ConsumeEventStream(
                lineage.clone(),
                reactive_module,
                reactive_name.clone(),
            ),
            CapabilityPermissionV1::SeekEventStreamConsumer(
                lineage.clone(),
                reactive_module,
                reactive_name.clone(),
            ),
            CapabilityPermissionV1::WatchNamedQuery(
                lineage.clone(),
                reactive_module,
                reactive_name.clone(),
            ),
            CapabilityPermissionV1::ConsumeContextualSubscription(
                lineage.clone(),
                reactive_module,
                reactive_name,
            ),
            CapabilityPermissionV1::InstallApplication(lineage.clone()),
        ]);

        let permissions = CapabilityPermissionsV1::new(values).expect("complete permission set");
        assert_eq!(permissions.0.len(), permissions.1.len());
        for (permission, retained_key) in permissions.0.iter().zip(permissions.1.iter()) {
            assert_eq!(retained_key, &permission.canonical_key());
            assert!(permissions.contains_exact(permission));
        }
        assert!(permissions.1.windows(2).all(|pair| pair[0] < pair[1]));
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
        assert!(Arc::ptr_eq(&original.permissions.1, &cloned.permissions.1));
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
        assert!(!Arc::ptr_eq(
            &original.permissions.1,
            &independently_built.permissions.1
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

    #[test]
    fn row_policy_grants_are_role_bound_canonical_and_narrowing_only() {
        fn facts(groups: &[&str]) -> CapabilityPrincipalFactsV1 {
            CapabilityPrincipalFactsV1::new(vec![
                crate::CapabilityPrincipalFactV1::new(
                    "groups",
                    crate::CanonicalValue::list(
                        groups
                            .iter()
                            .map(|group| crate::CanonicalValue::string(*group).expect("text"))
                            .collect(),
                    )
                    .expect("list"),
                )
                .expect("fact"),
            ])
            .expect("facts")
        }

        let role = ApplicationRoleHash::from_bytes([0x51; 32]);
        let lineage = ContractLineage::new("ticketdesk").expect("lineage");
        let parent_binding = CapabilityRowPolicyBindingV1::new(
            lineage.clone(),
            RowPolicyName::new("TicketVisible").expect("policy"),
            EntityTypeId::first(),
            vec![
                CapabilityRowPolicyOperationV1::Update,
                CapabilityRowPolicyOperationV1::Read,
            ],
        )
        .expect("binding");
        assert_eq!(
            parent_binding.operations(),
            [
                CapabilityRowPolicyOperationV1::Read,
                CapabilityRowPolicyOperationV1::Update,
            ]
        );
        let parent = CapabilityRowPolicyGrantV1::new(
            role,
            facts(&["authors", "operators"]),
            vec![parent_binding],
        )
        .expect("parent extension");
        let child = CapabilityRowPolicyGrantV1::new(
            role,
            facts(&["authors"]),
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
        .expect("child extension");
        assert!(child.is_narrowing_of(&parent));

        let duplicate_entity = CapabilityRowPolicyGrantV1::new(
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
                CapabilityRowPolicyBindingV1::new(
                    lineage,
                    RowPolicyName::new("TicketOwned").expect("policy"),
                    EntityTypeId::first(),
                    vec![CapabilityRowPolicyOperationV1::Read],
                )
                .expect("binding"),
            ],
        );
        assert_eq!(duplicate_entity, Err(CapabilityGrantError::Duplicate));

        let base = CapabilityGrantV1::new(
            TenantScope::Global,
            PartitionScopeV1::All,
            CapabilityPermissionsV1::new(vec![CapabilityPermissionV1::ApplicationRoleIdentity(
                ApplicationRoleHash::from_bytes([0x52; 32]),
            )])
            .expect("permissions"),
            Vec::new(),
            NonZeroU16::MIN,
            Vec::new(),
        )
        .expect("grant");
        assert_eq!(
            base.with_row_policy(parent),
            Err(CapabilityGrantError::InvalidShape)
        );
    }

    #[test]
    fn export_grants_are_canonical_bounded_and_explicit() {
        let whole = CapabilityApplicationExportGrantV1::new(
            ContractLineage::new("zeta").expect("lineage"),
            CapabilityApplicationExportScopeV1::WholeApplication,
            true,
            false,
            true,
            true,
        )
        .expect("whole grant");
        let principal = CapabilityApplicationExportGrantV1::new(
            ContractLineage::new("alpha").expect("lineage"),
            CapabilityApplicationExportScopeV1::PrincipalFiltered,
            false,
            true,
            false,
            false,
        )
        .expect("principal grant");
        let extension = CapabilityExportGrantV1::new(vec![whole.clone(), principal.clone()])
            .expect("canonical extension");
        assert_eq!(extension.applications()[0].lineage().as_str(), "alpha");
        assert_eq!(extension.applications()[1].lineage().as_str(), "zeta");

        assert_eq!(
            CapabilityApplicationExportGrantV1::new(
                ContractLineage::new("empty").expect("lineage"),
                CapabilityApplicationExportScopeV1::WholeApplication,
                false,
                false,
                true,
                true,
            ),
            Err(CapabilityGrantError::InvalidShape)
        );
        assert_eq!(
            CapabilityExportGrantV1::new(vec![whole.clone(), whole]),
            Err(CapabilityGrantError::Duplicate)
        );
        assert_eq!(
            CapabilityExportGrantV1::new(Vec::new()),
            Err(CapabilityGrantError::Empty)
        );
        let over_bound = (0..=MAX_CAPABILITY_APPLICATION_EXPORT_GRANTS)
            .map(|index| {
                CapabilityApplicationExportGrantV1::new(
                    ContractLineage::new(format!("lineage_{index:03}")).expect("lineage"),
                    CapabilityApplicationExportScopeV1::WholeApplication,
                    true,
                    false,
                    false,
                    false,
                )
                .expect("application grant")
            })
            .collect();
        assert_eq!(
            CapabilityExportGrantV1::new(over_bound),
            Err(CapabilityGrantError::LimitExceeded)
        );
        assert_eq!(CapabilityApplicationExportScopeV1::from_tag(0), None);
        assert_eq!(CapabilityApplicationExportScopeV1::from_tag(3), None);
    }

    #[test]
    fn reimport_authority_is_exact_campaign_bound_and_narrowing_only() {
        let mut campaign_bytes = [0x61; 16];
        campaign_bytes[6] = 0x71;
        campaign_bytes[8] = 0x81;
        let campaign =
            ApplicationInstallationCampaignId::from_bytes(campaign_bytes).expect("UUIDv7 campaign");
        let parent = CapabilityApplicationReimportGrantV1::new(
            ContractLineage::new("ticketdesk").expect("lineage"),
            campaign,
            ApplicationPortabilityManifestHash::from_bytes([0x62; 32]),
            CapabilityApplicationReimportScopeV1::WholeApplication,
        );
        let principal = CapabilityApplicationReimportGrantV1::new(
            ContractLineage::new("ticketdesk").expect("lineage"),
            campaign,
            ApplicationPortabilityManifestHash::from_bytes([0x62; 32]),
            CapabilityApplicationReimportScopeV1::PrincipalFiltered,
        );
        let substituted = CapabilityApplicationReimportGrantV1::new(
            ContractLineage::new("ticketdesk").expect("lineage"),
            campaign,
            ApplicationPortabilityManifestHash::from_bytes([0x63; 32]),
            CapabilityApplicationReimportScopeV1::PrincipalFiltered,
        );

        assert!(principal.is_narrowing_of(&parent, true));
        assert!(!principal.is_narrowing_of(&parent, false));
        assert!(!parent.is_narrowing_of(&principal, true));
        assert!(!substituted.is_narrowing_of(&parent, true));
        assert_eq!(CapabilityApplicationReimportScopeV1::from_tag(0), None);
        assert_eq!(CapabilityApplicationReimportScopeV1::from_tag(3), None);
    }

    #[test]
    fn export_scope_requires_distinct_current_authority() {
        let role = ApplicationRoleHash::from_bytes([0x71; 32]);
        let base = || {
            CapabilityGrantV1::new(
                TenantScope::Global,
                PartitionScopeV1::All,
                CapabilityPermissionsV1::new(vec![
                    CapabilityPermissionV1::ApplicationRoleIdentity(role),
                ])
                .expect("permissions"),
                Vec::new(),
                NonZeroU16::MIN,
                Vec::new(),
            )
            .expect("base grant")
        };
        let principal_export = || {
            CapabilityExportGrantV1::new(vec![
                CapabilityApplicationExportGrantV1::new(
                    ContractLineage::new("ticketdesk").expect("lineage"),
                    CapabilityApplicationExportScopeV1::PrincipalFiltered,
                    true,
                    true,
                    false,
                    false,
                )
                .expect("application grant"),
            ])
            .expect("export grant")
        };
        assert_eq!(
            base().with_export(principal_export()),
            Err(CapabilityGrantError::InvalidShape)
        );

        let policy = CapabilityRowPolicyGrantV1::new(
            role,
            CapabilityPrincipalFactsV1::empty(),
            vec![
                CapabilityRowPolicyBindingV1::new(
                    ContractLineage::new("ticketdesk").expect("lineage"),
                    RowPolicyName::new("TicketVisible").expect("policy"),
                    EntityTypeId::first(),
                    vec![CapabilityRowPolicyOperationV1::Read],
                )
                .expect("binding"),
            ],
        )
        .expect("policy extension");
        let protected = base()
            .with_row_policy(policy)
            .expect("protected grant")
            .with_export(principal_export())
            .expect("principal export authority");
        assert_eq!(
            protected.internal_export().expect("export").applications()[0].scope(),
            CapabilityApplicationExportScopeV1::PrincipalFiltered
        );

        let tenant_bound = CapabilityGrantV1::new(
            TenantScope::Tenant(crate::TenantId::new("tenant-a").expect("tenant")),
            PartitionScopeV1::All,
            CapabilityPermissionsV1::new(Vec::new()).expect("permissions"),
            Vec::new(),
            NonZeroU16::MIN,
            Vec::new(),
        )
        .expect("tenant-bound grant");
        let whole_export = CapabilityExportGrantV1::new(vec![
            CapabilityApplicationExportGrantV1::new(
                ContractLineage::new("ticketdesk").expect("lineage"),
                CapabilityApplicationExportScopeV1::WholeApplication,
                true,
                true,
                true,
                true,
            )
            .expect("application grant"),
        ])
        .expect("export grant");
        assert_eq!(
            tenant_bound.with_export(whole_export),
            Err(CapabilityGrantError::InvalidShape)
        );
    }
}
