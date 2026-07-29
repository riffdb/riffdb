//! Closed typed facts for the 22 application-service operations.

use std::{error::Error, fmt, num::NonZeroU16};

use riffdb_types::{
    ActorId, CanonicalValue, CapabilityPermissionKindV1, CapabilityPermissionV1, CommandId,
    CommitSequence, ContractBundleHash, ContractLineage, ContractVersion, EntityTypeId, FieldId,
    IndexId, MAX_CAPABILITY_FIELD_VISIBILITY, MAX_PROJECTION_GROUP_COMPONENTS, PartitionKey,
    ProjectionGeneration, ProjectionGroupPrefixBuilder, ProjectionId, ProjectionIdentity,
    ProvenanceId, ScopedPartitionV1, ServiceOperationV1, TenantScope,
};

use crate::{
    AbsentCapabilityRevokeTargetFacts, AuditClass, CapabilityCreateTargetFacts,
    CapabilityMutationRequest, CapabilityRevokeTargetFacts, OutputClassification,
    RevocationReasonCodeV1,
};

/// Whether an execute target is a command mutation or an unjournaled command read.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CommandExecutionClass {
    /// The checked plan is read-only.
    ReadOnly,
    /// The checked plan may produce authoritative mutations.
    Mutation,
}

/// Checked static tenant scope supplied by the POC grammar or a service schema.
///
/// This is not an authorization decision. It is a policy-owned proof that the
/// operation's static semantics require the retained scope. Grammar v1 has no
/// tenant mapping, so its only constructible command scope is global. A future
/// mapped scope requires a separately reviewed constructor and lowering.
#[derive(Clone, Eq, PartialEq)]
pub struct OperationTenantScope(TenantScope);

static GLOBAL_ONLY_OPERATION_SCOPE: OperationTenantScope = OperationTenantScope::global_only();

impl OperationTenantScope {
    /// Constructs the exact global scope required by grammar-v1 commands.
    #[must_use]
    pub const fn grammar_v1_global() -> Self {
        Self(TenantScope::Global)
    }

    /// Constructs the fail-closed global scope used by current POC data reads.
    ///
    /// Command paths should prefer [`Self::grammar_v1_global`] so the source of
    /// the static scope remains explicit at the service boundary.
    #[must_use]
    pub const fn global_only() -> Self {
        Self::grammar_v1_global()
    }

    /// Borrows the exact tenant scope required by this static proof.
    #[must_use]
    pub const fn tenant_scope(&self) -> &TenantScope {
        &self.0
    }
}

impl fmt::Debug for OperationTenantScope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OperationTenantScope::GlobalOnly")
    }
}

/// The one selector accepted by provenance tracing.
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum ProvenanceSelector {
    /// Trace from one exact application commit.
    Commit(CommitSequence),
    /// Trace from one exact provenance record.
    Provenance(ProvenanceId),
}

impl fmt::Debug for ProvenanceSelector {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProvenanceSelector([REDACTED])")
    }
}

/// One resource candidate checked independently during policy-filtered discovery.
#[derive(Clone, Eq, PartialEq)]
pub enum DiscoveryResource {
    /// Active contract metadata.
    ActiveContract,
    /// One historical contract version.
    ContractVersion {
        /// Exact contract lineage.
        lineage: ContractLineage,
        /// Exact nonzero contract version.
        version: ContractVersion,
    },
    /// One compiled command plan.
    CommandPlan {
        /// Exact contract lineage.
        lineage: ContractLineage,
        /// Exact lineage-scoped command identity.
        command_id: CommandId,
    },
    /// Generated documentation for one compiled command.
    CommandDocumentation {
        /// Exact contract lineage.
        lineage: ContractLineage,
        /// Exact lineage-scoped command identity.
        command_id: CommandId,
    },
    /// One command outcome resource.
    CommandOutcome {
        /// Exact contract lineage.
        lineage: ContractLineage,
        /// Exact lineage-scoped command identity.
        command_id: CommandId,
    },
    /// One checked entity-schema resource and its complete non-key field set.
    EntitySchema(EntitySchemaCandidate),
    /// One projection status resource.
    ProjectionStatus {
        /// Exact contract lineage.
        lineage: ContractLineage,
        /// Exact lineage-scoped projection.
        projection_id: ProjectionId,
    },
    /// One commit resource class.
    Commit,
    /// One provenance resource class.
    Provenance,
    /// Server health metadata.
    Health,
}

impl fmt::Debug for DiscoveryResource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::ActiveContract => "ActiveContract",
            Self::ContractVersion { .. } => "ContractVersion",
            Self::CommandPlan { .. } => "CommandPlan",
            Self::CommandDocumentation { .. } => "CommandDocumentation",
            Self::CommandOutcome { .. } => "CommandOutcome",
            Self::EntitySchema(_) => "EntitySchema",
            Self::ProjectionStatus { .. } => "ProjectionStatus",
            Self::Commit => "Commit",
            Self::Provenance => "Provenance",
            Self::Health => "Health",
        };
        write!(formatter, "DiscoveryResource::{name}([REDACTED])")
    }
}

/// Target-bound checked fields for one entity-schema discovery candidate.
#[derive(Clone, Eq, PartialEq)]
pub struct EntitySchemaCandidate(FieldRequest);

impl EntitySchemaCandidate {
    /// Constructs one bounded canonical schema-field candidate.
    pub fn new(
        lineage: ContractLineage,
        entity_type_id: EntityTypeId,
        non_key_fields: Vec<FieldId>,
    ) -> Result<Self, OperationRequestError> {
        FieldRequest::new(lineage, entity_type_id, non_key_fields).map(Self)
    }

    /// Returns the exact candidate lineage.
    #[must_use]
    pub const fn lineage(&self) -> &ContractLineage {
        &self.0.lineage
    }

    /// Returns the exact entity type.
    #[must_use]
    pub const fn entity_type_id(&self) -> EntityTypeId {
        self.0.entity_type_id
    }

    /// Returns every declared non-key field in increasing stable-ID order.
    #[must_use]
    pub fn non_key_fields(&self) -> &[FieldId] {
        &self.0.non_key_fields
    }
}

impl fmt::Debug for EntitySchemaCandidate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("EntitySchemaCandidate([REDACTED])")
    }
}

/// One SPEC POC fixed MCP tool considered for policy-filtered discovery.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum FixedToolCandidate {
    /// `riffdb.contract.validate`.
    ValidateContract,
    /// `riffdb.contract.get_active`.
    GetActiveContract,
    /// `riffdb.contract.explain_command`.
    ExplainCommand,
    /// `riffdb.contract.deploy`.
    DeployContract,
    /// `riffdb.command.get_outcome`.
    ResolveCommandOutcome,
    /// `riffdb.entity.get`.
    GetEntity,
    /// `riffdb.entity.scan_index`.
    ScanIndex,
    /// `riffdb.commit.get`.
    GetCommit,
    /// `riffdb.commit.scan`.
    ScanCommits,
    /// `riffdb.provenance.trace`.
    TraceProvenance,
    /// `riffdb.projection.query`.
    QueryProjection,
    /// `riffdb.projection.status`.
    GetProjectionStatus,
    /// `riffdb.outbox.list_pending`.
    ListPendingOutboxDeliveries,
    /// `riffdb.server.health`.
    GetHealth,
    /// `riffdb.contract.describe`.
    DescribeContract,
    /// `riffdb.query.check`.
    CheckQuery,
    /// `riffdb.query.explain`.
    ExplainQuery,
    /// `riffdb.query`.
    ExecuteQuery,
}

impl FixedToolCandidate {
    /// The exact SPEC POC fixed-tool inventory in stable presentation order.
    pub const ALL: [Self; 18] = [
        Self::ValidateContract,
        Self::GetActiveContract,
        Self::ExplainCommand,
        Self::DeployContract,
        Self::ResolveCommandOutcome,
        Self::GetEntity,
        Self::ScanIndex,
        Self::GetCommit,
        Self::ScanCommits,
        Self::TraceProvenance,
        Self::QueryProjection,
        Self::GetProjectionStatus,
        Self::ListPendingOutboxDeliveries,
        Self::GetHealth,
        Self::DescribeContract,
        Self::CheckQuery,
        Self::ExplainQuery,
        Self::ExecuteQuery,
    ];
}

/// One compiled command considered for tool discovery.
#[derive(Clone, Eq, PartialEq)]
pub struct CommandToolCandidate {
    lineage: ContractLineage,
    command_id: CommandId,
}

impl CommandToolCandidate {
    /// Constructs one exact lineage-scoped command candidate.
    #[must_use]
    pub const fn new(lineage: ContractLineage, command_id: CommandId) -> Self {
        Self {
            lineage,
            command_id,
        }
    }

    /// Returns the exact candidate lineage.
    #[must_use]
    pub const fn lineage(&self) -> &ContractLineage {
        &self.lineage
    }

    /// Returns the exact candidate command identity.
    #[must_use]
    pub const fn command_id(&self) -> CommandId {
        self.command_id
    }
}

impl fmt::Debug for CommandToolCandidate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CommandToolCandidate([REDACTED])")
    }
}

/// Safe failure to construct checked field-selection facts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperationRequestError {
    /// The same stable field ID was supplied more than once.
    DuplicateField,
    /// The field selection exceeds the capability hard bound.
    TooManyFields,
    /// A projection selector exceeds the checked foundational component bound.
    TooManyProjectionComponents,
    /// A projection selector is not a bounded canonical scalar prefix.
    InvalidProjectionSelector,
}

impl fmt::Display for OperationRequestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::DuplicateField => "operation field selection contains a duplicate",
            Self::TooManyFields => "operation field selection exceeds the hard limit",
            Self::TooManyProjectionComponents => {
                "projection selector exceeds the hard component limit"
            }
            Self::InvalidProjectionSelector => {
                "projection selector is not a bounded canonical prefix"
            }
        })
    }
}

impl Error for OperationRequestError {}

#[derive(Clone, Eq, PartialEq)]
struct FieldRequest {
    lineage: ContractLineage,
    entity_type_id: EntityTypeId,
    non_key_fields: Vec<FieldId>,
}

impl FieldRequest {
    fn new(
        lineage: ContractLineage,
        entity_type_id: EntityTypeId,
        mut non_key_fields: Vec<FieldId>,
    ) -> Result<Self, OperationRequestError> {
        if non_key_fields.len() > MAX_CAPABILITY_FIELD_VISIBILITY {
            return Err(OperationRequestError::TooManyFields);
        }
        non_key_fields.sort_unstable();
        if non_key_fields.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(OperationRequestError::DuplicateField);
        }
        Ok(Self {
            lineage,
            entity_type_id,
            non_key_fields,
        })
    }
}

#[derive(Clone, Eq, PartialEq)]
struct ExactDataScope {
    tenant_scope: OperationTenantScope,
    partition: ScopedPartitionV1,
}

impl ExactDataScope {
    fn new(
        tenant_scope: OperationTenantScope,
        lineage: ContractLineage,
        partition: PartitionKey,
    ) -> Self {
        Self {
            tenant_scope,
            partition: ScopedPartitionV1::new(lineage, partition),
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
enum OperationKind {
    ValidateContract,
    ExplainCommand {
        lineage: ContractLineage,
        version: ContractVersion,
        command_id: CommandId,
    },
    DeployContract {
        lineage: ContractLineage,
        version: ContractVersion,
        bundle_hash: ContractBundleHash,
        expected_active_version: Option<ContractVersion>,
    },
    GetActiveContract,
    GetContractVersion {
        lineage: ContractLineage,
        version: ContractVersion,
    },
    ExecuteCommand {
        lineage: ContractLineage,
        version: ContractVersion,
        command_id: CommandId,
        class: CommandExecutionClass,
        scope: ExactDataScope,
    },
    ResolveCommandOutcomePreLookup {
        lineage: ContractLineage,
        command_id: CommandId,
        tenant_scope: OperationTenantScope,
    },
    ResolveCommandOutcome {
        lineage: ContractLineage,
        version: ContractVersion,
        command_id: CommandId,
        owner_principal_id: ActorId,
        owner_tenant_scope: TenantScope,
        scope: ExactDataScope,
    },
    GetEntity {
        lineage: ContractLineage,
        version: ContractVersion,
        entity_type_id: EntityTypeId,
        scope: ExactDataScope,
        fields: FieldRequest,
    },
    ScanIndex {
        lineage: ContractLineage,
        version: ContractVersion,
        index_id: IndexId,
        tenant_scope: OperationTenantScope,
        fields: FieldRequest,
        requested_rows: NonZeroU16,
    },
    QueryProjection {
        version: ContractVersion,
        identity: ProjectionIdentity,
        leading_components: Vec<CanonicalValue>,
        tenant_scope: OperationTenantScope,
        requested_rows: NonZeroU16,
    },
    GetProjectionStatus {
        lineage: ContractLineage,
        version: ContractVersion,
        projection_id: ProjectionId,
    },
    GetCommit {
        sequence: CommitSequence,
    },
    ScanCommits {
        requested_rows: NonZeroU16,
    },
    SubscribeToCommits,
    TraceProvenance {
        selector: ProvenanceSelector,
    },
    GetHealth,
    GetStatistics,
    CreateCapability {
        target: CapabilityCreateTargetFacts,
    },
    RevokeCapability {
        target: CapabilityRevokeTargetFacts,
        reason: RevocationReasonCodeV1,
    },
    RevokeAbsentCapability {
        target: AbsentCapabilityRevokeTargetFacts,
        reason: RevocationReasonCodeV1,
    },
    ListPendingOutboxDeliveries {
        requested_rows: NonZeroU16,
    },
    DiscoverCommandTools,
    DiscoverResources,
    DescribeContract,
    CheckQuery,
    ExplainQuery,
    ExecuteQuery,
}

/// Checked policy facts for exactly one closed application-service operation.
#[derive(Clone, Eq, PartialEq)]
pub struct OperationRequest(OperationKind);

impl OperationRequest {
    /// Constructs a contract-validation request.
    #[must_use]
    pub const fn validate_contract() -> Self {
        Self(OperationKind::ValidateContract)
    }

    /// Constructs an exact command-explanation request.
    #[must_use]
    pub const fn explain_command(
        lineage: ContractLineage,
        version: ContractVersion,
        command_id: CommandId,
    ) -> Self {
        Self(OperationKind::ExplainCommand {
            lineage,
            version,
            command_id,
        })
    }

    /// Constructs an exact contract-deployment request.
    #[must_use]
    pub const fn deploy_contract(
        lineage: ContractLineage,
        version: ContractVersion,
        bundle_hash: ContractBundleHash,
        expected_active_version: Option<ContractVersion>,
    ) -> Self {
        Self(OperationKind::DeployContract {
            lineage,
            version,
            bundle_hash,
            expected_active_version,
        })
    }

    /// Constructs an active-contract metadata request.
    #[must_use]
    pub const fn get_active_contract() -> Self {
        Self(OperationKind::GetActiveContract)
    }

    /// Constructs an exact historical-contract request.
    #[must_use]
    pub const fn get_contract_version(lineage: ContractLineage, version: ContractVersion) -> Self {
        Self(OperationKind::GetContractVersion { lineage, version })
    }

    /// Constructs a classified exact command invocation.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn execute_command(
        lineage: ContractLineage,
        version: ContractVersion,
        command_id: CommandId,
        class: CommandExecutionClass,
        partition: PartitionKey,
    ) -> Self {
        let scope = ExactDataScope::new(
            OperationTenantScope::grammar_v1_global(),
            lineage.clone(),
            partition,
        );
        Self(OperationKind::ExecuteCommand {
            lineage,
            version,
            command_id,
            class,
            scope,
        })
    }

    /// Constructs the authorization request required before outcome lookup.
    ///
    /// An allow decision for this request permits only the bounded internal
    /// lookup. A present outcome must be authorized again with
    /// [`Self::resolve_command_outcome`] and every exact stored fact before any
    /// protected result is returned.
    #[must_use]
    pub const fn resolve_command_outcome_pre_lookup(
        lineage: ContractLineage,
        command_id: CommandId,
    ) -> Self {
        Self(OperationKind::ResolveCommandOutcomePreLookup {
            lineage,
            command_id,
            tenant_scope: OperationTenantScope::grammar_v1_global(),
        })
    }

    /// Constructs an exact historical outcome-resolution request.
    #[must_use]
    pub fn resolve_command_outcome(
        lineage: ContractLineage,
        version: ContractVersion,
        command_id: CommandId,
        owner_principal_id: ActorId,
        owner_tenant_scope: TenantScope,
        partition: PartitionKey,
    ) -> Self {
        let scope = ExactDataScope::new(
            OperationTenantScope::grammar_v1_global(),
            lineage.clone(),
            partition,
        );
        Self(OperationKind::ResolveCommandOutcome {
            lineage,
            version,
            command_id,
            owner_principal_id,
            owner_tenant_scope,
            scope,
        })
    }

    /// Constructs an exact entity read with requested non-key fields.
    #[allow(clippy::too_many_arguments)]
    pub fn get_entity(
        lineage: ContractLineage,
        version: ContractVersion,
        entity_type_id: EntityTypeId,
        tenant_scope: OperationTenantScope,
        partition: PartitionKey,
        non_key_fields: Vec<FieldId>,
    ) -> Result<Self, OperationRequestError> {
        let scope = ExactDataScope::new(tenant_scope, lineage.clone(), partition);
        let fields = FieldRequest::new(lineage.clone(), entity_type_id, non_key_fields)?;
        Ok(Self(OperationKind::GetEntity {
            lineage,
            version,
            entity_type_id,
            scope,
            fields,
        }))
    }

    /// Constructs an index scan and its exact returned entity-field request.
    #[allow(clippy::too_many_arguments)]
    pub fn scan_index(
        lineage: ContractLineage,
        version: ContractVersion,
        index_id: IndexId,
        result_entity_type_id: EntityTypeId,
        tenant_scope: OperationTenantScope,
        non_key_fields: Vec<FieldId>,
        requested_rows: NonZeroU16,
    ) -> Result<Self, OperationRequestError> {
        let fields = FieldRequest::new(lineage.clone(), result_entity_type_id, non_key_fields)?;
        Ok(Self(OperationKind::ScanIndex {
            lineage,
            version,
            index_id,
            tenant_scope,
            fields,
            requested_rows,
        }))
    }

    /// Constructs a bounded projection query.
    pub fn query_projection(
        version: ContractVersion,
        identity: ProjectionIdentity,
        leading_components: Vec<CanonicalValue>,
        requested_rows: NonZeroU16,
    ) -> Result<Self, OperationRequestError> {
        if leading_components.len() > MAX_PROJECTION_GROUP_COMPONENTS {
            return Err(OperationRequestError::TooManyProjectionComponents);
        }
        let mut selector =
            ProjectionGroupPrefixBuilder::new(identity.clone(), ProjectionGeneration::first());
        for component in leading_components {
            selector
                .push_component(component)
                .map_err(|_| OperationRequestError::InvalidProjectionSelector)?;
        }
        // Generation one is used only to reuse the foundational prefix bound
        // validator. No generation is retained as an authorization fact.
        let leading_components = selector.finish().components().to_vec();
        Ok(Self(OperationKind::QueryProjection {
            version,
            identity,
            leading_components,
            tenant_scope: OperationTenantScope::global_only(),
            requested_rows,
        }))
    }

    /// Constructs an exact projection-status request.
    #[must_use]
    pub const fn get_projection_status(
        lineage: ContractLineage,
        version: ContractVersion,
        projection_id: ProjectionId,
    ) -> Self {
        Self(OperationKind::GetProjectionStatus {
            lineage,
            version,
            projection_id,
        })
    }

    /// Constructs a single-commit request.
    #[must_use]
    pub const fn get_commit(sequence: CommitSequence) -> Self {
        Self(OperationKind::GetCommit { sequence })
    }

    /// Constructs a bounded commit scan.
    #[must_use]
    pub const fn scan_commits(requested_rows: NonZeroU16) -> Self {
        Self(OperationKind::ScanCommits { requested_rows })
    }

    /// Constructs a commit-subscription establishment request.
    #[must_use]
    pub const fn subscribe_to_commits() -> Self {
        Self(OperationKind::SubscribeToCommits)
    }

    /// Constructs an exact provenance trace request.
    #[must_use]
    pub const fn trace_provenance(selector: ProvenanceSelector) -> Self {
        Self(OperationKind::TraceProvenance { selector })
    }

    /// Constructs a server-health request.
    #[must_use]
    pub const fn get_health() -> Self {
        Self(OperationKind::GetHealth)
    }

    /// Constructs a server-statistics request.
    #[must_use]
    pub const fn get_statistics() -> Self {
        Self(OperationKind::GetStatistics)
    }

    /// Constructs a capability-create authorization request with complete target facts.
    #[must_use]
    pub const fn create_capability(target: CapabilityCreateTargetFacts) -> Self {
        Self(OperationKind::CreateCapability { target })
    }

    /// Constructs a capability-revoke request with complete target facts and reason.
    #[must_use]
    pub const fn revoke_capability(
        target: CapabilityRevokeTargetFacts,
        reason: RevocationReasonCodeV1,
    ) -> Self {
        Self(OperationKind::RevokeCapability { target, reason })
    }

    /// Constructs a capability-revoke request whose exact target is absent.
    #[must_use]
    pub const fn revoke_absent_capability(
        target: AbsentCapabilityRevokeTargetFacts,
        reason: RevocationReasonCodeV1,
    ) -> Self {
        Self(OperationKind::RevokeAbsentCapability { target, reason })
    }

    /// Constructs a bounded outbox-status request.
    #[must_use]
    pub const fn list_pending_outbox_deliveries(requested_rows: NonZeroU16) -> Self {
        Self(OperationKind::ListPendingOutboxDeliveries { requested_rows })
    }

    /// Constructs one command-tool discovery snapshot check.
    #[must_use]
    pub const fn discover_command_tools() -> Self {
        Self(OperationKind::DiscoverCommandTools)
    }

    /// Constructs one resource-discovery snapshot check.
    #[must_use]
    pub const fn discover_resources() -> Self {
        Self(OperationKind::DiscoverResources)
    }

    /// Constructs one symbolic contract-description request.
    #[must_use]
    pub const fn describe_contract() -> Self {
        Self(OperationKind::DescribeContract)
    }

    /// Constructs one symbolic query-check request.
    #[must_use]
    pub const fn check_query() -> Self {
        Self(OperationKind::CheckQuery)
    }

    /// Constructs one symbolic query-explanation request.
    #[must_use]
    pub const fn explain_query() -> Self {
        Self(OperationKind::ExplainQuery)
    }

    /// Constructs one symbolic query-execution lifecycle request.
    ///
    /// The shared service additionally authorizes every compiler-derived
    /// entity, field, index, partition, and row requirement before execution
    /// and again before release.
    #[must_use]
    pub const fn execute_query() -> Self {
        Self(OperationKind::ExecuteQuery)
    }

    /// Returns the exact closed service operation.
    #[must_use]
    pub const fn operation(&self) -> ServiceOperationV1 {
        match &self.0 {
            OperationKind::ValidateContract => ServiceOperationV1::ValidateContract,
            OperationKind::ExplainCommand { .. } => ServiceOperationV1::ExplainCommand,
            OperationKind::DeployContract { .. } => ServiceOperationV1::DeployContract,
            OperationKind::GetActiveContract => ServiceOperationV1::GetActiveContract,
            OperationKind::GetContractVersion { .. } => ServiceOperationV1::GetContractVersion,
            OperationKind::ExecuteCommand { .. } => ServiceOperationV1::ExecuteCommand,
            OperationKind::ResolveCommandOutcomePreLookup { .. }
            | OperationKind::ResolveCommandOutcome { .. } => {
                ServiceOperationV1::ResolveCommandOutcome
            }
            OperationKind::GetEntity { .. } => ServiceOperationV1::GetEntity,
            OperationKind::ScanIndex { .. } => ServiceOperationV1::ScanIndex,
            OperationKind::QueryProjection { .. } => ServiceOperationV1::QueryProjection,
            OperationKind::GetProjectionStatus { .. } => ServiceOperationV1::GetProjectionStatus,
            OperationKind::GetCommit { .. } => ServiceOperationV1::GetCommit,
            OperationKind::ScanCommits { .. } => ServiceOperationV1::ScanCommits,
            OperationKind::SubscribeToCommits => ServiceOperationV1::SubscribeToCommits,
            OperationKind::TraceProvenance { .. } => ServiceOperationV1::TraceProvenance,
            OperationKind::GetHealth => ServiceOperationV1::GetHealth,
            OperationKind::GetStatistics => ServiceOperationV1::GetStatistics,
            OperationKind::CreateCapability { .. } => ServiceOperationV1::CreateCapability,
            OperationKind::RevokeCapability { .. }
            | OperationKind::RevokeAbsentCapability { .. } => ServiceOperationV1::RevokeCapability,
            OperationKind::ListPendingOutboxDeliveries { .. } => {
                ServiceOperationV1::ListPendingOutboxDeliveries
            }
            OperationKind::DiscoverCommandTools => ServiceOperationV1::DiscoverCommandTools,
            OperationKind::DiscoverResources => ServiceOperationV1::DiscoverResources,
            OperationKind::DescribeContract => ServiceOperationV1::DescribeContract,
            OperationKind::CheckQuery => ServiceOperationV1::CheckQuery,
            OperationKind::ExplainQuery => ServiceOperationV1::ExplainQuery,
            OperationKind::ExecuteQuery => ServiceOperationV1::ExecuteQuery,
        }
    }

    pub(crate) fn into_catalog_deployment_parts(
        self,
    ) -> Option<(
        ContractLineage,
        ContractVersion,
        ContractBundleHash,
        Option<ContractVersion>,
    )> {
        match self.0 {
            OperationKind::DeployContract {
                lineage,
                version,
                bundle_hash,
                expected_active_version,
            } => Some((lineage, version, bundle_hash, expected_active_version)),
            _ => None,
        }
    }

    pub(crate) fn permission_requirement(&self) -> Option<PermissionRequirement> {
        use CapabilityPermissionKindV1 as Kind;
        let requirement = match &self.0 {
            OperationKind::ValidateContract => PermissionRequirement::Kind(Kind::ValidateContract),
            OperationKind::ExplainCommand {
                lineage,
                command_id,
                ..
            } => PermissionRequirement::Exact(CapabilityPermissionV1::ExplainCommand(
                lineage.clone(),
                *command_id,
            )),
            OperationKind::DeployContract { .. } => {
                PermissionRequirement::Kind(Kind::DeployContract)
            }
            OperationKind::GetActiveContract
            | OperationKind::GetContractVersion { .. }
            | OperationKind::DescribeContract
            | OperationKind::CheckQuery
            | OperationKind::ExplainQuery
            | OperationKind::ExecuteQuery => PermissionRequirement::Kind(Kind::ReadContract),
            OperationKind::ExecuteCommand {
                lineage,
                command_id,
                ..
            }
            | OperationKind::ResolveCommandOutcomePreLookup {
                lineage,
                command_id,
                ..
            }
            | OperationKind::ResolveCommandOutcome {
                lineage,
                command_id,
                ..
            } => PermissionRequirement::Exact(CapabilityPermissionV1::InvokeCommand(
                lineage.clone(),
                *command_id,
            )),
            OperationKind::GetEntity {
                lineage,
                entity_type_id,
                ..
            } => PermissionRequirement::Exact(CapabilityPermissionV1::ReadEntity(
                lineage.clone(),
                *entity_type_id,
            )),
            OperationKind::ScanIndex {
                lineage, index_id, ..
            } => PermissionRequirement::Exact(CapabilityPermissionV1::ScanIndex(
                lineage.clone(),
                *index_id,
            )),
            OperationKind::QueryProjection { identity, .. } => {
                PermissionRequirement::Exact(CapabilityPermissionV1::QueryProjection(
                    identity.contract_lineage().clone(),
                    identity.projection_id(),
                ))
            }
            OperationKind::GetProjectionStatus {
                lineage,
                projection_id,
                ..
            } => PermissionRequirement::Exact(CapabilityPermissionV1::ReadProjectionStatus(
                lineage.clone(),
                *projection_id,
            )),
            OperationKind::GetCommit { .. } => PermissionRequirement::Kind(Kind::ReadCommit),
            OperationKind::ScanCommits { .. } => PermissionRequirement::Kind(Kind::ScanCommits),
            OperationKind::SubscribeToCommits => {
                PermissionRequirement::Kind(Kind::SubscribeCommits)
            }
            OperationKind::TraceProvenance { .. } => {
                PermissionRequirement::Kind(Kind::ReadProvenance)
            }
            OperationKind::GetHealth => PermissionRequirement::Kind(Kind::ReadHealth),
            OperationKind::GetStatistics => PermissionRequirement::Kind(Kind::ReadStatistics),
            OperationKind::CreateCapability { .. } => {
                PermissionRequirement::Either(Kind::CreateCapability, Kind::AdministerCapabilities)
            }
            OperationKind::RevokeCapability { .. } => {
                PermissionRequirement::Either(Kind::RevokeCapability, Kind::AdministerCapabilities)
            }
            OperationKind::RevokeAbsentCapability { .. } => {
                PermissionRequirement::Kind(Kind::AdministerCapabilities)
            }
            OperationKind::ListPendingOutboxDeliveries { .. } => {
                PermissionRequirement::Kind(Kind::InspectOutbox)
            }
            OperationKind::DiscoverCommandTools | OperationKind::DiscoverResources => return None,
        };
        Some(requirement)
    }

    pub(crate) const fn tenant_requirement(&self) -> Option<&OperationTenantScope> {
        match &self.0 {
            OperationKind::ExecuteCommand { scope, .. }
            | OperationKind::ResolveCommandOutcome { scope, .. }
            | OperationKind::GetEntity { scope, .. } => Some(&scope.tenant_scope),
            OperationKind::ResolveCommandOutcomePreLookup { tenant_scope, .. }
            | OperationKind::ScanIndex { tenant_scope, .. }
            | OperationKind::QueryProjection { tenant_scope, .. } => Some(tenant_scope),
            OperationKind::GetCommit { .. }
            | OperationKind::ScanCommits { .. }
            | OperationKind::SubscribeToCommits
            | OperationKind::TraceProvenance { .. }
            | OperationKind::GetStatistics
            | OperationKind::ListPendingOutboxDeliveries { .. } => {
                Some(&GLOBAL_ONLY_OPERATION_SCOPE)
            }
            OperationKind::ExecuteQuery => Some(&GLOBAL_ONLY_OPERATION_SCOPE),
            _ => None,
        }
    }

    pub(crate) const fn outcome_owner_requirement(&self) -> Option<OutcomeOwnerRequirement<'_>> {
        match &self.0 {
            OperationKind::ResolveCommandOutcome {
                owner_principal_id,
                owner_tenant_scope,
                ..
            } => Some(OutcomeOwnerRequirement {
                principal_id: owner_principal_id,
                tenant_scope: owner_tenant_scope,
            }),
            _ => None,
        }
    }

    pub(crate) const fn partition_requirement(&self) -> PartitionRequirement<'_> {
        match &self.0 {
            OperationKind::ExecuteCommand { scope, .. }
            | OperationKind::ResolveCommandOutcome { scope, .. }
            | OperationKind::GetEntity { scope, .. } => {
                PartitionRequirement::Exact(&scope.partition)
            }
            OperationKind::ScanIndex { .. } | OperationKind::QueryProjection { .. } => {
                if matches!(&self.0, OperationKind::QueryProjection { .. }) {
                    PartitionRequirement::AllOnly
                } else {
                    PartitionRequirement::Filter
                }
            }
            OperationKind::GetCommit { .. }
            | OperationKind::ScanCommits { .. }
            | OperationKind::SubscribeToCommits
            | OperationKind::TraceProvenance { .. }
            | OperationKind::ListPendingOutboxDeliveries { .. } => PartitionRequirement::AllOnly,
            _ => PartitionRequirement::None,
        }
    }

    pub(crate) fn field_requirement(&self) -> Option<FieldRequirement<'_>> {
        match &self.0 {
            OperationKind::GetEntity { fields, .. } | OperationKind::ScanIndex { fields, .. } => {
                Some(FieldRequirement {
                    lineage: &fields.lineage,
                    entity_type_id: fields.entity_type_id,
                    non_key_fields: &fields.non_key_fields,
                })
            }
            _ => None,
        }
    }

    pub(crate) const fn requested_rows(&self) -> Option<NonZeroU16> {
        match &self.0 {
            OperationKind::ScanIndex { requested_rows, .. }
            | OperationKind::QueryProjection { requested_rows, .. }
            | OperationKind::ScanCommits { requested_rows }
            | OperationKind::ListPendingOutboxDeliveries { requested_rows } => {
                Some(*requested_rows)
            }
            _ => None,
        }
    }

    pub(crate) const fn audit_obligation(&self) -> Option<AuditClass> {
        match &self.0 {
            OperationKind::ExecuteCommand {
                class: CommandExecutionClass::Mutation,
                ..
            } => Some(AuditClass::CommandMutation),
            OperationKind::DeployContract { .. }
            | OperationKind::CreateCapability { .. }
            | OperationKind::RevokeCapability { .. }
            | OperationKind::RevokeAbsentCapability { .. } => {
                Some(AuditClass::ControlPlaneMutation)
            }
            OperationKind::GetCommit { .. }
            | OperationKind::ScanCommits { .. }
            | OperationKind::SubscribeToCommits
            | OperationKind::TraceProvenance { .. }
            | OperationKind::GetStatistics
            | OperationKind::ListPendingOutboxDeliveries { .. } => {
                Some(AuditClass::AdministrativeRead)
            }
            _ => None,
        }
    }

    pub(crate) const fn output_classification(&self) -> OutputClassification {
        match &self.0 {
            OperationKind::ExecuteCommand { .. }
            | OperationKind::ResolveCommandOutcomePreLookup { .. }
            | OperationKind::ResolveCommandOutcome { .. }
            | OperationKind::GetEntity { .. }
            | OperationKind::ScanIndex { .. }
            | OperationKind::QueryProjection { .. }
            | OperationKind::ExecuteQuery => OutputClassification::PolicyFilteredApplicationData,
            OperationKind::DeployContract { .. }
            | OperationKind::GetCommit { .. }
            | OperationKind::ScanCommits { .. }
            | OperationKind::SubscribeToCommits
            | OperationKind::TraceProvenance { .. }
            | OperationKind::GetStatistics
            | OperationKind::CreateCapability { .. }
            | OperationKind::RevokeCapability { .. }
            | OperationKind::RevokeAbsentCapability { .. }
            | OperationKind::ListPendingOutboxDeliveries { .. } => {
                OutputClassification::AdministrativeRedactedData
            }
            _ => OutputClassification::PublicMetadata,
        }
    }

    pub(crate) fn into_capability_mutation(self) -> Option<CapabilityMutationRequest> {
        match self.0 {
            OperationKind::CreateCapability { target } => {
                Some(CapabilityMutationRequest::Create(target))
            }
            OperationKind::RevokeCapability { target, reason } => {
                Some(CapabilityMutationRequest::Revoke { target, reason })
            }
            OperationKind::RevokeAbsentCapability { target, reason } => {
                Some(CapabilityMutationRequest::RevokeAbsent { target, reason })
            }
            _ => None,
        }
    }

    pub(crate) fn into_command_execution(self) -> Option<CommandOperationBinding> {
        match self.0 {
            OperationKind::ExecuteCommand {
                lineage,
                version,
                command_id,
                class,
                scope,
            } => Some(CommandOperationBinding {
                lineage,
                version,
                command_id,
                class,
                partition: scope.partition,
            }),
            _ => None,
        }
    }
}

impl fmt::Debug for OperationRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "OperationRequest::{:?}([REDACTED])",
            self.operation()
        )
    }
}

pub(crate) enum PermissionRequirement {
    Kind(CapabilityPermissionKindV1),
    Exact(CapabilityPermissionV1),
    Either(CapabilityPermissionKindV1, CapabilityPermissionKindV1),
}

pub(crate) enum PartitionRequirement<'a> {
    None,
    Exact(&'a ScopedPartitionV1),
    Filter,
    AllOnly,
}

pub(crate) struct FieldRequirement<'a> {
    pub(crate) lineage: &'a ContractLineage,
    pub(crate) entity_type_id: EntityTypeId,
    pub(crate) non_key_fields: &'a [FieldId],
}

pub(crate) struct OutcomeOwnerRequirement<'a> {
    pub(crate) principal_id: &'a ActorId,
    pub(crate) tenant_scope: &'a TenantScope,
}

pub(crate) struct CommandOperationBinding {
    pub(crate) lineage: ContractLineage,
    pub(crate) version: ContractVersion,
    pub(crate) command_id: CommandId,
    pub(crate) class: CommandExecutionClass,
    pub(crate) partition: ScopedPartitionV1,
}

pub(crate) fn command_tool_permission(candidate: &CommandToolCandidate) -> PermissionRequirement {
    PermissionRequirement::Exact(CapabilityPermissionV1::InvokeCommand(
        candidate.lineage.clone(),
        candidate.command_id,
    ))
}

pub(crate) fn resource_permission(candidate: &DiscoveryResource) -> PermissionRequirement {
    use CapabilityPermissionKindV1 as Kind;
    match candidate {
        DiscoveryResource::ActiveContract | DiscoveryResource::ContractVersion { .. } => {
            PermissionRequirement::Kind(Kind::ReadContract)
        }
        DiscoveryResource::CommandPlan {
            lineage,
            command_id,
        }
        | DiscoveryResource::CommandDocumentation {
            lineage,
            command_id,
        } => PermissionRequirement::Exact(CapabilityPermissionV1::ExplainCommand(
            lineage.clone(),
            *command_id,
        )),
        DiscoveryResource::CommandOutcome {
            lineage,
            command_id,
        } => PermissionRequirement::Exact(CapabilityPermissionV1::InvokeCommand(
            lineage.clone(),
            *command_id,
        )),
        DiscoveryResource::EntitySchema(candidate) => {
            PermissionRequirement::Exact(CapabilityPermissionV1::ReadEntity(
                candidate.lineage().clone(),
                candidate.entity_type_id(),
            ))
        }
        DiscoveryResource::ProjectionStatus {
            lineage,
            projection_id,
        } => PermissionRequirement::Exact(CapabilityPermissionV1::ReadProjectionStatus(
            lineage.clone(),
            *projection_id,
        )),
        DiscoveryResource::Commit => PermissionRequirement::Kind(Kind::ReadCommit),
        DiscoveryResource::Provenance => PermissionRequirement::Kind(Kind::ReadProvenance),
        DiscoveryResource::Health => PermissionRequirement::Kind(Kind::ReadHealth),
    }
}

pub(crate) const fn fixed_tool_permission_kind(
    candidate: FixedToolCandidate,
) -> CapabilityPermissionKindV1 {
    use CapabilityPermissionKindV1 as Kind;
    match candidate {
        FixedToolCandidate::ValidateContract => Kind::ValidateContract,
        FixedToolCandidate::GetActiveContract => Kind::ReadContract,
        FixedToolCandidate::ExplainCommand => Kind::ExplainCommand,
        FixedToolCandidate::DeployContract => Kind::DeployContract,
        FixedToolCandidate::ResolveCommandOutcome => Kind::InvokeCommand,
        FixedToolCandidate::GetEntity => Kind::ReadEntity,
        FixedToolCandidate::ScanIndex => Kind::ScanIndex,
        FixedToolCandidate::GetCommit => Kind::ReadCommit,
        FixedToolCandidate::ScanCommits => Kind::ScanCommits,
        FixedToolCandidate::TraceProvenance => Kind::ReadProvenance,
        FixedToolCandidate::QueryProjection => Kind::QueryProjection,
        FixedToolCandidate::GetProjectionStatus => Kind::ReadProjectionStatus,
        FixedToolCandidate::ListPendingOutboxDeliveries => Kind::InspectOutbox,
        FixedToolCandidate::GetHealth => Kind::ReadHealth,
        FixedToolCandidate::DescribeContract
        | FixedToolCandidate::CheckQuery
        | FixedToolCandidate::ExplainQuery
        | FixedToolCandidate::ExecuteQuery => Kind::ReadContract,
    }
}

pub(crate) fn resource_field_requirement(
    candidate: &DiscoveryResource,
) -> Option<FieldRequirement<'_>> {
    match candidate {
        DiscoveryResource::EntitySchema(candidate) => Some(FieldRequirement {
            lineage: candidate.lineage(),
            entity_type_id: candidate.entity_type_id(),
            non_key_fields: candidate.non_key_fields(),
        }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::num::NonZeroU32;

    use riffdb_types::{
        ActorKind, AggregateTypeId, Audience, CanonicalValue, CapabilityGrantV1, CapabilityId,
        CapabilityPermissionsV1, DatabaseId, Environment, MAX_KEY_BYTES, PartitionKeyBuilder,
        PartitionScopeV1, ProjectionPlanHash, RequestId, Timestamp,
    };

    use super::*;

    fn lineage() -> ContractLineage {
        ContractLineage::new("example.contract").expect("valid lineage")
    }

    fn version() -> ContractVersion {
        ContractVersion::new(1).expect("nonzero version")
    }

    fn command() -> CommandId {
        CommandId::first()
    }

    fn partition() -> PartitionKey {
        let mut builder = PartitionKeyBuilder::new(AggregateTypeId::first());
        builder.push_u64(7).expect("bounded component");
        builder.finish().expect("valid partition")
    }

    fn capability_id() -> CapabilityId {
        CapabilityId::from_unix_milliseconds_and_random(1, [2; 10]).expect("valid UUIDv7")
    }

    fn create_target() -> CapabilityCreateTargetFacts {
        let request_id =
            RequestId::from_unix_milliseconds_and_random(1, [3; 10]).expect("valid UUIDv7");
        let database_id =
            DatabaseId::from_unix_milliseconds_and_random(1, [4; 10]).expect("valid UUIDv7");
        let grant = CapabilityGrantV1::new(
            TenantScope::Global,
            PartitionScopeV1::All,
            CapabilityPermissionsV1::new(Vec::new()).expect("empty permission set"),
            Vec::new(),
            NonZeroU16::new(1).expect("nonzero rows"),
            Vec::new(),
        )
        .expect("valid grant");
        let requested_record = crate::NormalizedCapabilityCreateRecord::new(
            database_id,
            Environment::new("dev").expect("valid environment"),
            actor_id(),
            ActorKind::Service,
            NonZeroU32::new(60).expect("nonzero duration"),
            vec![Audience::new("grpc").expect("valid audience")],
            grant,
        )
        .expect("valid requested record");
        CapabilityCreateTargetFacts::new(request_id, capability_id(), requested_record)
    }

    fn revoke_target() -> CapabilityRevokeTargetFacts {
        let grant = CapabilityGrantV1::new(
            TenantScope::Global,
            PartitionScopeV1::All,
            CapabilityPermissionsV1::new(Vec::new()).expect("empty permission set"),
            Vec::new(),
            NonZeroU16::new(1).expect("nonzero rows"),
            Vec::new(),
        )
        .expect("valid grant");
        CapabilityRevokeTargetFacts::new(
            RequestId::from_unix_milliseconds_and_random(1, [5; 10]).expect("valid UUIDv7"),
            capability_id(),
            std::num::NonZeroU64::MIN,
            crate::CapabilityActivity::Active,
            DatabaseId::from_unix_milliseconds_and_random(1, [4; 10]).expect("valid UUIDv7"),
            Environment::new("dev").expect("valid environment"),
            actor_id(),
            ActorKind::Service,
            vec![Audience::new("grpc").expect("valid audience")],
            Timestamp::new(1, 0).expect("valid timestamp"),
            Timestamp::new(2, 0).expect("valid timestamp"),
            grant,
        )
        .expect("valid revoke target")
    }

    fn absent_revoke_target() -> AbsentCapabilityRevokeTargetFacts {
        AbsentCapabilityRevokeTargetFacts::new(
            RequestId::from_unix_milliseconds_and_random(1, [6; 10]).expect("valid UUIDv7"),
            capability_id(),
            DatabaseId::from_unix_milliseconds_and_random(1, [4; 10]).expect("valid UUIDv7"),
            Environment::new("dev").expect("valid environment"),
        )
    }

    fn actor_id() -> ActorId {
        ActorId::new("principal-1").expect("valid actor")
    }

    fn bundle_hash(byte: u8) -> ContractBundleHash {
        ContractBundleHash::from_bytes([byte; 32])
    }

    fn projection_identity() -> ProjectionIdentity {
        ProjectionIdentity::new(
            lineage(),
            ProjectionId::first(),
            ProjectionPlanHash::from_bytes([9; 32]),
        )
    }

    fn requests() -> Vec<OperationRequest> {
        let lineage = lineage();
        vec![
            OperationRequest::validate_contract(),
            OperationRequest::explain_command(lineage.clone(), version(), command()),
            OperationRequest::deploy_contract(lineage.clone(), version(), bundle_hash(1), None),
            OperationRequest::get_active_contract(),
            OperationRequest::get_contract_version(lineage.clone(), version()),
            OperationRequest::execute_command(
                lineage.clone(),
                version(),
                command(),
                CommandExecutionClass::Mutation,
                partition(),
            ),
            OperationRequest::resolve_command_outcome(
                lineage.clone(),
                version(),
                command(),
                actor_id(),
                TenantScope::Global,
                partition(),
            ),
            OperationRequest::get_entity(
                lineage.clone(),
                version(),
                EntityTypeId::first(),
                OperationTenantScope::global_only(),
                partition(),
                vec![FieldId::first()],
            )
            .expect("valid fields"),
            OperationRequest::scan_index(
                lineage.clone(),
                version(),
                IndexId::first(),
                EntityTypeId::first(),
                OperationTenantScope::global_only(),
                vec![FieldId::first()],
                NonZeroU16::new(10).expect("nonzero"),
            )
            .expect("valid fields"),
            OperationRequest::query_projection(
                version(),
                projection_identity(),
                Vec::new(),
                NonZeroU16::new(10).expect("nonzero"),
            )
            .expect("valid projection selector"),
            OperationRequest::get_projection_status(
                lineage.clone(),
                version(),
                ProjectionId::first(),
            ),
            OperationRequest::get_commit(CommitSequence::first()),
            OperationRequest::scan_commits(NonZeroU16::new(10).expect("nonzero")),
            OperationRequest::subscribe_to_commits(),
            OperationRequest::trace_provenance(ProvenanceSelector::Commit(CommitSequence::first())),
            OperationRequest::get_health(),
            OperationRequest::get_statistics(),
            OperationRequest::create_capability(create_target()),
            OperationRequest::revoke_capability(revoke_target(), RevocationReasonCodeV1::Requested),
            OperationRequest::list_pending_outbox_deliveries(NonZeroU16::new(10).expect("nonzero")),
            OperationRequest::discover_command_tools(),
            OperationRequest::discover_resources(),
        ]
    }

    #[test]
    fn request_inventory_is_exactly_the_shared_22_operations() {
        let requests = requests();
        assert_eq!(requests.len(), 22);
        assert_eq!(
            requests
                .iter()
                .map(OperationRequest::operation)
                .collect::<Vec<_>>(),
            ServiceOperationV1::ALL
        );
    }

    #[test]
    fn permission_mapping_reaches_every_closed_permission_kind() {
        let mut kinds = BTreeSet::new();
        for request in requests() {
            let Some(requirement) = request.permission_requirement() else {
                continue;
            };
            match requirement {
                PermissionRequirement::Kind(kind) => {
                    kinds.insert(kind);
                }
                PermissionRequirement::Exact(permission) => {
                    kinds.insert(permission.kind());
                }
                PermissionRequirement::Either(left, right) => {
                    kinds.insert(left);
                    kinds.insert(right);
                }
            }
        }
        assert_eq!(kinds.len(), 19);
        assert_eq!(
            kinds
                .into_iter()
                .map(CapabilityPermissionKindV1::tag)
                .collect::<Vec<_>>(),
            (1..=19).collect::<Vec<_>>()
        );
    }

    #[test]
    fn absent_revoke_request_maps_only_to_capability_administration() {
        let request = OperationRequest::revoke_absent_capability(
            absent_revoke_target(),
            RevocationReasonCodeV1::Requested,
        );
        assert_eq!(request.operation(), ServiceOperationV1::RevokeCapability);
        assert!(matches!(
            request.permission_requirement(),
            Some(PermissionRequirement::Kind(
                CapabilityPermissionKindV1::AdministerCapabilities
            ))
        ));
        assert_eq!(
            request.audit_obligation(),
            Some(AuditClass::ControlPlaneMutation)
        );
        assert_eq!(
            request.output_classification(),
            OutputClassification::AdministrativeRedactedData
        );
        assert!(format!("{request:?}").contains("[REDACTED]"));
    }

    #[test]
    fn discovery_is_candidate_specific_instead_of_blanket_authority() {
        let command_discovery = OperationRequest::discover_command_tools();
        assert!(command_discovery.permission_requirement().is_none());
        assert!(matches!(
            command_tool_permission(&CommandToolCandidate::new(lineage(), command())),
            PermissionRequirement::Exact(CapabilityPermissionV1::InvokeCommand(..))
        ));

        let health = OperationRequest::discover_resources();
        assert!(health.permission_requirement().is_none());
        assert!(matches!(
            resource_permission(&DiscoveryResource::Health),
            PermissionRequirement::Kind(CapabilityPermissionKindV1::ReadHealth)
        ));
    }

    #[test]
    fn field_requests_are_canonical_and_reject_duplicates() {
        let result = OperationRequest::get_entity(
            lineage(),
            version(),
            EntityTypeId::first(),
            OperationTenantScope::global_only(),
            partition(),
            vec![FieldId::first(), FieldId::first()],
        );
        assert_eq!(result, Err(OperationRequestError::DuplicateField));
    }

    #[test]
    fn grammar_v1_tenant_scope_is_an_explicit_global_proof() {
        let scope = OperationTenantScope::grammar_v1_global();

        assert_eq!(scope.tenant_scope(), &TenantScope::Global);
        assert_eq!(scope, OperationTenantScope::global_only());
        assert_eq!(format!("{scope:?}"), "OperationTenantScope::GlobalOnly");
    }

    #[test]
    fn outcome_pre_lookup_authorizes_no_fabricated_stored_facts() {
        let request = OperationRequest::resolve_command_outcome_pre_lookup(lineage(), command());

        assert_eq!(
            request.operation(),
            ServiceOperationV1::ResolveCommandOutcome
        );
        assert!(matches!(
            request.permission_requirement(),
            Some(PermissionRequirement::Exact(
                CapabilityPermissionV1::InvokeCommand(required_lineage, required_command)
            )) if required_lineage == lineage() && required_command == command()
        ));
        assert_eq!(
            request
                .tenant_requirement()
                .expect("grammar-v1 scope")
                .tenant_scope(),
            &TenantScope::Global
        );
        assert!(request.outcome_owner_requirement().is_none());
        assert!(matches!(
            request.partition_requirement(),
            PartitionRequirement::None
        ));
        assert_eq!(request.audit_obligation(), None);
        assert_eq!(
            request.output_classification(),
            OutputClassification::PolicyFilteredApplicationData
        );
    }

    #[test]
    fn request_debug_redacts_typed_targets() {
        let request = OperationRequest::discover_command_tools();
        let debug = format!("{request:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("example.contract"));
    }

    #[test]
    fn projection_request_binds_a_bounded_canonical_selector() {
        let oversized =
            CanonicalValue::string("x".repeat(MAX_KEY_BYTES)).expect("bounded canonical string");
        let result = OperationRequest::query_projection(
            version(),
            projection_identity(),
            vec![oversized],
            NonZeroU16::new(10).expect("nonzero rows"),
        );
        assert_eq!(
            result,
            Err(OperationRequestError::InvalidProjectionSelector)
        );

        let value = CanonicalValue::Bool(true);
        let request = OperationRequest::query_projection(
            version(),
            projection_identity(),
            vec![value.clone()],
            NonZeroU16::new(10).expect("nonzero rows"),
        )
        .expect("bounded selector");
        let OperationKind::QueryProjection {
            leading_components, ..
        } = request.0
        else {
            panic!("query constructor must retain query facts");
        };
        assert_eq!(leading_components, vec![value]);
    }

    #[test]
    fn outcome_resolution_proof_binds_the_stored_contract_version() {
        let first = OperationRequest::resolve_command_outcome(
            lineage(),
            ContractVersion::new(1).expect("nonzero version"),
            command(),
            actor_id(),
            TenantScope::Global,
            partition(),
        );
        let second = OperationRequest::resolve_command_outcome(
            lineage(),
            ContractVersion::new(2).expect("nonzero version"),
            command(),
            actor_id(),
            TenantScope::Global,
            partition(),
        );
        assert_ne!(first, second);
    }

    #[test]
    fn deployment_proof_binds_bundle_and_expected_active_version() {
        let first = OperationRequest::deploy_contract(lineage(), version(), bundle_hash(1), None);
        let different_bundle =
            OperationRequest::deploy_contract(lineage(), version(), bundle_hash(2), None);
        let different_expectation = OperationRequest::deploy_contract(
            lineage(),
            version(),
            bundle_hash(1),
            Some(version()),
        );
        assert_ne!(first, different_bundle);
        assert_ne!(first, different_expectation);
    }
}
