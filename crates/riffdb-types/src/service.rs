//! Closed service-audit vocabulary and canonical target collection.

use std::error::Error;
use std::fmt;

use crate::limits::MAX_SERVICE_AUDIT_TARGETS;
use crate::{
    AdministrationSequence, CapabilityId, CommandId, CommitSequence, ContractLineage,
    ContractVersion, EntityTypeId, EventConsumerIdentityHash, IndexId, ProjectionId, ProvenanceId,
    ReactiveModuleHash, ReactiveOperationName, ReplicationFollowerAuditTargetV1,
};

/// The closed v1 application-service operation registry.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ServiceOperationV1 {
    /// Validate contract source without deploying it.
    ValidateContract,
    /// Explain one compiled command.
    ExplainCommand,
    /// Deploy one contract version.
    DeployContract,
    /// Read the active contract metadata.
    GetActiveContract,
    /// Read one contract version.
    GetContractVersion,
    /// Execute one classified command.
    ExecuteCommand,
    /// Resolve a previously submitted command outcome.
    ResolveCommandOutcome,
    /// Read one entity.
    GetEntity,
    /// Scan one index.
    ScanIndex,
    /// Query one derived projection.
    QueryProjection,
    /// Read one projection's status.
    GetProjectionStatus,
    /// Read one application commit.
    GetCommit,
    /// Scan application commits.
    ScanCommits,
    /// Subscribe to application commits.
    SubscribeToCommits,
    /// Trace one provenance selector.
    TraceProvenance,
    /// Read server health.
    GetHealth,
    /// Read server statistics.
    GetStatistics,
    /// Create or bootstrap a capability.
    CreateCapability,
    /// Revoke a capability.
    RevokeCapability,
    /// List pending outbox deliveries.
    ListPendingOutboxDeliveries,
    /// Discover policy-visible command tools.
    DiscoverCommandTools,
    /// Discover policy-visible resources.
    DiscoverResources,
    /// Describe one exact symbolic contract catalog.
    DescribeContract,
    /// Parse, resolve, type-check, and plan one symbolic query.
    CheckQuery,
    /// Return one bounded symbolic query plan explanation.
    ExplainQuery,
    /// Execute one closed symbolic query.
    ExecuteQuery,
    /// Compile and atomically activate one immutable query module.
    DeployQueryModule,
    /// Apply one accepted offline contract migration.
    ApplyContractMigration,
    /// Describe one active symbolic domain event.
    DescribeEvent,
    /// Replay one bounded upper-fenced symbolic event page.
    ReplayEvents,
    /// Wait for and return one bounded symbolic event page.
    TailEvents,
    /// Execute one projected columnar query under a freshness policy.
    ExecuteProjectedQuery,
    /// Publish one exact immutable reactive module.
    DeployReactiveModule,
    /// Pull or stream leased events from one exact symbolic stream.
    ConsumeEventStream,
    /// Acknowledge one exact leased event attempt.
    AcknowledgeEventStream,
    /// Negatively acknowledge one exact leased event attempt.
    NegativeAcknowledgeEventStream,
    /// Administratively seek one exact durable event consumer.
    SeekEventStreamConsumer,
    /// Administratively retire one exact durable event consumer.
    RetireEventStreamConsumer,
    /// Read one exact durable event consumer status.
    GetEventStreamConsumerStatus,
    /// Establish and advance one exact live named-query watch.
    WatchNamedQuery,
    /// Pull one bounded contextual work batch.
    ConsumeContextualSubscription,
    /// Acknowledge one exact contextual work lease.
    AcknowledgeContextualSubscription,
    /// Negatively acknowledge one exact contextual work lease.
    NegativeAcknowledgeContextualSubscription,
    /// Read one exact contextual consumer status.
    GetContextualSubscriptionStatus,
    /// Execute one causally fenced contextual reaction.
    ExecuteContextualReaction,
    /// Read one opaque reactive wakeup generation.
    GetReactiveWakeup,
    /// Start or resume one exact application installation campaign.
    StartApplicationInstallation,
    /// Observe one exact application installation campaign.
    GetApplicationInstallation,
    /// Start or exactly replay one snapshot-bound symbolic application export.
    StartApplicationExport,
    /// Release one bounded symbolic application-export page.
    GetApplicationExportPage,
    /// Observe one protected application-export checkpoint or receipt.
    GetApplicationExport,
    /// Close one nonterminal application export with an incomplete receipt.
    CancelApplicationExport,
    /// Start or exactly resume one not-ready application reimport campaign.
    StartApplicationReimport,
    /// Apply one exact bounded export page through compiler-owned commands.
    ApplyApplicationReimportPage,
    /// Observe one protected application-reimport checkpoint or receipt.
    GetApplicationReimport,
    /// Close one nonterminal application reimport without publishing readiness.
    CancelApplicationReimport,
    /// Inspect one exact compiler-declared production vector field.
    InspectVectorState,
    /// Register one lineage-scoped follower retention policy.
    RegisterFollower,
    /// Retire one exact follower registration generation.
    RetireFollower,
    /// Durably fence one primary for an exact registered follower generation.
    FenceReplicationPrimary,
    /// Establish an audited administrative changelog replication stream.
    StreamChangelog,
    /// Promote one drained follower under authenticated source fencing.
    PromoteFollower,
}

impl ServiceOperationV1 {
    /// Every accepted v1 service operation, in tag order.
    pub const ALL: [Self; 62] = [
        Self::ValidateContract,
        Self::ExplainCommand,
        Self::DeployContract,
        Self::GetActiveContract,
        Self::GetContractVersion,
        Self::ExecuteCommand,
        Self::ResolveCommandOutcome,
        Self::GetEntity,
        Self::ScanIndex,
        Self::QueryProjection,
        Self::GetProjectionStatus,
        Self::GetCommit,
        Self::ScanCommits,
        Self::SubscribeToCommits,
        Self::TraceProvenance,
        Self::GetHealth,
        Self::GetStatistics,
        Self::CreateCapability,
        Self::RevokeCapability,
        Self::ListPendingOutboxDeliveries,
        Self::DiscoverCommandTools,
        Self::DiscoverResources,
        Self::DescribeContract,
        Self::CheckQuery,
        Self::ExplainQuery,
        Self::ExecuteQuery,
        Self::DeployQueryModule,
        Self::ApplyContractMigration,
        Self::DescribeEvent,
        Self::ReplayEvents,
        Self::TailEvents,
        Self::ExecuteProjectedQuery,
        Self::DeployReactiveModule,
        Self::ConsumeEventStream,
        Self::AcknowledgeEventStream,
        Self::NegativeAcknowledgeEventStream,
        Self::SeekEventStreamConsumer,
        Self::RetireEventStreamConsumer,
        Self::GetEventStreamConsumerStatus,
        Self::WatchNamedQuery,
        Self::ConsumeContextualSubscription,
        Self::AcknowledgeContextualSubscription,
        Self::NegativeAcknowledgeContextualSubscription,
        Self::GetContextualSubscriptionStatus,
        Self::ExecuteContextualReaction,
        Self::GetReactiveWakeup,
        Self::StartApplicationInstallation,
        Self::GetApplicationInstallation,
        Self::StartApplicationExport,
        Self::GetApplicationExportPage,
        Self::GetApplicationExport,
        Self::CancelApplicationExport,
        Self::StartApplicationReimport,
        Self::ApplyApplicationReimportPage,
        Self::GetApplicationReimport,
        Self::CancelApplicationReimport,
        Self::InspectVectorState,
        Self::RegisterFollower,
        Self::RetireFollower,
        Self::FenceReplicationPrimary,
        Self::StreamChangelog,
        Self::PromoteFollower,
    ];

    /// Returns the stable v1 semantic tag.
    #[must_use]
    pub const fn tag(self) -> u8 {
        match self {
            Self::ValidateContract => 0x01,
            Self::ExplainCommand => 0x02,
            Self::DeployContract => 0x03,
            Self::GetActiveContract => 0x04,
            Self::GetContractVersion => 0x05,
            Self::ExecuteCommand => 0x06,
            Self::ResolveCommandOutcome => 0x07,
            Self::GetEntity => 0x08,
            Self::ScanIndex => 0x09,
            Self::QueryProjection => 0x0a,
            Self::GetProjectionStatus => 0x0b,
            Self::GetCommit => 0x0c,
            Self::ScanCommits => 0x0d,
            Self::SubscribeToCommits => 0x0e,
            Self::TraceProvenance => 0x0f,
            Self::GetHealth => 0x10,
            Self::GetStatistics => 0x11,
            Self::CreateCapability => 0x12,
            Self::RevokeCapability => 0x13,
            Self::ListPendingOutboxDeliveries => 0x14,
            Self::DiscoverCommandTools => 0x15,
            Self::DiscoverResources => 0x16,
            Self::DescribeContract => 0x17,
            Self::CheckQuery => 0x18,
            Self::ExplainQuery => 0x19,
            Self::ExecuteQuery => 0x1a,
            Self::DeployQueryModule => 0x1b,
            Self::ApplyContractMigration => 0x1c,
            Self::DescribeEvent => 0x1d,
            Self::ReplayEvents => 0x1e,
            Self::TailEvents => 0x1f,
            Self::ExecuteProjectedQuery => 0x20,
            Self::DeployReactiveModule => 0x21,
            Self::ConsumeEventStream => 0x22,
            Self::AcknowledgeEventStream => 0x23,
            Self::NegativeAcknowledgeEventStream => 0x24,
            Self::SeekEventStreamConsumer => 0x25,
            Self::RetireEventStreamConsumer => 0x26,
            Self::GetEventStreamConsumerStatus => 0x27,
            Self::WatchNamedQuery => 0x28,
            Self::ConsumeContextualSubscription => 0x29,
            Self::AcknowledgeContextualSubscription => 0x2a,
            Self::NegativeAcknowledgeContextualSubscription => 0x2b,
            Self::GetContextualSubscriptionStatus => 0x2c,
            Self::ExecuteContextualReaction => 0x2d,
            Self::GetReactiveWakeup => 0x2e,
            Self::StartApplicationInstallation => 0x2f,
            Self::GetApplicationInstallation => 0x30,
            Self::StartApplicationExport => 0x31,
            Self::GetApplicationExportPage => 0x32,
            Self::GetApplicationExport => 0x33,
            Self::CancelApplicationExport => 0x34,
            Self::StartApplicationReimport => 0x35,
            Self::ApplyApplicationReimportPage => 0x36,
            Self::GetApplicationReimport => 0x37,
            Self::CancelApplicationReimport => 0x38,
            Self::InspectVectorState => 0x39,
            Self::RegisterFollower => 0x3a,
            Self::RetireFollower => 0x3b,
            Self::FenceReplicationPrimary => 0x3c,
            Self::StreamChangelog => 0x3d,
            Self::PromoteFollower => 0x3e,
        }
    }

    /// Decodes a stable v1 semantic tag, rejecting zero and unknown values.
    #[must_use]
    pub const fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            0x01 => Some(Self::ValidateContract),
            0x02 => Some(Self::ExplainCommand),
            0x03 => Some(Self::DeployContract),
            0x04 => Some(Self::GetActiveContract),
            0x05 => Some(Self::GetContractVersion),
            0x06 => Some(Self::ExecuteCommand),
            0x07 => Some(Self::ResolveCommandOutcome),
            0x08 => Some(Self::GetEntity),
            0x09 => Some(Self::ScanIndex),
            0x0a => Some(Self::QueryProjection),
            0x0b => Some(Self::GetProjectionStatus),
            0x0c => Some(Self::GetCommit),
            0x0d => Some(Self::ScanCommits),
            0x0e => Some(Self::SubscribeToCommits),
            0x0f => Some(Self::TraceProvenance),
            0x10 => Some(Self::GetHealth),
            0x11 => Some(Self::GetStatistics),
            0x12 => Some(Self::CreateCapability),
            0x13 => Some(Self::RevokeCapability),
            0x14 => Some(Self::ListPendingOutboxDeliveries),
            0x15 => Some(Self::DiscoverCommandTools),
            0x16 => Some(Self::DiscoverResources),
            0x17 => Some(Self::DescribeContract),
            0x18 => Some(Self::CheckQuery),
            0x19 => Some(Self::ExplainQuery),
            0x1a => Some(Self::ExecuteQuery),
            0x1b => Some(Self::DeployQueryModule),
            0x1c => Some(Self::ApplyContractMigration),
            0x1d => Some(Self::DescribeEvent),
            0x1e => Some(Self::ReplayEvents),
            0x1f => Some(Self::TailEvents),
            0x20 => Some(Self::ExecuteProjectedQuery),
            0x21 => Some(Self::DeployReactiveModule),
            0x22 => Some(Self::ConsumeEventStream),
            0x23 => Some(Self::AcknowledgeEventStream),
            0x24 => Some(Self::NegativeAcknowledgeEventStream),
            0x25 => Some(Self::SeekEventStreamConsumer),
            0x26 => Some(Self::RetireEventStreamConsumer),
            0x27 => Some(Self::GetEventStreamConsumerStatus),
            0x28 => Some(Self::WatchNamedQuery),
            0x29 => Some(Self::ConsumeContextualSubscription),
            0x2a => Some(Self::AcknowledgeContextualSubscription),
            0x2b => Some(Self::NegativeAcknowledgeContextualSubscription),
            0x2c => Some(Self::GetContextualSubscriptionStatus),
            0x2d => Some(Self::ExecuteContextualReaction),
            0x2e => Some(Self::GetReactiveWakeup),
            0x2f => Some(Self::StartApplicationInstallation),
            0x30 => Some(Self::GetApplicationInstallation),
            0x31 => Some(Self::StartApplicationExport),
            0x32 => Some(Self::GetApplicationExportPage),
            0x33 => Some(Self::GetApplicationExport),
            0x34 => Some(Self::CancelApplicationExport),
            0x35 => Some(Self::StartApplicationReimport),
            0x36 => Some(Self::ApplyApplicationReimportPage),
            0x37 => Some(Self::GetApplicationReimport),
            0x38 => Some(Self::CancelApplicationReimport),
            0x39 => Some(Self::InspectVectorState),
            0x3a => Some(Self::RegisterFollower),
            0x3b => Some(Self::RetireFollower),
            0x3c => Some(Self::FenceReplicationPrimary),
            0x3d => Some(Self::StreamChangelog),
            0x3e => Some(Self::PromoteFollower),
            _ => None,
        }
    }
}

/// The lifecycle phase of one durable service-audit record.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ServiceAuditPhaseV1 {
    /// Protected work has been durably admitted for this invocation.
    Started,
    /// A safe result or protected output is ready for release.
    Succeeded,
    /// Current policy denied the invocation.
    Denied,
    /// Work was cancelled without a known authoritative result or released output.
    Cancelled,
    /// Work failed without a known authoritative application or control-plane result.
    Failed,
    /// The authoritative command outcome is uncertain.
    OutcomeUncertain,
}

impl ServiceAuditPhaseV1 {
    /// Every accepted v1 audit phase, in tag order.
    pub const ALL: [Self; 6] = [
        Self::Started,
        Self::Succeeded,
        Self::Denied,
        Self::Cancelled,
        Self::Failed,
        Self::OutcomeUncertain,
    ];

    /// Returns the stable v1 semantic tag.
    #[must_use]
    pub const fn tag(self) -> u8 {
        match self {
            Self::Started => 0x01,
            Self::Succeeded => 0x02,
            Self::Denied => 0x03,
            Self::Cancelled => 0x04,
            Self::Failed => 0x05,
            Self::OutcomeUncertain => 0x06,
        }
    }

    /// Decodes a stable v1 semantic tag, rejecting zero and unknown values.
    #[must_use]
    pub const fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            0x01 => Some(Self::Started),
            0x02 => Some(Self::Succeeded),
            0x03 => Some(Self::Denied),
            0x04 => Some(Self::Cancelled),
            0x05 => Some(Self::Failed),
            0x06 => Some(Self::OutcomeUncertain),
            _ => None,
        }
    }
}

/// The trusted ingress by which an application-service invocation arrived.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ServiceIngressKindV1 {
    /// Public gRPC, including CLI, SDK, and production MCP stdio clients.
    Grpc,
    /// Hosted MCP Streamable HTTP.
    McpHttp,
    /// The in-process test and comparison application surface.
    InProcessTestComparison,
}

impl ServiceIngressKindV1 {
    /// Every accepted v1 ingress kind, in tag order.
    pub const ALL: [Self; 3] = [Self::Grpc, Self::McpHttp, Self::InProcessTestComparison];

    /// Returns the stable v1 semantic tag.
    #[must_use]
    pub const fn tag(self) -> u8 {
        match self {
            Self::Grpc => 0x01,
            Self::McpHttp => 0x02,
            Self::InProcessTestComparison => 0x03,
        }
    }

    /// Decodes a stable v1 semantic tag, rejecting zero and unknown values.
    #[must_use]
    pub const fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            0x01 => Some(Self::Grpc),
            0x02 => Some(Self::McpHttp),
            0x03 => Some(Self::InProcessTestComparison),
            _ => None,
        }
    }
}

/// The closed authoritative-result link carried by a service-audit record.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ServiceAuditLinkV1 {
    /// No authoritative application or control-plane result is known.
    None,
    /// A known committed command result, including a terminal replay.
    Command {
        /// The original application commit sequence.
        commit_sequence: CommitSequence,
        /// The original durable provenance identity.
        provenance_id: ProvenanceId,
    },
    /// A known control-plane mutation result.
    ControlPlane {
        /// The administration sequence assigned to the result.
        administration_sequence: AdministrationSequence,
    },
}

/// A bounded stable object reference addressed by a service invocation.
#[derive(Clone, Eq, Hash, PartialEq)]
pub enum ServiceAuditTargetV1 {
    /// One exact contract lineage.
    ContractLineage(ContractLineage),
    /// One version scoped to its exact contract lineage.
    ContractVersion {
        /// Exact contract lineage.
        lineage: ContractLineage,
        /// Nonzero contract version.
        version: ContractVersion,
    },
    /// One entity type scoped to its exact contract lineage.
    EntityType {
        /// Exact contract lineage.
        lineage: ContractLineage,
        /// Nonzero compiler-assigned entity type identity.
        entity_type_id: EntityTypeId,
    },
    /// One command scoped to its exact contract lineage.
    Command {
        /// Exact contract lineage.
        lineage: ContractLineage,
        /// Nonzero compiler-assigned command identity.
        command_id: CommandId,
    },
    /// One projection scoped to its exact contract lineage.
    Projection {
        /// Exact contract lineage.
        lineage: ContractLineage,
        /// Nonzero compiler-assigned projection identity.
        projection_id: ProjectionId,
    },
    /// One index scoped to its exact contract lineage.
    Index {
        /// Exact contract lineage.
        lineage: ContractLineage,
        /// Nonzero compiler-assigned index identity.
        index_id: IndexId,
    },
    /// One application commit.
    Commit(CommitSequence),
    /// One durable provenance record.
    Provenance(ProvenanceId),
    /// One authorization capability.
    Capability(CapabilityId),
    /// One exact immutable reactive operation and durable consumer identity.
    EventConsumer {
        /// Exact contract lineage.
        lineage: ContractLineage,
        /// Immutable reactive module identity.
        module_hash: ReactiveModuleHash,
        /// Exact stream operation name.
        operation_name: ReactiveOperationName,
        /// Domain-separated complete consumer identity.
        consumer_identity_hash: EventConsumerIdentityHash,
    },
    /// One follower registration scoped to its exact source lineage.
    ReplicationFollower(ReplicationFollowerAuditTargetV1),
}

impl ServiceAuditTargetV1 {
    /// Returns the stable ADR-0021 semantic tag.
    #[must_use]
    pub const fn tag(&self) -> u8 {
        match self {
            Self::ContractLineage(_) => 0x01,
            Self::ContractVersion { .. } => 0x02,
            Self::EntityType { .. } => 0x03,
            Self::Command { .. } => 0x04,
            Self::Projection { .. } => 0x05,
            Self::Index { .. } => 0x06,
            Self::Commit(_) => 0x07,
            Self::Provenance(_) => 0x08,
            Self::Capability(_) => 0x09,
            Self::EventConsumer { .. } => 0x0a,
            Self::ReplicationFollower(_) => 0x0b,
        }
    }

    /// Constructs the immutable ADR-0021 canonical comparison key.
    #[must_use]
    pub fn canonical_key(&self) -> Vec<u8> {
        let mut key = Vec::new();
        key.push(self.tag());
        match self {
            Self::ContractLineage(lineage) => append_lineage(&mut key, lineage),
            Self::ContractVersion { lineage, version } => {
                append_lineage(&mut key, lineage);
                key.extend_from_slice(&version.to_be_bytes());
            }
            Self::EntityType {
                lineage,
                entity_type_id,
            } => {
                append_lineage(&mut key, lineage);
                key.extend_from_slice(&entity_type_id.to_be_bytes());
            }
            Self::Command {
                lineage,
                command_id,
            } => {
                append_lineage(&mut key, lineage);
                key.extend_from_slice(&command_id.to_be_bytes());
            }
            Self::Projection {
                lineage,
                projection_id,
            } => {
                append_lineage(&mut key, lineage);
                key.extend_from_slice(&projection_id.to_be_bytes());
            }
            Self::Index { lineage, index_id } => {
                append_lineage(&mut key, lineage);
                key.extend_from_slice(&index_id.to_be_bytes());
            }
            Self::Commit(sequence) => key.extend_from_slice(&sequence.to_be_bytes()),
            Self::Provenance(provenance_id) => key.extend_from_slice(provenance_id.as_bytes()),
            Self::Capability(capability_id) => key.extend_from_slice(capability_id.as_bytes()),
            Self::ReplicationFollower(target) => {
                key.extend_from_slice(target.database_id().as_bytes());
                key.extend_from_slice(&target.history_incarnation().to_be_bytes());
                key.extend_from_slice(&target.leadership_epoch().get().to_be_bytes());
                key.extend_from_slice(target.hold_id().as_bytes());
            }
            Self::EventConsumer {
                lineage,
                module_hash,
                operation_name,
                consumer_identity_hash,
            } => {
                append_lineage(&mut key, lineage);
                key.extend_from_slice(module_hash.as_bytes());
                let operation = operation_name.as_str().as_bytes();
                key.extend_from_slice(&(operation.len() as u32).to_be_bytes());
                key.extend_from_slice(operation);
                key.extend_from_slice(consumer_identity_hash.as_bytes());
            }
        }
        key
    }
}

impl fmt::Debug for ServiceAuditTargetV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let variant = match self {
            Self::ContractLineage(_) => "ContractLineage",
            Self::ContractVersion { .. } => "ContractVersion",
            Self::EntityType { .. } => "EntityType",
            Self::Command { .. } => "Command",
            Self::Projection { .. } => "Projection",
            Self::Index { .. } => "Index",
            Self::Commit(_) => "Commit",
            Self::Provenance(_) => "Provenance",
            Self::Capability(_) => "Capability",
            Self::EventConsumer { .. } => "EventConsumer",
            Self::ReplicationFollower(_) => "ReplicationFollower",
        };
        write!(formatter, "ServiceAuditTargetV1::{variant}([REDACTED])")
    }
}

fn append_lineage(key: &mut Vec<u8>, lineage: &ContractLineage) {
    let lineage_bytes = lineage.as_bytes();
    // `ContractLineage` validation fixes this length at no more than 256 bytes.
    key.extend_from_slice(&(lineage_bytes.len() as u32).to_be_bytes());
    key.extend_from_slice(lineage_bytes);
}

/// A safe validation failure for a service-audit target collection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServiceAuditTargetsError {
    /// The collection contains more than the accepted v1 maximum.
    TooMany {
        /// The accepted maximum number of targets.
        maximum: usize,
    },
    /// The collection contains the same complete canonical target twice.
    Duplicate,
}

impl fmt::Display for ServiceAuditTargetsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooMany { maximum } => {
                write!(formatter, "service audit target count exceeds {maximum}")
            }
            Self::Duplicate => formatter.write_str("service audit targets contain a duplicate"),
        }
    }
}

impl Error for ServiceAuditTargetsError {}

/// A checked, duplicate-free service-audit target list in canonical order.
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct ServiceAuditTargetsV1(Vec<ServiceAuditTargetV1>);

impl ServiceAuditTargetsV1 {
    /// Checks operation-specific target shape without granting authority.
    /// Follower lifecycle audits identify exactly their selected registration.
    #[must_use]
    pub fn is_valid_for_operation(&self, operation: ServiceOperationV1) -> bool {
        match operation {
            ServiceOperationV1::RegisterFollower
            | ServiceOperationV1::RetireFollower
            | ServiceOperationV1::FenceReplicationPrimary
            | ServiceOperationV1::PromoteFollower => {
                matches!(
                    self.as_slice(),
                    [ServiceAuditTargetV1::ReplicationFollower(_)]
                )
            }
            ServiceOperationV1::StreamChangelog => matches!(
                self.as_slice(),
                [] | [ServiceAuditTargetV1::ReplicationFollower(_)]
            ),
            _ => true,
        }
    }

    /// Validates, canonicalizes, and constructs a list of zero through 16 targets.
    pub fn new(
        targets: impl IntoIterator<Item = ServiceAuditTargetV1>,
    ) -> Result<Self, ServiceAuditTargetsError> {
        let mut keyed_targets = Vec::with_capacity(MAX_SERVICE_AUDIT_TARGETS);
        for target in targets {
            if keyed_targets.len() == MAX_SERVICE_AUDIT_TARGETS {
                return Err(ServiceAuditTargetsError::TooMany {
                    maximum: MAX_SERVICE_AUDIT_TARGETS,
                });
            }
            keyed_targets.push((target.canonical_key(), target));
        }

        keyed_targets.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        if keyed_targets
            .windows(2)
            .any(|pair| pair[0].0.as_slice() == pair[1].0.as_slice())
        {
            return Err(ServiceAuditTargetsError::Duplicate);
        }

        Ok(Self(
            keyed_targets
                .into_iter()
                .map(|(_, target)| target)
                .collect(),
        ))
    }

    /// Constructs the canonical empty target list.
    #[must_use]
    pub const fn empty() -> Self {
        Self(Vec::new())
    }

    /// Borrows the checked targets in canonical order.
    #[must_use]
    pub fn as_slice(&self) -> &[ServiceAuditTargetV1] {
        &self.0
    }

    /// Returns the target count.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Reports whether the canonical target list is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Consumes the collection and returns its canonical target vector.
    #[must_use]
    pub fn into_vec(self) -> Vec<ServiceAuditTargetV1> {
        self.0
    }
}

impl fmt::Debug for ServiceAuditTargetsV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServiceAuditTargetsV1")
            .field("len", &self.len())
            .field("targets", &"[REDACTED]")
            .finish()
    }
}

impl TryFrom<Vec<ServiceAuditTargetV1>> for ServiceAuditTargetsV1 {
    type Error = ServiceAuditTargetsError;

    fn try_from(targets: Vec<ServiceAuditTargetV1>) -> Result<Self, Self::Error> {
        Self::new(targets)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lineage(value: &str) -> ContractLineage {
        ContractLineage::new(value).expect("valid test lineage")
    }

    fn version(value: u64) -> ContractVersion {
        ContractVersion::new(value).expect("nonzero test contract version")
    }

    fn entity_type_id(value: u32) -> EntityTypeId {
        EntityTypeId::new(value).expect("nonzero test entity type ID")
    }

    fn command_id(value: u32) -> CommandId {
        CommandId::new(value).expect("nonzero test command ID")
    }

    fn projection_id(value: u32) -> ProjectionId {
        ProjectionId::new(value).expect("nonzero test projection ID")
    }

    fn index_id(value: u32) -> IndexId {
        IndexId::new(value).expect("nonzero test index ID")
    }

    fn commit_sequence(value: u64) -> CommitSequence {
        CommitSequence::new(value).expect("nonzero test commit sequence")
    }

    fn uuid_bytes(seed: u8) -> [u8; 16] {
        let mut bytes = [seed; 16];
        bytes[6] = 0x70 | (seed & 0x0f);
        bytes[8] = 0x80 | (seed & 0x3f);
        bytes
    }

    fn provenance_id(seed: u8) -> ProvenanceId {
        ProvenanceId::from_bytes(uuid_bytes(seed)).expect("valid test UUIDv7")
    }

    fn capability_id(seed: u8) -> CapabilityId {
        CapabilityId::from_bytes(uuid_bytes(seed)).expect("valid test UUIDv7")
    }

    // req: REP-003, REP-005
    #[test]
    fn replication_stream_audit_targets_are_empty_or_one_selected_follower() {
        let follower = |seed| {
            ServiceAuditTargetV1::ReplicationFollower(
                ReplicationFollowerAuditTargetV1::new(
                    crate::DatabaseId::from_bytes(uuid_bytes(1)).unwrap(),
                    1,
                    crate::LeadershipEpochV1::initial(),
                    crate::ReplicationSourceHoldIdV1::new([seed; 16]).unwrap(),
                )
                .unwrap(),
            )
        };
        for targets in [vec![], vec![follower(1)]] {
            assert!(
                ServiceAuditTargetsV1::new(targets)
                    .unwrap()
                    .is_valid_for_operation(ServiceOperationV1::StreamChangelog)
            );
        }
        for targets in [
            vec![ServiceAuditTargetV1::Capability(capability_id(1))],
            vec![follower(1), follower(2)],
            vec![
                follower(1),
                ServiceAuditTargetV1::Commit(commit_sequence(1)),
            ],
        ] {
            assert!(
                !ServiceAuditTargetsV1::new(targets)
                    .unwrap()
                    .is_valid_for_operation(ServiceOperationV1::StreamChangelog)
            );
        }
    }

    #[test]
    fn service_operation_registry_is_exact_and_closed() {
        let expected: Vec<u8> = (0x01..=0x3e).collect();
        assert_eq!(
            ServiceOperationV1::ALL
                .into_iter()
                .map(ServiceOperationV1::tag)
                .collect::<Vec<_>>(),
            expected
        );
        for operation in ServiceOperationV1::ALL {
            assert_eq!(
                ServiceOperationV1::from_tag(operation.tag()),
                Some(operation)
            );
        }
        assert_eq!(ServiceOperationV1::from_tag(0), None);
        assert_eq!(
            ServiceOperationV1::from_tag(0x2e),
            Some(ServiceOperationV1::GetReactiveWakeup)
        );
        assert_eq!(
            ServiceOperationV1::from_tag(0x30),
            Some(ServiceOperationV1::GetApplicationInstallation)
        );
        assert_eq!(
            ServiceOperationV1::from_tag(0x31),
            Some(ServiceOperationV1::StartApplicationExport)
        );
        assert_eq!(
            ServiceOperationV1::from_tag(0x34),
            Some(ServiceOperationV1::CancelApplicationExport)
        );
        assert_eq!(
            ServiceOperationV1::from_tag(0x38),
            Some(ServiceOperationV1::CancelApplicationReimport)
        );
        assert_eq!(
            ServiceOperationV1::from_tag(0x39),
            Some(ServiceOperationV1::InspectVectorState)
        );
        assert_eq!(
            ServiceOperationV1::from_tag(0x3c),
            Some(ServiceOperationV1::FenceReplicationPrimary)
        );
        assert_eq!(
            ServiceOperationV1::from_tag(0x3d),
            Some(ServiceOperationV1::StreamChangelog)
        );
        assert_eq!(
            ServiceOperationV1::from_tag(0x3e),
            Some(ServiceOperationV1::PromoteFollower)
        );
        assert_eq!(ServiceOperationV1::from_tag(0x3f), None);
        assert_eq!(ServiceOperationV1::from_tag(u8::MAX), None);
    }

    #[test]
    // req: REP-006
    fn follower_lifecycle_operations_have_distinct_additive_audit_tags() {
        for (tag, name) in [(0x3a, "RegisterFollower"), (0x3b, "RetireFollower")] {
            let operation =
                ServiceOperationV1::from_tag(tag).expect("approved lifecycle operation");
            assert_eq!(format!("{operation:?}"), name);
            assert_eq!(operation.tag(), tag);
        }
        assert_eq!(ServiceOperationV1::InspectVectorState.tag(), 0x39);
    }

    #[test]
    fn audit_phase_and_ingress_registries_are_exact_and_closed() {
        assert_eq!(
            ServiceAuditPhaseV1::ALL.map(ServiceAuditPhaseV1::tag),
            [1, 2, 3, 4, 5, 6]
        );
        for phase in ServiceAuditPhaseV1::ALL {
            assert_eq!(ServiceAuditPhaseV1::from_tag(phase.tag()), Some(phase));
        }
        assert_eq!(ServiceAuditPhaseV1::from_tag(0), None);
        assert_eq!(ServiceAuditPhaseV1::from_tag(7), None);

        assert_eq!(
            ServiceIngressKindV1::ALL.map(ServiceIngressKindV1::tag),
            [1, 2, 3]
        );
        for ingress in ServiceIngressKindV1::ALL {
            assert_eq!(ServiceIngressKindV1::from_tag(ingress.tag()), Some(ingress));
        }
        assert_eq!(ServiceIngressKindV1::from_tag(0), None);
        assert_eq!(ServiceIngressKindV1::from_tag(4), None);
    }

    #[test]
    fn result_link_is_closed_and_command_link_is_all_or_nothing() {
        let command = ServiceAuditLinkV1::Command {
            commit_sequence: commit_sequence(7),
            provenance_id: provenance_id(0x41),
        };
        assert!(matches!(
            command,
            ServiceAuditLinkV1::Command {
                commit_sequence: sequence,
                provenance_id: _
            } if sequence == commit_sequence(7)
        ));

        let control_plane = ServiceAuditLinkV1::ControlPlane {
            administration_sequence: AdministrationSequence::new(9)
                .expect("nonzero test administration sequence"),
        };
        assert!(matches!(
            control_plane,
            ServiceAuditLinkV1::ControlPlane {
                administration_sequence
            } if administration_sequence.get() == 9
        ));
        assert_eq!(ServiceAuditLinkV1::None, ServiceAuditLinkV1::None);
    }

    #[test]
    fn all_nine_target_tags_and_canonical_keys_are_frozen() {
        let contract_lineage = lineage("LegalSpend");
        let targets = [
            ServiceAuditTargetV1::ContractLineage(contract_lineage.clone()),
            ServiceAuditTargetV1::ContractVersion {
                lineage: contract_lineage.clone(),
                version: version(2),
            },
            ServiceAuditTargetV1::EntityType {
                lineage: contract_lineage.clone(),
                entity_type_id: entity_type_id(3),
            },
            ServiceAuditTargetV1::Command {
                lineage: contract_lineage.clone(),
                command_id: command_id(4),
            },
            ServiceAuditTargetV1::Projection {
                lineage: contract_lineage.clone(),
                projection_id: projection_id(5),
            },
            ServiceAuditTargetV1::Index {
                lineage: contract_lineage.clone(),
                index_id: index_id(6),
            },
            ServiceAuditTargetV1::Commit(commit_sequence(7)),
            ServiceAuditTargetV1::Provenance(provenance_id(8)),
            ServiceAuditTargetV1::Capability(capability_id(9)),
        ];

        assert_eq!(
            targets.each_ref().map(|target| target.tag()),
            [1, 2, 3, 4, 5, 6, 7, 8, 9]
        );

        let mut lineage_frame = Vec::from(10_u32.to_be_bytes());
        lineage_frame.extend_from_slice(b"LegalSpend");
        for (target, suffix) in targets[..6].iter().zip([
            Vec::new(),
            2_u64.to_be_bytes().to_vec(),
            3_u32.to_be_bytes().to_vec(),
            4_u32.to_be_bytes().to_vec(),
            5_u32.to_be_bytes().to_vec(),
            6_u32.to_be_bytes().to_vec(),
        ]) {
            let mut expected = vec![target.tag()];
            expected.extend_from_slice(&lineage_frame);
            expected.extend_from_slice(&suffix);
            assert_eq!(target.canonical_key(), expected);
        }

        let mut expected_commit = vec![0x07];
        expected_commit.extend_from_slice(&7_u64.to_be_bytes());
        assert_eq!(targets[6].canonical_key(), expected_commit);

        let mut expected_provenance = vec![0x08];
        expected_provenance.extend_from_slice(&uuid_bytes(8));
        assert_eq!(targets[7].canonical_key(), expected_provenance);

        let mut expected_capability = vec![0x09];
        expected_capability.extend_from_slice(&uuid_bytes(9));
        assert_eq!(targets[8].canonical_key(), expected_capability);
    }

    #[test]
    fn construction_is_permutation_independent_and_uses_complete_keys() {
        let short_lineage = ServiceAuditTargetV1::Command {
            lineage: lineage("z"),
            command_id: command_id(1),
        };
        let long_lineage = ServiceAuditTargetV1::Command {
            lineage: lineage("aa"),
            command_id: command_id(1),
        };
        let other_tag = ServiceAuditTargetV1::Commit(commit_sequence(1));

        let first = ServiceAuditTargetsV1::new([
            other_tag.clone(),
            long_lineage.clone(),
            short_lineage.clone(),
        ])
        .expect("distinct bounded targets are valid");
        let second = ServiceAuditTargetsV1::new([
            short_lineage.clone(),
            other_tag.clone(),
            long_lineage.clone(),
        ])
        .expect("permutation is valid");

        assert_eq!(first, second);
        assert_eq!(first.as_slice(), &[short_lineage, long_lineage, other_tag]);
    }

    #[test]
    fn equal_local_ids_in_distinct_lineages_are_distinct() {
        let targets = ServiceAuditTargetsV1::new([
            ServiceAuditTargetV1::EntityType {
                lineage: lineage("A"),
                entity_type_id: entity_type_id(1),
            },
            ServiceAuditTargetV1::EntityType {
                lineage: lineage("B"),
                entity_type_id: entity_type_id(1),
            },
        ])
        .expect("lineage scopes make the targets distinct");
        assert_eq!(targets.len(), 2);
    }

    #[test]
    fn duplicate_and_seventeenth_targets_fail_closed_without_payloads() {
        let duplicate = ServiceAuditTargetV1::Command {
            lineage: lineage("SensitiveLineage"),
            command_id: command_id(42),
        };
        let error = ServiceAuditTargetsV1::new([duplicate.clone(), duplicate])
            .expect_err("exact duplicate must reject");
        assert_eq!(error, ServiceAuditTargetsError::Duplicate);
        assert!(!error.to_string().contains("SensitiveLineage"));
        assert!(!format!("{error:?}").contains("SensitiveLineage"));

        let too_many = (1..=17)
            .map(commit_sequence)
            .map(ServiceAuditTargetV1::Commit);
        let error =
            ServiceAuditTargetsV1::new(too_many).expect_err("a seventeenth target must reject");
        assert_eq!(
            error,
            ServiceAuditTargetsError::TooMany {
                maximum: MAX_SERVICE_AUDIT_TARGETS,
            }
        );
    }

    #[test]
    fn empty_and_exactly_sixteen_targets_are_valid() {
        let empty = ServiceAuditTargetsV1::new([]).expect("empty list is canonical");
        assert!(empty.is_empty());
        assert_eq!(empty, ServiceAuditTargetsV1::empty());

        let full = ServiceAuditTargetsV1::new(
            (1..=16)
                .map(commit_sequence)
                .rev()
                .map(ServiceAuditTargetV1::Commit),
        )
        .expect("exactly sixteen distinct targets are valid");
        assert_eq!(full.len(), MAX_SERVICE_AUDIT_TARGETS);
        for (index, target) in full.as_slice().iter().enumerate() {
            assert_eq!(
                target,
                &ServiceAuditTargetV1::Commit(commit_sequence((index + 1) as u64))
            );
        }
    }

    #[test]
    fn target_and_collection_debug_output_is_redacted() {
        let target = ServiceAuditTargetV1::ContractVersion {
            lineage: lineage("SensitiveLineage"),
            version: version(123),
        };
        let debug = format!("{target:?}");
        assert!(debug.contains("ContractVersion"));
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("SensitiveLineage"));
        assert!(!debug.contains("123"));

        let targets = ServiceAuditTargetsV1::new([target]).expect("one target is valid");
        let debug = format!("{targets:?}");
        assert!(debug.contains("len: 1"));
        assert!(!debug.contains("SensitiveLineage"));
    }
}
