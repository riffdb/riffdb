//! Concrete pre-sequence service-audit input owned by the application service.

use std::fmt;
use std::num::NonZeroU64;

use riffdb_commit::AdministrationAuditInputView;
use riffdb_policy::ProvenanceSelector;
use riffdb_types::{
    ActorId, ActorKind, ApprovalId, CapabilityId, CommandId, CommitSequence, ContractLineage,
    ContractVersion, EntityTypeId, IndexId, ProjectionId, RequestId, ServiceAuditLinkV1,
    ServiceAuditPhaseV1, ServiceAuditTargetV1, ServiceAuditTargetsError, ServiceAuditTargetsV1,
    ServiceIngressKindV1, ServiceOperationV1,
};

use crate::RequestContext;

/// Checked service-owned input consumed by the coordinator audit executor.
///
/// This value contains no timestamp or sequence field. A control-plane result
/// link may name an already assigned authoritative transition sequence, but it
/// can never select the sequence of the new audit record.
pub(crate) struct ServiceAuditInput {
    request_id: RequestId,
    operation: ServiceOperationV1,
    phase: ServiceAuditPhaseV1,
    principal_id: ActorId,
    actor_kind: ActorKind,
    capability_id: CapabilityId,
    capability_revision: NonZeroU64,
    ingress: ServiceIngressKindV1,
    targets: ServiceAuditTargetsV1,
    approval_id: Option<ApprovalId>,
    link: ServiceAuditLinkV1,
}

impl ServiceAuditInput {
    /// Copies the exact authenticated invocation identity into one audit phase.
    pub(crate) fn new(
        context: &RequestContext,
        operation: ServiceOperationV1,
        phase: ServiceAuditPhaseV1,
        targets: ServiceAuditTargetsV1,
        approval_id: Option<ApprovalId>,
        link: ServiceAuditLinkV1,
    ) -> Result<Self, ServiceAuditInputError> {
        validate_phase_link(operation, phase, link)?;
        Ok(Self {
            request_id: context.request_id(),
            operation,
            phase,
            principal_id: context.principal().principal_id().clone(),
            actor_kind: context.principal().actor_kind(),
            capability_id: context.principal().capability_id(),
            capability_revision: context.principal().capability_revision(),
            ingress: context.ingress(),
            targets,
            approval_id,
            link,
        })
    }
}

fn validate_phase_link(
    operation: ServiceOperationV1,
    phase: ServiceAuditPhaseV1,
    link: ServiceAuditLinkV1,
) -> Result<(), ServiceAuditInputError> {
    if phase != ServiceAuditPhaseV1::Succeeded && link != ServiceAuditLinkV1::None {
        return Err(ServiceAuditInputError::InvalidPhaseLink);
    }
    let operation_accepts_link = match link {
        ServiceAuditLinkV1::None => true,
        ServiceAuditLinkV1::Command { .. } => matches!(
            operation,
            ServiceOperationV1::ExecuteCommand | ServiceOperationV1::ResolveCommandOutcome
        ),
        ServiceAuditLinkV1::ControlPlane { .. } => matches!(
            operation,
            ServiceOperationV1::DeployContract
                | ServiceOperationV1::DeployQueryModule
                | ServiceOperationV1::CreateCapability
                | ServiceOperationV1::RevokeCapability
        ),
    };
    if !operation_accepts_link {
        return Err(ServiceAuditInputError::InvalidOperationLink);
    }
    Ok(())
}

impl AdministrationAuditInputView for ServiceAuditInput {
    fn request_id(&self) -> &RequestId {
        &self.request_id
    }

    fn operation(&self) -> &ServiceOperationV1 {
        &self.operation
    }

    fn phase(&self) -> &ServiceAuditPhaseV1 {
        &self.phase
    }

    fn principal_id(&self) -> &ActorId {
        &self.principal_id
    }

    fn actor_kind(&self) -> &ActorKind {
        &self.actor_kind
    }

    fn capability_id(&self) -> &CapabilityId {
        &self.capability_id
    }

    fn capability_revision(&self) -> &NonZeroU64 {
        &self.capability_revision
    }

    fn ingress(&self) -> &ServiceIngressKindV1 {
        &self.ingress
    }

    fn targets(&self) -> &ServiceAuditTargetsV1 {
        &self.targets
    }

    fn approval_id(&self) -> Option<&ApprovalId> {
        self.approval_id.as_ref()
    }

    fn link(&self) -> &ServiceAuditLinkV1 {
        &self.link
    }
}

impl fmt::Debug for ServiceAuditInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ServiceAuditInput([REDACTED])")
    }
}

/// Safe structural rejection before an audit input can reach the coordinator.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServiceAuditInputError {
    /// A non-success phase attempted to claim an authoritative result link.
    InvalidPhaseLink,
    /// The closed operation cannot produce the selected authoritative link kind.
    InvalidOperationLink,
}

impl fmt::Display for ServiceAuditInputError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidPhaseLink => "service audit phase and result link are inconsistent",
            Self::InvalidOperationLink => {
                "service audit operation and result link are inconsistent"
            }
        })
    }
}

impl std::error::Error for ServiceAuditInputError {}

/// Exhaustive request-only ADR-0021 target construction.
///
/// These functions accept only independently addressed request identities. No
/// authorizing capability, result link, traversed object, or returned row can
/// enter through this surface.
pub(crate) struct ServiceAuditTargetMap;

impl ServiceAuditTargetMap {
    /// Targets for contract validation.
    #[must_use]
    pub(crate) const fn validate_contract() -> ServiceAuditTargetsV1 {
        ServiceAuditTargetsV1::empty()
    }

    /// Targets for one exact command explanation.
    pub(crate) fn explain_command(
        lineage: ContractLineage,
        version: ContractVersion,
        command_id: CommandId,
    ) -> Result<ServiceAuditTargetsV1, ServiceAuditTargetsError> {
        version_and_command(lineage, version, command_id)
    }

    /// Targets for one contract deployment.
    pub(crate) fn deploy_contract(
        lineage: ContractLineage,
        version: ContractVersion,
    ) -> Result<ServiceAuditTargetsV1, ServiceAuditTargetsError> {
        one(ServiceAuditTargetV1::ContractVersion { lineage, version })
    }

    /// Targets for the singleton active-contract read.
    #[must_use]
    pub(crate) const fn get_active_contract() -> ServiceAuditTargetsV1 {
        ServiceAuditTargetsV1::empty()
    }

    /// Targets for one historical contract version.
    pub(crate) fn get_contract_version(
        lineage: ContractLineage,
        version: ContractVersion,
    ) -> Result<ServiceAuditTargetsV1, ServiceAuditTargetsError> {
        one(ServiceAuditTargetV1::ContractVersion { lineage, version })
    }

    /// Targets for one symbolic operation pinned to an exact contract.
    pub(crate) fn symbolic_query(
        lineage: ContractLineage,
        version: ContractVersion,
    ) -> Result<ServiceAuditTargetsV1, ServiceAuditTargetsError> {
        one(ServiceAuditTargetV1::ContractVersion { lineage, version })
    }

    /// Targets for one exact classified command execution.
    pub(crate) fn execute_command(
        lineage: ContractLineage,
        version: ContractVersion,
        command_id: CommandId,
    ) -> Result<ServiceAuditTargetsV1, ServiceAuditTargetsError> {
        version_and_command(lineage, version, command_id)
    }

    /// Targets for command-outcome resolution selected before lookup.
    pub(crate) fn resolve_command_outcome(
        lineage: ContractLineage,
        command_id: CommandId,
    ) -> Result<ServiceAuditTargetsV1, ServiceAuditTargetsError> {
        one(ServiceAuditTargetV1::Command {
            lineage,
            command_id,
        })
    }

    /// Targets for one exact entity read.
    pub(crate) fn get_entity(
        lineage: ContractLineage,
        version: ContractVersion,
        entity_type_id: EntityTypeId,
    ) -> Result<ServiceAuditTargetsV1, ServiceAuditTargetsError> {
        ServiceAuditTargetsV1::new([
            ServiceAuditTargetV1::ContractVersion {
                lineage: lineage.clone(),
                version,
            },
            ServiceAuditTargetV1::EntityType {
                lineage,
                entity_type_id,
            },
        ])
    }

    /// Targets for one exact index scan.
    pub(crate) fn scan_index(
        lineage: ContractLineage,
        version: ContractVersion,
        index_id: IndexId,
    ) -> Result<ServiceAuditTargetsV1, ServiceAuditTargetsError> {
        ServiceAuditTargetsV1::new([
            ServiceAuditTargetV1::ContractVersion {
                lineage: lineage.clone(),
                version,
            },
            ServiceAuditTargetV1::Index { lineage, index_id },
        ])
    }

    /// Targets for one exact projection query.
    pub(crate) fn query_projection(
        lineage: ContractLineage,
        version: ContractVersion,
        projection_id: ProjectionId,
    ) -> Result<ServiceAuditTargetsV1, ServiceAuditTargetsError> {
        version_and_projection(lineage, version, projection_id)
    }

    /// Targets for one exact projection-status read.
    pub(crate) fn get_projection_status(
        lineage: ContractLineage,
        version: ContractVersion,
        projection_id: ProjectionId,
    ) -> Result<ServiceAuditTargetsV1, ServiceAuditTargetsError> {
        version_and_projection(lineage, version, projection_id)
    }

    /// Targets for one exact commit read.
    pub(crate) fn get_commit(
        sequence: CommitSequence,
    ) -> Result<ServiceAuditTargetsV1, ServiceAuditTargetsError> {
        one(ServiceAuditTargetV1::Commit(sequence))
    }

    /// Targets for a broad bounded commit scan.
    #[must_use]
    pub(crate) const fn scan_commits() -> ServiceAuditTargetsV1 {
        ServiceAuditTargetsV1::empty()
    }

    /// Targets for broad commit-subscription establishment.
    #[must_use]
    pub(crate) const fn subscribe_to_commits() -> ServiceAuditTargetsV1 {
        ServiceAuditTargetsV1::empty()
    }

    /// Targets for the request-selected provenance root only.
    pub(crate) fn trace_provenance(
        selector: ProvenanceSelector,
    ) -> Result<ServiceAuditTargetsV1, ServiceAuditTargetsError> {
        match selector {
            ProvenanceSelector::Commit(sequence) => one(ServiceAuditTargetV1::Commit(sequence)),
            ProvenanceSelector::Provenance(provenance_id) => {
                one(ServiceAuditTargetV1::Provenance(provenance_id))
            }
        }
    }

    /// Targets for health.
    #[must_use]
    pub(crate) const fn health() -> ServiceAuditTargetsV1 {
        ServiceAuditTargetsV1::empty()
    }

    /// Targets for statistics.
    #[must_use]
    pub(crate) const fn statistics() -> ServiceAuditTargetsV1 {
        ServiceAuditTargetsV1::empty()
    }

    /// Targets for one normal or bootstrap capability creation.
    pub(crate) fn create_capability(
        capability_id: CapabilityId,
    ) -> Result<ServiceAuditTargetsV1, ServiceAuditTargetsError> {
        one(ServiceAuditTargetV1::Capability(capability_id))
    }

    /// Targets for one capability revocation.
    pub(crate) fn revoke_capability(
        capability_id: CapabilityId,
    ) -> Result<ServiceAuditTargetsV1, ServiceAuditTargetsError> {
        one(ServiceAuditTargetV1::Capability(capability_id))
    }

    /// Targets for a broad bounded outbox-status page.
    #[must_use]
    pub(crate) const fn list_pending_outbox_deliveries() -> ServiceAuditTargetsV1 {
        ServiceAuditTargetsV1::empty()
    }

    /// Targets for policy-filtered command discovery.
    #[must_use]
    pub(crate) const fn discover_command_tools() -> ServiceAuditTargetsV1 {
        ServiceAuditTargetsV1::empty()
    }

    /// Targets for policy-filtered resource discovery.
    #[must_use]
    pub(crate) const fn discover_resources() -> ServiceAuditTargetsV1 {
        ServiceAuditTargetsV1::empty()
    }
}

fn one(target: ServiceAuditTargetV1) -> Result<ServiceAuditTargetsV1, ServiceAuditTargetsError> {
    ServiceAuditTargetsV1::new([target])
}

fn version_and_command(
    lineage: ContractLineage,
    version: ContractVersion,
    command_id: CommandId,
) -> Result<ServiceAuditTargetsV1, ServiceAuditTargetsError> {
    ServiceAuditTargetsV1::new([
        ServiceAuditTargetV1::ContractVersion {
            lineage: lineage.clone(),
            version,
        },
        ServiceAuditTargetV1::Command {
            lineage,
            command_id,
        },
    ])
}

fn version_and_projection(
    lineage: ContractLineage,
    version: ContractVersion,
    projection_id: ProjectionId,
) -> Result<ServiceAuditTargetsV1, ServiceAuditTargetsError> {
    ServiceAuditTargetsV1::new([
        ServiceAuditTargetV1::ContractVersion {
            lineage: lineage.clone(),
            version,
        },
        ServiceAuditTargetV1::Projection {
            lineage,
            projection_id,
        },
    ])
}

#[cfg(test)]
mod tests {
    use riffdb_policy::ProvenanceSelector;
    use riffdb_types::{
        CapabilityId, CommandId, CommitSequence, ContractLineage, ContractVersion, EntityTypeId,
        IndexId, ProjectionId, ProvenanceId,
    };

    use super::*;

    #[test]
    fn only_succeeded_may_carry_an_authoritative_result_link() {
        let link = ServiceAuditLinkV1::Command {
            commit_sequence: CommitSequence::first(),
            provenance_id: ProvenanceId::from_bytes([
                0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0x70, 0x01, 0x82, 0x03, 0x04, 0x05, 0x06, 0x07,
                0x08, 0x09,
            ])
            .expect("UUIDv7"),
        };
        for phase in [
            ServiceAuditPhaseV1::Started,
            ServiceAuditPhaseV1::Denied,
            ServiceAuditPhaseV1::Cancelled,
            ServiceAuditPhaseV1::Failed,
            ServiceAuditPhaseV1::OutcomeUncertain,
        ] {
            assert_eq!(
                validate_phase_link(ServiceOperationV1::ExecuteCommand, phase, link),
                Err(ServiceAuditInputError::InvalidPhaseLink)
            );
        }
        assert_eq!(
            validate_phase_link(
                ServiceOperationV1::ExecuteCommand,
                ServiceAuditPhaseV1::Succeeded,
                link
            ),
            Ok(())
        );
        assert_eq!(
            validate_phase_link(
                ServiceOperationV1::GetCommit,
                ServiceAuditPhaseV1::Succeeded,
                link
            ),
            Err(ServiceAuditInputError::InvalidOperationLink)
        );
    }

    #[test]
    fn request_target_map_covers_every_current_public_operation() {
        let lineage = ContractLineage::new("example.contract").expect("bounded lineage");
        let version = ContractVersion::new(1).expect("version one is valid");
        let command_id = CommandId::first();
        let entity_type_id = EntityTypeId::first();
        let index_id = IndexId::first();
        let projection_id = ProjectionId::first();
        let sequence = CommitSequence::first();
        let provenance_id = ProvenanceId::from_bytes(uuid_bytes(0x31)).expect("UUIDv7");
        let capability_id = CapabilityId::from_bytes(uuid_bytes(0x41)).expect("UUIDv7");

        let mapped = [
            (
                ServiceOperationV1::ValidateContract,
                ServiceAuditTargetMap::validate_contract(),
            ),
            (
                ServiceOperationV1::ExplainCommand,
                ServiceAuditTargetMap::explain_command(lineage.clone(), version, command_id)
                    .expect("canonical targets"),
            ),
            (
                ServiceOperationV1::DeployContract,
                ServiceAuditTargetMap::deploy_contract(lineage.clone(), version)
                    .expect("canonical targets"),
            ),
            (
                ServiceOperationV1::GetActiveContract,
                ServiceAuditTargetMap::get_active_contract(),
            ),
            (
                ServiceOperationV1::GetContractVersion,
                ServiceAuditTargetMap::get_contract_version(lineage.clone(), version)
                    .expect("canonical targets"),
            ),
            (
                ServiceOperationV1::ExecuteCommand,
                ServiceAuditTargetMap::execute_command(lineage.clone(), version, command_id)
                    .expect("canonical targets"),
            ),
            (
                ServiceOperationV1::ResolveCommandOutcome,
                ServiceAuditTargetMap::resolve_command_outcome(lineage.clone(), command_id)
                    .expect("canonical targets"),
            ),
            (
                ServiceOperationV1::GetEntity,
                ServiceAuditTargetMap::get_entity(lineage.clone(), version, entity_type_id)
                    .expect("canonical targets"),
            ),
            (
                ServiceOperationV1::ScanIndex,
                ServiceAuditTargetMap::scan_index(lineage.clone(), version, index_id)
                    .expect("canonical targets"),
            ),
            (
                ServiceOperationV1::QueryProjection,
                ServiceAuditTargetMap::query_projection(lineage.clone(), version, projection_id)
                    .expect("canonical targets"),
            ),
            (
                ServiceOperationV1::GetProjectionStatus,
                ServiceAuditTargetMap::get_projection_status(
                    lineage.clone(),
                    version,
                    projection_id,
                )
                .expect("canonical targets"),
            ),
            (
                ServiceOperationV1::GetCommit,
                ServiceAuditTargetMap::get_commit(sequence).expect("canonical targets"),
            ),
            (
                ServiceOperationV1::ScanCommits,
                ServiceAuditTargetMap::scan_commits(),
            ),
            (
                ServiceOperationV1::SubscribeToCommits,
                ServiceAuditTargetMap::subscribe_to_commits(),
            ),
            (
                ServiceOperationV1::TraceProvenance,
                ServiceAuditTargetMap::trace_provenance(ProvenanceSelector::Provenance(
                    provenance_id,
                ))
                .expect("canonical targets"),
            ),
            (
                ServiceOperationV1::GetHealth,
                ServiceAuditTargetMap::health(),
            ),
            (
                ServiceOperationV1::GetStatistics,
                ServiceAuditTargetMap::statistics(),
            ),
            (
                ServiceOperationV1::CreateCapability,
                ServiceAuditTargetMap::create_capability(capability_id).expect("canonical targets"),
            ),
            (
                ServiceOperationV1::RevokeCapability,
                ServiceAuditTargetMap::revoke_capability(capability_id).expect("canonical targets"),
            ),
            (
                ServiceOperationV1::ListPendingOutboxDeliveries,
                ServiceAuditTargetMap::list_pending_outbox_deliveries(),
            ),
            (
                ServiceOperationV1::DiscoverCommandTools,
                ServiceAuditTargetMap::discover_command_tools(),
            ),
            (
                ServiceOperationV1::DiscoverResources,
                ServiceAuditTargetMap::discover_resources(),
            ),
            (
                ServiceOperationV1::DescribeContract,
                ServiceAuditTargetMap::symbolic_query(lineage.clone(), version)
                    .expect("canonical targets"),
            ),
            (
                ServiceOperationV1::CheckQuery,
                ServiceAuditTargetMap::symbolic_query(lineage.clone(), version)
                    .expect("canonical targets"),
            ),
            (
                ServiceOperationV1::ExplainQuery,
                ServiceAuditTargetMap::symbolic_query(lineage.clone(), version)
                    .expect("canonical targets"),
            ),
            (
                ServiceOperationV1::ExecuteQuery,
                ServiceAuditTargetMap::symbolic_query(lineage.clone(), version)
                    .expect("canonical targets"),
            ),
            (
                ServiceOperationV1::DeployQueryModule,
                ServiceAuditTargetMap::symbolic_query(lineage.clone(), version)
                    .expect("canonical targets"),
            ),
            (
                ServiceOperationV1::DescribeEvent,
                ServiceAuditTargetMap::symbolic_query(lineage.clone(), version)
                    .expect("canonical targets"),
            ),
            (
                ServiceOperationV1::ReplayEvents,
                ServiceAuditTargetMap::symbolic_query(lineage.clone(), version)
                    .expect("canonical targets"),
            ),
            (
                ServiceOperationV1::TailEvents,
                ServiceAuditTargetMap::symbolic_query(lineage.clone(), version)
                    .expect("canonical targets"),
            ),
            (
                ServiceOperationV1::ExecuteProjectedQuery,
                ServiceAuditTargetMap::symbolic_query(lineage.clone(), version)
                    .expect("canonical targets"),
            ),
        ];

        let public_operations = ServiceOperationV1::ALL
            .into_iter()
            .filter(|operation| *operation != ServiceOperationV1::ApplyContractMigration)
            .collect::<Vec<_>>();
        assert_eq!(mapped.len(), public_operations.len());
        assert_eq!(
            mapped
                .each_ref()
                .map(|(operation, _)| *operation)
                .as_slice(),
            public_operations
        );

        let expected_nonempty_lengths = [
            0, 2, 1, 0, 1, 2, 1, 2, 2, 2, 2, 1, 0, 0, 1, 0, 0, 1, 1, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1,
            1, 1,
        ];
        assert_eq!(
            mapped.each_ref().map(|(_, targets)| targets.len()),
            expected_nonempty_lengths
        );
        assert_eq!(
            mapped[1].1.as_slice(),
            [
                ServiceAuditTargetV1::ContractVersion {
                    lineage: lineage.clone(),
                    version,
                },
                ServiceAuditTargetV1::Command {
                    lineage: lineage.clone(),
                    command_id,
                },
            ]
        );
        assert_eq!(
            mapped[6].1.as_slice(),
            [ServiceAuditTargetV1::Command {
                lineage: lineage.clone(),
                command_id,
            }]
        );
        assert_eq!(
            mapped[14].1.as_slice(),
            [ServiceAuditTargetV1::Provenance(provenance_id)]
        );
        assert_eq!(
            mapped[17].1.as_slice(),
            [ServiceAuditTargetV1::Capability(capability_id)]
        );
    }

    fn uuid_bytes(seed: u8) -> [u8; 16] {
        let mut bytes = [seed; 16];
        bytes[6] = 0x70 | (seed & 0x0f);
        bytes[8] = 0x80 | (seed & 0x3f);
        bytes
    }
}
