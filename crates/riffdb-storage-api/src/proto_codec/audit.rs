use riffdb_proto::storage::v1 as wire;
use riffdb_types::{
    AdministrationSequence, ApprovalId, CapabilityId, CommandId, CommitSequence, ContractLineage,
    ContractVersion, EntityTypeId, IndexId, ProjectionId, ProvenanceId, RequestId,
    ServiceAuditLinkV1, ServiceAuditPhaseV1, ServiceAuditTargetV1, ServiceAuditTargetsV1,
    ServiceIngressKindV1, ServiceOperationV1,
};

use crate::{EncodedPageItem, StoredServiceAuditRecordV1};

use super::{
    CanonicalStoredEnvelopeV1, DurableCodecError, audit_principal_from_proto,
    audit_principal_to_proto, decode_message, encode_message, fixed, require, storage_result,
    timestamp_from_proto, timestamp_to_proto,
};

const AUDIT: &str = "riffdb.storage.v1.ServiceAuditRecordV1";
const SERVICE_AUDIT_REQUEST_INDEX: &str = "riffdb.storage.v1.StoredServiceAuditRequestIndexV1";

/// Durable secondary-index value binding one request identity to one audit sequence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StoredServiceAuditRequestIndexV1 {
    request_id: RequestId,
    administration_sequence: AdministrationSequence,
}

impl StoredServiceAuditRequestIndexV1 {
    /// Constructs one self-describing index row.
    #[must_use]
    pub const fn new(
        request_id: RequestId,
        administration_sequence: AdministrationSequence,
    ) -> Self {
        Self {
            request_id,
            administration_sequence,
        }
    }

    /// Returns the request identity.
    #[must_use]
    pub const fn request_id(self) -> RequestId {
        self.request_id
    }

    /// Returns the administration sequence.
    #[must_use]
    pub const fn administration_sequence(self) -> AdministrationSequence {
        self.administration_sequence
    }
}

/// Encodes one service-audit request index row.
pub fn encode_service_audit_request_index_v1(
    value: StoredServiceAuditRequestIndexV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_message(
        SERVICE_AUDIT_REQUEST_INDEX,
        &wire::StoredServiceAuditRequestIndexV1 {
            request_id: value.request_id().as_bytes().to_vec(),
            administration_sequence: value.administration_sequence().get(),
        },
    )
}

/// Decodes one service-audit request index row.
pub fn decode_service_audit_request_index_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredServiceAuditRequestIndexV1>, DurableCodecError> {
    decode_message::<wire::StoredServiceAuditRequestIndexV1, _, _>(
        SERVICE_AUDIT_REQUEST_INDEX,
        encoded,
        |value| {
            let request_id = RequestId::from_bytes(fixed(value.request_id)?)
                .map_err(|_| DurableCodecError::corrupt())?;
            let administration_sequence =
                AdministrationSequence::new(value.administration_sequence)
                    .ok_or_else(DurableCodecError::corrupt)?;
            Ok(StoredServiceAuditRequestIndexV1::new(
                request_id,
                administration_sequence,
            ))
        },
    )
}

fn operation_from_proto(value: i32) -> Result<ServiceOperationV1, DurableCodecError> {
    let tag = u8::try_from(value).map_err(|_| DurableCodecError::corrupt())?;
    ServiceOperationV1::from_tag(tag).ok_or_else(DurableCodecError::corrupt)
}

fn phase_from_proto(value: i32) -> Result<ServiceAuditPhaseV1, DurableCodecError> {
    let tag = u8::try_from(value).map_err(|_| DurableCodecError::corrupt())?;
    ServiceAuditPhaseV1::from_tag(tag).ok_or_else(DurableCodecError::corrupt)
}

fn ingress_from_proto(value: i32) -> Result<ServiceIngressKindV1, DurableCodecError> {
    let tag = u8::try_from(value).map_err(|_| DurableCodecError::corrupt())?;
    ServiceIngressKindV1::from_tag(tag).ok_or_else(DurableCodecError::corrupt)
}

fn target_to_proto(value: &ServiceAuditTargetV1) -> wire::ServiceAuditTargetV1 {
    use wire::service_audit_target_v1::Target;
    let target = match value {
        ServiceAuditTargetV1::ContractLineage(lineage) => {
            Target::ContractLineage(lineage.as_str().to_owned())
        }
        ServiceAuditTargetV1::ContractVersion { lineage, version } => {
            Target::ContractVersion(wire::ContractVersionAuditTargetV1 {
                contract_lineage: lineage.as_str().to_owned(),
                contract_version: version.get(),
            })
        }
        ServiceAuditTargetV1::EntityType {
            lineage,
            entity_type_id,
        } => Target::EntityType(wire::EntityTypeAuditTargetV1 {
            contract_lineage: lineage.as_str().to_owned(),
            entity_type_id: entity_type_id.get(),
        }),
        ServiceAuditTargetV1::Command {
            lineage,
            command_id,
        } => Target::Command(wire::CommandAuditTargetV1 {
            contract_lineage: lineage.as_str().to_owned(),
            command_id: command_id.get(),
        }),
        ServiceAuditTargetV1::Projection {
            lineage,
            projection_id,
        } => Target::Projection(wire::ProjectionAuditTargetV1 {
            contract_lineage: lineage.as_str().to_owned(),
            projection_id: projection_id.get(),
        }),
        ServiceAuditTargetV1::Index { lineage, index_id } => {
            Target::Index(wire::IndexAuditTargetV1 {
                contract_lineage: lineage.as_str().to_owned(),
                index_id: index_id.get(),
            })
        }
        ServiceAuditTargetV1::Commit(sequence) => Target::CommitSequence(sequence.get()),
        ServiceAuditTargetV1::Provenance(id) => Target::ProvenanceId(id.as_bytes().to_vec()),
        ServiceAuditTargetV1::Capability(id) => Target::CapabilityId(id.as_bytes().to_vec()),
    };
    wire::ServiceAuditTargetV1 {
        target: Some(target),
    }
}

fn lineage(value: String) -> Result<ContractLineage, DurableCodecError> {
    ContractLineage::new(value).map_err(|_| DurableCodecError::corrupt())
}

fn target_from_proto(
    value: wire::ServiceAuditTargetV1,
) -> Result<ServiceAuditTargetV1, DurableCodecError> {
    use wire::service_audit_target_v1::Target;
    Ok(match require(value.target)? {
        Target::ContractLineage(value) => ServiceAuditTargetV1::ContractLineage(lineage(value)?),
        Target::ContractVersion(value) => ServiceAuditTargetV1::ContractVersion {
            lineage: lineage(value.contract_lineage)?,
            version: ContractVersion::new(value.contract_version)
                .ok_or_else(DurableCodecError::corrupt)?,
        },
        Target::EntityType(value) => ServiceAuditTargetV1::EntityType {
            lineage: lineage(value.contract_lineage)?,
            entity_type_id: EntityTypeId::new(value.entity_type_id)
                .ok_or_else(DurableCodecError::corrupt)?,
        },
        Target::Command(value) => ServiceAuditTargetV1::Command {
            lineage: lineage(value.contract_lineage)?,
            command_id: CommandId::new(value.command_id).ok_or_else(DurableCodecError::corrupt)?,
        },
        Target::Projection(value) => ServiceAuditTargetV1::Projection {
            lineage: lineage(value.contract_lineage)?,
            projection_id: ProjectionId::new(value.projection_id)
                .ok_or_else(DurableCodecError::corrupt)?,
        },
        Target::Index(value) => ServiceAuditTargetV1::Index {
            lineage: lineage(value.contract_lineage)?,
            index_id: IndexId::new(value.index_id).ok_or_else(DurableCodecError::corrupt)?,
        },
        Target::CommitSequence(value) => ServiceAuditTargetV1::Commit(
            CommitSequence::new(value).ok_or_else(DurableCodecError::corrupt)?,
        ),
        Target::ProvenanceId(value) => ServiceAuditTargetV1::Provenance(
            ProvenanceId::from_bytes(fixed(value)?).map_err(|_| DurableCodecError::corrupt())?,
        ),
        Target::CapabilityId(value) => ServiceAuditTargetV1::Capability(
            CapabilityId::from_bytes(fixed(value)?).map_err(|_| DurableCodecError::corrupt())?,
        ),
    })
}

fn link_to_proto(value: ServiceAuditLinkV1) -> wire::ServiceAuditLinkV1 {
    use wire::service_audit_link_v1::Link;
    let link = match value {
        ServiceAuditLinkV1::None => Link::None(wire::UnitV1 {}),
        ServiceAuditLinkV1::Command {
            commit_sequence,
            provenance_id,
        } => Link::Command(wire::CommandServiceAuditLinkV1 {
            commit_sequence: commit_sequence.get(),
            provenance_id: provenance_id.as_bytes().to_vec(),
        }),
        ServiceAuditLinkV1::ControlPlane {
            administration_sequence,
        } => Link::ControlPlane(wire::ControlPlaneServiceAuditLinkV1 {
            administration_sequence: administration_sequence.get(),
        }),
    };
    wire::ServiceAuditLinkV1 { link: Some(link) }
}

fn link_from_proto(
    value: wire::ServiceAuditLinkV1,
) -> Result<ServiceAuditLinkV1, DurableCodecError> {
    use wire::service_audit_link_v1::Link;
    Ok(match require(value.link)? {
        Link::None(_) => ServiceAuditLinkV1::None,
        Link::Command(value) => ServiceAuditLinkV1::Command {
            commit_sequence: CommitSequence::new(value.commit_sequence)
                .ok_or_else(DurableCodecError::corrupt)?,
            provenance_id: ProvenanceId::from_bytes(fixed(value.provenance_id)?)
                .map_err(|_| DurableCodecError::corrupt())?,
        },
        Link::ControlPlane(value) => ServiceAuditLinkV1::ControlPlane {
            administration_sequence: AdministrationSequence::new(value.administration_sequence)
                .ok_or_else(DurableCodecError::corrupt)?,
        },
    })
}

/// Encodes one durable API-neutral service-audit record.
pub fn encode_service_audit_record_v1(
    value: &StoredServiceAuditRecordV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_message(
        AUDIT,
        &wire::ServiceAuditRecordV1 {
            administration_sequence: value.administration_sequence().get(),
            request_id: value.request_id().as_bytes().to_vec(),
            timestamp: Some(timestamp_to_proto(value.timestamp())),
            operation: i32::from(value.operation().tag()),
            phase: i32::from(value.phase().tag()),
            principal: value.principal().map(audit_principal_to_proto),
            ingress: i32::from(value.ingress().tag()),
            targets: value
                .targets()
                .as_slice()
                .iter()
                .map(target_to_proto)
                .collect(),
            approval_id: value.approval_id().map(|value| value.as_str().to_owned()),
            link: Some(link_to_proto(value.link())),
        },
    )
}

/// Decodes one durable API-neutral service-audit record.
pub fn decode_service_audit_record_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredServiceAuditRecordV1>, DurableCodecError> {
    decode_message::<wire::ServiceAuditRecordV1, _, _>(AUDIT, encoded, |value| {
        let raw_targets = value
            .targets
            .into_iter()
            .map(target_from_proto)
            .collect::<Result<Vec<_>, _>>()?;
        let targets = ServiceAuditTargetsV1::new(raw_targets.clone())
            .map_err(|_| DurableCodecError::corrupt())?;
        if targets.as_slice() != raw_targets {
            return Err(DurableCodecError::corrupt());
        }
        storage_result(StoredServiceAuditRecordV1::from_stored_parts(
            AdministrationSequence::new(value.administration_sequence)
                .ok_or_else(DurableCodecError::corrupt)?,
            RequestId::from_bytes(fixed(value.request_id)?)
                .map_err(|_| DurableCodecError::corrupt())?,
            timestamp_from_proto(require(value.timestamp)?)?,
            operation_from_proto(value.operation)?,
            phase_from_proto(value.phase)?,
            value
                .principal
                .map(audit_principal_from_proto)
                .transpose()?,
            ingress_from_proto(value.ingress)?,
            targets,
            value
                .approval_id
                .map(ApprovalId::new)
                .transpose()
                .map_err(|_| DurableCodecError::corrupt())?,
            link_from_proto(require(value.link)?)?,
        ))
    })
}
