//! Successor follower-target audit codec; V1/V2 keep their frozen identities.

use super::audit::{
    ingress_from_proto, link_from_proto, link_to_proto, operation_from_proto, phase_from_proto,
    target_from_proto_v2, target_to_proto_v2,
};
use super::{
    CanonicalStoredEnvelopeV1, DurableCodecError, audit_principal_from_proto,
    audit_principal_to_proto, decode_message, encode_message, fixed, require, storage_result,
    timestamp_from_proto, timestamp_to_proto,
};
use crate::{EncodedPageItem, StoredServiceAuditRecordV1};
use riffdb_proto::storage::v1 as wire;
use riffdb_types::{
    AdministrationSequence, ApprovalId, DatabaseId, LeadershipEpochV1,
    ReplicationFollowerAuditTargetV1, ReplicationSourceHoldIdV1, RequestId, ServiceAuditTargetV1,
    ServiceAuditTargetsV1,
};

pub(super) const AUDIT_V3: &str = "riffdb.storage.v1.ServiceAuditRecordV3";

fn target_to_proto_v3(
    value: &ServiceAuditTargetV1,
) -> Result<wire::ServiceAuditTargetV3, DurableCodecError> {
    use wire::service_audit_target_v2::Target as Old;
    use wire::service_audit_target_v3::Target as New;
    let target = if let ServiceAuditTargetV1::ReplicationFollower(value) = value {
        New::ReplicationFollower(wire::ReplicationFollowerAuditTargetV3 {
            database_id: value.database_id().as_bytes().to_vec(),
            history_incarnation: value.history_incarnation(),
            leadership_epoch: value.leadership_epoch().get(),
            hold_id: value.hold_id().as_bytes().to_vec(),
        })
    } else {
        match require(target_to_proto_v2(value)?.target)? {
            Old::ContractLineage(value) => New::ContractLineage(value),
            Old::ContractVersion(value) => New::ContractVersion(value),
            Old::EntityType(value) => New::EntityType(value),
            Old::Command(value) => New::Command(value),
            Old::Projection(value) => New::Projection(value),
            Old::Index(value) => New::Index(value),
            Old::CommitSequence(value) => New::CommitSequence(value),
            Old::ProvenanceId(value) => New::ProvenanceId(value),
            Old::CapabilityId(value) => New::CapabilityId(value),
            Old::EventConsumer(value) => New::EventConsumer(value),
        }
    };
    Ok(wire::ServiceAuditTargetV3 {
        target: Some(target),
    })
}

fn target_from_proto_v3(
    value: wire::ServiceAuditTargetV3,
) -> Result<ServiceAuditTargetV1, DurableCodecError> {
    use wire::service_audit_target_v2::Target as Old;
    use wire::service_audit_target_v3::Target as New;
    let target = match require(value.target)? {
        New::ReplicationFollower(value) => {
            let database_id = DatabaseId::from_bytes(fixed(value.database_id)?)
                .map_err(|_| DurableCodecError::corrupt())?;
            let epoch = LeadershipEpochV1::new(value.leadership_epoch)
                .ok_or_else(DurableCodecError::corrupt)?;
            let hold = ReplicationSourceHoldIdV1::new(fixed(value.hold_id)?)
                .ok_or_else(DurableCodecError::corrupt)?;
            return ReplicationFollowerAuditTargetV1::new(
                database_id,
                value.history_incarnation,
                epoch,
                hold,
            )
            .map(ServiceAuditTargetV1::ReplicationFollower)
            .ok_or_else(DurableCodecError::corrupt);
        }
        New::ContractLineage(value) => Old::ContractLineage(value),
        New::ContractVersion(value) => Old::ContractVersion(value),
        New::EntityType(value) => Old::EntityType(value),
        New::Command(value) => Old::Command(value),
        New::Projection(value) => Old::Projection(value),
        New::Index(value) => Old::Index(value),
        New::CommitSequence(value) => Old::CommitSequence(value),
        New::ProvenanceId(value) => Old::ProvenanceId(value),
        New::CapabilityId(value) => Old::CapabilityId(value),
        New::EventConsumer(value) => Old::EventConsumer(value),
    };
    target_from_proto_v2(wire::ServiceAuditTargetV2 {
        target: Some(target),
    })
}

/// Encodes one V3 durable API-neutral service-audit record.
pub fn encode_service_audit_record_v3(
    value: &StoredServiceAuditRecordV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    if !value
        .targets()
        .as_slice()
        .iter()
        .any(|target| matches!(target, ServiceAuditTargetV1::ReplicationFollower(_)))
    {
        return Err(DurableCodecError::invariant());
    }
    encode_message(
        AUDIT_V3,
        &wire::ServiceAuditRecordV3 {
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
                .map(target_to_proto_v3)
                .collect::<Result<Vec<_>, _>>()?,
            approval_id: value.approval_id().map(|value| value.as_str().to_owned()),
            link: Some(link_to_proto(value.link())),
        },
    )
}

/// Decodes one V3 durable API-neutral service-audit record.
pub fn decode_service_audit_record_v3(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredServiceAuditRecordV1>, DurableCodecError> {
    decode_message::<wire::ServiceAuditRecordV3, _, _>(AUDIT_V3, encoded, |value| {
        let raw_targets = value
            .targets
            .into_iter()
            .map(target_from_proto_v3)
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
