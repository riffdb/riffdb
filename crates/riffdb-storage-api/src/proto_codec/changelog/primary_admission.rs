//! Bounded closed primary-fence records; deserialization grants no source authority.
use super::super::{
    audit_principal_from_proto, audit_principal_to_proto, timestamp_from_proto, timestamp_to_proto,
};
use super::*;
use crate::{
    ReplicationPrimaryAdmissionV1 as Admission, StoredPrimaryFenceAdministrationV1 as Fence,
};
use riffdb_types::{
    ApprovalId, ReplicationFenceOperationId, ReplicationFollowerAuditTargetV1, RequestId,
};
use wire::replication_primary_admission_v1::{Active, State};

const ADMISSION: &str = "riffdb.storage.v1.ReplicationPrimaryAdmissionV1";
const FENCE: &str = "riffdb.storage.v1.StoredPrimaryFenceAdministrationV1";

pub(super) fn fence_to_wire(value: &Fence) -> wire::StoredPrimaryFenceAdministrationV1 {
    let target = value.target();
    wire::StoredPrimaryFenceAdministrationV1 {
        administration_sequence: value.administration_sequence().get(),
        timestamp: Some(timestamp_to_proto(value.timestamp())),
        operation_id: value.operation_id().as_bytes().to_vec(),
        request_id: value.request_id().as_bytes().to_vec(),
        principal: Some(audit_principal_to_proto(value.principal())),
        approval_id: value.approval_id().map(|id| id.as_str().to_owned()),
        target: Some(wire::ReplicationFollowerAuditTargetV3 {
            database_id: target.database_id().as_bytes().to_vec(),
            history_incarnation: target.history_incarnation(),
            leadership_epoch: target.leadership_epoch().get(),
            hold_id: target.hold_id().as_bytes().to_vec(),
        }),
        registration_generation: value.generation().get(),
        observed: Some(encode_position(value.observed())),
    }
}
pub(super) fn fence_from_wire(
    value: wire::StoredPrimaryFenceAdministrationV1,
) -> Result<Fence, DurableCodecError> {
    let target = require(value.target)?;
    Fence::new(
        AdministrationSequence::new(value.administration_sequence)
            .ok_or_else(DurableCodecError::corrupt)?,
        timestamp_from_proto(require(value.timestamp)?)?,
        ReplicationFenceOperationId::from_bytes(fixed(value.operation_id)?)
            .map_err(|_| DurableCodecError::corrupt())?,
        RequestId::from_bytes(fixed(value.request_id)?)
            .map_err(|_| DurableCodecError::corrupt())?,
        audit_principal_from_proto(require(value.principal)?)?,
        value
            .approval_id
            .map(ApprovalId::new)
            .transpose()
            .map_err(|_| DurableCodecError::corrupt())?,
        ReplicationFollowerAuditTargetV1::new(
            DatabaseId::from_bytes(fixed(target.database_id)?)
                .map_err(|_| DurableCodecError::corrupt())?,
            target.history_incarnation,
            LeadershipEpochV1::new(target.leadership_epoch)
                .ok_or_else(DurableCodecError::corrupt)?,
            ReplicationSourceHoldIdV1::new(fixed(target.hold_id)?)
                .ok_or_else(DurableCodecError::corrupt)?,
        )
        .ok_or_else(DurableCodecError::corrupt)?,
        ChangelogTransactionSequence::new(value.registration_generation)
            .ok_or_else(DurableCodecError::corrupt)?,
        decode_position(require(value.observed)?)?,
    )
    .map_err(|_| DurableCodecError::corrupt())
}

/// Encodes complete immutable evidence under its distinct administration role.
pub fn encode_primary_fence_administration_v1(
    value: &Fence,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_message(FENCE, &fence_to_wire(value))
}
/// Refuses absent identities, unknown shapes and contradictory sequence facts.
pub fn decode_primary_fence_administration_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<Fence>, DurableCodecError> {
    decode_message::<wire::StoredPrimaryFenceAdministrationV1, _, _>(
        FENCE,
        encoded,
        fence_from_wire,
    )
}
/// Encodes exactly one explicit state; there is no missing-to-Active conversion.
pub fn encode_replication_primary_admission_v1(
    value: &Admission,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    let state = match value.fence() {
        Some(fence) => State::Fenced(Box::new(fence_to_wire(fence))),
        None => {
            let lineage = value.lineage();
            State::Active(Active {
                database_id: lineage.database_id().as_bytes().to_vec(),
                history_incarnation: lineage.history_incarnation(),
                leadership_epoch: lineage.leadership_epoch().get(),
            })
        }
    };
    encode_message(
        ADMISSION,
        &wire::ReplicationPrimaryAdmissionV1 { state: Some(state) },
    )
}
/// Reconstructs evidence only. Source startup must validate the exact catalog,
/// retained administration/V3 receipt, lineage and current application head.
pub fn decode_replication_primary_admission_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<Admission>, DurableCodecError> {
    decode_message::<wire::ReplicationPrimaryAdmissionV1, _, _>(ADMISSION, encoded, |value| {
        match require(value.state)? {
            State::Active(v) => Admission::active(
                ChangelogLineageV3::new_with_catalog(
                    DatabaseId::from_bytes(fixed(v.database_id)?)
                        .map_err(|_| DurableCodecError::corrupt())?,
                    v.history_incarnation,
                    LeadershipEpochV1::new(v.leadership_epoch)
                        .ok_or_else(DurableCodecError::corrupt)?,
                    AuthoritativeStateCatalogV2.digest(),
                )
                .map_err(|_| DurableCodecError::corrupt())?,
            )
            .map_err(|_| DurableCodecError::corrupt()),
            State::Fenced(v) => Ok(Admission::fenced(fence_from_wire(*v)?)),
        }
    })
}
