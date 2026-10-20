//! Explicit catalog-bound successors. Version-one decoders keep fixed V2 meaning.
use riffdb_proto::storage::v1 as wire;
use riffdb_types::{
    AdministrationSequence, ApprovalId, DatabaseId, LeadershipEpochV1, ReplicationFenceOperationId,
    ReplicationFollowerAuditTargetV1, ReplicationPromotionOperationId, RequestId,
    ServiceIngressKindV1,
};

use super::super::{
    CanonicalStoredEnvelopeV1, DurableCodecError, DurableCodecErrorKind,
    audit_principal_from_proto, audit_principal_to_proto, decode_message, encode_message, fixed,
    require, timestamp_from_proto, timestamp_to_proto,
};
use super::{decode_position, encode_position, history_from_wire, history_to_wire};
use crate::{
    AuthoritativeStateCatalogV2, AuthoritativeStateCatalogV3, ChangelogLineageV3,
    ChangelogTransactionSequence, EncodedPageItem, MAX_REPLICATION_PROMOTION_STEPS_V1,
    PrimaryFenceSourceEvidenceV1, ReplicationPrimaryAdmissionV1 as Admission,
    ReplicationPromotionFailureV1 as Failure, ReplicationPromotionPhaseV1 as Phase,
    ReplicationPromotionReceiptV1 as Receipt, ReplicationPromotionRequestV1 as Request,
    ReplicationPromotionSelectionV1 as Selection, ReplicationPromotionStepV1 as Step,
    ReplicationSourceHoldIdV1, StoredPrimaryFenceAdministrationV1 as Fence,
    StoredPromotionAdministrationV1 as Promotion,
};

const ADMISSION_V2: &str = "riffdb.storage.v1.ReplicationPrimaryAdmissionV2";
const FENCE_V2: &str = "riffdb.storage.v1.StoredPrimaryFenceAdministrationV2";
const PROMOTION_V2: &str = "riffdb.storage.v1.StoredPromotionAdministrationV2";

fn catalog_fencing_v3_digest(bytes: Vec<u8>) -> Result<[u8; 32], DurableCodecError> {
    let digest: [u8; 32] = fixed(bytes)?;
    if digest != AuthoritativeStateCatalogV3.digest() {
        return Err(DurableCodecError::new(
            DurableCodecErrorKind::IncompatibleFormat,
        ));
    }
    Ok(digest)
}
fn catalog_fencing_require_v3(lineage: ChangelogLineageV3) -> Result<(), DurableCodecError> {
    if lineage.catalog_digest() != AuthoritativeStateCatalogV3.digest() {
        return Err(DurableCodecError::new(
            DurableCodecErrorKind::IncompatibleFormat,
        ));
    }
    Ok(())
}
fn catalog_fencing_v2_to_wire(value: &Fence) -> wire::StoredPrimaryFenceAdministrationV2 {
    let target = value.target();
    wire::StoredPrimaryFenceAdministrationV2 {
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
        catalog_digest: value.lineage().catalog_digest().to_vec(),
    }
}
fn catalog_fencing_v2_from_wire(
    value: wire::StoredPrimaryFenceAdministrationV2,
) -> Result<Fence, DurableCodecError> {
    let digest = catalog_fencing_v3_digest(value.catalog_digest)?;
    let target = require(value.target)?;
    let target = ReplicationFollowerAuditTargetV1::new(
        DatabaseId::from_bytes(fixed(target.database_id)?)
            .map_err(|_| DurableCodecError::corrupt())?,
        target.history_incarnation,
        LeadershipEpochV1::new(target.leadership_epoch).ok_or_else(DurableCodecError::corrupt)?,
        ReplicationSourceHoldIdV1::new(fixed(target.hold_id)?)
            .ok_or_else(DurableCodecError::corrupt)?,
    )
    .ok_or_else(DurableCodecError::corrupt)?;
    let lineage = ChangelogLineageV3::new_with_catalog(
        target.database_id(),
        target.history_incarnation(),
        target.leadership_epoch(),
        digest,
    )
    .map_err(|_| DurableCodecError::corrupt())?;
    Fence::new_catalog_bound(
        lineage,
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
        target,
        ChangelogTransactionSequence::new(value.registration_generation)
            .ok_or_else(DurableCodecError::corrupt)?,
        decode_position(require(value.observed)?)?,
    )
    .map_err(|_| DurableCodecError::corrupt())
}

/// Encodes only explicit catalog-V3 fence evidence under family77/revision2.
pub fn encode_primary_fence_administration_v2(
    value: &Fence,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    catalog_fencing_require_v3(value.lineage())?;
    encode_message(FENCE_V2, &catalog_fencing_v2_to_wire(value))
}
/// Strictly reconstructs explicit catalog-V3 fence evidence, never authority.
pub fn decode_primary_fence_administration_v2(
    encoded: &[u8],
) -> Result<EncodedPageItem<Fence>, DurableCodecError> {
    decode_message::<wire::StoredPrimaryFenceAdministrationV2, _, _>(
        FENCE_V2,
        encoded,
        catalog_fencing_v2_from_wire,
    )
}
/// Encodes explicit catalog-V3 Active or complete Fenced evidence.
pub fn encode_replication_primary_admission_v2(
    value: &Admission,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    catalog_fencing_require_v3(value.lineage())?;
    use wire::replication_primary_admission_v2::{Active, State};
    let state = match value.fence() {
        Some(fence) => State::Fenced(Box::new(catalog_fencing_v2_to_wire(fence))),
        None => {
            let lineage = value.lineage();
            State::Active(Active {
                database_id: lineage.database_id().as_bytes().to_vec(),
                history_incarnation: lineage.history_incarnation(),
                leadership_epoch: lineage.leadership_epoch().get(),
                catalog_digest: lineage.catalog_digest().to_vec(),
            })
        }
    };
    encode_message(
        ADMISSION_V2,
        &wire::ReplicationPrimaryAdmissionV2 { state: Some(state) },
    )
}
/// Refuses missing/foreign catalog, mixed versions and contradictory identities.
pub fn decode_replication_primary_admission_v2(
    encoded: &[u8],
) -> Result<EncodedPageItem<Admission>, DurableCodecError> {
    use wire::replication_primary_admission_v2::State;
    decode_message::<wire::ReplicationPrimaryAdmissionV2, _, _>(ADMISSION_V2, encoded, |value| {
        match require(value.state)? {
            State::Active(value) => {
                let digest = catalog_fencing_v3_digest(value.catalog_digest)?;
                let lineage = ChangelogLineageV3::new_with_catalog(
                    DatabaseId::from_bytes(fixed(value.database_id)?)
                        .map_err(|_| DurableCodecError::corrupt())?,
                    value.history_incarnation,
                    LeadershipEpochV1::new(value.leadership_epoch)
                        .ok_or_else(DurableCodecError::corrupt)?,
                    digest,
                )
                .map_err(|_| DurableCodecError::corrupt())?;
                Admission::active_catalog_bound(lineage).map_err(|_| DurableCodecError::corrupt())
            }
            State::Fenced(value) => Ok(Admission::fenced(catalog_fencing_v2_from_wire(*value)?)),
        }
    })
}
#[path = "catalog_fencing_v2/promotion.rs"]
mod promotion;
pub use promotion::{decode_promotion_administration_v2, encode_promotion_administration_v2};
#[path = "catalog_fencing_v2/dispatch.rs"]
mod dispatch;
pub use dispatch::*;

#[cfg(test)]
#[path = "catalog_fencing_v2/tests.rs"]
mod tests;
