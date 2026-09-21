//! Bounded promotion control codec, distinct from the external attempt envelope.
use super::super::{
    audit_principal_from_proto, audit_principal_to_proto, timestamp_from_proto, timestamp_to_proto,
};
use super::primary_admission::{fence_from_wire, fence_to_wire};
use super::*;
use crate::{
    MAX_REPLICATION_PROMOTION_STEPS_V1, PrimaryFenceSourceEvidenceV1,
    ReplicationPromotionFailureV1 as Failure, ReplicationPromotionPhaseV1 as Phase,
    ReplicationPromotionReceiptV1 as Receipt, ReplicationPromotionRequestV1 as Request,
    ReplicationPromotionSelectionV1 as Selection, ReplicationPromotionStepV1 as Step,
    StoredPromotionAdministrationV1 as Administration,
};
use riffdb_types::{ApprovalId, ReplicationPromotionOperationId, RequestId, ServiceIngressKindV1};

const RECORD: &str = "riffdb.storage.v1.StoredPromotionAdministrationV1";

/// Encodes the complete pending attempt and exact cutover result allocation.
/// The constructor already refused incomplete evidence, MCP and counter exhaustion.
pub fn encode_promotion_administration_v1(
    value: &Administration,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    let attempt = value.attempt();
    let selection = attempt.selection().ok_or_else(DurableCodecError::corrupt)?;
    let published = selection.published_lineage();
    let steps = attempt
        .steps()
        .iter()
        .map(|step| match step {
            Step::Phase(Phase::Attempted) => Ok(1),
            Step::Phase(Phase::Draining) => Ok(2),
            Step::Phase(Phase::Offline) => Ok(3),
            Step::Phase(Phase::Selected) => Ok(4),
            Step::Phase(Phase::CutoverPending) => Ok(5),
            Step::Uncertain(Failure::StorageUnavailable) => Ok(129),
            Step::Uncertain(Failure::ValidationFailed) => Ok(130),
            _ => Err(DurableCodecError::corrupt()),
        })
        .collect::<Result<Vec<_>, _>>()?;
    encode_message(
        RECORD,
        &wire::StoredPromotionAdministrationV1 {
            administration_sequence: value.administration_sequence().get(),
            timestamp: Some(timestamp_to_proto(value.timestamp())),
            promotion_operation_id: attempt.request().operation_id().as_bytes().to_vec(),
            request_id: attempt.request_id().as_bytes().to_vec(),
            principal: Some(audit_principal_to_proto(attempt.principal())),
            approval_id: attempt.approval_id().map(|id| id.as_str().to_owned()),
            attempted_at: Some(timestamp_to_proto(attempt.timestamp())),
            fence: Some(fence_to_wire(selection.evidence().fence())),
            applied: Some(encode_position(selection.applied())),
            source_history: Some(history_to_wire(selection.evidence().source_history())),
            published_incarnation: published.history_incarnation(),
            published_epoch: published.leadership_epoch().get(),
            application_rpo: selection.application_rpo(),
            attempt_steps: steps,
            ingress: i32::from(value.ingress().tag()),
        },
    )
}

/// Revalidates every nested identity, frontier, phase and derived result. This
/// returns metadata only; source startup still needs the exact atomic cutover
/// and its retained external receipt before granting readiness.
pub fn decode_promotion_administration_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<Administration>, DurableCodecError> {
    decode_message::<wire::StoredPromotionAdministrationV1, _, _>(RECORD, encoded, |v| {
        if v.attempt_steps.len() > MAX_REPLICATION_PROMOTION_STEPS_V1 {
            return Err(DurableCodecError::corrupt());
        }
        let steps = v
            .attempt_steps
            .into_iter()
            .map(|tag| match tag {
                1 => Ok(Step::Phase(Phase::Attempted)),
                2 => Ok(Step::Phase(Phase::Draining)),
                3 => Ok(Step::Phase(Phase::Offline)),
                4 => Ok(Step::Phase(Phase::Selected)),
                5 => Ok(Step::Phase(Phase::CutoverPending)),
                129 => Ok(Step::Uncertain(Failure::StorageUnavailable)),
                130 => Ok(Step::Uncertain(Failure::ValidationFailed)),
                _ => Err(DurableCodecError::corrupt()),
            })
            .collect::<Result<Vec<_>, _>>()?;
        let fence = fence_from_wire(require(v.fence)?)?;
        let request = Request::new(
            ReplicationPromotionOperationId::from_bytes(fixed(v.promotion_operation_id)?)
                .map_err(|_| DurableCodecError::corrupt())?,
            fence.operation_id(),
            fence.target(),
            fence.generation(),
        );
        let evidence = PrimaryFenceSourceEvidenceV1::new(
            fence,
            decode_position(require(v.applied)?)?,
            history_from_wire(require(v.source_history)?)?,
        )
        .map_err(|_| DurableCodecError::corrupt())?;
        let source = evidence.source_history().lineage();
        let published = ChangelogLineageV3::new_with_catalog(
            source.database_id(),
            v.published_incarnation,
            LeadershipEpochV1::new(v.published_epoch).ok_or_else(DurableCodecError::corrupt)?,
            source.catalog_digest(),
        )
        .map_err(|_| DurableCodecError::corrupt())?;
        let selection =
            Selection::from_canonical_parts(request, evidence, published, v.application_rpo)
                .map_err(|_| DurableCodecError::corrupt())?;
        let attempt = Receipt::from_canonical_parts(
            request,
            RequestId::from_bytes(fixed(v.request_id)?)
                .map_err(|_| DurableCodecError::corrupt())?,
            audit_principal_from_proto(require(v.principal)?)?,
            v.approval_id
                .map(ApprovalId::new)
                .transpose()
                .map_err(|_| DurableCodecError::corrupt())?,
            timestamp_from_proto(require(v.attempted_at)?)?,
            Some(selection),
            steps,
        )
        .map_err(|_| DurableCodecError::corrupt())?;
        let ingress = u8::try_from(v.ingress)
            .ok()
            .and_then(ServiceIngressKindV1::from_tag)
            .ok_or_else(DurableCodecError::corrupt)?;
        Administration::from_canonical_parts(
            AdministrationSequence::new(v.administration_sequence)
                .ok_or_else(DurableCodecError::corrupt)?,
            attempt,
            timestamp_from_proto(require(v.timestamp)?)?,
            ingress,
        )
        .map_err(|_| DurableCodecError::corrupt())
    })
}
