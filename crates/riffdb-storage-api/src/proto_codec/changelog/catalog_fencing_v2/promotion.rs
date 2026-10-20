//! Catalog-bound promotion persistence through the existing semantic owner.
use super::{
    AdministrationSequence, ApprovalId, CanonicalStoredEnvelopeV1, ChangelogLineageV3,
    DurableCodecError, EncodedPageItem, Failure, LeadershipEpochV1,
    MAX_REPLICATION_PROMOTION_STEPS_V1, PROMOTION_V2, Phase, PrimaryFenceSourceEvidenceV1,
    Promotion, Receipt, ReplicationPromotionOperationId, Request, RequestId, Selection,
    ServiceIngressKindV1, Step, audit_principal_from_proto, audit_principal_to_proto,
    catalog_fencing_require_v3, catalog_fencing_v2_from_wire, catalog_fencing_v2_to_wire,
    catalog_fencing_v3_digest, decode_message, decode_position, encode_message, encode_position,
    fixed, history_from_wire, history_to_wire, require, timestamp_from_proto, timestamp_to_proto,
    wire,
};

/// Encodes a complete catalog-V3 cutover, preserving all prior semantic checks.
pub fn encode_promotion_administration_v2(
    value: &Promotion,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    if value.inheritance_sequence().is_none() {
        return Err(DurableCodecError::corrupt());
    }
    let attempt = value.attempt();
    let selection = attempt.selection().ok_or_else(DurableCodecError::corrupt)?;
    let published = selection.published_lineage();
    catalog_fencing_require_v3(published)?;
    catalog_fencing_require_v3(selection.evidence().source_history().lineage())?;
    catalog_fencing_require_v3(selection.evidence().fence().lineage())?;
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
        PROMOTION_V2,
        &wire::StoredPromotionAdministrationV2 {
            administration_sequence: value.administration_sequence().get(),
            timestamp: Some(timestamp_to_proto(value.timestamp())),
            promotion_operation_id: attempt.request().operation_id().as_bytes().to_vec(),
            request_id: attempt.request_id().as_bytes().to_vec(),
            principal: Some(audit_principal_to_proto(attempt.principal())),
            approval_id: attempt.approval_id().map(|id| id.as_str().to_owned()),
            attempted_at: Some(timestamp_to_proto(attempt.timestamp())),
            fence: Some(catalog_fencing_v2_to_wire(selection.evidence().fence())),
            applied: Some(encode_position(selection.applied())),
            source_history: Some(history_to_wire(selection.evidence().source_history())),
            published_incarnation: published.history_incarnation(),
            published_epoch: published.leadership_epoch().get(),
            application_rpo: selection.application_rpo(),
            attempt_steps: steps,
            ingress: i32::from(value.ingress().tag()),
            published_catalog_digest: published.catalog_digest().to_vec(),
        },
    )
}
/// Revalidates exact catalog/lineage/RPO/phases and checked successor allocation.
pub fn decode_promotion_administration_v2(
    encoded: &[u8],
) -> Result<EncodedPageItem<Promotion>, DurableCodecError> {
    decode_message::<wire::StoredPromotionAdministrationV2, _, _>(PROMOTION_V2, encoded, |value| {
        let published_catalog = catalog_fencing_v3_digest(value.published_catalog_digest)?;
        if value.attempt_steps.len() > MAX_REPLICATION_PROMOTION_STEPS_V1 {
            return Err(DurableCodecError::corrupt());
        }
        let steps = value
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
        let fence = catalog_fencing_v2_from_wire(require(value.fence)?)?;
        let request = Request::new(
            ReplicationPromotionOperationId::from_bytes(fixed(value.promotion_operation_id)?)
                .map_err(|_| DurableCodecError::corrupt())?,
            fence.operation_id(),
            fence.target(),
            fence.generation(),
        );
        let evidence = PrimaryFenceSourceEvidenceV1::new(
            fence,
            decode_position(require(value.applied)?)?,
            history_from_wire(require(value.source_history)?)?,
        )
        .map_err(|_| DurableCodecError::corrupt())?;
        let source = evidence.source_history().lineage();
        catalog_fencing_require_v3(source)?;
        if published_catalog != source.catalog_digest() {
            return Err(DurableCodecError::corrupt());
        }
        let published = ChangelogLineageV3::new_with_catalog(
            source.database_id(),
            value.published_incarnation,
            LeadershipEpochV1::new(value.published_epoch).ok_or_else(DurableCodecError::corrupt)?,
            published_catalog,
        )
        .map_err(|_| DurableCodecError::corrupt())?;
        let selection =
            Selection::from_canonical_parts(request, evidence, published, value.application_rpo)
                .map_err(|_| DurableCodecError::corrupt())?;
        let attempt = Receipt::from_canonical_parts(
            request,
            RequestId::from_bytes(fixed(value.request_id)?)
                .map_err(|_| DurableCodecError::corrupt())?,
            audit_principal_from_proto(require(value.principal)?)?,
            value
                .approval_id
                .map(ApprovalId::new)
                .transpose()
                .map_err(|_| DurableCodecError::corrupt())?,
            timestamp_from_proto(require(value.attempted_at)?)?,
            Some(selection),
            steps,
        )
        .map_err(|_| DurableCodecError::corrupt())?;
        let ingress = u8::try_from(value.ingress)
            .ok()
            .and_then(ServiceIngressKindV1::from_tag)
            .ok_or_else(DurableCodecError::corrupt)?;
        Promotion::from_catalog_bound_canonical_parts(
            AdministrationSequence::new(value.administration_sequence)
                .ok_or_else(DurableCodecError::corrupt)?,
            attempt,
            timestamp_from_proto(require(value.timestamp)?)?,
            ingress,
        )
        .map_err(|_| DurableCodecError::corrupt())
    })
}
