//! Distinct external replication_promotion_receipt/v1. Never a restore receipt
//! or peer proof. Existing embedded fence-record bytes retain their own codec.
use super::*;
use riffdb_storage_api::{
    AuditPrincipalV1, ChangelogHistoryPointV3 as Point, ChangelogLineageV3 as Lineage,
    ChangelogTransactionSequence as Sequence, MAX_REPLICATION_PROMOTION_STEPS_V1,
    PrimaryFenceSourceEvidenceV1 as Evidence, ReplicationPromotionFailureV1 as Failure,
    ReplicationPromotionPhaseV1 as Phase, ReplicationPromotionReceiptV1 as Receipt,
    ReplicationPromotionRequestV1 as Request, ReplicationPromotionSelectionV1 as Selection,
    ReplicationPromotionStepV1 as Step,
};
use riffdb_types::{
    AdministrationSequence, DualFrontier, LeadershipEpochV1, ReplicationFenceOperationId,
    ReplicationFollowerAuditTargetV1, ReplicationPromotionOperationId, ReplicationSourceHoldIdV1,
};
use std::num::NonZeroU64;

pub(in crate::maintenance) const MAX_PROMOTION_RECEIPT_BYTES: usize = 8 * 1024;
const MAGIC: &[u8] = b"RIFFDB-PROMOTION-RECEIPT\0";
const VERSION: u32 = 1;

pub(in crate::maintenance) fn encode_promotion_receipt(
    receipt: &Receipt,
) -> Result<Vec<u8>, StorageError> {
    let mut out = Encoder::new_with_limit(MAX_PROMOTION_RECEIPT_BYTES - SHA256_BYTES);
    out.bytes(MAGIC)?;
    out.u32(VERSION)?;
    let request = receipt.request();
    out.bytes(request.operation_id().as_bytes())?;
    out.bytes(request.fence_operation_id().as_bytes())?;
    let target = request.target();
    out.bytes(target.database_id().as_bytes())?;
    out.u64(target.history_incarnation())?;
    out.u64(target.leadership_epoch().get())?;
    out.bytes(target.hold_id().as_bytes())?;
    out.u64(request.generation().get())?;
    out.bytes(receipt.request_id().as_bytes())?;
    let principal = receipt.principal();
    out.u8(principal.actor_kind().tag())?;
    out.framed_u16(principal.principal_id().as_str().as_bytes())?;
    out.bytes(principal.capability_id().as_bytes())?;
    out.u64(principal.capability_revision().get())?;
    match receipt.approval_id() {
        None => out.u8(0)?,
        Some(approval) => {
            out.u8(1)?;
            out.framed_u16(approval.as_bytes())?;
        }
    }
    out.i64(receipt.timestamp().seconds())?;
    out.u32(receipt.timestamp().nanoseconds())?;
    match receipt.selection() {
        None => out.u8(0)?,
        Some(selection) => {
            out.u8(1)?;
            let evidence = selection.evidence();
            out.framed_u16(&evidence.encode_fence_record().map_err(value_error)?)?;
            for point in [
                evidence.applied(),
                evidence.source_history().anchor(),
                evidence.source_history().tail(),
                evidence.source_history().minimum_resume(),
            ] {
                encode_point(&mut out, point)?;
            }
            let lineage = selection.published_lineage();
            out.bytes(lineage.database_id().as_bytes())?;
            out.u64(lineage.history_incarnation())?;
            out.u64(lineage.leadership_epoch().get())?;
            out.bytes(&lineage.catalog_digest())?;
            out.u64(selection.application_rpo())?;
        }
    }
    out.u8(u8::try_from(receipt.steps().len()).map_err(|_| limit_exceeded())?)?;
    for step in receipt.steps() {
        let (kind, value) = match *step {
            Step::Phase(phase) => (1, phase_tag(phase)),
            Step::Denied(failure) => (2, failure_tag(failure)),
            Step::FailedClosed(failure) => (3, failure_tag(failure)),
            Step::Uncertain(failure) => (4, failure_tag(failure)),
        };
        out.u8(kind)?;
        out.u8(value)?;
    }
    let mut encoded = out.finish();
    let checksum = Sha256::digest(&encoded);
    encoded.extend_from_slice(&checksum);
    Ok(encoded)
}

pub(in crate::maintenance) fn decode_promotion_receipt(
    encoded: &[u8],
) -> Result<Receipt, StorageError> {
    if encoded.len() > MAX_PROMOTION_RECEIPT_BYTES {
        return Err(limit_exceeded());
    }
    let end = encoded
        .len()
        .checked_sub(SHA256_BYTES)
        .ok_or_else(corrupt)?;
    let (body, checksum) = encoded.split_at(end);
    if Sha256::digest(body).as_slice() != checksum {
        return Err(corrupt());
    }
    let mut input = Decoder::new(body);
    if input.bytes(MAGIC.len())? != MAGIC || input.u32()? != VERSION {
        return Err(incompatible());
    }
    let operation =
        ReplicationPromotionOperationId::from_bytes(input.array()?).map_err(|_| corrupt())?;
    let fence = ReplicationFenceOperationId::from_bytes(input.array()?).map_err(|_| corrupt())?;
    let database = DatabaseId::from_bytes(input.array()?).map_err(|_| corrupt())?;
    let incarnation = input.u64()?;
    let epoch = LeadershipEpochV1::new(input.u64()?).ok_or_else(corrupt)?;
    let hold = ReplicationSourceHoldIdV1::new(input.array()?).ok_or_else(corrupt)?;
    let target = ReplicationFollowerAuditTargetV1::new(database, incarnation, epoch, hold)
        .ok_or_else(corrupt)?;
    let request = Request::new(operation, fence, target, sequence(&mut input)?);
    let request_id = RequestId::from_bytes(input.array()?).map_err(|_| corrupt())?;
    let actor_kind = ActorKind::from_tag(input.u8()?).ok_or_else(corrupt)?;
    let actor = ActorId::new(input.text_u16(MAX_ACTOR_ID_BYTES)?).map_err(|_| corrupt())?;
    let capability = CapabilityId::from_bytes(input.array()?).map_err(|_| corrupt())?;
    let revision = NonZeroU64::new(input.u64()?).ok_or_else(corrupt)?;
    let principal = AuditPrincipalV1::new(actor, actor_kind, capability, revision);
    let approval = match input.u8()? {
        0 => None,
        1 => Some(ApprovalId::new(input.text_u16(MAX_APPROVAL_ID_BYTES)?).map_err(|_| corrupt())?),
        _ => return Err(corrupt()),
    };
    let timestamp = Timestamp::new(input.i64()?, input.u32()?).map_err(|_| corrupt())?;
    let selection = match input.u8()? {
        0 => None,
        1 => {
            let fence_bytes = input.framed_u16(MAX_PROMOTION_RECEIPT_BYTES)?;
            let evidence = Evidence::from_fence_record(
                fence_bytes,
                decode_point(&mut input)?,
                decode_point(&mut input)?,
                decode_point(&mut input)?,
                decode_point(&mut input)?,
            )
            .map_err(value_error)?;
            let lineage = Lineage::new_with_catalog(
                DatabaseId::from_bytes(input.array()?).map_err(|_| corrupt())?,
                input.u64()?,
                LeadershipEpochV1::new(input.u64()?).ok_or_else(corrupt)?,
                input.array()?,
            )
            .map_err(|_| corrupt())?;
            Some(
                Selection::from_canonical_parts(request, evidence, lineage, input.u64()?)
                    .map_err(value_error)?,
            )
        }
        _ => return Err(corrupt()),
    };
    let count = usize::from(input.u8()?);
    if count == 0 || count > MAX_REPLICATION_PROMOTION_STEPS_V1 {
        return Err(corrupt());
    }
    let mut steps = Vec::with_capacity(count);
    for _ in 0..count {
        let kind = input.u8()?;
        let value = input.u8()?;
        steps.push(match kind {
            1 => Step::Phase(decode_phase(value)?),
            2 => Step::Denied(decode_failure(value)?),
            3 => Step::FailedClosed(decode_failure(value)?),
            4 => Step::Uncertain(decode_failure(value)?),
            _ => return Err(corrupt()),
        });
    }
    input.finish()?;
    let receipt = Receipt::from_canonical_parts(
        request, request_id, principal, approval, timestamp, selection, steps,
    )
    .map_err(value_error)?;
    if encode_promotion_receipt(&receipt)? != encoded {
        return Err(corrupt());
    }
    Ok(receipt)
}

fn encode_point(out: &mut Encoder, point: Point) -> Result<(), StorageError> {
    out.u64(point.sequence().get())?;
    out.bytes(&point.history_hash())?;
    out.u64(point.frontier().application().map_or(0, |v| v.get()))?;
    out.u64(point.frontier().administration().map_or(0, |v| v.get()))
}
fn decode_point(input: &mut Decoder<'_>) -> Result<Point, StorageError> {
    Ok(Point::new(
        sequence(input)?,
        input.array()?,
        DualFrontier::new(
            CommitSequence::new(input.u64()?),
            AdministrationSequence::new(input.u64()?),
        ),
    ))
}
fn sequence(input: &mut Decoder<'_>) -> Result<Sequence, StorageError> {
    Sequence::new(input.u64()?).ok_or_else(corrupt)
}

fn phase_tag(phase: Phase) -> u8 {
    match phase {
        Phase::Attempted => 1,
        Phase::Draining => 2,
        Phase::Offline => 3,
        Phase::Selected => 4,
        Phase::CutoverPending => 5,
        Phase::CutoverCommitted => 6,
        Phase::Validated => 7,
        Phase::Succeeded => 8,
    }
}
fn decode_phase(tag: u8) -> Result<Phase, StorageError> {
    match tag {
        1 => Ok(Phase::Attempted),
        2 => Ok(Phase::Draining),
        3 => Ok(Phase::Offline),
        4 => Ok(Phase::Selected),
        5 => Ok(Phase::CutoverPending),
        6 => Ok(Phase::CutoverCommitted),
        7 => Ok(Phase::Validated),
        8 => Ok(Phase::Succeeded),
        _ => Err(corrupt()),
    }
}
fn failure_tag(failure: Failure) -> u8 {
    match failure {
        Failure::AuthorizationDenied => 1,
        Failure::SelectionConflict => 2,
        Failure::FenceUnavailable => 3,
        Failure::FenceInvalid => 4,
        Failure::DrainFailed => 5,
        Failure::CounterExhausted => 6,
        Failure::StorageUnavailable => 7,
        Failure::ValidationFailed => 8,
    }
}
fn decode_failure(tag: u8) -> Result<Failure, StorageError> {
    match tag {
        1 => Ok(Failure::AuthorizationDenied),
        2 => Ok(Failure::SelectionConflict),
        3 => Ok(Failure::FenceUnavailable),
        4 => Ok(Failure::FenceInvalid),
        5 => Ok(Failure::DrainFailed),
        6 => Ok(Failure::CounterExhausted),
        7 => Ok(Failure::StorageUnavailable),
        8 => Ok(Failure::ValidationFailed),
        _ => Err(corrupt()),
    }
}
