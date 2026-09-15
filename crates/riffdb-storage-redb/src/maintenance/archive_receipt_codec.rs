//! Bounded V3 external receipt codec. V1/V2 encoders and bytes are independent.
use super::*;
use riffdb_storage_api::{
    ARCHIVE_MANIFEST_V1_BYTES, ArchiveManifestV1, ArchiveRestoreSelectionV3,
    ArchiveRestoreSuffixV3, AuthoritativeStateCatalogV1, ChangelogHistoryPointV3,
    ChangelogLineageV3, ChangelogTransactionSequence, LeadershipEpochV1,
    OFFLINE_MAINTENANCE_RECEIPT_VERSION_V3, OfflineMaintenanceReceiptV3,
};
use riffdb_types::{ArchiveNameV1, ArchiveRestoreStopV1, DualFrontier, MAX_ARCHIVE_NAME_V1_BYTES};

pub(in crate::maintenance) fn encode_archive_receipt(
    receipt: &OfflineMaintenanceReceiptV3,
) -> Result<Vec<u8>, StorageError> {
    let mut out = Encoder::new();
    out.bytes(RECEIPT_MAGIC)?;
    out.u32(OFFLINE_MAINTENANCE_RECEIPT_VERSION_V3)?;
    out.bytes(receipt.operation_id().as_bytes())?;
    out.framed_u16(receipt.backup_name().as_bytes())?;
    out.framed_u16(receipt.archive_name().as_bytes())?;
    match receipt.stop() {
        ArchiveRestoreStopV1::LastArchived => out.u8(0)?,
        ArchiveRestoreStopV1::AtApplicationSequence(s) => {
            out.u8(1)?;
            out.u64(s.get())?;
        }
    }
    out.bytes(receipt.input_hash().as_bytes())?;
    out.u8(confirmation_tag(receipt.replacement_confirmation()))?;
    let admission = receipt.admission();
    out.u8(admission.actor_kind().tag())?;
    out.framed_u16(admission.principal_id().as_str().as_bytes())?;
    out.bytes(admission.capability_id().as_bytes())?;
    match admission.approval_id() {
        None => out.u8(0)?,
        Some(a) => {
            out.u8(1)?;
            out.framed_u16(a.as_bytes())?;
        }
    }
    out.optional_database_id(receipt.source_database_id())?;
    match receipt.selection() {
        None => out.u8(0)?,
        Some(s) => {
            out.u8(1)?;
            encode_selection(&mut out, s)?;
        }
    }
    out.optional_database_id(receipt.staged_database_id())?;
    match receipt.restored_frontier() {
        None => out.u8(0)?,
        Some(f) => {
            out.u8(1)?;
            out.bytes(&f.to_canonical_bytes())?;
        }
    }
    match receipt.published_history_incarnation() {
        None => out.u8(0)?,
        Some(i) => {
            out.u8(1)?;
            out.u64(i)?;
        }
    }
    out.u8(u8::try_from(receipt.transitions().len()).map_err(|_| limit_exceeded())?)?;
    for t in receipt.transitions() {
        out.u8(t.receipt_phase().tag())?;
        out.u8(t.failure().map_or(0, |f| f.tag()))?;
    }
    let mut bytes = out.finish();
    let digest = Sha256::digest(&bytes);
    reserve_bytes(&bytes, digest.len())?;
    bytes.extend_from_slice(&digest);
    Ok(bytes)
}
fn encode_selection(out: &mut Encoder, s: &ArchiveRestoreSelectionV3) -> Result<(), StorageError> {
    out.bytes(s.backup().manifest_checksum().as_bytes())?;
    out.bytes(s.backup().database_id().as_bytes())?;
    match s.backup().included_application_frontier() {
        None => out.u8(0)?,
        Some(c) => {
            out.u8(1)?;
            out.u64(c.get())?;
        }
    }
    out.u64(s.lineage().history_incarnation())?;
    out.u64(s.lineage().leadership_epoch().get())?;
    out.bytes(&s.lineage().catalog_digest())?;
    let fence = s.backup_fence();
    out.u64(fence.sequence().get())?;
    out.bytes(&fence.history_hash())?;
    out.bytes(&fence.frontier().to_canonical_bytes())?;
    match s.suffix() {
        ArchiveRestoreSuffixV3::Empty => out.u8(0)?,
        ArchiveRestoreSuffixV3::Terminal(m) => {
            out.u8(1)?;
            out.bytes(&m.encode())?;
        }
    }
    Ok(())
}
pub(in crate::maintenance) fn decode_archive_receipt(
    encoded: &[u8],
) -> Result<OfflineMaintenanceReceiptV3, StorageError> {
    if encoded.len() > MAX_RECEIPT_BYTES {
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
    if input.bytes(RECEIPT_MAGIC.len())? != RECEIPT_MAGIC
        || input.u32()? != OFFLINE_MAINTENANCE_RECEIPT_VERSION_V3
    {
        return Err(incompatible());
    }
    let id = OfflineMaintenanceOperationId::from_bytes(input.array()?).map_err(|_| corrupt())?;
    let backup = BackupNameV1::new(input.text_u16(riffdb_types::MAX_BACKUP_NAME_V1_BYTES)?)
        .map_err(|_| corrupt())?;
    let archive =
        ArchiveNameV1::new(input.text_u16(MAX_ARCHIVE_NAME_V1_BYTES)?).map_err(|_| corrupt())?;
    let stop = match input.u8()? {
        0 => ArchiveRestoreStopV1::LastArchived,
        1 => ArchiveRestoreStopV1::AtApplicationSequence(
            CommitSequence::new(input.u64()?).ok_or_else(corrupt)?,
        ),
        _ => return Err(corrupt()),
    };
    let hash = OfflineMaintenanceInputHash::from_bytes(input.array()?);
    let confirmation = confirmation_from_tag(input.u8()?).ok_or_else(corrupt)?;
    let actor_kind = ActorKind::from_tag(input.u8()?).ok_or_else(corrupt)?;
    let principal = ActorId::new(input.text_u16(MAX_ACTOR_ID_BYTES)?).map_err(|_| corrupt())?;
    let capability = CapabilityId::from_bytes(input.array()?).map_err(|_| corrupt())?;
    let approval = match input.u8()? {
        0 => None,
        1 => Some(ApprovalId::new(input.text_u16(MAX_APPROVAL_ID_BYTES)?).map_err(|_| corrupt())?),
        _ => return Err(corrupt()),
    };
    let admission = OfflineMaintenanceAdmissionV1::new(principal, actor_kind, capability, approval);
    let source = input.optional_database_id()?;
    let selection = match input.u8()? {
        0 => None,
        1 => Some(decode_selection(&mut input)?),
        _ => return Err(corrupt()),
    };
    let staged = input.optional_database_id()?;
    let restored = match input.u8()? {
        0 => None,
        1 => Some(DualFrontier::from_canonical_bytes(input.array()?).map_err(|_| corrupt())?),
        _ => return Err(corrupt()),
    };
    let incarnation = match input.u8()? {
        0 => None,
        1 => Some(input.u64()?),
        _ => return Err(corrupt()),
    };
    let count = usize::from(input.u8()?);
    if count == 0 || count > MAX_OFFLINE_MAINTENANCE_RECEIPT_TRANSITIONS_V1 {
        return Err(corrupt());
    }
    let mut transitions = Vec::with_capacity(count);
    for _ in 0..count {
        let phase = OfflineMaintenanceReceiptPhaseV1::from_tag(input.u8()?).ok_or_else(corrupt)?;
        let failure = input.u8()?;
        let transition = if phase == OfflineMaintenanceReceiptPhaseV1::FailedClosed {
            OfflineMaintenanceReceiptTransitionV1::failed(
                OfflineMaintenanceReceiptFailureV1::from_tag(failure).ok_or_else(corrupt)?,
            )
        } else {
            if failure != 0 {
                return Err(corrupt());
            }
            OfflineMaintenanceReceiptTransitionV1::phase(phase)
        };
        transitions.push(transition);
    }
    input.finish()?;
    let receipt = OfflineMaintenanceReceiptV3::from_canonical_parts(
        id,
        backup,
        archive,
        stop,
        hash,
        confirmation,
        admission,
        source,
        selection,
        staged,
        restored,
        incarnation,
        transitions,
    )
    .map_err(value_error)?;
    if encode_archive_receipt(&receipt)? != encoded {
        return Err(corrupt());
    }
    Ok(receipt)
}
fn decode_selection(input: &mut Decoder<'_>) -> Result<ArchiveRestoreSelectionV3, StorageError> {
    let checksum =
        BackupIntegrityChecksumV1::new(input.bytes(SHA256_BYTES)?.to_vec()).map_err(value_error)?;
    let db = DatabaseId::from_bytes(input.array()?).map_err(|_| corrupt())?;
    let retained = match input.u8()? {
        0 => None,
        1 => Some(CommitSequence::new(input.u64()?).ok_or_else(corrupt)?),
        _ => return Err(corrupt()),
    };
    let incarnation = input.u64()?;
    let epoch = LeadershipEpochV1::new(input.u64()?).ok_or_else(corrupt)?;
    let lineage = ChangelogLineageV3::new(db, incarnation, epoch).map_err(|_| corrupt())?;
    if input.array::<32>()? != AuthoritativeStateCatalogV1.digest() {
        return Err(incompatible());
    }
    let sequence = ChangelogTransactionSequence::new(input.u64()?).ok_or_else(corrupt)?;
    let hash = input.array()?;
    let frontier = DualFrontier::from_canonical_bytes(input.array()?).map_err(|_| corrupt())?;
    let fence = ChangelogHistoryPointV3::new(sequence, hash, frontier);
    let suffix = match input.u8()? {
        0 => ArchiveRestoreSuffixV3::Empty,
        1 => ArchiveRestoreSuffixV3::Terminal(Box::new(
            ArchiveManifestV1::decode(input.bytes(ARCHIVE_MANIFEST_V1_BYTES)?)
                .map_err(|_| corrupt())?,
        )),
        _ => return Err(corrupt()),
    };
    ArchiveRestoreSelectionV3::new(
        OfflineBackupManifestIdentityV1::new(checksum, db, retained),
        lineage,
        fence,
        suffix,
    )
    .map_err(value_error)
}
