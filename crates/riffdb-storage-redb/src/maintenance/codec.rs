use riffdb_storage_api::{
    BackupIntegrityChecksumV1,
    CONTRACT_MIGRATION_CHECK_RECEIPT_VERSION as MIGRATION_RECEIPT_CHECK_FORMAT_VERSION,
    ContractMigrationAdmissionV1, ContractMigrationArtifactFileV1, ContractMigrationArtifactsV1,
    ContractMigrationOperationArtifactsV1, ContractMigrationOperationKindV1,
    ContractMigrationReceiptFailureV1, ContractMigrationReceiptPhaseV1,
    ContractMigrationReceiptTransitionV1, ContractMigrationReceiptV1,
    MAX_BACKUP_INTEGRITY_CHECKSUM_BYTES, MAX_CONTRACT_MIGRATION_RECEIPT_TRANSITIONS_V1,
    MAX_OFFLINE_MAINTENANCE_RECEIPT_TRANSITIONS_V1,
    OFFLINE_MAINTENANCE_RECEIPT_VERSION_V1 as RECEIPT_FORMAT_VERSION_V1,
    OFFLINE_MAINTENANCE_RECEIPT_VERSION_V2 as RECEIPT_FORMAT_VERSION_V2,
    OfflineBackupManifestIdentityV1, OfflineBackupRetirementEvidenceV2,
    OfflineMaintenanceAdmissionV1, OfflineMaintenanceReceiptFailureV1,
    OfflineMaintenanceReceiptPhaseV1, OfflineMaintenanceReceiptTransitionV1,
    OfflineMaintenanceReceiptV1, OfflineMaintenanceReceiptV2, StorageError, StorageErrorKind,
    StorageValueError,
};
use riffdb_types::{
    ActorId, ActorKind, ApprovalId, BackupNameV1, CapabilityId, CommitSequence, ContractBundleHash,
    ContractMigrationInputHash, ContractMigrationOperationId, DatabaseId, MAX_ACTOR_ID_BYTES,
    MAX_APPROVAL_ID_BYTES, MigrationBundleHash, OfflineMaintenanceInputHash,
    OfflineMaintenanceOperationId, OfflineMaintenanceOperationKind,
    OfflineMaintenanceReplacementConfirmation, RequestId, ServiceIngressKindV1, Timestamp,
};
use sha2::{Digest, Sha256};

use crate::error::storage_error;

#[path = "archive_receipt_codec.rs"]
mod archive_receipt;
pub(super) use archive_receipt::{decode_archive_receipt, encode_archive_receipt};

pub(super) const RECEIPT_FILE_SUFFIX: &str = ".receipt-v1";
pub(super) const RECEIPT_TEMP_SUFFIX: &str = ".receipt-v1.tmp";
pub(super) const RETIRE_RECEIPT_FILE_SUFFIX: &str = ".receipt-v2";
pub(super) const RETIRE_RECEIPT_TEMP_SUFFIX: &str = ".receipt-v2.tmp";
pub(super) const ARCHIVE_RECEIPT_FILE_SUFFIX: &str = ".receipt-v3";
pub(super) const ARCHIVE_RECEIPT_TEMP_SUFFIX: &str = ".receipt-v3.tmp";
pub(super) const MAX_RECEIPT_BYTES: usize = 4 * 1024;

const RECEIPT_MAGIC: &[u8] = b"RIFFDB-MAINT-RECEIPT\0";
const SHA256_BYTES: usize = 32;
const MIGRATION_RECEIPT_MAGIC: &[u8] = b"RIFFDB-MIGRATION-RECEIPT\0";
pub(super) const MAX_MIGRATION_RECEIPT_BYTES: usize = 8 * 1024;

pub(super) fn encode_receipt(
    receipt: &OfflineMaintenanceReceiptV1,
) -> Result<Vec<u8>, StorageError> {
    let mut output = Encoder::new();
    output.bytes(RECEIPT_MAGIC)?;
    output.u32(RECEIPT_FORMAT_VERSION_V1)?;
    output.bytes(receipt.operation_id().as_bytes())?;
    output.u8(operation_kind_tag(receipt.operation_kind()))?;
    output.framed_u16(receipt.backup_name().as_bytes())?;
    output.bytes(receipt.input_hash().as_bytes())?;
    output.u8(confirmation_tag(receipt.replacement_confirmation()))?;

    let admission = receipt.admission();
    output.u8(admission.actor_kind().tag())?;
    output.framed_u16(admission.principal_id().as_str().as_bytes())?;
    output.bytes(admission.capability_id().as_bytes())?;
    match admission.approval_id() {
        None => output.u8(0)?,
        Some(approval_id) => {
            output.u8(1)?;
            output.framed_u16(approval_id.as_bytes())?;
        }
    }

    output.optional_database_id(receipt.source_database_id())?;
    output.optional_database_id(receipt.staged_database_id())?;
    match receipt.manifest_identity() {
        None => output.u8(0)?,
        Some(identity) => {
            if identity.manifest_checksum().as_bytes().len() != SHA256_BYTES {
                return Err(corrupt());
            }
            output.u8(1)?;
            output.framed_u16(identity.manifest_checksum().as_bytes())?;
            output.bytes(identity.database_id().as_bytes())?;
            match identity.included_application_frontier() {
                None => output.u8(0)?,
                Some(sequence) => {
                    output.u8(1)?;
                    output.u64(sequence.get())?;
                }
            }
        }
    }
    match receipt.published_history_incarnation() {
        None => output.u8(0)?,
        Some(incarnation) => {
            output.u8(1)?;
            output.u64(incarnation)?;
        }
    }

    output.u8(u8::try_from(receipt.transitions().len()).map_err(|_| limit_exceeded())?)?;
    for transition in receipt.transitions() {
        output.u8(transition.receipt_phase().tag())?;
        output.u8(transition.failure().map_or(0, |failure| failure.tag()))?;
    }

    let mut bytes = output.finish();
    let checksum = Sha256::digest(&bytes);
    reserve_bytes(&bytes, checksum.len())?;
    bytes.extend_from_slice(&checksum);
    Ok(bytes)
}

pub(super) fn decode_receipt(encoded: &[u8]) -> Result<OfflineMaintenanceReceiptV1, StorageError> {
    if encoded.len() > MAX_RECEIPT_BYTES {
        return Err(limit_exceeded());
    }
    let body_length = encoded
        .len()
        .checked_sub(SHA256_BYTES)
        .ok_or_else(corrupt)?;
    let (body, stored_checksum) = encoded.split_at(body_length);
    if Sha256::digest(body).as_slice() != stored_checksum {
        return Err(corrupt());
    }

    // Prefer post-fence (presence-tagged published incarnation). Fall through to
    // pre-fence on any post-fence parse/canonicalization mismatch so mid-upgrade
    // receipts remain resumable.
    if let Ok(receipt) = decode_receipt_body(body, true)
        && encode_receipt(&receipt)? == encoded
    {
        return Ok(receipt);
    }
    let receipt = decode_receipt_body(body, false)?;
    if encode_receipt_pre_fence(&receipt)? != encoded {
        return Err(corrupt());
    }
    Ok(receipt)
}

fn decode_receipt_body(
    body: &[u8],
    include_published_incarnation: bool,
) -> Result<OfflineMaintenanceReceiptV1, StorageError> {
    let mut input = Decoder::new(body);
    if input.bytes(RECEIPT_MAGIC.len())? != RECEIPT_MAGIC {
        return Err(incompatible());
    }
    if input.u32()? != RECEIPT_FORMAT_VERSION_V1 {
        return Err(incompatible());
    }
    let operation_id =
        OfflineMaintenanceOperationId::from_bytes(input.array()?).map_err(|_| corrupt())?;
    let operation_kind = operation_kind_from_tag(input.u8()?).ok_or_else(corrupt)?;
    let backup_name = BackupNameV1::new(input.text_u16(riffdb_types::MAX_BACKUP_NAME_V1_BYTES)?)
        .map_err(|_| corrupt())?;
    let input_hash = OfflineMaintenanceInputHash::from_bytes(input.array()?);
    let replacement_confirmation = confirmation_from_tag(input.u8()?).ok_or_else(corrupt)?;

    let actor_kind = ActorKind::from_tag(input.u8()?).ok_or_else(corrupt)?;
    let principal_id = ActorId::new(input.text_u16(MAX_ACTOR_ID_BYTES)?).map_err(|_| corrupt())?;
    let capability_id = CapabilityId::from_bytes(input.array()?).map_err(|_| corrupt())?;
    let approval_id = match input.u8()? {
        0 => None,
        1 => Some(ApprovalId::new(input.text_u16(MAX_APPROVAL_ID_BYTES)?).map_err(|_| corrupt())?),
        _ => return Err(corrupt()),
    };
    let admission =
        OfflineMaintenanceAdmissionV1::new(principal_id, actor_kind, capability_id, approval_id);

    let source_database_id = input.optional_database_id()?;
    let staged_database_id = input.optional_database_id()?;
    let manifest_identity = match input.u8()? {
        0 => None,
        1 => {
            let checksum = input.framed_u16(MAX_BACKUP_INTEGRITY_CHECKSUM_BYTES)?;
            if checksum.len() != SHA256_BYTES {
                return Err(corrupt());
            }
            let checksum =
                BackupIntegrityChecksumV1::new(checksum.to_vec()).map_err(value_error)?;
            let database_id = DatabaseId::from_bytes(input.array()?).map_err(|_| corrupt())?;
            let frontier = match input.u8()? {
                0 => None,
                1 => Some(CommitSequence::new(input.u64()?).ok_or_else(corrupt)?),
                _ => return Err(corrupt()),
            };
            Some(OfflineBackupManifestIdentityV1::new(
                checksum,
                database_id,
                frontier,
            ))
        }
        _ => return Err(corrupt()),
    };
    let published_history_incarnation = if include_published_incarnation {
        match input.u8()? {
            0 => None,
            1 => {
                let incarnation = input.u64()?;
                if incarnation < 1 {
                    return Err(corrupt());
                }
                Some(incarnation)
            }
            _ => return Err(corrupt()),
        }
    } else {
        None
    };

    let transition_count = usize::from(input.u8()?);
    if transition_count == 0 || transition_count > MAX_OFFLINE_MAINTENANCE_RECEIPT_TRANSITIONS_V1 {
        return Err(corrupt());
    }
    let mut transitions = Vec::with_capacity(transition_count);
    for _ in 0..transition_count {
        let phase = OfflineMaintenanceReceiptPhaseV1::from_tag(input.u8()?).ok_or_else(corrupt)?;
        let failure_tag = input.u8()?;
        let transition = match (phase, failure_tag) {
            (OfflineMaintenanceReceiptPhaseV1::FailedClosed, tag) => {
                OfflineMaintenanceReceiptTransitionV1::failed(
                    OfflineMaintenanceReceiptFailureV1::from_tag(tag).ok_or_else(corrupt)?,
                )
            }
            (_, 0) => OfflineMaintenanceReceiptTransitionV1::phase(phase),
            (_, _) => return Err(corrupt()),
        };
        transitions.push(transition);
    }
    input.finish()?;

    OfflineMaintenanceReceiptV1::from_canonical_parts(
        operation_id,
        operation_kind,
        backup_name,
        input_hash,
        replacement_confirmation,
        admission,
        source_database_id,
        staged_database_id,
        manifest_identity,
        published_history_incarnation,
        transitions,
    )
    .map_err(value_error)
}

pub(super) fn encode_retire_receipt(
    receipt: &OfflineMaintenanceReceiptV2,
) -> Result<Vec<u8>, StorageError> {
    let mut output = Encoder::new();
    output.bytes(RECEIPT_MAGIC)?;
    output.u32(RECEIPT_FORMAT_VERSION_V2)?;
    output.bytes(receipt.operation_id().as_bytes())?;
    output.u8(0x03)?;
    output.framed_u16(receipt.backup_name().as_bytes())?;
    output.bytes(receipt.input_hash().as_bytes())?;

    let admission = receipt.admission();
    output.u8(admission.actor_kind().tag())?;
    output.framed_u16(admission.principal_id().as_str().as_bytes())?;
    output.bytes(admission.capability_id().as_bytes())?;
    match admission.approval_id() {
        None => output.u8(0)?,
        Some(approval_id) => {
            output.u8(1)?;
            output.framed_u16(approval_id.as_bytes())?;
        }
    }

    output.bytes(
        receipt
            .retirement()
            .originating_create_operation_id()
            .as_bytes(),
    )?;
    let identity = receipt.retirement().manifest_identity();
    if identity.manifest_checksum().as_bytes().len() != SHA256_BYTES {
        return Err(corrupt());
    }
    output.framed_u16(identity.manifest_checksum().as_bytes())?;
    output.bytes(identity.database_id().as_bytes())?;
    match identity.included_application_frontier() {
        None => output.u8(0)?,
        Some(sequence) => {
            output.u8(1)?;
            output.u64(sequence.get())?;
        }
    }

    output.u8(u8::try_from(receipt.transitions().len()).map_err(|_| limit_exceeded())?)?;
    for transition in receipt.transitions() {
        output.u8(transition.receipt_phase().tag())?;
        output.u8(transition.failure().map_or(0, |failure| failure.tag()))?;
    }
    let mut bytes = output.finish();
    let checksum = Sha256::digest(&bytes);
    reserve_bytes(&bytes, checksum.len())?;
    bytes.extend_from_slice(&checksum);
    Ok(bytes)
}

pub(super) fn decode_retire_receipt(
    encoded: &[u8],
) -> Result<OfflineMaintenanceReceiptV2, StorageError> {
    if encoded.len() > MAX_RECEIPT_BYTES {
        return Err(limit_exceeded());
    }
    let body_length = encoded
        .len()
        .checked_sub(SHA256_BYTES)
        .ok_or_else(corrupt)?;
    let (body, stored_checksum) = encoded.split_at(body_length);
    if Sha256::digest(body).as_slice() != stored_checksum {
        return Err(corrupt());
    }
    let mut input = Decoder::new(body);
    if input.bytes(RECEIPT_MAGIC.len())? != RECEIPT_MAGIC
        || input.u32()? != RECEIPT_FORMAT_VERSION_V2
    {
        return Err(incompatible());
    }
    let operation_id =
        OfflineMaintenanceOperationId::from_bytes(input.array()?).map_err(|_| corrupt())?;
    if input.u8()? != 0x03 {
        return Err(corrupt());
    }
    let backup_name = BackupNameV1::new(input.text_u16(riffdb_types::MAX_BACKUP_NAME_V1_BYTES)?)
        .map_err(|_| corrupt())?;
    let input_hash = OfflineMaintenanceInputHash::from_bytes(input.array()?);
    let actor_kind = ActorKind::from_tag(input.u8()?).ok_or_else(corrupt)?;
    let principal_id = ActorId::new(input.text_u16(MAX_ACTOR_ID_BYTES)?).map_err(|_| corrupt())?;
    let capability_id = CapabilityId::from_bytes(input.array()?).map_err(|_| corrupt())?;
    let approval_id = match input.u8()? {
        0 => None,
        1 => Some(ApprovalId::new(input.text_u16(MAX_APPROVAL_ID_BYTES)?).map_err(|_| corrupt())?),
        _ => return Err(corrupt()),
    };
    let admission =
        OfflineMaintenanceAdmissionV1::new(principal_id, actor_kind, capability_id, approval_id);
    let originating_create_operation_id =
        OfflineMaintenanceOperationId::from_bytes(input.array()?).map_err(|_| corrupt())?;
    let checksum = input.framed_u16(MAX_BACKUP_INTEGRITY_CHECKSUM_BYTES)?;
    if checksum.len() != SHA256_BYTES {
        return Err(corrupt());
    }
    let checksum = BackupIntegrityChecksumV1::new(checksum.to_vec()).map_err(value_error)?;
    let database_id = DatabaseId::from_bytes(input.array()?).map_err(|_| corrupt())?;
    let frontier = match input.u8()? {
        0 => None,
        1 => Some(CommitSequence::new(input.u64()?).ok_or_else(corrupt)?),
        _ => return Err(corrupt()),
    };
    let transition_count = usize::from(input.u8()?);
    if transition_count == 0 || transition_count > MAX_OFFLINE_MAINTENANCE_RECEIPT_TRANSITIONS_V1 {
        return Err(corrupt());
    }
    let mut transitions = Vec::with_capacity(transition_count);
    for _ in 0..transition_count {
        let phase = OfflineMaintenanceReceiptPhaseV1::from_tag(input.u8()?).ok_or_else(corrupt)?;
        let failure_tag = input.u8()?;
        transitions.push(match (phase, failure_tag) {
            (OfflineMaintenanceReceiptPhaseV1::FailedClosed, tag) => {
                OfflineMaintenanceReceiptTransitionV1::failed(
                    OfflineMaintenanceReceiptFailureV1::from_tag(tag).ok_or_else(corrupt)?,
                )
            }
            (_, 0) => OfflineMaintenanceReceiptTransitionV1::phase(phase),
            _ => return Err(corrupt()),
        });
    }
    input.finish()?;
    let manifest = OfflineBackupManifestIdentityV1::new(checksum, database_id, frontier);
    let receipt = OfflineMaintenanceReceiptV2::from_canonical_parts(
        operation_id,
        backup_name,
        input_hash,
        admission,
        OfflineBackupRetirementEvidenceV2::new(originating_create_operation_id, manifest),
        transitions,
    )
    .map_err(value_error)?;
    if encode_retire_receipt(&receipt)? != encoded {
        return Err(corrupt());
    }
    Ok(receipt)
}

fn encode_receipt_pre_fence(
    receipt: &OfflineMaintenanceReceiptV1,
) -> Result<Vec<u8>, StorageError> {
    if receipt.published_history_incarnation().is_some() {
        return Err(corrupt());
    }
    let mut output = Encoder::new();
    output.bytes(RECEIPT_MAGIC)?;
    output.u32(RECEIPT_FORMAT_VERSION_V1)?;
    output.bytes(receipt.operation_id().as_bytes())?;
    output.u8(operation_kind_tag(receipt.operation_kind()))?;
    output.framed_u16(receipt.backup_name().as_bytes())?;
    output.bytes(receipt.input_hash().as_bytes())?;
    output.u8(confirmation_tag(receipt.replacement_confirmation()))?;

    let admission = receipt.admission();
    output.u8(admission.actor_kind().tag())?;
    output.framed_u16(admission.principal_id().as_str().as_bytes())?;
    output.bytes(admission.capability_id().as_bytes())?;
    match admission.approval_id() {
        None => output.u8(0)?,
        Some(approval_id) => {
            output.u8(1)?;
            output.framed_u16(approval_id.as_bytes())?;
        }
    }

    output.optional_database_id(receipt.source_database_id())?;
    output.optional_database_id(receipt.staged_database_id())?;
    match receipt.manifest_identity() {
        None => output.u8(0)?,
        Some(identity) => {
            if identity.manifest_checksum().as_bytes().len() != SHA256_BYTES {
                return Err(corrupt());
            }
            output.u8(1)?;
            output.framed_u16(identity.manifest_checksum().as_bytes())?;
            output.bytes(identity.database_id().as_bytes())?;
            match identity.included_application_frontier() {
                None => output.u8(0)?,
                Some(sequence) => {
                    output.u8(1)?;
                    output.u64(sequence.get())?;
                }
            }
        }
    }

    output.u8(u8::try_from(receipt.transitions().len()).map_err(|_| limit_exceeded())?)?;
    for transition in receipt.transitions() {
        output.u8(transition.receipt_phase().tag())?;
        output.u8(transition.failure().map_or(0, |failure| failure.tag()))?;
    }

    let mut bytes = output.finish();
    let checksum = Sha256::digest(&bytes);
    reserve_bytes(&bytes, checksum.len())?;
    bytes.extend_from_slice(&checksum);
    Ok(bytes)
}

pub(super) fn encode_migration_receipt(
    receipt: &ContractMigrationReceiptV1,
) -> Result<Vec<u8>, StorageError> {
    let mut output = Encoder::new_with_limit(MAX_MIGRATION_RECEIPT_BYTES);
    output.bytes(MIGRATION_RECEIPT_MAGIC)?;
    match receipt.operation_kind() {
        ContractMigrationOperationKindV1::Apply => output.u32(RECEIPT_FORMAT_VERSION_V1)?,
        ContractMigrationOperationKindV1::Check => {
            output.u32(MIGRATION_RECEIPT_CHECK_FORMAT_VERSION)?;
            output.u8(1)?;
        }
    }
    output.bytes(receipt.database_id().as_bytes())?;
    output.bytes(receipt.operation_id().as_bytes())?;
    output.bytes(receipt.input_hash().as_bytes())?;
    output.bytes(receipt.artifacts().parent().as_bytes())?;
    output.bytes(receipt.artifacts().candidate().as_bytes())?;
    output.bytes(receipt.artifacts().migration().as_bytes())?;
    encode_artifact_file(&mut output, receipt.operation_artifacts().candidate())?;
    encode_artifact_file(&mut output, receipt.operation_artifacts().migration())?;
    encode_migration_admission(&mut output, receipt.admission())?;
    match (receipt.backup_name(), receipt.backup_manifest()) {
        (None, None) => output.u8(0)?,
        (Some(name), Some(manifest)) => {
            output.u8(1)?;
            output.framed_u16(name.as_bytes())?;
            output.framed_u16(manifest.manifest_checksum().as_bytes())?;
            output.bytes(manifest.database_id().as_bytes())?;
            match manifest.included_application_frontier() {
                None => output.u8(0)?,
                Some(frontier) => {
                    output.u8(1)?;
                    output.u64(frontier.get())?;
                }
            }
        }
        _ => return Err(corrupt()),
    }
    match receipt.stage_identity() {
        None => output.u8(0)?,
        Some(identity) => {
            output.u8(1)?;
            output.bytes(&identity)?;
        }
    }
    output.u8(u8::try_from(receipt.transitions().len()).map_err(|_| limit_exceeded())?)?;
    for transition in receipt.transitions() {
        output.u8(migration_phase_tag(transition.receipt_phase()))?;
        output.u8(transition.failure().map_or(0, migration_failure_tag))?;
    }
    let mut bytes = output.finish();
    let checksum = Sha256::digest(&bytes);
    if bytes
        .len()
        .checked_add(checksum.len())
        .is_none_or(|len| len > MAX_MIGRATION_RECEIPT_BYTES)
    {
        return Err(limit_exceeded());
    }
    bytes.extend_from_slice(&checksum);
    Ok(bytes)
}

pub(super) fn decode_migration_receipt(
    encoded: &[u8],
) -> Result<ContractMigrationReceiptV1, StorageError> {
    if encoded.len() > MAX_MIGRATION_RECEIPT_BYTES {
        return Err(limit_exceeded());
    }
    let body_len = encoded
        .len()
        .checked_sub(SHA256_BYTES)
        .ok_or_else(corrupt)?;
    let (body, checksum) = encoded.split_at(body_len);
    if Sha256::digest(body).as_slice() != checksum {
        return Err(corrupt());
    }
    let mut input = Decoder::new(body);
    if input.bytes(MIGRATION_RECEIPT_MAGIC.len())? != MIGRATION_RECEIPT_MAGIC {
        return Err(incompatible());
    }
    let operation_kind = match input.u32()? {
        RECEIPT_FORMAT_VERSION_V1 => ContractMigrationOperationKindV1::Apply,
        MIGRATION_RECEIPT_CHECK_FORMAT_VERSION => match input.u8()? {
            1 => ContractMigrationOperationKindV1::Check,
            _ => return Err(incompatible()),
        },
        _ => return Err(incompatible()),
    };
    let database_id = DatabaseId::from_bytes(input.array()?).map_err(|_| corrupt())?;
    let operation_id =
        ContractMigrationOperationId::from_bytes(input.array()?).map_err(|_| corrupt())?;
    let input_hash = ContractMigrationInputHash::from_bytes(input.array()?);
    let artifacts = ContractMigrationArtifactsV1::new(
        ContractBundleHash::from_bytes(input.array()?),
        ContractBundleHash::from_bytes(input.array()?),
        MigrationBundleHash::from_bytes(input.array()?),
    );
    let operation_artifacts = ContractMigrationOperationArtifactsV1::new(
        decode_artifact_file(&mut input)?,
        decode_artifact_file(&mut input)?,
    );
    let admission = decode_migration_admission(&mut input)?;
    let (backup_name, backup_manifest) = match input.u8()? {
        0 => (None, None),
        1 => {
            let name = BackupNameV1::new(input.text_u16(riffdb_types::MAX_BACKUP_NAME_V1_BYTES)?)
                .map_err(|_| corrupt())?;
            let checksum = BackupIntegrityChecksumV1::new(
                input
                    .framed_u16(MAX_BACKUP_INTEGRITY_CHECKSUM_BYTES)?
                    .to_vec(),
            )
            .map_err(value_error)?;
            let manifest_database =
                DatabaseId::from_bytes(input.array()?).map_err(|_| corrupt())?;
            let frontier = match input.u8()? {
                0 => None,
                1 => Some(CommitSequence::new(input.u64()?).ok_or_else(corrupt)?),
                _ => return Err(corrupt()),
            };
            (
                Some(name),
                Some(OfflineBackupManifestIdentityV1::new(
                    checksum,
                    manifest_database,
                    frontier,
                )),
            )
        }
        _ => return Err(corrupt()),
    };
    let stage_identity = match input.u8()? {
        0 => None,
        1 => Some(input.array()?),
        _ => return Err(corrupt()),
    };
    let count = usize::from(input.u8()?);
    if count == 0 || count > MAX_CONTRACT_MIGRATION_RECEIPT_TRANSITIONS_V1 {
        return Err(corrupt());
    }
    let mut transitions = Vec::with_capacity(count);
    for _ in 0..count {
        let phase = migration_phase_from_tag(input.u8()?).ok_or_else(corrupt)?;
        let failure = input.u8()?;
        transitions.push(match (phase, failure) {
            (
                ContractMigrationReceiptPhaseV1::FailedClosed
                | ContractMigrationReceiptPhaseV1::FailedRolledBack,
                tag,
            ) => ContractMigrationReceiptTransitionV1::failed(
                phase,
                migration_failure_from_tag(tag).ok_or_else(corrupt)?,
            ),
            (_, 0) => ContractMigrationReceiptTransitionV1::phase(phase),
            _ => return Err(corrupt()),
        });
    }
    input.finish()?;
    let receipt = ContractMigrationReceiptV1::from_canonical_parts_for_operation(
        operation_kind,
        database_id,
        operation_id,
        input_hash,
        artifacts,
        operation_artifacts,
        admission,
        backup_name,
        backup_manifest,
        stage_identity,
        transitions,
    )
    .map_err(value_error)?;
    if encode_migration_receipt(&receipt)? != encoded {
        return Err(corrupt());
    }
    Ok(receipt)
}

fn encode_migration_admission(
    output: &mut Encoder,
    admission: &ContractMigrationAdmissionV1,
) -> Result<(), StorageError> {
    output.u8(admission.principal().actor_kind().tag())?;
    output.framed_u16(admission.principal().principal_id().as_str().as_bytes())?;
    output.bytes(admission.principal().capability_id().as_bytes())?;
    output.u64(admission.principal().capability_revision().get())?;
    match admission.approval_id() {
        None => output.u8(0)?,
        Some(approval) => {
            output.u8(1)?;
            output.framed_u16(approval.as_bytes())?;
        }
    }
    output.bytes(admission.request_id().as_bytes())?;
    output.i64(admission.accepted_at().seconds())?;
    output.u32(admission.accepted_at().nanoseconds())?;
    output.u8(admission.ingress().tag())
}

fn decode_migration_admission(
    input: &mut Decoder<'_>,
) -> Result<ContractMigrationAdmissionV1, StorageError> {
    let actor_kind = ActorKind::from_tag(input.u8()?).ok_or_else(corrupt)?;
    let principal_id = ActorId::new(input.text_u16(MAX_ACTOR_ID_BYTES)?).map_err(|_| corrupt())?;
    let capability_id = CapabilityId::from_bytes(input.array()?).map_err(|_| corrupt())?;
    let capability_revision = std::num::NonZeroU64::new(input.u64()?).ok_or_else(corrupt)?;
    let approval_id = match input.u8()? {
        0 => None,
        1 => Some(ApprovalId::new(input.text_u16(MAX_APPROVAL_ID_BYTES)?).map_err(|_| corrupt())?),
        _ => return Err(corrupt()),
    };
    let request_id = RequestId::from_bytes(input.array()?).map_err(|_| corrupt())?;
    let accepted_at = Timestamp::new(input.i64()?, input.u32()?).map_err(|_| corrupt())?;
    let ingress = ServiceIngressKindV1::from_tag(input.u8()?).ok_or_else(corrupt)?;
    Ok(ContractMigrationAdmissionV1::new(
        riffdb_storage_api::AuditPrincipalV1::new(
            principal_id,
            actor_kind,
            capability_id,
            capability_revision,
        ),
        approval_id,
        request_id,
        accepted_at,
        ingress,
    ))
}

fn encode_artifact_file(
    output: &mut Encoder,
    artifact: ContractMigrationArtifactFileV1,
) -> Result<(), StorageError> {
    output.u64(artifact.length())?;
    output.bytes(&artifact.sha256())
}

fn decode_artifact_file(
    input: &mut Decoder<'_>,
) -> Result<ContractMigrationArtifactFileV1, StorageError> {
    ContractMigrationArtifactFileV1::new(input.u64()?, input.array()?).map_err(value_error)
}

const fn migration_phase_tag(phase: ContractMigrationReceiptPhaseV1) -> u8 {
    use ContractMigrationReceiptPhaseV1 as P;
    match phase {
        P::Accepted => 1,
        P::Draining => 2,
        P::Preflight => 3,
        P::BackupPublished => 4,
        P::Staging => 5,
        P::Transforming => 6,
        P::RebuildingProjections => 7,
        P::ValidatingStage => 8,
        P::Publishing => 9,
        P::ValidatingPublished => 10,
        P::RollingBack => 11,
        P::Succeeded => 12,
        P::FailedClosed => 13,
        P::FailedRolledBack => 14,
    }
}

const fn migration_phase_from_tag(tag: u8) -> Option<ContractMigrationReceiptPhaseV1> {
    use ContractMigrationReceiptPhaseV1 as P;
    match tag {
        1 => Some(P::Accepted),
        2 => Some(P::Draining),
        3 => Some(P::Preflight),
        4 => Some(P::BackupPublished),
        5 => Some(P::Staging),
        6 => Some(P::Transforming),
        7 => Some(P::RebuildingProjections),
        8 => Some(P::ValidatingStage),
        9 => Some(P::Publishing),
        10 => Some(P::ValidatingPublished),
        11 => Some(P::RollingBack),
        12 => Some(P::Succeeded),
        13 => Some(P::FailedClosed),
        14 => Some(P::FailedRolledBack),
        _ => None,
    }
}

const fn migration_failure_tag(failure: ContractMigrationReceiptFailureV1) -> u8 {
    use ContractMigrationReceiptFailureV1 as F;
    match failure {
        F::ArtifactMismatch => 1,
        F::InvalidPredecessor => 2,
        F::PendingAdmission => 3,
        F::CapacityExhausted => 4,
        F::DiskUnavailable => 5,
        F::StageCorrupt => 6,
        F::PublicationUncertain => 7,
        F::PublishedValidationFailed => 8,
        F::RollbackFailed => 9,
    }
}

const fn migration_failure_from_tag(tag: u8) -> Option<ContractMigrationReceiptFailureV1> {
    use ContractMigrationReceiptFailureV1 as F;
    match tag {
        1 => Some(F::ArtifactMismatch),
        2 => Some(F::InvalidPredecessor),
        3 => Some(F::PendingAdmission),
        4 => Some(F::CapacityExhausted),
        5 => Some(F::DiskUnavailable),
        6 => Some(F::StageCorrupt),
        7 => Some(F::PublicationUncertain),
        8 => Some(F::PublishedValidationFailed),
        9 => Some(F::RollbackFailed),
        _ => None,
    }
}

const fn operation_kind_tag(kind: OfflineMaintenanceOperationKind) -> u8 {
    match kind {
        OfflineMaintenanceOperationKind::CreateBackup => 0x01,
        OfflineMaintenanceOperationKind::RestoreBackup => 0x02,
        OfflineMaintenanceOperationKind::RetireBackup => 0x03,
    }
}

const fn operation_kind_from_tag(tag: u8) -> Option<OfflineMaintenanceOperationKind> {
    match tag {
        0x01 => Some(OfflineMaintenanceOperationKind::CreateBackup),
        0x02 => Some(OfflineMaintenanceOperationKind::RestoreBackup),
        _ => None,
    }
}

const fn confirmation_tag(value: OfflineMaintenanceReplacementConfirmation) -> u8 {
    match value {
        OfflineMaintenanceReplacementConfirmation::NotProvided => 0x00,
        OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget => 0x01,
    }
}

const fn confirmation_from_tag(tag: u8) -> Option<OfflineMaintenanceReplacementConfirmation> {
    match tag {
        0x00 => Some(OfflineMaintenanceReplacementConfirmation::NotProvided),
        0x01 => Some(OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget),
        _ => None,
    }
}

struct Encoder {
    bytes: Vec<u8>,
    limit: usize,
}

impl Encoder {
    fn new() -> Self {
        Self::new_with_limit(MAX_RECEIPT_BYTES)
    }

    fn new_with_limit(limit: usize) -> Self {
        Self {
            bytes: Vec::new(),
            limit,
        }
    }

    fn finish(self) -> Vec<u8> {
        self.bytes
    }

    fn bytes(&mut self, value: &[u8]) -> Result<(), StorageError> {
        reserve_bytes_with_limit(&self.bytes, value.len(), self.limit)?;
        self.bytes.extend_from_slice(value);
        Ok(())
    }

    fn u8(&mut self, value: u8) -> Result<(), StorageError> {
        self.bytes(&[value])
    }

    fn u16(&mut self, value: u16) -> Result<(), StorageError> {
        self.bytes(&value.to_be_bytes())
    }

    fn u32(&mut self, value: u32) -> Result<(), StorageError> {
        self.bytes(&value.to_be_bytes())
    }

    fn u64(&mut self, value: u64) -> Result<(), StorageError> {
        self.bytes(&value.to_be_bytes())
    }

    fn i64(&mut self, value: i64) -> Result<(), StorageError> {
        self.bytes(&value.to_be_bytes())
    }

    fn framed_u16(&mut self, value: &[u8]) -> Result<(), StorageError> {
        self.u16(u16::try_from(value.len()).map_err(|_| limit_exceeded())?)?;
        self.bytes(value)
    }

    fn optional_database_id(
        &mut self,
        database_id: Option<DatabaseId>,
    ) -> Result<(), StorageError> {
        match database_id {
            None => self.u8(0),
            Some(database_id) => {
                self.u8(1)?;
                self.bytes(database_id.as_bytes())
            }
        }
    }
}

struct Decoder<'a> {
    encoded: &'a [u8],
    offset: usize,
}

impl<'a> Decoder<'a> {
    fn new(encoded: &'a [u8]) -> Self {
        Self { encoded, offset: 0 }
    }

    fn finish(self) -> Result<(), StorageError> {
        if self.offset == self.encoded.len() {
            Ok(())
        } else {
            Err(corrupt())
        }
    }

    fn bytes(&mut self, length: usize) -> Result<&'a [u8], StorageError> {
        let end = self.offset.checked_add(length).ok_or_else(corrupt)?;
        let bytes = self.encoded.get(self.offset..end).ok_or_else(corrupt)?;
        self.offset = end;
        Ok(bytes)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], StorageError> {
        self.bytes(N)?.try_into().map_err(|_| corrupt())
    }

    fn u8(&mut self) -> Result<u8, StorageError> {
        Ok(self.array::<1>()?[0])
    }

    fn u16(&mut self) -> Result<u16, StorageError> {
        Ok(u16::from_be_bytes(self.array()?))
    }

    fn u32(&mut self) -> Result<u32, StorageError> {
        Ok(u32::from_be_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64, StorageError> {
        Ok(u64::from_be_bytes(self.array()?))
    }

    fn i64(&mut self) -> Result<i64, StorageError> {
        Ok(i64::from_be_bytes(self.array()?))
    }

    fn framed_u16(&mut self, maximum: usize) -> Result<&'a [u8], StorageError> {
        let length = usize::from(self.u16()?);
        if length > maximum {
            return Err(limit_exceeded());
        }
        self.bytes(length)
    }

    fn text_u16(&mut self, maximum: usize) -> Result<String, StorageError> {
        let bytes = self.framed_u16(maximum)?;
        std::str::from_utf8(bytes)
            .map(str::to_owned)
            .map_err(|_| corrupt())
    }

    fn optional_database_id(&mut self) -> Result<Option<DatabaseId>, StorageError> {
        match self.u8()? {
            0 => Ok(None),
            1 => DatabaseId::from_bytes(self.array()?)
                .map(Some)
                .map_err(|_| corrupt()),
            _ => Err(corrupt()),
        }
    }
}

fn reserve_bytes(bytes: &[u8], additional: usize) -> Result<(), StorageError> {
    reserve_bytes_with_limit(bytes, additional, MAX_RECEIPT_BYTES)
}

fn reserve_bytes_with_limit(
    bytes: &[u8],
    additional: usize,
    limit: usize,
) -> Result<(), StorageError> {
    if bytes
        .len()
        .checked_add(additional)
        .is_none_or(|length| length > limit)
    {
        return Err(limit_exceeded());
    }
    Ok(())
}

fn value_error(error: StorageValueError) -> StorageError {
    match error {
        StorageValueError::LimitExceeded | StorageValueError::SizeOverflow => limit_exceeded(),
        StorageValueError::Empty
        | StorageValueError::NonCanonicalOrder
        | StorageValueError::Duplicate
        | StorageValueError::IdentityMismatch
        | StorageValueError::InvalidShape => corrupt(),
    }
}

fn corrupt() -> StorageError {
    storage_error(StorageErrorKind::CorruptData)
}

fn incompatible() -> StorageError {
    storage_error(StorageErrorKind::IncompatibleFormat)
}

fn limit_exceeded() -> StorageError {
    storage_error(StorageErrorKind::LimitExceeded)
}

#[cfg(test)]
mod tests {
    use riffdb_storage_api::{
        AuditPrincipalV1, BackupIntegrityChecksumV1, ContractMigrationAdmissionV1,
        ContractMigrationArtifactFileV1, ContractMigrationArtifactsV1,
        ContractMigrationOperationArtifactsV1, ContractMigrationOperationKindV1,
        ContractMigrationReceiptPhaseV1, ContractMigrationReceiptTransitionV1,
        ContractMigrationReceiptV1, OfflineBackupManifestIdentityV1,
        OfflineBackupRetirementEvidenceV2, OfflineMaintenanceAdmissionV1,
        OfflineMaintenanceReceiptV2,
    };
    use riffdb_types::{
        ActorId, ActorKind, ApprovalId, BackupNameV1, CapabilityId, CommitSequence,
        ContractBundleHash, ContractMigrationInputHash, ContractMigrationOperationId, DatabaseId,
        MigrationBundleHash, OfflineMaintenanceOperationId, OfflineMaintenanceOperationKind,
        OfflineMaintenanceReplacementConfirmation, RequestId, ServiceIngressKindV1, Timestamp,
        offline_maintenance_input_hash,
    };

    use sha2::{Digest, Sha256};

    use super::{
        MIGRATION_RECEIPT_MAGIC, SHA256_BYTES, decode_migration_receipt, decode_retire_receipt,
        encode_migration_receipt, encode_retire_receipt,
    };

    fn uuid_v7(seed: u8) -> [u8; 16] {
        let mut bytes = [seed; 16];
        bytes[6] = 0x70 | (seed & 0x0f);
        bytes[8] = 0x80 | (seed & 0x3f);
        bytes
    }

    fn accepted_receipt() -> ContractMigrationReceiptV1 {
        accepted_receipt_for(ContractMigrationOperationKindV1::Apply)
    }

    fn accepted_retire_receipt() -> OfflineMaintenanceReceiptV2 {
        let operation_id =
            OfflineMaintenanceOperationId::from_bytes(uuid_v7(11)).expect("maintenance operation");
        let name = BackupNameV1::new("before-upgrade").expect("backup name");
        OfflineMaintenanceReceiptV2::accepted_retirement(
            operation_id,
            name.clone(),
            offline_maintenance_input_hash(
                OfflineMaintenanceOperationKind::RetireBackup,
                &name,
                OfflineMaintenanceReplacementConfirmation::NotProvided,
            ),
            OfflineMaintenanceAdmissionV1::new(
                ActorId::new("operator").expect("actor"),
                ActorKind::Human,
                CapabilityId::from_bytes(uuid_v7(12)).expect("capability"),
                Some(ApprovalId::new("approval").expect("approval")),
            ),
            OfflineBackupRetirementEvidenceV2::new(
                OfflineMaintenanceOperationId::from_bytes(uuid_v7(13)).expect("originating create"),
                OfflineBackupManifestIdentityV1::new(
                    BackupIntegrityChecksumV1::new(vec![14; 32]).expect("checksum"),
                    DatabaseId::from_bytes(uuid_v7(15)).expect("database"),
                    Some(CommitSequence::new(16).expect("frontier")),
                ),
            ),
        )
        .expect("retire receipt")
    }

    fn accepted_receipt_for(
        operation_kind: ContractMigrationOperationKindV1,
    ) -> ContractMigrationReceiptV1 {
        ContractMigrationReceiptV1::from_canonical_parts_for_operation(
            operation_kind,
            DatabaseId::from_bytes(uuid_v7(1)).expect("database"),
            ContractMigrationOperationId::from_bytes(uuid_v7(2)).expect("operation"),
            ContractMigrationInputHash::from_bytes([3; 32]),
            ContractMigrationArtifactsV1::new(
                ContractBundleHash::from_bytes([4; 32]),
                ContractBundleHash::from_bytes([5; 32]),
                MigrationBundleHash::from_bytes([6; 32]),
            ),
            ContractMigrationOperationArtifactsV1::new(
                ContractMigrationArtifactFileV1::new(101, [7; 32]).expect("candidate file"),
                ContractMigrationArtifactFileV1::new(202, [8; 32]).expect("migration file"),
            ),
            ContractMigrationAdmissionV1::new(
                AuditPrincipalV1::new(
                    ActorId::new("operator").expect("actor"),
                    ActorKind::Human,
                    CapabilityId::from_bytes(uuid_v7(9)).expect("capability"),
                    std::num::NonZeroU64::new(7).expect("revision"),
                ),
                Some(ApprovalId::new("approval").expect("approval")),
                RequestId::from_bytes(uuid_v7(10)).expect("request"),
                Timestamp::new(1_700_000_000, 123).expect("timestamp"),
                ServiceIngressKindV1::Grpc,
            ),
            None,
            None,
            None,
            vec![ContractMigrationReceiptTransitionV1::phase(
                ContractMigrationReceiptPhaseV1::Accepted,
            )],
        )
        .expect("receipt")
    }

    #[test]
    fn migration_receipt_round_trips_canonically_and_rejects_checksum_damage() {
        let receipt = accepted_receipt();
        let encoded = encode_migration_receipt(&receipt).expect("encode");
        assert_eq!(decode_migration_receipt(&encoded).expect("decode"), receipt);
        assert_eq!(
            encode_migration_receipt(&decode_migration_receipt(&encoded).expect("decode"))
                .expect("reencode"),
            encoded
        );
        let mut corrupt = encoded;
        corrupt[40] ^= 0x01;
        assert!(decode_migration_receipt(&corrupt).is_err());
    }

    #[test]
    fn check_receipt_uses_the_read_only_graph_and_rejects_unknown_kind() {
        let receipt = accepted_receipt_for(ContractMigrationOperationKindV1::Check)
            .advance(ContractMigrationReceiptPhaseV1::Preflight)
            .expect("preflight")
            .advance(ContractMigrationReceiptPhaseV1::Succeeded)
            .expect("checked");
        let encoded = encode_migration_receipt(&receipt).expect("encode");
        assert_eq!(decode_migration_receipt(&encoded).expect("decode"), receipt);
        assert_eq!(
            decode_migration_receipt(&encoded)
                .expect("decode")
                .operation_kind(),
            ContractMigrationOperationKindV1::Check
        );

        let kind_offset = MIGRATION_RECEIPT_MAGIC.len() + std::mem::size_of::<u32>();
        let mut unknown = encoded;
        unknown[kind_offset] = 0xff;
        let body_len = unknown.len() - SHA256_BYTES;
        let checksum = Sha256::digest(&unknown[..body_len]);
        unknown[body_len..].copy_from_slice(&checksum);
        assert!(decode_migration_receipt(&unknown).is_err());
    }

    #[test]
    fn retire_receipt_v2_round_trips_canonically_and_rejects_checksum_damage() {
        let receipt = accepted_retire_receipt();
        let encoded = encode_retire_receipt(&receipt).expect("encode");
        let canonical_hex = encoded
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        assert_eq!(
            canonical_hex,
            include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../fixtures/compatibility/offline-maintenance-retire-receipt-v2.hex"
            ))
            .trim(),
            "retire receipt V2 exact bytes are a compatibility boundary"
        );
        assert_eq!(decode_retire_receipt(&encoded).expect("decode"), receipt);
        assert_eq!(
            encode_retire_receipt(&decode_retire_receipt(&encoded).expect("decode"))
                .expect("reencode"),
            encoded
        );
        let mut corrupt = encoded;
        corrupt[40] ^= 0x01;
        assert!(decode_retire_receipt(&corrupt).is_err());
    }
}
