use riffdb_storage_api::{
    BackupIntegrityChecksumV1, MAX_BACKUP_INTEGRITY_CHECKSUM_BYTES,
    MAX_OFFLINE_MAINTENANCE_RECEIPT_TRANSITIONS_V1, OfflineBackupManifestIdentityV1,
    OfflineMaintenanceAdmissionV1, OfflineMaintenanceReceiptFailureV1,
    OfflineMaintenanceReceiptPhaseV1, OfflineMaintenanceReceiptTransitionV1,
    OfflineMaintenanceReceiptV1, StorageError, StorageErrorKind, StorageValueError,
};
use riffdb_types::{
    ActorId, ActorKind, ApprovalId, BackupNameV1, CapabilityId, CommitSequence, DatabaseId,
    MAX_ACTOR_ID_BYTES, MAX_APPROVAL_ID_BYTES, OfflineMaintenanceInputHash,
    OfflineMaintenanceOperationId, OfflineMaintenanceOperationKind,
    OfflineMaintenanceReplacementConfirmation,
};
use sha2::{Digest, Sha256};

use crate::error::storage_error;

pub(super) const RECEIPT_FILE_SUFFIX: &str = ".receipt-v1";
pub(super) const RECEIPT_TEMP_SUFFIX: &str = ".receipt-v1.tmp";
pub(super) const MAX_RECEIPT_BYTES: usize = 4 * 1024;

const RECEIPT_MAGIC: &[u8] = b"RIFFDB-MAINT-RECEIPT\0";
const RECEIPT_FORMAT_VERSION: u32 = 1;
const SHA256_BYTES: usize = 32;

pub(super) fn encode_receipt(
    receipt: &OfflineMaintenanceReceiptV1,
) -> Result<Vec<u8>, StorageError> {
    let mut output = Encoder::new();
    output.bytes(RECEIPT_MAGIC)?;
    output.u32(RECEIPT_FORMAT_VERSION)?;
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

    match decode_receipt_body(body, true) {
        Ok(receipt) if encode_receipt(&receipt)? == encoded => Ok(receipt),
        Ok(_) => Err(corrupt()),
        Err(_) => {
            let receipt = decode_receipt_body(body, false)?;
            if encode_receipt_pre_fence(&receipt)? != encoded {
                return Err(corrupt());
            }
            Ok(receipt)
        }
    }
}

fn decode_receipt_body(
    body: &[u8],
    include_published_incarnation: bool,
) -> Result<OfflineMaintenanceReceiptV1, StorageError> {
    let mut input = Decoder::new(body);
    if input.bytes(RECEIPT_MAGIC.len())? != RECEIPT_MAGIC {
        return Err(incompatible());
    }
    if input.u32()? != RECEIPT_FORMAT_VERSION {
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

fn encode_receipt_pre_fence(
    receipt: &OfflineMaintenanceReceiptV1,
) -> Result<Vec<u8>, StorageError> {
    if receipt.published_history_incarnation().is_some() {
        return Err(corrupt());
    }
    let mut output = Encoder::new();
    output.bytes(RECEIPT_MAGIC)?;
    output.u32(RECEIPT_FORMAT_VERSION)?;
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

const fn operation_kind_tag(kind: OfflineMaintenanceOperationKind) -> u8 {
    match kind {
        OfflineMaintenanceOperationKind::CreateBackup => 0x01,
        OfflineMaintenanceOperationKind::RestoreBackup => 0x02,
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
}

impl Encoder {
    fn new() -> Self {
        Self { bytes: Vec::new() }
    }

    fn finish(self) -> Vec<u8> {
        self.bytes
    }

    fn bytes(&mut self, value: &[u8]) -> Result<(), StorageError> {
        reserve_bytes(&self.bytes, value.len())?;
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
    if bytes
        .len()
        .checked_add(additional)
        .is_none_or(|length| length > MAX_RECEIPT_BYTES)
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
