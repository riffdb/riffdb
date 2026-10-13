//! Private durable facts only. Native transfer/startup activation is separate.
use super::super::codec::publication_intent::{
    MAX_INTENT_BYTES, PublicationIntent, PublicationPhase,
};
use super::*;
use sha2::Digest;
use std::ffi::OsStr;

const SUFFIX: &str = ".publication-intent-v1";
const TEMP_SUFFIX: &str = ".publication-intent-v1.tmp";

pub(super) struct IntentInventory {
    pub(super) current: Option<PublicationIntent>,
    temporary: Option<OfflineMaintenanceOperationId>,
    current_file: Option<File>,
    complete_temporary: Option<PublicationIntent>,
}

impl IntentInventory {
    pub(super) fn read(directory: &PinnedDirectory) -> Result<Self, StorageError> {
        let mut inventory = Self {
            current: None,
            temporary: None,
            current_file: None,
            complete_temporary: None,
        };
        for (name, is_directory) in directory.bounded_entries(
            MAX_OFFLINE_MAINTENANCE_RECEIPTS_V1 + MAX_UNPUBLISHED_RECEIPT_TEMPS + 2,
        )? {
            if is_directory {
                return Err(corrupt());
            }
            let text = name.to_str().ok_or_else(corrupt)?;
            if let Some(operation) = text
                .strip_prefix('.')
                .and_then(|s| s.strip_suffix(TEMP_SUFFIX))
            {
                let operation = parse_operation_id(operation).ok_or_else(corrupt)?;
                if text != format!(".{operation}{TEMP_SUFFIX}")
                    || inventory.temporary.replace(operation).is_some()
                {
                    return Err(corrupt());
                }
                let mut file = checked_record_file(directory, &name)?;
                let bytes = read_record_bytes(&mut file)?;
                // A torn unpublished write carries no authority. A complete
                // checksum envelope must decode and bind as the exact next
                // record before any temporary, including an ordinary receipt,
                // can be removed. Unknown complete versions fail closed.
                if bytes.len() >= 32 {
                    let (body, checksum) = bytes.split_at(bytes.len() - 32);
                    if sha2::Sha256::digest(body).as_slice() == checksum {
                        let intent = PublicationIntent::decode(&bytes)?;
                        if intent.operation_id != operation {
                            return Err(corrupt());
                        }
                        inventory.complete_temporary = Some(intent);
                    }
                }
                if !directory.regular_file_matches(&name, &file)? {
                    return Err(corrupt());
                }
            } else if let Some(operation) = text.strip_suffix(SUFFIX) {
                let operation = parse_operation_id(operation).ok_or_else(corrupt)?;
                if text != format!("{operation}{SUFFIX}") || inventory.current.is_some() {
                    return Err(corrupt());
                }
                let mut file = checked_record_file(directory, &name)?;
                let bytes = read_record_bytes(&mut file)?;
                let intent = PublicationIntent::decode(&bytes)?;
                if intent.operation_id != operation
                    || !directory.regular_file_matches(&name, &file)?
                {
                    return Err(corrupt());
                }
                inventory.current = Some(intent);
                inventory.current_file = Some(file);
            }
        }
        if let (Some(current), Some(temporary)) = (&inventory.current, inventory.temporary)
            && current.operation_id != temporary
        {
            return Err(corrupt());
        }
        directory.verify()?;
        Ok(inventory)
    }

    pub(super) fn owns_name(&self, name: &str) -> bool {
        self.current
            .as_ref()
            .is_some_and(|v| name == format!("{}{SUFFIX}", v.operation_id))
            || self
                .temporary
                .is_some_and(|v| name == format!(".{v}{TEMP_SUFFIX}"))
    }

    pub(super) fn temporary_name(&self) -> Option<String> {
        self.temporary.map(|v| format!(".{v}{TEMP_SUFFIX}"))
    }

    pub(super) fn validate(
        &self,
        v1: &OfflineMaintenanceReceiptInventoryV1,
        v2: &OfflineMaintenanceReceiptInventoryV2,
        v3: &OfflineMaintenanceReceiptInventoryV3,
        v4: &OfflineMaintenanceReceiptInventoryV4,
    ) -> Result<(), StorageError> {
        let operation = self
            .current
            .as_ref()
            .map(|v| v.operation_id)
            .or(self.temporary);
        let Some(operation) = operation else {
            return Ok(());
        };
        let receipt = v1
            .receipts()
            .iter()
            .find(|r| r.operation_id() == operation)
            .ok_or_else(corrupt)?;
        if receipt.operation_kind() != OfflineMaintenanceOperationKind::RestoreBackup
            || receipt.source_database_id().is_some()
            || v1
                .receipts()
                .iter()
                .any(|r| r.operation_id() != operation && !r.current_phase().is_terminal())
            || v2
                .receipts()
                .iter()
                .any(|r| r.operation_id() == operation || !r.current_phase().is_terminal())
            || v3
                .receipts()
                .iter()
                .any(|r| r.operation_id() == operation || !r.current_phase().is_terminal())
            || v4
                .receipts()
                .iter()
                .any(|r| r.operation_id() == operation || !r.current_phase().is_terminal())
        {
            return Err(corrupt());
        }
        if let Some(intent) = &self.current {
            validate_receipt_binding(intent, receipt)?;
        } else if receipt.current_phase() != OfflineMaintenanceReceiptPhaseV1::Offline {
            return Err(corrupt());
        }
        if self.temporary.is_some()
            && (receipt.current_phase().is_terminal()
                || self
                    .current
                    .as_ref()
                    .is_some_and(|v| v.phase == PublicationPhase::StartupConsumed))
        {
            return Err(corrupt());
        }
        if let Some(temporary) = &self.complete_temporary {
            validate_receipt_binding(temporary, receipt)?;
            match &self.current {
                Some(prior) => {
                    let mut next = prior.clone();
                    next.advance(temporary.phase)?;
                    if next != *temporary {
                        return Err(corrupt());
                    }
                }
                None if temporary.phase == PublicationPhase::Prepared => {}
                None => return Err(corrupt()),
            }
        }
        Ok(())
    }
}

fn read_record_bytes(file: &mut File) -> Result<Vec<u8>, StorageError> {
    let mut bytes = Vec::new();
    Read::by_ref(file)
        .take((MAX_INTENT_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(io_unavailable)?;
    if bytes.len() > MAX_INTENT_BYTES {
        return Err(limit_exceeded());
    }
    Ok(bytes)
}

fn checked_record_file(directory: &PinnedDirectory, name: &OsStr) -> Result<File, StorageError> {
    let file = directory.open_file(name)?.into_std();
    let metadata = file.metadata().map_err(io_unavailable)?;
    if metadata.len() > MAX_INTENT_BYTES as u64 {
        return Err(limit_exceeded());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 {
            return Err(corrupt());
        }
    }
    Ok(file)
}

fn validate_receipt_binding(
    intent: &PublicationIntent,
    receipt: &OfflineMaintenanceReceiptV1,
) -> Result<(), StorageError> {
    intent.validate()?;
    let phase_allowed = match intent.phase {
        PublicationPhase::Prepared => {
            receipt.current_phase() == OfflineMaintenanceReceiptPhaseV1::Offline
        }
        PublicationPhase::Published => matches!(
            receipt.current_phase(),
            OfflineMaintenanceReceiptPhaseV1::Offline
                | OfflineMaintenanceReceiptPhaseV1::ArtifactPublished
                | OfflineMaintenanceReceiptPhaseV1::Validating
        ),
        PublicationPhase::StartupConsumed => matches!(
            receipt.current_phase(),
            OfflineMaintenanceReceiptPhaseV1::ArtifactPublished
                | OfflineMaintenanceReceiptPhaseV1::Validating
                | OfflineMaintenanceReceiptPhaseV1::Succeeded
        ),
    };
    if !phase_allowed
        || intent.operation_id != receipt.operation_id()
        || receipt.operation_kind() != OfflineMaintenanceOperationKind::RestoreBackup
        || receipt.source_database_id().is_some()
        || receipt.manifest_identity() != Some(&intent.manifest)
        || receipt.staged_database_id() != Some(intent.manifest.database_id())
        || receipt.published_history_incarnation() != Some(intent.incarnation)
    {
        return Err(corrupt());
    }
    Ok(())
}

impl RedbMaintenanceStorage {
    /// Removes only the consumed record of an already durable successful restore.
    /// This never opens, repairs, replaces or certifies the published database.
    /// The caller retains its validated engine until cleanup is durable.
    pub(in crate::maintenance) fn complete_native_publication_cleanup(
        &self,
        operation: OfflineMaintenanceOperationId,
    ) -> Result<(), StorageError> {
        self.verify_path_ownership()?;
        // Refuse all unknown inventories and every unpublished temporary before
        // unlinking anything. In particular, cleanup cannot finish Validating.
        let (receipts, _, _, _, _) = self.read_complete_inventory_mode(false, false)?;
        let receipt = receipts
            .receipts()
            .iter()
            .find(|receipt| receipt.operation_id() == operation)
            .ok_or_else(corrupt)?;
        if receipt.operation_kind() != OfflineMaintenanceOperationKind::RestoreBackup
            || receipt.current_phase() != OfflineMaintenanceReceiptPhaseV1::Succeeded
        {
            return Err(invariant());
        }
        let inventory = IntentInventory::read(&self.receipts_directory_guard)?;
        let directory = &self.receipts_directory_guard;
        let Some(intent) = &inventory.current else {
            // An earlier unlink may have succeeded before its sync failed. The
            // idempotent continuation still synchronizes before admission.
            directory.sync().map_err(|_| unknown())?;
            return self.verify_path_ownership();
        };
        if intent.operation_id != operation
            || intent.phase != PublicationPhase::StartupConsumed
            || inventory.temporary.is_some()
        {
            return Err(corrupt());
        }
        validate_receipt_binding(intent, receipt)?;
        let name = format!("{operation}{SUFFIX}");
        let original = inventory.current_file.as_ref().ok_or_else(corrupt)?;
        self.hit(RedbMaintenanceFailpoint::BeforeNativeIntentRemoval, true)?;
        // Keep the original record descriptor through removal, and recheck its
        // exact canonical bytes and identity after the last interruptible hook.
        self.verify_path_ownership()?;
        if !directory.regular_file_matches(OsStr::new(&name), original)? {
            return Err(corrupt());
        }
        use std::io::{Seek, SeekFrom};
        let mut reader = original.try_clone().map_err(io_unavailable)?;
        reader.seek(SeekFrom::Start(0)).map_err(io_unavailable)?;
        if read_record_bytes(&mut reader)? != intent.encode()?
            || !directory.regular_file_matches(OsStr::new(&name), original)?
        {
            return Err(corrupt());
        }
        // The maintenance owner serializes receipt writers. A replaced or
        // uncertain success receipt cannot authorize deletion of this record.
        let latest = read_receipt_file(&self.receipt_path(operation))?;
        if encode_receipt(&latest)? != encode_receipt(receipt)? {
            return Err(corrupt());
        }
        if !directory
            .remove_file_if_present(OsStr::new(&name))
            .map_err(|_| unknown())?
        {
            return Err(unknown());
        }
        self.hit(RedbMaintenanceFailpoint::AfterNativeIntentRemoval, true)?;
        directory.sync().map_err(|_| unknown())?;
        self.hit(
            RedbMaintenanceFailpoint::AfterNativeIntentDirectorySync,
            true,
        )?;
        self.verify_path_ownership().map_err(|_| unknown())
    }

    pub(in crate::maintenance) fn write_native_publication_intent(
        &self,
        expected: Option<&PublicationIntent>,
        replacement: &PublicationIntent,
    ) -> Result<(), StorageError> {
        self.verify_path_ownership()?;
        // The original ordinary scanner validates every other receipt and
        // unknown filename before a byte is written or removed.
        self.read_complete_inventory(false)?;
        let inventory = IntentInventory::read(&self.receipts_directory_guard)?;
        if inventory.current.as_ref() != expected || inventory.temporary.is_some() {
            return Err(corrupt());
        }
        match expected {
            None if replacement.phase == PublicationPhase::Prepared => {}
            Some(prior) => {
                let mut next = prior.clone();
                next.advance(replacement.phase)?;
                if next != *replacement {
                    return Err(corrupt());
                }
            }
            _ => return Err(corrupt()),
        }
        let receipt = read_receipt_file(&self.receipt_path(replacement.operation_id))?;
        validate_receipt_binding(replacement, &receipt)?;
        let directory = &self.receipts_directory_guard;
        let destination = format!("{}{SUFFIX}", replacement.operation_id);
        let temporary = format!(".{}{TEMP_SUFFIX}", replacement.operation_id);
        let mut file = directory
            .create_new_file(OsStr::new(&temporary))?
            .into_std();
        file.write_all(&replacement.encode()?)
            .map_err(io_unavailable)?;
        self.hit(RedbMaintenanceFailpoint::BeforeReceiptFileSync, false)?;
        file.sync_all().map_err(io_unavailable)?;
        self.hit(RedbMaintenanceFailpoint::AfterReceiptFileSync, false)?;
        directory.verify()?;
        if !directory.regular_file_matches(OsStr::new(&temporary), &file)? {
            return Err(corrupt());
        }
        self.hit(RedbMaintenanceFailpoint::BeforeReceiptRename, false)?;
        if let Some(original) = &inventory.current_file {
            if !directory.regular_file_matches(OsStr::new(&destination), original)? {
                return Err(corrupt());
            }
            use std::io::{Seek, SeekFrom};
            let mut reader = original.try_clone().map_err(io_unavailable)?;
            reader.seek(SeekFrom::Start(0)).map_err(io_unavailable)?;
            let checksum = crate::backup::sha256_reader(reader)?;
            let expected_checksum = sha2::Sha256::digest(expected.ok_or_else(corrupt)?.encode()?);
            if checksum.as_bytes() != expected_checksum.as_slice() {
                return Err(corrupt());
            }
            directory
                .rename(OsStr::new(&temporary), OsStr::new(&destination))
                .map_err(|_| unknown())?;
        } else {
            directory
                .rename_to(OsStr::new(&temporary), directory, OsStr::new(&destination))
                .map_err(|_| unknown())?;
        }
        self.hit(RedbMaintenanceFailpoint::AfterReceiptRename, true)?;
        // Preserve both the intent and every artifact on uncertain sync. There
        // is no destructor that deletes or silently retries this operation.
        directory.sync().map_err(|_| unknown())?;
        self.hit(RedbMaintenanceFailpoint::AfterReceiptParentSync, true)?;
        if !directory.regular_file_matches(OsStr::new(&destination), &file)? {
            return Err(unknown());
        }
        self.verify_path_ownership()
    }
}

#[cfg(test)]
#[path = "publication_intent_store_tests.rs"]
mod tests;
