//! External audit only. No method grants peer trust, cutover, or primary readiness.
use super::*;
use crate::maintenance::codec::{
    MAX_PROMOTION_RECEIPT_BYTES, decode_promotion_receipt, encode_promotion_receipt,
};
use riffdb_storage_api::{
    MAX_REPLICATION_PROMOTION_RECEIPTS_V1, ReplicationPromotionPhaseV1 as Phase,
    ReplicationPromotionReceiptInventoryV1 as Inventory, ReplicationPromotionReceiptV1 as Receipt,
    ReplicationPromotionStepV1 as Step,
};
use riffdb_types::RequestId;

const SUFFIX: &str = ".receipt-v1";
const TEMP_SUFFIX: &str = ".receipt-v1.tmp";

impl RedbMaintenanceStorage {
    #[cfg(test)]
    pub(in crate::maintenance) fn arm_promotion_test_controller(
        &mut self,
        controller: RedbMaintenanceTestController,
    ) {
        self.test_controller = Some(controller);
    }

    /// Acquires the existing maintenance lock to inspect or persist external
    /// promotion audit. Does not reconcile a database, authorize a retry, or
    /// grant readiness. Ordinary maintenance remains fenced by unresolved audit.
    pub fn open_for_promotion_recovery(
        database_file: impl AsRef<Path>,
        backup_root: impl AsRef<Path>,
    ) -> Result<Self, StorageError> {
        let storage = Self::acquire_owner(database_file.as_ref(), backup_root.as_ref(), None)?;
        let (_, temporaries) = storage.read_promotion_inventory()?;
        storage.remove_unpublished_promotion_temps(&temporaries)?;
        Ok(storage)
    }

    /// Complete canonical published attempt inventory. No decoded phase proves
    /// that a runtime precondition or committed cutover exists.
    pub fn promotion_receipts(&self) -> Result<Inventory, StorageError> {
        self.verify_path_custody()?;
        let (inventory, temporaries) = self.read_promotion_inventory()?;
        if !temporaries.is_empty() {
            return Err(invariant());
        }
        Ok(inventory)
    }

    /// Selects the sole unresolved pre-cutover operation under pinned custody.
    /// This is audit inventory, never authorization, a follower-open proof or
    /// permission to cut over. The caller must independently validate local
    /// attached state and reconcile any already committed promotion first.
    pub fn pending_promotion_request(
        &self,
    ) -> Result<Option<riffdb_storage_api::ReplicationPromotionRequestV1>, StorageError> {
        let inventory = self.promotion_receipts()?;
        let mut pending = None;
        for receipt in inventory.receipts() {
            if matches!(
                receipt.phase(),
                Phase::CutoverCommitted | Phase::Validated | Phase::Succeeded
            ) {
                return Err(invariant());
            }
            if !receipt.is_terminal() || receipt.selection().is_some() {
                if pending.is_some_and(|request| request != receipt.request()) {
                    return Err(invariant());
                }
                pending = Some(receipt.request());
            }
        }
        if pending.is_some() {
            // Attached followers cannot originate ordinary backup, restore,
            // retirement or migration operations. Never use promotion recovery
            // to waive an ordinary maintenance operation or its artifact custody.
            for directory in [
                &self.receipts_directory_guard,
                &self.staged_directory_guard,
                &self.retired_directory_guard,
                &self.migrations_directory_guard,
            ] {
                directory.verify()?;
                directory.visit_entries_bounded(1, |_, _| Err(invariant()))?;
            }
        }
        self.verify_path_custody()?;
        Ok(pending)
    }

    /// Publishes one exact new attempt or monotonic replacement under exclusive
    /// ownership. The full proposed inventory freezes selection across attempts.
    /// Callers must await success before any lifecycle action or terminal reply.
    pub fn persist_promotion_receipt(&mut self, receipt: &Receipt) -> Result<(), StorageError> {
        self.verify_path_custody()?;
        let (inventory, temporaries) = self.read_promotion_inventory()?;
        let mut receipts = inventory.receipts().to_vec();
        match receipts.binary_search_by_key(&receipt.request_id(), Receipt::request_id) {
            Ok(index) => {
                if !receipt.monotonically_extends(&receipts[index]) {
                    return Err(invariant());
                }
                receipts[index] = receipt.clone();
            }
            Err(index) => {
                // A conflicting request can only enter as an atomic denial;
                // publishing its bare Attempted phase would retarget the operation.
                if receipt.phase() != Phase::Attempted
                    || !(receipt.steps().len() == 1
                        || (receipt.steps().len() == 2
                            && matches!(receipt.steps().last(), Some(Step::Denied(_)))))
                {
                    return Err(invariant());
                }
                if receipts.len() == MAX_REPLICATION_PROMOTION_RECEIPTS_V1 {
                    return Err(limit_exceeded());
                }
                receipts.insert(index, receipt.clone());
            }
        }
        Inventory::new(receipts).map_err(value_error)?;
        let encoded = encode_promotion_receipt(receipt)?;
        // Only unpublished staging names may be removed, and only after all
        // published evidence and the complete proposed replacement are checked.
        self.remove_unpublished_promotion_temps(&temporaries)?;
        if self.promotion_directory_guard.is_none() {
            self.promotion_directory_guard = Some(
                self.maintenance_directory_guard
                    .create_private_child(OsStr::new(PROMOTIONS_DIRECTORY_NAME))?,
            );
        }
        self.verify_path_custody()?;
        let directory = self
            .promotion_directory_guard
            .as_ref()
            .ok_or_else(invariant)?;
        let name = file_name(receipt.request_id());
        if inventory.receipts().iter().any(|prior| prior == receipt) {
            // Retrying an uncertain rename must establish file and directory
            // durability again before reporting success, even for identical bytes.
            directory
                .open_file(OsStr::new(&name))?
                .sync_all()
                .map_err(io_unavailable)?;
            directory.sync()?;
            return self.verify_path_custody();
        }
        let temporary = format!(".{}{TEMP_SUFFIX}", receipt.request_id());
        let mut file = directory
            .create_new_file(OsStr::new(&temporary))?
            .into_std();
        file.write_all(&encoded).map_err(io_unavailable)?;
        self.hit(RedbMaintenanceFailpoint::BeforeReceiptFileSync, false)?;
        file.sync_all().map_err(io_unavailable)?;
        self.hit(RedbMaintenanceFailpoint::AfterReceiptFileSync, false)?;
        self.verify_path_custody()?;
        if !directory.regular_file_matches(OsStr::new(&temporary), &file)? {
            return Err(corrupt());
        }
        self.hit(RedbMaintenanceFailpoint::BeforeReceiptRename, false)?;
        directory.rename(OsStr::new(&temporary), OsStr::new(&name))?;
        self.hit(RedbMaintenanceFailpoint::AfterReceiptRename, true)?;
        directory
            .sync()
            .map_err(|_| storage_error(StorageErrorKind::CommitStatusUnknown))?;
        self.hit(RedbMaintenanceFailpoint::AfterReceiptParentSync, true)?;
        self.verify_path_custody()
    }

    pub(super) fn require_no_unreconciled_promotion(&self) -> Result<(), StorageError> {
        let (inventory, temporaries) = self.read_promotion_inventory()?;
        // Only this continuous owner's completed reconciliation can join a
        // successful external receipt. A decoded phase cannot mint that proof.
        // Require its continued presence as well as equality: deleting all
        // evidence must not turn a previously reconciled ledger into an empty one.
        let joined = self.reconciled_promotion.as_ref();
        if !temporaries.is_empty()
            || joined.is_some_and(|proof| {
                !inventory
                    .receipts()
                    .iter()
                    .any(|receipt| proof.matches(receipt))
            })
            || inventory.receipts().iter().any(|receipt| {
                let resolved =
                    joined.is_some_and(|proof| proof.covers_pre_cutover_attempt(receipt));
                (!receipt.is_terminal() && !resolved)
                    || (receipt.phase() == Phase::Succeeded
                        && !joined.is_some_and(|proof| proof.matches(receipt)))
                    || (receipt.phase() != Phase::Succeeded
                        && receipt.selection().is_some()
                        && !resolved)
            })
        {
            return Err(invariant());
        }
        if let Some(joined) = joined {
            joined.verify_restores(self)?;
        }
        Ok(())
    }

    fn read_promotion_inventory(&self) -> Result<(Inventory, Vec<OsString>), StorageError> {
        let mut receipts = Vec::new();
        let mut temporaries = Vec::new();
        if let Some(directory) = &self.promotion_directory_guard {
            directory.verify_private()?;
            directory.visit_entries_bounded(
                MAX_REPLICATION_PROMOTION_RECEIPTS_V1 + 1,
                |name, is_dir| {
                    if is_dir {
                        return Err(corrupt());
                    }
                    let text = name.to_str().ok_or_else(corrupt)?;
                    if let Some(id) = text
                        .strip_prefix('.')
                        .and_then(|s| s.strip_suffix(TEMP_SUFFIX))
                    {
                        canonical_request_id(id)?;
                        if !temporaries.is_empty() {
                            return Err(limit_exceeded());
                        }
                        // A torn, unpublished write is disposable. Bound and inspect
                        // its file shape before any cleanup; never follow a symlink.
                        read_bounded(directory, name)?;
                        temporaries.push(name.to_os_string());
                    } else {
                        let id =
                            canonical_request_id(text.strip_suffix(SUFFIX).ok_or_else(corrupt)?)?;
                        if receipts.len() == MAX_REPLICATION_PROMOTION_RECEIPTS_V1 {
                            return Err(limit_exceeded());
                        }
                        let receipt = decode_promotion_receipt(&read_bounded(directory, name)?)?;
                        if receipt.request_id() != id {
                            return Err(corrupt());
                        }
                        receipts.push(receipt);
                    }
                    Ok(())
                },
            )?;
        }
        receipts.sort_by_key(Receipt::request_id);
        Ok((Inventory::new(receipts).map_err(value_error)?, temporaries))
    }

    fn remove_unpublished_promotion_temps(
        &self,
        temporaries: &[OsString],
    ) -> Result<(), StorageError> {
        self.verify_path_custody()?;
        if let Some(directory) = &self.promotion_directory_guard {
            for name in temporaries {
                directory.remove_file_if_present(name)?;
            }
            if !temporaries.is_empty() {
                directory.sync()?;
            }
        }
        self.verify_path_custody()
    }
}

fn canonical_request_id(text: &str) -> Result<RequestId, StorageError> {
    let id = RequestId::from_bytes(parse_uuid_bytes(text).ok_or_else(corrupt)?)
        .map_err(|_| corrupt())?;
    if id.to_string() != text {
        return Err(corrupt());
    }
    Ok(id)
}
fn file_name(request_id: RequestId) -> String {
    format!("{request_id}{SUFFIX}")
}
fn read_bounded(directory: &PinnedDirectory, name: &OsStr) -> Result<Vec<u8>, StorageError> {
    let mut file = directory.open_file(name)?.into_std();
    if file.metadata().map_err(io_unavailable)?.len() > MAX_PROMOTION_RECEIPT_BYTES as u64 {
        return Err(limit_exceeded());
    }
    let mut bytes = Vec::new();
    Read::by_ref(&mut file)
        .take(MAX_PROMOTION_RECEIPT_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(io_unavailable)?;
    if bytes.len() > MAX_PROMOTION_RECEIPT_BYTES || !directory.regular_file_matches(name, &file)? {
        return Err(corrupt());
    }
    Ok(bytes)
}
