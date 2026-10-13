//! Whole-owner read-only preflight before native continuation or cleanup.
//! The result carries candidate ownership only; startup still gates admission.
use super::native_publication::IntentInventory;
use super::*;
use crate::maintenance::codec::publication_intent::PublicationPhase;

impl RedbMaintenanceStorage {
    pub(super) fn reconcile_native_publication(
        &self,
    ) -> Result<Option<OfflineMaintenanceOperationId>, StorageError> {
        let publication = IntentInventory::read(&self.receipts_directory_guard)?;
        if publication.current.is_none() && publication.temporary_name().is_none() {
            return Ok(None);
        }
        let (receipts, retired, archives, _, _) = self.read_complete_inventory_mode(true, false)?;
        // The four receipt versions and publication record are validated first.
        // Check every other reserved owner inventory before any native move,
        // intent removal, ordinary temporary cleanup or reservation release.
        if self
            .validate_migration_inventory_mode(false)?
            .iter()
            .any(|receipt| !receipt.current_phase().is_terminal())
        {
            return Err(invariant());
        }
        checked_staged_inventory_mode(
            &self.staged_directory,
            &receipts
                .receipts()
                .iter()
                .map(|r| (r.operation_id().to_string(), r))
                .collect(),
            &archives
                .receipts()
                .iter()
                .map(|r| r.operation_id().to_string())
                .collect(),
            false,
        )?;
        validate_retired_directory_inventory(&self.retired_directory, &retired)?;
        let mut retired_creates = BTreeSet::new();
        for retirement in retired.receipts() {
            let create = receipts
                .receipts()
                .iter()
                .find(|r| {
                    r.operation_id() == retirement.retirement().originating_create_operation_id()
                })
                .ok_or_else(corrupt)?;
            if create.operation_kind() != OfflineMaintenanceOperationKind::CreateBackup
                || create.current_phase() != OfflineMaintenanceReceiptPhaseV1::Succeeded
                || create.backup_name() != retirement.backup_name()
                || create.manifest_identity() != Some(retirement.retirement().manifest_identity())
                || !retired_creates.insert(create.operation_id())
            {
                return Err(corrupt());
            }
        }
        let Some(intent) = publication.current else {
            // No durable intent authorized an artifact move. The ordinary
            // reconciler can remove an admitted unpublished record temporary.
            return Ok(None);
        };
        let receipt = receipts
            .receipts()
            .iter()
            .find(|r| r.operation_id() == intent.operation_id)
            .ok_or_else(corrupt)?;
        let recovered = self
            .prepare_native_publication_recovery()?
            .ok_or_else(corrupt)?;
        if receipt.current_phase() == OfflineMaintenanceReceiptPhaseV1::Succeeded {
            // Terminal success, not the old backup's seals, authorizes record
            // cleanup. Release native exclusion before ordinary source startup.
            drop(recovered);
            self.complete_native_publication_cleanup(intent.operation_id)?;
            return Ok(None);
        }
        if intent.phase == PublicationPhase::StartupConsumed {
            // This is only an actual-target ownership candidate. The server
            // must still perform complete recovery/validation before success.
            drop(recovered);
        } else {
            recovered.resume_publication(self)?;
        }
        Ok(Some(intent.operation_id))
    }
}
