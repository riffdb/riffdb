//! Receipt-version-preserving routing of interrupted current-database restores.
use super::*;
use riffdb_storage_api::OfflineMaintenanceReceiptV3;
use riffdb_types::{DatabaseId, OfflineMaintenanceInputHash, OfflineMaintenanceOperationId};

pub(super) enum ResumableMaintenanceReceipt {
    Ordinary(Box<OfflineMaintenanceReceiptV1>),
    Archive(Box<OfflineMaintenanceReceiptV3>),
}
impl From<OfflineMaintenanceReceiptV1> for ResumableMaintenanceReceipt {
    fn from(receipt: OfflineMaintenanceReceiptV1) -> Self {
        Self::Ordinary(Box::new(receipt))
    }
}
impl From<OfflineMaintenanceReceiptV3> for ResumableMaintenanceReceipt {
    fn from(receipt: OfflineMaintenanceReceiptV3) -> Self {
        Self::Archive(Box::new(receipt))
    }
}
impl ResumableMaintenanceReceipt {
    pub(super) fn operation_id(&self) -> OfflineMaintenanceOperationId {
        match self {
            Self::Ordinary(receipt) => receipt.operation_id(),
            Self::Archive(receipt) => receipt.operation_id(),
        }
    }
    pub(super) fn input_hash(&self) -> OfflineMaintenanceInputHash {
        match self {
            Self::Ordinary(receipt) => receipt.input_hash(),
            Self::Archive(receipt) => receipt.input_hash(),
        }
    }
    pub(super) fn source_database_id(&self) -> Option<DatabaseId> {
        match self {
            Self::Ordinary(receipt) => receipt.source_database_id(),
            Self::Archive(receipt) => receipt.source_database_id(),
        }
    }
}

pub(super) fn initial_action(
    storage: &mut RedbMaintenanceStorage,
    reconciliation: &RedbMaintenanceReconciliation,
    target_requires_recovery: bool,
) -> Result<Option<InitialDatabaseAction>, DaemonError> {
    let mut archives = reconciliation
        .archive_receipts()
        .receipts()
        .iter()
        .filter(|receipt| !receipt.current_phase().is_terminal());
    let Some(receipt) = archives.next() else {
        return Ok(None);
    };
    if archives.next().is_some()
        || (receipt.source_database_id().is_none() && receipt.selection().is_none())
        || reconciliation
            .receipts()
            .receipts()
            .iter()
            .any(|receipt| !receipt.current_phase().is_terminal())
        || reconciliation
            .retire_receipts()
            .receipts()
            .iter()
            .any(|receipt| !receipt.current_phase().is_terminal())
    {
        return Err(DaemonError::MaintenanceDriver);
    }
    use OfflineMaintenanceReceiptPhaseV1::*;
    let published = matches!(receipt.current_phase(), ArtifactPublished | Validating)
        || (receipt.current_phase() == Offline
            && storage
                .archive_restore_target_matches(receipt.operation_id())
                .map_err(DaemonError::MaintenanceStorage)?);
    if receipt.source_database_id().is_none() || (!published && target_requires_recovery) {
        if published {
            let request = riffdb_service::RestoreArchivedBackupRequest::new(
                receipt.operation_id(),
                receipt.backup_name().clone(),
                receipt.archive_name().clone(),
                receipt.stop(),
                receipt.replacement_confirmation(),
            )
            .map_err(|_| DaemonError::MaintenanceDriver)?;
            return Ok(Some(InitialDatabaseAction::ResumeRecovery(request.into())));
        }
        return Ok(Some(InitialDatabaseAction::AwaitRecoveryCredential(
            receipt.clone().into(),
        )));
    }
    if published {
        // The archive driver revalidates the receipt-bound stage and complete
        // source authority before readiness. This grants no ordinary V1 authority.
        return Ok(Some(InitialDatabaseAction::ResumeCurrent {
            receipt: receipt.clone().into(),
            request: MaintenanceDriverRequest::resume_published_restore(receipt.operation_id()),
            validate_current_source: false,
        }));
    }
    if matches!(receipt.current_phase(), Accepted | Draining | Offline) && !target_requires_recovery
    {
        return Ok(Some(InitialDatabaseAction::AwaitRestoreCredential(
            receipt.clone().into(),
        )));
    }
    Ok(Some(InitialDatabaseAction::FailClosed))
}
