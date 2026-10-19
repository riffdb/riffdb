//! Receipt-version-preserving routing of interrupted current-database restores.
use super::*;
use riffdb_storage_api::{OfflineMaintenanceReceiptV3, OfflineMaintenanceReceiptV4};
use riffdb_types::{DatabaseId, OfflineMaintenanceInputHash, OfflineMaintenanceOperationId};

pub(crate) enum ResumableMaintenanceReceipt {
    Ordinary(Box<OfflineMaintenanceReceiptV1>),
    Archive(Box<OfflineMaintenanceReceiptV3>),
    Scrub(Box<OfflineMaintenanceReceiptV4>),
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
            Self::Scrub(receipt) => receipt.operation_id(),
        }
    }
    pub(super) fn input_hash(&self) -> OfflineMaintenanceInputHash {
        match self {
            Self::Ordinary(receipt) => receipt.input_hash(),
            Self::Archive(receipt) => receipt.input_hash(),
            Self::Scrub(receipt) => receipt.input_hash(),
        }
    }
    pub(super) fn source_database_id(&self) -> Option<DatabaseId> {
        match self {
            Self::Ordinary(receipt) => receipt.source_database_id(),
            Self::Archive(receipt) => receipt.source_database_id(),
            Self::Scrub(_) => None,
        }
    }
}

/// An interrupted scrub (ADR-0203) reruns its closed-database pass before
/// ordinary startup; it is the only incomplete maintenance operation allowed.
pub(super) fn scrub_initial_action(
    reconciliation: &RedbMaintenanceReconciliation,
) -> Result<Option<InitialDatabaseAction>, DaemonError> {
    let mut scrubs = reconciliation
        .scrub_receipts()
        .receipts()
        .iter()
        .filter(|receipt| !receipt.current_phase().is_terminal());
    let Some(receipt) = scrubs.next() else {
        return Ok(None);
    };
    if scrubs.next().is_some()
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
        || reconciliation
            .archive_receipts()
            .receipts()
            .iter()
            .any(|receipt| !receipt.current_phase().is_terminal())
        || reconciliation
            .migration_receipts()
            .iter()
            .any(|receipt| !receipt.current_phase().is_terminal())
    {
        return Err(DaemonError::MaintenanceDriver);
    }
    Ok(Some(InitialDatabaseAction::ResumeCurrent {
        request: MaintenanceDriverRequest::storage_scrub(receipt.operation_id()),
        receipt: ResumableMaintenanceReceipt::Scrub(Box::new(receipt.clone())),
        validate_current_source: false,
    }))
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

#[cfg(test)]
mod scrub_restart_tests {
    use super::*;
    use riffdb_storage_api::{
        OfflineMaintenanceAdmissionV1, OfflineMaintenanceReceiptPersistencePort,
        OfflineMaintenanceReceiptTransitionV1,
    };
    use riffdb_types::{ActorId, ActorKind, CapabilityId};

    #[test]
    // req: REC-008
    fn interrupted_scrub_reruns_its_offline_pass_before_ordinary_startup() {
        let root = tempfile::TempDir::new().unwrap();
        let target = root.path().join("db.redb");
        let backups = root.path().join("backups");
        let (mut store, _) = RedbMaintenanceStorage::open(&target, &backups).unwrap();
        let mut receipt = OfflineMaintenanceReceiptV4::accepted_storage_scrub(
            OfflineMaintenanceOperationId::from_unix_milliseconds_and_random(1000, [6; 10])
                .unwrap(),
            OfflineMaintenanceAdmissionV1::new(
                ActorId::new("operator").unwrap(),
                ActorKind::Human,
                CapabilityId::from_unix_milliseconds_and_random(1000, [7; 10]).unwrap(),
                None,
            ),
        )
        .unwrap();
        store.create_or_read_scrub_receipt(&receipt).unwrap();
        use OfflineMaintenanceReceiptPhaseV1::*;
        for phase in [Accepted, Draining, Offline, Validating, Succeeded] {
            if phase != receipt.current_phase() {
                receipt
                    .advance(OfflineMaintenanceReceiptTransitionV1::phase(phase))
                    .unwrap();
                store.replace_scrub_receipt(&receipt).unwrap();
            }
            drop(store);
            let (reopened, reconciliation) =
                RedbMaintenanceStorage::open(&target, &backups).unwrap();
            store = reopened;
            for target_requires_recovery in [false, true] {
                let action =
                    initial_database_action(&mut store, &reconciliation, target_requires_recovery);
                if phase == Succeeded {
                    // A terminal scrub never resumes and never forces recovery.
                    assert!(matches!(
                        action,
                        Ok(InitialDatabaseAction::OpenCurrent
                            | InitialDatabaseAction::RecoveryOnly)
                    ));
                    continue;
                }
                // The pass restarts from its beginning against the current
                // database; it neither consults nor requires a source identity.
                assert!(matches!(
                    action,
                    Ok(InitialDatabaseAction::ResumeCurrent {
                        receipt: ResumableMaintenanceReceipt::Scrub(ref resumed),
                        request: MaintenanceDriverRequest::StorageScrub { operation_id },
                        validate_current_source: false,
                    }) if **resumed == receipt && operation_id == receipt.operation_id()
                ));
            }
        }
    }
}
