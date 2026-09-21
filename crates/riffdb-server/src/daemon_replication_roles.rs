//! Local role recovery precedes any connection to a configured former source.
use super::*;
use crate::config::FollowerSourceConfig;
use crate::startup::CheckedRedbStartup;
use riffdb_storage_redb::RedbCommitProfile;

pub(super) struct PreparedMaintenanceStartup {
    pub(super) owner: RedbMaintenanceStorage,
    pub(super) reconciliation: RedbMaintenanceReconciliation,
    pub(super) promoted: Option<CheckedRedbStartup>,
}

pub(super) enum PreparedReplicationRole {
    Ready(Box<PreparedMaintenanceStartup>),
    Retry(Box<PendingPromotionStartup>),
}

pub(super) struct PendingPromotionStartup {
    pub(super) owner: RedbMaintenanceStorage,
    pub(super) request: riffdb_storage_api::ReplicationPromotionRequestV1,
}

pub(super) fn prepare(
    path: &Path,
    backup_root: &Path,
    source: Option<&FollowerSourceConfig>,
    inputs: StartupValidationInputs,
    profile: RedbCommitProfile,
) -> Result<PreparedMaintenanceStartup, DaemonError> {
    match prepare_role(path, backup_root, source, inputs, profile)? {
        PreparedReplicationRole::Ready(prepared) => Ok(*prepared),
        PreparedReplicationRole::Retry(_) => Err(DaemonError::MaintenanceDriver),
    }
}

pub(super) fn prepare_role(
    path: &Path,
    backup_root: &Path,
    source: Option<&FollowerSourceConfig>,
    inputs: StartupValidationInputs,
    profile: RedbCommitProfile,
) -> Result<PreparedReplicationRole, DaemonError> {
    let mut owner = RedbMaintenanceStorage::open_for_promotion_recovery(path, backup_root)
        .map_err(DaemonError::MaintenanceStorage)?;
    let promotions = owner
        .promotion_receipts()
        .map_err(DaemonError::MaintenanceStorage)?;
    let needs_reconciliation = promotions.receipts().iter().any(|receipt| {
        !receipt.is_terminal()
            || receipt.phase() == riffdb_storage_api::ReplicationPromotionPhaseV1::Succeeded
            || receipt.selection().is_some()
    });
    // Ordinary source startup keeps its existing format/clean-start path when
    // there is no promotion to reconcile. A configured follower must inspect an
    // existing local file even when its external ledger has been lost.
    let inspect_local = if source.is_some() {
        match fs::symlink_metadata(owner.configured_database_file()) {
            Ok(metadata) if metadata.is_file() => true,
            Err(error) if error.kind() == io::ErrorKind::NotFound => needs_reconciliation,
            _ => {
                return Err(DaemonError::MaintenanceStorage(StorageError::new(
                    StorageErrorKind::InvariantViolation,
                    None,
                )));
            }
        }
    } else {
        needs_reconciliation
    };
    let record = if inspect_local {
        owner
            .discover_committed_promotion()
            .map_err(DaemonError::MaintenanceStorage)?
    } else {
        None
    };
    let promoted = match record {
        Some(record) => {
            let selection = record
                .attempt()
                .selection()
                .ok_or(DaemonError::MaintenanceDriver)?;
            if source.is_some_and(|source| {
                source.lineage != selection.evidence().fence().lineage()
                    || source.hold != record.attempt().request().target().hold_id()
            }) {
                return Err(DaemonError::MaintenanceDriver);
            }
            Some(
                crate::startup::promotion::reconcile_promoted_redb_startup(
                    &mut owner,
                    &record,
                    inputs,
                    Arc::new(std::sync::atomic::AtomicBool::new(false)),
                    profile,
                )
                .map_err(DaemonError::Startup)?,
            )
        }
        None if needs_reconciliation => {
            if !promotions
                .receipts()
                .iter()
                .any(|r| r.phase() == riffdb_storage_api::ReplicationPromotionPhaseV1::Succeeded)
            {
                let source = source.ok_or(DaemonError::MaintenanceDriver)?;
                let request = owner
                    .pending_promotion_request()
                    .map_err(DaemonError::MaintenanceStorage)?
                    .ok_or(DaemonError::MaintenanceDriver)?;
                let target = request.target();
                if target.database_id() != source.lineage.database_id()
                    || target.history_incarnation() != source.lineage.history_incarnation()
                    || target.leadership_epoch() != source.lineage.leadership_epoch()
                    || target.hold_id() != source.hold
                {
                    return Err(DaemonError::MaintenanceDriver);
                }
                return Ok(PreparedReplicationRole::Retry(Box::new(
                    PendingPromotionStartup { owner, request },
                )));
            }
            let attempt = promotions
                .receipts()
                .iter()
                .find(|r| r.phase() == riffdb_storage_api::ReplicationPromotionPhaseV1::Succeeded)
                .ok_or(DaemonError::MaintenanceDriver)?;
            let selection = attempt.selection().ok_or(DaemonError::MaintenanceDriver)?;
            if source.is_some_and(|source| {
                source.lineage != selection.evidence().fence().lineage()
                    || source.hold != attempt.request().target().hold_id()
            }) {
                return Err(DaemonError::MaintenanceDriver);
            }
            Some(
                crate::startup::promotion::reconcile_restored_redb_startup(
                    &mut owner,
                    attempt,
                    inputs,
                    Arc::new(std::sync::atomic::AtomicBool::new(false)),
                    profile,
                )
                .map_err(DaemonError::Startup)?,
            )
        }
        None => None,
    };
    // None is never permission to ignore a pending or unmatched success receipt.
    // The same exclusive owner validates all ordinary and migration inventory.
    let reconciliation = owner
        .reconcile_for_startup()
        .map_err(DaemonError::MaintenanceStorage)?;
    Ok(PreparedReplicationRole::Ready(Box::new(
        PreparedMaintenanceStartup {
            owner,
            reconciliation,
            promoted,
        },
    )))
}

#[cfg(test)]
#[path = "daemon_replication_roles_tests.rs"]
mod tests;
