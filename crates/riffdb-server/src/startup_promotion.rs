//! Join an exactly reconciled cutover to the ordinary server startup proof.
use super::*;
use riffdb_storage_api::{ChangelogPublicationPort, StoredPromotionAdministrationV1};
use riffdb_storage_redb::RedbMaintenanceStorage;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

/// Resumes committed authority only. The owner remains exclusively held and no
/// peer, clock, fresh identifier, or new cutover is selected by this path.
pub(crate) fn reconcile_promoted_redb_startup(
    owner: &mut RedbMaintenanceStorage,
    record: &StoredPromotionAdministrationV1,
    inputs: StartupValidationInputs,
    cancellation: Arc<AtomicBool>,
    profile: RedbCommitProfile,
) -> Result<CheckedRedbStartup, RedbStartupError> {
    let (publication, reader) = crate::replication_publication::ReplicationPublication::channel();
    let store = timed(StartupStage::StoreOpen, || {
        owner.reconcile_committed_promotion(
            record,
            inputs.clone(),
            Arc::clone(&cancellation),
            profile,
            publication.clone(),
        )
    })?;
    let mut checked =
        complete_promoted_redb_startup(store, inputs, cancellation, publication, reader)?;
    checked.promotion = Some(record.clone());
    Ok(checked)
}

fn complete_promoted_redb_startup(
    store: RedbStore,
    inputs: StartupValidationInputs,
    cancellation: Arc<AtomicBool>,
    publication: Arc<crate::replication_publication::ReplicationPublication>,
    reader: crate::replication_publication::ReplicationPublishedSnapshots,
) -> Result<CheckedRedbStartup, RedbStartupError> {
    check_cancellation(&cancellation)?;
    let initialized = match DatabaseInitializationExecutor::new(store).probe()? {
        DatabaseInitializationDecision::Existing(initialized) => initialized,
        DatabaseInitializationDecision::NeedsInitialization(_) => {
            return Err(RedbStartupError::Integrity(
                StartupIntegrityFailure::InitializationIdentityMismatch,
            ));
        }
    };
    let mut checked = complete_initialized_redb_startup(initialized, inputs)?;
    check_cancellation(&cancellation)?;
    publication.observe_published_snapshot_v3(
        checked
            .operational_ports
            .published_changelog_snapshot_v3()?,
    );
    checked.replication_publications = Some(reader);
    Ok(checked)
}

/// A checked restore supersedes the old promotion anchor while its original
/// external audit remains retained. Storage rejoins that history before the
/// ordinary catalog proof and publication channel become operational.
pub(crate) fn reconcile_restored_redb_startup(
    owner: &mut RedbMaintenanceStorage,
    attempt: &riffdb_storage_api::ReplicationPromotionReceiptV1,
    inputs: StartupValidationInputs,
    cancellation: Arc<AtomicBool>,
    profile: RedbCommitProfile,
) -> Result<CheckedRedbStartup, RedbStartupError> {
    let (publication, reader) = crate::replication_publication::ReplicationPublication::channel();
    let store = timed(StartupStage::StoreOpen, || {
        owner.reconcile_restored_promotion(
            attempt,
            inputs.clone(),
            Arc::clone(&cancellation),
            profile,
            publication.clone(),
        )
    })?;
    complete_promoted_redb_startup(store, inputs, cancellation, publication, reader)
}

fn check_cancellation(cancellation: &AtomicBool) -> Result<(), RedbStartupError> {
    if cancellation.load(Ordering::Acquire) {
        Err(RedbStartupError::Storage(StorageError::new(
            riffdb_storage_api::StorageErrorKind::Unavailable,
            None,
        )))
    } else {
        Ok(())
    }
}

#[cfg(test)]
#[path = "startup_promotion_tests.rs"]
pub(crate) mod tests;
