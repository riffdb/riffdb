//! Single private acquisition path for the actual catalog-cut predecessor.
//! Current Source factory/write/publication proof is still a later phase.
#![cfg_attr(not(test), allow(dead_code))]
use super::*;

// This owner is meaningful only with the closed authorizing-work constructor
// graph: archive workers retain the actual EngineOwner (qualified1975), live
// bootstrap/acknowledgement/Original owners retain actual primary custody, and
// old services follow their required joined offline-shutdown protocol. A bare
// maintenance observation cannot be converted into this owner.
pub(crate) struct CatalogCutPredecessorV1 {
    predecessor: MaintenanceValidatedPredecessorV1,
}

impl CatalogCutPredecessorV1 {
    pub(crate) fn acquire(
        source: &Path,
        backup: &Path,
        inputs: StartupValidationInputs,
        cancellation: Arc<AtomicBool>,
    ) -> Result<Self, StorageError> {
        check_cancelled(&cancellation)?;
        // The existing artifact factory takes actual primary native exclusion
        // BEFORE marker/backup reads. Any live supported authorizing-work owner
        // holds that exact engine/custody, so it must refuse here before Accept.
        let maintenance = OfflinePredecessorMaintenanceV1::acquire(
            source,
            backup,
            inputs,
            Arc::clone(&cancellation),
        )?;
        let predecessor = maintenance.validate_complete_maintenance()?;
        check_cancelled(&cancellation)?;
        // Complete original validation includes exact V2 layout/absence,
        // original lineage/admission, D7 and every original source-hold release.
        // Keep its actual owner, not copied census/IDs or a separate digest.
        predecessor.artifacts.verify()?;
        check_cancelled(&cancellation)?;
        Ok(Self { predecessor })
    }

    // Fixed Source-phase/cut consumers must use this actual retained owner.
    // This does not construct a Source reader, grant operational authority or
    // stage an Accepted receipt. All remaining phase proofs are still required.
    pub(super) fn verify_retained_custody(&self) -> Result<(), StorageError> {
        let cancellation = self.predecessor.artifacts.cancellation();
        check_cancelled(&cancellation)?;
        self.predecessor.artifacts.verify()?;
        check_cancelled(&cancellation)
    }
}

fn check_cancelled(cancellation: &AtomicBool) -> Result<(), StorageError> {
    if cancellation.load(Ordering::Acquire) {
        return Err(storage_error(StorageErrorKind::Unavailable));
    }
    Ok(())
}

#[cfg(test)]
#[path = "cut_predecessor_entry_tests.rs"]
mod tests;
