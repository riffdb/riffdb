//! Full ordinary source evidence under a promotion-specific fenced owner.
use super::*;
use crate::maintenance::PromotionValidationBinding;

pub(crate) fn validate_committed_promotion(
    path: &Path,
    file: &std::fs::File,
    binding: Arc<PromotionValidationBinding>,
    inputs: StartupValidationInputs,
    cancellation: Arc<AtomicBool>,
) -> Result<(), StorageError> {
    if cancellation.load(Ordering::Acquire) {
        return Err(storage_error(StorageErrorKind::Unavailable));
    }
    let store = RedbStore::open_private_promotion(path, file, binding)?;
    let mut session =
        store.begin_structural_evidence_for(inputs, EvidenceOpenPurpose::OfflineIntegrityScrub)?;
    session.set_cancellation(cancellation);
    RedbOfflineIntegrityScrub::validate_session(session)?;
    Ok(())
}

/// Even a terminal external phase is not a substitute for current validation.
/// Keep the source private until the full post-recovery snapshot is checked.
pub(crate) fn validate_reconciled_promotion(
    store: RedbStore,
    inputs: StartupValidationInputs,
    cancellation: Arc<AtomicBool>,
) -> Result<RedbStore, StorageError> {
    let shared = Arc::clone(&store.shared);
    let mut session =
        store.begin_structural_evidence_for(inputs, EvidenceOpenPurpose::OfflineIntegrityScrub)?;
    session.set_cancellation(cancellation);
    RedbOfflineIntegrityScrub::validate_session(session)?;
    Ok(RedbStore { shared })
}
