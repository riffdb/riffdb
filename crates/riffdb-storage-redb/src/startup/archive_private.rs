//! Complete offline validation for a typed, quarantined reconstruction.
use super::*;
use crate::maintenance::PrivateArchiveValidationBinding;

pub(crate) fn validate_private_archive(
    path: &Path,
    file: &std::fs::File,
    binding: PrivateArchiveValidationBinding,
    inputs: StartupValidationInputs,
    cancellation: Arc<AtomicBool>,
) -> Result<(), StorageError> {
    if cancellation.load(Ordering::Acquire) {
        return Err(storage_error(StorageErrorKind::Unavailable));
    }
    let store = RedbStore::open_private_restore(path, file, binding)?;
    let read = store
        .shared
        .database
        .begin_read()
        .map_err(transaction_error)?;
    binding.validate(&read)?;
    super::archive_private_graph::validate(&read, &cancellation)?;
    drop(read);
    let mut session =
        store.begin_structural_evidence_for(inputs, EvidenceOpenPurpose::OfflineIntegrityScrub)?;
    session.set_cancellation(cancellation);
    RedbOfflineIntegrityScrub::validate_session(session)?;
    Ok(())
}
