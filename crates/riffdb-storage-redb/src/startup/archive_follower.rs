//! Ordinary structural/catalog proof join for offline archive reconstruction.
//! This releases only a follower applier for the verified predecessor stage.
use super::*;

pub(crate) fn open_validated_archive_follower(
    path: &Path,
    inputs: StartupValidationInputs,
    cancellation: &AtomicBool,
) -> Result<crate::RedbFollowerApplier, StorageError> {
    let cancelled = |flag: &AtomicBool| {
        if flag.load(Ordering::Acquire) {
            Err(storage_error(StorageErrorKind::Unavailable))
        } else {
            Ok(())
        }
    };
    cancelled(cancellation)?;
    let mut session = crate::RedbFollowerStore::open(path)?.begin_structural_evidence(inputs)?;
    let mut cursor =
        StructuralEvidenceCursor::start(session.database_id(), session.open_session_id());
    let end = loop {
        cancelled(cancellation)?;
        match session
            .read_structural_evidence(cursor, EvidencePageLimit::new(64).ok_or_else(corrupt)?)?
        {
            StructuralEvidencePage::Page { findings, next, .. } => {
                if !findings.is_empty() {
                    return Err(corrupt());
                }
                cursor = next;
            }
            StructuralEvidencePage::ExactEnd(end) => break end,
        }
    };
    let (catalog, historical) = validate_catalog_history(&mut session)
        .map_err(|_| corrupt())?
        .into_parts();
    let CatalogHistoryOutcome::Ready(catalog) = catalog else {
        return Err(corrupt());
    };
    cancelled(cancellation)?;
    let StructuralOpenOutcome::Clean(opened) = session.finish(end, historical)? else {
        return Err(corrupt());
    };
    let (database, session_id, _, dormant) = opened.into_parts();
    if !catalog.matches(database, session_id) {
        return Err(corrupt());
    }
    dormant.into_follower_after_catalog_validation()
}
