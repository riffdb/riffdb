//! Complete logical evidence is tied to its purpose and exact immutable pin.
// req: REC-001, REC-006, END-001

use super::*;

fn completed_session(
    path: &TestDatabasePath,
    purpose: EvidenceOpenPurpose,
) -> (
    RedbStructuralEvidenceSession,
    RedbStructuralEvidenceEnd,
    RedbHistoricalEvidenceEnd,
) {
    let store = initialized_store(path, database_id(0x61));
    let mut session = store
        .begin_structural_evidence_for(inputs(), purpose)
        .unwrap();
    let structural = finish_structural(&mut session);
    let validation = validate_catalog_history(&mut session).unwrap();
    let (catalog, historical) = validation.into_parts();
    assert!(matches!(catalog, CatalogHistoryOutcome::Ready(_)));
    (session, structural, historical)
}

#[test]
fn complete_offline_handoff_refuses_an_ordinary_startup_session() {
    let path = TestDatabasePath::new("offline-handoff-purpose");
    let (session, structural, historical) = completed_session(&path, EvidenceOpenPurpose::Startup);
    let result = session.finish_offline_integrity_scrub(structural, historical);
    assert!(
        matches!(result, Err(error) if error.kind() == StorageErrorKind::CorruptData),
        "ordinary startup evidence cannot mint an owned complete-offline handoff"
    );
}

#[test]
fn complete_offline_handoff_refuses_dirty_startup_shortcut_state() {
    let path = TestDatabasePath::new("offline-handoff-dirty-shortcut");
    let (mut session, structural, historical) =
        completed_session(&path, EvidenceOpenPurpose::OfflineIntegrityScrub);
    // Deterministically inject the forbidden completion state. Exact-end tokens
    // cannot turn evidence marked as a dirty-startup shortcut into a full proof.
    session.bounded_dirty_startup = true;
    let result = session.finish_offline_integrity_scrub(structural, historical);
    assert!(
        matches!(result, Err(error) if error.kind() == StorageErrorKind::CorruptData),
        "the complete-offline handoff must explicitly bar dirty-startup shortcuts"
    );
}
