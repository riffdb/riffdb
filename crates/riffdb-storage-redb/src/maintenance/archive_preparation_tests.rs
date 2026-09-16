//! Empty selected suffixes retain the verified backup boundary.
// req: REP-007, AFC-007
use super::*;
use std::sync::Arc;

#[test]
fn archive_preparation_empty_suffix_keeps_the_backup_frontier_and_selection() {
    let scope = crate::test_path::ScopedDirectory::new("archive-empty-preparation");
    let (backup, history) = crate::maintenance::archive_backup_tests::backup(&scope);
    let repository = crate::RedbVerifiedArchiveBackup::open(&backup)
        .unwrap()
        .open_archive(
            &scope.join("archive"),
            ArchiveEncryptionPostureV1::Unencrypted,
        )
        .unwrap();
    let stage = materialize(&scope, &backup, "stage")
        .begin_archive_replay(repository, &AtomicBool::new(false))
        .unwrap();
    let candidate = stage
        .prepare_restore(
            riffdb_types::ArchiveRestoreStopV1::LastArchived,
            inputs(),
            Arc::new(AtomicBool::new(false)),
        )
        .unwrap();
    assert_eq!(candidate.restored_frontier(), history.tail().frontier());
    assert!(matches!(
        candidate.selection().suffix(),
        ArchiveRestoreSuffixV3::Empty
    ));
    let snapshot = candidate.authorization_snapshot().unwrap();
    assert_eq!(
        snapshot.application_frontier().unwrap(),
        history.tail().frontier().application()
    );
    drop(snapshot);
    let seal = candidate
        .seal_after_authorization(history.lineage().database_id())
        .unwrap();
    assert_eq!(seal.restored_frontier(), history.tail().frontier());
    drop(seal);
    assert!(!scope.join("configured.redb").exists());
    assert!(!scope.join("stage").exists());
}
