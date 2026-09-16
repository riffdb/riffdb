//! V3 receipt-gated archive publication and process recovery.
// req: REP-007, AFC-007
use super::*;

#[test]
fn archive_publication_requires_durable_exact_receipt_and_preserves_original_manifest() {
    use redb::ReadableDatabase;
    use riffdb_types::{
        ActorId, ActorKind, ArchiveNameV1, ArchiveRestoreStopV1, CapabilityId,
        OfflineMaintenanceReplacementConfirmation, archive_restore_input_hash,
    };
    for mode in [
        "no-receipt",
        "no-incarnation",
        "wrong-selection",
        "changed-seal",
        "nonempty-target",
        "publish",
    ] {
        let scope = crate::test_path::ScopedDirectory::new("archive-publication");
        let (backup, _) = crate::maintenance::archive_backup_tests::backup(&scope);
        let backups = scope.join("backups");
        fs::create_dir(&backups).unwrap();
        let named = backups.join("baseline");
        fs::rename(backup, &named).unwrap();
        let (repository, last) = archive(&scope, &named);
        let target = scope.join("configured.redb");
        let (mut storage, _) = crate::RedbMaintenanceStorage::open(&target, &backups).unwrap();
        let id = OfflineMaintenanceOperationId::from_unix_milliseconds_and_random(1000, [31; 10])
            .unwrap();
        let name = BackupNameV1::new("baseline").unwrap();
        let archive_name = ArchiveNameV1::new("daily").unwrap();
        let stage = storage
            .stage_recovery_restore_candidate(id, &name, inputs())
            .unwrap();
        let manifest = stage.manifest().clone();
        let backup_bytes = fs::read(named.join(crate::backup::MANIFEST_FILE_NAME)).unwrap();
        let stage = stage
            .begin_archive_replay(repository, &AtomicBool::new(false))
            .unwrap();
        let applier = validated_applier(stage.staged_database_file());
        let replayed = stage.replay(applier, &AtomicBool::new(false)).unwrap();
        let selection = replayed.selection().clone();
        let frontier = last.frontier();
        let stop = ArchiveRestoreStopV1::LastArchived;
        let confirmation = OfflineMaintenanceReplacementConfirmation::NotProvided;
        let mut receipt = OfflineMaintenanceReceiptV3::accepted_archive_restore(
            id,
            name.clone(),
            archive_name.clone(),
            stop,
            archive_restore_input_hash(&name, &archive_name, stop, confirmation),
            confirmation,
            OfflineMaintenanceAdmissionV1::new(
                ActorId::new("operator").unwrap(),
                ActorKind::Human,
                CapabilityId::from_unix_milliseconds_and_random(1000, [32; 10]).unwrap(),
                None,
            ),
            None,
        )
        .unwrap();
        let recorded_selection = if mode == "wrong-selection" {
            ArchiveRestoreSelectionV3::new(
                selection.backup().clone(),
                selection.lineage(),
                selection.backup_fence(),
                ArchiveRestoreSuffixV3::Empty,
            )
            .unwrap()
        } else {
            selection.clone()
        };
        receipt.record_selection(recorded_selection).unwrap();
        receipt
            .record_validated_restore(manifest.database_id(), frontier)
            .unwrap();
        let stage_path = replayed.staged_database_file().to_path_buf();
        let sealed = replayed
            .seal_after_validation(manifest.database_id(), stop, inputs())
            .unwrap();
        if mode != "no-receipt" {
            storage.create_or_read_archive_receipt(&receipt).unwrap();
            receipt
                .advance(OfflineMaintenanceReceiptTransitionV1::phase(
                    OfflineMaintenanceReceiptPhaseV1::Offline,
                ))
                .unwrap();
            if mode != "no-incarnation" {
                receipt.record_published_incarnation(2).unwrap();
            }
            storage.replace_archive_receipt(&receipt).unwrap();
        }
        if mode == "changed-seal" {
            fs::write(&stage_path, b"changed after validation").unwrap();
        }
        if mode == "nonempty-target" {
            fs::write(&target, b"nonempty target preserved").unwrap();
        }
        if mode == "publish" {
            let result = storage.publish_sealed_archive_restore(sealed).unwrap();
            assert!(
                matches!(result,OfflineArchiveRestorePublicationV3::Published {backup_manifest,restored_frontier,published_history_incarnation:2} if *backup_manifest==manifest && restored_frontier==frontier)
            );
            assert_eq!(
                crate::backup::read_history_incarnation(&target).unwrap(),
                Some(2)
            );
            crate::startup::RedbOfflineIntegrityScrub::from_inputs(&target, inputs())
                .run()
                .unwrap();
            let store = crate::RedbStore::open(&target).unwrap();
            let read = store.shared.database.begin_read().unwrap();
            let history = crate::changelog_v3_roots::validate_retained_history(&read)
                .unwrap()
                .unwrap();
            assert_eq!(history.lineage().history_incarnation(), 2);
            assert_eq!(history.tail().frontier(), frontier);
            assert!(!crate::follower_lifecycle::is_attached(&read).unwrap());
        } else if mode == "nonempty-target" {
            assert!(matches!(
                storage.publish_sealed_archive_restore(sealed).unwrap(),
                OfflineArchiveRestorePublicationV3::TargetNotEmpty
            ));
            assert_eq!(fs::read(&target).unwrap(), b"nonempty target preserved");
        } else {
            assert!(storage.publish_sealed_archive_restore(sealed).is_err());
            assert!(!target.exists());
        }
        assert_eq!(
            fs::read(named.join(crate::backup::MANIFEST_FILE_NAME)).unwrap(),
            backup_bytes
        );
        assert_eq!(
            crate::RedbVerifiedArchiveBackup::open(&named)
                .unwrap()
                .manifest(),
            &manifest
        );
    }
}

fn publication_id() -> OfflineMaintenanceOperationId {
    OfflineMaintenanceOperationId::from_unix_milliseconds_and_random(1000, [41; 10]).unwrap()
}
fn create_admitted(scope: &crate::test_path::ScopedDirectory) -> OfflineMaintenanceReceiptV3 {
    use riffdb_types::*;
    let (backup, _) = crate::maintenance::archive_backup_tests::backup(scope);
    let backups = scope.join("backups");
    fs::create_dir(&backups).unwrap();
    let named = backups.join("baseline");
    fs::rename(backup, &named).unwrap();
    let (repository, last) = archive(scope, &named);
    let (mut store, _) =
        crate::RedbMaintenanceStorage::open(scope.join("configured.redb"), &backups).unwrap();
    let name = BackupNameV1::new("baseline").unwrap();
    let archive_name = ArchiveNameV1::new("daily").unwrap();
    let stop = ArchiveRestoreStopV1::LastArchived;
    let confirmation = OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget;
    let stage = store
        .stage_recovery_restore_candidate(publication_id(), &name, inputs())
        .unwrap();
    let stage = stage
        .begin_archive_replay(repository, &AtomicBool::new(false))
        .unwrap();
    let applier = validated_applier(stage.staged_database_file());
    let replayed = stage.replay(applier, &AtomicBool::new(false)).unwrap();
    let mut receipt = OfflineMaintenanceReceiptV3::accepted_archive_restore(
        publication_id(),
        name.clone(),
        archive_name.clone(),
        stop,
        archive_restore_input_hash(&name, &archive_name, stop, confirmation),
        confirmation,
        OfflineMaintenanceAdmissionV1::new(
            ActorId::new("operator").unwrap(),
            ActorKind::Human,
            CapabilityId::from_unix_milliseconds_and_random(1000, [42; 10]).unwrap(),
            None,
        ),
        None,
    )
    .unwrap();
    receipt
        .record_selection(replayed.selection().clone())
        .unwrap();
    receipt
        .record_validated_restore(replayed.history().lineage().database_id(), last.frontier())
        .unwrap();
    store.create_or_read_archive_receipt(&receipt).unwrap();
    receipt
        .advance(OfflineMaintenanceReceiptTransitionV1::phase(
            OfflineMaintenanceReceiptPhaseV1::Offline,
        ))
        .unwrap();
    receipt.record_published_incarnation(2).unwrap();
    store.replace_archive_receipt(&receipt).unwrap();
    replayed.discard().unwrap();
    receipt
}
fn rebuild(root: &Path, store: &mut crate::RedbMaintenanceStorage) -> RedbSealedArchiveRestore {
    let receipt = store
        .read_archive_receipt(publication_id())
        .unwrap()
        .unwrap();
    let binding = crate::RedbVerifiedArchiveBackup::open(&root.join("backups/baseline")).unwrap();
    let repository = binding
        .open_archive(
            &root.join("archive"),
            ArchiveEncryptionPostureV1::Unencrypted,
        )
        .unwrap();
    let stage = store
        .stage_archive_restore(publication_id(), inputs())
        .unwrap();
    let stage = stage
        .begin_selected_archive_replay(
            repository,
            receipt.selection().unwrap().clone(),
            &AtomicBool::new(false),
        )
        .unwrap();
    let applier = validated_applier(stage.staged_database_file());
    let replayed = stage.replay(applier, &AtomicBool::new(false)).unwrap();
    replayed
        .seal_after_validation(
            receipt.staged_database_id().unwrap(),
            receipt.stop(),
            inputs(),
        )
        .unwrap()
}

#[test]
fn archive_publication_crash_child() {
    let Some(root) = std::env::var_os("RIFFDB_ARCHIVE_PUBLICATION_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let mode = std::env::var("RIFFDB_ARCHIVE_PUBLICATION_MODE").unwrap();
    let point = match mode.as_str() {
        "before-stamp" => RedbMaintenanceFailpoint::BetweenReceiptWriteAndStagedStamp,
        "before-publish" => RedbMaintenanceFailpoint::BeforeTargetPublication,
        "after-publish" => RedbMaintenanceFailpoint::AfterTargetPublication,
        "after-sync" => RedbMaintenanceFailpoint::AfterTargetParentSync,
        _ => RedbMaintenanceFailpoint::AfterRetirementDeleteParentSync,
    };
    let controller = RedbMaintenanceTestController::abort_at(point);
    let (mut store, _) = crate::RedbMaintenanceStorage::open_with_test_controller(
        root.join("configured.redb"),
        root.join("backups"),
        controller,
    )
    .unwrap();
    let sealed = rebuild(&root, &mut store);
    store.publish_sealed_archive_restore(sealed).unwrap();
    panic!("publication crash was not reached");
}

fn crash_and_recover(mode: &str) {
    #[cfg(unix)]
    use std::os::unix::process::ExitStatusExt;
    use std::process::{Command, Stdio};
    eprintln!("archive publication crash boundary: {mode}");
    let scope = crate::test_path::ScopedDirectory::new("archive-publish-crash");
    let receipt = create_admitted(&scope);
    let root = scope.join("");
    let status = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "maintenance::staged::archive_tests::publication::archive_publication_crash_child",
            "--nocapture",
        ])
        .env("RIFFDB_ARCHIVE_PUBLICATION_ROOT", &root)
        .env("RIFFDB_ARCHIVE_PUBLICATION_MODE", mode)
        .env("RIFFDB_ARCHIVE_REPLAY_CRASH", mode)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .unwrap();
    if mode.starts_with("restore-") {
        assert_eq!(status.code(), Some(98));
    } else {
        #[cfg(unix)]
        assert_eq!(status.signal(), Some(6));
        #[cfg(not(unix))]
        assert!(!status.success());
    }
    let target = root.join("configured.redb");
    assert_eq!(
        target.exists(),
        matches!(mode, "after-publish" | "after-sync")
    );
    let (mut store, reconciliation) =
        crate::RedbMaintenanceStorage::open(&target, root.join("backups")).unwrap();
    assert_eq!(
        reconciliation.archive_receipts().receipts(),
        std::slice::from_ref(&receipt)
    );
    if mode == "before-publish" {
        assert_eq!(reconciliation.removed_unpublished_target_temps(), 3);
    }

    let sealed = rebuild(&root, &mut store);
    assert!(matches!(
        store.publish_sealed_archive_restore(sealed).unwrap(),
        OfflineArchiveRestorePublicationV3::Published {
            published_history_incarnation: 2,
            ..
        }
    ));
    assert_eq!(
        store.read_archive_receipt(publication_id()).unwrap(),
        Some(receipt)
    );
    assert_eq!(
        crate::backup::read_history_incarnation(&target).unwrap(),
        Some(2)
    );
    crate::startup::RedbOfflineIntegrityScrub::from_inputs(&target, inputs())
        .run()
        .unwrap();
}

#[test]
fn archive_publication_recovers_before_stamp() {
    crash_and_recover("before-stamp");
}

#[test]
fn archive_publication_recovers_after_incarnation_stamp() {
    crash_and_recover("restore-incarnation-stamped");
}

#[test]
fn archive_publication_recovers_after_journal_creation() {
    crash_and_recover("restore-journal-created");
}

#[test]
fn archive_publication_recovers_after_source_validation() {
    crash_and_recover("restore-source-validated");
}

#[test]
fn archive_publication_recovers_before_target_publication() {
    crash_and_recover("before-publish");
}

#[test]
fn archive_publication_recovers_after_target_publication() {
    crash_and_recover("after-publish");
}

#[test]
fn archive_publication_recovers_after_parent_sync() {
    crash_and_recover("after-sync");
}

#[test]
fn archive_publication_reconciliation_requires_exact_retained_stage_and_target_bytes() {
    let scope = crate::test_path::ScopedDirectory::new("archive-reconcile");
    create_admitted(&scope);
    let target = scope.join("configured.redb");
    let (mut store, _) =
        crate::RedbMaintenanceStorage::open(&target, scope.join("backups")).unwrap();
    assert!(
        !store
            .archive_restore_target_matches(publication_id())
            .unwrap()
    );
    let sealed = rebuild(&scope.join(""), &mut store);
    assert!(
        !store
            .archive_restore_target_matches(publication_id())
            .unwrap()
    );
    store.publish_sealed_archive_restore(sealed).unwrap();
    assert!(
        store
            .archive_restore_target_matches(publication_id())
            .unwrap()
    );
    // Valid retained identity/frontiers cannot substitute for exact bytes.
    let marker = crate::durable_format_marker_path(&target);
    let marker_bytes = fs::read(&marker).unwrap();
    fs::write(&marker, b"incomplete-marker").unwrap();
    assert!(
        !store
            .archive_restore_target_matches(publication_id())
            .unwrap()
    );
    fs::write(&marker, marker_bytes).unwrap();
    let journal = crate::journal::journal_path(&target);
    let journal_bytes = fs::read(&journal).unwrap();
    fs::write(&journal, b"incomplete-journal").unwrap();
    assert!(
        !store
            .archive_restore_target_matches(publication_id())
            .unwrap()
    );
    fs::write(&journal, journal_bytes).unwrap();
    assert!(
        store
            .archive_restore_target_matches(publication_id())
            .unwrap()
    );
    fs::remove_file(marker).unwrap();
    assert!(
        !store
            .archive_restore_target_matches(publication_id())
            .unwrap()
    );
}
