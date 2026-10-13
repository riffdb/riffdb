//! Complete original-artifact reads cannot escape unchecked ownership.
// req: REC-001, REC-004, REC-006, REC-008, END-001, END-008
use super::*;
use riffdb_storage_api::{DatabaseInitializationPort, OfflineBackupPersistencePort};

fn fixture() -> (
    crate::test_path::ScopedDirectory,
    PathBuf,
    OfflineBackupManifestV1,
    PathBuf,
) {
    let root = crate::test_path::ScopedDirectory::new("owned-complete-artifact-read");
    let source = root.join("source.redb");
    let id = DatabaseId::from_unix_milliseconds_and_random(1234, [0x36; 10]).unwrap();
    let mut store = crate::RedbStore::open(&source).unwrap();
    store.initialize_database(id).unwrap();
    drop(store);
    let journal = crate::journal::journal_path(&source);
    crate::journal::reset_journal(
        &journal,
        &crate::journal::JournalFileHeader::new(id, None, [0; 32]),
    )
    .unwrap();
    fs::rename(&journal, crate::journal::sealing_journal_path(&source)).unwrap();
    crate::journal::seal_extent_with_media(&RealJournalMedia, &source, id)
        .unwrap()
        .unwrap();
    crate::journal::recreate_missing_active_with_media(&RealJournalMedia, &source).unwrap();
    let backup = root.join("backup");
    let manifest = crate::RedbOfflineBackup::bind(&source, &backup)
        .create_offline_backup(
            &BackupBuildMetadataV1::new("0.1.0", "abc", "rustc", 1, vec![]).unwrap(),
        )
        .unwrap();
    let extent = sealed_artifacts_in(&backup).unwrap().pop().unwrap().1;
    (root, backup, manifest, extent)
}

#[test]
fn complete_artifact_read_refuses_mutation_replacement_or_cancellation_after_the_parser() {
    for mode in 0..3 {
        let (root, backup, manifest, extent) = fixture();
        let before = fs::read(&extent).unwrap();
        let m = fs::metadata(&extent).unwrap();
        let cancellation = Arc::new(AtomicBool::new(false));
        let target = extent.clone();
        let saved = before.clone();
        let original = root.join("original-extent");
        let cancelled = Arc::clone(&cancellation);
        COMPLETE_ARTIFACT_READ_HOOK.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || match mode {
                0 => {
                    let file = OpenOptions::new().write(true).open(&target).unwrap();
                    file.write_all_at(&[saved[0] ^ 1], 0).unwrap();
                    file.write_all_at(&saved[..1], 0).unwrap();
                    file.set_modified(m.modified().unwrap()).unwrap();
                    file.sync_all().unwrap();
                }
                1 => {
                    fs::rename(&target, &original).unwrap();
                    fs::write(&target, &saved).unwrap();
                }
                _ => cancelled.store(true, Ordering::Release),
            }))
        });
        assert!(
            OwnedBackupHistory::acquire(&backup, &manifest, cancellation).is_err(),
            "completed frame parsing cannot authorize changed original custody: {mode}"
        );
        assert_eq!(fs::read(&extent).unwrap(), before);
        assert!(COMPLETE_ARTIFACT_READ_HOOK.with(|hook| hook.borrow().is_none()));
    }
}

#[test]
fn complete_original_artifact_validation_checks_footer_generation_beyond_a_valid_checksum() {
    let (_root, backup, manifest, extent) = fixture();
    let generation = sealed_artifact_generation(extent.file_name().unwrap()).unwrap();
    fs::rename(&extent, backup.join(sealed_artifact_name(generation + 1))).unwrap();
    let error = OwnedBackupHistory::acquire(&backup, &manifest, Arc::new(AtomicBool::new(false)))
        .unwrap_err();
    assert_eq!(error.kind(), StorageErrorKind::CorruptData);
}

#[test]
fn borrowed_history_resolves_anchored_active_frames_after_closed_sealed_absence() {
    use crate::journal::{
        self, JournalFileHeader, JournalFrame, JournalLane, JournalMutation, JournalTable,
    };
    let (root, backup, manifest, _) = fixture();
    let owner =
        OwnedBackupHistory::acquire(&backup, &manifest, Arc::new(AtomicBool::new(false))).unwrap();
    let stage = root.join("stage");
    fs::create_dir(&stage).unwrap();
    let database = stage.join("database.redb");
    let active = journal::journal_path(&database);
    let header = JournalFileHeader::new(manifest.database_id(), None, [0; 32]);
    journal::reset_journal(&active, &header).unwrap();
    journal::renew_extent_above_with_media(
        &RealJournalMedia,
        &active,
        owner
            .artifacts
            .iter()
            .filter_map(|a| a.generation)
            .max()
            .unwrap(),
    )
    .unwrap();
    let lane = JournalLane::open(&active, &header).unwrap();
    let generation = lane.generation();
    let sequence = CommitSequence::first();
    let frame = JournalFrame::new(
        manifest.database_id(),
        sequence,
        sequence,
        1,
        [0; 32],
        vec![
            JournalMutation::put(
                JournalTable::Commits,
                crate::keys::encode_application_sequence_key(sequence).to_vec(),
                b"checked active frame payload".to_vec(),
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let fence = lane
        .submit(frame.encode().unwrap())
        .unwrap()
        .wait()
        .unwrap();
    drop(lane);
    let mut placement = None;
    journal::scan_located_journal_with_media(
        &RealJournalMedia,
        &active,
        manifest.database_id(),
        |_, p| {
            placement = p.map(|locator| locator.placement);
            Ok(())
        },
    )
    .unwrap();
    journal::advance_journal_anchor_with_media(
        &RealJournalMedia,
        &active,
        &JournalFileHeader::new(manifest.database_id(), Some(sequence), fence.frame_hash),
    )
    .unwrap();
    let media = BorrowedHistoryMedia::new(
        Arc::clone(&owner),
        database.clone(),
        Arc::new(PinnedDirectory::open(&stage).unwrap()),
    );
    let resolved = journal::read_retained_frame_with_media(
        &media,
        &database,
        manifest.database_id(),
        placement.unwrap(),
        generation,
    )
    .expect("closed sealed absence must permit a checked frame in the copied active journal");
    assert_eq!(resolved, frame);
    let absent = journal::sealed_journal_path(&database, generation);
    assert!(!media.try_exists(&absent).unwrap());
    assert_eq!(
        media.open_read(&absent).unwrap_err().kind(),
        io::ErrorKind::NotFound
    );
    assert_eq!(
        media.metadata(&absent).unwrap_err().kind(),
        io::ErrorKind::NotFound
    );
    assert!(media.open_read_write(&absent).is_err());
    assert!(media.create_new_read_write(&absent).is_err());
    fs::write(&absent, b"redirected source").unwrap();
    assert!(media.open_read(&absent).is_err());
    assert!(
        media.open_read(&active).is_err(),
        "a shadowing extent invalidates the source owner"
    );
}

#[test]
fn private_journal_reads_reuse_closed_inventory_and_refuse_namespace_change() {
    let (root, backup, manifest, _) = fixture();
    let owner =
        OwnedBackupHistory::acquire(&backup, &manifest, Arc::new(AtomicBool::new(false))).unwrap();
    let stage = root.join("stage");
    fs::create_dir(&stage).unwrap();
    let database = stage.join("database.redb");
    let active = crate::journal::journal_path(&database);
    fs::write(&active, b"owned private journal").unwrap();
    let absent = crate::journal::sealed_journal_path(&database, 99);
    let media = BorrowedHistoryMedia::new(
        Arc::clone(&owner),
        database,
        Arc::new(PinnedDirectory::open(&stage).unwrap()),
    );
    OWNED_BACKUP_INVENTORY_PASSES.with(|count| count.set(0));
    for _ in 0..5 {
        assert!(!media.try_exists(&absent).unwrap());
        assert_eq!(
            media.open_read(&absent).unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
        assert!(media.try_exists(&active).unwrap());
        assert_eq!(media.metadata(&active).unwrap().len, 21);
        let mut file = media.open_read(&active).unwrap();
        let mut bytes = [0; 21];
        file.read_exact(&mut bytes).unwrap();
        assert_eq!(&bytes, b"owned private journal");
    }
    assert_eq!(
        OWNED_BACKUP_INVENTORY_PASSES.with(|count| count.get()),
        0,
        "unchanged private reads must use their owned closed namespace without backup population walks"
    );
    fs::write(backup.join("unexpected"), b"changed namespace").unwrap();
    assert!(media.open_read(&active).is_err());
    fs::remove_file(backup.join("unexpected")).unwrap();
    assert!(
        media.open_read(&active).is_err(),
        "restoring names cannot launder an invalidated namespace"
    );
}

fn retained_frame_fixture() -> (
    crate::test_path::ScopedDirectory,
    Arc<OwnedBackupHistory>,
    BorrowedHistoryMedia,
    PathBuf,
    crate::journal::FramePlacement,
    u64,
    crate::journal::JournalFrame,
) {
    use crate::journal::{
        self, JournalFileHeader, JournalFrame, JournalLane, JournalMutation, JournalTable,
    };
    let (root, backup, manifest, extent) = fixture();
    let database = root.join("frame.redb");
    let active = journal::journal_path(&database);
    let header = JournalFileHeader::new(manifest.database_id(), None, [0; 32]);
    journal::reset_journal(&active, &header).unwrap();
    let lane = JournalLane::open(&active, &header).unwrap();
    let generation = lane.generation();
    let sequence = CommitSequence::first();
    let frame = JournalFrame::new(
        manifest.database_id(),
        sequence,
        sequence,
        1,
        [0; 32],
        vec![
            JournalMutation::put(
                JournalTable::Commits,
                crate::keys::encode_application_sequence_key(sequence).to_vec(),
                b"owned frame value".to_vec(),
            )
            .unwrap(),
        ],
    )
    .unwrap();
    lane.submit(frame.encode().unwrap())
        .unwrap()
        .wait()
        .unwrap();
    drop(lane);
    let mut placement = None;
    journal::scan_located_journal_with_media(
        &RealJournalMedia,
        &active,
        manifest.database_id(),
        |_, p| {
            placement = p.map(|p| p.placement);
            Ok(())
        },
    )
    .unwrap();
    fs::rename(&active, journal::sealing_journal_path(&database)).unwrap();
    journal::seal_extent_with_media(&RealJournalMedia, &database, manifest.database_id())
        .unwrap()
        .unwrap();
    assert_eq!(
        sealed_artifact_generation(extent.file_name().unwrap()),
        Some(generation)
    );
    fs::copy(journal::sealed_journal_path(&database, generation), &extent).unwrap();
    let checksums = manifest
        .checksums()
        .iter()
        .map(|item| {
            if item.artifact_ordinal().get() == FIRST_SEALED_ARTIFACT_ORDINAL {
                BackupArtifactChecksumV1::new(
                    item.artifact_ordinal(),
                    sha256_file(&extent).unwrap(),
                )
            } else {
                item.clone()
            }
        })
        .collect();
    let updated = OfflineBackupManifestV1::new_with_format_compatibility(
        manifest.storage_format_version(),
        manifest.database_id(),
        manifest.snapshot_kind(),
        manifest.catalog_bundles().to_vec(),
        manifest.active_catalog().cloned(),
        manifest.last_commit_sequence(),
        manifest.history_incarnation(),
        manifest.retention_watermark_sequence(),
        manifest.format_compatibility().unwrap(),
        checksums,
        manifest.build().clone(),
    )
    .unwrap();
    fs::write(
        backup.join(MANIFEST_FILE_NAME),
        encode_manifest(&updated).unwrap(),
    )
    .unwrap();
    let owner =
        OwnedBackupHistory::acquire(&backup, &updated, Arc::new(AtomicBool::new(false))).unwrap();
    let stage = root.join("stage");
    fs::create_dir(&stage).unwrap();
    let private = stage.join("database.redb");
    let media = BorrowedHistoryMedia::new(
        Arc::clone(&owner),
        private.clone(),
        Arc::new(PinnedDirectory::open(&stage).unwrap()),
    );
    (
        root,
        owner,
        media,
        private,
        placement.unwrap(),
        generation,
        frame,
    )
}

#[test]
fn borrowed_retained_frame_uses_one_checked_read_interval() {
    let (_root, owner, media, database, placement, generation, expected) = retained_frame_fixture();
    ORIGINAL_ARTIFACT_READ_CHECKS.with(|count| count.set(0));
    let frame = crate::journal::read_retained_frame_with_media(
        &media,
        &database,
        expected.database_id(),
        placement,
        generation,
    )
    .unwrap();
    assert_eq!(frame, expected);
    assert_eq!(
        ORIGINAL_ARTIFACT_READ_CHECKS.with(|count| count.get()),
        2,
        "one retained frame must check original custody at both read endpoints without repeating pathname walks for each header field"
    );
    owner.verify_full().unwrap();
}

#[test]
fn borrowed_retained_frame_refuses_changed_custody_at_its_decoded_endpoint() {
    for mode in 0..6 {
        let (root, owner, media, database, placement, generation, expected) =
            retained_frame_fixture();
        let artifact = owner
            .artifacts
            .iter()
            .find(|a| a.generation == Some(generation))
            .unwrap();
        let target = owner.path.join(&artifact.name);
        let before = fs::read(&target).unwrap();
        let saved = before.clone();
        let metadata = fs::metadata(&target).unwrap();
        let alias = root.join("original-frame-alias");
        let shadow = crate::journal::sealed_journal_path(&database, generation);
        let extra = owner.path.join("unexpected");
        let cancellation = Arc::clone(&owner.cancelled);
        RETAINED_FRAME_FINISH_HOOK.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || match mode {
                0 => {
                    let file = OpenOptions::new().write(true).open(&target).unwrap();
                    file.write_all_at(&[saved[0] ^ 1], 0).unwrap();
                    file.write_all_at(&saved[..1], 0).unwrap();
                    file.set_modified(metadata.modified().unwrap()).unwrap();
                    file.sync_all().unwrap();
                }
                1 => {
                    fs::rename(&target, &alias).unwrap();
                    fs::write(&target, &saved).unwrap();
                }
                2 => cancellation.store(true, Ordering::Release),
                3 => fs::write(&shadow, &saved).unwrap(),
                4 => fs::hard_link(&target, &alias).unwrap(),
                _ => fs::write(&extra, b"changed inventory").unwrap(),
            }))
        });
        assert!(
            crate::journal::read_retained_frame_with_media(
                &media,
                &database,
                expected.database_id(),
                placement,
                generation
            )
            .is_err(),
            "decoded frames must not escape changed original custody: {mode}"
        );
        assert!(
            RETAINED_FRAME_FINISH_HOOK.with(|hook| hook.borrow().is_none()),
            "the decoder must complete its read boundary before returning"
        );
        assert!(
            owner.verify_full().is_err(),
            "endpoint refusal invalidates future ownership handoffs"
        );
        assert_eq!(fs::read(owner.path.join(&artifact.name)).unwrap(), before);
    }
}

#[test]
fn unfinished_borrowed_frame_read_invalidates_the_owner_before_any_retry() {
    let (_root, owner, media, database, placement, generation, expected) = retained_frame_fixture();
    let mut wrong = placement;
    wrong.frame_hash[0] ^= 1;
    assert!(
        crate::journal::read_retained_frame_with_media(
            &media,
            &database,
            expected.database_id(),
            wrong,
            generation
        )
        .is_err()
    );
    assert!(
        owner.verify_full().is_err(),
        "a failed unfinished frame read cannot retain original authority"
    );
    assert!(
        crate::journal::read_retained_frame_with_media(
            &media,
            &database,
            expected.database_id(),
            placement,
            generation
        )
        .is_err()
    );
}

#[test]
fn another_extent_generation_finishes_its_read_before_trying_another_candidate() {
    let (_root, owner, media, database, placement, generation, expected) = retained_frame_fixture();
    let candidate = crate::journal::sealed_journal_path(&database, generation);
    assert!(
        crate::journal::read_retained_frame_from_with_media(
            &media,
            &[candidate],
            expected.database_id(),
            placement,
            generation + 1
        )
        .is_err()
    );
    owner.verify_full().unwrap();
    assert_eq!(
        crate::journal::read_retained_frame_with_media(
            &media,
            &database,
            expected.database_id(),
            placement,
            generation
        )
        .unwrap(),
        expected
    );
}
