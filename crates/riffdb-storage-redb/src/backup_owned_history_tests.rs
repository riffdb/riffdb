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

#[test]
fn borrowed_private_history_read_work_scales_with_completed_bounded_intervals() {
    let (_root, owner, media, database, placement, generation, expected) = retained_frame_fixture();
    ORIGINAL_ARTIFACT_READ_CHECKS.with(|count| count.set(0));
    let mut scope = media.begin_owned_restore_reads().unwrap();
    let scoped_media = scope.as_ref().map(|scope| scope.media());
    let source = scoped_media.as_deref().unwrap_or(&media);
    // Cross the frame-count boundary without caching any decoded frame.
    for _ in 0..257 {
        let frame = crate::journal::read_retained_frame_with_media(
            source,
            &database,
            expected.database_id(),
            placement,
            generation,
        )
        .unwrap();
        assert_eq!(frame, expected);
    }
    if let Some(scope) = &mut scope {
        scope.finish().unwrap();
        scope.verify_finished().unwrap();
    }
    assert!(
        ORIGINAL_ARTIFACT_READ_CHECKS.with(|count| count.get()) <= owner.artifacts.len() * 8,
        "bounded private history reads must check original custody at interval boundaries, not around every decoded frame"
    );
    owner.verify_full().unwrap();
}

#[test]
fn bounded_private_reads_refuse_mutated_originals_or_shadow_before_finishing() {
    for mode in 0..7 {
        let (root, owner, media, database, placement, generation, expected) =
            retained_frame_fixture();
        let mut scope = media.begin_owned_restore_reads().unwrap().unwrap();
        let input = scope.media();
        assert_eq!(
            crate::journal::read_retained_frame_with_media(
                input.as_ref(),
                &database,
                expected.database_id(),
                placement,
                generation
            )
            .unwrap(),
            expected
        );
        let artifact = owner
            .artifacts
            .iter()
            .find(|a| a.generation == Some(generation))
            .unwrap();
        let original = owner.path.join(&artifact.name);
        match mode {
            0 => {
                let saved = fs::metadata(&original).unwrap().modified().unwrap();
                let file = OpenOptions::new().write(true).open(&original).unwrap();
                let mut byte = [0];
                artifact.file.read_exact_at(&mut byte, 0).unwrap();
                file.write_all_at(&[byte[0] ^ 1], 0).unwrap();
                file.write_all_at(&byte, 0).unwrap();
                file.set_modified(saved).unwrap();
                file.sync_all().unwrap();
            }
            1 => {
                let saved = root.join("replaced");
                fs::rename(&original, &saved).unwrap();
                fs::copy(&saved, &original).unwrap();
            }
            2 => fs::hard_link(&original, root.join("extra-link")).unwrap(),
            3 => fs::rename(&original, root.join("missing")).unwrap(),
            4 => {
                fs::write(owner.path.join("unexpected"), b"extra").unwrap();
            }
            5 => {
                fs::write(
                    crate::journal::sealed_journal_path(&database, generation),
                    b"shadow",
                )
                .unwrap();
            }
            _ => owner.cancelled.store(true, Ordering::Release),
        }
        assert!(
            scope.finish().is_err(),
            "unfinished parsed input cannot authorize changed original custody: {mode}"
        );
        owner.cancelled.store(false, Ordering::Release);
        assert!(owner.verify().is_err());
        assert!(media.begin_owned_restore_reads().is_err());
    }
}

#[test]
fn bounded_private_read_drop_io_failure_and_cross_thread_cannot_restore_authority() {
    for mode in 0..4 {
        let (_root, owner, media, database, placement, generation, expected) =
            retained_frame_fixture();
        let mut scope = media.begin_owned_restore_reads().unwrap().unwrap();
        let input = scope.media();
        let path = crate::journal::sealed_journal_path(&database, generation);
        match mode {
            0 => {
                drop(scope);
            }
            1 => {
                let mut file = input.open_owned_retained_frame_read(&path, 4096).unwrap();
                assert!(file.read_exact_at(&mut [0; 8], usize::MAX).is_err());
                drop(file);
                assert!(scope.finish().is_err());
            }
            2 => {
                let mut file = input.open_owned_retained_frame_read(&path, 1).unwrap();
                assert!(file.read_exact_at(&mut [0; 2], 0).is_err());
                drop(file);
                assert!(scope.finish().is_err());
            }
            _ => {
                let id = expected.database_id();
                std::thread::scope(|threads| {
                    let result = threads
                        .spawn(move || {
                            crate::journal::read_retained_frame_with_media(
                                input.as_ref(),
                                &database,
                                id,
                                placement,
                                generation,
                            )
                        })
                        .join()
                        .unwrap();
                    assert!(result.is_err());
                });
                assert!(scope.finish().is_err());
            }
        }
        assert!(
            owner.verify().is_err(),
            "failed or unfinished scope remains invalid: {mode}"
        );
        assert!(media.begin_owned_restore_reads().is_err());
    }
}

#[test]
fn bounded_private_read_physical_budget_rotates_before_the_next_frame() {
    let (_root, owner, media, database, placement, generation, expected) = retained_frame_fixture();
    let mut scope = media.begin_owned_restore_reads().unwrap().unwrap();
    let input = scope.media();
    let path = crate::journal::sealed_journal_path(&database, generation);
    ORIGINAL_ARTIFACT_READ_CHECKS.with(|count| count.set(0));
    let mut endpoint_counts = Vec::new();
    for _ in 0..5 {
        // Conservatively reserve sixteen MiB for each small, actually read
        // frame. The fifth reservation must finish the prior byte interval.
        let mut file = input
            .open_owned_retained_frame_read(&path, 16 * 1024 * 1024)
            .unwrap();
        let mut bytes = vec![0; placement.padded_len];
        file.read_exact_at(&mut bytes, placement.position).unwrap();
        file.finish_retained_frame_read().unwrap();
        endpoint_counts.push(ORIGINAL_ARTIFACT_READ_CHECKS.with(|count| count.get()));
    }
    assert_eq!(endpoint_counts[0], endpoint_counts[3]);
    assert!(endpoint_counts[4] > endpoint_counts[3]);
    scope.finish().unwrap();
    owner.verify_full().unwrap();
    assert_eq!(
        expected.database_id(),
        decode_manifest(&fs::read(owner.path.join(MANIFEST_FILE_NAME)).unwrap())
            .unwrap()
            .database_id()
    );
}

#[test]
fn private_scope_never_uses_a_competing_same_database_resolver_after_custody_failure() {
    let (_root, owner, media, database, placement, generation, expected) = retained_frame_fixture();
    let id = expected.database_id();
    let selected = crate::history_stub::HistoryResolver::register(id, &database, Arc::new(media));
    let _competing = crate::history_stub::HistoryResolver::register_backup(id, &owner.path);
    let stub = crate::history_stub::HistoryStub {
        kind: crate::history_stub::StubKind::Receipt,
        database_id: id,
        generation,
        placement,
        summary: Default::default(),
    };
    let result = crate::history_stub::private_read_probe(&selected, || {
        let first = crate::history_stub::read_private_frame_probe(&stub)?;
        assert_eq!(first, expected);
        owner.cancelled.store(true, Ordering::Release);
        assert!(crate::history_stub::read_private_frame_probe(&stub).is_err());
        owner.cancelled.store(false, Ordering::Release);
        assert!(
            crate::history_stub::read_private_frame_probe(&stub).is_err(),
            "clearing cancellation cannot authorize fallback to another valid same-database image"
        );
        Ok(first)
    });
    assert!(result.is_err());
    assert!(owner.verify().is_err());
    assert_eq!(
        crate::history_stub::read_private_frame_probe(&stub).unwrap(),
        expected,
        "the competing ordinary resolver is valid, but could not rescue private custody"
    );
}

#[test]
fn a_nested_unrelated_private_scope_cannot_replace_the_current_owner() {
    let (_root, owner, media, database, placement, generation, expected) = retained_frame_fixture();
    let (_other_root, other_owner, other_media, other_database, _, _, _) = retained_frame_fixture();
    let selected = crate::history_stub::HistoryResolver::register(
        expected.database_id(),
        &database,
        Arc::new(media),
    );
    let other = crate::history_stub::HistoryResolver::register(
        expected.database_id(),
        &other_database,
        Arc::new(other_media),
    );
    let stub = crate::history_stub::HistoryStub {
        kind: crate::history_stub::StubKind::Receipt,
        database_id: expected.database_id(),
        generation,
        placement,
        summary: Default::default(),
    };
    let frame = crate::history_stub::private_read_probe(&selected, || {
        assert!(crate::history_stub::private_read_probe(&other, || Ok(())).is_err());
        crate::history_stub::read_private_frame_probe(&stub)
    })
    .unwrap();
    assert_eq!(frame, expected);
    owner.verify_full().unwrap();
    assert!(other_owner.verify().is_err());
}

struct NativeHydrationFixture {
    engine: MaterializationDatabase,
    _artifact: MaterializationArtifact,
    custody: crate::maintenance::RestoreCustody,
    resolver: Arc<crate::history_stub::HistoryResolver>,
    key: Vec<u8>,
    stub: Vec<u8>,
}

fn native_hydration_fixture(
    owner: &Arc<OwnedBackupHistory>,
    media: BorrowedHistoryMedia,
    database: &Path,
    placement: crate::journal::FramePlacement,
    generation: u64,
    expected: &crate::journal::JournalFrame,
) -> NativeHydrationFixture {
    use redb::ReadableDatabase as _;
    let mut artifact = MaterializationArtifact::create(
        database.parent().unwrap(),
        OsStr::new(DATABASE_ARTIFACT_FILE_NAME),
        true,
    )
    .unwrap();
    owner
        .copy_into(OsStr::new(DATABASE_ARTIFACT_FILE_NAME), &artifact)
        .unwrap();
    assert_eq!(artifact.checksum().unwrap(), owner.artifacts[0].checksum);
    let custody = crate::maintenance::RestoreCustody::from_verified_copy(
        artifact.verified_primary().unwrap(),
        Arc::clone(&owner.cancelled),
    )
    .unwrap();
    artifact.rename_under_custody(database, &custody).unwrap();
    let engine = MaterializationDatabase::Owned(
        crate::store::OwnedEngineOpen::from_private_restore_backend(custody.lease().unwrap())
            .unwrap(),
    );
    let sequence = CommitSequence::first();
    let key = crate::keys::encode_application_sequence_key(sequence).to_vec();
    let stub = crate::history_stub::HistoryStub {
        kind: crate::history_stub::StubKind::Segment,
        database_id: expected.database_id(),
        generation,
        placement,
        summary: crate::history_stub::SegmentSummary {
            first_commit_sequence: Some(sequence),
            last_commit_sequence: Some(sequence),
            command_count: 1,
            ..Default::default()
        },
    }
    .encode()
    .unwrap();
    let mut write = engine.begin_write().unwrap();
    write.set_two_phase_commit(true);
    write.set_durability(redb::Durability::Immediate).unwrap();
    {
        write
            .open_table(crate::layout::COMMITS)
            .unwrap()
            .insert(key.as_slice(), stub.as_slice())
            .unwrap();
    }
    write.commit().unwrap();
    assert_eq!(
        crate::store::read_identity_from_read_transaction(&engine.begin_read().unwrap()).unwrap(),
        expected.database_id()
    );
    let resolver = crate::history_stub::HistoryResolver::register(
        expected.database_id(),
        database,
        Arc::new(media),
    );
    NativeHydrationFixture {
        engine,
        _artifact: artifact,
        custody,
        resolver,
        key,
        stub,
    }
}

#[test]
fn native_hydration_finishes_original_custody_before_its_batch_commits() {
    use redb::ReadableDatabase as _;
    for mutate in [false, true] {
        let (_root, owner, media, database, placement, generation, expected) =
            retained_frame_fixture();
        let fixture =
            native_hydration_fixture(&owner, media, &database, placement, generation, &expected);
        if mutate {
            let path = owner.path.join(
                &owner
                    .artifacts
                    .iter()
                    .find(|a| a.generation == Some(generation))
                    .unwrap()
                    .name,
            );
            crate::history_stub::set_hydration_commit_hook(Box::new(move || {
                let metadata = fs::metadata(&path).unwrap();
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&path)
                    .unwrap();
                let mut byte = [0];
                file.read_exact_at(&mut byte, 0).unwrap();
                file.write_all_at(&[byte[0] ^ 1], 0).unwrap();
                file.write_all_at(&byte, 0).unwrap();
                file.set_modified(metadata.modified().unwrap()).unwrap();
                file.sync_all().unwrap();
            }));
        }
        let result = crate::history_stub::hydrate_surviving_command_stubs_owned(
            &fixture.engine,
            &fixture.resolver,
            &fixture.custody,
        );
        let read = fixture.engine.begin_read().unwrap();
        let table = read.open_table(crate::layout::COMMITS).unwrap();
        let encoded = table.get(fixture.key.as_slice()).unwrap().unwrap();
        if mutate {
            assert!(result.is_err());
            assert_eq!(encoded.value(), fixture.stub);
            assert!(owner.verify().is_err());
        } else {
            result.unwrap();
            assert_eq!(encoded.value(), b"owned frame value");
            owner.verify_full().unwrap();
        }
    }
}

#[cfg(unix)]
#[test]
fn private_owned_read_crash_child() {
    let Some(backup) = std::env::var_os("RIFFDB_TEST_OWNED_READ_BACKUP") else {
        return;
    };
    let backup = PathBuf::from(backup);
    let database = PathBuf::from(std::env::var_os("RIFFDB_TEST_OWNED_READ_DATABASE").unwrap());
    let stub_path = PathBuf::from(std::env::var_os("RIFFDB_TEST_OWNED_READ_STUB").unwrap());
    let stub_bytes = read_bounded_file(&stub_path, crate::history_stub::STUB_BYTES).unwrap();
    let stub = crate::history_stub::HistoryStub::decode(&stub_bytes)
        .unwrap()
        .unwrap();
    let manifest = decode_manifest(
        &read_bounded_file(&backup.join(MANIFEST_FILE_NAME), MANIFEST_MAX_BYTES).unwrap(),
    )
    .unwrap();
    let owner =
        OwnedBackupHistory::acquire(&backup, &manifest, Arc::new(AtomicBool::new(false))).unwrap();
    let artifact = owner
        .artifacts
        .iter()
        .find(|a| a.generation == Some(stub.generation))
        .unwrap();
    let expected = crate::journal::read_retained_frame_from_with_media(
        &OriginalMedia(Arc::clone(&owner)),
        &[backup.join(&artifact.name)],
        stub.database_id,
        stub.placement,
        stub.generation,
    )
    .unwrap();
    let media = BorrowedHistoryMedia::new(
        Arc::clone(&owner),
        database.clone(),
        Arc::new(PinnedDirectory::open(database.parent().unwrap()).unwrap()),
    );
    let fixture = native_hydration_fixture(
        &owner,
        media,
        &database,
        stub.placement,
        stub.generation,
        &expected,
    );
    let phase = std::env::var("RIFFDB_TEST_OWNED_READ_CRASH_PHASE").unwrap();
    if phase == "read" {
        let _ = crate::history_stub::private_read_probe::<()>(&fixture.resolver, || {
            let frame = crate::history_stub::read_private_frame_probe(&stub)?;
            assert_eq!(frame, expected);
            std::process::abort()
        });
    } else {
        assert_eq!(phase, "hydrate");
        crate::history_stub::set_hydration_commit_hook(Box::new(|| std::process::abort()));
        crate::history_stub::hydrate_surviving_command_stubs_owned(
            &fixture.engine,
            &fixture.resolver,
            &fixture.custody,
        )
        .unwrap();
    }
    panic!("private owned-read crash boundary was not reached");
}

#[cfg(unix)]
#[test]
fn private_owned_read_process_crashes_preserve_originals_and_uncommitted_hydration() {
    use redb::ReadableDatabase as _;
    use std::os::unix::process::ExitStatusExt as _;
    for phase in ["read", "hydrate"] {
        let (root, owner, _media, database, placement, generation, expected) =
            retained_frame_fixture();
        let before = owner
            .artifacts
            .iter()
            .map(|artifact| {
                (
                    artifact.name.clone(),
                    sha256_open_file(&artifact.file).unwrap(),
                )
            })
            .collect::<Vec<_>>();
        let sequence = CommitSequence::first();
        let stub = crate::history_stub::HistoryStub {
            kind: crate::history_stub::StubKind::Segment,
            database_id: expected.database_id(),
            generation,
            placement,
            summary: crate::history_stub::SegmentSummary {
                first_commit_sequence: Some(sequence),
                last_commit_sequence: Some(sequence),
                command_count: 1,
                ..Default::default()
            },
        }
        .encode()
        .unwrap();
        let stub_path = root.join("crash-stub.bin");
        fs::write(&stub_path, &stub).unwrap();
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "backup::owned_history::tests::private_owned_read_crash_child",
                "--nocapture",
            ])
            .env("RIFFDB_TEST_OWNED_READ_BACKUP", &owner.path)
            .env("RIFFDB_TEST_OWNED_READ_DATABASE", &database)
            .env("RIFFDB_TEST_OWNED_READ_STUB", &stub_path)
            .env("RIFFDB_TEST_OWNED_READ_CRASH_PHASE", phase)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            if std::time::Instant::now() >= deadline {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("bounded private read crash child did not finish");
            }
            std::thread::yield_now();
        };
        assert_eq!(
            status.signal(),
            Some(6),
            "must stop at the actual private input boundary: {phase}"
        );
        owner.verify_full().unwrap();
        for (name, checksum) in before {
            assert_eq!(sha256_file(&owner.path.join(name)).unwrap(), checksum);
        }
        assert!(!root.join("configured.redb").exists());
        let stage = redb::Database::open(&database).unwrap();
        let read = stage.begin_read().unwrap();
        let table = read.open_table(crate::layout::COMMITS).unwrap();
        let key = crate::keys::encode_application_sequence_key(sequence);
        assert_eq!(
            table.get(key.as_slice()).unwrap().unwrap().value(),
            stub,
            "unfinished reads or an interrupted private write batch cannot commit hydration: {phase}"
        );
        assert!(
            crate::RedbOfflineRestore::bind(&owner.path, database.parent().unwrap())
                .restore_owned_stamp_preparation(
                    &root.join("configured.redb"),
                    Arc::new(AtomicBool::new(false))
                )
                .is_err(),
            "partial stage cannot be accepted as an empty target or reconstructed validation authority"
        );
    }
}
