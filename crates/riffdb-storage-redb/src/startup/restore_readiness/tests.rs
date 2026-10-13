//! Real owned restore readiness, native exclusion and semantic deferral.
// req: REC-001, REC-004, REC-005, REC-006, REC-007, REC-008, END-001
use super::*;
use riffdb_storage_api::{AuthoritativePointReader, OfflineBackupPersistencePort};

fn inputs() -> StartupValidationInputs {
    let key = ReadableDigestKey::v1(DigestKeyId::new(1).unwrap());
    StartupValidationInputs::new(
        Timestamp::new(1, 0).unwrap(),
        ReadableCapabilityDigestInventory::new(vec![key]).unwrap(),
        ReadableIdempotencyDigestInventory::new(vec![key]).unwrap(),
    )
}

fn fixture(
    corrupt_entity: bool,
) -> (
    crate::test_path::ScopedDirectory,
    PathBuf,
    riffdb_storage_api::EntityTarget,
) {
    use riffdb_types::{CanonicalRecord, EntityKeyBuilder, EntityTypeId, EntityVersion};
    let root = crate::test_path::ScopedDirectory::new("restore-readiness");
    let source = root.join("source.redb");
    let id = DatabaseId::from_unix_milliseconds_and_random(1700000000000, [0x64; 10]).unwrap();
    let store = crate::startup::tests::deployed_restore_readiness_fixture(&source, id);
    let mut session = store.begin_structural_evidence(inputs()).unwrap();
    let (catalog, structural, historical) =
        RedbOfflineIntegrityScrub::walk_session(&mut session).unwrap();
    let CatalogHistoryOutcome::Ready(catalog) = catalog else {
        panic!("fixture catalog")
    };
    let StructuralOpenOutcome::Clean(opened) = session.finish(structural, historical).unwrap()
    else {
        panic!("fixture startup")
    };
    let (db, open, _, dormant) = opened.into_parts();
    assert!(catalog.matches(db, open));
    drop(dormant.into_operational_after_catalog_validation().unwrap());
    let entity = EntityTypeId::new(7).unwrap();
    let mut key = EntityKeyBuilder::new(entity);
    key.push_u64(9).unwrap();
    let target = riffdb_storage_api::EntityTarget::new(entity, key.finish().unwrap()).unwrap();
    if corrupt_entity {
        let database = redb::Database::open(&source).unwrap();
        let read = database.begin_read().unwrap();
        let catalog = read.open_table(CATALOG_ACTIVE).unwrap();
        let encoded = catalog.get(CATALOG_ACTIVE_KEY.as_slice()).unwrap().unwrap();
        let active = codec::decode_active_catalog_pointer_v1(encoded.value()).unwrap();
        let active = active.value();
        let binding = riffdb_storage_api::DurableKeySchemaBindingV1::new(
            active.lineage().clone(),
            active.contract_version(),
            active.bundle_hash(),
        );
        let mut other_key = EntityKeyBuilder::new(entity);
        other_key.push_u64(10).unwrap();
        let other =
            riffdb_storage_api::EntityTarget::new(entity, other_key.finish().unwrap()).unwrap();
        let record = riffdb_storage_api::StoredEntityRecordV1::new(
            other,
            EntityVersion::new(1).unwrap(),
            active.contract_version(),
            binding,
            CanonicalRecord::new(vec![]).unwrap(),
        )
        .unwrap();
        let record = codec::encode_entity_record_v1(&record).unwrap();
        drop(encoded);
        drop(catalog);
        drop(read);
        let write = database.begin_write().unwrap();
        write
            .open_table(ENTITIES)
            .unwrap()
            .insert(keys::encode_entity_key(target.key()), record.as_bytes())
            .unwrap();
        write.commit().unwrap();
    }
    let backup = root.join("backup");
    let build = riffdb_storage_api::BackupBuildMetadataV1::new(
        "0.1.0",
        "0123456789abcdef",
        "rustc-test",
        1,
        vec![],
    )
    .unwrap();
    crate::RedbOfflineBackup::bind(&source, &backup)
        .create_offline_backup(&build)
        .unwrap();
    (root, backup, target)
}

#[test]
fn restore_readiness_defers_nonroot_semantics_without_granting_complete_archive_proof() {
    let (root, backup, target) = fixture(true);
    let before = crate::backup::validate_immutable_backup(&backup).unwrap();
    let stage = root.join("stage");
    let (_, owned) = crate::RedbOfflineRestore::bind(&backup, &stage)
        .restore_owned_staged_backup(
            &root.join("configured.redb"),
            Arc::new(AtomicBool::new(false)),
        )
        .unwrap();
    POPULATION_COUNTS.with(|count| count.set(0));
    let (owner, evidence) = owned.validate_readiness(inputs()).unwrap();
    assert_eq!(POPULATION_COUNTS.with(|count| count.get()), 0);
    assert!(
        evidence.archive_snapshot().is_err(),
        "bounded proof must not authorize an archive join"
    );
    let (_, _, retained, _, reader) = evidence.into_parts().unwrap();
    assert!(
        matches!(reader.read_entity(&target), Err(error) if error.kind() == StorageErrorKind::CorruptData)
    );
    assert!(matches!(
        redb::Database::open(stage.join(crate::backup::DATABASE_ARTIFACT_FILE_NAME)),
        Err(redb::DatabaseError::DatabaseAlreadyOpen)
    ));
    drop(reader);
    let mut closed = owner.close().unwrap();
    let incarnation = retained.history_incarnation() + 1;
    closed.stamp_history_incarnation(incarnation).unwrap();
    closed.verify_publication_seal().unwrap();
    drop(closed);
    let store = RedbStore::open(stage.join(crate::backup::DATABASE_ARTIFACT_FILE_NAME)).unwrap();
    POPULATION_COUNTS.with(|count| count.set(0));
    let mut session = store
        .begin_published_restore_readiness(inputs(), retained.history_incarnation())
        .unwrap();
    assert!(session.bounded_dirty_startup_path());
    let (catalog, structural, historical) =
        RedbOfflineIntegrityScrub::walk_session(&mut session).unwrap();
    assert!(matches!(catalog, CatalogHistoryOutcome::Ready(_)));
    assert_eq!(session.command_history_rows_decoded, 0);
    assert!(session.structural_cursors.is_none());
    assert!(!session.command_cache_built);
    assert!(session.recovery_owned_peak_bytes <= MAX_RECOVERY_OWNED_STATE_BYTES as u64);
    let StructuralOpenOutcome::Clean(opened) = session.finish(structural, historical).unwrap()
    else {
        panic!("bounded reopen")
    };
    assert_eq!(POPULATION_COUNTS.with(|count| count.get()), 0);
    drop(opened);
    let store = RedbStore::open(stage.join(crate::backup::DATABASE_ARTIFACT_FILE_NAME)).unwrap();
    let mut scrub = store
        .begin_structural_evidence_for(inputs(), EvidenceOpenPurpose::BackgroundScrub)
        .unwrap();
    assert!(!scrub.bounded_dirty_startup_path());
    assert!(
        RedbOfflineIntegrityScrub::walk_session(&mut scrub).is_err(),
        "the unchanged complete background walk must find the deferred corruption"
    );
    assert_eq!(
        crate::backup::validate_immutable_backup(&backup).unwrap(),
        before
    );
    assert!(!root.join("configured.redb").exists());
}

#[test]
fn restore_readiness_rejects_corrupt_bounded_root_before_authorization() {
    let (root, backup, _) = fixture(false);
    let source = root.join("source.redb");
    let database = redb::Database::open(&source).unwrap();
    let write = database.begin_write().unwrap();
    write
        .open_table(META)
        .unwrap()
        .insert(META_APPLICATION_SEQUENCE, b"invalid allocator".as_slice())
        .unwrap();
    write.commit().unwrap();
    drop(database);
    assert!(
        RedbStore::open(&source)
            .and_then(|store| store.begin_published_restore_readiness(inputs(), 1))
            .is_err()
    );
    assert!(crate::backup::validate_immutable_backup(&backup).is_ok());
    assert!(!root.join("configured.redb").exists());
}

#[test]
fn restore_readiness_reuses_complete_history_only_for_the_same_receipt_stamp() {
    let (root, backup, _) = fixture(false);
    let (_, owned) = crate::RedbOfflineRestore::bind(&backup, root.join("stage"))
        .restore_owned_staged_backup(
            &root.join("configured.redb"),
            Arc::new(AtomicBool::new(false)),
        )
        .unwrap();
    let (owner, evidence) = owned.validate_readiness(inputs()).unwrap();
    assert!(
        evidence.archive_snapshot().is_err(),
        "history proof is not complete scrub assurance"
    );
    drop(evidence);
    let prepared = owner.into_stamp_preparation().unwrap();
    let incarnation = prepared.history_incarnation() + 1;
    crate::changelog_v3_roots::COMPLETE_HISTORY_VALIDATIONS.with(|count| count.set(0));
    let closed = prepared.stamp_and_close(incarnation).unwrap();
    assert_eq!(
        crate::changelog_v3_roots::COMPLETE_HISTORY_VALIDATIONS.with(|count| count.get()),
        1,
        "only the new one-row RestoreAnchor needs a fresh full validation"
    );
    closed.verify_publication_seal().unwrap();
    assert_eq!(closed.history_incarnation(), incarnation);
}

fn receipt_stamp_backup() -> (crate::test_path::ScopedDirectory, PathBuf) {
    let (root, backup, _) = fixture(false);
    let source = root.join("source.redb");
    let journal = crate::journal::journal_path(&source);
    // The deployed compatibility fixture has only Immediate writes and no
    // active source journal. Its genuine backup supplies the empty bound one.
    std::fs::copy(
        backup.join(crate::backup::JOURNAL_ARTIFACT_FILE_NAME),
        &journal,
    )
    .unwrap();
    std::fs::rename(&journal, crate::journal::sealing_journal_path(&source)).unwrap();
    let id = crate::backup::validate_immutable_backup(&backup)
        .unwrap()
        .0
        .database_id();
    crate::journal::seal_extent_with_media(&crate::media::RealJournalMedia, &source, id)
        .unwrap()
        .unwrap();
    crate::journal::recreate_missing_active_with_media(&crate::media::RealJournalMedia, &source)
        .unwrap();
    std::fs::remove_dir_all(&backup).unwrap();
    let build = riffdb_storage_api::BackupBuildMetadataV1::new(
        "0.1.0",
        "0123456789abcdef",
        "rustc-test",
        1,
        vec![],
    )
    .unwrap();
    crate::RedbOfflineBackup::bind(&source, &backup)
        .create_offline_backup(&build)
        .unwrap();
    (root, backup)
}

#[cfg(unix)]
#[test]
fn restore_readiness_borrows_original_backup_extents_without_private_copies() {
    use std::os::unix::fs::MetadataExt;
    let (root, backup) = receipt_stamp_backup();
    let originals: Vec<_> = std::fs::read_dir(&backup)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("journal.sealed.")
        })
        .map(|path| {
            let metadata = std::fs::metadata(&path).unwrap();
            let bytes = std::fs::read(&path).unwrap();
            (path, metadata.dev(), metadata.ino(), bytes)
        })
        .collect();
    assert_eq!(
        originals.len(),
        1,
        "genuine backup has one sealed history source"
    );
    let stage = root.join("stage");
    let (_, owned) = crate::RedbOfflineRestore::bind(&backup, &stage)
        .restore_owned_stamp_preparation(
            &root.join("configured.redb"),
            Arc::new(AtomicBool::new(false)),
        )
        .unwrap();
    assert!(
        copied_extent_paths(&stage).is_empty(),
        "eligible receipt restore must read owned originals without private extent copies"
    );
    let (owner, evidence) = owned.validate_readiness(inputs()).unwrap();
    let (_, _, retained, _, reader) = evidence.into_parts().unwrap();
    drop(reader);
    let closed = owner
        .into_stamp_preparation()
        .unwrap()
        .stamp_and_close(retained.history_incarnation() + 1)
        .unwrap();
    closed.verify_publication_seal().unwrap();
    assert!(copied_extent_paths(&stage).is_empty());
    for (path, device, inode, bytes) in originals {
        let metadata = std::fs::metadata(&path).unwrap();
        assert_eq!(
            (metadata.dev(), metadata.ino(), metadata.nlink()),
            (device, inode, 1)
        );
        assert_eq!(std::fs::read(path).unwrap(), bytes);
    }
    assert!(!root.join("configured.redb").exists());
}

#[test]
fn restore_readiness_receipt_stamp_retains_original_extents_until_the_anchor_and_close() {
    let (root, backup) = receipt_stamp_backup();
    let before = crate::backup::validate_immutable_backup(&backup).unwrap();
    let stage = root.join("stage");
    let (_, owned) = crate::RedbOfflineRestore::bind(&backup, &stage)
        .restore_owned_stamp_preparation(
            &root.join("configured.redb"),
            Arc::new(AtomicBool::new(false)),
        )
        .unwrap();
    assert!(copied_extent_paths(&stage).is_empty());
    crate::changelog_v3_roots::COMPLETE_HISTORY_VALIDATIONS.with(|count| count.set(0));
    let (owner, evidence) = owned.validate_readiness(inputs()).unwrap();
    assert_eq!(
        crate::changelog_v3_roots::COMPLETE_HISTORY_VALIDATIONS.with(|count| count.get()),
        1
    );
    let (_, _, retained, _, reader) = evidence.into_parts().unwrap();
    drop(reader);
    assert!(copied_extent_paths(&stage).is_empty());
    let incarnation = retained.history_incarnation() + 1;
    let mut closed = owner
        .into_stamp_preparation()
        .unwrap()
        .stamp_and_close(incarnation)
        .unwrap();
    closed.verify_publication_seal().unwrap();
    assert!(copied_extent_paths(&stage).is_empty());
    closed.stamp_history_incarnation(incarnation).unwrap();
    closed.verify_publication_seal().unwrap();
    assert_eq!(
        crate::backup::validate_immutable_backup(&backup).unwrap(),
        before
    );
    assert!(!root.join("configured.redb").exists());
}

#[test]
fn restore_readiness_receipt_stamp_cannot_close_without_removing_predecessor_references() {
    let (root, backup) = receipt_stamp_backup();
    let (_, owned) = crate::RedbOfflineRestore::bind(&backup, root.join("stage"))
        .restore_owned_stamp_preparation(
            &root.join("configured.redb"),
            Arc::new(AtomicBool::new(false)),
        )
        .unwrap();
    let (owner, evidence) = owned.validate_readiness(inputs()).unwrap();
    drop(evidence);
    assert!(
        owner.close().is_err(),
        "the legacy close cannot authorize an unanchored deferred stage"
    );
}

#[test]
fn restore_readiness_receipt_stamp_refuses_missing_replaced_and_same_length_changed_extents() {
    for mode in 0..3 {
        let (root, backup) = receipt_stamp_backup();
        let stage = root.join("stage");
        let (_, owned) = crate::RedbOfflineRestore::bind(&backup, &stage)
            .restore_owned_stamp_preparation(
                &root.join("configured.redb"),
                Arc::new(AtomicBool::new(false)),
            )
            .unwrap();
        let extent = std::fs::read_dir(&backup)
            .unwrap()
            .map(|row| row.unwrap().path())
            .find(|path| {
                path.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with("journal.sealed.")
            })
            .unwrap();
        let bytes = std::fs::read(&extent).unwrap();
        let fixed = [
            crate::backup::DATABASE_ARTIFACT_FILE_NAME,
            crate::backup::JOURNAL_ARTIFACT_FILE_NAME,
            crate::backup::FORMAT_MARKER_ARTIFACT_FILE_NAME,
        ]
        .map(|name| {
            (
                name,
                crate::backup::sha256_file(&backup.join(name)).unwrap(),
            )
        });
        match mode {
            0 => std::fs::remove_file(&extent).unwrap(),
            1 => {
                std::fs::rename(&extent, root.join("original-extent")).unwrap();
                std::fs::write(&extent, &bytes).unwrap();
            }
            _ => {
                use std::io::{Seek, SeekFrom, Write};
                let mut file = std::fs::OpenOptions::new()
                    .write(true)
                    .open(&extent)
                    .unwrap();
                file.seek(SeekFrom::Start(0)).unwrap();
                file.write_all(&[bytes[0] ^ 1]).unwrap();
                file.sync_all().unwrap();
            }
        }
        assert!(owned.validate_readiness(inputs()).is_err());
        for (name, checksum) in fixed {
            assert_eq!(
                crate::backup::sha256_file(&backup.join(name)).unwrap(),
                checksum
            );
        }
        assert!(!root.join("configured.redb").exists());
    }
}

#[cfg(unix)]
#[test]
fn restore_readiness_borrowed_extent_cursors_are_independent_and_read_only() {
    let (root, backup) = receipt_stamp_backup();
    let (_, owned) = crate::RedbOfflineRestore::bind(&backup, root.join("stage"))
        .restore_owned_stamp_preparation(
            &root.join("configured.redb"),
            Arc::new(AtomicBool::new(false)),
        )
        .unwrap();
    let (media, path) = owned.borrowed_extent_media_for_test().unwrap();
    let mut first = media.open_read(&path).unwrap();
    let mut second = media.open_read(&path).unwrap();
    let mut a = [0; 32];
    let mut b = [0; 32];
    first.read_exact(&mut a).unwrap();
    second.read_exact(&mut b).unwrap();
    assert_eq!(a, b);
    first.read_exact(&mut a).unwrap();
    second.read_exact(&mut b).unwrap();
    assert_eq!(a, b);
    first.read_exact_at(&mut a, 0).unwrap();
    first.read_exact(&mut a).unwrap();
    second.read_exact(&mut b).unwrap();
    assert_eq!(a, b);
    assert!(first.write_all(b"refuse").is_err());
    assert!(first.write_all_at(b"refuse", 0).is_err());
    assert!(first.set_len(0).is_err());
    assert!(media.open_read_write(&path).is_err());
    assert!(media.remove_file(&path).is_err());
    drop(first);
    drop(second);
    drop(media);
    owned.validate_readiness(inputs()).unwrap();
}

#[cfg(unix)]
#[test]
fn restore_readiness_borrowed_backup_mutations_invalidate_already_open_cursors() {
    use std::io::{Seek, SeekFrom, Write};
    for mode in 0..7 {
        let (root, backup) = receipt_stamp_backup();
        let (_, owned) = crate::RedbOfflineRestore::bind(&backup, root.join("stage"))
            .restore_owned_stamp_preparation(
                &root.join("configured.redb"),
                Arc::new(AtomicBool::new(false)),
            )
            .unwrap();
        let (media, path) = owned.borrowed_extent_media_for_test().unwrap();
        let mut cursor = media.open_read(&path).unwrap();
        let original = std::fs::read_dir(&backup)
            .unwrap()
            .map(|r| r.unwrap().path())
            .find(|p| {
                p.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with("journal.sealed.")
            })
            .unwrap();
        let before = std::fs::read(&original).unwrap();
        match mode {
            0 => std::fs::remove_file(&original).unwrap(),
            1 => {
                std::fs::rename(&original, root.join("old-extent")).unwrap();
                std::fs::write(&original, &before).unwrap();
            }
            2 => {
                let m = std::fs::metadata(&original).unwrap();
                let mut file = std::fs::OpenOptions::new()
                    .write(true)
                    .open(&original)
                    .unwrap();
                file.seek(SeekFrom::Start(0)).unwrap();
                file.write_all(&[before[0] ^ 1]).unwrap();
                file.set_modified(m.modified().unwrap()).unwrap();
                file.sync_all().unwrap();
            }
            3 => std::fs::hard_link(&original, root.join("linked-extent")).unwrap(),
            4 => std::fs::write(backup.join("unknown-artifact"), b"unexpected").unwrap(),
            5 => std::fs::write(
                root.join("stage/database.redb.riffjournal.sealed.00000000000000000099"),
                b"unexpected",
            )
            .unwrap(),
            _ => {
                std::fs::rename(&backup, root.join("original-backup")).unwrap();
                std::fs::create_dir(&backup).unwrap();
            }
        }
        if mode != 5 {
            assert!(
                cursor.read_exact(&mut [0; 16]).is_err(),
                "changed original must invalidate an existing read: {mode}"
            );
        }
        assert!(
            owned.validate_readiness(inputs()).is_err(),
            "backup and stage inventory must be checked: {mode}"
        );
        if mode == 2 {
            std::fs::write(&original, &before).unwrap();
            assert!(
                cursor.read_exact(&mut [0; 16]).is_err(),
                "restoring bytes cannot launder an invalidated owner"
            );
        }
    }
}

#[cfg(unix)]
#[test]
fn restore_readiness_borrowed_cursor_cancellation_is_sticky_and_preserves_backup() {
    let (root, backup) = receipt_stamp_backup();
    let cancellation = Arc::new(AtomicBool::new(false));
    let before: Vec<_> = std::fs::read_dir(&backup)
        .unwrap()
        .map(|r| r.unwrap().path())
        .map(|p| (p.clone(), crate::backup::sha256_file(&p).unwrap()))
        .collect();
    let (_, owned) = crate::RedbOfflineRestore::bind(&backup, root.join("stage"))
        .restore_owned_stamp_preparation(&root.join("configured.redb"), Arc::clone(&cancellation))
        .unwrap();
    let (media, path) = owned.borrowed_extent_media_for_test().unwrap();
    let mut cursor = media.open_read(&path).unwrap();
    cancellation.store(true, Ordering::Release);
    assert!(cursor.read_exact(&mut [0; 16]).is_err());
    cancellation.store(false, Ordering::Release);
    assert!(cursor.read_exact(&mut [0; 16]).is_err());
    assert!(owned.validate_readiness(inputs()).is_err());
    drop(cursor);
    drop(media);
    for (path, checksum) in before {
        assert_eq!(crate::backup::sha256_file(&path).unwrap(), checksum);
    }
}

#[cfg(unix)]
#[test]
fn restore_readiness_borrowed_reader_must_release_before_stamp_retirement() {
    let (root, backup) = receipt_stamp_backup();
    let (_, owned) = crate::RedbOfflineRestore::bind(&backup, root.join("stage"))
        .restore_owned_stamp_preparation(
            &root.join("configured.redb"),
            Arc::new(AtomicBool::new(false)),
        )
        .unwrap();
    let (media, path) = owned.borrowed_extent_media_for_test().unwrap();
    let _cursor = media.open_read(&path).unwrap();
    drop(media);
    let (owner, evidence) = owned.validate_readiness(inputs()).unwrap();
    let (_, _, retained, _, reader) = evidence.into_parts().unwrap();
    drop(reader);
    assert!(
        owner
            .into_stamp_preparation()
            .unwrap()
            .stamp_and_close(retained.history_incarnation() + 1)
            .is_err(),
        "a surviving original read cannot cross retirement or publication"
    );
    assert!(!root.join("configured.redb").exists());
}

fn copied_extent_paths(stage: &Path) -> Vec<PathBuf> {
    std::fs::read_dir(stage)
        .unwrap()
        .map(|row| row.unwrap().path())
        .filter(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .contains(".riffjournal.sealed.")
        })
        .collect()
}

#[test]
fn restore_readiness_receipt_stamp_refuses_deferred_history_for_missing_locator_images() {
    for missing in [
        crate::layout::IDEMPOTENCY_LOCATORS,
        crate::layout::PROVENANCE_LOCATORS,
        crate::layout::AUDIT_BY_REQUEST_LOCATORS,
    ] {
        let (root, backup, _) = fixture(false);
        let source = root.join("source.redb");
        let database = redb::Database::open(&source).unwrap();
        assert!(private_restore_image_supports_bounded_readiness(&database).unwrap());
        let write = database.begin_write().unwrap();
        assert!(write.delete_table(missing).unwrap());
        write.commit().unwrap();
        assert!(
            !private_restore_image_supports_bounded_readiness(&database).unwrap(),
            "registry completion cannot authorize deferred history across native table preparation"
        );
        drop(database);
        std::fs::remove_dir_all(&backup).unwrap();
        let build = riffdb_storage_api::BackupBuildMetadataV1::new(
            "0.1.0",
            "0123456789abcdef",
            "rustc-test",
            1,
            vec![],
        )
        .unwrap();
        crate::RedbOfflineBackup::bind(&source, &backup)
            .create_offline_backup(&build)
            .unwrap();
        let before = crate::backup::validate_immutable_backup(&backup).unwrap();
        let (_, control) = crate::RedbOfflineRestore::bind(&backup, root.join("control"))
            .restore_owned_staged_backup(
                &root.join("control-configured.redb"),
                Arc::new(AtomicBool::new(false)),
            )
            .unwrap();
        let control_error = match control.validate_complete(inputs()) {
            Err(error) => error,
            Ok(_) => panic!("incomplete current image cannot acquire a complete restore proof"),
        };
        assert_eq!(control_error.kind(), StorageErrorKind::IncompatibleFormat);
        let (_, owned) = crate::RedbOfflineRestore::bind(&backup, root.join("stage"))
            .restore_owned_stamp_preparation(
                &root.join("configured.redb"),
                Arc::new(AtomicBool::new(false)),
            )
            .unwrap();
        let error = match owned.validate_readiness(inputs()) {
            Err(error) => error,
            Ok(_) => panic!("receipt stamping cannot bypass the complete preparation refusal"),
        };
        assert_eq!(error.kind(), control_error.kind());
        assert_eq!(
            crate::backup::validate_immutable_backup(&backup).unwrap(),
            before
        );
        assert!(!root.join("configured.redb").exists());
        assert!(!root.join("control-configured.redb").exists());
    }
}

#[cfg(unix)]
fn foreign_capacity_backup_fixture() -> (crate::test_path::ScopedDirectory, PathBuf) {
    let (root, backup, _) = fixture(false);
    let (manifest, _) = crate::backup::validate_immutable_backup(&backup).unwrap();
    std::fs::copy(
        backup.join(crate::backup::JOURNAL_ARTIFACT_FILE_NAME),
        crate::journal::journal_path(&root.join("source.redb")),
    )
    .unwrap();
    crate::journal::prepare_foreign_capacity_restore_fixture(
        &root.join("source.redb"),
        manifest.database_id(),
    )
    .unwrap();
    let source_backup = root.join("foreign-capacity-backup");
    let build =
        riffdb_storage_api::BackupBuildMetadataV1::new("0.1.0", "abc", "rustc", 1, vec![]).unwrap();
    crate::RedbOfflineBackup::bind(root.join("source.redb"), &source_backup)
        .create_offline_backup(&build)
        .unwrap();
    (root, source_backup)
}

#[cfg(unix)]
#[test]
fn other_supported_extent_capacity_uses_complete_copy_and_hydration() {
    let (root, source_backup) = foreign_capacity_backup_fixture();
    let original = crate::backup::validate_immutable_backup(&source_backup).unwrap();
    let stage = root.join("foreign-capacity-stage");
    let (_, owned) = crate::RedbOfflineRestore::bind(&source_backup, &stage)
        .restore_owned_stamp_preparation(
            &root.join("configured.redb"),
            Arc::new(AtomicBool::new(false)),
        )
        .unwrap();
    assert!(
        owned.borrowed_extent_media_for_test().is_err(),
        "a capacity rotation cannot acquire closed borrowed history authority"
    );
    let (authorization, evidence) = owned.validate_readiness(inputs()).unwrap();
    drop(evidence);
    drop(authorization);
    assert_eq!(
        crate::backup::validate_immutable_backup(&source_backup).unwrap(),
        original
    );
}

#[cfg(unix)]
#[test]
fn private_capacity_conversion_process_child() {
    let Some(backup) = std::env::var_os("RIFFDB_TEST_PRIVATE_CAPACITY_BACKUP") else {
        return;
    };
    let stage = PathBuf::from(std::env::var_os("RIFFDB_TEST_PRIVATE_CAPACITY_STAGE").unwrap());
    let configured = stage.parent().unwrap().join("configured.redb");
    let _ = crate::RedbOfflineRestore::bind(PathBuf::from(backup), &stage)
        .restore_owned_stamp_preparation(&configured, Arc::new(AtomicBool::new(false)))
        .unwrap();
    panic!("private conversion crash boundary was not reached");
}

#[cfg(unix)]
#[test]
fn interrupted_private_capacity_conversion_preserves_backup_and_refuses_partial_stage() {
    use std::os::unix::process::ExitStatusExt;
    let (root, backup) = foreign_capacity_backup_fixture();
    let original = crate::backup::validate_immutable_backup(&backup).unwrap();
    let stage = root.join("interrupted-private-capacity");
    let configured = root.join("configured.redb");
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "startup::restore_readiness::tests::private_capacity_conversion_process_child",
            "--nocapture",
        ])
        .env("RIFFDB_TEST_PRIVATE_CAPACITY_BACKUP", &backup)
        .env("RIFFDB_TEST_PRIVATE_CAPACITY_STAGE", &stage)
        .env("RIFFDB_TEST_PRIVATE_CAPACITY_ABORT", "1")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .unwrap();
    assert_eq!(
        status.signal(),
        Some(6),
        "the child must stop at the conversion's synchronized write boundary"
    );
    assert!(
        !configured.exists(),
        "an interrupted private conversion cannot publish a primary"
    );
    assert!(stage.is_dir());
    assert!(
        crate::RedbOfflineRestore::bind(&backup, &stage)
            .restore_owned_stamp_preparation(&configured, Arc::new(AtomicBool::new(false)))
            .is_err(),
        "an interrupted stage cannot be accepted as an empty restore target"
    );
    assert_eq!(
        crate::backup::validate_immutable_backup(&backup).unwrap(),
        original
    );
    let fresh = root.join("fresh-private-capacity");
    let (_, owned) = crate::RedbOfflineRestore::bind(&backup, &fresh)
        .restore_owned_stamp_preparation(&configured, Arc::new(AtomicBool::new(false)))
        .unwrap();
    let (authorization, evidence) = owned.validate_readiness(inputs()).unwrap();
    drop(evidence);
    drop(authorization);
    assert_eq!(
        crate::backup::validate_immutable_backup(&backup).unwrap(),
        original
    );
}
