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
    let (_, owned) = crate::RedbOfflineRestore::bind(&backup, &root.join("stage"))
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
    assert_eq!(copied_extent_paths(&stage).len(), 1);
    crate::changelog_v3_roots::COMPLETE_HISTORY_VALIDATIONS.with(|count| count.set(0));
    let (owner, evidence) = owned.validate_readiness(inputs()).unwrap();
    assert_eq!(
        crate::changelog_v3_roots::COMPLETE_HISTORY_VALIDATIONS.with(|count| count.get()),
        1
    );
    let (_, _, retained, _, reader) = evidence.into_parts().unwrap();
    drop(reader);
    assert_eq!(copied_extent_paths(&stage).len(), 1);
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
    let (_, owned) = crate::RedbOfflineRestore::bind(&backup, &root.join("stage"))
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
        let extent = std::fs::read_dir(&stage)
            .unwrap()
            .map(|row| row.unwrap().path())
            .find(|path| {
                path.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .contains(".riffjournal.sealed.")
            })
            .unwrap();
        let bytes = std::fs::read(&extent).unwrap();
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
        assert!(crate::backup::validate_immutable_backup(&backup).is_ok());
    }
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
