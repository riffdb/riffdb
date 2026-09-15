//! An archive fence comes from the exact verified physical backup, not a caller claim.
// req: REP-007, AFC-007
use super::*;
use crate::backup::{DATABASE_ARTIFACT_FILE_NAME, MANIFEST_FILE_NAME};
use riffdb_storage_api::*;
use riffdb_types::{DatabaseId, DualFrontier};
use std::{fs, path::Path};

fn backup(
    scope: &crate::test_path::ScopedDirectory,
) -> (std::path::PathBuf, ChangelogHistoryStateV3) {
    backup_with_pruned_head(scope, false)
}

fn backup_with_pruned_head(
    scope: &crate::test_path::ScopedDirectory,
    pruned: bool,
) -> (std::path::PathBuf, ChangelogHistoryStateV3) {
    let source = scope.join("source.redb");
    let mut store = crate::RedbStore::open(&source).unwrap();
    let id = DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x74; 10]).unwrap();
    store.initialize_database(id).unwrap();
    let frontier = if pruned {
        use riffdb_storage_api::proto_codec::*;
        let first = riffdb_types::CommitSequence::first();
        let write = store.shared.database.begin_write().unwrap();
        {
            let mut meta = write.open_table(crate::layout::META).unwrap();
            meta.insert(
                crate::layout::META_APPLICATION_SEQUENCE,
                encode_application_sequence_allocator_v1(ApplicationSequenceAllocator::Next(
                    first.checked_next().unwrap(),
                ))
                .unwrap()
                .as_bytes(),
            )
            .unwrap();
            // This fixture proves physical backup binding, not tombstone/catalog
            // semantics. Restore publication must still run the full scrub.
            let watermark = StoredRetentionWatermarkV1::new(
                first.get(),
                1,
                Some(riffdb_types::SchemaHash::from_bytes([0x42; 32])),
            )
            .unwrap();
            meta.insert(
                crate::layout::META_RETENTION_WATERMARK,
                encode_retention_watermark_v1(&watermark)
                    .unwrap()
                    .as_bytes(),
            )
            .unwrap();
        }
        write.commit().unwrap();
        DualFrontier::new(Some(first), None)
    } else {
        DualFrontier::INITIAL
    };
    let history = crate::changelog_v3_activation::activate_validated(
        store.shared.database.begin_write().unwrap(),
        ChangelogLineageV3::new(id, 1, LeadershipEpochV1::initial()).unwrap(),
        frontier,
    )
    .unwrap();
    drop(store);
    let path = scope.join("backup");
    crate::RedbOfflineBackup::bind(&source, &path)
        .create_offline_backup(
            &BackupBuildMetadataV1::new("0.1.0", "abc", "rustc", 1, vec![]).unwrap(),
        )
        .unwrap();
    (path, history)
}

#[test]
fn archive_backup_binding_uses_the_v3_fence_when_retained_commands_are_pruned() {
    let scope = crate::test_path::ScopedDirectory::new("archive-backup-pruned");
    let (path, history) = backup_with_pruned_head(&scope, true);
    let binding = RedbVerifiedArchiveBackup::open(&path).unwrap();
    assert_eq!(binding.manifest().last_commit_sequence(), None);
    assert_eq!(
        binding.manifest().effective_retention_watermark_sequence(),
        1
    );
    assert_eq!(binding.history(), history);
    assert_eq!(
        binding.history().tail().frontier().application(),
        Some(riffdb_types::CommitSequence::first())
    );
}

fn hashes(path: &Path) -> Vec<(std::ffi::OsString, BackupIntegrityChecksumV1)> {
    let mut rows: Vec<_> = fs::read_dir(path)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            (
                entry.file_name(),
                crate::backup::sha256_file(&entry.path()).unwrap(),
            )
        })
        .collect();
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    rows
}

#[test]
fn archive_backup_binding_is_read_only_and_uses_the_exact_backup_receipt() {
    let scope = crate::test_path::ScopedDirectory::new("archive-backup-binding");
    let (path, history) = backup(&scope);
    let original = hashes(&path);
    let binding = RedbVerifiedArchiveBackup::open(&path).unwrap();
    assert_eq!(binding.history(), history);
    assert_eq!(
        binding.manifest().database_id(),
        history.lineage().database_id()
    );
    assert_eq!(
        binding.manifest_digest().as_slice(),
        crate::backup::sha256_file(&path.join(MANIFEST_FILE_NAME))
            .unwrap()
            .as_bytes()
    );
    assert!(
        redb::Database::open(path.join(DATABASE_ARTIFACT_FILE_NAME)).is_err(),
        "the verified backup keeps its read lock"
    );
    let repository = binding
        .open_archive(
            &scope.join("archive"),
            ArchiveEncryptionPostureV1::Unencrypted,
        )
        .unwrap();
    assert_eq!(repository.position(), history.tail());
    assert!(repository.head().is_none());
    drop(binding);
    assert_eq!(
        hashes(&path),
        original,
        "read-only verification must not repair or dirty the backup"
    );
}

#[test]
fn archive_backup_binding_refuses_damage_and_lost_custody_before_creating_archive() {
    for name in [
        MANIFEST_FILE_NAME,
        DATABASE_ARTIFACT_FILE_NAME,
        crate::backup::JOURNAL_ARTIFACT_FILE_NAME,
        crate::backup::FORMAT_MARKER_ARTIFACT_FILE_NAME,
    ] {
        let scope = crate::test_path::ScopedDirectory::new("archive-backup-damaged");
        let (path, _) = backup(&scope);
        let binding = RedbVerifiedArchiveBackup::open(&path).unwrap();
        fs::rename(path.join(name), scope.join("retained")).unwrap();
        fs::write(path.join(name), b"unrelated replacement").unwrap();
        let archive = scope.join("archive");
        assert!(
            binding
                .open_archive(&archive, ArchiveEncryptionPostureV1::Unencrypted)
                .is_err()
        );
        assert!(!archive.exists());
        assert_eq!(fs::read(path.join(name)).unwrap(), b"unrelated replacement");
        drop(binding);
        assert!(RedbVerifiedArchiveBackup::open(&path).is_err());
    }
}

#[test]
fn archive_backup_binding_rehashes_retained_files_before_archive_creation() {
    use std::io::{Read, Seek, Write};
    for name in [
        MANIFEST_FILE_NAME,
        crate::backup::JOURNAL_ARTIFACT_FILE_NAME,
        crate::backup::FORMAT_MARKER_ARTIFACT_FILE_NAME,
    ] {
        let scope = crate::test_path::ScopedDirectory::new("archive-backup-in-place");
        let (path, _) = backup(&scope);
        let binding = RedbVerifiedArchiveBackup::open(&path).unwrap();
        let mut file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.join(name))
            .unwrap();
        let mut byte = [0];
        file.read_exact(&mut byte).unwrap();
        byte[0] ^= 1;
        file.rewind().unwrap();
        file.write_all(&byte).unwrap();
        file.sync_all().unwrap();
        assert!(
            binding
                .open_archive(
                    &scope.join("archive"),
                    ArchiveEncryptionPostureV1::Unencrypted
                )
                .is_err()
        );
        assert!(!scope.join("archive").exists());
    }
}
