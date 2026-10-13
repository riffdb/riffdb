//! Actual native restore artifacts, never synthetic byte copies as authority.
// req: REC-001, REC-004, REC-005, REC-006, REC-008, END-001
use super::*;
use crate::test_path::ScopedDirectory;
use riffdb_storage_api::*;
use riffdb_types::{DigestKeyId, Timestamp};
use std::os::unix::fs::MetadataExt;
use std::sync::atomic::{AtomicBool, Ordering};

const TEMP: [&str; 3] = ["primary.tmp", "journal.tmp", "format.tmp"];
const FINAL: [&str; 3] = [
    "primary.redb",
    "primary.redb.riffjournal",
    "primary.redb.format",
];

fn fixture(root: &ScopedDirectory) -> (OwnedClosedMaterialization, PathBuf, Arc<AtomicBool>) {
    let source = root.join("source.redb");
    let mut store = crate::RedbStore::open(&source).unwrap();
    store
        .initialize_database(
            DatabaseId::from_unix_milliseconds_and_random(1234, [0x71; 10]).unwrap(),
        )
        .unwrap();
    drop(store);
    let backup = root.join("backup");
    RedbOfflineBackup::bind(&source, &backup)
        .create_offline_backup(
            &BackupBuildMetadataV1::new("0.1.0", "abc", "rustc", 1, vec![]).unwrap(),
        )
        .unwrap();
    let cancel = Arc::new(AtomicBool::new(false));
    let (_, owner) = RedbOfflineRestore::bind(&backup, root.join("stage"))
        .restore_owned_stamp_preparation(&root.join("configured.redb"), Arc::clone(&cancel))
        .unwrap();
    let key = ReadableDigestKey::v1(DigestKeyId::new(1).unwrap());
    let inputs = StartupValidationInputs::new(
        Timestamp::new(1, 0).unwrap(),
        ReadableCapabilityDigestInventory::new(vec![key]).unwrap(),
        ReadableIdempotencyDigestInventory::new(vec![key]).unwrap(),
    );
    let (owner, evidence) = owner.validate_readiness(inputs).unwrap();
    drop(evidence);
    let closed = owner
        .into_stamp_preparation()
        .unwrap()
        .stamp_and_close(2)
        .unwrap();
    (closed, backup, cancel)
}

fn target(root: &ScopedDirectory) -> Arc<PinnedDirectory> {
    let path = root.join("target");
    fs::create_dir(&path).unwrap();
    Arc::new(PinnedDirectory::open(&path).unwrap())
}

fn names(values: [&str; 3]) -> [&OsStr; 3] {
    values.map(OsStr::new)
}

fn paths(owner: &OwnedClosedMaterialization) -> [PathBuf; 3] {
    [
        owner.materialization.primary.path().to_path_buf(),
        owner.materialization.journal.path().to_path_buf(),
        owner.materialization.marker.path().to_path_buf(),
    ]
}

fn mutate(path: &Path) {
    use std::io::{Seek, SeekFrom};
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    let modified = file.metadata().unwrap().modified().unwrap();
    let mut byte = [0];
    file.read_exact(&mut byte).unwrap();
    byte[0] ^= 0x40;
    file.seek(SeekFrom::Start(0)).unwrap();
    file.write_all(&byte).unwrap();
    file.set_times(fs::FileTimes::new().set_modified(modified))
        .unwrap();
    file.sync_all().unwrap();
}

#[test]
fn publication_artifact_handoff_preserves_original_descriptors_seals_and_native_exclusion() {
    let root = ScopedDirectory::new("publication-artifact-set");
    let (closed, backup, _) = fixture(&root);
    let originals = paths(&closed);
    let original_identity = originals.each_ref().map(|path| {
        let m = fs::metadata(path).unwrap();
        (m.dev(), m.ino(), m.len())
    });
    let frozen = closed.checksums().unwrap();
    let before = validate_immutable_backup(&backup).unwrap();
    let mut owner = closed.into_publication_artifacts().unwrap();
    let target = target(&root);
    owner
        .move_to_temporaries(Arc::clone(&target), names(TEMP))
        .unwrap();
    for (index, name) in TEMP.into_iter().enumerate() {
        assert!(!originals[index].exists());
        let temporary = target.child_path(OsStr::new(name)).unwrap();
        let m = fs::metadata(&temporary).unwrap();
        assert_eq!((m.dev(), m.ino(), m.len()), original_identity[index]);
        assert_eq!(m.nlink(), 1);
        if index == 0 {
            crate::maintenance::probe_restore_custody(&temporary, true);
        }
    }
    owner.publish(Arc::clone(&target), names(FINAL)).unwrap();
    for (index, checksum) in [frozen.0, frozen.1, frozen.2].into_iter().enumerate() {
        let path = target.child_path(OsStr::new(FINAL[index])).unwrap();
        assert_eq!(sha256_file(&path).unwrap(), checksum);
        let m = fs::metadata(&path).unwrap();
        assert_eq!((m.dev(), m.ino(), m.len()), original_identity[index]);
        assert!(!target.child_path(OsStr::new(TEMP[index])).unwrap().exists());
    }
    let primary = target.child_path(OsStr::new(FINAL[0])).unwrap();
    crate::maintenance::probe_restore_custody(&primary, true);
    drop(owner);
    crate::maintenance::probe_restore_custody(&primary, false);
    assert_eq!(validate_immutable_backup(&backup).unwrap(), before);
}

#[test]
fn publication_artifact_handoff_refuses_same_length_mtime_restored_mutation_of_each_source() {
    for index in 0..3 {
        let root = ScopedDirectory::new("publication-artifact-source-mutation");
        let (closed, backup, _) = fixture(&root);
        let originals = paths(&closed);
        let before = validate_immutable_backup(&backup).unwrap();
        mutate(&originals[index]);
        assert!(closed.into_publication_artifacts().is_err());
        for path in originals {
            assert!(path.exists());
        }
        assert_eq!(validate_immutable_backup(&backup).unwrap(), before);
        assert!(!root.join("configured.redb").exists());
    }
}

#[test]
fn publication_artifact_readback_refuses_mutation_during_the_known_rename_span_without_resealing() {
    for (index, temporary_name) in TEMP.into_iter().enumerate() {
        let root = ScopedDirectory::new("publication-artifact-readback");
        let (closed, _, _) = fixture(&root);
        let mut owner = closed.into_publication_artifacts().unwrap();
        let checksums = owner.checksums.clone();
        let target = target(&root);
        for (ordinal, name) in names(TEMP).into_iter().enumerate() {
            owner.move_one(ordinal, Arc::clone(&target), name).unwrap();
        }
        let path = target.child_path(OsStr::new(temporary_name)).unwrap();
        mutate(&path);
        // Model the metadata observation permitted by a known rename. Even if
        // that observation includes the write, it cannot create a new seal.
        owner.continuity[index] =
            ArtifactContinuity::read(owner.artifacts[index].private_file().unwrap()).unwrap();
        assert!(owner.verify_full().is_err());
        assert!(owner.checksums == checksums);
        assert!(owner.publish(Arc::clone(&target), names(FINAL)).is_err());
        for name in FINAL {
            assert!(!target.child_path(OsStr::new(name)).unwrap().exists());
        }
        drop(owner);
        for name in TEMP {
            assert!(target.child_path(OsStr::new(name)).unwrap().exists());
        }
    }
}

#[test]
fn publication_artifact_publish_refuses_changes_after_readback_before_any_final_rename() {
    for temporary_name in TEMP {
        let root = ScopedDirectory::new("publication-artifact-publish-mutation");
        let (closed, _, _) = fixture(&root);
        let mut owner = closed.into_publication_artifacts().unwrap();
        let target = target(&root);
        owner
            .move_to_temporaries(Arc::clone(&target), names(TEMP))
            .unwrap();
        mutate(&target.child_path(OsStr::new(temporary_name)).unwrap());
        assert!(owner.publish(Arc::clone(&target), names(FINAL)).is_err());
        for name in FINAL {
            assert!(!target.child_path(OsStr::new(name)).unwrap().exists());
        }
    }
}

#[test]
fn publication_artifact_publish_refuses_cancellation_and_preserves_transferred_evidence() {
    let root = ScopedDirectory::new("publication-artifact-cancel");
    let (closed, backup, cancel) = fixture(&root);
    let before = validate_immutable_backup(&backup).unwrap();
    let mut owner = closed.into_publication_artifacts().unwrap();
    let target = target(&root);
    owner
        .move_to_temporaries(Arc::clone(&target), names(TEMP))
        .unwrap();
    cancel.store(true, Ordering::Release);
    assert!(owner.publish(Arc::clone(&target), names(FINAL)).is_err());
    drop(owner);
    for name in TEMP {
        assert!(target.child_path(OsStr::new(name)).unwrap().exists());
    }
    for name in FINAL {
        assert!(!target.child_path(OsStr::new(name)).unwrap().exists());
    }
    assert_eq!(validate_immutable_backup(&backup).unwrap(), before);
}

#[test]
fn publication_artifact_handoff_refuses_a_link_alias_on_any_original_artifact() {
    for index in 0..3 {
        let root = ScopedDirectory::new("publication-artifact-alias");
        let (closed, _, _) = fixture(&root);
        let originals = paths(&closed);
        fs::hard_link(&originals[index], root.join("foreign-alias")).unwrap();
        assert!(closed.into_publication_artifacts().is_err());
        assert!(originals[index].exists());
        assert!(root.join("foreign-alias").exists());
    }
}
