use crate::layout::{META, META_CLEAN_CLOSE_LIFECYCLE};
use crate::media::{
    JournalMedia, JournalMediaDirectory, MediaFile, MediaFileMetadata, RealJournalMedia,
};
use crate::startup::open_cleanly_for_test as open_cleanly;
use crate::store::{
    read_administration_tail, read_commit_tail, read_identity_from_read_transaction,
};
use crate::{RedbOperationalPorts, RedbStore};
use redb::ReadableDatabase;
use riffdb_storage_api::DatabaseInitializationPort;
use riffdb_types::DatabaseId;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug)]
struct SpareCreationMedia {
    preparing: PathBuf,
    creates: AtomicU64,
}

impl JournalMedia for SpareCreationMedia {
    fn open_directory(&self, path: &Path) -> io::Result<Box<dyn JournalMediaDirectory>> {
        RealJournalMedia.open_directory(path)
    }
    fn try_exists(&self, path: &Path) -> io::Result<bool> {
        RealJournalMedia.try_exists(path)
    }
    fn metadata(&self, path: &Path) -> io::Result<MediaFileMetadata> {
        RealJournalMedia.metadata(path)
    }
    fn open_read(&self, path: &Path) -> io::Result<MediaFile> {
        RealJournalMedia.open_read(path)
    }
    fn open_read_write(&self, path: &Path) -> io::Result<MediaFile> {
        RealJournalMedia.open_read_write(path)
    }
    fn create_new_read_write(&self, path: &Path) -> io::Result<MediaFile> {
        if path == self.preparing {
            self.creates.fetch_add(1, Ordering::Relaxed);
        }
        RealJournalMedia.create_new_read_write(path)
    }
    fn create_new_write_only(&self, path: &Path) -> io::Result<MediaFile> {
        RealJournalMedia.create_new_write_only(path)
    }
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        RealJournalMedia.rename(from, to)
    }
    fn remove_file(&self, path: &Path) -> io::Result<()> {
        RealJournalMedia.remove_file(path)
    }
    fn sync_parent_data(&self, path: &Path) -> io::Result<()> {
        RealJournalMedia.sync_parent_data(path)
    }
    fn sync_parent_all(&self, path: &Path) -> io::Result<()> {
        RealJournalMedia.sync_parent_all(path)
    }
}

fn fixture(
    label: &str,
) -> (
    crate::test_path::ScopedDirectory,
    RedbOperationalPorts,
    Arc<SpareCreationMedia>,
) {
    let scope = crate::test_path::ScopedDirectory::new(label);
    let path = scope.join("db.redb");
    let mut store = RedbStore::open(&path).unwrap();
    let database_id =
        DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x74; 10]).unwrap();
    store.initialize_database(database_id).unwrap();
    let media = Arc::new(SpareCreationMedia {
        preparing: crate::journal::preparing_spare_journal_path(&path),
        creates: AtomicU64::new(0),
    });
    Arc::get_mut(&mut store.shared).unwrap().journal_media = media.clone();
    let ports = open_cleanly(store);
    (scope, ports, media)
}

fn assert_clean_without_spare(ports: &RedbOperationalPorts) {
    let before = ports.shared.database.begin_read().unwrap();
    let application = read_commit_tail(&before).unwrap();
    let administration = read_administration_tail(&before).unwrap();
    let identity = read_identity_from_read_transaction(&before).unwrap();
    drop(before);
    let receipt = ports.complete_graceful_close();
    assert_eq!(
        receipt.lifecycle(),
        crate::GracefulLifecycleOutcomeV1::CleanCommitted
    );
    assert!(!crate::journal::spare_journal_path(&ports.shared.path).exists());
    assert!(!crate::journal::preparing_spare_journal_path(&ports.shared.path).exists());
    let after = ports.shared.database.begin_read().unwrap();
    assert_eq!(read_commit_tail(&after).unwrap(), application);
    assert_eq!(read_administration_tail(&after).unwrap(), administration);
    crate::journal::verify_clean_close_header_digest_with_media(
        ports.shared.journal_media.as_ref(),
        &ports.shared.path,
        identity,
        application,
        administration,
    )
    .unwrap();
    let meta = after.open_table(META).unwrap();
    let value = meta.get(META_CLEAN_CLOSE_LIFECYCLE).unwrap().unwrap();
    assert!(matches!(
        crate::clean_close::CleanCloseLifecycle::decode(value.value())
            .unwrap()
            .state(),
        crate::clean_close::CleanCloseState::Clean(_)
    ));
}

// req: REC-001, REC-002, REC-004, STO-032, PERF-019
#[test]
fn graceful_close_does_not_prepare_a_spare_it_immediately_deletes() {
    let (_scope, ports, media) = fixture("graceful-no-unused-spare");
    assert_clean_without_spare(&ports);
    assert_eq!(
        media.creates.load(Ordering::Relaxed),
        0,
        "closing must durably establish its active extent without creating future writer scratch"
    );
}

// req: REC-001, REC-002, REC-004, STO-032, PERF-019
#[test]
fn ordinary_journal_runtime_still_prepares_its_spare_before_close() {
    let (_scope, ports, media) = fixture("writer-retains-spare-preparation");
    // Ordinary writers install their durable root before opening the lane.
    let root = ports.shared.capture_checkpoint_root().unwrap();
    *ports.shared.durable_read_frontier.write().unwrap() = Some(root);
    drop(ports.shared.journal_runtime().unwrap());
    ports.shared.join_journal_sealer();
    assert_eq!(media.creates.load(Ordering::Relaxed), 1);
    assert!(crate::journal::spare_journal_path(&ports.shared.path).is_file());
    assert_clean_without_spare(&ports);
    assert_eq!(media.creates.load(Ordering::Relaxed), 1);
}

// req: REC-001, REC-002, REC-004, STO-032, PERF-019
#[test]
fn failed_close_leaves_later_writer_spare_preparation_intact() {
    let (_scope, mut ports, media) = fixture("failed-close-writer-spare");
    Arc::get_mut(&mut ports.shared).unwrap().test_controller =
        Some(crate::RedbTestController::return_before_commit(
            crate::RedbTestOperation::GracefulCheckpointClassification,
        ));
    let receipt = ports.complete_graceful_close();
    assert_eq!(
        receipt.disposition(),
        crate::GracefulCheckpointDispositionV1::ClassificationFailed
    );
    assert_eq!(
        receipt.lifecycle(),
        crate::GracefulLifecycleOutcomeV1::CleanNotAttempted
    );
    assert_eq!(media.creates.load(Ordering::Relaxed), 0);
    assert!(ports.shared.journal_runtime.lock().unwrap().is_none());
    let read = ports.shared.database.begin_read().unwrap();
    let meta = read.open_table(META).unwrap();
    let value = meta.get(META_CLEAN_CLOSE_LIFECYCLE).unwrap().unwrap();
    assert_eq!(
        crate::clean_close::CleanCloseLifecycle::decode(value.value())
            .unwrap()
            .state(),
        crate::clean_close::CleanCloseState::Dirty
    );
    drop(value);
    drop(meta);
    drop(read);
    drop(ports.shared.journal_runtime().unwrap());
    ports.shared.join_journal_sealer();
    assert_eq!(media.creates.load(Ordering::Relaxed), 1);
    assert!(crate::journal::spare_journal_path(&ports.shared.path).is_file());
}
