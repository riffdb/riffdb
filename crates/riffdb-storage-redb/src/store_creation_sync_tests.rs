//! Actual native primary publication and fail-closed directory-sync coverage.
use super::*;
use crate::media::{MediaFile, MediaFileMetadata, RealJournalMedia};
use std::io;

#[derive(Debug)]
struct PrimaryPublicationMedia {
    primary: PathBuf,
    fail: AtomicBool,
    synced_lengths: Mutex<Vec<u64>>,
}

impl PrimaryPublicationMedia {
    fn new(primary: PathBuf, fail: bool) -> Self {
        Self {
            primary,
            fail: AtomicBool::new(fail),
            synced_lengths: Mutex::new(Vec::new()),
        }
    }
}

impl JournalMedia for PrimaryPublicationMedia {
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
        if path == self.primary {
            self.synced_lengths
                .lock()
                .unwrap()
                .push(std::fs::metadata(path)?.len());
            if self.fail.load(Ordering::Acquire) {
                return Err(io::Error::other("injected primary directory-sync failure"));
            }
        }
        RealJournalMedia.sync_parent_all(path)
    }
}

fn open_primary(media: Arc<PrimaryPublicationMedia>) -> Result<RedbStore, StorageError> {
    let path = media.primary.clone();
    let preflight = preflight_durable_format_path_with_media(media.as_ref(), &path)
        .map_err(format_preflight_storage_error)?;
    RedbStore::open_after_format_preflight(
        OpenMode::Source,
        &path,
        RedbCommitProfile::Hardened,
        None,
        default_changelog_port(),
        (preflight == RedbDurableFormatPreflight::InitializeCurrent).then_some(preflight),
        None,
        media,
    )
}

// req: STO-001, REC-001, REC-002
#[test]
fn primary_creation_syncs_its_directory_after_the_engine_file_exists() {
    let scope = crate::test_path::ScopedDirectory::new("primary-publication-sync");
    let media = Arc::new(PrimaryPublicationMedia::new(scope.join("db.redb"), false));
    let store = open_primary(Arc::clone(&media)).unwrap();
    assert_eq!(media.synced_lengths.lock().unwrap().len(), 1);
    assert!(media.synced_lengths.lock().unwrap()[0] > 0);
    assert!(matches!(
        store.probe_database_identity().unwrap(),
        DatabaseIdentityProbe::NeedsInitialization
    ));
    drop(store);
    drop(open_primary(Arc::clone(&media)).unwrap());
    assert_eq!(media.synced_lengths.lock().unwrap().len(), 2);
}

// req: STO-001, REC-001, REC-002
#[test]
fn failed_primary_directory_sync_refuses_open_and_cannot_be_bypassed_on_retry() {
    let scope = crate::test_path::ScopedDirectory::new("primary-publication-failure");
    let media = Arc::new(PrimaryPublicationMedia::new(scope.join("db.redb"), true));
    for _ in 0..2 {
        assert_eq!(
            open_primary(Arc::clone(&media)).err().unwrap().kind(),
            StorageErrorKind::Unavailable
        );
        assert!(std::fs::metadata(&media.primary).unwrap().len() > 0);
    }
    media.fail.store(false, Ordering::Release);
    let mut store = open_primary(Arc::clone(&media)).unwrap();
    let identity =
        DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x76; 10]).unwrap();
    assert_eq!(
        store.initialize_database(identity).unwrap(),
        DatabaseInitializationResult::Installed(identity)
    );
    drop(store);
    let store = open_primary(Arc::clone(&media)).unwrap();
    assert_eq!(
        store.probe_database_identity().unwrap(),
        DatabaseIdentityProbe::Existing(identity)
    );
    assert_eq!(media.synced_lengths.lock().unwrap().len(), 4);
}
