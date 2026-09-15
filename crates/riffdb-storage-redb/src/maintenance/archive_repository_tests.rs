//! Exact external archive durability, recovery and path ownership.
// req: REP-007, AFC-007
use super::RedbArchiveRepository;
use riffdb_storage_api::*;
use std::{fs, path::Path};

#[path = "../../../riffdb-storage-api/tests/support/archive_manifest_fixture.rs"]
mod fixture;

fn open(path: &Path) -> Result<RedbArchiveRepository, ArchiveConsumerErrorV1> {
    let (lineage, fence) = fixture::fixture();
    RedbArchiveRepository::open(
        path,
        lineage,
        fence,
        [0x33; 32],
        ArchiveEncryptionPostureV1::Unencrypted,
    )
}
struct Uncertain {
    repository: RedbArchiveRepository,
    once: bool,
}
impl ArchiveFrameSinkV1 for Uncertain {
    fn persist(&mut self, frame: &ArchiveFrameV1) -> Result<(), ArchiveConsumerErrorV1> {
        self.repository.persist(frame)?;
        if std::mem::take(&mut self.once) {
            Err(ArchiveConsumerErrorV1::SinkUnavailable)
        } else {
            Ok(())
        }
    }
}

#[test]
fn archive_repository_exact_retry_and_reopen_preserve_complete_linked_frames() {
    let scope = crate::test_path::ScopedDirectory::new("archive-repository");
    let path = scope.join("archive");
    let (lineage, fence) = fixture::fixture();
    let repository = open(&path).unwrap();
    assert_eq!(repository.position(), fence);
    assert!(repository.head().is_none());
    assert!(open(&path).is_err(), "actual file lock is exclusive");
    let mut consumer = ArchiveConsumerV1::new(
        Uncertain {
            repository,
            once: true,
        },
        lineage,
        fence,
    );
    let first_bytes = fixture::frame(lineage, fence, 1);
    assert_eq!(
        consumer.append(first_bytes.clone()),
        Err(ArchiveConsumerErrorV1::SinkUnavailable)
    );
    assert_eq!(consumer.position(), fence);
    let first = consumer.retry_pending().unwrap();
    consumer.begin_stream().unwrap();
    let second_bytes = fixture::frame(lineage, first, 2);
    let second = consumer.append(second_bytes.clone()).unwrap();
    let repository = consumer.into_sink().repository;
    let head = repository.head().unwrap();
    assert_eq!(head.covered(), second);
    assert_eq!(head.backup_fence(), fence);
    assert_eq!(head.full_backup_manifest_digest(), [0x33; 32]);
    drop(repository);
    let reopened = open(&path).unwrap();
    assert_eq!(reopened.position(), second);
    assert_eq!(reopened.head(), Some(head));
    assert_eq!(
        fs::read(path.join("frame-0000000000000002.v3")).unwrap(),
        first_bytes
    );
    assert_eq!(
        fs::read(path.join("frame-0000000000000003.v3")).unwrap(),
        second_bytes
    );
}

#[test]
fn archive_repository_refuses_changed_binding_and_damaged_prefix_without_cleanup() {
    for mode in 0..4 {
        let scope = crate::test_path::ScopedDirectory::new("archive-corrupt");
        let path = scope.join("archive");
        let (lineage, fence) = fixture::fixture();
        let mut consumer = ArchiveConsumerV1::new(open(&path).unwrap(), lineage, fence);
        let first = consumer.append(fixture::frame(lineage, fence, 1)).unwrap();
        consumer.begin_stream().unwrap();
        consumer.append(fixture::frame(lineage, first, 2)).unwrap();
        drop(consumer);
        fs::write(path.join("frame.tmp"), b"preserve on invalid prefix").unwrap();
        match mode {
            0 => fs::write(path.join("frame-0000000000000002.v3"), b"truncated").unwrap(),
            1 => fs::remove_file(path.join("manifest-0000000000000002.v1")).unwrap(),
            2 => {
                let second = fs::read(path.join("manifest-0000000000000003.v1")).unwrap();
                fs::write(path.join("manifest-0000000000000002.v1"), second).unwrap();
            }
            _ => {}
        }
        let result = if mode == 3 {
            RedbArchiveRepository::open(
                &path,
                lineage,
                fence,
                [0x44; 32],
                ArchiveEncryptionPostureV1::Unencrypted,
            )
        } else {
            open(&path)
        };
        assert!(result.is_err());
        assert_eq!(
            fs::read(path.join("frame.tmp")).unwrap(),
            b"preserve on invalid prefix"
        );
    }
}

#[test]
fn archive_repository_process_crashes_recover_only_a_complete_selected_prefix() {
    const ROOT: &str = "RIFFDB_ARCHIVE_TEST_ROOT";
    const EDGE: &str = "RIFFDB_ARCHIVE_TEST_EDGE";
    let (lineage, fence) = fixture::fixture();
    if let Some(root) = std::env::var_os(ROOT) {
        let mut repository = open(Path::new(&root)).unwrap();
        let before = repository.position();
        repository.crash_at(std::env::var(EDGE).unwrap());
        let mut consumer = ArchiveConsumerV1::new(repository, lineage, before);
        consumer.append(fixture::frame(lineage, before, 2)).unwrap();
        panic!("archive crash boundary did not fire");
    }
    for edge in [
        "frame-staged",
        "frame-linked",
        "frame-published",
        "manifest-staged",
        "manifest-linked",
        "manifest-published",
        "current-staged",
        "current-renamed",
        "current-synced",
    ] {
        let scope = crate::test_path::ScopedDirectory::new("archive-crash");
        let path = scope.join("archive");
        let mut consumer = ArchiveConsumerV1::new(open(&path).unwrap(), lineage, fence);
        let first = consumer.append(fixture::frame(lineage, fence, 1)).unwrap();
        drop(consumer);
        let child = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("maintenance::archive_repository_tests::archive_repository_process_crashes_recover_only_a_complete_selected_prefix")
            .arg("--nocapture")
            .env(ROOT, &path).env(EDGE, edge).output().unwrap();
        assert_eq!(
            child.status.code(),
            Some(97),
            "{edge}: {}",
            String::from_utf8_lossy(&child.stderr)
        );
        let repository = open(&path).unwrap();
        let recovered = repository.position();
        assert!(
            recovered == first || recovered.sequence() == first.sequence().checked_next().unwrap()
        );
        let second_bytes = fixture::frame(lineage, first, 2);
        let mut consumer = ArchiveConsumerV1::new(repository, lineage, recovered);
        let second = if recovered == first {
            consumer.append(second_bytes.clone()).unwrap()
        } else {
            recovered
        };
        assert_eq!(second.frontier().application().unwrap().get(), 2);
        assert_eq!(
            fs::read(path.join("frame-0000000000000003.v3")).unwrap(),
            second_bytes
        );
        drop(consumer);
        let reopened = open(&path).unwrap();
        assert_eq!(reopened.position(), second);
        assert_eq!(
            fs::read_dir(&path).unwrap().count(),
            6,
            "only selected immutable pairs and selector remain"
        );
    }
}

#[test]
fn archive_repository_discards_only_exact_unconfirmed_names_and_bounds_cleanup() {
    let scope = crate::test_path::ScopedDirectory::new("archive-orphan");
    let path = scope.join("archive");
    drop(open(&path).unwrap());
    for name in [
        "frame.tmp",
        "manifest.tmp",
        "current.tmp",
        "frame-0000000000000002.v3",
        "manifest-0000000000000002.v1",
    ] {
        fs::write(path.join(name), b"unconfirmed").unwrap();
    }
    fs::write(path.join("operator-note"), b"must remain").unwrap();
    drop(open(&path).unwrap());
    assert_eq!(
        fs::read(path.join("operator-note")).unwrap(),
        b"must remain"
    );
    assert_eq!(fs::read_dir(&path).unwrap().count(), 2);
    fs::write(path.join("frame.tmp"), b"must remain on bounds failure").unwrap();
    fs::File::create(path.join("manifest.tmp"))
        .unwrap()
        .set_len(ARCHIVE_MANIFEST_V1_BYTES as u64 + 1)
        .unwrap();
    assert!(open(&path).is_err());
    assert_eq!(
        fs::read(path.join("frame.tmp")).unwrap(),
        b"must remain on bounds failure"
    );
}

#[cfg(unix)]
#[test]
fn archive_repository_refuses_symlinks_nonprivate_roots_and_lock_substitution() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let scope = crate::test_path::ScopedDirectory::new("archive-paths");
    let path = scope.join("archive");
    let (lineage, fence) = fixture::fixture();
    let repository = open(&path).unwrap();
    let outside = scope.join("outside");
    fs::write(&outside, b"not archive data").unwrap();
    symlink(&outside, path.join("frame.tmp")).unwrap();
    let mut consumer = ArchiveConsumerV1::new(repository, lineage, fence);
    assert!(consumer.append(fixture::frame(lineage, fence, 1)).is_err());
    assert_eq!(fs::read(&outside).unwrap(), b"not archive data");
    drop(consumer);
    assert!(open(&path).is_err());
    fs::remove_file(path.join("frame.tmp")).unwrap();
    let repository = open(&path).unwrap();
    fs::rename(path.join("inventory.lock"), path.join("old-lock")).unwrap();
    fs::write(path.join("inventory.lock"), b"").unwrap();
    let mut consumer = ArchiveConsumerV1::new(repository, lineage, fence);
    assert_eq!(
        consumer.append(fixture::frame(lineage, fence, 1)),
        Err(ArchiveConsumerErrorV1::ResyncRequired)
    );
    assert!(!path.join("CURRENT").exists());
    drop(consumer);
    fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
    assert!(open(&path).is_err());
}

#[test]
fn archive_repository_cancelled_open_creates_no_inventory() {
    let scope = crate::test_path::ScopedDirectory::new("archive-cancel");
    let path = scope.join("archive");
    let (lineage, fence) = fixture::fixture();
    assert!(
        RedbArchiveRepository::open_cancellable(
            &path,
            lineage,
            fence,
            [0x33; 32],
            ArchiveEncryptionPostureV1::Unencrypted,
            &std::sync::atomic::AtomicBool::new(true)
        )
        .is_err()
    );
    assert!(!path.exists());
}

#[test]
fn archive_repository_uncertain_io_retries_exact_pending_bytes_at_every_boundary() {
    for edge in [
        "frame-staged",
        "frame-linked",
        "frame-published",
        "manifest-staged",
        "manifest-linked",
        "manifest-published",
        "current-staged",
        "current-renamed",
        "current-synced",
    ] {
        let scope = crate::test_path::ScopedDirectory::new("archive-uncertain");
        let path = scope.join("archive");
        let (lineage, fence) = fixture::fixture();
        let repository = open(&path).unwrap();
        repository.fail_once_at(edge);
        let mut consumer = ArchiveConsumerV1::new(repository, lineage, fence);
        let bytes = fixture::frame(lineage, fence, 1);
        assert_eq!(
            consumer.append(bytes.clone()),
            Err(ArchiveConsumerErrorV1::SinkUnavailable),
            "{edge}"
        );
        assert_eq!(consumer.position(), fence);
        assert!(consumer.has_pending_frame());
        let confirmed = consumer.retry_pending().unwrap();
        assert_eq!(confirmed.frontier().application().unwrap().get(), 1);
        assert!(!consumer.has_pending_frame());
        drop(consumer);
        let reopened = open(&path).unwrap();
        assert_eq!(reopened.position(), confirmed);
        assert_eq!(
            fs::read(path.join("frame-0000000000000002.v3")).unwrap(),
            bytes
        );
    }
}
