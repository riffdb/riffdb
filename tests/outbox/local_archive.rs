#![forbid(unsafe_code)]
#![cfg(target_os = "linux")]
// req: EFF-002, EFF-003, STO-020, REC-001, REC-002
//! Durable destination identity, quota, and restart semantics.

use std::num::NonZeroU64;

use riffdb_outbox::{DurableLocalArchive, LocalArchiveError};
use riffdb_storage_api::{LocalOutboxArchiveRecordV1, StoredDurableEventV1, derive_event_hash_v1};
use riffdb_types::{CanonicalRecord, CommitSequence, DatabaseId, EventId, EventTypeId};

fn private_directory() -> tempfile::TempDir {
    use std::os::unix::fs::PermissionsExt;
    tempfile::Builder::new()
        .permissions(std::fs::Permissions::from_mode(0o700))
        .tempdir()
        .unwrap()
}

fn database(seed: u8) -> DatabaseId {
    DatabaseId::from_unix_milliseconds_and_random(42, [seed; 10]).unwrap()
}

fn record(incarnation: u64, event_type: u32) -> LocalOutboxArchiveRecordV1 {
    let id = EventId::new(CommitSequence::first(), 0);
    let kind = EventTypeId::try_from(event_type).unwrap();
    let payload = CanonicalRecord::new(vec![]).unwrap();
    let hash = derive_event_hash_v1(id, kind, &payload).unwrap();
    LocalOutboxArchiveRecordV1::new(
        database(1),
        NonZeroU64::new(incarnation).unwrap(),
        StoredDurableEventV1::new(id, kind, payload, hash).unwrap(),
    )
}

fn validated(archive: &mut DurableLocalArchive) {
    for _ in 0..16 {
        if archive.validate_next_page().unwrap() {
            return;
        }
    }
    panic!("small fixture must finish bounded validation");
}

#[test]
fn restart_and_equal_retry_preserve_one_exact_record_and_atomic_accounting() {
    let root = private_directory();
    let mut archive =
        DurableLocalArchive::open(root.path(), database(1), NonZeroU64::new(4096).unwrap())
            .unwrap();
    validated(&mut archive);
    let event = record(1, 1);
    archive.accept(&event).unwrap();
    let first = archive.observation().unwrap();
    archive.accept(&event).unwrap();
    assert_eq!(archive.observation().unwrap(), first);
    assert_eq!(first.records(), 1);
    assert_eq!(archive.read(&event.key()).unwrap(), Some(event.clone()));
    drop(archive);
    let mut archive =
        DurableLocalArchive::open(root.path(), database(1), NonZeroU64::new(4096).unwrap())
            .unwrap();
    validated(&mut archive);
    assert_eq!(archive.observation().unwrap(), first);
    assert_eq!(archive.read(&event.key()).unwrap(), Some(event.clone()));
    archive.accept(&event).unwrap();
    assert_eq!(archive.observation().unwrap(), first);
}

#[test]
fn an_identity_conflict_never_replaces_a_durable_record() {
    let root = private_directory();
    let mut archive =
        DurableLocalArchive::open(root.path(), database(1), NonZeroU64::new(4096).unwrap())
            .unwrap();
    validated(&mut archive);
    archive.accept(&record(1, 1)).unwrap();
    assert_eq!(
        archive.accept(&record(1, 2)),
        Err(LocalArchiveError::Integrity)
    );
    assert_eq!(
        archive.read(&record(1, 1).key()).unwrap(),
        Some(record(1, 1))
    );
    assert_eq!(archive.observation().unwrap().records(), 1);
}

#[test]
fn quota_refusal_does_not_mutate_and_incarnations_are_distinct() {
    let root = private_directory();
    let mut archive =
        DurableLocalArchive::open(root.path(), database(1), NonZeroU64::new(4096).unwrap())
            .unwrap();
    validated(&mut archive);
    archive.accept(&record(1, 1)).unwrap();
    let first = archive.observation().unwrap();
    drop(archive);
    let mut archive = DurableLocalArchive::open(
        root.path(),
        database(1),
        NonZeroU64::new(first.encoded_bytes()).unwrap(),
    )
    .unwrap();
    validated(&mut archive);
    archive.accept(&record(1, 1)).unwrap();
    assert_eq!(archive.accept(&record(2, 1)), Err(LocalArchiveError::Quota));
    assert_eq!(archive.observation().unwrap(), first);
    drop(archive);
    let mut archive =
        DurableLocalArchive::open(root.path(), database(1), NonZeroU64::new(4096).unwrap())
            .unwrap();
    validated(&mut archive);
    archive.accept(&record(2, 1)).unwrap();
    assert_eq!(archive.observation().unwrap().records(), 2);
}

#[test]
fn another_database_cannot_adopt_the_archive() {
    let root = private_directory();
    drop(
        DurableLocalArchive::open(root.path(), database(1), NonZeroU64::new(4096).unwrap())
            .unwrap(),
    );
    assert!(matches!(
        DurableLocalArchive::open(root.path(), database(2), NonZeroU64::new(4096).unwrap()),
        Err(LocalArchiveError::Binding)
    ));
}

#[test]
fn corrupted_accounting_key_and_record_are_refused_before_delivery() {
    use redb::{Database, TableDefinition};
    const ROWS: TableDefinition<&[u8], &[u8]> =
        TableDefinition::new("local_outbox_archive_records_v1");
    const COUNTERS: TableDefinition<u8, u64> =
        TableDefinition::new("local_outbox_archive_counters_v1");
    for corruption in ["counter", "key", "body", "oversized"] {
        let root = private_directory();
        let mut archive =
            DurableLocalArchive::open(root.path(), database(1), NonZeroU64::MAX).unwrap();
        validated(&mut archive);
        let event = record(1, 1);
        archive.accept(&event).unwrap();
        drop(archive);
        let engine = Database::open(root.path().join("archive.redb")).unwrap();
        let transaction = engine.begin_write().unwrap();
        match corruption {
            "counter" => {
                transaction
                    .open_table(COUNTERS)
                    .unwrap()
                    .insert(1, 2)
                    .unwrap();
            }
            "key" => {
                let mut rows = transaction.open_table(ROWS).unwrap();
                let body = rows
                    .remove(event.key().as_slice())
                    .unwrap()
                    .unwrap()
                    .value()
                    .to_vec();
                rows.insert(record(2, 1).key().as_slice(), body.as_slice())
                    .unwrap();
            }
            "body" => {
                transaction
                    .open_table(ROWS)
                    .unwrap()
                    .insert(event.key().as_slice(), &[1, 2, 3][..])
                    .unwrap();
            }
            "oversized" => {
                transaction
                    .open_table(ROWS)
                    .unwrap()
                    .insert(
                        event.key().as_slice(),
                        vec![0; 4 * 1024 * 1024 + 1].as_slice(),
                    )
                    .unwrap();
            }
            _ => unreachable!(),
        }
        transaction.commit().unwrap();
        drop(engine);
        let mut archive =
            DurableLocalArchive::open(root.path(), database(1), NonZeroU64::MAX).unwrap();
        assert_eq!(
            archive.validate_next_page(),
            Err(LocalArchiveError::Integrity)
        );
        assert!(!archive.validated());
        assert_eq!(
            archive.accept(&record(3, 1)),
            Err(LocalArchiveError::Unavailable)
        );
    }
}

#[test]
fn protected_custody_refuses_links_permission_changes_and_directory_replacement() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let root = private_directory();
    let path = root.path().join("destination");
    std::fs::create_dir(&path).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
    let alias = root.path().join("alias");
    symlink(&path, &alias).unwrap();
    assert!(matches!(
        DurableLocalArchive::open(&alias, database(1), NonZeroU64::MAX),
        Err(LocalArchiveError::UnsafePath)
    ));
    let mut archive = DurableLocalArchive::open(&path, database(1), NonZeroU64::MAX).unwrap();
    validated(&mut archive);
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert_eq!(
        archive.accept(&record(1, 1)),
        Err(LocalArchiveError::UnsafePath)
    );
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
    assert_eq!(archive.observation().unwrap().records(), 0);
    let extra_link = root.path().join("linked-engine");
    std::fs::hard_link(path.join("archive.redb"), &extra_link).unwrap();
    assert_eq!(
        archive.accept(&record(1, 1)),
        Err(LocalArchiveError::UnsafePath)
    );
    std::fs::remove_file(extra_link).unwrap();
    archive.accept(&record(1, 1)).unwrap();
    std::fs::rename(&path, root.path().join("retained")).unwrap();
    std::fs::create_dir(&path).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
    assert_eq!(
        archive.accept(&record(2, 1)),
        Err(LocalArchiveError::UnsafePath)
    );
    assert!(!path.join("archive.redb").exists());
}

#[test]
fn inspection_does_not_create_a_missing_archive_and_requires_completed_validation() {
    let root = private_directory();
    assert!(matches!(
        DurableLocalArchive::open_existing(root.path(), database(1)),
        Err(LocalArchiveError::Unavailable)
    ));
    assert!(!root.path().join("archive.redb").exists());
    let mut archive = DurableLocalArchive::open(root.path(), database(1), NonZeroU64::MAX).unwrap();
    assert_eq!(
        archive.accept(&record(1, 1)),
        Err(LocalArchiveError::Unavailable)
    );
    validated(&mut archive);
    for incarnation in 1..=300 {
        archive.accept(&record(incarnation, 1)).unwrap();
    }
    drop(archive);
    let mut archive = DurableLocalArchive::open_existing(root.path(), database(1)).unwrap();
    assert!(!archive.validate_next_page().unwrap());
    assert!(!archive.validated());
    assert_eq!(
        archive.accept(&record(301, 1)),
        Err(LocalArchiveError::Unavailable)
    );
    assert!(archive.validate_next_page().unwrap());
    assert_eq!(archive.observation().unwrap().records(), 300);
}

#[cfg(feature = "test-fixtures")]
mod crash {
    use super::*;
    use riffdb_outbox::{LocalArchiveCrashHooks, LocalArchiveCrashPoint};
    use std::io::{BufRead, Read, Write};
    use std::process::{Command, Stdio};
    use std::sync::{Arc, mpsc};
    use std::time::Duration;

    struct PipeHook(LocalArchiveCrashPoint);
    impl LocalArchiveCrashHooks for PipeHook {
        fn reached(&self, point: LocalArchiveCrashPoint) {
            if point == self.0 {
                println!("archive-crash-boundary");
                std::io::stdout().flush().unwrap();
                let mut byte = [0];
                std::io::stdin().read_exact(&mut byte).unwrap();
                panic!("parent must kill the blocked process");
            }
        }
    }

    #[test]
    fn archive_crash_child() {
        let Ok(path) = std::env::var("RIFFDB_LOCAL_ARCHIVE_CRASH_DIRECTORY") else {
            return;
        };
        let point = match std::env::var("RIFFDB_LOCAL_ARCHIVE_CRASH_POINT")
            .unwrap()
            .as_str()
        {
            "init-before" => LocalArchiveCrashPoint::InitializationBeforeCommit,
            "init-after" => LocalArchiveCrashPoint::InitializationAfterCommit,
            "record-before" => LocalArchiveCrashPoint::RecordBeforeCommit,
            "record-after" => LocalArchiveCrashPoint::RecordAfterCommit,
            _ => panic!("closed test point"),
        };
        let mut archive = DurableLocalArchive::open_with_crash_hooks(
            std::path::Path::new(&path),
            database(1),
            NonZeroU64::MAX,
            Arc::new(PipeHook(point)),
        )
        .unwrap();
        validated(&mut archive);
        archive.accept(&record(1, 1)).unwrap();
        panic!("selected boundary must block");
    }

    #[test]
    fn process_kill_at_each_publication_boundary_preserves_atomic_records_and_counters() {
        for point in ["init-before", "init-after", "record-before", "record-after"] {
            let root = private_directory();
            let mut child = Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "crash::archive_crash_child", "--nocapture"])
                .env("RIFFDB_LOCAL_ARCHIVE_CRASH_DIRECTORY", root.path())
                .env("RIFFDB_LOCAL_ARCHIVE_CRASH_POINT", point)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .spawn()
                .unwrap();
            let stdout = child.stdout.take().unwrap();
            let (sender, receiver) = mpsc::sync_channel(1);
            let reader = std::thread::spawn(move || {
                // Child output is constant, bounded test harness text.
                let mut lines = std::io::BufReader::new(stdout).take(4096);
                let mut line = String::new();
                while lines.read_line(&mut line).unwrap() != 0 {
                    if line.trim() == "archive-crash-boundary" {
                        sender.send(()).unwrap();
                        return;
                    }
                    line.clear();
                }
            });
            let reached = receiver.recv_timeout(Duration::from_secs(30));
            child.kill().unwrap();
            child.wait().unwrap();
            reader.join().unwrap();
            reached.expect("explicit crash pipe boundary was not reached");
            for _ in 0..3 {
                let mut archive =
                    DurableLocalArchive::open(root.path(), database(1), NonZeroU64::MAX).unwrap();
                validated(&mut archive);
                let committed = point == "record-after";
                assert_eq!(
                    archive.observation().unwrap().records(),
                    u64::from(committed)
                );
                assert_eq!(
                    archive.read(&record(1, 1).key()).unwrap().is_some(),
                    committed
                );
                if committed {
                    let before = archive.observation().unwrap();
                    archive.accept(&record(1, 1)).unwrap();
                    assert_eq!(archive.observation().unwrap(), before);
                } else {
                    assert_eq!(archive.observation().unwrap().encoded_bytes(), 0);
                }
            }
        }
    }
}
