//! Real-file checkpoint-sized writes, final-fence failures, and abrupt reopen.
// req: STO-001, REC-001, REC-002

use super::*;
use redb::{ReadableDatabase, TableDefinition};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

const ROWS: TableDefinition<u64, &[u8]> = TableDefinition::new("rewrite-proof");
const COUNT: u64 = 384;
const BYTES: usize = 64 * 1024;
const CHILD_PATH: &str = "RIFFDB_WP810_REWRITE_CRASH_PATH";
const CHILD_EDGE: &str = "RIFFDB_WP810_REWRITE_CRASH_EDGE";

#[derive(Debug, Default)]
struct IoCounts {
    armed: AtomicBool,
    writes: AtomicUsize,
    syncs: AtomicUsize,
}

#[derive(Debug)]
struct RealBackend {
    file: redb::backends::FileBackend,
    counts: Arc<IoCounts>,
    edge: Option<String>,
}

impl StorageBackend for RealBackend {
    fn try_lock_range(
        &self,
        start: std::ops::Bound<u64>,
        end: std::ops::Bound<u64>,
    ) -> Result<bool, redb::BackendError> {
        self.file.try_lock_range(start, end)
    }

    fn try_lock_shared_range(
        &self,
        start: std::ops::Bound<u64>,
        end: std::ops::Bound<u64>,
    ) -> Result<bool, redb::BackendError> {
        self.file.try_lock_shared_range(start, end)
    }

    fn lock_range(
        &self,
        start: std::ops::Bound<u64>,
        end: std::ops::Bound<u64>,
    ) -> Result<(), redb::BackendError> {
        self.file.lock_range(start, end)
    }

    fn lock_shared_range(
        &self,
        start: std::ops::Bound<u64>,
        end: std::ops::Bound<u64>,
    ) -> Result<(), redb::BackendError> {
        self.file.lock_shared_range(start, end)
    }

    fn unlock_range(
        &self,
        start: std::ops::Bound<u64>,
        end: std::ops::Bound<u64>,
    ) -> Result<(), redb::BackendError> {
        self.file.unlock_range(start, end)
    }

    fn query_lock_range(
        &self,
        start: std::ops::Bound<u64>,
        end: std::ops::Bound<u64>,
    ) -> Result<bool, redb::BackendError> {
        self.file.query_lock_range(start, end)
    }

    fn len(&self) -> io::Result<u64> {
        self.file.len()
    }
    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.file.read(offset, out)
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        self.file.set_len(len)
    }
    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.file.write(offset, data)?;
        if self.counts.armed.load(Ordering::SeqCst) {
            self.counts.writes.fetch_add(data.len(), Ordering::SeqCst);
        }
        Ok(())
    }
    fn sync_data(&self) -> io::Result<()> {
        if self.counts.armed.load(Ordering::SeqCst) {
            match self.edge.as_deref() {
                Some("before-sync") => std::process::exit(73),
                Some("fail-sync") => return Err(io::Error::other("injected final sync failure")),
                _ => (),
            }
            self.counts.syncs.fetch_add(1, Ordering::SeqCst);
        }
        self.file.sync_data()?;
        if self.counts.armed.load(Ordering::SeqCst) && self.edge.as_deref() == Some("after-sync") {
            std::process::exit(73);
        }
        Ok(())
    }
    fn close(&self) -> io::Result<()> {
        self.file.close()
    }
}

fn open(
    path: &std::path::Path,
    state: Arc<CheckpointRewrite>,
    counts: Arc<IoCounts>,
    edge: Option<String>,
) -> redb::Database {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .unwrap();
    let mut builder = redb::Builder::new();
    builder.set_cache_size(4 * 1024 * 1024);
    builder
        .create_with_backend(CheckpointRewriteBackend::new(
            RealBackend {
                file: redb::backends::FileBackend::new(file).unwrap(),
                counts,
                edge,
            },
            state,
        ))
        .unwrap()
}

fn stage(db: &redb::Database, generation: u8) -> redb::WriteTransaction {
    let mut transaction = db.begin_write().unwrap();
    transaction.set_two_phase_commit(false);
    transaction
        .set_durability(redb::Durability::Immediate)
        .unwrap();
    {
        let mut table = transaction.open_table(ROWS).unwrap();
        for row in 0..COUNT {
            let mut value = vec![generation; BYTES];
            value[..8].copy_from_slice(&row.to_le_bytes());
            table.insert(row, value.as_slice()).unwrap();
        }
    }
    transaction
}

fn generation(db: &redb::Database) -> u8 {
    let read = db.begin_read().unwrap();
    let table = read.open_table(ROWS).unwrap();
    let expected = table.get(0).unwrap().unwrap().value()[8];
    for row in 0..COUNT {
        let value = table.get(row).unwrap().unwrap();
        assert_eq!(&value.value()[..8], &row.to_le_bytes());
        assert!(value.value()[8..].iter().all(|byte| *byte == expected));
    }
    expected
}

#[test]
fn real_redb_elides_redundant_finalization_writes_but_syncs_and_reopens() {
    let directory = crate::test_path::ScopedDirectory::new("rewrite-real-redb");
    let mut final_bytes = Vec::new();
    for enabled in [false, true] {
        let path = directory.join(if enabled { "candidate" } else { "control" });
        let state = Arc::new(CheckpointRewrite::default());
        let counts = Arc::new(IoCounts::default());
        let db = open(&path, state.clone(), counts.clone(), None);
        let tx = stage(&db, 1);
        let _scope = enabled.then(|| state.enter().unwrap());
        counts.armed.store(true, Ordering::SeqCst);
        tx.commit().unwrap();
        counts.armed.store(false, Ordering::SeqCst);
        assert_eq!(counts.syncs.load(Ordering::SeqCst), 1);
        final_bytes.push(counts.writes.load(Ordering::SeqCst));
        assert_eq!(generation(&db), 1);
        drop(db);
        assert_eq!(generation(&redb::Database::open(&path).unwrap()), 1);
    }
    assert!(
        final_bytes[1] < final_bytes[0] / 4,
        "finalization writes: {final_bytes:?}"
    );
}

#[test]
fn real_redb_final_sync_failure_never_reports_success_and_fences_new_writes() {
    let directory = crate::test_path::ScopedDirectory::new("rewrite-final-failure");
    let state = Arc::new(CheckpointRewrite::default());
    let counts = Arc::new(IoCounts::default());
    let db = open(
        &directory.join("db"),
        state.clone(),
        counts.clone(),
        Some("fail-sync".into()),
    );
    let tx = stage(&db, 1);
    let _scope = state.enter().unwrap();
    counts.armed.store(true, Ordering::SeqCst);
    assert!(tx.commit().is_err());
    assert!(db.begin_write().is_err());
}

#[test]
fn rewrite_process_crash_child() {
    let Some(path) = std::env::var_os(CHILD_PATH) else {
        return;
    };
    let edge = std::env::var(CHILD_EDGE).unwrap();
    let state = Arc::new(CheckpointRewrite::default());
    let counts = Arc::new(IoCounts::default());
    let db = open(
        std::path::Path::new(&path),
        state.clone(),
        counts.clone(),
        Some(edge),
    );
    let tx = stage(&db, 2);
    let _scope = state.enter().unwrap();
    counts.armed.store(true, Ordering::SeqCst);
    tx.commit().unwrap();
    panic!("crash edge did not run");
}

#[test]
fn rewrite_final_fence_crashes_reopen_atomically_and_idempotently() {
    let directory = crate::test_path::ScopedDirectory::new("rewrite-crashes");
    for edge in ["before-sync", "after-sync"] {
        let path = directory.join(edge);
        let db = redb::Database::create(&path).unwrap();
        stage(&db, 1).commit().unwrap();
        drop(db);
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("checkpoint_rewrite::tests::engine::rewrite_process_crash_child")
            .arg("--nocapture")
            .env(CHILD_PATH, &path)
            .env(CHILD_EDGE, edge)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(73));
        let first = generation(&redb::Database::open(&path).unwrap());
        if edge == "after-sync" {
            assert_eq!(first, 2);
        } else {
            assert!([1, 2].contains(&first));
        }
        assert_eq!(generation(&redb::Database::open(&path).unwrap()), first);
    }
}

// req: STO-001, REC-001, REC-002
#[test]
fn file_backend_wrappers_exclude_another_process_and_release_ownership() {
    const CHILD_LOCK_PATH: &str = "RIFFDB_ENGINE_LOCK_PROOF_PATH";
    const CHILD_LOCK_EXPECTED: &str = "RIFFDB_ENGINE_LOCK_PROOF_EXPECTED";
    if let Some(path) = std::env::var_os(CHILD_LOCK_PATH) {
        let expected = std::env::var(CHILD_LOCK_EXPECTED).unwrap();
        let result = crate::RedbStore::open(std::path::Path::new(&path));
        match expected.as_str() {
            "held" => {
                let Err(error) = result else {
                    panic!("a second process must not acquire the file");
                };
                assert_eq!(
                    error.kind(),
                    riffdb_storage_api::StorageErrorKind::Unavailable
                );
            }
            "released" => assert!(result.is_ok(), "closed ownership must be released"),
            _ => panic!("unknown lock proof state"),
        }
        return;
    }
    let directory = crate::test_path::ScopedDirectory::new("file-wrapper-lock");
    for medium in ["native", "dynamic", "external-kill"] {
        let path = directory.join(medium);
        let store = match medium {
            "native" => crate::RedbStore::open(&path).unwrap(),
            "dynamic" => {
                let file = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create_new(true)
                    .open(&path)
                    .unwrap();
                let backend = redb::backends::FileBackend::new(file).unwrap();
                crate::RedbStore::open_with_storage_media(
                    &path,
                    crate::RedbStorageMedia::new(backend, Arc::new(crate::RealJournalMedia)),
                )
                .unwrap()
            }
            "external-kill" => {
                drop(crate::RedbStore::open(&path).unwrap());
                crate::RedbStore::open_with_external_kill_barrier(
                    &path,
                    crate::RedbCommitProfile::Hardened,
                    crate::RedbTestController::observe_index_migration(),
                )
                .unwrap()
            }
            _ => unreachable!(),
        };
        let child = |expected| {
            let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("checkpoint_rewrite::tests::engine::file_backend_wrappers_exclude_another_process_and_release_ownership")
            .env(CHILD_LOCK_PATH, &path)
            .env(CHILD_LOCK_EXPECTED, expected)
            .status()
            .unwrap();
            assert!(status.success(), "two-process lock proof failed");
        };
        child("held");
        drop(store);
        child("released");
    }
}
