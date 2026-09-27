//! Real redb I/O and abrupt exits at the added early durability boundary.
// req: STO-001, REC-001, REC-002

use super::*;
use redb::{Database, ReadableDatabase, ReadableTableMetadata, TableDefinition};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

const ROWS: TableDefinition<u64, &[u8]> = TableDefinition::new("checkpoint-writeback-proof");
const ROW_COUNT: u64 = 384;
const VALUE_BYTES: usize = 64 * 1024;
const CHILD_PATH: &str = "RIFFDB_WP810_WRITEBACK_CRASH_PATH";
const CHILD_EDGE: &str = "RIFFDB_WP810_WRITEBACK_CRASH_EDGE";

#[derive(Clone, Copy, Debug)]
enum Action {
    Observe,
    Fail,
    ExitBefore,
    ExitAfter,
}

#[derive(Debug, Default)]
struct PhysicalIo {
    armed: AtomicBool,
    unsynced: AtomicUsize,
    largest_unsynced: AtomicUsize,
    budget_fences: AtomicUsize,
}

#[derive(Debug)]
struct PhysicalBackend {
    file: redb::backends::FileBackend,
    io: Arc<PhysicalIo>,
    action: Action,
}

impl StorageBackend for PhysicalBackend {
    fn len(&self) -> io::Result<u64> {
        self.file.len()
    }
    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.file.read(offset, out)
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        self.file.set_len(len)
    }
    fn sync_data(&self) -> io::Result<()> {
        let early = self.io.armed.load(Ordering::SeqCst)
            && self.io.unsynced.load(Ordering::SeqCst) == WRITEBACK_BYTES;
        if early {
            self.io.budget_fences.fetch_add(1, Ordering::SeqCst);
            match self.action {
                Action::Fail => {
                    return Err(io::Error::other("injected early checkpoint sync failure"));
                }
                Action::ExitBefore => std::process::exit(73),
                Action::Observe | Action::ExitAfter => {}
            }
        }
        self.file.sync_data()?;
        self.io.unsynced.store(0, Ordering::SeqCst);
        if early && matches!(self.action, Action::ExitAfter) {
            std::process::exit(73);
        }
        Ok(())
    }
    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.file.write(offset, data)?;
        let pending = self.io.unsynced.fetch_add(data.len(), Ordering::SeqCst) + data.len();
        if self.io.armed.load(Ordering::SeqCst) {
            self.io
                .largest_unsynced
                .fetch_max(pending, Ordering::SeqCst);
        }
        Ok(())
    }
    fn close(&self) -> io::Result<()> {
        self.file.close()
    }
}

fn open(path: &Path, action: Action) -> (Database, Arc<CheckpointWriteback>, Arc<PhysicalIo>) {
    let state = Arc::new(CheckpointWriteback::default());
    let observed = Arc::new(PhysicalIo::default());
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .unwrap();
    let backend = PhysicalBackend {
        file: redb::backends::FileBackend::new(file).unwrap(),
        io: Arc::clone(&observed),
        action,
    };
    let mut builder = redb::Builder::new();
    builder.set_cache_size(4 * 1024 * 1024); // Production cache size.
    let database = builder
        .create_with_backend(CheckpointWritebackBackend::new(backend, Arc::clone(&state)))
        .unwrap();
    (database, state, observed)
}

fn seed(path: &Path) {
    let (database, _, _) = open(path, Action::Observe);
    let transaction = database.begin_write().unwrap();
    transaction
        .open_table(ROWS)
        .unwrap()
        .insert(0, b"original".as_slice())
        .unwrap();
    transaction.commit().unwrap();
}

fn checkpoint(
    database: &Database,
    state: &CheckpointWriteback,
    observed: &PhysicalIo,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut transaction = database.begin_write()?;
    let scope = state.enter()?;
    transaction.set_two_phase_commit(false);
    transaction.set_durability(redb::Durability::Immediate)?;
    observed.armed.store(true, Ordering::SeqCst);
    {
        let mut table = transaction.open_table(ROWS)?;
        for key in 1..=ROW_COUNT {
            table.insert(key, vec![key as u8; VALUE_BYTES].as_slice())?;
        }
    }
    transaction.commit()?;
    drop(scope);
    Ok(())
}

fn assert_reopened(path: &Path, committed: bool) {
    for _ in 0..2 {
        let database = Database::create(path).unwrap();
        let transaction = database.begin_read().unwrap();
        let table = transaction.open_table(ROWS).unwrap();
        assert_eq!(table.get(0).unwrap().unwrap().value(), b"original");
        assert_eq!(
            table.len().unwrap(),
            if committed { ROW_COUNT + 1 } else { 1 }
        );
        for key in 1..=ROW_COUNT {
            let row = table.get(key).unwrap();
            if committed {
                assert_eq!(row.unwrap().value(), vec![key as u8; VALUE_BYTES]);
            } else {
                assert!(
                    row.is_none(),
                    "early durability cannot commit a partial transaction"
                );
            }
        }
    }
}

#[test]
fn checkpoint_writeback_real_engine_commits_exactly_or_refuses_early_sync_failure() {
    for action in [Action::Observe, Action::Fail] {
        let directory = crate::test_path::ScopedDirectory::new("checkpoint-writeback-engine");
        let path = directory.join("db.redb");
        seed(&path);
        let (database, state, observed) = open(&path, action);
        let result = checkpoint(&database, &state, &observed);
        assert_eq!(result.is_ok(), matches!(action, Action::Observe));
        assert!(
            observed.budget_fences.load(Ordering::SeqCst) > 0,
            "real early sync reached"
        );
        assert!(observed.largest_unsynced.load(Ordering::SeqCst) <= WRITEBACK_BYTES);
        drop(database);
        assert_reopened(&path, matches!(action, Action::Observe));
    }
}

#[test]
fn checkpoint_writeback_crash_child() {
    let Ok(edge) = std::env::var(CHILD_EDGE) else {
        return;
    };
    let path = std::path::PathBuf::from(std::env::var_os(CHILD_PATH).unwrap());
    let action = match edge.as_str() {
        "before-early-sync" => Action::ExitBefore,
        "after-early-sync" => Action::ExitAfter,
        _ => panic!("unknown fixed checkpoint crash edge"),
    };
    let (database, state, observed) = open(&path, action);
    checkpoint(&database, &state, &observed).unwrap();
    panic!("early sync crash boundary not reached");
}

#[test]
fn checkpoint_writeback_process_crashes_do_not_publish_partial_transactions() {
    for edge in ["before-early-sync", "after-early-sync"] {
        let directory = crate::test_path::ScopedDirectory::new("checkpoint-writeback-crash");
        let path = directory.join("db.redb");
        seed(&path);
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "checkpoint_writeback::tests::engine::checkpoint_writeback_crash_child",
                "--nocapture",
            ])
            .env(CHILD_PATH, &path)
            .env(CHILD_EDGE, edge)
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(73),
            "{edge}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_reopened(&path, false);
    }
}
