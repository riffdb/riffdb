#![forbid(unsafe_code)]

//! Child-process crash and reopen evidence for the redb storage boundary.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};

use riffdb_storage_api::{
    DatabaseIdentityProbe, DatabaseIdentityProbePort, DatabaseInitializationPort,
};
use riffdb_storage_redb::{RedbStore, RedbTestController, RedbTestOperation};
use riffdb_types::DatabaseId;

const CHILD_MODE: &str = "RIFFDB_STORAGE_RECOVERY_CHILD_MODE";
const CHILD_PATH: &str = "RIFFDB_STORAGE_RECOVERY_CHILD_PATH";
static NEXT_PATH: AtomicU64 = AtomicU64::new(1);

struct TestDatabasePath(PathBuf);

impl TestDatabasePath {
    fn new(label: &str) -> Self {
        let ordinal = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        Self(std::env::temp_dir().join(format!(
            "riffdb-storage-recovery-{label}-{}-{ordinal}.redb",
            std::process::id()
        )))
    }
}

impl Drop for TestDatabasePath {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn database_id() -> DatabaseId {
    DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x71; 10])
        .expect("valid deterministic database ID")
}

fn run_crashing_child(mode: &str, path: &Path) {
    let status = Command::new(std::env::current_exe().expect("current test executable"))
        .arg("--exact")
        .arg("process_recovery_child")
        .arg("--nocapture")
        .env(CHILD_MODE, mode)
        .env(CHILD_PATH, path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("run recovery child");
    assert!(!status.success(), "the armed child must terminate abruptly");
}

#[test]
fn process_recovery_child() {
    let Ok(mode) = std::env::var(CHILD_MODE) else {
        return;
    };
    let path = PathBuf::from(std::env::var_os(CHILD_PATH).expect("child database path"));
    let controller = match mode.as_str() {
        "before-initialization-commit" => {
            RedbTestController::abort_before_commit(RedbTestOperation::Initialization)
        }
        "after-initialization-commit" => {
            RedbTestController::abort_after_commit(RedbTestOperation::Initialization)
        }
        _ => panic!("unknown closed child mode"),
    };
    let mut store =
        RedbStore::open_with_test_controller(path, controller).expect("open child database");
    let _ = store.initialize_database(database_id());
    panic!("the armed failpoint did not terminate the child");
}

#[test]
fn crash_before_initialization_commit_leaves_a_proven_empty_store() {
    let path = TestDatabasePath::new("before-initialization");
    run_crashing_child("before-initialization-commit", &path.0);

    let store = RedbStore::open(&path.0).expect("recover precommit crash");
    assert_eq!(
        store.probe_database_identity().expect("probe recovery"),
        DatabaseIdentityProbe::NeedsInitialization
    );
}

#[test]
fn crash_after_initialization_commit_preserves_the_durable_database_identity() {
    let path = TestDatabasePath::new("after-initialization");
    run_crashing_child("after-initialization-commit", &path.0);

    let store = RedbStore::open(&path.0).expect("recover postcommit crash");
    assert_eq!(
        store.probe_database_identity().expect("probe recovery"),
        DatabaseIdentityProbe::Existing(database_id())
    );
}
