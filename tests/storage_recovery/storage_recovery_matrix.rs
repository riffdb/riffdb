#![forbid(unsafe_code)]

//! Child-process crash and reopen evidence for the redb storage boundary.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};

use riffdb_storage_api::{
    DatabaseIdentityProbe, DatabaseIdentityProbePort, DatabaseInitializationPort,
    EvidencePageLimit, HistoricalEvidenceCursor, HistoricalEvidencePage,
    ReadableCapabilityDigestInventory, ReadableDigestKey, ReadableIdempotencyDigestInventory,
    StartupValidationInputs, StructuralEvidenceCursor, StructuralEvidenceOpen,
    StructuralEvidencePage, StructuralEvidenceSession,
};
use riffdb_storage_redb::{RedbStore, RedbTestController, RedbTestOperation};
use riffdb_types::{DatabaseId, DigestKeyId, Timestamp};

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

fn complete_structural_open(store: RedbStore) -> DatabaseId {
    let digest_key = DigestKeyId::new(1).expect("digest key ID");
    let inputs = StartupValidationInputs::new(
        Timestamp::new(1_700_000_000, 0).expect("startup timestamp"),
        ReadableCapabilityDigestInventory::new(vec![ReadableDigestKey::v1(digest_key)])
            .expect("capability digest inventory"),
        ReadableIdempotencyDigestInventory::new(vec![ReadableDigestKey::v1(digest_key)])
            .expect("idempotency digest inventory"),
    );
    let mut session = store
        .begin_structural_evidence(inputs)
        .expect("begin exclusive structural evidence");
    let database_id = session.database_id();
    let open_session_id = session.open_session_id();
    let limit = EvidencePageLimit::new(64).expect("page limit");

    let mut structural_cursor = StructuralEvidenceCursor::start(database_id, open_session_id);
    let structural_end = loop {
        match session
            .read_structural_evidence(structural_cursor, limit)
            .expect("read structural evidence")
        {
            StructuralEvidencePage::Page { next, .. } => structural_cursor = next,
            StructuralEvidencePage::ExactEnd(end) => break end,
        }
    };

    let mut historical_cursor = HistoricalEvidenceCursor::start(database_id, open_session_id);
    let historical_end = loop {
        match session
            .read_historical_evidence(historical_cursor, limit)
            .expect("read historical evidence")
        {
            HistoricalEvidencePage::Page { next, .. } => historical_cursor = next,
            HistoricalEvidencePage::ExactEnd(end) => break end,
        }
    };

    let opened = session
        .finish(structural_end, historical_end)
        .expect("finish structural evidence");
    assert_eq!(opened.database_id(), database_id);
    database_id
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
    assert_eq!(complete_structural_open(store), database_id());
}
