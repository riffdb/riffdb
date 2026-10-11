// req: REC-001, REC-006, END-001

use super::*;
use crate::test_path::ScopedDirectory;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const PROBE: &str = "maintenance::restore_custody::tests::external_native_open_probe";
const PROBE_PATH: &str = "RIFFDB_TEST_PRIVATE_CUSTODY_PATH";
const PROBE_HELD: &str = "RIFFDB_TEST_PRIVATE_CUSTODY_HELD";

fn healthy(scope: &ScopedDirectory) -> std::path::PathBuf {
    let path = scope.join("primary.redb");
    drop(redb::Database::create(&path).expect("create native database"));
    path
}

fn capture(path: &Path) -> RestoreCustody {
    RestoreCustody::capture(path, Arc::new(AtomicBool::new(false)))
        .expect("capture private native custody")
}

fn engine(owner: &RestoreCustody) -> redb::Database {
    let mut builder = redb::Builder::new();
    builder.set_cache_size(crate::store::PRIMARY_REDB_CACHE_SIZE_BYTES);
    builder.set_repair_callback(|session| session.abort());
    builder
        .create_with_backend(owner.lease().expect("engine sublease"))
        .expect("open healthy private engine without repair")
}

fn probe(path: &Path, held: bool) {
    let mut child = Command::new(std::env::current_exe().expect("current test executable"))
        .args(["--exact", PROBE, "--nocapture"])
        .env(PROBE_PATH, path)
        .env(PROBE_HELD, if held { "yes" } else { "no" })
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn independent native lock observer");
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(status) = child.try_wait().expect("observe native probe") {
            assert!(status.success(), "independent native custody probe failed");
            break;
        }
        if Instant::now() >= deadline {
            child.kill().expect("stop bounded native probe");
            child.wait().expect("join bounded native probe");
            panic!("independent native custody probe exceeded its bound");
        }
        std::thread::yield_now();
    }
}

#[test]
fn external_native_open_probe() {
    let Some(path) = std::env::var_os(PROBE_PATH) else {
        return;
    };
    let held = std::env::var(PROBE_HELD).expect("probe expectation") == "yes";
    let path = Path::new(&path);
    let writer = redb::Database::open(path);
    if held {
        assert!(
            matches!(writer, Err(redb::DatabaseError::DatabaseAlreadyOpen)),
            "ordinary writer must observe native exclusion"
        );
    } else {
        drop(writer.expect("ordinary writer after custody release"));
    }
    let reader = redb::ReadOnlyDatabase::open(path);
    if held {
        assert!(
            matches!(reader, Err(redb::DatabaseError::DatabaseAlreadyOpen)),
            "ordinary read-only engine must observe native exclusion"
        );
    } else {
        drop(reader.expect("ordinary reader after custody release"));
    }
    let legacy = File::options()
        .read(true)
        .write(true)
        .open(path)
        .expect("probe file");
    let lock = legacy.try_lock();
    if held {
        assert!(lock.is_err(), "legacy lock must remain held");
    } else {
        lock.expect("legacy lock after custody release");
        legacy.unlock().expect("release probe legacy lock");
    }
    let native = redb::backends::FileBackend::new(legacy).expect("probe native backend");
    let acquired = native
        .try_lock_range(Bound::Unbounded, Bound::Unbounded)
        .expect("probe native range lock");
    assert_eq!(acquired, !held, "native range exclusion must match custody");
    native.close().expect("release probe native capabilities");
}

#[test]
fn engine_close_and_reopen_preserve_cross_process_private_custody() {
    let scope = ScopedDirectory::new("restore-custody-close");
    let path = healthy(&scope);
    let owner = capture(&path);
    probe(&path, true);
    let first = engine(&owner);
    probe(&path, true);
    drop(first);
    owner
        .verify_identity()
        .expect("closed engine retains identity");
    probe(&path, true);
    let second = engine(&owner);
    probe(&path, true);
    drop(second);
    probe(&path, true);
    drop(owner);
    probe(&path, false);
}

#[test]
fn shared_engine_sublease_cannot_downgrade_native_custody() {
    let scope = ScopedDirectory::new("restore-custody-shared");
    let path = healthy(&scope);
    let owner = capture(&path);
    let backend = owner.lease().expect("engine lease");
    assert!(
        backend
            .try_lock_shared_range(Bound::Unbounded, Bound::Unbounded)
            .unwrap()
    );
    probe(&path, true);
    backend
        .unlock_range(Bound::Unbounded, Bound::Unbounded)
        .unwrap();
    probe(&path, true);
    backend.close().unwrap();
    probe(&path, true);
    drop(backend);
    drop(owner);
    probe(&path, false);
}

#[test]
fn stale_close_cannot_revoke_a_later_engine_lease() {
    let scope = ScopedDirectory::new("restore-custody-generation");
    let path = healthy(&scope);
    let owner = capture(&path);
    let first = owner.lease().unwrap();
    assert!(owner.lease().is_err(), "one active engine owner");
    first.close().unwrap();
    assert_eq!(first.len().unwrap_err().kind(), io::ErrorKind::BrokenPipe);
    let second = owner.lease().unwrap();
    first.close().unwrap();
    assert!(second.len().unwrap() > 0);
    assert!(owner.lease().is_err());
    drop(first);
    assert!(second.len().unwrap() > 0);
    second.close().unwrap();
    drop(second);
    probe(&path, true);
}

#[test]
fn owner_drop_keeps_exclusion_until_the_last_engine_handle_drops() {
    let scope = ScopedDirectory::new("restore-custody-last-owner");
    let path = healthy(&scope);
    let owner = capture(&path);
    let backend = owner.lease().unwrap();
    drop(owner);
    probe(&path, true);
    backend.close().unwrap();
    probe(&path, true);
    drop(backend);
    probe(&path, false);
}

#[test]
fn a_live_native_engine_retains_custody_after_the_outer_handle_drops() {
    let scope = ScopedDirectory::new("restore-custody-live-engine");
    let path = healthy(&scope);
    let owner = capture(&path);
    let database = engine(&owner);
    drop(owner);
    probe(&path, true);
    drop(database);
    probe(&path, false);
}

#[test]
fn cancellation_is_sticky_and_close_releases_only_the_engine_lease() {
    let scope = ScopedDirectory::new("restore-custody-cancel");
    let path = healthy(&scope);
    let cancellation = Arc::new(AtomicBool::new(false));
    let owner = RestoreCustody::capture(&path, Arc::clone(&cancellation)).unwrap();
    let backend = owner.lease().unwrap();
    cancellation.store(true, Ordering::Release);
    assert_eq!(
        backend.len().unwrap_err().kind(),
        io::ErrorKind::Interrupted
    );
    cancellation.store(false, Ordering::Release);
    assert!(owner.verify_identity().is_err());
    assert!(owner.lease().is_err());
    backend.close().unwrap();
    probe(&path, true);
    drop(backend);
    drop(owner);
    probe(&path, false);
}

#[test]
fn capacity_and_offset_overflow_refuse_before_changing_the_artifact() {
    let scope = ScopedDirectory::new("restore-custody-capacity");
    let path = healthy(&scope);
    let original = std::fs::read(&path).unwrap();
    let owner = capture(&path);
    let backend = owner.lease().unwrap();
    assert_eq!(
        backend
            .set_len(original.len() as u64 + 1)
            .unwrap_err()
            .kind(),
        io::ErrorKind::StorageFull
    );
    assert_eq!(
        backend
            .write(original.len() as u64, &[1])
            .unwrap_err()
            .kind(),
        io::ErrorKind::StorageFull
    );
    assert!(backend.write(u64::MAX, &[1]).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), original);
}

#[test]
fn replaced_primary_invalidates_custody_without_touching_replacement() {
    let scope = ScopedDirectory::new("restore-custody-replaced");
    let path = healthy(&scope);
    let owner = capture(&path);
    std::fs::rename(&path, scope.join("retained.redb")).unwrap();
    std::fs::write(&path, b"replacement sentinel").unwrap();
    assert!(owner.verify_identity().is_err());
    assert!(owner.lease().is_err());
    assert_eq!(std::fs::read(&path).unwrap(), b"replacement sentinel");
}

#[test]
fn replaced_parent_invalidates_live_access_without_touching_replacement() {
    let scope = ScopedDirectory::new("restore-custody-parent");
    let stage = scope.join("stage");
    std::fs::create_dir(&stage).unwrap();
    let path = stage.join("primary.redb");
    drop(redb::Database::create(&path).unwrap());
    let owner = capture(&path);
    let backend = owner.lease().unwrap();
    std::fs::rename(&stage, scope.join("retained-stage")).unwrap();
    std::fs::create_dir(&stage).unwrap();
    std::fs::write(&path, b"replacement parent sentinel").unwrap();
    assert!(owner.verify_identity().is_err());
    assert!(backend.write(0, &[1]).is_err());
    assert!(owner.lease().is_err());
    assert_eq!(
        std::fs::read(&path).unwrap(),
        b"replacement parent sentinel"
    );
}

#[test]
fn range_queries_and_collection_limits_are_bounded() {
    let scope = ScopedDirectory::new("restore-custody-ranges");
    let path = healthy(&scope);
    let owner = capture(&path);
    let backend = owner.lease().unwrap();
    for index in 0..MAX_ENGINE_RANGES as u64 {
        assert!(
            backend
                .try_lock_range(Bound::Included(index), Bound::Included(index))
                .unwrap()
        );
    }
    assert!(
        backend
            .try_lock_range(Bound::Included(100), Bound::Included(100))
            .is_err()
    );
    assert!(
        backend
            .query_lock_range(Bound::Included(0), Bound::Included(0))
            .is_err()
    );
    assert!(
        !backend
            .query_lock_range(Bound::Included(100), Bound::Included(100))
            .unwrap()
    );
    assert!(
        backend
            .try_lock_range(Bound::Excluded(u64::MAX), Bound::Unbounded)
            .is_err()
    );
    assert!(
        backend
            .unlock_range(Bound::Included(100), Bound::Included(100))
            .is_err()
    );
    backend.close().unwrap();
    assert!(
        backend
            .query_lock_range(Bound::Unbounded, Bound::Unbounded)
            .is_err()
    );
}

#[test]
fn concurrent_engine_claims_grant_exactly_one_lease() {
    let scope = ScopedDirectory::new("restore-custody-concurrent");
    let path = healthy(&scope);
    let owner = capture(&path);
    let start = std::sync::Barrier::new(3);
    let release = std::sync::Barrier::new(3);
    let (send, receive) = std::sync::mpsc::channel();
    std::thread::scope(|threads| {
        for _ in 0..2 {
            let send = send.clone();
            let owner = &owner;
            let start = &start;
            let release = &release;
            threads.spawn(move || {
                start.wait();
                let lease = owner.lease();
                send.send(lease.is_ok()).unwrap();
                release.wait();
                drop(lease);
            });
        }
        start.wait();
        let first = receive.recv_timeout(Duration::from_secs(15)).unwrap();
        let second = receive.recv_timeout(Duration::from_secs(15)).unwrap();
        // Release every contender before asserting, including on a bad grant.
        release.wait();
        assert_ne!(first, second, "exactly one simultaneous owner");
    });
    let next = owner
        .lease()
        .expect("both contenders released their capability");
    drop(next);
    probe(&path, true);
}

#[test]
fn close_joins_an_in_flight_access_before_revoking_its_lease() {
    let scope = ScopedDirectory::new("restore-custody-access-close");
    let path = healthy(&scope);
    let owner = capture(&path);
    let backend = owner.lease().unwrap();
    let access = backend.access().unwrap();
    let (started, observe_start) = std::sync::mpsc::channel();
    let (closed, observe_close) = std::sync::mpsc::channel();
    let mut joined_before_release = false;
    std::thread::scope(|threads| {
        let backend = &backend;
        threads.spawn(move || {
            started.send(()).unwrap();
            let result = backend.close();
            closed.send(result).unwrap();
        });
        observe_start.recv_timeout(Duration::from_secs(15)).unwrap();
        joined_before_release = !matches!(
            observe_close.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        );
        drop(access);
        if !joined_before_release {
            observe_close
                .recv_timeout(Duration::from_secs(15))
                .unwrap()
                .unwrap();
        }
    });
    assert!(
        !joined_before_release,
        "close must wait for the active access"
    );
    assert_eq!(backend.len().unwrap_err().kind(), io::ErrorKind::BrokenPipe);
    probe(&path, true);
}
