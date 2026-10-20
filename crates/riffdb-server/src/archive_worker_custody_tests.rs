//! Actual archive threads and actual primary file exclusion, with fixed barriers.
// req: STO-032, REP-007, AFC-007
use super::*;
use std::fs::{OpenOptions, TryLockError};
use std::sync::mpsc::{self, Receiver, SyncSender};

fn is_excluded(path: &Path) -> bool {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    match file.try_lock() {
        Err(TryLockError::WouldBlock) => true,
        Ok(()) => {
            file.unlock().unwrap();
            false
        }
        result => panic!("actual file exclusion error: {result:?}"),
    }
}
fn exclusion(path: &Path, held: bool) {
    assert_eq!(is_excluded(path), held);
}
fn channel() -> (SyncSender<()>, Receiver<()>) {
    mpsc::sync_channel(1)
}
fn reached(receiver: &Receiver<()>) {
    receiver.recv_timeout(Duration::from_secs(20)).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_startup_without_publication_keeps_engine_through_stop_only_worker_drop() {
    startup_custody_case(false);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_old_shadow_startup_missing_keepalive_releases_lock_while_thread_is_live() {
    startup_custody_case(true);
}

fn startup_custody_case(omit_engine_keepalive: bool) {
    let Fixture {
        root,
        archive,
        pin,
        _ports: ports,
        ..
    } = fixture();
    let database = root.path().join("database.redb");
    let (_publisher, publications) = ReplicationPublication::channel();
    let (entered, observe) = channel();
    let (release, proceed) = channel();
    let (exiting, observe_exit) = channel();
    let (finish, may_finish) = channel();
    let mut worker = Worker::start_inner(
        archive,
        root.path().join("backups"),
        publications,
        ports.archive_job_engine_custody_v1(),
        Some(CustodyTestBoundary {
            omit_engine_keepalive,
            entered: Some(entered),
            release: Some(proceed),
            exiting: Some(exiting),
            finish: Some(may_finish),
        }),
    );
    reached(&observe);
    drop(pin);
    drop(ports);
    let before_drop = is_excluded(&database);
    // Ordinary Drop signals cancellation; the actual thread remains owned by
    // this join handle and paused before it can finish its native custody.
    let thread = worker.thread.take().unwrap();
    drop(worker);
    let after_stop_only_drop = is_excluded(&database);
    release.send(()).unwrap();
    reached(&observe_exit);
    let before_thread_exit = is_excluded(&database);
    finish.send(()).unwrap();
    thread.join().unwrap();
    exclusion(&database, false);
    eprintln!(
        "actual_archive_startup_shadow={omit_engine_keepalive} before_drop={before_drop} after_stop_only_drop={after_stop_only_drop} before_thread_exit={before_thread_exit}"
    );
    let expected = !omit_engine_keepalive;
    assert_eq!(
        (before_drop, after_stop_only_drop, before_thread_exit),
        (expected, expected, expected),
        "actual live archive thread must retain primary exclusion"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_real_error_keeps_engine_until_thread_exit_and_join() {
    error_custody_case(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_old_shadow_error_missing_keepalive_releases_lock_before_thread_exit() {
    error_custody_case(true).await;
}

async fn error_custody_case(omit_engine_keepalive: bool) {
    let Fixture {
        root,
        archive,
        pin,
        _ports: ports,
        ..
    } = fixture();
    let database = root.path().join("database.redb");
    let archive_path = archive.path().to_path_buf();
    let (_publisher, publications) = ReplicationPublication::channel();
    let (exiting, observe_exit) = channel();
    let (finish, may_finish) = channel();
    let mut worker = Worker::start_inner(
        archive,
        root.path().join("missing-backups"),
        publications,
        ports.archive_job_engine_custody_v1(),
        Some(CustodyTestBoundary {
            omit_engine_keepalive,
            entered: None,
            release: None,
            exiting: Some(exiting),
            finish: Some(may_finish),
        }),
    );
    drop(pin);
    drop(ports);
    reached(&observe_exit);
    assert_eq!(failed(&mut worker).await, Error::InvalidManifest);
    assert!(
        !archive_path.exists(),
        "failed input must not create an archive"
    );
    let before_thread_exit = is_excluded(&database);
    finish.send(()).unwrap();
    worker.join();
    exclusion(&database, false);
    eprintln!(
        "actual_archive_error_shadow={omit_engine_keepalive} real_invalid_manifest=true before_thread_exit={before_thread_exit}"
    );
    assert_eq!(
        before_thread_exit, !omit_engine_keepalive,
        "actual archive error thread must retain primary exclusion until physical exit"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_idle_custom_repository_retains_actual_primary_until_join() {
    let Fixture {
        root,
        archive,
        pin,
        _ports: ports,
        head,
        ..
    } = fixture();
    let database = root.path().join("database.redb");
    // This is the existing genuine configured external repository, independent
    // of the source's canonical bootstrap sibling and its hold-policy census.
    assert_eq!(archive.path(), root.path().join("archive"));
    let (publisher, publications) = ReplicationPublication::channel();
    publisher.observe_published_snapshot_v3(pin.clone());
    let mut worker = Worker::start(
        archive,
        root.path().join("backups"),
        publications,
        ports.archive_job_engine_custody_v1(),
    );
    caught_up(&mut worker, head).await;
    drop(pin);
    drop(ports);
    exclusion(&database, true);
    worker.join();
    // The real notification slot independently retains its last published pin.
    // Retire it explicitly before asserting final physical exclusion release.
    drop(publisher);
    exclusion(&database, false);
}
