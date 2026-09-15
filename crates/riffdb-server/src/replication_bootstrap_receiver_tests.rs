//! Real receiver custody, restart, and deterministic cancellation evidence.
// req: REP-002, REP-003, REC-001, PERF-007
use super::*;
use riffdb_storage_api::{
    ReadableCapabilityDigestInventory, ReadableDigestKey, ReadableIdempotencyDigestInventory,
    ReplicationSourceHoldIdV1,
};
use riffdb_types::{DigestKeyId, Timestamp};

fn inputs() -> StartupValidationInputs {
    let key = ReadableDigestKey::v1(DigestKeyId::new(1).unwrap());
    StartupValidationInputs::new(
        Timestamp::new(1000, 0).unwrap(),
        ReadableCapabilityDigestInventory::new(vec![key]).unwrap(),
        ReadableIdempotencyDigestInventory::new(vec![key]).unwrap(),
    )
}

#[tokio::test]
async fn receiver_job_resumes_real_pages_and_construction_before_checked_publication() {
    let (_scope, source_path) =
        crate::real_storage_support::temporary_database_scope("bootstrap-receiver-job");
    let startup = crate::startup::open_redb_startup(
        &source_path,
        inputs(),
        &crate::identifiers::ProductionIdentifierSources::new().database_ids(),
    )
    .unwrap();
    let (_, _, _, _, _, ports) = startup.into_parts();
    let root = source_path.parent().unwrap();
    let held = ports
        .prepare_replication_bootstrap_v3(
            &root.join("source"),
            ReplicationSourceHoldIdV1::new([0x59; 16]).unwrap(),
        )
        .unwrap();
    let manifest = held.manifest();
    let jobs = BootstrapReceiverJobs::new();
    let transfer = root.join("transfer");
    let candidate = root.join("candidate");
    let live = root.join("live.redb");
    let mut receiving = jobs.begin(transfer.clone(), manifest).await.unwrap();
    let refused = root.join("over-capacity");
    assert!(jobs.begin(refused.clone(), manifest).await.is_err());
    assert!(!refused.exists());
    assert_eq!(receiving.progress().unwrap().page_count(), 0);
    let first = held.read_page(1).unwrap().encode().unwrap();
    receiving.append(first.clone()).await.unwrap();
    drop(receiving);
    let mut receiving = jobs.resume(transfer.clone(), manifest).await.unwrap();
    assert_eq!(receiving.progress().unwrap().page_count(), 1);
    assert_eq!(receiving.append(first).await.unwrap().page_count(), 1);
    for ordinal in 2..=manifest.page_count() {
        receiving
            .append(held.read_page(ordinal).unwrap().encode().unwrap())
            .await
            .unwrap();
    }
    let mut build = receiving
        .materialize(candidate.clone(), false, inputs())
        .await
        .unwrap();
    assert!(!build.advance().await.unwrap());
    assert!(!live.exists());
    assert!(build.publish_and_activate(live.clone()).await.is_err());
    assert!(!live.exists(), "early publication must not create a target");
    let receiving = jobs.resume(transfer, manifest).await.unwrap();
    let mut build = receiving
        .materialize(candidate, true, inputs())
        .await
        .unwrap();
    let mut steps = 0;
    while !build.advance().await.unwrap() {
        steps += 1;
        assert!(steps < 200);
    }
    assert!(!live.exists());
    let mut applier = build.publish_and_activate(live.clone()).await.unwrap();
    assert_eq!(
        applier.durable_history().unwrap(),
        manifest.fence().history()
    );
    assert!(riffdb_storage_redb::RedbFollowerStore::open(&live).is_err());
    let acknowledged = applier.acknowledge_durable_position().unwrap();
    assert_eq!(acknowledged, manifest.fence().history().tail());
    ports
        .attach_replication_bootstrap_v3(manifest, acknowledged)
        .unwrap();
    drop(applier);
    assert!(!root.join("live.redb.riffjournal").exists());
    let flag = Arc::new(AtomicBool::new(false));
    let mut session = riffdb_storage_redb::RedbFollowerStore::open(&live)
        .unwrap()
        .begin_structural_evidence_cancellable(inputs(), Arc::clone(&flag))
        .unwrap();
    use riffdb_storage_api::StructuralEvidenceSession;
    let cursor = riffdb_storage_api::StructuralEvidenceCursor::start(
        session.database_id(),
        session.open_session_id(),
    );
    let limit = riffdb_storage_api::EvidencePageLimit::new(1).unwrap();
    session.read_structural_evidence(cursor, limit).unwrap();
    flag.store(true, Ordering::Release);
    assert!(
        matches!(session.read_structural_evidence(cursor, limit), Err(error) if error.kind() == StorageErrorKind::Unavailable)
    );
    drop(session);
    assert!(crate::startup::open_redb_follower_startup_cancellable(&live, inputs(), flag).is_err());
    let reopened = crate::startup::open_redb_follower_startup(&live, inputs()).unwrap();
    assert_eq!(
        reopened.applier.durable_history().unwrap(),
        manifest.fence().history()
    );
    drop(reopened);
    assert_eq!(jobs.capacity.available_permits(), 1);
}

struct ObservedResource(Arc<AtomicBool>);
impl Drop for ObservedResource {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn receiver_cancellation_signals_worker_and_retains_capacity_through_resource_drop() {
    let jobs = BootstrapReceiverJobs::new();
    let dropped = Arc::new(AtomicBool::new(false));
    let cancellation = Arc::new(AtomicBool::new(false));
    let owner = Custody {
        repository: None,
        value: ObservedResource(Arc::clone(&dropped)),
        permit: Arc::clone(&jobs.capacity).try_acquire_owned().unwrap(),
        cancellation: Arc::clone(&cancellation),
        deadline: Instant::now() + LIFETIME,
    };
    let (started, observing) = tokio::sync::oneshot::channel();
    let (release, blocked) = std::sync::mpsc::sync_channel(1);
    let waiter = tokio::spawn(run_step(owner, move |value, flag| {
        started.send(()).unwrap();
        blocked.recv().unwrap();
        assert!(flag.load(Ordering::Acquire));
        Ok(value)
    }));
    observing.await.unwrap();
    waiter.abort();
    assert!(matches!(waiter.await, Err(error) if error.is_cancelled()));
    assert!(cancellation.load(Ordering::Acquire));
    assert!(!dropped.load(Ordering::SeqCst));
    assert!(Arc::clone(&jobs.capacity).try_acquire_owned().is_err());
    release.send(()).unwrap();
    let _permit = Arc::clone(&jobs.capacity).acquire_owned().await.unwrap();
    assert!(dropped.load(Ordering::SeqCst));
}

#[tokio::test(start_paused = true)]
async fn receiver_deadline_does_not_admit_overlapping_detached_storage_work() {
    let jobs = BootstrapReceiverJobs::new();
    let dropped = Arc::new(AtomicBool::new(false));
    let cancellation = Arc::new(AtomicBool::new(false));
    let owner = Custody {
        repository: None,
        value: ObservedResource(Arc::clone(&dropped)),
        permit: Arc::clone(&jobs.capacity).try_acquire_owned().unwrap(),
        cancellation: Arc::clone(&cancellation),
        deadline: Instant::now() + LIFETIME,
    };
    let (started, observing) = tokio::sync::oneshot::channel();
    let (release, blocked) = std::sync::mpsc::sync_channel(1);
    let waiter = tokio::spawn(run_step(owner, move |value, _| {
        started.send(()).unwrap();
        blocked.recv().unwrap();
        Ok(value)
    }));
    observing.await.unwrap();
    tokio::time::advance(LIFETIME).await;
    assert!(
        matches!(waiter.await.unwrap(), Err(error) if error.kind() == StorageErrorKind::Unavailable)
    );
    assert!(cancellation.load(Ordering::Acquire));
    assert!(!dropped.load(Ordering::SeqCst));
    assert!(Arc::clone(&jobs.capacity).try_acquire_owned().is_err());
    release.send(()).unwrap();
    let _permit = Arc::clone(&jobs.capacity).acquire_owned().await.unwrap();
    assert!(dropped.load(Ordering::SeqCst));
}

#[tokio::test]
async fn expired_receiver_step_never_enters_storage() {
    let jobs = BootstrapReceiverJobs::new();
    let owner = Custody {
        repository: None,
        value: (),
        permit: Arc::clone(&jobs.capacity).try_acquire_owned().unwrap(),
        cancellation: Arc::new(AtomicBool::new(false)),
        deadline: Instant::now(),
    };
    assert!(
        run_step(owner, |(), _| -> Result<(), StorageError> {
            panic!("expired step must not run")
        })
        .await
        .is_err()
    );
    assert_eq!(jobs.capacity.available_permits(), 1);
}

#[tokio::test]
async fn cancelled_receiver_append_keeps_durable_progress_and_locks_until_storage_finishes() {
    let (_scope, source_path) =
        crate::real_storage_support::temporary_database_scope("bootstrap-receiver-cancel-page");
    let startup = crate::startup::open_redb_startup(
        &source_path,
        inputs(),
        &crate::identifiers::ProductionIdentifierSources::new().database_ids(),
    )
    .unwrap();
    let (_, _, _, _, _, ports) = startup.into_parts();
    let root = source_path.parent().unwrap();
    let held = ports
        .prepare_replication_bootstrap_v3(
            &root.join("source"),
            ReplicationSourceHoldIdV1::new([0x61; 16]).unwrap(),
        )
        .unwrap();
    let manifest = held.manifest();
    let path = root.join("receiver");
    let jobs = BootstrapReceiverJobs::new();
    let mut receiving = jobs.begin(path.clone(), manifest).await.unwrap();
    let owner = receiving.owner.take().unwrap();
    let cancellation = Arc::clone(&owner.cancellation);
    let page = held.read_page(1).unwrap().encode().unwrap();
    let (started, observing) = tokio::sync::oneshot::channel();
    let (release, blocked) = std::sync::mpsc::sync_channel(1);
    let waiter = tokio::spawn(run_step(owner, move |mut stage, _| {
        stage.append(&page)?;
        started.send(()).unwrap();
        blocked.recv().unwrap();
        Ok(stage)
    }));
    observing.await.unwrap();
    waiter.abort();
    assert!(matches!(waiter.await, Err(error) if error.is_cancelled()));
    assert!(cancellation.load(Ordering::Acquire));
    assert!(jobs.resume(path.clone(), manifest).await.is_err());
    assert!(RedbBootstrapStage::open(&path, manifest).is_err());
    assert!(receiving.progress().is_err());
    release.send(()).unwrap();
    drop(Arc::clone(&jobs.capacity).acquire_owned().await.unwrap());
    let mut resumed = jobs.resume(path.clone(), manifest).await.unwrap();
    assert_eq!(resumed.progress().unwrap().page_count(), 1);
    let exact = held.read_page(1).unwrap().encode().unwrap();
    assert_eq!(resumed.append(exact.clone()).await.unwrap().page_count(), 1);
    let mut corrupt = exact;
    corrupt[0] ^= 1;
    assert!(resumed.append(corrupt).await.is_err());
    assert!(resumed.progress().is_err());
    assert_eq!(
        jobs.resume(path, manifest)
            .await
            .unwrap()
            .progress()
            .unwrap()
            .page_count(),
        1
    );
}
