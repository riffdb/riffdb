//! Continuous tail custody over a real published source and validated follower.
// req: REP-002, REP-003, REC-001, PERF-007
use super::*;
use crate::{
    replication_bootstrap::BootstrapSourceJobs, replication_source::PublishedReplicationSource,
};
use riffdb_service::{
    ReplicationFailure as Failure, ReplicationFuture, ReplicationItem, ReplicationItemSource,
    ReplicationPhase, ReplicationRequest, ReplicationSourcePort,
};
use riffdb_storage_api::{
    ChangelogHistoryStateV3, ReadableCapabilityDigestInventory, ReadableDigestKey,
    ReadableIdempotencyDigestInventory, ReplicationSourceHoldIdV1,
};
use riffdb_storage_redb::RedbOperationalPorts;
use riffdb_types::{DigestKeyId, Timestamp};
use std::sync::Mutex;

#[path = "replication_follower_read_auth_tests.rs"]
mod read_auth;

#[path = "maintenance_staged_authorization_tests.rs"]
mod staged_authorization;

#[path = "replication_follower_columnar_tests.rs"]
mod columnar;

fn inputs() -> StartupValidationInputs {
    let key = ReadableDigestKey::v1(DigestKeyId::new(1).unwrap());
    StartupValidationInputs::new(
        Timestamp::new(1000, 0).unwrap(),
        ReadableCapabilityDigestInventory::new(vec![key]).unwrap(),
        ReadableIdempotencyDigestInventory::new(vec![key]).unwrap(),
    )
}
struct Fixture {
    peer: FinitePeer,
    jobs: BootstrapReceiverJobs,
    manifest: Manifest,
    path: PathBuf,
    _scope: tempfile::TempDir,
}
struct FinitePeer {
    source: PublishedReplicationSource,
    ports: Arc<RedbOperationalPorts>,
    requests: Mutex<Vec<ReplicationRequest>>,
}
impl FinitePeer {
    fn history(&self) -> ChangelogHistoryStateV3 {
        self.ports
            .published_changelog_snapshot_v3()
            .unwrap()
            .authoritative_state_v3()
            .unwrap()
            .history()
    }
}
// Explicit connection rollover after one real frame, including exact EOF when
// the source has no successor. No clock sleeps synchronize this test.
struct FiniteStream {
    source: Box<dyn ReplicationItemSource>,
    remaining: bool,
}
impl ReplicationItemSource for FiniteStream {
    fn next_item(&mut self) -> ReplicationFuture<'_, Option<ReplicationItem>> {
        Box::pin(async move {
            if !std::mem::take(&mut self.remaining) {
                return Ok(None);
            }
            self.source.next_item().await
        })
    }
}
impl ReplicationSourcePort for FinitePeer {
    fn open(
        &self,
        request: ReplicationRequest,
    ) -> ReplicationFuture<'_, Box<dyn ReplicationItemSource>> {
        Box::pin(async move {
            self.requests.lock().unwrap().push(request.clone());
            let source = self.source.open(request.clone()).await?;
            Ok(Box::new(FiniteStream {
                source,
                remaining: request.after_sequence < self.history().tail().sequence().get(),
            }) as Box<dyn ReplicationItemSource>)
        })
    }
}
async fn fixture() -> (Fixture, BootstrapReceiverBuildJob) {
    fixture_with_setup(|_| {}).await
}

async fn fixture_with_setup(
    setup: impl FnOnce(&mut RedbOperationalPorts),
) -> (Fixture, BootstrapReceiverBuildJob) {
    let (scope, path) =
        crate::real_storage_support::temporary_database_scope("continuous-receiver");
    let mut startup = crate::startup::open_redb_startup(
        &path,
        inputs(),
        &crate::identifiers::ProductionIdentifierSources::new().database_ids(),
    )
    .unwrap();
    let publications = startup.take_replication_publications().unwrap();
    let (_, _, _, _, _, mut ports) = startup.into_parts();
    setup(&mut ports);
    let ports = Arc::new(ports);
    let repository = ports
        .bootstrap_repository(&scope.path().join("source-artifacts"))
        .unwrap();
    let id = ReplicationSourceHoldIdV1::new([0x71; 16]).unwrap();
    let mut source = repository.begin(id).unwrap();
    while !source.advance().unwrap() {}
    let held = source.finish().unwrap();
    let manifest = held.manifest();
    let jobs = BootstrapReceiverJobs::new();
    let mut transfer = jobs
        .begin(scope.path().join("transfer"), manifest)
        .await
        .unwrap();
    for ordinal in 1..=manifest.page_count() {
        transfer
            .append(held.read_page(ordinal).unwrap().encode().unwrap())
            .await
            .unwrap();
    }
    drop(held);
    let mut build = transfer
        .materialize(scope.path().join("candidate"), false, inputs())
        .await
        .unwrap();
    while !build.advance().await.unwrap() {}
    let peer = FinitePeer {
        source: PublishedReplicationSource::new(
            publications,
            BootstrapSourceJobs::from_repository(repository),
        ),
        ports,
        requests: Mutex::new(vec![]),
    };
    (
        Fixture {
            peer,
            jobs,
            manifest,
            path: scope.path().join("follower.redb"),
            _scope: scope,
        },
        build,
    )
}

#[tokio::test]
async fn continuous_receiver_attaches_after_local_ack_and_reconnects_without_idle_receipt_churn() {
    let (fixture, build) = fixture().await;
    let mut receiver = build
        .publish_and_follow(fixture.path.clone())
        .await
        .unwrap();
    assert_eq!(fixture.jobs.capacity.available_permits(), 0);
    let mut applied = receiver.advance(&fixture.peer).await.unwrap().unwrap();
    for _ in 0..8 {
        if applied == fixture.peer.history().tail() {
            break;
        }
        if let Some(next) = receiver.advance(&fixture.peer).await.unwrap() {
            assert!(next.sequence() > applied.sequence());
            applied = next;
        }
    }
    assert_eq!(applied, fixture.peer.history().tail());
    assert!(applied.sequence() > fixture.manifest.fence().history().tail().sequence());
    let after_attachment = fixture.peer.history();
    for _ in 0..4 {
        assert_eq!(receiver.advance(&fixture.peer).await.unwrap(), None);
    }
    assert_eq!(
        fixture.peer.history(),
        after_attachment,
        "idle reconnects cannot acknowledge acknowledgement receipts forever"
    );
    {
        let requests = fixture.peer.requests.lock().unwrap();
        assert!(matches!(requests[0].phase, ReplicationPhase::Attach { .. }));
        assert_eq!(
            requests[0].after_sequence,
            fixture.manifest.fence().history().tail().sequence().get()
        );
        assert!(
            requests[1..]
                .iter()
                .all(|r| matches!(r.phase, ReplicationPhase::Tail)
                    && r.after_sequence <= applied.sequence().get())
        );
    }
    receiver.close().await.unwrap();
    assert_eq!(fixture.jobs.capacity.available_permits(), 1);
    let reopened = crate::startup::open_redb_follower_startup(&fixture.path, inputs()).unwrap();
    assert_eq!(reopened.applier.durable_position().unwrap(), applied);
}

struct WithheldPeer<'a> {
    peer: &'a FinitePeer,
    entered: Arc<tokio::sync::Notify>,
    dropped: Arc<AtomicBool>,
}
struct WithheldStream {
    _source: Box<dyn ReplicationItemSource>,
    entered: Arc<tokio::sync::Notify>,
    dropped: Arc<AtomicBool>,
}
impl Drop for WithheldStream {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}
impl ReplicationItemSource for WithheldStream {
    fn next_item(&mut self) -> ReplicationFuture<'_, Option<ReplicationItem>> {
        Box::pin(async move {
            self.entered.notify_one();
            std::future::pending().await
        })
    }
}
impl ReplicationSourcePort for WithheldPeer<'_> {
    fn open(
        &self,
        request: ReplicationRequest,
    ) -> ReplicationFuture<'_, Box<dyn ReplicationItemSource>> {
        Box::pin(async move {
            let source = self.peer.open(request).await?;
            Ok(Box::new(WithheldStream {
                _source: source,
                entered: Arc::clone(&self.entered),
                dropped: Arc::clone(&self.dropped),
            }) as Box<dyn ReplicationItemSource>)
        })
    }
}

#[tokio::test]
async fn continuous_receiver_cancellation_recovers_a_lost_attachment_reply_from_local_state() {
    let (fixture, build) = fixture().await;
    let mut receiver = build
        .publish_and_follow(fixture.path.clone())
        .await
        .unwrap();
    let readers = receiver.prepare_readers().await.unwrap();
    let historical = readers.latest().unwrap();
    let peer = WithheldPeer {
        peer: &fixture.peer,
        entered: Arc::new(tokio::sync::Notify::new()),
        dropped: Arc::new(AtomicBool::new(false)),
    };
    let mut receive = Box::pin(receiver.advance(&peer));
    tokio::select! {
        () = peer.entered.notified() => {},
        result = &mut receive => panic!("expected withheld reply, got {result:?}"),
    }
    let after_attachment = fixture.peer.history();
    assert!(
        after_attachment.tail().sequence() > fixture.manifest.fence().history().tail().sequence()
    );
    drop(receive);
    assert_eq!(
        readers.latest().unwrap_err().kind(),
        StorageErrorKind::Unavailable
    );
    assert_eq!(historical.history(), fixture.manifest.fence().history());
    drop(historical);
    assert!(peer.dropped.load(Ordering::SeqCst));
    assert!(matches!(
        receiver.advance(&fixture.peer).await,
        Err(Failure::Unavailable)
    ));
    assert_eq!(fixture.jobs.capacity.available_permits(), 1);
    let mut recovered = fixture
        .jobs
        .reopen_follower(
            fixture.path.clone(),
            inputs(),
            fixture.manifest.fence().history().lineage(),
            fixture.manifest.fence().hold_id(),
            Some(fixture.manifest),
        )
        .await
        .unwrap();
    assert_eq!(
        recovered
            .owner
            .as_ref()
            .unwrap()
            .value
            .durable_position()
            .unwrap(),
        fixture.manifest.fence().history().tail()
    );
    let mut applied = None;
    for _ in 0..8 {
        applied = recovered.advance(&fixture.peer).await.unwrap().or(applied);
        if applied == Some(after_attachment.tail()) {
            break;
        }
    }
    assert_eq!(applied, Some(after_attachment.tail()));
    assert_eq!(
        fixture.peer.history(),
        after_attachment,
        "attachment retry is read-only even after a lost response"
    );
    assert!(matches!(
        fixture.peer.requests.lock().unwrap()[1].phase,
        ReplicationPhase::Attach { .. }
    ));
    recovered.close().await.unwrap();
}

struct ItemPeer {
    item: Mutex<Option<ReplicationItem>>,
}
struct ItemStream {
    item: Option<ReplicationItem>,
}
impl ReplicationItemSource for ItemStream {
    fn next_item(&mut self) -> ReplicationFuture<'_, Option<ReplicationItem>> {
        Box::pin(async move { Ok(self.item.take()) })
    }
}
impl ReplicationSourcePort for ItemPeer {
    fn open(&self, _: ReplicationRequest) -> ReplicationFuture<'_, Box<dyn ReplicationItemSource>> {
        Box::pin(async move {
            Ok(Box::new(ItemStream {
                item: self.item.lock().unwrap().take(),
            }) as Box<dyn ReplicationItemSource>)
        })
    }
}

#[tokio::test]
async fn continuous_receiver_wrong_phase_or_corrupt_frame_fuses_without_advancing_the_prefix() {
    let (fixture, build) = fixture().await;
    let mut receiver = build
        .publish_and_follow(fixture.path.clone())
        .await
        .unwrap();
    for (index, item) in [
        ReplicationItem::BootstrapManifest(vec![1]),
        ReplicationItem::Frame(vec![1].into()),
        ReplicationItem::Frame(vec![].into()),
    ]
    .into_iter()
    .enumerate()
    {
        if index != 0 {
            receiver = fixture
                .jobs
                .reopen_follower(
                    fixture.path.clone(),
                    inputs(),
                    fixture.manifest.fence().history().lineage(),
                    fixture.manifest.fence().hold_id(),
                    Some(fixture.manifest),
                )
                .await
                .unwrap();
        }
        let peer = ItemPeer {
            item: Mutex::new(Some(item)),
        };
        let readers = receiver.prepare_readers().await.unwrap();
        assert_eq!(
            receiver.advance(&peer).await,
            Err(Failure::Source(
                riffdb_service::ReplicationStreamErrorV3::CorruptHistory
            ))
        );
        assert_eq!(receiver.advance(&peer).await, Err(Failure::Unavailable));
        assert_eq!(
            readers.latest().unwrap_err().kind(),
            StorageErrorKind::Unavailable
        );
        assert_eq!(fixture.jobs.capacity.available_permits(), 1);
        let opened = crate::startup::open_redb_follower_startup(&fixture.path, inputs()).unwrap();
        assert_eq!(
            opened.applier.durable_position().unwrap(),
            fixture.manifest.fence().history().tail()
        );
    }
    let lineage = fixture.manifest.fence().history().lineage();
    let wrong = ChangelogLineageV3::new(
        lineage.database_id(),
        lineage.history_incarnation() + 1,
        lineage.leadership_epoch(),
    )
    .unwrap();
    assert!(
        fixture
            .jobs
            .reopen_follower(
                fixture.path.clone(),
                inputs(),
                wrong,
                fixture.manifest.fence().hold_id(),
                None
            )
            .await
            .is_err()
    );
    assert_eq!(fixture.jobs.capacity.available_permits(), 1);
}

#[tokio::test]
async fn continuous_receiver_cancelled_blocking_apply_retains_the_sole_writer_until_completion() {
    let (fixture, build) = fixture().await;
    let mut receiver = build
        .publish_and_follow(fixture.path.clone())
        .await
        .unwrap();
    let owner = receiver.owner.take().unwrap();
    let owner = run_step(owner, |mut applier, _| {
        applier.acknowledge_durable_position()?;
        Ok(applier)
    })
    .await
    .unwrap();
    let lineage = fixture.manifest.fence().history().lineage();
    let position = fixture.manifest.fence().history().tail();
    let mut source = fixture
        .peer
        .source
        .open(ReplicationRequest {
            database_id: lineage.database_id(),
            history_incarnation: lineage.history_incarnation(),
            leadership_epoch: lineage.leadership_epoch().get(),
            phase: ReplicationPhase::Attach {
                manifest: fixture.manifest.encode().unwrap(),
            },
            after_sequence: position.sequence().get(),
            after_hash: position.history_hash(),
            after_frontier: position.frontier(),
            readable_format: ChangelogFrameV3::IDENTITY.to_owned(),
            catalog_digest: AuthoritativeStateCatalogV1.digest(),
            maximum_frame_bytes: MAX_CHANGELOG_FRAME_BYTES as u64,
            maximum_transitions: MAX_STAGED_COMMANDS as u64,
        })
        .await
        .unwrap();
    let Some(ReplicationItem::Frame(bytes)) = source.next_item().await.unwrap() else {
        panic!("source frame");
    };
    drop(source);
    let (started, observing) = tokio::sync::oneshot::channel();
    let (release, blocked) = std::sync::mpsc::sync_channel(1);
    // Pause real storage after the durable apply and before its caller regains
    // ownership. Cancellation must not release the engine or receiver slot.
    let waiter = tokio::spawn(run_step(owner, move |mut applier, flag| {
        let applied = applier.apply_frame(&bytes)?;
        started.send(applied).unwrap();
        blocked.recv().unwrap();
        check_cancel(flag)?;
        applier.acknowledge_durable_position()?;
        Ok(applier)
    }));
    let applied = observing.await.unwrap();
    waiter.abort();
    assert!(matches!(waiter.await, Err(error) if error.is_cancelled()));
    assert_eq!(fixture.jobs.capacity.available_permits(), 0);
    assert!(riffdb_storage_redb::RedbFollowerStore::open(&fixture.path).is_err());
    release.send(()).unwrap();
    drop(
        Arc::clone(&fixture.jobs.capacity)
            .acquire_owned()
            .await
            .unwrap(),
    );
    let recovered = fixture
        .jobs
        .reopen_follower(
            fixture.path.clone(),
            inputs(),
            lineage,
            fixture.manifest.fence().hold_id(),
            Some(fixture.manifest),
        )
        .await
        .unwrap();
    assert_eq!(
        recovered
            .owner
            .as_ref()
            .unwrap()
            .value
            .durable_position()
            .unwrap(),
        applied
    );
    assert!(recovered.attachment.is_none());
    assert!(recovered.report_pending);
    recovered.close().await.unwrap();
}

#[cfg(test)]
#[path = "replication_follower_reporting_tests.rs"]
mod reporting;

struct WorkerPeer {
    peer: FinitePeer,
    calls: std::sync::atomic::AtomicUsize,
    waiting: tokio::sync::Notify,
    terminal: bool,
}
impl ReplicationSourcePort for WorkerPeer {
    fn open(
        &self,
        request: ReplicationRequest,
    ) -> ReplicationFuture<'_, Box<dyn ReplicationItemSource>> {
        Box::pin(async move {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if self.terminal {
                return Err(Failure::AuthorizationDenied);
            }
            if call == 0 {
                return Err(Failure::Unavailable);
            }
            if request.after_sequence == self.peer.history().tail().sequence().get() {
                self.waiting.notify_one();
                return std::future::pending().await;
            }
            self.peer.open(request).await
        })
    }
}

#[tokio::test]
async fn follower_worker_retries_transient_source_failure_and_drains_on_shutdown() {
    let (fixture, build) = fixture().await;
    let receiver = build
        .publish_and_follow(fixture.path.clone())
        .await
        .unwrap();
    let peer = Arc::new(WorkerPeer {
        peer: fixture.peer,
        calls: 0.into(),
        waiting: tokio::sync::Notify::new(),
        terminal: false,
    });
    let worker = super::worker::RunningFollowerReceiver::start(receiver, peer.clone()).unwrap();
    peer.waiting.notified().await;
    assert!(peer.calls.load(Ordering::SeqCst) >= 3);
    assert_eq!(fixture.jobs.capacity.available_permits(), 0);
    worker.shutdown().await.unwrap();
    assert_eq!(fixture.jobs.capacity.available_permits(), 1);
    let checked = crate::startup::open_redb_follower_startup(&fixture.path, inputs()).unwrap();
    assert_eq!(
        checked.applier.durable_history().unwrap(),
        peer.peer.history()
    );
}

#[tokio::test]
async fn follower_worker_terminal_source_refusal_stops_without_retry_and_releases_engine() {
    let (fixture, build) = fixture().await;
    let receiver = build
        .publish_and_follow(fixture.path.clone())
        .await
        .unwrap();
    let peer = Arc::new(WorkerPeer {
        peer: fixture.peer,
        calls: 0.into(),
        waiting: tokio::sync::Notify::new(),
        terminal: true,
    });
    let mut worker = super::worker::RunningFollowerReceiver::start(receiver, peer.clone()).unwrap();
    assert_eq!(worker.finished().await, Err(Failure::AuthorizationDenied));
    assert_eq!(peer.calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.jobs.capacity.available_permits(), 1);
    assert!(crate::startup::open_redb_follower_startup(&fixture.path, inputs()).is_ok());
}

#[tokio::test]
async fn dropping_follower_worker_requests_stop_and_preserves_drain_custody() {
    let (fixture, build) = fixture().await;
    let receiver = build
        .publish_and_follow(fixture.path.clone())
        .await
        .unwrap();
    let peer = Arc::new(WorkerPeer {
        peer: fixture.peer,
        calls: 0.into(),
        waiting: tokio::sync::Notify::new(),
        terminal: false,
    });
    let worker = super::worker::RunningFollowerReceiver::start(receiver, peer.clone()).unwrap();
    peer.waiting.notified().await;
    drop(worker);
    // Real capacity release, rather than a delay or a task-handle drop, proves
    // that the engine can be reopened after the caller abandons its supervisor.
    let drained = fixture.jobs.capacity.acquire().await.unwrap();
    let checked = crate::startup::open_redb_follower_startup(&fixture.path, inputs()).unwrap();
    assert_eq!(
        checked.applier.durable_history().unwrap(),
        peer.peer.history()
    );
    drop(drained);
}
