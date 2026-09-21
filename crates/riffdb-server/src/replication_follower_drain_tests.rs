//! A complete received frame survives supervised stop at the apply boundary.
// req: REP-002, REP-005, REC-001
use super::*;
use std::future::Future;
use std::task::Poll;
use tokio::sync::oneshot;

#[tokio::test]
async fn follower_stop_drains_complete_received_frame_before_engine_release() {
    for abandon_wait in [false, true] {
        let (fixture, build) = fixture().await;
        let mut receiver = build
            .publish_and_follow(fixture.path.clone())
            .await
            .unwrap();
        let readers = receiver.prepare_readers().await.unwrap();
        let before = fixture.manifest.fence().history().tail();
        let (received, observing) = oneshot::channel();
        let (release, blocked) = oneshot::channel();
        receiver.received_barrier = Some((received, blocked));
        let peer = Arc::new(fixture.peer);
        let routing = crate::runtime_support::RuntimeRoutingState::new();
        let worker =
            RunningFollowerReceiver::start_with_routing(receiver, peer.clone(), routing.clone())
                .unwrap();
        let received_bytes = observing.await.unwrap();
        let received = ChangelogFrameV3::decode(&received_bytes).unwrap();
        let expected = Point::from_receipt(received.receipts().last().unwrap()).unwrap();
        // The source can have a later, unreceived frame. Stop must neither
        // discard this frame nor advance through that unreceived suffix.
        assert!(expected.sequence() < peer.history().tail().sequence());
        assert!(expected.sequence() > before.sequence());
        assert_eq!(readers.latest().unwrap().history().tail(), before);
        assert_eq!(fixture.jobs.capacity.available_permits(), 0);
        assert!(routing.is_routing_allowed());
        let mut shutdown = Box::pin(worker.shutdown());
        // Poll once: this sends stop while the worker is parked after receive.
        // Neither a scheduler delay nor a wall-clock sleep establishes the race.
        std::future::poll_fn(|cx| {
            assert!(shutdown.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        assert!(
            routing.is_routing_allowed(),
            "a supervised drain still owns its complete frame"
        );
        if abandon_wait {
            drop(shutdown);
            release.send(()).unwrap();
        } else {
            release.send(()).unwrap();
            shutdown.await.unwrap();
        }
        assert_eq!(
            routing.stopped().await,
            crate::runtime_support::RuntimeStopReason::CoordinatorFenced
        );
        let drained = fixture.jobs.capacity.acquire().await.unwrap();
        assert_eq!(
            readers.latest().unwrap_err().kind(),
            StorageErrorKind::Unavailable
        );
        let checked = crate::startup::open_redb_follower_startup(&fixture.path, inputs()).unwrap();
        assert_eq!(checked.applier.durable_position().unwrap(), expected);
        let reopened_view = FollowerProjectionTail::default()
            .capture_read_view(&checked.applier)
            .unwrap();
        assert_eq!(reopened_view.acknowledged(), Some(expected));
        drop(reopened_view);
        assert_eq!(
            peer.requests.lock().unwrap().len(),
            1,
            "stop does not reconnect"
        );
        drop(checked);
        drop(drained);
    }
}

#[tokio::test]
async fn follower_stop_does_not_hide_a_received_corrupt_frame() {
    let (fixture, build) = fixture().await;
    let mut receiver = build
        .publish_and_follow(fixture.path.clone())
        .await
        .unwrap();
    let before = fixture.manifest.fence().history().tail();
    let (received, observing) = oneshot::channel();
    let (release, blocked) = oneshot::channel();
    receiver.received_barrier = Some((received, blocked));
    let peer = Arc::new(ItemPeer {
        item: Mutex::new(Some(ReplicationItem::Frame(vec![1].into()))),
    });
    let routing = crate::runtime_support::RuntimeRoutingState::new();
    let worker =
        RunningFollowerReceiver::start_with_routing(receiver, peer, routing.clone()).unwrap();
    observing.await.unwrap();
    let mut shutdown = Box::pin(worker.shutdown());
    std::future::poll_fn(|cx| {
        assert!(shutdown.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    release.send(()).unwrap();
    assert_eq!(
        shutdown.await,
        Err(Failure::Source(
            riffdb_service::ReplicationStreamErrorV3::CorruptHistory
        ))
    );
    assert_eq!(fixture.jobs.capacity.available_permits(), 1);
    assert_eq!(
        routing.stopped().await,
        crate::runtime_support::RuntimeStopReason::CoordinatorFenced
    );
    let checked = crate::startup::open_redb_follower_startup(&fixture.path, inputs()).unwrap();
    assert_eq!(checked.applier.durable_position().unwrap(), before);
}

struct PendingReceivePeer {
    receiving: Arc<tokio::sync::Notify>,
}
struct PendingReceiveStream {
    receiving: Arc<tokio::sync::Notify>,
}
impl ReplicationItemSource for PendingReceiveStream {
    fn next_item(&mut self) -> ReplicationFuture<'_, Option<ReplicationItem>> {
        Box::pin(async move {
            self.receiving.notify_one();
            std::future::pending().await
        })
    }
}
impl ReplicationSourcePort for PendingReceivePeer {
    fn open(&self, _: ReplicationRequest) -> ReplicationFuture<'_, Box<dyn ReplicationItemSource>> {
        Box::pin(async move {
            Ok(Box::new(PendingReceiveStream {
                receiving: self.receiving.clone(),
            }) as Box<dyn ReplicationItemSource>)
        })
    }
}
#[tokio::test]
async fn follower_stop_interrupts_pending_receive_without_inventing_progress() {
    let (fixture, build) = fixture().await;
    let receiver = build
        .publish_and_follow(fixture.path.clone())
        .await
        .unwrap();
    let before = fixture.manifest.fence().history().tail();
    let receiving = Arc::new(tokio::sync::Notify::new());
    let peer = Arc::new(PendingReceivePeer {
        receiving: receiving.clone(),
    });
    let worker = RunningFollowerReceiver::start(receiver, peer).unwrap();
    receiving.notified().await;
    worker.shutdown().await.unwrap();
    assert_eq!(fixture.jobs.capacity.available_permits(), 1);
    let checked = crate::startup::open_redb_follower_startup(&fixture.path, inputs()).unwrap();
    assert_eq!(checked.applier.durable_position().unwrap(), before);
}

#[tokio::test]
async fn follower_stop_before_first_poll_never_opens_a_source() {
    let (fixture, build) = fixture().await;
    let receiver = build
        .publish_and_follow(fixture.path.clone())
        .await
        .unwrap();
    let before = fixture.manifest.fence().history().tail();
    let peer = Arc::new(fixture.peer);
    let worker = RunningFollowerReceiver::start(receiver, peer.clone()).unwrap();
    // On the single-thread runtime there is no yield between spawn and stop.
    worker.shutdown().await.unwrap();
    assert!(peer.requests.lock().unwrap().is_empty());
    assert_eq!(fixture.jobs.capacity.available_permits(), 1);
    let checked = crate::startup::open_redb_follower_startup(&fixture.path, inputs()).unwrap();
    assert_eq!(checked.applier.durable_position().unwrap(), before);
}

struct PendingOpenPeer {
    opening: Arc<tokio::sync::Notify>,
}
impl ReplicationSourcePort for PendingOpenPeer {
    fn open(&self, _: ReplicationRequest) -> ReplicationFuture<'_, Box<dyn ReplicationItemSource>> {
        Box::pin(async move {
            self.opening.notify_one();
            std::future::pending().await
        })
    }
}

#[tokio::test]
async fn follower_stop_interrupts_pending_open_without_inventing_progress() {
    let (fixture, build) = fixture().await;
    let receiver = build
        .publish_and_follow(fixture.path.clone())
        .await
        .unwrap();
    let before = fixture.manifest.fence().history().tail();
    let opening = Arc::new(tokio::sync::Notify::new());
    let peer = Arc::new(PendingOpenPeer {
        opening: opening.clone(),
    });
    let worker = RunningFollowerReceiver::start(receiver, peer).unwrap();
    opening.notified().await;
    worker.shutdown().await.unwrap();
    assert_eq!(fixture.jobs.capacity.available_permits(), 1);
    let checked = crate::startup::open_redb_follower_startup(&fixture.path, inputs()).unwrap();
    assert_eq!(checked.applier.durable_position().unwrap(), before);
}
