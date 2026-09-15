//! Isolated exact V3 control receipts prove receiver batching and retries.
//! Full authoritative workload equivalence is a separate package exit proof.
// req: REP-002, REP-003, REC-001, PERF-007
use super::*;
use riffdb_storage_api::{
    AuthoritativeTransactionBindingV3, AuthoritativeTransactionV3, ChangelogFrameBindingV3,
};
use std::collections::VecDeque;

struct ReceiptPeer {
    lineage: ChangelogLineageV3,
    receipts: Vec<AuthoritativeTransactionV3>,
    requests: Mutex<Vec<ReplicationRequest>>,
}
struct ReceiptStream {
    frames: VecDeque<Vec<u8>>,
    head: Option<riffdb_service::ReplicationSourceHead>,
}
impl ReplicationItemSource for ReceiptStream {
    fn next_item(&mut self) -> ReplicationFuture<'_, Option<ReplicationItem>> {
        Box::pin(async move {
            Ok(self.frames.pop_front().map(|bytes| {
                ReplicationItem::Frame(riffdb_service::ReplicationFrame::new(bytes, self.head))
            }))
        })
    }
}
impl ReplicationSourcePort for ReceiptPeer {
    fn open(
        &self,
        request: ReplicationRequest,
    ) -> ReplicationFuture<'_, Box<dyn ReplicationItemSource>> {
        Box::pin(async move {
            self.requests.lock().unwrap().push(request.clone());
            let mut frame_hash = request.after_hash;
            let frames = self
                .receipts
                .iter()
                .filter(|receipt| receipt.binding().sequence.get() > request.after_sequence)
                .map(|receipt| {
                    let bytes = ChangelogFrameV3::new(
                        ChangelogFrameBindingV3::new(
                            self.lineage.database_id(),
                            self.lineage.history_incarnation(),
                            self.lineage.leadership_epoch().get(),
                            AuthoritativeStateCatalogV1.digest(),
                            frame_hash,
                        )
                        .unwrap(),
                        vec![receipt.clone()],
                    )
                    .unwrap()
                    .encode()
                    .unwrap();
                    frame_hash = *bytes.last_chunk::<32>().unwrap();
                    bytes
                })
                .collect();
            let head = self.receipts.last().and_then(|receipt| {
                riffdb_service::ReplicationSourceHead::new(
                    receipt.binding().sequence.get(),
                    receipt.binding().covered_frontier,
                )
            });
            Ok(Box::new(ReceiptStream { frames, head }) as Box<dyn ReplicationItemSource>)
        })
    }
}
fn receipts(manifest: Manifest, count: usize) -> ReceiptPeer {
    let lineage = manifest.fence().history().lineage();
    let mut position = manifest.fence().history().tail();
    let mut receipts = Vec::new();
    for index in 0..count {
        let receipt = AuthoritativeTransactionV3::new(
            AuthoritativeTransactionBindingV3 {
                database_id: lineage.database_id(),
                history_incarnation: lineage.history_incarnation(),
                predecessor: Some(position.sequence()),
                sequence: position.sequence().checked_next().unwrap(),
                predecessor_frontier: position.frontier(),
                covered_frontier: position.frontier(),
                prior_history_hash: position.history_hash(),
            },
            if index == count - 1 {
                ChangelogAttributionV3::ReplicationSourceHold
            } else {
                ChangelogAttributionV3::HistoryReclamation
            },
            vec![],
        )
        .unwrap();
        position = Point::from_receipt(&receipt).unwrap();
        receipts.push(receipt);
    }
    ReceiptPeer {
        lineage,
        receipts,
        requests: Mutex::new(vec![]),
    }
}

#[tokio::test]
// req: REP-004
async fn continuous_receiver_reports_after_bounded_batches_and_flushes_progress_at_eof() {
    let (fixture, build) = fixture().await;
    let peer = receipts(fixture.manifest, 34);
    let mut receiver = build
        .publish_and_follow(fixture.path.clone())
        .await
        .unwrap();
    let readers = receiver.prepare_readers().await.unwrap();
    assert!(readers.latest().unwrap().source_head().is_none());
    for receipt in &peer.receipts {
        let applied = receiver.advance(&peer).await.unwrap().unwrap();
        assert_eq!(applied, Point::from_receipt(receipt).unwrap());
        let view = readers.latest().unwrap();
        assert_eq!(view.history().tail(), applied);
        let head = view.source_head().unwrap();
        assert_eq!(
            head.transaction_sequence(),
            peer.receipts.last().unwrap().binding().sequence.get()
        );
        assert_eq!(
            head.frontier(),
            peer.receipts.last().unwrap().binding().covered_frontier
        );
    }
    assert_eq!(peer.requests.lock().unwrap().len(), 2);
    assert_eq!(receiver.advance(&peer).await.unwrap(), None); // EOF with pending progress
    assert_eq!(receiver.advance(&peer).await.unwrap(), None); // report final durable point
    assert_eq!(receiver.advance(&peer).await.unwrap(), None); // idle read-only tail
    {
        let requests = peer.requests.lock().unwrap();
        assert_eq!(requests.len(), 4);
        assert!(matches!(requests[0].phase, ReplicationPhase::Attach { .. }));
        for (index, receipt) in [(1, &peer.receipts[31]), (2, &peer.receipts[33])] {
            assert!(
                matches!(requests[index].phase, ReplicationPhase::Follower { hold_id } if hold_id == *fixture.manifest.fence().hold_id().as_bytes())
            );
            let point = Point::from_receipt(receipt).unwrap();
            assert_eq!(requests[index].after_sequence, point.sequence().get());
            assert_eq!(requests[index].after_hash, point.history_hash());
            assert_eq!(requests[index].after_frontier, point.frontier());
        }
        assert!(matches!(requests[3].phase, ReplicationPhase::Tail));
    }
    receiver.close().await.unwrap();
    let reopened = crate::startup::open_redb_follower_startup(&fixture.path, inputs()).unwrap();
    assert_eq!(
        reopened.applier.durable_position().unwrap(),
        Point::from_receipt(peer.receipts.last().unwrap()).unwrap()
    );
}

struct LostClaim<'a>(&'a ReceiptPeer);
#[tokio::test]
// req: REP-004, REC-002
async fn source_head_mismatch_or_regression_refuses_before_apply_and_withdraws_reads() {
    for fault in 0..3 {
        let (fixture, build) = fixture().await;
        let peer = receipts(fixture.manifest, 2);
        let mut receiver = build
            .publish_and_follow(fixture.path.clone())
            .await
            .unwrap();
        let readers = receiver.prepare_readers().await.unwrap();
        let initial = fixture.manifest.fence().history().tail();
        let first = Point::from_receipt(&peer.receipts[0]).unwrap();
        let second = Point::from_receipt(&peer.receipts[1]).unwrap();
        let item = |receipt: &AuthoritativeTransactionV3, head| {
            let frame = ChangelogFrameV3::new(
                ChangelogFrameBindingV3::new(
                    peer.lineage.database_id(),
                    peer.lineage.history_incarnation(),
                    peer.lineage.leadership_epoch().get(),
                    AuthoritativeStateCatalogV1.digest(),
                    receipt.binding().prior_history_hash,
                )
                .unwrap(),
                vec![receipt.clone()],
            )
            .unwrap()
            .encode()
            .unwrap();
            ItemPeer {
                item: Mutex::new(Some(ReplicationItem::Frame(
                    riffdb_service::ReplicationFrame::new(frame, Some(head)),
                ))),
            }
        };
        let expected = if fault == 2 {
            let far = riffdb_service::ReplicationSourceHead::new(
                second.sequence().get() + 10,
                second.frontier(),
            )
            .unwrap();
            assert_eq!(
                receiver
                    .advance(&item(&peer.receipts[0], far))
                    .await
                    .unwrap(),
                Some(first)
            );
            // Source progress never substitutes for actual durable apply.
            assert_eq!(readers.latest().unwrap().history().tail(), first);
            assert_eq!(readers.latest().unwrap().source_head(), Some(far));
            assert_eq!(
                receiver
                    .advance(&item(&peer.receipts[0], far))
                    .await
                    .unwrap(),
                None
            );
            first
        } else {
            initial
        };
        let (receipt, sequence, frontier) = match fault {
            0 => (
                &peer.receipts[0],
                initial.sequence().get(),
                first.frontier(),
            ),
            1 => (
                &peer.receipts[0],
                first.sequence().get(),
                riffdb_types::DualFrontier::new(
                    Some(
                        riffdb_types::CommitSequence::new(
                            first.frontier().application().map_or(1, |v| v.get() + 1),
                        )
                        .unwrap(),
                    ),
                    first.frontier().administration(),
                ),
            ),
            _ => (
                &peer.receipts[1],
                second.sequence().get(),
                second.frontier(),
            ),
        };
        let bad = riffdb_service::ReplicationSourceHead::new(sequence, frontier).unwrap();
        assert_eq!(
            receiver.advance(&item(receipt, bad)).await,
            Err(Failure::Source(
                riffdb_service::ReplicationStreamErrorV3::CorruptHistory,
            ))
        );
        assert_eq!(
            readers.latest().unwrap_err().kind(),
            StorageErrorKind::Unavailable
        );
        drop(receiver);
        let opened = crate::startup::open_redb_follower_startup(&fixture.path, inputs()).unwrap();
        assert_eq!(opened.applier.durable_position().unwrap(), expected);
    }
}

struct LostClaimStream(Box<dyn ReplicationItemSource>);
impl ReplicationSourcePort for LostClaim<'_> {
    fn open(
        &self,
        request: ReplicationRequest,
    ) -> ReplicationFuture<'_, Box<dyn ReplicationItemSource>> {
        Box::pin(async move {
            Ok(Box::new(LostClaimStream(self.0.open(request).await?))
                as Box<dyn ReplicationItemSource>)
        })
    }
}
impl ReplicationItemSource for LostClaimStream {
    fn next_item(&mut self) -> ReplicationFuture<'_, Option<ReplicationItem>> {
        Box::pin(async move {
            assert!(self.0.next_item().await?.is_none());
            Err(Failure::Unavailable)
        })
    }
}

#[tokio::test]
async fn continuous_receiver_retries_an_uncertain_tail_claim_after_validated_reopen() {
    let (fixture, build) = fixture().await;
    let peer = receipts(fixture.manifest, 2);
    let mut receiver = build
        .publish_and_follow(fixture.path.clone())
        .await
        .unwrap();
    receiver.advance(&peer).await.unwrap().unwrap();
    let position = receiver.advance(&peer).await.unwrap().unwrap();
    assert_eq!(receiver.advance(&peer).await.unwrap(), None);
    assert_eq!(
        receiver.advance(&LostClaim(&peer)).await,
        Err(Failure::Unavailable)
    );
    assert_eq!(fixture.jobs.capacity.available_permits(), 0);
    drop(receiver); // Simulate a restart after the uncertain network result.
    assert_eq!(fixture.jobs.capacity.available_permits(), 1);
    let mut receiver = fixture
        .jobs
        .reopen_follower(
            fixture.path.clone(),
            inputs(),
            peer.lineage,
            fixture.manifest.fence().hold_id(),
            Some(fixture.manifest),
        )
        .await
        .unwrap();
    assert_eq!(receiver.advance(&peer).await.unwrap(), None);
    {
        let requests = peer.requests.lock().unwrap();
        for index in [1, 2] {
            assert!(matches!(
                requests[index].phase,
                ReplicationPhase::Follower { .. }
            ));
            assert_eq!(requests[index].after_sequence, position.sequence().get());
            assert_eq!(requests[index].after_hash, position.history_hash());
        }
    }
    receiver.close().await.unwrap();
}

#[tokio::test]
async fn continuous_receiver_network_deadline_reconnects_without_releasing_the_applier() {
    let (fixture, build) = fixture().await;
    let mut receiver = build
        .publish_and_follow(fixture.path.clone())
        .await
        .unwrap();
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
    tokio::time::pause();
    tokio::time::advance(LIFETIME).await;
    assert_eq!(receive.await.unwrap(), None);
    tokio::time::resume();
    assert!(peer.dropped.load(Ordering::SeqCst));
    assert_eq!(fixture.jobs.capacity.available_permits(), 0);
    assert!(riffdb_storage_redb::RedbFollowerStore::open(&fixture.path).is_err());
    assert!(receiver.attachment.is_some());
    assert!(receiver.advance(&fixture.peer).await.unwrap().is_some());
    assert_eq!(fixture.peer.history(), after_attachment);
    receiver.close().await.unwrap();
}

struct UnavailablePeer {
    at_open: bool,
    error: Failure,
}
struct UnavailableStream(Failure);
impl ReplicationSourcePort for UnavailablePeer {
    fn open(&self, _: ReplicationRequest) -> ReplicationFuture<'_, Box<dyn ReplicationItemSource>> {
        Box::pin(async move {
            if self.at_open {
                Err(self.error)
            } else {
                Ok(Box::new(UnavailableStream(self.error)) as Box<dyn ReplicationItemSource>)
            }
        })
    }
}
impl ReplicationItemSource for UnavailableStream {
    fn next_item(&mut self) -> ReplicationFuture<'_, Option<ReplicationItem>> {
        Box::pin(async move { Err(self.0) })
    }
}

#[tokio::test]
async fn continuous_receiver_transient_peer_failures_preserve_custody_for_same_position_retry() {
    let (fixture, build) = fixture().await;
    let mut receiver = build
        .publish_and_follow(fixture.path.clone())
        .await
        .unwrap();
    for at_open in [false, true] {
        for error in [
            Failure::Unavailable,
            Failure::Source(riffdb_service::ReplicationStreamErrorV3::Unavailable),
        ] {
            assert_eq!(
                receiver.advance(&UnavailablePeer { at_open, error }).await,
                Err(error)
            );
            assert_eq!(fixture.jobs.capacity.available_permits(), 0);
            assert!(riffdb_storage_redb::RedbFollowerStore::open(&fixture.path).is_err());
            assert!(receiver.attachment.is_some());
        }
    }
    assert!(receiver.advance(&fixture.peer).await.unwrap().is_some());
    receiver.close().await.unwrap();
}

#[tokio::test]
async fn continuous_receiver_terminal_refusals_require_validated_reopen() {
    let (fixture, build) = fixture().await;
    let mut receiver = build
        .publish_and_follow(fixture.path.clone())
        .await
        .unwrap();
    for (index, error) in [
        Failure::AuthorizationDenied,
        Failure::Source(riffdb_service::ReplicationStreamErrorV3::HistoryPruned),
        Failure::Source(riffdb_service::ReplicationStreamErrorV3::StaleEpoch),
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
        let peer = UnavailablePeer {
            at_open: index % 2 == 0,
            error,
        };
        assert_eq!(receiver.advance(&peer).await, Err(error));
        assert_eq!(fixture.jobs.capacity.available_permits(), 1);
        assert_eq!(
            receiver.advance(&fixture.peer).await,
            Err(Failure::Unavailable)
        );
        let reopened = crate::startup::open_redb_follower_startup(&fixture.path, inputs()).unwrap();
        assert_eq!(
            reopened.applier.durable_position().unwrap(),
            fixture.manifest.fence().history().tail()
        );
    }
}
