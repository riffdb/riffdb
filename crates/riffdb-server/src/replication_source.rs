//! Bounded production replication over immutable publication pins and held artifacts.
#[path = "replication_source_bootstrap.rs"]
mod bootstrap;
use crate::replication_bootstrap::BootstrapSourceJobs;

use crate::replication_publication::ReplicationPublishedSnapshots;
use riffdb_service::{
    PrimaryFenceRequestV1, PrimaryFenceSourceEvidenceV1, ReplicationFailure, ReplicationFuture,
    ReplicationItemSource, ReplicationRequest, ReplicationSourcePort,
};
use riffdb_storage_api::{
    ChangelogFrameCursorV3, ChangelogHistoryPointV3, ChangelogLineageV3,
    ChangelogTransactionSequence, LeadershipEpochV1, ReplicationHandshakeV3,
};
use std::{collections::VecDeque, sync::Arc, time::Duration};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const MAX_STREAMS: usize = 16;
const MAX_PREFETCH_FRAMES: usize = 64;
const PREFETCH_TARGET_BYTES: usize = 256 * 1024;
// A frame cannot be split. Retention is bounded by TARGET + one maximum
// frame, independently of history size; no batch changes wire framing.
#[derive(Default)]
struct FrameBatch {
    frames: VecDeque<riffdb_service::ReplicationFrame>,
    failure: Option<riffdb_errors::ReplicationStreamErrorV3>,
}

fn read_frame_batch(
    mut next: impl FnMut() -> Result<
        Option<riffdb_service::ReplicationFrame>,
        riffdb_errors::ReplicationStreamErrorV3,
    >,
) -> FrameBatch {
    let mut batch = FrameBatch::default();
    let mut bytes = 0;
    while batch.frames.len() < MAX_PREFETCH_FRAMES && bytes < PREFETCH_TARGET_BYTES {
        match next() {
            Ok(Some(frame)) => {
                bytes += frame.len();
                batch.frames.push_back(frame);
            }
            Ok(None) => break,
            Err(error) => {
                // Release the already validated prefix before reporting the
                // next frame's failure, matching one-at-a-time streaming.
                batch.failure = Some(error);
                break;
            }
        }
    }
    batch
}
const IDLE_LIMIT: Duration = Duration::from_secs(30);
const LIFETIME_LIMIT: Duration = Duration::from_secs(15 * 60);

async fn bounded_read<T: Send + 'static>(
    permit: Arc<OwnedSemaphorePermit>,
    read: impl FnOnce() -> T + Send + 'static,
) -> Result<T, tokio::task::JoinError> {
    tokio::task::spawn_blocking(move || {
        let value = read();
        // Cancellation of the async waiter cannot free capacity while this
        // non-cancellable bounded storage read still runs.
        drop(permit);
        value
    })
    .await
}

pub(crate) struct PublishedReplicationSource {
    publications: ReplicationPublishedSnapshots,
    capacity: Arc<Semaphore>,
    bootstrap: BootstrapSourceJobs,
}

impl PublishedReplicationSource {
    pub(crate) fn new(
        publications: ReplicationPublishedSnapshots,
        bootstrap: BootstrapSourceJobs,
    ) -> Self {
        Self {
            publications,
            bootstrap,
            capacity: Arc::new(Semaphore::new(MAX_STREAMS)),
        }
    }
}

/// Checks the source coordinates and exact supported catalog named by the peer.
/// This is request validation; only the published pin can prove that lineage.
pub(crate) fn request_lineage(
    request: &ReplicationRequest,
) -> Result<ChangelogLineageV3, ReplicationFailure> {
    let invalid =
        || ReplicationFailure::Source(riffdb_errors::ReplicationStreamErrorV3::InvalidPosition);
    let coordinates = ChangelogLineageV3::new(
        request.database_id,
        request.history_incarnation,
        LeadershipEpochV1::new(request.leadership_epoch).ok_or_else(invalid)?,
    )
    .map_err(|_| invalid())?;
    ChangelogLineageV3::new_with_catalog(
        coordinates.database_id(),
        coordinates.history_incarnation(),
        coordinates.leadership_epoch(),
        request.catalog_digest,
    )
    .map_err(|_| {
        ReplicationFailure::Source(riffdb_errors::ReplicationStreamErrorV3::UnsupportedCatalog)
    })
}

impl ReplicationSourcePort for PublishedReplicationSource {
    fn primary_fence_source_evidence(
        &self,
        request: PrimaryFenceRequestV1,
        applied: ChangelogHistoryPointV3,
    ) -> ReplicationFuture<'_, PrimaryFenceSourceEvidenceV1> {
        Box::pin(async move {
            let permit = Arc::new(
                Arc::clone(&self.capacity)
                    .try_acquire_owned()
                    .map_err(|_| ReplicationFailure::Unavailable)?,
            );
            let mut publications = self.publications.clone();
            let pin = publications
                .latest()
                .map_err(ReplicationFailure::Source)?
                .ok_or(ReplicationFailure::Unavailable)?;
            let evidence = tokio::time::timeout(
                IDLE_LIMIT,
                bounded_read(permit, move || {
                    pin.primary_fence_source_evidence_v1(request, applied)
                }),
            )
            .await
            .map_err(|_| ReplicationFailure::Unavailable)?
            .map_err(|_| ReplicationFailure::Unavailable)?
            .map_err(bootstrap::storage_failure)?
            .ok_or(ReplicationFailure::Source(
                riffdb_errors::ReplicationStreamErrorV3::InvalidPosition,
            ))?;
            // An uncertainty notification while reading must still refuse the
            // release. Later audit-only publications cannot undo this fence.
            publications
                .latest()
                .map_err(ReplicationFailure::Source)?
                .ok_or(ReplicationFailure::Unavailable)?;
            Ok(evidence)
        })
    }

    fn open(
        &self,
        request: ReplicationRequest,
    ) -> ReplicationFuture<'_, Box<dyn ReplicationItemSource>> {
        Box::pin(async move {
            let invalid = || {
                ReplicationFailure::Source(riffdb_errors::ReplicationStreamErrorV3::InvalidPosition)
            };
            let lineage = request_lineage(&request)?;
            let permit = Arc::new(
                Arc::clone(&self.capacity)
                    .try_acquire_owned()
                    .map_err(|_| ReplicationFailure::Unavailable)?,
            );
            let expires = tokio::time::Instant::now() + LIFETIME_LIMIT;
            if matches!(
                request.phase,
                riffdb_service::ReplicationPhase::Bootstrap { .. }
            ) {
                return self.open_bootstrap(request, lineage, permit, expires).await;
            }
            let after = ChangelogHistoryPointV3::new(
                ChangelogTransactionSequence::new(request.after_sequence).ok_or_else(invalid)?,
                request.after_hash,
                request.after_frontier,
            );
            let handshake = bootstrap::handshake(&request, lineage, after)?;
            let mut publications = self.publications.clone();
            let pin = publications
                .latest()
                .map_err(ReplicationFailure::Source)?
                .ok_or(ReplicationFailure::Unavailable)?;
            if let riffdb_service::ReplicationPhase::FenceEvidence { request: selected } =
                request.phase
            {
                let evidence = bounded_read(Arc::clone(&permit), move || {
                    ChangelogFrameCursorV3::open(pin.as_ref(), handshake)?;
                    pin.primary_fence_source_evidence_v1(selected, after)
                        .map_err(riffdb_errors::ReplicationStreamErrorV3::from)?
                        .ok_or(riffdb_errors::ReplicationStreamErrorV3::InvalidPosition)
                })
                .await
                .map_err(|_| ReplicationFailure::Unavailable)?
                .map_err(ReplicationFailure::Source)?;
                return Ok(Box::new(FenceItems {
                    evidence: Some(evidence),
                    publications,
                    expires,
                    _permit: permit,
                }) as Box<dyn ReplicationItemSource>);
            }
            let cursor = bounded_read(Arc::clone(&permit), move || {
                ChangelogFrameCursorV3::open(pin.as_ref(), handshake)
            })
            .await
            .map_err(|_| ReplicationFailure::Unavailable)?
            .map_err(ReplicationFailure::Source)?;
            if let riffdb_service::ReplicationPhase::Follower { hold_id } = request.phase {
                let id = riffdb_storage_api::ReplicationSourceHoldIdV1::new(hold_id)
                    .ok_or_else(invalid)?;
                self.bootstrap
                    .acknowledge_follower(id, lineage, after)
                    .await
                    .map_err(bootstrap::storage_failure)?;
            }
            if let riffdb_service::ReplicationPhase::Attach { manifest } = &request.phase {
                let manifest = riffdb_storage_api::ReplicationBootstrapManifestV1::decode(manifest)
                    .map_err(|_| invalid())?;
                if manifest.fence().history().lineage() != lineage
                    || manifest.fence().history().tail() != after
                {
                    return Err(invalid());
                }
                self.bootstrap
                    .attach(manifest, after)
                    .await
                    .map_err(bootstrap::storage_failure)?;
            }
            Ok(Box::new(PublishedFrames {
                publications,
                cursor: Some(cursor),
                pending: FrameBatch::default(),
                expires,
                permit,
            }) as Box<dyn ReplicationItemSource>)
        })
    }
}

struct FenceItems {
    evidence: Option<PrimaryFenceSourceEvidenceV1>,
    publications: ReplicationPublishedSnapshots,
    expires: tokio::time::Instant,
    _permit: Arc<OwnedSemaphorePermit>,
}
impl ReplicationItemSource for FenceItems {
    fn next_item(&mut self) -> ReplicationFuture<'_, Option<riffdb_service::ReplicationItem>> {
        Box::pin(async move {
            let Some(evidence) = self.evidence.take() else {
                return Ok(None);
            };
            if tokio::time::Instant::now() >= self.expires {
                return Err(ReplicationFailure::Unavailable);
            }
            self.publications
                .latest()
                .map_err(ReplicationFailure::Source)?
                .ok_or(ReplicationFailure::Unavailable)?;
            Ok(Some(riffdb_service::ReplicationItem::FenceEvidence(
                Box::new(evidence),
            )))
        })
    }
}

struct PublishedFrames {
    publications: ReplicationPublishedSnapshots,
    cursor: Option<ChangelogFrameCursorV3>,
    pending: FrameBatch,
    expires: tokio::time::Instant,
    permit: Arc<OwnedSemaphorePermit>,
}

impl ReplicationItemSource for PublishedFrames {
    fn next_item(&mut self) -> ReplicationFuture<'_, Option<riffdb_service::ReplicationItem>> {
        Box::pin(async move {
            loop {
                if tokio::time::Instant::now() >= self.expires {
                    return Ok(None);
                }
                let newer = self
                    .publications
                    .take_newer()
                    .map_err(ReplicationFailure::Source)?;
                if let Some(pin) = newer {
                    // Revalidate changed publication authority before releasing
                    // buffered bytes; an unavailable/changed lineage cannot be
                    // hidden behind the prefetch queue.
                    let mut cursor = self.cursor.take().ok_or(ReplicationFailure::Unavailable)?;
                    self.cursor = Some(
                        bounded_read(Arc::clone(&self.permit), move || {
                            cursor.advance_snapshot(pin.as_ref())?;
                            Ok(cursor)
                        })
                        .await
                        .map_err(|_| ReplicationFailure::Unavailable)?
                        .map_err(ReplicationFailure::Source)?,
                    );
                }
                if let Some(frame) = self.pending.frames.pop_front() {
                    return Ok(Some(riffdb_service::ReplicationItem::Frame(frame)));
                }
                if let Some(error) = self.pending.failure {
                    return Err(ReplicationFailure::Source(error));
                }
                let mut cursor = self.cursor.take().ok_or(ReplicationFailure::Unavailable)?;
                let (returned, batch) = bounded_read(Arc::clone(&self.permit), move || {
                    let result = (|| {
                        let head = cursor.published_head()?;
                        let observation = riffdb_service::ReplicationSourceHead::new(
                            head.sequence().get(),
                            head.frontier(),
                        )
                        .ok_or(riffdb_errors::ReplicationStreamErrorV3::CorruptHistory)?;
                        Ok(read_frame_batch(|| {
                            cursor.next_frame().map(|frame| {
                                frame.map(|frame| {
                                    riffdb_service::ReplicationFrame::new(
                                        frame.into_bytes(),
                                        Some(observation),
                                    )
                                })
                            })
                        }))
                    })();
                    (cursor, result)
                })
                .await
                .map_err(|_| ReplicationFailure::Unavailable)?;
                self.cursor = Some(returned);
                self.pending = batch.map_err(ReplicationFailure::Source)?;
                if let Some(frame) = self.pending.frames.pop_front() {
                    return Ok(Some(riffdb_service::ReplicationItem::Frame(frame)));
                }
                if let Some(error) = self.pending.failure {
                    return Err(ReplicationFailure::Source(error));
                }
                let deadline = self.expires.min(tokio::time::Instant::now() + IDLE_LIMIT);
                let pin = match tokio::time::timeout_at(deadline, self.publications.changed()).await
                {
                    Ok(result) => result.map_err(ReplicationFailure::Source)?,
                    Err(_) => return Ok(None),
                };
                let mut cursor = self.cursor.take().ok_or(ReplicationFailure::Unavailable)?;
                self.cursor = Some(
                    bounded_read(Arc::clone(&self.permit), move || {
                        cursor.advance_snapshot(pin.as_ref())?;
                        Ok(cursor)
                    })
                    .await
                    .map_err(|_| ReplicationFailure::Unavailable)?
                    .map_err(ReplicationFailure::Source)?,
                );
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // req: REP-003, PERF-007
    #[test]
    fn prefetch_bounds_reads_and_preserves_valid_prefix_before_failure() {
        let mut calls = 0;
        let batch = read_frame_batch(|| {
            calls += 1;
            Ok(Some(vec![calls as u8].into()))
        });
        assert_eq!(calls, MAX_PREFETCH_FRAMES);
        assert_eq!(batch.frames.len(), MAX_PREFETCH_FRAMES);
        assert_eq!(
            batch
                .frames
                .iter()
                .map(|frame| frame[0])
                .collect::<Vec<_>>(),
            (1..=64).collect::<Vec<_>>()
        );
        let mut calls = 0;
        let batch = read_frame_batch(|| {
            calls += 1;
            Ok(Some(vec![0; PREFETCH_TARGET_BYTES / 2 + 1].into()))
        });
        assert_eq!(calls, 2);
        assert_eq!(batch.frames.len(), 2);
        let mut calls = 0;
        let batch = read_frame_batch(|| {
            calls += 1;
            if calls == 3 {
                Err(riffdb_errors::ReplicationStreamErrorV3::CorruptHistory)
            } else {
                Ok(Some(vec![calls as u8].into()))
            }
        });
        assert_eq!(calls, 3);
        assert_eq!(
            batch
                .frames
                .iter()
                .map(|frame| frame[0])
                .collect::<Vec<_>>(),
            [1, 2]
        );
        assert_eq!(
            batch.failure,
            Some(riffdb_errors::ReplicationStreamErrorV3::CorruptHistory)
        );
    }

    #[tokio::test]
    // req: REP-003, PERF-007
    async fn cancelled_replication_read_keeps_capacity_until_the_storage_read_finishes() {
        let capacity = Arc::new(Semaphore::new(1));
        let permit = Arc::new(Arc::clone(&capacity).try_acquire_owned().unwrap());
        let (started, observing) = tokio::sync::oneshot::channel();
        let (release, blocked) = std::sync::mpsc::sync_channel(1);
        let waiter = tokio::spawn(bounded_read(permit, move || {
            started.send(()).unwrap();
            blocked.recv().unwrap();
        }));
        observing.await.unwrap();
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        assert!(Arc::clone(&capacity).try_acquire_owned().is_err());
        release.send(()).unwrap();
        let _returned_capacity = capacity.acquire_owned().await.unwrap();
    }
}

#[cfg(test)]
#[path = "replication_source_tests.rs"]
mod source_tests;
