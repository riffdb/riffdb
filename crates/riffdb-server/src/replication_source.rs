//! Bounded production replication over immutable publication pins and held artifacts.
#[path = "replication_source_bootstrap.rs"]
mod bootstrap;
use crate::replication_bootstrap::BootstrapSourceJobs;

use crate::replication_publication::ReplicationPublishedSnapshots;
use riffdb_service::{
    ReplicationFailure, ReplicationFuture, ReplicationItemSource, ReplicationRequest,
    ReplicationSourcePort,
};
use riffdb_storage_api::{
    ChangelogFrameCursorV3, ChangelogHistoryPointV3, ChangelogLineageV3,
    ChangelogTransactionSequence, LeadershipEpochV1, ReplicationHandshakeV3,
};
use std::{sync::Arc, time::Duration};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const MAX_STREAMS: usize = 16;
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

impl ReplicationSourcePort for PublishedReplicationSource {
    fn open(
        &self,
        request: ReplicationRequest,
    ) -> ReplicationFuture<'_, Box<dyn ReplicationItemSource>> {
        Box::pin(async move {
            let invalid = || {
                ReplicationFailure::Source(riffdb_errors::ReplicationStreamErrorV3::InvalidPosition)
            };
            let lineage = ChangelogLineageV3::new(
                request.database_id,
                request.history_incarnation,
                LeadershipEpochV1::new(request.leadership_epoch).ok_or_else(invalid)?,
            )
            .map_err(|_| invalid())?;
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
                expires,
                permit,
            }) as Box<dyn ReplicationItemSource>)
        })
    }
}

struct PublishedFrames {
    publications: ReplicationPublishedSnapshots,
    cursor: Option<ChangelogFrameCursorV3>,
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
                let mut cursor = self.cursor.take().ok_or(ReplicationFailure::Unavailable)?;
                let newer = self
                    .publications
                    .take_newer()
                    .map_err(ReplicationFailure::Source)?;
                let (returned, frame) = bounded_read(Arc::clone(&self.permit), move || {
                    let result = (|| {
                        if let Some(pin) = newer {
                            cursor.advance_snapshot(pin.as_ref())?;
                        }
                        cursor.next_frame()
                    })();
                    (cursor, result)
                })
                .await
                .map_err(|_| ReplicationFailure::Unavailable)?;
                self.cursor = Some(returned);
                let frame = frame.map_err(ReplicationFailure::Source)?;
                if frame.is_some() {
                    return Ok(frame
                        .map(|frame| riffdb_service::ReplicationItem::Frame(frame.into_bytes())));
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
