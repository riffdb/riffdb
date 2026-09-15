//! One held immutable transfer per connection, with fresh source fencing.
use super::*;
use crate::replication_bootstrap::HeldBootstrapSourceJob;
use riffdb_service::{ReplicationItem, ReplicationPhase};
use riffdb_storage_api::{
    ChangelogCursorErrorV3, ReplicationBootstrapManifestV1 as Manifest,
    ReplicationSourceHoldIdV1 as HoldId,
};
use tokio::time::Instant;

pub(super) fn storage_failure(error: ChangelogCursorErrorV3) -> ReplicationFailure {
    ReplicationFailure::Source(error.into())
}
fn invalid() -> ReplicationFailure {
    ReplicationFailure::Source(riffdb_errors::ReplicationStreamErrorV3::InvalidPosition)
}
pub(super) fn handshake(
    request: &ReplicationRequest,
    lineage: ChangelogLineageV3,
    after: ChangelogHistoryPointV3,
) -> Result<ReplicationHandshakeV3, ReplicationFailure> {
    ReplicationHandshakeV3::new(
        lineage,
        after,
        &request.readable_format,
        request.catalog_digest,
        request.maximum_frame_bytes,
        request.maximum_transitions,
    )
    .map_err(ReplicationFailure::Source)
}
impl PublishedReplicationSource {
    pub(super) async fn open_bootstrap(
        &self,
        request: ReplicationRequest,
        lineage: ChangelogLineageV3,
        permit: Arc<OwnedSemaphorePermit>,
        expires: Instant,
    ) -> Result<Box<dyn ReplicationItemSource>, ReplicationFailure> {
        let ReplicationPhase::Bootstrap {
            hold_id,
            resume_manifest,
            after_page,
        } = &request.phase
        else {
            return Err(invalid());
        };
        let id = HoldId::new(*hold_id).ok_or_else(invalid)?;
        let expected = if resume_manifest.is_empty() {
            if *after_page != 0 {
                return Err(invalid());
            }
            None
        } else {
            let manifest = Manifest::decode(resume_manifest).map_err(|_| invalid())?;
            if manifest.fence().hold_id() != id
                || manifest.fence().history().lineage() != lineage
                || *after_page > manifest.page_count()
            {
                return Err(invalid());
            }
            Some(manifest)
        };
        let mut publications = self.publications.clone();
        let pin = publications
            .latest()
            .map_err(ReplicationFailure::Source)?
            .ok_or(ReplicationFailure::Unavailable)?;
        // Opening the authoritative cursor reads only bounded root metadata;
        // no authority rows are scanned to negotiate this source's current tail.
        let requested = request.clone();
        bounded_read(Arc::clone(&permit), move || {
            let history = pin
                .authoritative_state_v3()
                .map_err(storage_failure)?
                .history();
            let handshake = handshake(&requested, lineage, history.tail())?;
            ChangelogFrameCursorV3::open(pin.as_ref(), handshake)
                .map_err(ReplicationFailure::Source)
        })
        .await
        .map_err(|_| ReplicationFailure::Unavailable)??;
        let mut build = if expected.is_some() {
            self.bootstrap.resume_managed(id).await
        } else {
            self.bootstrap.begin_managed(id).await
        }
        .map_err(storage_failure)?;
        while !build.advance().await.map_err(storage_failure)? {}
        let mut held = build.finish().await.map_err(storage_failure)?;
        let manifest = held.manifest().await.map_err(storage_failure)?;
        if expected.is_some_and(|expected| expected != manifest)
            || manifest.fence().history().lineage() != lineage
            || *after_page > manifest.page_count()
        {
            return Err(invalid());
        }
        let handshake = handshake(&request, lineage, manifest.fence().history().tail())?;
        let mut source = BootstrapItems {
            publications,
            held: Some(held),
            manifest,
            handshake,
            next_page: after_page + 1,
            emit_manifest: true,
            permit,
            expires,
        };
        source.check_current().await?;
        Ok(Box::new(source))
    }
}
struct BootstrapItems {
    publications: ReplicationPublishedSnapshots,
    held: Option<HeldBootstrapSourceJob>,
    manifest: Manifest,
    handshake: ReplicationHandshakeV3,
    next_page: u32,
    emit_manifest: bool,
    permit: Arc<OwnedSemaphorePermit>,
    expires: Instant,
}
impl BootstrapItems {
    async fn check_current(&mut self) -> Result<(), ReplicationFailure> {
        if Instant::now() >= self.expires {
            return Err(ReplicationFailure::Unavailable);
        }
        let pin = self
            .publications
            .latest()
            .map_err(ReplicationFailure::Source)?
            .ok_or(ReplicationFailure::Unavailable)?;
        let handshake = self.handshake;
        bounded_read(Arc::clone(&self.permit), move || {
            ChangelogFrameCursorV3::open(pin.as_ref(), handshake)
        })
        .await
        .map_err(|_| ReplicationFailure::Unavailable)?
        .map_err(ReplicationFailure::Source)?;
        Ok(())
    }
}
impl ReplicationItemSource for BootstrapItems {
    fn next_item(&mut self) -> ReplicationFuture<'_, Option<ReplicationItem>> {
        Box::pin(async move {
            // Move the sole artifact owner out before any wait; failure or
            // cancellation fuses this handle and releases it after actual work.
            let Some(mut held) = self.held.take() else {
                return Ok(None);
            };
            if !self.emit_manifest && self.next_page > self.manifest.page_count() {
                return Ok(None);
            }
            self.check_current().await?;
            let item = if self.emit_manifest {
                if held.manifest().await.map_err(storage_failure)? != self.manifest {
                    return Err(invalid());
                }
                self.emit_manifest = false;
                ReplicationItem::BootstrapManifest(self.manifest.encode().map_err(|_| invalid())?)
            } else {
                let page = held
                    .read_page(self.next_page)
                    .await
                    .map_err(storage_failure)?;
                self.next_page += 1;
                ReplicationItem::BootstrapPage(
                    bounded_read(Arc::clone(&self.permit), move || page.encode())
                        .await
                        .map_err(|_| ReplicationFailure::Unavailable)?
                        .map_err(|_| invalid())?,
                )
            };
            self.check_current().await?;
            self.held = Some(held);
            Ok(Some(item))
        })
    }
}
