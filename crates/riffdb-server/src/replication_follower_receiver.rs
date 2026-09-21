//! Bounded continuous receive/apply custody, below the application boundary.
#[path = "replication_follower_reads.rs"]
mod reads;
pub use reads::{FollowerReadSnapshots, FollowerReadView};

#[path = "replication_follower_worker.rs"]
mod worker;
use super::*;
use crate::replication_bootstrap::projection_tail::FollowerProjectionTail;
use riffdb_service::{
    ReplicationFailure as Failure, ReplicationItem, ReplicationItemSource, ReplicationPhase,
    ReplicationRequest, ReplicationSourcePort,
};
use riffdb_storage_api::{
    ChangelogAttributionV3, ChangelogFrameV3, ChangelogHistoryPointV3 as Point, ChangelogLineageV3,
    MAX_CHANGELOG_FRAME_BYTES, MAX_STAGED_COMMANDS, ReplicationSourceHoldIdV1 as HoldId,
};
pub use worker::RunningFollowerReceiver;

const REPORT_FRAMES: u8 = 32;

impl BootstrapReceiverBuildJob {
    /// Retains the same construction slot and sole validated applier through
    /// publication into continuous tail custody. The first request attaches only
    /// after persisting a local acknowledgement at the manifest fence.
    pub async fn publish_and_follow(self, path: PathBuf) -> Result<FollowerReceiver, StorageError> {
        let (owner, manifest) = self.publish_owner(path).await?.separate();
        Ok(FollowerReceiver::new(
            owner,
            manifest.fence().history().lineage(),
            manifest.fence().hold_id(),
            Some(manifest),
        ))
    }
}
impl BootstrapReceiverJobs {
    /// Revalidates an existing follower under the receiver slot before reconnect.
    /// Expected identity and stable hold ID come from operator configuration;
    /// positions come exclusively from validated local durable state. Managed
    /// jobs recover the saved bootstrap manifest from their repository; offline
    /// callers supply it to recover an uncertain initial attachment.
    pub async fn reopen_follower(
        &self,
        path: PathBuf,
        inputs: StartupValidationInputs,
        lineage: ChangelogLineageV3,
        hold_id: HoldId,
        bootstrap: Option<Manifest>,
    ) -> Result<FollowerReceiver, StorageError> {
        let owner = Custody {
            value: (),
            repository: self.repository.clone(),
            permit: Arc::clone(&self.capacity)
                .try_acquire_owned()
                .map_err(|_| unavailable())?,
            cancellation: Arc::new(AtomicBool::new(false)),
            deadline: Instant::now() + LIFETIME,
        };
        let repository = owner.repository.clone();
        let owner = run_step(owner, move |(), cancellation| {
            if bootstrap.is_some_and(|manifest| {
                manifest.fence().history().lineage() != lineage
                    || manifest.fence().hold_id() != hold_id
            }) {
                return Err(corrupt());
            }
            let checked = crate::startup::open_redb_follower_startup_cancellable(
                &path,
                inputs,
                Arc::clone(cancellation),
            )
            .map_err(|error| match error {
                crate::startup::RedbStartupError::Storage(error) => error,
                _ => corrupt(),
            })?;
            let history = checked.applier.durable_history()?;
            let retained = repository
                .as_ref()
                .map(|repository| repository.attachment_manifest())
                .transpose()?
                .flatten();
            if retained.is_some_and(|retained| {
                retained.fence().history().lineage() != lineage
                    || retained.fence().hold_id() != hold_id
                    || bootstrap.is_some_and(|supplied| supplied != retained)
            }) {
                return Err(corrupt());
            }
            let bootstrap = retained.or(bootstrap);
            if history.lineage() != lineage {
                return Err(corrupt());
            }
            let attachment = match bootstrap {
                Some(manifest) if history.tail() == manifest.fence().history().tail() => {
                    Some(manifest)
                }
                Some(manifest)
                    if history.tail().sequence()
                        <= manifest.fence().history().tail().sequence() =>
                {
                    return Err(corrupt());
                }
                _ => None,
            };
            Ok((checked, attachment))
        })
        .await?;
        let (owner, attachment) = owner.separate();
        Ok(FollowerReceiver::new(owner, lineage, hold_id, attachment))
    }
}

/// Move-only continuous receiver. Every step retains at most one frame and
/// performs blocking storage work outside the async runtime. Cancellation,
/// malformed frames, storage errors and terminal refusals fuse this handle and
/// require validated local reopen. Transient peer unavailability retains the
/// existing durable owner for exact retry, while still returning the refusal.
/// Neither durable progress nor this owner grants application serving readiness.
pub struct FollowerReceiver {
    owner: Option<Custody<RedbFollowerApplier>>,
    stream: Option<TailStream>,
    lineage: ChangelogLineageV3,
    hold_id: HoldId,
    attachment: Option<Manifest>,
    report_pending: bool,
    projection: Option<FollowerProjectionTail>,
    reads: reads::Publication,
    startup: Option<crate::startup::FollowerStartupEvidence>,
    source_head: Option<riffdb_service::ReplicationSourceHead>,
    #[cfg(test)]
    received_barrier: Option<(
        tokio::sync::oneshot::Sender<Vec<u8>>,
        tokio::sync::oneshot::Receiver<()>,
    )>,
}
struct TailStream {
    source: Box<dyn ReplicationItemSource>,
    deadline: Instant,
    claim_pending: bool,
    frames: u8,
}
impl FollowerReceiver {
    /// Pins checked local authority for restricted promotion recovery. This
    /// neither connects nor publishes ordinary reads or projection workers.
    pub(crate) async fn prepare_promotion_retry(
        &mut self,
    ) -> Result<
        (
            riffdb_storage_api::ChangelogHistoryStateV3,
            riffdb_storage_api::ReplicationFollowerStateV3,
            riffdb_storage_redb::RedbOwnedSnapshot,
        ),
        StorageError,
    > {
        let mut owner = self.owner.take().ok_or_else(unavailable)?;
        owner.deadline = Instant::now() + LIFETIME;
        let returned = run_step(owner, |applier, cancellation| {
            check_cancel(cancellation)?;
            let captured = applier.capture_read_progress_snapshot()?;
            Ok((applier, captured))
        })
        .await?;
        let (owner, captured) = returned.separate();
        self.owner = Some(owner);
        Ok(captured)
    }

    fn new(
        owner: Custody<crate::startup::CheckedRedbFollowerStartup>,
        lineage: ChangelogLineageV3,
        hold_id: HoldId,
        attachment: Option<Manifest>,
    ) -> Self {
        let Custody {
            value,
            repository,
            permit,
            cancellation,
            deadline,
        } = owner;
        let (applier, startup) = value.into_parts();
        let owner = Custody {
            value: applier,
            repository,
            permit,
            cancellation,
            deadline,
        };
        Self {
            owner: Some(owner),
            startup: Some(startup),
            source_head: None,
            #[cfg(test)]
            received_barrier: None,
            stream: None,
            lineage,
            hold_id,
            attachment,
            report_pending: true,
            projection: Some(FollowerProjectionTail::default()),
            reads: reads::Publication::new(),
        }
    }

    /// Transfers the matching startup proof once, with a live read publication.
    pub(crate) async fn prepare_service(
        &mut self,
    ) -> Result<
        (
            crate::startup::FollowerStartupEvidence,
            FollowerReadSnapshots,
            riffdb_projection::ProjectionNotifier,
        ),
        StorageError,
    > {
        let reads = self.prepare_readers().await?;
        let startup = self.startup.take().ok_or_else(unavailable)?;
        let notifier = self.reads.projection_notifier()?;
        Ok((startup, reads, notifier))
    }

    /// Seeds a read-only publication from the fully validated startup owner.
    /// No application writer, mutable root, or readiness claim is released.
    pub async fn prepare_readers(&mut self) -> Result<FollowerReadSnapshots, StorageError> {
        let mut attempt = self.reads.attempt();
        let mut owner = self.owner.take().ok_or_else(unavailable)?;
        check_cancel(&owner.cancellation)?;
        owner.deadline = Instant::now() + LIFETIME;
        let mut projection = self.projection.take().ok_or_else(unavailable)?;
        let returned = run_step(owner, move |applier, _| {
            let view = projection.capture_read_view(&applier)?;
            Ok((applier, (projection, view)))
        })
        .await?;
        let (owner, (projection, view)) = returned.separate();
        self.reads.publish(view, &[])?;
        self.projection = Some(projection);
        self.owner = Some(owner);
        attempt.complete();
        Ok(self.reads.readers())
    }

    /// Applies and locally acknowledges one exact successor, or returns `None`
    /// at a bounded connection end. The next step reconnects from local durable
    /// state. The peer must own verified confidential transport and credentials.
    /// Reports are batched at 32 frames or EOF; control-only hold receipts never
    /// create a new report by themselves, preventing idle acknowledgement churn.
    pub async fn advance(
        &mut self,
        peer: &dyn ReplicationSourcePort,
    ) -> Result<Option<Point>, Failure> {
        self.advance_until_stopped(peer, &mut worker::StopSignal::unobserved())
            .await
    }

    async fn advance_until_stopped(
        &mut self,
        peer: &dyn ReplicationSourcePort,
        stop: &mut worker::StopSignal,
    ) -> Result<Option<Point>, Failure> {
        let mut publication_attempt = self.reads.attempt();
        // Ownership leaves self before every possible wait. A cancelled caller
        // cannot regain an applier whose blocking operation may still be running.
        let mut owner = self.owner.take().ok_or_else(busy)?;
        let stream = self.stream.take();
        check_cancel(&owner.cancellation).map_err(storage)?;
        owner.deadline = Instant::now() + LIFETIME;
        let mut stream = match stream {
            Some(stream) => stream,
            None => {
                let lineage = self.lineage;
                let attachment = self.attachment;
                let returned = run_step(owner, move |mut applier, flag| {
                    let history = applier.durable_history()?;
                    if history.lineage() != lineage
                        || attachment.is_some_and(|manifest| {
                            history.tail() != manifest.fence().history().tail()
                        })
                    {
                        return Err(corrupt());
                    }
                    let position = applier.resume_stream()?;
                    check_cancel(flag)?;
                    if applier.acknowledge_durable_position()? != position {
                        return Err(corrupt());
                    }
                    Ok((applier, position))
                })
                .await
                .map_err(storage)?;
                let (returned, position) = returned.separate();
                owner = returned;
                let phase = match self.attachment {
                    Some(manifest) => ReplicationPhase::Attach {
                        manifest: manifest.encode().map_err(|_| storage(corrupt()))?,
                    },
                    None if self.report_pending => ReplicationPhase::Follower {
                        hold_id: *self.hold_id.as_bytes(),
                    },
                    None => ReplicationPhase::Tail,
                };
                let request = ReplicationRequest {
                    database_id: lineage.database_id(),
                    history_incarnation: lineage.history_incarnation(),
                    leadership_epoch: lineage.leadership_epoch().get(),
                    phase,
                    after_sequence: position.sequence().get(),
                    after_hash: position.history_hash(),
                    after_frontier: position.frontier(),
                    readable_format: ChangelogFrameV3::IDENTITY.to_owned(),
                    catalog_digest: lineage.catalog_digest(),
                    maximum_frame_bytes: MAX_CHANGELOG_FRAME_BYTES as u64,
                    maximum_transitions: MAX_STAGED_COMMANDS as u64,
                };
                let deadline = owner.deadline;
                let source = match stop
                    .network(async { tokio::time::timeout_at(deadline, peer.open(request)).await })
                    .await
                {
                    Some(Ok(Ok(source))) => source,
                    Some(Ok(Err(error))) => {
                        if transient(error) {
                            self.owner = Some(owner);
                            publication_attempt.complete();
                        }
                        return Err(error);
                    }
                    None | Some(Err(_)) => {
                        self.owner = Some(owner);
                        publication_attempt.complete();
                        return Ok(None);
                    }
                };
                TailStream {
                    source,
                    deadline,
                    claim_pending: self.report_pending,
                    frames: 0,
                }
            }
        };
        let result = if Instant::now() >= stream.deadline {
            None
        } else {
            stop.network(async {
                tokio::time::timeout_at(stream.deadline, stream.source.next_item()).await
            })
            .await
            .and_then(Result::ok)
        };
        let Some(result) = result else {
            // No storage work has started for this receive. Expiry closes only
            // the network stream; an uncertain claim remains pending for retry.
            self.owner = Some(owner);
            publication_attempt.complete();
            return Ok(None);
        };
        let item = match result {
            Ok(item) => item,
            Err(error) => {
                // A failed network read has performed no local apply. Keep
                // the exact durable writer only for the closed transient class.
                if transient(error) {
                    self.owner = Some(owner);
                    publication_attempt.complete();
                }
                return Err(error);
            }
        };
        let Some(ReplicationItem::Frame(bytes)) = item else {
            if item.is_some() {
                return Err(storage(corrupt()));
            }
            if stream.claim_pending {
                owner = retire_scratch(owner).await.map_err(storage)?;
                self.attachment = None;
                self.report_pending = false;
            }
            self.owner = Some(owner);
            publication_attempt.complete();
            return Ok(None);
        };
        if bytes.is_empty() || bytes.len() > MAX_CHANGELOG_FRAME_BYTES {
            return Err(storage(corrupt()));
        }
        #[cfg(test)]
        if let Some((received, release)) = self.received_barrier.take() {
            let _ = received.send(bytes.to_vec());
            release.await.map_err(|_| busy())?;
        }
        // A frame received at the connection boundary gets its own bounded
        // storage step, without extending that connection's network lifetime.
        owner.deadline = Instant::now() + LIFETIME;
        let mut projection = self.projection.take().ok_or_else(busy)?;
        let source_head = bytes.source_head();
        let previous_head = self.source_head;
        let returned = run_step(owner, move |mut applier, cancellation| {
            let frame = ChangelogFrameV3::decode(&bytes).map_err(|_| corrupt())?;
            let covered = Point::from_receipt(frame.receipts().last().ok_or_else(corrupt)?)
                .map_err(|_| corrupt())?;
            if covered.sequence() <= applier.durable_position()?.sequence() {
                return Err(corrupt());
            }
            validate_source_head(source_head, previous_head, covered)?;
            let meaningful = frame.receipts().iter().any(|receipt| {
                receipt.attribution() != ChangelogAttributionV3::ReplicationSourceHold
            });
            let changes = projection.plan(&frame)?;
            drop(frame);
            check_cancel(cancellation)?;
            let applied = applier.apply_frame(&bytes)?;
            check_cancel(cancellation)?;
            let changed = projection.replay(&mut applier, changes, cancellation)?;
            if applied != covered || applier.acknowledge_durable_position()? != applied {
                return Err(corrupt());
            }
            let mut view = projection.capture_read_view(&applier)?;
            view.source_head = source_head;
            Ok((applier, (applied, meaningful, projection, view, changed)))
        })
        .await
        .map_err(storage)?;
        let (mut owner, (applied, meaningful, projection, view, changed)) = returned.separate();
        self.projection = Some(projection);
        if stream.claim_pending {
            owner = retire_scratch(owner).await.map_err(storage)?;
            self.attachment = None;
            self.report_pending = false;
            stream.claim_pending = false;
        }
        self.report_pending |= meaningful;
        stream.frames = stream.frames.saturating_add(1).min(REPORT_FRAMES);
        if !self.report_pending || stream.frames < REPORT_FRAMES {
            self.stream = Some(stream);
        }
        self.reads.publish(view, &changed).map_err(storage)?;
        self.source_head = source_head;
        self.owner = Some(owner);
        publication_attempt.complete();
        Ok(Some(applied))
    }

    /// Drains the owned applier outside the async runtime, without writing the
    /// source CLEAN lifecycle. Cancellation retains capacity until close ends.
    pub async fn close(mut self) -> Result<(), StorageError> {
        self.reads.withdraw();
        let mut owner = self.owner.take().ok_or_else(unavailable)?;
        self.stream.take();
        owner.deadline = Instant::now() + LIFETIME;
        run_step(owner, |applier, _| applier.close()).await?;
        Ok(())
    }
}
fn validate_source_head(
    head: Option<riffdb_service::ReplicationSourceHead>,
    previous: Option<riffdb_service::ReplicationSourceHead>,
    covered: Point,
) -> Result<(), StorageError> {
    let Some(head) = head else {
        return Ok(());
    };
    let covers = |frontier: riffdb_types::DualFrontier, prior| {
        frontier == prior || frontier.advances_from(prior)
    };
    if head.transaction_sequence() < covered.sequence().get()
        || !covers(head.frontier(), covered.frontier())
        || (head.transaction_sequence() == covered.sequence().get()
            && head.frontier() != covered.frontier())
        || previous.is_some_and(|prior| {
            head.transaction_sequence() < prior.transaction_sequence()
                || !covers(head.frontier(), prior.frontier())
                || (head.transaction_sequence() == prior.transaction_sequence()
                    && head.frontier() != prior.frontier())
        })
    {
        return Err(corrupt());
    }
    Ok(())
}

async fn retire_scratch(
    mut owner: Custody<RedbFollowerApplier>,
) -> Result<Custody<RedbFollowerApplier>, StorageError> {
    let Some(repository) = owner.repository.clone() else {
        return Ok(owner);
    };
    owner.deadline = Instant::now() + LIFETIME;
    run_step(owner, move |mut applier, cancellation| {
        check_cancel(cancellation)?;
        applier.retire_bootstrap_scratch(&repository)?;
        Ok(applier)
    })
    .await
}
impl std::fmt::Debug for FollowerReceiver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FollowerReceiver([redacted])")
    }
}
fn corrupt() -> StorageError {
    StorageError::new(StorageErrorKind::CorruptData, None)
}
fn storage(error: StorageError) -> Failure {
    Failure::Source(riffdb_storage_api::ChangelogCursorErrorV3::Storage(error).into())
}
fn transient(error: Failure) -> bool {
    matches!(
        error,
        Failure::Unavailable
            | Failure::Source(riffdb_service::ReplicationStreamErrorV3::Unavailable)
    )
}
fn busy() -> Failure {
    Failure::Unavailable
}

#[cfg(test)]
#[path = "replication_follower_receiver_tests.rs"]
mod tests;
