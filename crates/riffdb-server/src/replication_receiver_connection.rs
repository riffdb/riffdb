//! One bounded connection owns receiver capacity and an exact durable transfer.
use super::*;
use riffdb_service::{
    ReplicationFailure as Failure, ReplicationItem as Item, ReplicationItemSource,
    ReplicationPhase, ReplicationRequest, ReplicationSourcePort,
};
use riffdb_storage_api::{
    ChangelogCursorErrorV3, ChangelogLineageV3, LeadershipEpochV1, ReplicationHandshakeV3,
};
use riffdb_types::DualFrontier;

impl BootstrapReceiverJobs {
    /// Receives a manifest under one reserved construction slot. The peer port
    /// must already own confidential transport and its administrative credential.
    /// Only server-configured paths and expected source identity belong here.
    /// Resume derives its manifest and ordinal from local durable progress.
    pub async fn connect(
        &self,
        peer: &dyn ReplicationSourcePort,
        path: PathBuf,
        request: ReplicationRequest,
        resume: bool,
    ) -> Result<BootstrapReceiverConnection, Failure> {
        if self.repository.is_some() {
            return Err(busy());
        }
        self.connect_inner(peer, Some(path), request, resume).await
    }

    /// Recovers the fixed managed transfer before contacting the peer. Caller
    /// supplies only expected source identity and its stable bootstrap hold ID.
    pub async fn connect_managed(
        &self,
        peer: &dyn ReplicationSourcePort,
        request: ReplicationRequest,
    ) -> Result<BootstrapReceiverConnection, Failure> {
        if self.repository.is_none() {
            return Err(busy());
        }
        self.connect_inner(peer, None, request, true).await
    }

    async fn connect_inner(
        &self,
        peer: &dyn ReplicationSourcePort,
        path: Option<PathBuf>,
        mut request: ReplicationRequest,
        resume: bool,
    ) -> Result<BootstrapReceiverConnection, Failure> {
        let ReplicationPhase::Bootstrap {
            hold_id,
            resume_manifest,
            after_page,
        } = &request.phase
        else {
            return Err(invalid());
        };
        if *hold_id == [0; 16]
            || !resume_manifest.is_empty()
            || *after_page != 0
            || request.after_sequence != 0
            || request.after_hash != [0; 32]
            || request.after_frontier != DualFrontier::INITIAL
        {
            return Err(invalid());
        }
        let owner = Custody {
            value: (),
            repository: self.repository.clone(),
            permit: Arc::clone(&self.capacity)
                .try_acquire_owned()
                .map_err(|_| busy())?,
            cancellation: Arc::new(AtomicBool::new(false)),
            deadline: Instant::now() + LIFETIME,
        };
        let deadline = owner.deadline;
        let recovery_path = path.clone();
        let repository = self.repository.clone();
        let owner = run_step(owner, move |(), _| match (repository, recovery_path) {
            (Some(repository), None) => repository.recover_transfer(),
            (None, Some(path)) if resume => RedbBootstrapStage::recover(&path).map(Some),
            (None, Some(_)) => Ok(None),
            _ => Err(unavailable()),
        })
        .await
        .map_err(storage)?;
        let expected = owner
            .value
            .as_ref()
            .map(|stage| stage.progress().manifest());
        if let Some(manifest) = expected {
            validate_manifest(&request, manifest)?;
            request.phase = ReplicationPhase::Bootstrap {
                hold_id: *manifest.fence().hold_id().as_bytes(),
                resume_manifest: manifest.encode().map_err(|_| corrupt())?,
                after_page: owner
                    .value
                    .as_ref()
                    .ok_or_else(busy)?
                    .progress()
                    .page_count(),
            };
        }
        let mut stream = tokio::time::timeout_at(deadline, peer.open(request.clone()))
            .await
            .map_err(|_| busy())??;
        let first = tokio::time::timeout_at(deadline, stream.next_item())
            .await
            .map_err(|_| busy())??;
        let Some(Item::BootstrapManifest(bytes)) = first else {
            return Err(corrupt());
        };
        if bytes.is_empty() || bytes.len() > 512 {
            return Err(corrupt());
        }
        let manifest = Manifest::decode(&bytes).map_err(|_| corrupt())?;
        validate_manifest(&request, manifest)?;
        if expected.is_some_and(|expected| expected != manifest) {
            return Err(corrupt());
        }
        let repository = self.repository.clone();
        let owner = run_step(owner, move |stage, _| match (stage, repository, path) {
            (Some(stage), _, _) => Ok(stage),
            (None, Some(repository), None) => repository.begin_transfer(manifest),
            (None, None, Some(path)) => RedbBootstrapStage::create(&path, manifest),
            _ => Err(unavailable()),
        })
        .await
        .map_err(storage)?;
        Ok(BootstrapReceiverConnection {
            transfer: Some(BootstrapReceiverTransferJob { owner: Some(owner) }),
            stream: Some(stream),
            deadline,
            eof: false,
        })
    }
}

/// Move-only network/transfer custody. Losing any receive wait requires durable
/// resume. A complete page count still cannot bypass the source's exact EOF.
pub struct BootstrapReceiverConnection {
    transfer: Option<BootstrapReceiverTransferJob>,
    stream: Option<Box<dyn ReplicationItemSource>>,
    deadline: Instant,
    eof: bool,
}
impl BootstrapReceiverConnection {
    /// Locally durable page progress; never an attachment acknowledgement.
    pub fn progress(&self) -> Result<Progress, Failure> {
        self.transfer
            .as_ref()
            .ok_or_else(busy)?
            .progress()
            .map_err(storage)
    }
    /// Receives and persists one page, or proves complete source EOF. No page is
    /// buffered across a second receive. Failure or cancellation fuses custody.
    pub async fn receive_next(&mut self) -> Result<bool, Failure> {
        let mut transfer = self.transfer.take().ok_or_else(busy)?;
        if Instant::now() >= self.deadline {
            return Err(busy());
        }
        if self.eof {
            self.transfer = Some(transfer);
            return Ok(true);
        }
        let mut stream = self.stream.take().ok_or_else(busy)?;
        let progress = transfer.progress().map_err(storage)?;
        let next = tokio::time::timeout_at(self.deadline, stream.next_item())
            .await
            .map_err(|_| busy())??;
        if Instant::now() >= self.deadline {
            return Err(busy());
        }
        match next {
            Some(Item::BootstrapPage(bytes)) => {
                if progress.page_count() >= progress.manifest().page_count() {
                    return Err(corrupt());
                }
                let advanced = transfer.append(bytes).await.map_err(storage)?;
                if advanced.page_count() != progress.page_count() + 1 {
                    return Err(corrupt());
                }
            }
            None if progress.page_count() == progress.manifest().page_count() => self.eof = true,
            _ => return Err(corrupt()),
        }
        if !self.eof {
            self.stream = Some(stream);
        }
        self.transfer = Some(transfer);
        Ok(self.eof)
    }
    /// Releases a complete transfer to the existing offline materializer. This
    /// provides neither follower readiness nor source attachment authority.
    pub fn finish(mut self) -> Result<BootstrapReceiverTransferJob, Failure> {
        if !self.eof || Instant::now() >= self.deadline {
            return Err(busy());
        }
        self.transfer.take().ok_or_else(busy)
    }
}
fn validate_manifest(request: &ReplicationRequest, manifest: Manifest) -> Result<(), Failure> {
    use riffdb_errors::ReplicationStreamErrorV3 as R;
    let history = manifest.fence().history();
    let lineage = ChangelogLineageV3::new(
        request.database_id,
        request.history_incarnation,
        LeadershipEpochV1::new(request.leadership_epoch).ok_or_else(invalid)?,
    )
    .map_err(|_| invalid())?;
    ReplicationHandshakeV3::new(
        lineage,
        history.tail(),
        &request.readable_format,
        request.catalog_digest,
        request.maximum_frame_bytes,
        request.maximum_transitions,
    )
    .map_err(Failure::Source)?;
    if history.lineage().database_id() != lineage.database_id()
        || history.lineage().history_incarnation() != lineage.history_incarnation()
    {
        return Err(Failure::Source(R::ForeignLineage));
    }
    if history.lineage().leadership_epoch() != lineage.leadership_epoch() {
        return Err(Failure::Source(R::StaleEpoch));
    }
    if !matches!(request.phase, ReplicationPhase::Bootstrap { hold_id, .. }
        if &hold_id == manifest.fence().hold_id().as_bytes())
    {
        return Err(invalid());
    }
    Ok(())
}
fn storage(error: StorageError) -> Failure {
    Failure::Source(ChangelogCursorErrorV3::from(error).into())
}
fn busy() -> Failure {
    Failure::Unavailable
}
fn corrupt() -> Failure {
    Failure::Source(riffdb_errors::ReplicationStreamErrorV3::CorruptHistory)
}
fn invalid() -> Failure {
    Failure::Source(riffdb_errors::ReplicationStreamErrorV3::InvalidPosition)
}
