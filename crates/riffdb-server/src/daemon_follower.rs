//! One follower generation under the shared per-database daemon supervisor.
use super::*;
use crate::config::FollowerSourceConfig;
use crate::replication_bootstrap::{
    BootstrapReceiverJobs, FollowerReceiver, RunningFollowerReceiver,
};
use riffdb_service::{
    ReplicationFailure, ReplicationPhase, ReplicationRequest, ReplicationSourcePort,
};
use riffdb_storage_api::{ChangelogFrameV3, MAX_CHANGELOG_FRAME_BYTES, MAX_STAGED_COMMANDS};
use riffdb_storage_redb::RedbBootstrapReceiverRepository;
use riffdb_types::DualFrontier;

/// A follower generation belongs to the same per-database daemon owner as a source.
/// Transport admission closes before this value drains the applier and readers.
pub(super) struct RunningFollowerGeneration {
    worker: RunningFollowerReceiver,
    service: crate::process_graph::RunningFollowerService,
}
impl RunningFollowerGeneration {
    pub(super) fn is_available(&self) -> bool {
        self.service.is_available()
    }
    pub(super) fn hosted_mcp_dependencies(
        &self,
    ) -> Option<crate::process_graph::HostedMcpDependencies> {
        self.service.hosted_mcp_dependencies()
    }
    pub(super) async fn shutdown(self) -> Result<(), DaemonError> {
        let worker = self
            .worker
            .shutdown()
            .await
            .map_err(DaemonError::Replication);
        let service = self
            .service
            .shutdown()
            .await
            .map_err(|error| DaemonError::Replication(storage(error)));
        worker.and(service)
    }
}

/// Called only after local promotion and maintenance evidence has been checked.
/// None means a process shutdown signal was received before activation.
#[allow(clippy::too_many_arguments)]
pub(super) async fn start(
    config: &ServerConfig,
    database: &DatabaseConfig,
    inputs: StartupValidationInputs,
    keys: ProductionDigestKeys,
    clocks: &ProductionWallClocks,
    activator: riffdb_service::RiffDbServiceActivator,
    lifecycle: Arc<ProductionLifecycleRoute>,
    signal: &mut ProductionShutdownSignal,
    promotion: Arc<crate::promotion_admission::PromotionController>,
) -> Result<Option<RunningFollowerGeneration>, DaemonError> {
    let source = database.follower().ok_or(DaemonError::Config(
        ServerConfigError::InvalidFollowerConfiguration,
    ))?;
    let peer = tokio::select! {
        biased;
        result = signal.received() => {
            result.map_err(DaemonError::ShutdownSignal)?;
            return Ok(None);
        },
        result = connect(source.clone()) => result.map_err(DaemonError::Replication)?,
    };
    let root = database.replication_receiver_root();
    let repository =
        tokio::task::spawn_blocking(move || RedbBootstrapReceiverRepository::open(&root))
            .await
            .map_err(|_| DaemonError::Replication(ReplicationFailure::Unavailable))?
            .map_err(|error| DaemonError::Replication(storage(error)))?;
    let jobs = BootstrapReceiverJobs::from_repository(repository.clone());
    let opening = open_managed(
        &jobs,
        repository,
        peer.as_ref(),
        database.database_path().to_path_buf(),
        inputs,
        source,
    );
    let opened = tokio::select! {
        biased;
        result = signal.received() => result.map(|()| None).map_err(DaemonError::ShutdownSignal),
        result = opening => result.map(Some).map_err(DaemonError::Replication),
    };
    let mut receiver = match opened {
        Ok(Some(receiver)) => receiver,
        Ok(None) => {
            jobs.drain()
                .await
                .map_err(|error| DaemonError::Replication(storage(error)))?;
            return Ok(None);
        }
        Err(error) => {
            jobs.drain()
                .await
                .map_err(|error| DaemonError::Replication(storage(error)))?;
            return Err(error);
        }
    };
    let prepared = receiver.prepare_service().await;
    let (evidence, reads, notifier) = match prepared {
        Ok(value) => value,
        Err(error) => {
            receiver
                .close()
                .await
                .map_err(|error| DaemonError::Replication(storage(error)))?;
            return Err(DaemonError::Replication(storage(error)));
        }
    };
    let build = build_info_for_format(evidence.retained_metadata.storage_format_version().get())?;
    let process = riffdb_service::ServiceProcessMetadata::new(
        clocks.process_time().map_err(DaemonError::ProcessClock)?,
        build,
    );
    let routing = lifecycle.runtime_routing();
    let service = crate::process_graph::RunningFollowerService::start(
        evidence,
        reads,
        notifier,
        database.projections_root(),
        database.projections(),
        activator,
        keys,
        config.audience().clone(),
        config.mcp_audience().cloned(),
        database.environment().clone(),
        process,
        clocks,
        lifecycle,
        Some(promotion),
    );
    let service = match service {
        Ok(service) => service,
        Err(error) => {
            receiver
                .close()
                .await
                .map_err(|error| DaemonError::Replication(storage(error)))?;
            return Err(DaemonError::GraphBuild(error));
        }
    };
    match RunningFollowerReceiver::start_with_routing(receiver, peer, routing) {
        Ok(worker) => Ok(Some(RunningFollowerGeneration { worker, service })),
        Err(error) => {
            service
                .shutdown()
                .await
                .map_err(|error| DaemonError::Replication(storage(error)))?;
            Err(DaemonError::Replication(error))
        }
    }
}
pub(super) async fn connect(
    source: FollowerSourceConfig,
) -> Result<Arc<replication_peer::VerifiedReplicationPeer>, ReplicationFailure> {
    let credential = tokio::task::spawn_blocking(move || {
        riffdb_auth::load_capability_token_file(source.credential.as_path())
    })
    .await
    .map_err(|_| ReplicationFailure::Unavailable)?
    .map_err(|_| ReplicationFailure::Unavailable)?;
    Ok(Arc::new(
        replication_peer::VerifiedReplicationPeer::connect(source.tls, credential, source.database)
            .await?,
    ))
}

pub(super) async fn open_managed(
    jobs: &BootstrapReceiverJobs,
    repository: RedbBootstrapReceiverRepository,
    peer: &dyn ReplicationSourcePort,
    path: PathBuf,
    inputs: StartupValidationInputs,
    source: &FollowerSourceConfig,
) -> Result<FollowerReceiver, ReplicationFailure> {
    let probe_path = path.clone();
    let exists = tokio::task::spawn_blocking(move || match fs::symlink_metadata(probe_path) {
        Ok(metadata) if metadata.is_file() => Ok(true),
        Ok(_) => Err(ReplicationFailure::Source(
            riffdb_service::ReplicationStreamErrorV3::CorruptHistory,
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(_) => Err(ReplicationFailure::Unavailable),
    })
    .await
    .map_err(|_| ReplicationFailure::Unavailable)??;
    if exists {
        match jobs
            .reopen_follower(
                path.clone(),
                inputs.clone(),
                source.lineage,
                source.hold,
                None,
            )
            .await
        {
            Ok(receiver) => return Ok(receiver),
            Err(error) => {
                jobs.drain().await.map_err(storage)?;
                // Only a retained complete transfer may attempt the existing
                // candidate-publication recovery. The publisher still checks its
                // exact fence, inode bindings and non-regressing follower target.
                let recoverable = tokio::task::spawn_blocking(move || {
                    repository.recover_transfer().map(|stage| {
                        stage.is_some_and(|stage| {
                            stage.progress().page_count()
                                == stage.progress().manifest().page_count()
                        })
                    })
                })
                .await
                .map_err(|_| ReplicationFailure::Unavailable)?
                .map_err(storage)?;
                if !recoverable {
                    return Err(storage(error));
                }
            }
        }
    }
    let request = ReplicationRequest {
        database_id: source.lineage.database_id(),
        history_incarnation: source.lineage.history_incarnation(),
        leadership_epoch: source.lineage.leadership_epoch().get(),
        phase: ReplicationPhase::Bootstrap {
            hold_id: *source.hold.as_bytes(),
            resume_manifest: vec![],
            after_page: 0,
        },
        after_sequence: 0,
        after_hash: [0; 32],
        after_frontier: DualFrontier::INITIAL,
        readable_format: ChangelogFrameV3::IDENTITY.to_owned(),
        catalog_digest: source.lineage.catalog_digest(),
        maximum_frame_bytes: MAX_CHANGELOG_FRAME_BYTES as u64,
        maximum_transitions: MAX_STAGED_COMMANDS as u64,
    };
    let mut transfer = jobs.connect_managed(peer, request).await?;
    while !transfer.receive_next().await? {}
    let mut build = transfer
        .finish()?
        .materialize_managed(inputs)
        .await
        .map_err(storage)?;
    while !build.advance().await.map_err(storage)? {}
    build.publish_and_follow(path).await.map_err(storage)
}
fn storage(error: StorageError) -> ReplicationFailure {
    ReplicationFailure::Source(riffdb_storage_api::ChangelogCursorErrorV3::from(error).into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication_bootstrap::BootstrapSourceJobs;
    use crate::replication_source::PublishedReplicationSource;
    use riffdb_storage_api::{ReadableDigestKey, ReplicationSourceHoldIdV1};
    use riffdb_types::{DigestKeyId, Timestamp};
    fn inputs() -> StartupValidationInputs {
        let key = ReadableDigestKey::v1(DigestKeyId::new(1).unwrap());
        StartupValidationInputs::new(
            Timestamp::new(1000, 0).unwrap(),
            ReadableCapabilityDigestInventory::new(vec![key]).unwrap(),
            ReadableIdempotencyDigestInventory::new(vec![key]).unwrap(),
        )
    }
    fn source_config(lineage: riffdb_storage_api::ChangelogLineageV3) -> FollowerSourceConfig {
        use riffdb_config::*;
        FollowerSourceConfig {
            tls: TlsClientConfig::new(
                CanonicalHttpsEndpoint::parse("https://primary.example:7443").unwrap(),
                ProtectedFilePath::new(PathBuf::from("/etc/riffdb/ca.pem")).unwrap(),
                TlsServerIdentity::parse("primary.example").unwrap(),
                Duration::from_secs(5),
                Duration::from_secs(30),
                NonZeroU32::MIN,
                NonZeroU32::MIN,
            )
            .unwrap(),
            credential: ProtectedFilePath::new(PathBuf::from("/etc/riffdb/token")).unwrap(),
            database: DatabaseAlias::default_alias(),
            lineage,
            hold: ReplicationSourceHoldIdV1::new([0x79; 16]).unwrap(),
        }
    }
    struct Peer {
        source: PublishedReplicationSource,
        opens: std::sync::atomic::AtomicUsize,
    }
    impl ReplicationSourcePort for Peer {
        fn open(
            &self,
            request: ReplicationRequest,
        ) -> riffdb_service::ReplicationFuture<'_, Box<dyn riffdb_service::ReplicationItemSource>>
        {
            self.opens.fetch_add(1, Ordering::SeqCst);
            self.source.open(request)
        }
    }
    // req: REP-002, REP-003, REC-001
    #[tokio::test]
    async fn daemon_managed_start_bootstraps_then_reopens_without_new_source_construction() {
        let scope = tempfile::tempdir().unwrap();
        let source_path = scope.path().join("primary.redb");
        let mut startup = open_redb_startup_with_commit_profile(
            &source_path,
            inputs(),
            &ProductionIdentifierSources::new().database_ids(),
            riffdb_storage_redb::RedbCommitProfile::Standard,
        )
        .unwrap();
        let publications = startup.take_replication_publications().unwrap();
        let (_, _, _, _, _, ports) = startup.into_parts();
        let history = ports
            .published_changelog_snapshot_v3()
            .unwrap()
            .authoritative_state_v3()
            .unwrap()
            .history();
        let config = source_config(history.lineage());
        let peer = Peer {
            source: PublishedReplicationSource::new(
                publications,
                BootstrapSourceJobs::from_repository(
                    ports
                        .bootstrap_repository(&scope.path().join("source-artifacts"))
                        .unwrap(),
                ),
            ),
            opens: 0.into(),
        };
        let repository =
            RedbBootstrapReceiverRepository::open(&scope.path().join("receiver")).unwrap();
        let jobs = BootstrapReceiverJobs::from_repository(repository.clone());
        let path = scope.path().join("follower.redb");
        let receiver = open_managed(
            &jobs,
            repository.clone(),
            &peer,
            path.clone(),
            inputs(),
            &config,
        )
        .await
        .unwrap();
        receiver.close().await.unwrap();
        assert_eq!(peer.opens.load(Ordering::SeqCst), 1);
        let receiver = open_managed(&jobs, repository, &peer, path.clone(), inputs(), &config)
            .await
            .unwrap();
        assert_eq!(
            peer.opens.load(Ordering::SeqCst),
            1,
            "validated reopen never creates a fresh source hold"
        );
        receiver.close().await.unwrap();
        jobs.drain().await.unwrap();
        let checked = crate::startup::open_redb_follower_startup(&path, inputs()).unwrap();
        assert_eq!(checked.database_id, history.lineage().database_id());
    }
}
