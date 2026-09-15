//! Follower process startup and supervision, isolated from primary maintenance.
use super::*;
use crate::config::FollowerSourceConfig;
use crate::replication_bootstrap::{
    BootstrapReceiverJobs, FollowerReceiver, RunningFollowerReceiver,
};
use riffdb_service::{
    ReplicationFailure, ReplicationPhase, ReplicationRequest, ReplicationSourcePort,
};
use riffdb_storage_api::{
    AuthoritativeStateCatalogV1, ChangelogFrameV3, MAX_CHANGELOG_FRAME_BYTES, MAX_STAGED_COMMANDS,
};
use riffdb_storage_redb::RedbBootstrapReceiverRepository;
use riffdb_types::DualFrontier;

pub(super) async fn run(
    config: ServerConfig,
    signal: &mut ProductionShutdownSignal,
) -> Result<(), DaemonError> {
    let clocks = ProductionWallClocks::new();
    let keys = load_production_digest_keys(&config)?;
    let inputs = startup_validation_inputs(
        &keys,
        clocks
            .authorization()
            .now()
            .map_err(DaemonError::StartupClock)?,
    )?;
    let mut lifecycles = Vec::with_capacity(config.databases().len());
    let mut activators = Vec::with_capacity(config.databases().len());
    let mut routes = Vec::with_capacity(config.databases().len());
    for database in config.databases() {
        let (initializing, activator, issuer) = RiffDbService::begin_initialization();
        let lifecycle = Arc::new(ProductionLifecycleRoute::new(
            initializing,
            issuer,
            RuntimeRoutingState::new(),
        ));
        let route: Arc<dyn GrpcLifecycleRoute> = lifecycle.clone();
        routes.push((database.alias().clone(), route));
        lifecycles.push(lifecycle);
        activators.push(activator);
    }
    let routes =
        Arc::new(GrpcDatabaseRoutes::new(routes).map_err(|_| DaemonError::GrpcConfiguration)?);
    let application = GrpcApplication::with_database_routes_and_audience(
        routes,
        GrpcRequestLimits::new(REQUEST_DURATION_LIMIT)
            .map_err(|_| DaemonError::GrpcConfiguration)?,
        config.audience().clone(),
    );
    let mut transport = HostedGrpc::bind(config.application_listener().clone(), &application)?;
    drop(application);
    let mut workers = Vec::with_capacity(config.databases().len());
    let mut services = Vec::with_capacity(config.databases().len());
    let mut hosted_mcp = None;
    let result = start_and_supervise(
        &config,
        inputs,
        keys,
        clocks,
        activators,
        &lifecycles,
        signal,
        &mut transport,
        &mut workers,
        &mut services,
        &mut hosted_mcp,
    )
    .await;
    for lifecycle in lifecycles {
        lifecycle.stop();
    }
    let transport_result = if matches!(result, Err(DaemonError::TransportEnded)) {
        Ok(())
    } else {
        transport.drain_after_signal().await
    };
    let mcp_result = if matches!(
        result,
        Err(DaemonError::McpTransportEnded | DaemonError::McpStop(_))
    ) {
        Ok(())
    } else {
        match &mut hosted_mcp {
            Some(hosted) => hosted
                .drain_after_signal()
                .await
                .map_err(DaemonError::McpStop),
            None => Ok(()),
        }
    };
    let mut drained = Ok(());
    for worker in workers {
        if let Err(error) = worker.shutdown().await {
            drained = Err(DaemonError::Replication(error));
        }
    }
    for service in services {
        if let Err(error) = service.shutdown().await {
            drained = Err(DaemonError::Replication(storage(error)));
        }
    }
    result.and(transport_result).and(mcp_result).and(drained)
}

#[allow(clippy::too_many_arguments)]
async fn start_and_supervise(
    config: &ServerConfig,
    inputs: StartupValidationInputs,
    keys: ProductionDigestKeys,
    clocks: ProductionWallClocks,
    activators: Vec<riffdb_service::RiffDbServiceActivator>,
    lifecycles: &[Arc<ProductionLifecycleRoute>],
    signal: &mut ProductionShutdownSignal,
    transport: &mut HostedGrpc,
    workers: &mut Vec<RunningFollowerReceiver>,
    services: &mut Vec<crate::process_graph::RunningFollowerService>,
    hosted_mcp: &mut Option<HostedMcp>,
) -> Result<(), DaemonError> {
    // Every peer binding and credential is checked before any local construction.
    let mut peers = Vec::with_capacity(config.databases().len());
    for database in config.databases() {
        let source = database
            .follower()
            .ok_or(DaemonError::Config(
                ServerConfigError::InvalidFollowerConfiguration,
            ))?
            .clone();
        let connecting = connect(source);
        let peer = tokio::select! {
            biased;
            result = signal.received() => { result.map_err(DaemonError::ShutdownSignal)?; return Ok(()); },
            result = connecting => result.map_err(DaemonError::Replication)?,
        };
        peers.push(peer);
    }
    for (((database, peer), activator), lifecycle) in config
        .databases()
        .iter()
        .zip(peers)
        .zip(activators)
        .zip(lifecycles)
    {
        let root = database.replication_receiver_root();
        let repository =
            tokio::task::spawn_blocking(move || RedbBootstrapReceiverRepository::open(&root))
                .await
                .map_err(|_| DaemonError::Replication(ReplicationFailure::Unavailable))?
                .map_err(|error| DaemonError::Replication(storage(error)))?;
        let jobs = BootstrapReceiverJobs::from_repository(repository.clone());
        let source = database.follower().ok_or(DaemonError::Config(
            ServerConfigError::InvalidFollowerConfiguration,
        ))?;
        let opening = open_managed(
            &jobs,
            repository,
            peer.as_ref(),
            database.database_path().to_path_buf(),
            inputs.clone(),
            source,
        );
        let opened = tokio::select! {
            biased;
            result = signal.received() => { result.map_err(DaemonError::ShutdownSignal)?; None },
            result = opening => Some(result),
        };
        let Some(opened) = opened else {
            jobs.drain()
                .await
                .map_err(|error| DaemonError::Replication(storage(error)))?;
            return Ok(());
        };
        match opened {
            Ok(mut receiver) => {
                let (evidence, reads, notifier) = receiver
                    .prepare_service()
                    .await
                    .map_err(|error| DaemonError::Replication(storage(error)))?;
                let build = build_info_for_format(
                    evidence.retained_metadata.storage_format_version().get(),
                )?;
                let process = riffdb_service::ServiceProcessMetadata::new(
                    clocks.process_time().map_err(DaemonError::ProcessClock)?,
                    build,
                );
                let service = crate::process_graph::RunningFollowerService::start(
                    evidence,
                    reads,
                    notifier,
                    database.projections_root(),
                    activator,
                    keys.clone(),
                    config.audience().clone(),
                    config.mcp_audience().cloned(),
                    database.environment().clone(),
                    process,
                    &clocks,
                    lifecycle.clone(),
                );
                match service {
                    Ok(service) => services.push(service),
                    Err(error) => {
                        receiver
                            .close()
                            .await
                            .map_err(|error| DaemonError::Replication(storage(error)))?;
                        return Err(DaemonError::GraphBuild(error));
                    }
                }
                workers.push(
                    RunningFollowerReceiver::start(receiver, peer)
                        .map_err(DaemonError::Replication)?,
                );
            }
            Err(error) => {
                jobs.drain()
                    .await
                    .map_err(|error| DaemonError::Replication(storage(error)))?;
                return Err(DaemonError::Replication(error));
            }
        }
    }
    if let Some(address) = config.mcp_listen_address() {
        let routes = config
            .databases()
            .iter()
            .zip(services.iter())
            .map(|(database, service)| {
                service
                    .hosted_mcp_dependencies()
                    .map(|dependencies| (database.alias().clone(), dependencies))
                    .ok_or(DaemonError::McpDependencies)
            })
            .collect::<Result<Vec<_>, _>>()?;
        *hosted_mcp = Some(
            HostedMcp::bind_routes(address, config.mcp_origins(), routes)
                .await
                .map_err(DaemonError::McpStart)?,
        );
    }
    if transport.is_finished() {
        let _ = transport.completed().await;
        return Err(DaemonError::TransportEnded);
    }
    if hosted_mcp.as_ref().is_some_and(HostedMcp::is_finished) {
        if let Some(hosted) = hosted_mcp {
            hosted.completed().await.map_err(DaemonError::McpStop)?;
        }
        return Err(DaemonError::McpTransportEnded);
    }
    if services.iter().any(|service| !service.is_available()) {
        return Err(DaemonError::Replication(ReplicationFailure::Unavailable));
    }
    let (shutdown, stdin_thread) = spawn_shutdown_reader()?;
    let mut shutdown = Some(shutdown);
    publish_readiness(&transport.endpoint)?;
    let mut endings: FuturesUnordered<_> = workers
        .iter_mut()
        .map(|worker| Box::pin(worker.finished()))
        .collect();
    let mut runtime_endings: FuturesUnordered<_> = lifecycles
        .iter()
        .map(|lifecycle| {
            let runtime = lifecycle.runtime_routing();
            Box::pin(async move { runtime.stopped().await })
        })
        .collect();
    loop {
        tokio::select! {
            result = signal.received() => return result.map_err(DaemonError::ShutdownSignal),
            _ = &mut transport.task => return Err(DaemonError::TransportEnded),
            result = wait_for_hosted_mcp(hosted_mcp) => return result.map_or_else(|error| Err(DaemonError::McpStop(error)), |()| Err(DaemonError::McpTransportEnded)),
            _ = futures_util::StreamExt::next(&mut runtime_endings) => return Err(DaemonError::RuntimeStopped),
            result = futures_util::StreamExt::next(&mut endings) => return Err(DaemonError::Replication(result.and_then(Result::err).unwrap_or(ReplicationFailure::Unavailable))),
            input = wait_for_shutdown_input(&mut shutdown) => match input {
                Some(ReadyProcessTrigger::Command) => return stdin_thread.join().map_err(|_| DaemonError::ShutdownReaderPanicked),
                Some(_) => { let _ = stdin_thread.join(); return Err(DaemonError::ShutdownInput); },
                None => {},
            },
        }
    }
}
async fn connect(
    source: FollowerSourceConfig,
) -> Result<Arc<dyn ReplicationSourcePort>, ReplicationFailure> {
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
        catalog_digest: AuthoritativeStateCatalogV1.digest(),
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
