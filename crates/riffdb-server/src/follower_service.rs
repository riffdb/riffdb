//! Follower service graph: immutable reads, current policy and no primary executors.
use super::*;
use crate::columnar_adapter::{FollowerColumnarRuntime, RunningFollowerColumnarWorker};
use crate::maintenance_adapter::SharedMaintenanceStorage;
use crate::port_driver::BlockingPortExecutor;
use crate::replication_bootstrap::FollowerReadSnapshots;
use crate::startup::FollowerStartupEvidence;
use riffdb_api_grpc::ReplicationEpochVoteHandler;
use riffdb_auth::AuthenticatedPrincipal;
use riffdb_errors::PublicError;
use riffdb_policy::ReplicationDecision;
use riffdb_service::{
    ComponentHealth, HealthComponentKind, HealthComponentStatus, OperationalHealthSnapshot,
    OperationalStatisticsSnapshot, OperationalStatusError,
};
use riffdb_types::{
    LeadershipEpochV1, ReplicationEpochVotePositionV1, ReplicationEpochVoteRequestV1,
    ReplicationEpochVoteV1, ReplicationSourceHoldIdV1,
};

pub(crate) struct FollowerEpochVoteConfig {
    pub(crate) voter_hold_id: ReplicationSourceHoldIdV1,
    pub(crate) maintenance_storage: SharedMaintenanceStorage,
    pub(crate) promised_epoch: Arc<std::sync::atomic::AtomicU64>,
}

struct FollowerEpochVoteService {
    policy: Arc<ServerCurrentPolicyPort<FollowerReadSnapshots>>,
    reads: FollowerReadSnapshots,
    voter_hold_id: ReplicationSourceHoldIdV1,
    maintenance_storage: SharedMaintenanceStorage,
    promised_epoch: Arc<std::sync::atomic::AtomicU64>,
}

impl FollowerEpochVoteService {
    fn new(
        _database_id: riffdb_types::DatabaseId,
        _environment: Environment,
        policy: Arc<ServerCurrentPolicyPort<FollowerReadSnapshots>>,
        reads: FollowerReadSnapshots,
        config: FollowerEpochVoteConfig,
    ) -> Self {
        Self {
            policy,
            reads,
            voter_hold_id: config.voter_hold_id,
            maintenance_storage: config.maintenance_storage,
            promised_epoch: config.promised_epoch,
        }
    }
}

impl ReplicationEpochVoteHandler for FollowerEpochVoteService {
    fn request_epoch_vote(
        &self,
        principal: AuthenticatedPrincipal,
        request: ReplicationEpochVoteRequestV1,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<ReplicationEpochVoteV1, PublicError>>
                + Send
                + '_,
        >,
    > {
        let policy = Arc::clone(&self.policy);
        let reads = self.reads.clone();
        let voter_hold_id = self.voter_hold_id;
        let storage = self.maintenance_storage.clone();
        let promised_epoch = Arc::clone(&self.promised_epoch);
        Box::pin(async move {
            match policy.authorize_replication(&principal) {
                Ok(ReplicationDecision::Allow(_)) => {}
                Ok(ReplicationDecision::Deny(_)) => {
                    return Err(PublicError::authorization_denied());
                }
                Err(_) => return Err(PublicError::storage_unavailable()),
            }
            tokio::task::spawn_blocking(move || {
                let view = reads
                    .latest()
                    .map_err(|_| PublicError::storage_unavailable())?;
                let history = view.history();
                let lineage = history.lineage();
                let tail = history.tail();
                let target = request.candidate_target();
                if target.database_id() != lineage.database_id()
                    || target.history_incarnation() != lineage.history_incarnation()
                    || target.leadership_epoch() != lineage.leadership_epoch()
                {
                    return Err(PublicError::storage_unavailable());
                }
                let seq = std::num::NonZeroU64::new(tail.sequence().get())
                    .ok_or_else(PublicError::storage_unavailable)?;
                let voter_applied =
                    ReplicationEpochVotePositionV1::new(seq, tail.history_hash(), tail.frontier());
                let mut slot = storage
                    .lock()
                    .map_err(|_| PublicError::storage_unavailable())?;
                let owner = slot
                    .ordinary_mut()
                    .map_err(|_| PublicError::storage_unavailable())?;
                let existing = owner
                    .ordinary_ref()
                    .read_promised_leadership_epoch()
                    .map_err(|_| PublicError::storage_unavailable())?;
                let current_promised = existing
                    .map(|(e, _)| e)
                    .unwrap_or(lineage.leadership_epoch());
                promised_epoch
                    .fetch_max(current_promised.get(), std::sync::atomic::Ordering::SeqCst);
                let proposed = request.proposed_epoch();
                let candidate_pos = request.candidate_applied();
                let up_to_date = candidate_pos.is_at_least_as_up_to_date_as(voter_applied);
                let (granted, effective_epoch) = if !up_to_date || proposed < current_promised {
                    (false, current_promised.max(proposed))
                } else {
                    let ok = owner
                        .ordinary_ref()
                        .persist_promised_leadership_epoch(proposed, target.hold_id())
                        .map_err(|_| PublicError::storage_unavailable())?;
                    if ok {
                        promised_epoch
                            .fetch_max(proposed.get(), std::sync::atomic::Ordering::SeqCst);
                        (true, proposed)
                    } else {
                        let refreshed = owner
                            .ordinary_ref()
                            .read_promised_leadership_epoch()
                            .map_err(|_| PublicError::storage_unavailable())?
                            .map(|(e, _)| e)
                            .unwrap_or(current_promised);
                        promised_epoch
                            .fetch_max(refreshed.get(), std::sync::atomic::Ordering::SeqCst);
                        (false, refreshed.max(proposed))
                    }
                };
                let effective_epoch = LeadershipEpochV1::new(
                    effective_epoch
                        .get()
                        .max(promised_epoch.load(std::sync::atomic::Ordering::Acquire)),
                )
                .unwrap_or(effective_epoch);
                ReplicationEpochVoteV1::new(
                    voter_hold_id,
                    lineage.database_id(),
                    lineage.history_incarnation(),
                    effective_epoch,
                    target.hold_id(),
                    candidate_pos,
                    voter_applied,
                    granted,
                )
                .ok_or_else(PublicError::storage_unavailable)
            })
            .await
            .map_err(|_| PublicError::storage_unavailable())?
        })
    }
}

pub(crate) struct RunningFollowerService {
    lifecycle: Arc<ProductionLifecycleRoute>,
    spawner: SupervisedServiceJobSpawner,
    blocking: BlockingPortDriver,
    notifier: ProjectionNotifier,
    hosted: Option<HostedMcpDependencies>,
    #[allow(dead_code)]
    hosted_http: HostedHttpDependencies,
    exact_worker: RunningExactTextWorker,
    columnar_worker: RunningFollowerColumnarWorker,
}
impl RunningFollowerService {
    #[allow(dead_code)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn start(
        startup: FollowerStartupEvidence,
        reads: FollowerReadSnapshots,
        notifier: ProjectionNotifier,
        projections_root: &std::path::Path,
        projections: &[crate::config::ConfiguredProjection],
        activator: RiffDbServiceActivator,
        keys: ProductionDigestKeys,
        audience: Audience,
        mcp_audience: Option<Audience>,
        environment: Environment,
        process: ServiceProcessMetadata,
        clocks: &ProductionWallClocks,
        lifecycle: Arc<ProductionLifecycleRoute>,
        promotion: Option<Arc<crate::promotion_admission::PromotionController>>,
    ) -> Result<Self, ProductionGraphBuildError> {
        Self::start_with_epoch_vote(
            startup,
            reads,
            notifier,
            projections_root,
            projections,
            activator,
            keys,
            audience,
            mcp_audience,
            !matches!(environment.as_str(), "prod" | "production"),
            environment,
            process,
            clocks,
            lifecycle,
            promotion,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn start_with_epoch_vote(
        startup: FollowerStartupEvidence,
        reads: FollowerReadSnapshots,
        notifier: ProjectionNotifier,
        projections_root: &std::path::Path,
        projections: &[crate::config::ConfiguredProjection],
        activator: RiffDbServiceActivator,
        keys: ProductionDigestKeys,
        audience: Audience,
        mcp_audience: Option<Audience>,
        authorization_diagnostics: bool,
        environment: Environment,
        process: ServiceProcessMetadata,
        clocks: &ProductionWallClocks,
        lifecycle: Arc<ProductionLifecycleRoute>,
        promotion: Option<Arc<crate::promotion_admission::PromotionController>>,
        epoch_vote: Option<FollowerEpochVoteConfig>,
    ) -> Result<Self, ProductionGraphBuildError> {
        let initial = reads
            .latest()
            .map_err(|_| ProductionGraphBuildError::CurrentView)?;
        if initial.history().lineage().database_id() != startup.database_id
            || initial.history().lineage().history_incarnation()
                != startup.retained_metadata.history_incarnation()
        {
            return Err(ProductionGraphBuildError::CurrentView);
        }
        crate::columnar_adapter::validate_follower_columnar_admission(&initial, projections)
            .map_err(|source| ProductionGraphBuildError::ColumnarRegistration {
                source,
                cleanup: None,
            })?;
        drop(initial);
        let FollowerStartupEvidence {
            database_id,
            retained_metadata,
            catalog_history: _validated_catalog_history,
            lifecycle: startup_lifecycle,
            allocator_capacity,
        } = startup;
        let generation = ProductionServerGenerationSource::new()
            .next_generation()
            .map_err(ProductionGraphBuildError::ServerGeneration)?;
        let columnar_runtime = FollowerColumnarRuntime::open(
            reads.clone(),
            projections,
            projections_root,
            generation.bytes(),
            clocks.columnar_replay(),
        )
        .map_err(|source| ProductionGraphBuildError::ColumnarRegistration {
            source,
            cleanup: None,
        })?;
        let cursor_clock: Arc<dyn CursorMonotonicClock> =
            Arc::new(ProductionCursorMonotonicClock::new());
        let catalog_reads = reads.clone();
        let provider_source =
            crate::projection_read_source::ProjectionReadSource::new(reads.clone())
                .with_catalog_probe(move |snapshot, graph| {
                    use crate::exact_text_adapter::RebuildFailure;
                    use riffdb_storage_api::CatalogRepository;
                    let _work = graph
                        .budget
                        .reserve(32 * 1024)
                        .map_err(|_| RebuildFailure::Transient)?;
                    let pointer = snapshot
                        .read_active_catalog()
                        .map_err(|_| RebuildFailure::Transient)?;
                    let view = catalog_reads
                        .latest()
                        .map_err(|_| RebuildFailure::Transient)?;
                    let catalog = view
                        .catalog()
                        .filter(|catalog| Some(catalog.pointer()) == pointer.as_ref())
                        .ok_or(RebuildFailure::Transient)?;
                    let _admitted = graph
                        .admit(1024, catalog, |catalog, inspection| {
                            inspection.visit(catalog)
                        })
                        .map_err(|error| match error {
                            crate::exact_provider_graph::ProviderAdmissionError::Capacity => {
                                RebuildFailure::Transient
                            }
                            crate::exact_provider_graph::ProviderAdmissionError::Integrity => {
                                RebuildFailure::Integrity
                            }
                        })?;
                    Ok(catalog.clone())
                });
        let exact_runtime = ExactTextRuntime::open(
            provider_source,
            projections_root,
            retained_metadata.history_incarnation(),
            exact_generation_from_process(&generation),
            Arc::clone(&cursor_clock),
        )
        .map_err(|source| ProductionGraphBuildError::ExactTextRuntime {
            source,
            cleanup: None,
        })?;
        let runtime = lifecycle.runtime_routing();
        let spawner = SupervisedServiceJobSpawner::from_current_runtime(runtime.clone())
            .map_err(ProductionGraphBuildError::Runtime)?;
        let identifiers = ProductionIdentifierSources::new();
        let incident_ids: Arc<dyn IncidentIdSource> = Arc::new(identifiers.incident_ids());
        let observability = Arc::new(
            Observability::new(incident_ids.clone(), MAX_TRACE_RECORDS)
                .map_err(ProductionGraphBuildError::Observability)?,
        );
        for component in [
            AuthoritativeComponent::Storage,
            AuthoritativeComponent::Catalog,
        ] {
            observability
                .health()
                .set_authoritative(component, AuthoritativeCondition::Healthy);
        }
        let (capability_keys, _) = keys.into_parts();
        let mut audiences = vec![audience.clone()];
        audiences.extend(mcp_audience.clone());
        let policy = Arc::new(ServerCurrentPolicyPort::new(
            reads.clone(),
            clocks.authorization(),
            database_id,
            environment.clone(),
            TrustedAudienceCatalog::new(audiences)
                .map_err(ProductionGraphBuildError::TrustedAudience)?,
            observability.clone(),
        ));
        if let Some(controller) = promotion {
            lifecycle
                .install_promotion(Arc::new(riffdb_service::FollowerPromotionService::new(
                    database_id,
                    environment.clone(),
                    policy.clone(),
                    controller,
                )))
                .map_err(|source| ProductionGraphBuildError::Activation {
                    source,
                    cleanup: None,
                })?;
        }
        if let Some(epoch_vote_config) = epoch_vote {
            lifecycle
                .install_replication_epoch_vote(Arc::new(FollowerEpochVoteService::new(
                    database_id,
                    environment.clone(),
                    policy.clone(),
                    reads.clone(),
                    epoch_vote_config,
                )))
                .map_err(|source| ProductionGraphBuildError::Activation {
                    source,
                    cleanup: None,
                })?;
        }
        let authenticator = Arc::new(ServerCredentialAuthenticator::new(
            reads.clone(),
            capability_keys.clone(),
            clocks.authentication(),
            observability.clone(),
        ));
        let application_authentication =
            AuthenticationContext::new(database_id, environment.clone(), audience);
        let security = CheckedGrpcSecurityContext::new(
            authenticator.clone(),
            application_authentication.clone(),
            capability_keys.clone(),
        );
        let hosted_http = HostedHttpDependencies {
            authenticator: authenticator.clone(),
            authentication: application_authentication,
            service: Arc::new(LifecycleApplicationService::new(lifecycle.clone())),
            request_ids: identifiers.request_ids(),
        };
        let hosted = mcp_audience.map(|audience| HostedMcpDependencies {
            authenticator,
            authentication: AuthenticationContext::new(database_id, environment.clone(), audience),
            service: Arc::new(LifecycleApplicationService::new(lifecycle.clone())),
            request_ids: identifiers.request_ids(),
            telemetry: observability.clone(),
        });
        let blocking = BlockingPortDriver::new(runtime.clone())
            .map_err(ProductionGraphBuildError::BlockingDriver)?;
        let catalog = Arc::new(ServerCatalogReadPort::new(reads.clone(), &blocking));
        let authoritative = Arc::new(ServerAuthoritativeReadPort::follower(
            reads.clone(),
            &blocking,
        ));
        let projection = Arc::new(ServerProjectionQueryPort::new(
            reads.clone(),
            notifier.clone(),
            &blocking,
        ));
        let operational = Arc::new(FollowerOperationalStatus::new(
            reads.clone(),
            columnar_runtime.clone(),
            &blocking,
        ));
        let diagnostics = Arc::new(ProductionObservabilityDiagnostics::new(
            runtime.clone(),
            observability.clone(),
        ));
        let health = Arc::new(ProductionObservabilityHealthHooks::new(
            runtime.clone(),
            observability.clone(),
        ));
        let providers = ServiceProviders::new(
            catalog.clone(),
            policy,
            authoritative,
            projection,
            None,
            operational,
            Arc::new(ServerCapabilityTokenIssuer::new(capability_keys.clone())),
            incident_ids,
            diagnostics,
            observability.clone(),
            health,
            Arc::new(spawner.clone()),
            Arc::new(TokioRequestDeadlineScheduler),
            Arc::new(ProductionCursorTokenGenerator::new()),
            cursor_clock,
        )
        .with_query_executor(Arc::new(riffdb_query_executor::StorageQueryExecutor::new(
            reads.clone(),
        )))
        .with_query_modules(catalog.clone())
        .with_reactive_modules(catalog)
        .with_exact_text(exact_runtime.clone())
        .with_exact_predicate(exact_runtime.clone())
        .with_long_pattern(exact_runtime.clone())
        .with_tokenized_text(exact_runtime.clone())
        .with_columnar(columnar_runtime.clone())
        .with_vector_projection(columnar_runtime.clone())
        .with_live_query_clock(Arc::new(clocks.administration()))
        .with_contextual_causation(
            riffdb_service::ContextualCausationTokenCodec::from_provider(capability_keys),
        );
        let identity = ServiceIdentity::new(
            database_id,
            environment,
            AgentSessionAdmissionPolicy::Discard,
            retained_metadata.history_incarnation(),
        )
        .with_authorization_diagnostics(authorization_diagnostics);
        let exact_worker = match RunningExactTextWorker::start(exact_runtime) {
            Ok(worker) => worker,
            Err(source) => {
                let cleanup = blocking.shutdown_and_drain().err().map(|blocking| {
                    ProductionGraphShutdownError {
                        blocking: Some(blocking),
                        ..ProductionGraphShutdownError::checkpoint_close_only()
                    }
                });
                return Err(ProductionGraphBuildError::ExactTextWorker { source, cleanup });
            }
        };
        let columnar_worker = match RunningFollowerColumnarWorker::start(columnar_runtime) {
            Ok(worker) => worker,
            Err(source) => {
                let exact = exact_worker.shutdown().err();
                let blocking = blocking.shutdown_and_drain().err();
                let cleanup = (exact.is_some() || blocking.is_some()).then_some(
                    ProductionGraphShutdownError {
                        exact,
                        blocking,
                        ..ProductionGraphShutdownError::checkpoint_close_only()
                    },
                );
                return Err(ProductionGraphBuildError::ColumnarWorker { source, cleanup });
            }
        };
        let service = Arc::new(activator.activate(
            identity,
            process,
            ServiceExecutors::follower(),
            providers,
        ));
        let installed = (|| {
            lifecycle.bind_follower_reads(reads)?;
            lifecycle.install_contract_migration(service.clone())?;
            lifecycle.install_application_export(service.clone())?;
            lifecycle.install_application_reimport(service.clone())?;
            lifecycle.install_activated_with_telemetry(
                service,
                security,
                generation,
                retained_metadata.history_incarnation(),
                startup_lifecycle,
                allocator_capacity,
                Some(observability),
            )
        })();
        if let Err(source) = installed {
            lifecycle.stop();
            let exact = exact_worker.shutdown().err();
            let columnar = columnar_worker.shutdown().err();
            let blocking = blocking.shutdown_and_drain().err();
            let cleanup = (exact.is_some() || columnar.is_some() || blocking.is_some()).then_some(
                ProductionGraphShutdownError {
                    exact,
                    columnar,
                    blocking,
                    ..ProductionGraphShutdownError::checkpoint_close_only()
                },
            );
            return Err(ProductionGraphBuildError::Activation { source, cleanup });
        }
        Ok(Self {
            lifecycle,
            spawner,
            blocking,
            notifier,
            hosted,
            hosted_http,
            exact_worker,
            columnar_worker,
        })
    }
    pub(crate) fn is_available(&self) -> bool {
        use riffdb_api_grpc::GrpcLifecycleRoute;
        self.lifecycle
            .admit_authenticated(riffdb_types::ServiceOperationV1::GetHealth)
            .is_some()
    }
    pub(crate) fn hosted_mcp_dependencies(&self) -> Option<HostedMcpDependencies> {
        self.hosted.clone()
    }
    #[allow(dead_code)]
    pub(crate) fn hosted_http_dependencies(&self) -> HostedHttpDependencies {
        self.hosted_http.clone()
    }
    /// Transport admission must be closed before draining admitted requests.
    pub(crate) async fn shutdown(self) -> Result<(), riffdb_storage_api::StorageError> {
        self.lifecycle.stop();
        self.columnar_worker.stop_admission();
        self.lifecycle
            .runtime_routing()
            .fail_authoritative_readiness(
                riffdb_service::AuthoritativeReadinessFailure::CoordinatorFenced,
            );
        let notification = ProjectionSchemaRegistry::new(Vec::new())
            .map_err(|_| unavailable())
            .and_then(|empty| {
                self.notifier
                    .synchronize_registry(&empty)
                    .map(|_| ())
                    .map_err(|_| unavailable())
            });
        self.spawner.wait_for_idle().await;
        tokio::task::spawn_blocking(move || {
            let exact = self.exact_worker.shutdown().map_err(|_| unavailable());
            let columnar = self.columnar_worker.shutdown().map_err(|_| unavailable());
            let blocking = self
                .blocking
                .shutdown_and_drain()
                .map_err(|_| unavailable());
            exact.and(columnar).and(blocking)
        })
        .await
        .map_err(|_| unavailable())?
        .map_err(|_| unavailable())
        .and(notification)
    }
}
fn unavailable() -> riffdb_storage_api::StorageError {
    riffdb_storage_api::StorageError::new(riffdb_storage_api::StorageErrorKind::Unavailable, None)
}
struct FollowerOperationalStatus {
    health: BlockingPortExecutor<(), OperationalHealthSnapshot, OperationalStatusError>,
    statistics: BlockingPortExecutor<(), OperationalStatisticsSnapshot, OperationalStatusError>,
}
impl FollowerOperationalStatus {
    fn new(
        reads: FollowerReadSnapshots,
        columnar: Arc<FollowerColumnarRuntime>,
        driver: &BlockingPortDriver,
    ) -> Self {
        let source = reads.clone();
        let health = driver.executor(move |()| {
            let view = source
                .latest()
                .map_err(|_| OperationalStatusError::Unavailable)?;
            let catalog = if view.catalog().is_some() {
                HealthComponentStatus::Healthy
            } else {
                HealthComponentStatus::Degraded
            };
            OperationalHealthSnapshot::new(vec![
                ComponentHealth::new(
                    HealthComponentKind::AuthoritativeStorage,
                    HealthComponentStatus::Healthy,
                ),
                ComponentHealth::new(HealthComponentKind::Catalog, catalog),
                ComponentHealth::new(
                    HealthComponentKind::Projection,
                    if columnar.is_healthy() {
                        catalog
                    } else {
                        HealthComponentStatus::Degraded
                    },
                ),
                ComponentHealth::replication(follower_replication_statistics(&view)?),
            ])
            .and_then(|snapshot| {
                // ADR-0203: the member is present in authenticated health. A
                // follower's startup always takes the complete path and runs no
                // background scrub, so its member honestly stays pending.
                snapshot.with_storage_scrub(riffdb_service::StorageScrubHealth::new(
                    riffdb_service::StorageScrubHealthState::Pending,
                )?)
            })
            .map_err(|_| OperationalStatusError::Integrity)
        });
        let statistics = driver.executor(move |()| {
            let view = reads
                .latest()
                .map_err(|_| OperationalStatusError::Unavailable)?;
            let count = view
                .catalog()
                .map(|catalog| u32::try_from(catalog.bundle().bundle().projections().len()))
                .transpose()
                .map_err(|_| OperationalStatusError::Integrity)?;
            Ok(OperationalStatisticsSnapshot::new(
                view.history().tail().frontier().application(),
                None,
                count,
            )
            .with_replication(follower_replication_statistics(&view)?))
        });
        Self { health, statistics }
    }
}
fn follower_replication_statistics(
    view: &crate::replication_bootstrap::FollowerReadView,
) -> Result<riffdb_service::ReplicationStatistics, OperationalStatusError> {
    riffdb_service::ReplicationStatistics::follower(
        view.history().tail().frontier(),
        view.acknowledged().map(|point| point.frontier()),
        view.source_head().map(|head| head.frontier()),
    )
    .map_err(|_| OperationalStatusError::Integrity)
}
impl OperationalStatusPort for FollowerOperationalStatus {
    fn reserve_health<'a>(
        &'a self,
        control: &'a riffdb_service::RequestControl,
    ) -> riffdb_service::PortFuture<
        'a,
        riffdb_service::BoxPortCapacityPermit<
            (),
            OperationalHealthSnapshot,
            OperationalStatusError,
        >,
        riffdb_service::PortAdmissionError,
    > {
        Box::pin(self.health.reserve_async(control))
    }
    fn reserve_statistics<'a>(
        &'a self,
        control: &'a riffdb_service::RequestControl,
    ) -> riffdb_service::PortFuture<
        'a,
        riffdb_service::BoxPortCapacityPermit<
            (),
            OperationalStatisticsSnapshot,
            OperationalStatusError,
        >,
        riffdb_service::PortAdmissionError,
    > {
        Box::pin(self.statistics.reserve_async(control))
    }
}
