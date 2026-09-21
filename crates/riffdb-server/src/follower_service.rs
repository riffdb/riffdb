//! Follower service graph: immutable reads, current policy and no primary executors.
use super::*;
use crate::columnar_adapter::{FollowerColumnarRuntime, RunningFollowerColumnarWorker};
use crate::port_driver::BlockingPortExecutor;
use crate::replication_bootstrap::FollowerReadSnapshots;
use crate::startup::FollowerStartupEvidence;
use riffdb_service::{
    ComponentHealth, HealthComponentKind, HealthComponentStatus, OperationalHealthSnapshot,
    OperationalStatisticsSnapshot, OperationalStatusError,
};

pub(crate) struct RunningFollowerService {
    lifecycle: Arc<ProductionLifecycleRoute>,
    spawner: SupervisedServiceJobSpawner,
    blocking: BlockingPortDriver,
    notifier: ProjectionNotifier,
    hosted: Option<HostedMcpDependencies>,
    exact_worker: RunningExactTextWorker,
    columnar_worker: RunningFollowerColumnarWorker,
}
impl RunningFollowerService {
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
        let exact_runtime = ExactTextRuntime::open(
            crate::projection_read_source::ProjectionReadSource::new(reads.clone()),
            projections_root,
            retained_metadata.history_incarnation(),
            exact_generation_from_process(&generation),
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
        let authenticator = Arc::new(ServerCredentialAuthenticator::new(
            reads.clone(),
            capability_keys.clone(),
            clocks.authentication(),
            observability.clone(),
        ));
        let security = CheckedGrpcSecurityContext::new(
            authenticator.clone(),
            AuthenticationContext::new(database_id, environment.clone(), audience),
            capability_keys.clone(),
        );
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
            Arc::new(ProductionCursorMonotonicClock::new()),
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
        );
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
