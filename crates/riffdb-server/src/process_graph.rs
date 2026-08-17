//! Owning production component graph for the runnable P1 server.

use std::error::Error;
use std::fmt;
use std::num::{NonZeroU16, NonZeroU32};
use std::sync::Arc;

use riffdb_api_grpc::CheckedGrpcSecurityContext;
use riffdb_api_mcp::McpTelemetry;
use riffdb_auth::{
    AuthenticationContext, AuthenticationTelemetry, CapabilityDigestKeyProvider,
    CredentialAuthenticator, DigestKeyProviders, IdempotencyDigestKeyProvider,
};
use riffdb_commit::{
    CommitTelemetry, CoordinatorDurability, CoordinatorShutdownError, CoordinatorStartError,
    CoordinatorWorkloadCapacity, RunningCommandCoordinator,
};
use riffdb_conflict::{
    ConflictManager, ConflictManagerBuildError, ConflictManagerConfig, ConflictObserver,
    ShardedConflictManager,
};
use riffdb_errors::IncidentIdSource;
use riffdb_idempotency::IdempotencyDigestProvider;
use riffdb_observability::{
    AuthoritativeComponent, AuthoritativeCondition, MAX_TRACE_RECORDS, Observability,
    ObservabilityBuildError,
};
use riffdb_outbox::{
    DeliveryPolicy, NoOutboxFailpoints, NoOutboxTelemetry, OutboxRecoveryResult, RecoveringOutbox,
};
use riffdb_policy::{
    AgentSessionAdmissionPolicy, AuthorizationTelemetry, CapabilityMutationFactsError,
    TrustedAudienceCatalog,
};
use riffdb_projection::{ProjectionNotifier, ProjectionSchemaRegistry};
use riffdb_service::{
    ApplicationExportApplication, ApplicationReimportApplication, ApplicationService,
    AuthoritativeReadPort, BuildInfo, CapabilityTokenIssuer, CatalogReadPort,
    ColumnarProjectionPort, ContractMigrationApplication, CurrentPolicyPort, CursorMonotonicClock,
    CursorTokenGenerator, EventConsumerClock, EventConsumerPort, EventLeaseTokenSource,
    OperationalStatusPort, ProjectionQueryPort, QueryModuleReadPort, ReactiveModuleReadPort,
    RequestDeadlineScheduler, RiffDbServiceActivator, ServiceDiagnostics, ServiceExecutors,
    ServiceHealthHooks, ServiceIdentity, ServiceJobSpawner, ServiceProcessMetadata,
    ServiceProviders, ServiceTelemetry,
};
use riffdb_storage_api::{
    OutboxDestinationIdV1, OutboxPageLimit, ReadableDigestKey, ReadableIdempotencyDigestInventory,
    StorageValueError, recover_event_consumers,
};
use riffdb_types::{Audience, Environment, Timestamp};

use crate::application_export_adapter::ServerApplicationExportCoordinator;
use crate::application_reimport_adapter::ServerApplicationReimportCoordinator;
use crate::auth_adapters::{
    ServerCapabilityTokenIssuer, ServerCredentialAuthenticator, ServerCurrentPolicyPort,
    ServerIdempotencyDigestProvider,
};
use crate::clocks::ProductionWallClocks;
use crate::columnar_adapter::{
    ColumnarRegistrationError, ColumnarRuntime, ServerColumnarProjectionPort,
};
use crate::columnar_worker::{
    ColumnarWorkerShutdownError, ColumnarWorkerStartError, RunningColumnarWorker,
};
use crate::config::{ConfiguredProjection, ServerConfig};
use crate::consumer_adapter::ServerEventConsumerPort;
use crate::consumer_token::ProductionEventLeaseTokenSource;
use crate::cursor::{ProductionCursorMonotonicClock, ProductionCursorTokenGenerator};
use crate::identifiers::{ProductionIdentifierSources, ServerRequestIdSource};
use crate::installation_adapter::ServerApplicationInstallationCoordinator;
use crate::lifecycle::{LifecycleInstallError, ProductionLifecycleRoute};
use crate::lifecycle_service::LifecycleApplicationService;
use crate::maintenance_adapter::MaintenanceController;
use crate::notifications::{FirstCommitNotificationHub, NotificationHubError};
use crate::operational_status::{OutboxRecoveryReadiness, ProductionOperationalStatusPort};
use crate::outbox_adapter::{
    NoDestinationOutboxHealth, ServerCommitNotificationSink, ServerOutboxStatusPort,
};
use crate::port_driver::{
    BlockingPortDriver, BlockingPortDriverShutdownError, BlockingPortDriverStartError,
};
use crate::projection_adapter::ServerProjectionQueryPort;
use crate::projection_worker::{
    ProjectionWorkerShutdownError, ProjectionWorkerStartError, RunningProjectionWorker,
};
use crate::read_adapters::{ServerAuthoritativeReadPort, ServerCatalogReadPort};
use crate::runtime_support::{
    ProductionObservabilityDiagnostics, ProductionObservabilityHealthHooks, RuntimeSupportError,
    SupervisedServiceJobSpawner, TokioRequestDeadlineScheduler,
};
use crate::server_generation::{ProductionServerGenerationSource, ServerGenerationSourceError};
use crate::startup::CheckedRedbStartup;
use crate::storage::SharedRedbOperationalPorts;

/// Fixed P1 coordinator admission bound, independent of the 256-command
/// transaction ceiling and the reserved shutdown slot.
///
/// Two complete maximum groups may queue while the actor owns one physical
/// transition. Retained command byte bounds remain enforced by the public
/// request and storage transaction ceilings.
///
/// Override with `RIFFDB_P1_COORDINATOR_WORKLOAD_CAPACITY` (1..=4096) for
/// saturation evidence harnesses only; production deployments leave the env
/// unset so the compiled default applies.
const P1_COORDINATOR_WORKLOAD_CAPACITY: u16 = 512;

fn p1_coordinator_workload_capacity() -> u16 {
    const ENV: &str = "RIFFDB_P1_COORDINATOR_WORKLOAD_CAPACITY";
    match std::env::var(ENV) {
        Ok(raw) => {
            let parsed = raw
                .parse::<u16>()
                .unwrap_or(P1_COORDINATOR_WORKLOAD_CAPACITY);
            parsed.clamp(1, 4096)
        }
        Err(_) => P1_COORDINATOR_WORKLOAD_CAPACITY,
    }
}

/// One checked secret-key snapshot shared by startup, maintenance, and a graph generation.
#[derive(Clone)]
pub(crate) struct ProductionDigestKeys {
    capability: Arc<CapabilityDigestKeyProvider>,
    idempotency: Arc<IdempotencyDigestKeyProvider>,
}

impl ProductionDigestKeys {
    /// Shares the exact providers parsed and namespace-checked in one file load.
    pub(crate) fn new(providers: DigestKeyProviders) -> Self {
        let (capability, idempotency) = providers.into_parts();
        Self {
            capability: Arc::new(capability),
            idempotency: Arc::new(idempotency),
        }
    }

    /// Borrows capability keys for startup inventory construction.
    pub(crate) fn capability(&self) -> &CapabilityDigestKeyProvider {
        &self.capability
    }

    /// Borrows idempotency keys for startup inventory construction.
    pub(crate) fn idempotency(&self) -> &IdempotencyDigestKeyProvider {
        &self.idempotency
    }

    /// Shares capability keys with the private staged-authorization driver.
    pub(crate) fn shared_capability(&self) -> Arc<CapabilityDigestKeyProvider> {
        Arc::clone(&self.capability)
    }

    fn into_parts(
        self,
    ) -> (
        Arc<CapabilityDigestKeyProvider>,
        Arc<IdempotencyDigestKeyProvider>,
    ) {
        (self.capability, self.idempotency)
    }
}

impl fmt::Debug for ProductionDigestKeys {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProductionDigestKeys([REDACTED])")
    }
}

/// Complete move-only input set for the one production graph construction.
pub(crate) struct ProductionGraphBuilder {
    startup: CheckedRedbStartup,
    activator: RiffDbServiceActivator,
    digest_keys: ProductionDigestKeys,
    environment: Environment,
    grpc_audience: Audience,
    mcp_audience: Option<Audience>,
    trusted_audiences: Vec<Audience>,
    process: ServiceProcessMetadata,
    identifiers: ProductionIdentifierSources,
    clocks: ProductionWallClocks,
    server_generation: ProductionServerGenerationSource,
    lifecycle: Arc<ProductionLifecycleRoute>,
    maintenance: MaintenanceController,
    projections: Vec<ConfiguredProjection>,
    projections_root: std::path::PathBuf,
}

impl ProductionGraphBuilder {
    /// Captures checked process facts without retaining configuration paths.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        startup: CheckedRedbStartup,
        activator: RiffDbServiceActivator,
        digest_keys: ProductionDigestKeys,
        config: &ServerConfig,
        environment: Environment,
        started_at: Timestamp,
        build: BuildInfo,
        identifiers: ProductionIdentifierSources,
        clocks: ProductionWallClocks,
        lifecycle: Arc<ProductionLifecycleRoute>,
        maintenance: MaintenanceController,
    ) -> Self {
        let grpc_audience = config.audience().clone();
        let mcp_audience = config.mcp_audience().cloned();
        let mut trusted_audiences = vec![grpc_audience.clone()];
        trusted_audiences.extend(mcp_audience.clone());
        Self {
            startup,
            activator,
            digest_keys,
            environment,
            grpc_audience,
            mcp_audience,
            trusted_audiences,
            process: ServiceProcessMetadata::new(started_at, build),
            identifiers,
            clocks,
            server_generation: ProductionServerGenerationSource::new(),
            lifecycle,
            maintenance,
            projections: config.projections().to_vec(),
            projections_root: config.projections_root().to_path_buf(),
        }
    }

    /// Constructs each production authority exactly once and publishes it atomically.
    pub(crate) fn build(self) -> Result<RunningProductionGraph, ProductionGraphBuildError> {
        let Self {
            startup,
            activator,
            digest_keys,
            environment,
            grpc_audience,
            mcp_audience,
            trusted_audiences,
            process,
            identifiers,
            clocks,
            server_generation,
            lifecycle,
            maintenance,
            projections,
            projections_root,
        } = self;

        let server_generation = server_generation
            .next_generation()
            .map_err(ProductionGraphBuildError::ServerGeneration)?;

        let (
            database_id,
            retained_metadata,
            _validated_catalog_history,
            startup_lifecycle,
            allocator_capacity,
            operational_ports,
        ) = startup.into_parts();
        let (capability_keys, idempotency_keys) = digest_keys.into_parts();
        let readable_idempotency_digests = ReadableIdempotencyDigestInventory::new(
            idempotency_keys
                .readable_key_ids()
                .map(ReadableDigestKey::v1)
                .collect(),
        )
        .map_err(ProductionGraphBuildError::ReadableIdempotencyDigests)?;
        let trusted_audiences = TrustedAudienceCatalog::new(trusted_audiences)
            .map_err(ProductionGraphBuildError::TrustedAudience)?;
        let runtime = lifecycle.runtime_routing();
        let spawner = SupervisedServiceJobSpawner::from_current_runtime(runtime.clone())
            .map_err(ProductionGraphBuildError::Runtime)?;

        let health: Arc<dyn ServiceHealthHooks> = Arc::new(runtime.clone());
        let mut storage = SharedRedbOperationalPorts::new(operational_ports, Some(health))
            .map_err(|_| ProductionGraphBuildError::CurrentView)?;
        let consumer_recovery_time = clocks
            .process_time()
            .map_err(|_| ProductionGraphBuildError::CurrentView)?;
        recover_event_consumers(
            &mut storage,
            consumer_recovery_time,
            retained_metadata.history_incarnation(),
        )
        .map_err(|_| ProductionGraphBuildError::CurrentView)?;
        let outbox_recovery = recover_outbox(storage.clone(), clocks.outbox());
        let outbox_health = NoDestinationOutboxHealth::new(outbox_recovery);
        outbox_health.refresh(&storage);
        let blocking = BlockingPortDriver::new(runtime.clone())
            .map_err(ProductionGraphBuildError::BlockingDriver)?;
        let notifications = FirstCommitNotificationHub::from_retained_application_sequence(
            retained_metadata.application_sequence(),
            runtime.clone(),
        );
        let commit_notifications = ServerCommitNotificationSink::new(
            notifications.clone(),
            storage.clone(),
            outbox_health.clone(),
        );

        let idempotency_digests: Arc<dyn IdempotencyDigestProvider> = Arc::new(
            ServerIdempotencyDigestProvider::new(Arc::clone(&idempotency_keys)),
        );

        let authentication_clock = clocks.authentication();
        let authorization_clock = clocks.authorization();
        let admission_clock = clocks.admission();
        let administration_clock = clocks.administration();
        let consumer_clock: Arc<dyn EventConsumerClock> = Arc::new(administration_clock.clone());
        let live_query_clock: Arc<dyn riffdb_service::LiveQueryClock> =
            Arc::new(administration_clock.clone());
        let event_lease_tokens: Arc<dyn EventLeaseTokenSource> =
            Arc::new(ProductionEventLeaseTokenSource);
        let hosted_request_ids = identifiers.request_ids();
        let incident_ids: Arc<dyn IncidentIdSource> = Arc::new(identifiers.incident_ids());
        let observability = Arc::new(
            Observability::new(Arc::clone(&incident_ids), MAX_TRACE_RECORDS)
                .map_err(ProductionGraphBuildError::Observability)?,
        );
        for component in [
            AuthoritativeComponent::Storage,
            AuthoritativeComponent::Catalog,
            AuthoritativeComponent::CommitCoordinator,
        ] {
            observability
                .health()
                .set_authoritative(component, AuthoritativeCondition::Healthy);
        }
        let authentication_telemetry: Arc<dyn AuthenticationTelemetry> = observability.clone();
        let authorization_telemetry: Arc<dyn AuthorizationTelemetry> = observability.clone();
        let conflict_observer: Arc<dyn ConflictObserver> = observability.clone();
        let commit_telemetry: Arc<dyn CommitTelemetry> = observability.clone();
        let mcp_telemetry: Arc<dyn McpTelemetry> = observability.clone();

        let authenticator: Arc<dyn CredentialAuthenticator> =
            Arc::new(ServerCredentialAuthenticator::new(
                storage.clone(),
                Arc::clone(&capability_keys),
                authentication_clock,
                authentication_telemetry,
            ));
        let authentication =
            AuthenticationContext::new(database_id, environment.clone(), grpc_audience);
        let hosted_authentication = mcp_audience
            .map(|audience| AuthenticationContext::new(database_id, environment.clone(), audience));
        let security = CheckedGrpcSecurityContext::new(
            Arc::clone(&authenticator),
            authentication,
            Arc::clone(&capability_keys),
        );

        let policy: Arc<dyn CurrentPolicyPort> = Arc::new(ServerCurrentPolicyPort::new(
            storage.clone(),
            authorization_clock.clone(),
            database_id,
            environment.clone(),
            trusted_audiences,
            authorization_telemetry,
        ));
        let token_issuer: Arc<dyn CapabilityTokenIssuer> = Arc::new(
            ServerCapabilityTokenIssuer::new(Arc::clone(&capability_keys)),
        );
        let catalog_adapter = Arc::new(ServerCatalogReadPort::new(storage.clone(), &blocking));
        let catalog: Arc<dyn CatalogReadPort> = catalog_adapter.clone();
        let query_modules: Arc<dyn QueryModuleReadPort> = catalog_adapter.clone();
        let reactive_modules: Arc<dyn ReactiveModuleReadPort> = catalog_adapter;
        let authoritative: Arc<dyn AuthoritativeReadPort> =
            Arc::new(ServerAuthoritativeReadPort::new(
                storage.clone(),
                Arc::clone(&idempotency_digests),
                readable_idempotency_digests,
                database_id,
                environment.clone(),
                notifications.clone(),
                &blocking,
            ));
        let event_consumers: Arc<dyn EventConsumerPort> = Arc::new(ServerEventConsumerPort::new(
            storage.clone(),
            database_id,
            &blocking,
        ));

        let conflicts: Arc<dyn ConflictManager> = match ShardedConflictManager::with_observer(
            ConflictManagerConfig::default(),
            conflict_observer,
        ) {
            Ok(conflicts) => Arc::new(conflicts),
            Err(source) => {
                return Err(cleanup_after_conflict_start_failure(
                    source,
                    blocking,
                    &notifications,
                ));
            }
        };
        let coordinator_capacity =
            CoordinatorWorkloadCapacity::new(p1_coordinator_workload_capacity())
                .expect("the fixed P1 coordinator workload capacity is nonzero");
        let coordinator = match RunningCommandCoordinator::start_with_telemetry(
            coordinator_capacity,
            CoordinatorDurability::Group,
            storage.clone(),
            conflicts,
            Arc::new(admission_clock),
            Arc::new(identifiers.service_uuids()),
            Arc::new(administration_clock),
            Arc::new(authorization_clock),
            Arc::new(identifiers.provenance_ids()),
            Arc::new(commit_notifications),
            commit_telemetry,
        ) {
            Ok(coordinator) => coordinator,
            Err(source) => {
                return Err(cleanup_after_coordinator_start_failure(
                    source,
                    blocking,
                    &notifications,
                ));
            }
        };
        let empty_projection_registry = ProjectionSchemaRegistry::new(Vec::new())
            .expect("the empty checked projection registry is valid");
        let projection_notifier = ProjectionNotifier::from_registry(&empty_projection_registry);
        let projection_worker =
            match RunningProjectionWorker::start(storage.clone(), projection_notifier.clone()) {
                Ok(worker) => worker,
                Err(source) => {
                    return Err(cleanup_after_projection_start_failure(
                        source,
                        coordinator,
                        blocking,
                        &notifications,
                    ));
                }
            };
        let projection_status = projection_worker.status();

        let columnar_runtime = match ColumnarRuntime::open(
            storage.clone(),
            &projections,
            &projections_root,
            retained_metadata.history_incarnation(),
        ) {
            Ok(runtime) => runtime,
            Err(source) => {
                return Err(cleanup_after_columnar_registration_failure(
                    source,
                    projection_worker,
                    coordinator,
                    blocking,
                    &notifications,
                ));
            }
        };
        let columnar_worker = match RunningColumnarWorker::start(
            Arc::clone(&columnar_runtime),
            Some(observability.metrics().clone()),
        ) {
            Ok(worker) => worker,
            Err(source) => {
                return Err(cleanup_after_columnar_start_failure(
                    source,
                    projection_worker,
                    coordinator,
                    blocking,
                    &notifications,
                ));
            }
        };
        // Columnar apply readiness folds into aggregate operational health.
        let columnar_status = columnar_worker.status();
        let columnar: Arc<dyn ColumnarProjectionPort> =
            Arc::new(ServerColumnarProjectionPort::new(columnar_runtime));

        let executors = ServiceExecutors::new(
            coordinator.administration_audit_executor(),
            coordinator.control_plane_executor(),
            coordinator.command_executor(),
            coordinator
                .command_idempotency_inspector(idempotency_digests)
                .with_direct_repository(Arc::new(storage.clone())),
        );
        let diagnostics = Arc::new(ProductionObservabilityDiagnostics::new(
            runtime.clone(),
            observability.clone(),
        ));
        let projection: Arc<dyn ProjectionQueryPort> = Arc::new(ServerProjectionQueryPort::new(
            storage.clone(),
            projection_notifier,
            &blocking,
        ));
        let outbox = Arc::new(ServerOutboxStatusPort::new(storage.clone(), &blocking));
        let operational: Arc<dyn OperationalStatusPort> =
            Arc::new(ProductionOperationalStatusPort::new(
                allocator_capacity,
                runtime.clone(),
                notifications.clone(),
                outbox_health,
                projection_status,
                columnar_status,
            ));
        let health: Arc<dyn ServiceHealthHooks> = Arc::new(
            ProductionObservabilityHealthHooks::new(runtime.clone(), observability.clone()),
        );
        let service_spawner: Arc<dyn ServiceJobSpawner> = Arc::new(spawner.clone());
        let deadline_scheduler: Arc<dyn RequestDeadlineScheduler> =
            Arc::new(TokioRequestDeadlineScheduler);
        let cursor_tokens: Arc<dyn CursorTokenGenerator> =
            Arc::new(ProductionCursorTokenGenerator::new());
        let cursor_clock: Arc<dyn CursorMonotonicClock> =
            Arc::new(ProductionCursorMonotonicClock::new());
        let migration = maintenance.migration_coordinator(&blocking, storage.clone());
        let offline_maintenance = maintenance.coordinator(&blocking);
        let installation = Arc::new(ServerApplicationInstallationCoordinator::new(
            storage.clone(),
            maintenance.installation_migration_receipts(),
            &blocking,
        ));
        let application_export = Arc::new(ServerApplicationExportCoordinator::new(
            storage.clone(),
            &blocking,
            clocks.application_export(),
        ));
        let application_reimport = Arc::new(ServerApplicationReimportCoordinator::new(
            storage.clone(),
            &blocking,
        ));
        // Retained for the graceful-shutdown validated-prefix write (ADR-0019 A1).
        let shutdown_storage = storage.clone();
        let providers = ServiceProviders::new(
            catalog,
            policy,
            authoritative,
            projection,
            Some(outbox),
            operational,
            token_issuer,
            incident_ids,
            Arc::clone(&diagnostics) as Arc<dyn ServiceDiagnostics>,
            observability.clone() as Arc<dyn ServiceTelemetry>,
            health,
            service_spawner,
            deadline_scheduler,
            cursor_tokens,
            cursor_clock,
        )
        .with_query_executor(Arc::new(storage))
        .with_contextual_causation(
            riffdb_service::ContextualCausationTokenCodec::from_provider(Arc::clone(
                &capability_keys,
            )
                as Arc<dyn riffdb_service::ContextualCausationMacProvider>),
        )
        .with_query_modules(query_modules)
        .with_reactive_modules(reactive_modules)
        .with_event_consumers(event_consumers, consumer_clock, event_lease_tokens)
        .with_live_query_clock(live_query_clock)
        .with_columnar(columnar)
        .with_offline_maintenance(offline_maintenance)
        .with_contract_migration(migration)
        .with_application_installation(installation);
        let providers = providers
            .with_application_export(application_export)
            .with_application_reimport(application_reimport);
        let identity = ServiceIdentity::new(
            database_id,
            environment,
            AgentSessionAdmissionPolicy::Discard,
            retained_metadata.history_incarnation(),
        );
        let service = Arc::new(activator.activate(identity, process, executors, providers));
        let application_service: Arc<dyn ApplicationService> = service.clone();
        let migration_service: Arc<dyn ContractMigrationApplication> = service.clone();
        let export_service: Arc<dyn ApplicationExportApplication> = service.clone();
        let reimport_service: Arc<dyn ApplicationReimportApplication> = service;

        if let Err(source) = lifecycle.install_activated_with_telemetry(
            application_service,
            security,
            server_generation,
            retained_metadata.history_incarnation(),
            startup_lifecycle,
            allocator_capacity,
            Some(observability.clone() as Arc<dyn ServiceTelemetry>),
        ) {
            lifecycle.stop();
            let cleanup = cleanup_unpublished_graph(
                columnar_worker,
                projection_worker,
                coordinator,
                blocking,
                &notifications,
            );
            return Err(ProductionGraphBuildError::Activation { source, cleanup });
        }
        if let Err(source) = lifecycle.install_contract_migration(migration_service) {
            lifecycle.stop();
            let cleanup = cleanup_unpublished_graph(
                columnar_worker,
                projection_worker,
                coordinator,
                blocking,
                &notifications,
            );
            return Err(ProductionGraphBuildError::Activation { source, cleanup });
        }
        if let Err(source) = lifecycle.install_application_export(export_service) {
            lifecycle.stop();
            let cleanup = cleanup_unpublished_graph(
                columnar_worker,
                projection_worker,
                coordinator,
                blocking,
                &notifications,
            );
            return Err(ProductionGraphBuildError::Activation { source, cleanup });
        }
        if let Err(source) = lifecycle.install_application_reimport(reimport_service) {
            lifecycle.stop();
            let cleanup = cleanup_unpublished_graph(
                columnar_worker,
                projection_worker,
                coordinator,
                blocking,
                &notifications,
            );
            return Err(ProductionGraphBuildError::Activation { source, cleanup });
        }
        let lifecycle_for_hosted: Arc<dyn riffdb_api_grpc::GrpcLifecycleRoute> = lifecycle.clone();
        let hosted_service: Arc<dyn ApplicationService> =
            Arc::new(LifecycleApplicationService::new(lifecycle_for_hosted));

        Ok(RunningProductionGraph {
            lifecycle,
            spawner,
            notifications,
            hosted_authenticator: authenticator,
            hosted_authentication,
            hosted_service,
            hosted_request_ids,
            mcp_telemetry,
            observability,
            storage: shutdown_storage,
            columnar_worker: Some(columnar_worker),
            projection_worker: Some(projection_worker),
            coordinator: Some(coordinator),
            blocking: Some(blocking),
        })
    }
}

fn recover_outbox(
    storage: SharedRedbOperationalPorts,
    mut clock: crate::clocks::ServerOutboxClock,
) -> OutboxRecoveryReadiness {
    let mut failpoints = NoOutboxFailpoints;
    let mut telemetry = NoOutboxTelemetry;
    match RecoveringOutbox::after_authoritative_readiness(storage).recover(
        &server_outbox_recovery_policy(),
        &mut clock,
        &mut failpoints,
        &mut telemetry,
    ) {
        OutboxRecoveryResult::Ready { .. } => OutboxRecoveryReadiness::Ready,
        OutboxRecoveryResult::Degraded { .. } => OutboxRecoveryReadiness::Degraded,
    }
}

fn server_outbox_recovery_policy() -> DeliveryPolicy {
    let destination = OutboxDestinationIdV1::new("server/no-destination-recovery")
        .expect("static recovery destination identity is valid");
    let timeout = NonZeroU32::new(5).expect("static timeout is nonzero");
    let lease = NonZeroU32::new(10).expect("static lease is nonzero");
    let attempts = NonZeroU32::new(32).expect("static attempt bound is nonzero");
    let retry_delays = vec![NonZeroU32::MIN; 31];
    let scan_limit =
        OutboxPageLimit::new(NonZeroU16::new(500).expect("static page limit is nonzero"))
            .expect("static page limit is bounded");
    DeliveryPolicy::new(
        destination,
        timeout,
        lease,
        attempts,
        retry_delays,
        scan_limit,
    )
    .expect("static recovery policy is valid")
}

impl fmt::Debug for ProductionGraphBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProductionGraphBuilder([CHECKED_INPUTS])")
    }
}

/// Owning guard for every thread, service job, and process-local authority in P1.
#[must_use = "the production graph must be explicitly shut down and joined"]
pub(crate) struct RunningProductionGraph {
    lifecycle: Arc<ProductionLifecycleRoute>,
    spawner: SupervisedServiceJobSpawner,
    notifications: FirstCommitNotificationHub,
    hosted_authenticator: Arc<dyn CredentialAuthenticator>,
    hosted_authentication: Option<AuthenticationContext>,
    hosted_service: Arc<dyn ApplicationService>,
    hosted_request_ids: ServerRequestIdSource,
    mcp_telemetry: Arc<dyn McpTelemetry>,
    observability: Arc<Observability>,
    /// Activated storage retained for the graceful-shutdown checkpoint write.
    storage: SharedRedbOperationalPorts,
    columnar_worker: Option<RunningColumnarWorker>,
    projection_worker: Option<RunningProjectionWorker>,
    coordinator: Option<RunningCommandCoordinator>,
    blocking: Option<BlockingPortDriver>,
}

impl RunningProductionGraph {
    /// Exact successful command-completion groups by size, with no business labels.
    pub(crate) fn write_completion_group_snapshot(
        &self,
    ) -> [u64; riffdb_observability::MAX_WRITE_GROUP_SIZE] {
        self.observability.write_completion_group_snapshot()
    }

    /// Dispatch-reason counts plus selected/deferred totals for shutdown evidence.
    pub(crate) fn command_group_dispatch_snapshot(
        &self,
    ) -> (
        [u64; riffdb_observability::COMMAND_GROUP_DISPATCH_REASON_COUNT],
        u64,
        u64,
    ) {
        self.observability.command_group_dispatch_snapshot()
    }

    /// Per-stage read-pipeline histograms for shutdown evidence.
    pub(crate) fn read_stage_snapshot(
        &self,
    ) -> [(
        u64,
        u64,
        [u64; riffdb_observability::HISTOGRAM_UPPER_BOUNDS.len()],
    ); riffdb_observability::READ_PIPELINE_STAGE_COUNT] {
        self.observability.read_stage_snapshot()
    }

    /// Per-stage mutating-command service histograms for shutdown evidence.
    pub(crate) fn write_service_stage_snapshot(
        &self,
    ) -> [(
        u64,
        u64,
        [u64; riffdb_observability::HISTOGRAM_UPPER_BOUNDS.len()],
    ); riffdb_observability::WRITE_SERVICE_STAGE_COUNT] {
        self.observability.write_service_stage_snapshot()
    }

    /// Per-stage coordinator command histograms for shutdown evidence.
    pub(crate) fn command_stage_snapshot(
        &self,
    ) -> [(
        u64,
        u64,
        [u64; riffdb_observability::HISTOGRAM_UPPER_BOUNDS.len()],
    ); riffdb_observability::COMMAND_PIPELINE_STAGE_COUNT] {
        self.observability.command_stage_snapshot()
    }

    /// Complete fixed-cardinality writer evidence for benchmark and shutdown diagnosis.
    pub(crate) fn writer_evidence_snapshot(
        &self,
    ) -> riffdb_observability::WriterEvidenceSnapshotV1 {
        self.observability.writer_evidence_snapshot()
    }

    /// Fixed-cardinality ordered-completion evidence for shutdown diagnosis.
    pub(crate) fn completion_lane_evidence_snapshot(
        &self,
    ) -> riffdb_observability::CompletionLaneEvidenceSnapshotV1 {
        self.observability.completion_lane_evidence_snapshot()
    }

    /// History incarnation retained from the successful open that built this graph.
    ///
    /// Available after activation even once ordinary admission is closed for
    /// offline maintenance (corrupt-target restore floor).
    pub(crate) fn retained_history_incarnation(&self) -> Option<u64> {
        self.lifecycle.retained_history_incarnation()
    }

    /// Process metrics registry for maintenance-driver operator signals.
    pub(crate) fn metrics(&self) -> riffdb_observability::MetricRegistry {
        self.observability.metrics().clone()
    }

    /// Returns the optional hosted-MCP dependencies over the shared lifecycle.
    pub(crate) fn hosted_mcp_dependencies(&self) -> Option<HostedMcpDependencies> {
        Some(HostedMcpDependencies {
            authenticator: Arc::clone(&self.hosted_authenticator),
            authentication: self.hosted_authentication.clone()?,
            service: Arc::clone(&self.hosted_service),
            request_ids: self.hosted_request_ids.clone(),
            telemetry: Arc::clone(&self.mcp_telemetry),
        })
    }

    /// Stops new RPC admission and terminalizes live commit streams.
    ///
    /// The remaining graph stays alive so handlers admitted before closure can
    /// finish against valid coordinator and storage owners. Closing the hub is
    /// what lets a graceful transport drain complete when a subscription is
    /// waiting for its next sequence hint.
    pub(crate) fn begin_transport_shutdown(&self) -> Result<(), NotificationHubError> {
        self.lifecycle.stop();
        self.notifications.shutdown()
    }

    /// Closes admission, drains accepted work, and joins every retained worker.
    ///
    /// The hosted transport must first stop accepting requests and drain every
    /// handler that already obtained a service clone. This method then proves
    /// that all independently supervised service work and lower workers drain.
    /// After the writer lane and blocking ports drain, the final engine commit
    /// is a non-fatal validated-prefix checkpoint write (ADR-0019 Amendment 1).
    pub(crate) async fn shutdown(mut self) -> Result<(), ProductionGraphShutdownError> {
        self.lifecycle.stop();
        self.spawner.wait_for_idle().await;

        let columnar = self
            .columnar_worker
            .take()
            .expect("a running graph retains one columnar worker")
            .shutdown()
            .err();
        let projection = self
            .projection_worker
            .take()
            .expect("a running graph retains one projection worker")
            .shutdown()
            .err();
        let notification_failed = self.notifications.shutdown().is_err();
        let coordinator = self
            .coordinator
            .take()
            .expect("a running graph retains one coordinator")
            .shutdown()
            .err();
        let blocking = self
            .blocking
            .take()
            .expect("a running graph retains one blocking driver")
            .shutdown_and_drain()
            .err();
        // ADR-0019 A1 write point (2): after the writer lane drains, as the
        // final engine commit. A write failure is non-fatal (lost fast path).
        write_shutdown_validated_prefix_checkpoint(&self.storage);
        shutdown_result(
            columnar,
            projection,
            notification_failed,
            coordinator,
            blocking,
        )
    }

    /// Maintenance-only shutdown with a closed boundary between fully drained
    /// workers and closing the final blocking storage ports.
    pub(crate) async fn shutdown_for_maintenance(
        mut self,
        recovery: &crate::maintenance_recovery_controller::MaintenanceRecoveryController,
    ) -> Result<(), ProductionGraphShutdownError> {
        self.lifecycle.stop();
        self.spawner.wait_for_idle().await;

        let columnar = self
            .columnar_worker
            .take()
            .expect("a running graph retains one columnar worker")
            .shutdown()
            .err();
        let projection = self
            .projection_worker
            .take()
            .expect("a running graph retains one projection worker")
            .shutdown()
            .err();
        let notification_failed = self.notifications.shutdown().is_err();
        let coordinator = self
            .coordinator
            .take()
            .expect("a running graph retains one coordinator")
            .shutdown()
            .err();
        recovery.reached(
            crate::maintenance_recovery_controller::MaintenanceRecoveryBoundary::DrainComplete,
        );
        let blocking = self
            .blocking
            .take()
            .expect("a running graph retains one blocking driver")
            .shutdown_and_drain()
            .err();
        // Same non-fatal final checkpoint write as graceful production shutdown.
        write_shutdown_validated_prefix_checkpoint(&self.storage);
        shutdown_result(
            columnar,
            projection,
            notification_failed,
            coordinator,
            blocking,
        )
    }
}

/// Graceful-shutdown validated-prefix checkpoint write (ADR-0019 Amendment 1).
///
/// Non-fatal: a failure costs only the next open's fast path and is counted on
/// the storage handle. Must never contribute to [`ProductionGraphShutdownError`].
fn write_shutdown_validated_prefix_checkpoint(storage: &SharedRedbOperationalPorts) {
    // Deliberately ignore Err — ADR-0019 A1 write-failure semantics.
    let _ = storage.write_validated_prefix_checkpoint();
}

/// Cloned least-authority inputs for the optional loopback MCP transport.
pub(crate) struct HostedMcpDependencies {
    pub(crate) authenticator: Arc<dyn CredentialAuthenticator>,
    pub(crate) authentication: AuthenticationContext,
    pub(crate) service: Arc<dyn ApplicationService>,
    pub(crate) request_ids: ServerRequestIdSource,
    pub(crate) telemetry: Arc<dyn McpTelemetry>,
}

impl fmt::Debug for HostedMcpDependencies {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("HostedMcpDependencies([CAPABILITIES])")
    }
}

impl fmt::Debug for RunningProductionGraph {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RunningProductionGraph([OWNING_CAPABILITIES])")
    }
}

fn cleanup_after_conflict_start_failure(
    source: ConflictManagerBuildError,
    blocking: BlockingPortDriver,
    notifications: &FirstCommitNotificationHub,
) -> ProductionGraphBuildError {
    let notification_failed = notifications.shutdown().is_err();
    let blocking = blocking.shutdown_and_drain().err();
    ProductionGraphBuildError::ConflictManager {
        source,
        cleanup: shutdown_result(None, None, notification_failed, None, blocking).err(),
    }
}

fn cleanup_after_coordinator_start_failure(
    source: CoordinatorStartError,
    blocking: BlockingPortDriver,
    notifications: &FirstCommitNotificationHub,
) -> ProductionGraphBuildError {
    let notification_failed = notifications.shutdown().is_err();
    let blocking = blocking.shutdown_and_drain().err();
    ProductionGraphBuildError::Coordinator {
        source,
        cleanup: shutdown_result(None, None, notification_failed, None, blocking).err(),
    }
}

fn cleanup_after_projection_start_failure(
    source: ProjectionWorkerStartError,
    coordinator: RunningCommandCoordinator,
    blocking: BlockingPortDriver,
    notifications: &FirstCommitNotificationHub,
) -> ProductionGraphBuildError {
    let notification_failed = notifications.shutdown().is_err();
    let coordinator = coordinator.shutdown().err();
    let blocking = blocking.shutdown_and_drain().err();
    ProductionGraphBuildError::ProjectionWorker {
        source,
        cleanup: shutdown_result(None, None, notification_failed, coordinator, blocking).err(),
    }
}

fn cleanup_after_columnar_registration_failure(
    source: ColumnarRegistrationError,
    projection_worker: RunningProjectionWorker,
    coordinator: RunningCommandCoordinator,
    blocking: BlockingPortDriver,
    notifications: &FirstCommitNotificationHub,
) -> ProductionGraphBuildError {
    let projection = projection_worker.shutdown().err();
    let notification_failed = notifications.shutdown().is_err();
    let coordinator = coordinator.shutdown().err();
    let blocking = blocking.shutdown_and_drain().err();
    ProductionGraphBuildError::ColumnarRegistration {
        source,
        cleanup: shutdown_result(None, projection, notification_failed, coordinator, blocking)
            .err(),
    }
}

fn cleanup_after_columnar_start_failure(
    source: ColumnarWorkerStartError,
    projection_worker: RunningProjectionWorker,
    coordinator: RunningCommandCoordinator,
    blocking: BlockingPortDriver,
    notifications: &FirstCommitNotificationHub,
) -> ProductionGraphBuildError {
    let projection = projection_worker.shutdown().err();
    let notification_failed = notifications.shutdown().is_err();
    let coordinator = coordinator.shutdown().err();
    let blocking = blocking.shutdown_and_drain().err();
    ProductionGraphBuildError::ColumnarWorker {
        source,
        cleanup: shutdown_result(None, projection, notification_failed, coordinator, blocking)
            .err(),
    }
}

fn cleanup_unpublished_graph(
    columnar_worker: RunningColumnarWorker,
    projection_worker: RunningProjectionWorker,
    coordinator: RunningCommandCoordinator,
    blocking: BlockingPortDriver,
    notifications: &FirstCommitNotificationHub,
) -> Option<ProductionGraphShutdownError> {
    let columnar = columnar_worker.shutdown().err();
    let projection = projection_worker.shutdown().err();
    let notification_failed = notifications.shutdown().is_err();
    let coordinator = coordinator.shutdown().err();
    let blocking = blocking.shutdown_and_drain().err();
    shutdown_result(
        columnar,
        projection,
        notification_failed,
        coordinator,
        blocking,
    )
    .err()
}

fn shutdown_result(
    columnar: Option<ColumnarWorkerShutdownError>,
    projection: Option<ProjectionWorkerShutdownError>,
    notification_failed: bool,
    coordinator: Option<CoordinatorShutdownError>,
    blocking: Option<BlockingPortDriverShutdownError>,
) -> Result<(), ProductionGraphShutdownError> {
    if columnar.is_none()
        && projection.is_none()
        && !notification_failed
        && coordinator.is_none()
        && blocking.is_none()
    {
        Ok(())
    } else {
        Err(ProductionGraphShutdownError {
            columnar,
            projection,
            notification_failed,
            coordinator,
            blocking,
        })
    }
}

/// Closed construction failure with cleanup evidence for any started owner.
pub(crate) enum ProductionGraphBuildError {
    ServerGeneration(ServerGenerationSourceError),
    CurrentView,
    ReadableIdempotencyDigests(StorageValueError),
    TrustedAudience(CapabilityMutationFactsError),
    Runtime(RuntimeSupportError),
    Observability(ObservabilityBuildError),
    BlockingDriver(BlockingPortDriverStartError),
    ConflictManager {
        source: ConflictManagerBuildError,
        cleanup: Option<ProductionGraphShutdownError>,
    },
    Coordinator {
        source: CoordinatorStartError,
        cleanup: Option<ProductionGraphShutdownError>,
    },
    ProjectionWorker {
        source: ProjectionWorkerStartError,
        cleanup: Option<ProductionGraphShutdownError>,
    },
    ColumnarRegistration {
        source: ColumnarRegistrationError,
        cleanup: Option<ProductionGraphShutdownError>,
    },
    ColumnarWorker {
        source: ColumnarWorkerStartError,
        cleanup: Option<ProductionGraphShutdownError>,
    },
    Activation {
        source: LifecycleInstallError,
        cleanup: Option<ProductionGraphShutdownError>,
    },
}

impl fmt::Debug for ProductionGraphBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProductionGraphBuildError([REDACTED])")
    }
}

impl fmt::Display for ProductionGraphBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let cleanup_failed = match self {
            Self::ConflictManager { cleanup, .. }
            | Self::Coordinator { cleanup, .. }
            | Self::ProjectionWorker { cleanup, .. }
            | Self::ColumnarRegistration { cleanup, .. }
            | Self::ColumnarWorker { cleanup, .. }
            | Self::Activation { cleanup, .. } => cleanup.is_some(),
            Self::ServerGeneration(_)
            | Self::CurrentView
            | Self::ReadableIdempotencyDigests(_)
            | Self::TrustedAudience(_)
            | Self::Runtime(_)
            | Self::Observability(_)
            | Self::BlockingDriver(_) => false,
        };
        if cleanup_failed {
            formatter
                .write_str("the production component graph could not be constructed or cleaned up")
        } else {
            formatter.write_str("the production component graph could not be constructed")
        }
    }
}

impl Error for ProductionGraphBuildError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ServerGeneration(source) => Some(source),
            Self::CurrentView => None,
            Self::ReadableIdempotencyDigests(source) => Some(source),
            Self::TrustedAudience(source) => Some(source),
            Self::Runtime(source) => Some(source),
            Self::Observability(source) => Some(source),
            Self::BlockingDriver(source) => Some(source),
            Self::ConflictManager { source, .. } => Some(source),
            Self::Coordinator { source, .. } => Some(source),
            Self::ProjectionWorker { source, .. } => Some(source),
            Self::ColumnarRegistration { source, .. } => Some(source),
            Self::ColumnarWorker { source, .. } => Some(source),
            Self::Activation { source, .. } => Some(source),
        }
    }
}

/// Aggregate evidence that every shutdown stage was attempted in order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ProductionGraphShutdownError {
    columnar: Option<ColumnarWorkerShutdownError>,
    projection: Option<ProjectionWorkerShutdownError>,
    notification_failed: bool,
    coordinator: Option<CoordinatorShutdownError>,
    blocking: Option<BlockingPortDriverShutdownError>,
}

impl fmt::Display for ProductionGraphShutdownError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let _failed_stages = usize::from(self.columnar.is_some())
            + usize::from(self.projection.is_some())
            + usize::from(self.notification_failed)
            + usize::from(self.coordinator.is_some())
            + usize::from(self.blocking.is_some());
        formatter.write_str("the production component graph did not shut down cleanly")
    }
}

impl Error for ProductionGraphShutdownError {}

#[cfg(test)]
mod tests {
    use super::P1_COORDINATOR_WORKLOAD_CAPACITY;
    use riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS;

    const SOURCE: &str = include_str!("process_graph.rs");

    fn production_source() -> &'static str {
        SOURCE
            .split_once("#[cfg(test)]")
            .expect("process graph has an architecture-test boundary")
            .0
    }

    #[test]
    fn production_coordinator_selects_only_group_explicitly() {
        let source = production_source();
        assert!(source.contains("CoordinatorDurability::Group"));
        assert!(!source.contains("CoordinatorDurability::Sync"));
        assert!(!source.contains("DurabilityMode::Memory"));
    }

    #[test]
    fn production_coordinator_can_queue_two_maximum_physical_groups() {
        assert_eq!(
            usize::from(P1_COORDINATOR_WORKLOAD_CAPACITY),
            MAX_GROUPED_WRITE_TRANSITIONS * 2
        );
    }

    #[test]
    fn semantic_authorities_have_one_construction_site() {
        let source = production_source();
        for constructor in [
            "SharedRedbOperationalPorts::new(",
            "BlockingPortDriver::new(",
            "FirstCommitNotificationHub::from_retained_application_sequence(",
            "Observability::new(",
            "ShardedConflictManager::with_observer(",
            "RunningCommandCoordinator::start_with_telemetry(",
            "RunningProjectionWorker::start(",
            "RunningColumnarWorker::start(",
            "CheckedGrpcSecurityContext::new(",
            "activator.activate(",
        ] {
            assert_eq!(
                source.matches(constructor).count(),
                1,
                "unexpected construction count for {constructor}"
            );
        }
    }

    #[test]
    fn process_generation_is_sampled_once_before_service_publication() {
        let source = production_source();
        assert_eq!(
            source
                .matches("ProductionServerGenerationSource::new()")
                .count(),
            1
        );
        assert_eq!(source.matches(".next_generation()").count(), 1);
        let sample = source
            .find(".next_generation()")
            .expect("one generation sample");
        let activate = source
            .find("activator.activate(")
            .expect("service activation");
        let install = source
            .find("lifecycle.install_activated_with_telemetry(")
            .expect("route publication");
        assert!(sample < activate);
        assert!(activate < install);
    }

    #[test]
    fn hosted_request_id_authority_is_retained_without_premature_consumption() {
        let source = production_source();
        assert_eq!(source.matches("identifiers.request_ids()").count(), 1);
        assert!(source.contains("let hosted_request_ids = identifiers.request_ids();"));
        assert!(source.contains("hosted_request_ids: ServerRequestIdSource,"));
        assert!(source.contains("hosted_request_ids,"));
        assert!(source.contains("request_ids: self.hosted_request_ids.clone(),"));
        assert!(!source.contains("next_request_id("));
    }

    #[test]
    fn shutdown_order_is_route_jobs_workers_notifications_coordinator_ports_then_checkpoint() {
        let source = production_source();
        let body = source
            .split_once("pub(crate) async fn shutdown")
            .expect("shutdown method")
            .1
            // Bound the body to this method only (not shutdown_for_maintenance).
            .split_once("pub(crate) async fn shutdown_for_maintenance")
            .expect("maintenance shutdown follows production shutdown")
            .0;
        let route = body.find("self.lifecycle.stop()").expect("route close");
        let jobs = body.find("wait_for_idle().await").expect("job drain");
        let columnar = body
            .find("let columnar = self")
            .expect("columnar worker drain");
        let projection = body
            .find("let projection = self")
            .expect("projection worker drain");
        let notifications = body.find("notifications.shutdown()").expect("hub stop");
        let coordinator = body
            .find("let coordinator = self")
            .expect("coordinator drain");
        let ports = body.find("let blocking = self").expect("port drain");
        let checkpoint = body
            .find("write_shutdown_validated_prefix_checkpoint")
            .expect("shutdown checkpoint write");
        assert!(route < jobs);
        assert!(jobs < columnar);
        assert!(columnar < projection);
        assert!(projection < notifications);
        assert!(notifications < coordinator);
        assert!(coordinator < ports);
        // ADR-0019 A1: after the writer lane drains, as the final engine commit.
        assert!(ports < checkpoint);
    }

    #[test]
    fn maintenance_shutdown_writes_the_checkpoint_after_the_final_port_drain() {
        // ADR-0019 A1 applies to BOTH graceful teardown paths: the maintenance
        // shutdown must also place the checkpoint write after the blocking
        // ports drain (the last commit-capable stage) and before the pure
        // error aggregation.
        let source = production_source();
        let body = source
            .split_once("pub(crate) async fn shutdown_for_maintenance")
            .expect("maintenance shutdown method")
            .1
            // Bound the body to this method only (the helper definition follows).
            .split_once("\nfn write_shutdown_validated_prefix_checkpoint")
            .expect("checkpoint helper follows the maintenance shutdown")
            .0;
        let drain_boundary = body
            .find("MaintenanceRecoveryBoundary::DrainComplete")
            .expect("maintenance drain boundary");
        let ports = body.find("let blocking = self").expect("port drain");
        let checkpoint = body
            .find("write_shutdown_validated_prefix_checkpoint")
            .expect("maintenance checkpoint write");
        let aggregation = body.find("shutdown_result(").expect("error aggregation");
        assert!(drain_boundary < ports);
        assert!(ports < checkpoint);
        assert!(checkpoint < aggregation);
    }

    #[test]
    fn shutdown_checkpoint_write_failure_is_non_fatal() {
        // Falsifiability (a): making the write fatal (propagating Err into
        // shutdown_result) must fail this pin — the write is deliberately
        // discarded so a lost fast path never fails graceful shutdown.
        let source = production_source();
        let helper = source
            .split_once("fn write_shutdown_validated_prefix_checkpoint")
            .expect("shutdown checkpoint helper")
            .1;
        let helper_body = helper.split_once('}').expect("helper body").0;
        assert!(
            helper_body.contains("let _ = storage.write_validated_prefix_checkpoint()"),
            "shutdown checkpoint write must ignore Err (non-fatal)"
        );
        assert!(
            !helper_body.contains('?'),
            "shutdown checkpoint write must not propagate failure with ?"
        );
        let shutdown_body = source
            .split_once("pub(crate) async fn shutdown")
            .expect("shutdown method")
            .1
            .split_once("pub(crate) async fn shutdown_for_maintenance")
            .expect("maintenance shutdown")
            .0;
        assert!(
            !shutdown_body.contains("write_shutdown_validated_prefix_checkpoint(&self.storage)?")
                && !shutdown_body.contains("write_validated_prefix_checkpoint()?"),
            "checkpoint write failure must not fail the shutdown result"
        );
    }
}
