//! Owning production component graph for the runnable P1 server.

use std::error::Error;
use std::fmt;
use std::sync::Arc;

use riffdb_api_grpc::CheckedGrpcSecurityContext;
use riffdb_auth::{AuthenticationContext, DigestKeyProviders};
use riffdb_commit::{
    CoordinatorDurability, CoordinatorShutdownError, CoordinatorStartError,
    CoordinatorWorkloadCapacity, RunningCommandCoordinator,
};
use riffdb_conflict::{
    ConflictManager, ConflictManagerBuildError, ConflictManagerConfig, ShardedConflictManager,
};
use riffdb_errors::IncidentIdSource;
use riffdb_idempotency::IdempotencyDigestProvider;
use riffdb_policy::{
    AgentSessionAdmissionPolicy, CapabilityMutationFactsError, TrustedAudienceCatalog,
};
use riffdb_service::{
    ApplicationService, AuthoritativeReadPort, BuildInfo, CapabilityTokenIssuer, CatalogReadPort,
    CurrentPolicyPort, CursorMonotonicClock, CursorTokenGenerator, OperationalStatusPort,
    ProjectionQueryPort, RequestDeadlineScheduler, RiffDbServiceActivator, ServiceDiagnostics,
    ServiceExecutors, ServiceHealthHooks, ServiceIdentity, ServiceJobSpawner,
    ServiceProcessMetadata, ServiceProviders, ServiceTelemetry,
};
use riffdb_storage_api::{
    ReadableDigestKey, ReadableIdempotencyDigestInventory, StorageValueError,
};
use riffdb_types::{Audience, Environment, Timestamp};

use crate::auth_adapters::{
    ServerCapabilityTokenIssuer, ServerCredentialAuthenticator, ServerCurrentPolicyPort,
    ServerIdempotencyDigestProvider,
};
use crate::clocks::ProductionWallClocks;
use crate::config::ServerConfig;
use crate::cursor::{ProductionCursorMonotonicClock, ProductionCursorTokenGenerator};
use crate::identifiers::{ProductionIdentifierSources, ServerRequestIdSource};
use crate::lifecycle::{LifecycleInstallError, ProductionLifecycleRoute};
use crate::notifications::{FirstCommitNotificationHub, NotificationHubError};
use crate::operational_status::ProductionOperationalStatusPort;
use crate::port_driver::{
    BlockingPortDriver, BlockingPortDriverShutdownError, BlockingPortDriverStartError,
};
use crate::projection_adapter::UnavailableProjectionPort;
use crate::read_adapters::{ServerAuthoritativeReadPort, ServerCatalogReadPort};
use crate::runtime_support::{
    ProductionServiceDiagnostics, ProductionServiceTelemetry, RuntimeSupportError,
    SupervisedServiceJobSpawner, TokioRequestDeadlineScheduler,
};
use crate::server_generation::{ProductionServerGenerationSource, ServerGenerationSourceError};
use crate::startup::CheckedRedbStartup;
use crate::storage::SharedRedbOperationalPorts;

/// Fixed P1 coordinator admission bound, independent of transport and port-driver bounds.
const P1_COORDINATOR_WORKLOAD_CAPACITY: u16 = 32;

/// Complete move-only input set for the one production graph construction.
pub(crate) struct ProductionGraphBuilder {
    startup: CheckedRedbStartup,
    activator: RiffDbServiceActivator,
    digest_keys: DigestKeyProviders,
    environment: Environment,
    audience: Audience,
    process: ServiceProcessMetadata,
    identifiers: ProductionIdentifierSources,
    clocks: ProductionWallClocks,
    server_generation: ProductionServerGenerationSource,
    lifecycle: Arc<ProductionLifecycleRoute>,
}

impl ProductionGraphBuilder {
    /// Captures checked process facts without retaining configuration paths.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        startup: CheckedRedbStartup,
        activator: RiffDbServiceActivator,
        digest_keys: DigestKeyProviders,
        config: &ServerConfig,
        started_at: Timestamp,
        build: BuildInfo,
        identifiers: ProductionIdentifierSources,
        clocks: ProductionWallClocks,
        lifecycle: Arc<ProductionLifecycleRoute>,
    ) -> Self {
        Self {
            startup,
            activator,
            digest_keys,
            environment: config.environment().clone(),
            audience: config.audience().clone(),
            process: ServiceProcessMetadata::new(started_at, build),
            identifiers,
            clocks,
            server_generation: ProductionServerGenerationSource::new(),
            lifecycle,
        }
    }

    /// Constructs each production authority exactly once and publishes it atomically.
    pub(crate) fn build(self) -> Result<RunningProductionGraph, ProductionGraphBuildError> {
        let Self {
            startup,
            activator,
            digest_keys,
            environment,
            audience,
            process,
            identifiers,
            clocks,
            server_generation,
            lifecycle,
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
        let trusted_audiences = TrustedAudienceCatalog::new(vec![audience.clone()])
            .map_err(ProductionGraphBuildError::TrustedAudience)?;
        let runtime = lifecycle.runtime_routing();
        let spawner = SupervisedServiceJobSpawner::from_current_runtime(runtime.clone())
            .map_err(ProductionGraphBuildError::Runtime)?;

        let storage = SharedRedbOperationalPorts::new(operational_ports);
        let blocking = BlockingPortDriver::new(runtime.clone())
            .map_err(ProductionGraphBuildError::BlockingDriver)?;
        let notifications = FirstCommitNotificationHub::from_retained_application_sequence(
            retained_metadata.application_sequence(),
            runtime.clone(),
        );

        let capability_keys = Arc::new(capability_keys);
        let idempotency_keys = Arc::new(idempotency_keys);
        let idempotency_digests: Arc<dyn IdempotencyDigestProvider> = Arc::new(
            ServerIdempotencyDigestProvider::new(Arc::clone(&idempotency_keys)),
        );

        let authentication_clock = clocks.authentication();
        let authorization_clock = clocks.authorization();
        let admission_clock = clocks.admission();
        let administration_clock = clocks.administration();
        let hosted_request_ids = identifiers.request_ids();

        let authenticator = Arc::new(ServerCredentialAuthenticator::new(
            storage.clone(),
            Arc::clone(&capability_keys),
            authentication_clock,
        ));
        let authentication = AuthenticationContext::new(database_id, environment.clone(), audience);
        let security = CheckedGrpcSecurityContext::new(
            authenticator,
            authentication,
            Arc::clone(&capability_keys),
        );

        let policy: Arc<dyn CurrentPolicyPort> = Arc::new(ServerCurrentPolicyPort::new(
            storage.clone(),
            authorization_clock.clone(),
            database_id,
            environment.clone(),
            trusted_audiences,
        ));
        let token_issuer: Arc<dyn CapabilityTokenIssuer> = Arc::new(
            ServerCapabilityTokenIssuer::new(Arc::clone(&capability_keys)),
        );
        let catalog: Arc<dyn CatalogReadPort> =
            Arc::new(ServerCatalogReadPort::new(storage.clone(), &blocking));
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

        let conflicts: Arc<dyn ConflictManager> =
            match ShardedConflictManager::new(ConflictManagerConfig::default()) {
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
            CoordinatorWorkloadCapacity::new(P1_COORDINATOR_WORKLOAD_CAPACITY)
                .expect("the fixed P1 coordinator workload capacity is nonzero");
        let coordinator = match RunningCommandCoordinator::start(
            coordinator_capacity,
            CoordinatorDurability::Sync,
            storage,
            conflicts,
            Arc::new(admission_clock),
            Arc::new(administration_clock),
            Arc::new(authorization_clock),
            Arc::new(identifiers.provenance_ids()),
            Arc::new(notifications.clone()),
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

        let executors = ServiceExecutors::new(
            coordinator.administration_audit_executor(),
            coordinator.control_plane_executor(),
            coordinator.command_executor(),
            coordinator.command_idempotency_inspector(idempotency_digests),
        );
        let telemetry = Arc::new(ProductionServiceTelemetry::default());
        let diagnostics = Arc::new(ProductionServiceDiagnostics::new(runtime.clone()));
        let incident_ids: Arc<dyn IncidentIdSource> = Arc::new(identifiers.incident_ids());
        let projection: Arc<dyn ProjectionQueryPort> = Arc::new(UnavailableProjectionPort);
        let operational: Arc<dyn OperationalStatusPort> =
            Arc::new(ProductionOperationalStatusPort::new(
                allocator_capacity,
                runtime.clone(),
                notifications.clone(),
            ));
        let health: Arc<dyn ServiceHealthHooks> = Arc::new(runtime.clone());
        let service_spawner: Arc<dyn ServiceJobSpawner> = Arc::new(spawner.clone());
        let deadline_scheduler: Arc<dyn RequestDeadlineScheduler> =
            Arc::new(TokioRequestDeadlineScheduler);
        let cursor_tokens: Arc<dyn CursorTokenGenerator> =
            Arc::new(ProductionCursorTokenGenerator::new());
        let cursor_clock: Arc<dyn CursorMonotonicClock> =
            Arc::new(ProductionCursorMonotonicClock::new());
        let providers = ServiceProviders::new(
            catalog,
            policy,
            authoritative,
            projection,
            None,
            operational,
            token_issuer,
            incident_ids,
            Arc::clone(&diagnostics) as Arc<dyn ServiceDiagnostics>,
            Arc::clone(&telemetry) as Arc<dyn ServiceTelemetry>,
            health,
            service_spawner,
            deadline_scheduler,
            cursor_tokens,
            cursor_clock,
        );
        let identity = ServiceIdentity::new(
            database_id,
            environment,
            AgentSessionAdmissionPolicy::Discard,
        );
        let service: Arc<dyn ApplicationService> =
            Arc::new(activator.activate(identity, process, executors, providers));

        if let Err(source) = lifecycle.install_activated(
            service,
            security,
            server_generation,
            startup_lifecycle,
            allocator_capacity,
        ) {
            lifecycle.stop();
            let cleanup = cleanup_unpublished_graph(coordinator, blocking, &notifications);
            return Err(ProductionGraphBuildError::Activation { source, cleanup });
        }

        Ok(RunningProductionGraph {
            lifecycle,
            spawner,
            notifications,
            _hosted_request_ids: hosted_request_ids,
            coordinator: Some(coordinator),
            blocking: Some(blocking),
        })
    }
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
    // WP-185 injects this retained wrapper into the hosted MCP consumer port.
    _hosted_request_ids: ServerRequestIdSource,
    coordinator: Option<RunningCommandCoordinator>,
    blocking: Option<BlockingPortDriver>,
}

impl RunningProductionGraph {
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
    pub(crate) async fn shutdown(mut self) -> Result<(), ProductionGraphShutdownError> {
        self.lifecycle.stop();
        self.spawner.wait_for_idle().await;

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
        shutdown_result(notification_failed, coordinator, blocking)
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
        cleanup: shutdown_result(notification_failed, None, blocking).err(),
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
        cleanup: shutdown_result(notification_failed, None, blocking).err(),
    }
}

fn cleanup_unpublished_graph(
    coordinator: RunningCommandCoordinator,
    blocking: BlockingPortDriver,
    notifications: &FirstCommitNotificationHub,
) -> Option<ProductionGraphShutdownError> {
    let notification_failed = notifications.shutdown().is_err();
    let coordinator = coordinator.shutdown().err();
    let blocking = blocking.shutdown_and_drain().err();
    shutdown_result(notification_failed, coordinator, blocking).err()
}

fn shutdown_result(
    notification_failed: bool,
    coordinator: Option<CoordinatorShutdownError>,
    blocking: Option<BlockingPortDriverShutdownError>,
) -> Result<(), ProductionGraphShutdownError> {
    if !notification_failed && coordinator.is_none() && blocking.is_none() {
        Ok(())
    } else {
        Err(ProductionGraphShutdownError {
            notification_failed,
            coordinator,
            blocking,
        })
    }
}

/// Closed construction failure with cleanup evidence for any started owner.
pub(crate) enum ProductionGraphBuildError {
    ServerGeneration(ServerGenerationSourceError),
    ReadableIdempotencyDigests(StorageValueError),
    TrustedAudience(CapabilityMutationFactsError),
    Runtime(RuntimeSupportError),
    BlockingDriver(BlockingPortDriverStartError),
    ConflictManager {
        source: ConflictManagerBuildError,
        cleanup: Option<ProductionGraphShutdownError>,
    },
    Coordinator {
        source: CoordinatorStartError,
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
            | Self::Activation { cleanup, .. } => cleanup.is_some(),
            Self::ServerGeneration(_)
            | Self::ReadableIdempotencyDigests(_)
            | Self::TrustedAudience(_)
            | Self::Runtime(_)
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
            Self::ReadableIdempotencyDigests(source) => Some(source),
            Self::TrustedAudience(source) => Some(source),
            Self::Runtime(source) => Some(source),
            Self::BlockingDriver(source) => Some(source),
            Self::ConflictManager { source, .. } => Some(source),
            Self::Coordinator { source, .. } => Some(source),
            Self::Activation { source, .. } => Some(source),
        }
    }
}

/// Aggregate evidence that every shutdown stage was attempted in order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ProductionGraphShutdownError {
    notification_failed: bool,
    coordinator: Option<CoordinatorShutdownError>,
    blocking: Option<BlockingPortDriverShutdownError>,
}

impl fmt::Display for ProductionGraphShutdownError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let _failed_stages = usize::from(self.notification_failed)
            + usize::from(self.coordinator.is_some())
            + usize::from(self.blocking.is_some());
        formatter.write_str("the production component graph did not shut down cleanly")
    }
}

impl Error for ProductionGraphShutdownError {}

#[cfg(test)]
mod tests {
    const SOURCE: &str = include_str!("process_graph.rs");

    fn production_source() -> &'static str {
        SOURCE
            .split_once("#[cfg(test)]")
            .expect("process graph has an architecture-test boundary")
            .0
    }

    #[test]
    fn production_coordinator_selects_only_sync_explicitly() {
        let source = production_source();
        assert!(source.contains("CoordinatorDurability::Sync"));
        assert!(!source.contains("CoordinatorDurability::Group"));
        assert!(!source.contains("DurabilityMode::Memory"));
    }

    #[test]
    fn semantic_authorities_have_one_construction_site() {
        let source = production_source();
        for constructor in [
            "SharedRedbOperationalPorts::new(",
            "BlockingPortDriver::new(",
            "FirstCommitNotificationHub::from_retained_application_sequence(",
            "ShardedConflictManager::new(",
            "RunningCommandCoordinator::start(",
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
            .find("lifecycle.install_activated(")
            .expect("route publication");
        assert!(sample < activate);
        assert!(activate < install);
    }

    #[test]
    fn hosted_request_id_authority_is_retained_without_premature_consumption() {
        let source = production_source();
        assert_eq!(source.matches("identifiers.request_ids()").count(), 1);
        assert!(source.contains("let hosted_request_ids = identifiers.request_ids();"));
        assert!(source.contains("_hosted_request_ids: ServerRequestIdSource,"));
        assert!(source.contains("_hosted_request_ids: hosted_request_ids,"));
        assert!(!source.contains("next_request_id("));
    }

    #[test]
    fn shutdown_order_is_route_jobs_notifications_coordinator_then_ports() {
        let source = production_source();
        let body = source
            .split_once("pub(crate) async fn shutdown")
            .expect("shutdown method")
            .1;
        let route = body.find("self.lifecycle.stop()").expect("route close");
        let jobs = body.find("wait_for_idle().await").expect("job drain");
        let notifications = body.find("notifications.shutdown()").expect("hub stop");
        let coordinator = body
            .find("let coordinator = self")
            .expect("coordinator drain");
        let ports = body.find("let blocking = self").expect("port drain");
        assert!(route < jobs);
        assert!(jobs < notifications);
        assert!(notifications < coordinator);
        assert!(coordinator < ports);
    }
}
