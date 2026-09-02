#![expect(
    clippy::expect_used,
    reason = "a running restore-retry host retains its sole blocking driver until shutdown"
)]

//! Least-authority composition for one current-database restore retry.

use std::error::Error;
use std::fmt;
use std::sync::Arc;

use riffdb_api_grpc::CheckedGrpcRestoreRetrySecurityContext;
use riffdb_auth::{AuthenticationContext, AuthenticationTelemetry, CredentialAuthenticator};
use riffdb_errors::IncidentIdSource;
use riffdb_observability::{MAX_TRACE_RECORDS, Observability, ObservabilityBuildError};
use riffdb_policy::{AuthorizationTelemetry, CapabilityMutationFactsError, TrustedAudienceCatalog};
use riffdb_service::{
    CurrentPolicyPort, RequestDeadlineScheduler, RestoreRetryOfflineMaintenanceApplication,
    RestoreRetryOfflineMaintenanceService, ServiceDiagnostics, ServiceHealthHooks,
    ServiceJobSpawner,
};
use riffdb_types::Environment;
use riffdb_types::{OfflineMaintenanceInputHash, OfflineMaintenanceOperationId};

use crate::auth_adapters::{ServerCredentialAuthenticator, ServerCurrentPolicyPort};
use crate::clocks::ProductionWallClocks;
use crate::config::ServerConfig;
use crate::identifiers::ProductionIdentifierSources;
use crate::lifecycle::{LifecycleInstallError, ProductionLifecycleRoute};
use crate::maintenance_adapter::MaintenanceController;
use crate::port_driver::{
    BlockingPortDriver, BlockingPortDriverShutdownError, BlockingPortDriverStartError,
};
use crate::process_graph::ProductionDigestKeys;
use crate::runtime_support::{
    ProductionObservabilityDiagnostics, ProductionObservabilityHealthHooks, RuntimeStopReason,
    RuntimeSupportError, SupervisedServiceJobSpawner, TokioRequestDeadlineScheduler,
};
use crate::startup::{CheckedRedbStartup, ValidatedAllocatorCapacity, ValidatedStartupLifecycle};
use crate::storage::SharedRedbOperationalPorts;

/// Owning guard for one exact retry's authentication, policy, and service jobs.
///
/// This host intentionally has no normal application service, command
/// coordinator, projection worker, outbox worker, MCP capability, server
/// generation, backup-create method, receipt-observation method, or Health
/// route.
#[must_use = "the restore-retry host must be explicitly shut down and joined"]
pub(crate) struct RunningRestoreRetryHost {
    lifecycle: Arc<ProductionLifecycleRoute>,
    spawner: SupervisedServiceJobSpawner,
    blocking: Option<BlockingPortDriver>,
}

impl RunningRestoreRetryHost {
    /// Composes and publishes only the exact receipt-frozen restore retry.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn start(
        startup: CheckedRedbStartup,
        operation_id: OfflineMaintenanceOperationId,
        input_hash: OfflineMaintenanceInputHash,
        digest_keys: &ProductionDigestKeys,
        config: &ServerConfig,
        environment: &Environment,
        clocks: &ProductionWallClocks,
        controller: MaintenanceController,
        lifecycle: Arc<ProductionLifecycleRoute>,
        identifiers: &ProductionIdentifierSources,
    ) -> Result<Self, RestoreRetryHostStartError> {
        if startup.lifecycle() != ValidatedStartupLifecycle::ActiveContract
            || startup.allocator_capacity() != ValidatedAllocatorCapacity::Available
        {
            lifecycle.stop();
            return Err(RestoreRetryHostStartError::InvalidStartup);
        }
        let mut audiences = vec![config.audience().clone()];
        audiences.extend(config.mcp_audience().cloned());
        let trusted_audiences = TrustedAudienceCatalog::new(audiences)
            .map_err(RestoreRetryHostStartError::TrustedAudience)?;
        let runtime = lifecycle.runtime_routing();
        let spawner = SupervisedServiceJobSpawner::from_current_runtime(runtime.clone()).map_err(
            |source| {
                lifecycle.stop();
                RestoreRetryHostStartError::Runtime(source)
            },
        )?;
        let incident_ids: Arc<dyn IncidentIdSource> = Arc::new(identifiers.incident_ids());
        let observability = Arc::new(
            Observability::new(Arc::clone(&incident_ids), MAX_TRACE_RECORDS).map_err(|source| {
                lifecycle.stop();
                RestoreRetryHostStartError::Observability(source)
            })?,
        );
        let diagnostics: Arc<dyn ServiceDiagnostics> = Arc::new(
            ProductionObservabilityDiagnostics::new(runtime.clone(), Arc::clone(&observability)),
        );
        let health: Arc<dyn ServiceHealthHooks> = Arc::new(
            ProductionObservabilityHealthHooks::new(runtime.clone(), observability.clone()),
        );
        let blocking = BlockingPortDriver::new(runtime).map_err(|source| {
            lifecycle.stop();
            RestoreRetryHostStartError::BlockingDriver(source)
        })?;

        let (
            database_id,
            _retained_metadata,
            _catalog_history,
            _startup_lifecycle,
            _allocator_capacity,
            operational_ports,
        ) = startup.into_parts();
        let storage = SharedRedbOperationalPorts::new(operational_ports, None)
            .map_err(|_| RestoreRetryHostStartError::InvalidStartup)?;
        let authentication_telemetry: Arc<dyn AuthenticationTelemetry> = observability.clone();
        let authorization_telemetry: Arc<dyn AuthorizationTelemetry> = observability;
        let capability_keys = digest_keys.shared_capability();
        let authenticator: Arc<dyn CredentialAuthenticator> =
            Arc::new(ServerCredentialAuthenticator::new(
                storage.clone(),
                Arc::clone(&capability_keys),
                clocks.authentication(),
                authentication_telemetry,
            ));
        let policy: Arc<dyn CurrentPolicyPort> = Arc::new(ServerCurrentPolicyPort::new(
            storage,
            clocks.authorization(),
            database_id,
            environment.clone(),
            trusted_audiences,
            authorization_telemetry,
        ));
        let coordinator = controller.restore_retry_coordinator(&blocking, operation_id, input_hash);
        let service_spawner: Arc<dyn ServiceJobSpawner> = Arc::new(spawner.clone());
        let deadline_scheduler: Arc<dyn RequestDeadlineScheduler> =
            Arc::new(TokioRequestDeadlineScheduler);
        let service: Arc<dyn RestoreRetryOfflineMaintenanceApplication> =
            Arc::new(RestoreRetryOfflineMaintenanceService::new(
                operation_id,
                input_hash,
                database_id,
                environment.clone(),
                policy,
                coordinator,
                incident_ids,
                diagnostics,
                health,
                service_spawner,
                deadline_scheduler,
            ));
        let security = CheckedGrpcRestoreRetrySecurityContext::new(
            authenticator,
            AuthenticationContext::new(database_id, environment.clone(), config.audience().clone()),
        );

        if let Err(source) =
            lifecycle.install_restore_retry(operation_id, input_hash, service, security)
        {
            lifecycle.stop();
            let cleanup = blocking.shutdown_and_drain().err();
            return Err(RestoreRetryHostStartError::Install { source, cleanup });
        }

        Ok(Self {
            lifecycle,
            spawner,
            blocking: Some(blocking),
        })
    }

    /// Closes exact retry admission before transport drain begins.
    pub(crate) fn begin_transport_shutdown(&self) {
        self.lifecycle.stop();
    }

    /// Drains accepted service jobs and then joins the blocking worker set.
    pub(crate) async fn shutdown(mut self) -> Result<(), RestoreRetryHostShutdownError> {
        self.lifecycle.stop();
        self.spawner.wait_for_idle().await;
        let runtime = self.spawner.routing().stop_reason();
        let blocking = self
            .blocking
            .take()
            .expect("a running restore-retry host retains one blocking driver")
            .shutdown_and_drain()
            .err();
        if runtime.is_none() && blocking.is_none() {
            Ok(())
        } else {
            Err(RestoreRetryHostShutdownError { runtime, blocking })
        }
    }
}

impl fmt::Debug for RunningRestoreRetryHost {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RunningRestoreRetryHost([EXACT_RESTORE_CAPABILITIES])")
    }
}

/// Closed failure to compose or publish the current-database retry host.
pub(crate) enum RestoreRetryHostStartError {
    InvalidStartup,
    TrustedAudience(CapabilityMutationFactsError),
    Runtime(RuntimeSupportError),
    Observability(ObservabilityBuildError),
    BlockingDriver(BlockingPortDriverStartError),
    Install {
        source: LifecycleInstallError,
        cleanup: Option<BlockingPortDriverShutdownError>,
    },
}

impl fmt::Debug for RestoreRetryHostStartError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RestoreRetryHostStartError([REDACTED])")
    }
}

impl fmt::Display for RestoreRetryHostStartError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if matches!(
            self,
            Self::Install {
                cleanup: Some(_),
                ..
            }
        ) {
            formatter.write_str("the restore-retry host could not be started or cleaned up")
        } else {
            formatter.write_str("the restore-retry host could not be started")
        }
    }
}

impl Error for RestoreRetryHostStartError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::TrustedAudience(source) => Some(source),
            Self::Runtime(source) => Some(source),
            Self::Observability(source) => Some(source),
            Self::BlockingDriver(source) => Some(source),
            Self::Install { source, .. } => Some(source),
            Self::InvalidStartup => None,
        }
    }
}

/// Aggregate evidence that retry-host shutdown did not complete cleanly.
#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) struct RestoreRetryHostShutdownError {
    runtime: Option<RuntimeStopReason>,
    blocking: Option<BlockingPortDriverShutdownError>,
}

impl fmt::Debug for RestoreRetryHostShutdownError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RestoreRetryHostShutdownError([REDACTED])")
    }
}

impl fmt::Display for RestoreRetryHostShutdownError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let _failed_stages =
            usize::from(self.runtime.is_some()) + usize::from(self.blocking.is_some());
        formatter.write_str("the restore-retry host did not shut down cleanly")
    }
}

impl Error for RestoreRetryHostShutdownError {}

#[cfg(test)]
mod tests {
    use super::*;

    const SOURCE: &str = include_str!("restore_retry_host.rs");

    fn production_source() -> &'static str {
        SOURCE
            .split_once("#[cfg(test)]")
            .expect("retry host has an architecture-test boundary")
            .0
    }

    #[test]
    fn composition_has_only_current_restore_retry_authority() {
        let source = production_source();
        for forbidden in [
            "dyn ApplicationService",
            "ServiceExecutors",
            "RunningCommandCoordinator",
            "RunningProjectionWorker",
            "recover_outbox",
            "HostedMcp",
            "ProductionGraphBuilder",
            "CreateOfflineBackupRequest",
            "GetOfflineMaintenanceOperationRequest",
            "ServerCapabilityTokenIssuer",
            "ServerGeneration",
        ] {
            assert!(
                !source.contains(forbidden),
                "restore-retry host gained forbidden authority {forbidden}"
            );
        }
        assert_eq!(source.matches(".restore_retry_coordinator(").count(), 1);
        assert_eq!(source.matches(".install_restore_retry(").count(), 1);
        assert!(source.contains("CheckedGrpcRestoreRetrySecurityContext"));
        assert!(!source.contains("CheckedGrpcSecurityContext"));
    }

    #[test]
    fn shutdown_closes_route_then_drains_jobs_then_blocking_ports() {
        let body = production_source()
            .split_once("pub(crate) async fn shutdown")
            .expect("shutdown method")
            .1;
        let route = body.find("self.lifecycle.stop()").expect("route closure");
        let jobs = body
            .find("wait_for_idle().await")
            .expect("service-job drain");
        let ports = body
            .find(".shutdown_and_drain()")
            .expect("blocking-port drain");
        assert!(route < jobs);
        assert!(jobs < ports);
    }

    #[test]
    fn composition_failures_are_redacted() {
        let error = RestoreRetryHostStartError::InvalidStartup;
        assert_eq!(
            format!("{error:?}"),
            "RestoreRetryHostStartError([REDACTED])"
        );
    }
}
