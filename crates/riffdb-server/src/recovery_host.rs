//! Least-authority service composition for staged-only restore recovery.

// The daemon consumes this owner only when ordinary startup cannot establish a
// trustworthy current database.
#![allow(dead_code)]

use std::error::Error;
use std::fmt;
use std::sync::Arc;

use riffdb_errors::IncidentIdSource;
use riffdb_observability::{MAX_TRACE_RECORDS, Observability, ObservabilityBuildError};
use riffdb_service::{
    RecoveryOfflineMaintenanceApplication, RecoveryOfflineMaintenanceService,
    RequestDeadlineScheduler, ServiceDiagnostics, ServiceHealthHooks, ServiceJobSpawner,
};

use crate::identifiers::ProductionIdentifierSources;
use crate::lifecycle::{LifecycleInstallError, ProductionLifecycleRoute};
use crate::maintenance_adapter::MaintenanceController;
use crate::port_driver::{
    BlockingPortDriver, BlockingPortDriverShutdownError, BlockingPortDriverStartError,
};
use crate::runtime_support::{
    ProductionObservabilityDiagnostics, ProductionObservabilityHealthHooks, RuntimeStopReason,
    RuntimeSupportError, SupervisedServiceJobSpawner, TokioRequestDeadlineScheduler,
};

/// Owning guard for the restore-only service jobs and blocking workers.
///
/// This guard intentionally retains no normal application service, current
/// policy, authoritative database port, command graph, or MCP capability.
#[must_use = "the recovery host must be explicitly shut down and joined"]
pub(crate) struct RunningRecoveryHost {
    lifecycle: Arc<ProductionLifecycleRoute>,
    spawner: SupervisedServiceJobSpawner,
    blocking: Option<BlockingPortDriver>,
}

impl RunningRecoveryHost {
    /// Composes and publishes the sole staged-only restore capability.
    ///
    /// The lifecycle must already be in its failed-closed recovery stage.
    /// Construction uses the route's runtime state so any diagnostics,
    /// supervision, or blocking-worker defect closes the same admission path.
    pub(crate) fn start(
        controller: MaintenanceController,
        lifecycle: Arc<ProductionLifecycleRoute>,
        identifiers: &ProductionIdentifierSources,
    ) -> Result<Self, RecoveryHostStartError> {
        let runtime = lifecycle.runtime_routing();
        let spawner = SupervisedServiceJobSpawner::from_current_runtime(runtime.clone()).map_err(
            |source| {
                lifecycle.stop();
                RecoveryHostStartError::Runtime(source)
            },
        )?;
        let incident_ids: Arc<dyn IncidentIdSource> = Arc::new(identifiers.incident_ids());
        let observability = Arc::new(
            Observability::new(Arc::clone(&incident_ids), MAX_TRACE_RECORDS).map_err(|source| {
                lifecycle.stop();
                RecoveryHostStartError::Observability(source)
            })?,
        );
        let diagnostics: Arc<dyn ServiceDiagnostics> = Arc::new(
            ProductionObservabilityDiagnostics::new(runtime.clone(), Arc::clone(&observability)),
        );
        let health: Arc<dyn ServiceHealthHooks> = Arc::new(
            ProductionObservabilityHealthHooks::new(runtime.clone(), observability),
        );
        let blocking = BlockingPortDriver::new(runtime).map_err(|source| {
            lifecycle.stop();
            RecoveryHostStartError::BlockingDriver(source)
        })?;
        let coordinator = controller.recovery_coordinator(&blocking);
        let service_spawner: Arc<dyn ServiceJobSpawner> = Arc::new(spawner.clone());
        let deadline_scheduler: Arc<dyn RequestDeadlineScheduler> =
            Arc::new(TokioRequestDeadlineScheduler);
        let service: Arc<dyn RecoveryOfflineMaintenanceApplication> =
            Arc::new(RecoveryOfflineMaintenanceService::new(
                coordinator,
                incident_ids,
                diagnostics,
                health,
                service_spawner,
                deadline_scheduler,
            ));

        if let Err(source) = lifecycle.install_recovery(service) {
            lifecycle.stop();
            let cleanup = blocking.shutdown_and_drain().err();
            return Err(RecoveryHostStartError::Install { source, cleanup });
        }

        Ok(Self {
            lifecycle,
            spawner,
            blocking: Some(blocking),
        })
    }

    /// Closes lifecycle admission before the transport begins its own drain.
    pub(crate) fn begin_transport_shutdown(&self) {
        self.lifecycle.stop();
    }

    /// Drains accepted service jobs and then joins the blocking worker set.
    ///
    /// The caller must first stop and drain the gRPC transport so no handler
    /// still holding an admitted service clone can start work after the idle
    /// observation.
    pub(crate) async fn shutdown(mut self) -> Result<(), RecoveryHostShutdownError> {
        self.lifecycle.stop();
        self.spawner.wait_for_idle().await;
        let runtime = self.spawner.routing().stop_reason();
        let blocking = self
            .blocking
            .take()
            .expect("a running recovery host retains one blocking driver")
            .shutdown_and_drain()
            .err();
        if runtime.is_none() && blocking.is_none() {
            Ok(())
        } else {
            Err(RecoveryHostShutdownError { runtime, blocking })
        }
    }
}

impl fmt::Debug for RunningRecoveryHost {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RunningRecoveryHost([RESTORE_ONLY_CAPABILITIES])")
    }
}

/// Closed failure to compose or publish the recovery-only service.
pub(crate) enum RecoveryHostStartError {
    Runtime(RuntimeSupportError),
    Observability(ObservabilityBuildError),
    BlockingDriver(BlockingPortDriverStartError),
    Install {
        source: LifecycleInstallError,
        cleanup: Option<BlockingPortDriverShutdownError>,
    },
}

impl fmt::Debug for RecoveryHostStartError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RecoveryHostStartError([REDACTED])")
    }
}

impl fmt::Display for RecoveryHostStartError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if matches!(
            self,
            Self::Install {
                cleanup: Some(_),
                ..
            }
        ) {
            formatter.write_str("the recovery host could not be started or cleaned up")
        } else {
            formatter.write_str("the recovery host could not be started")
        }
    }
}

impl Error for RecoveryHostStartError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Runtime(source) => Some(source),
            Self::Observability(source) => Some(source),
            Self::BlockingDriver(source) => Some(source),
            Self::Install { source, .. } => Some(source),
        }
    }
}

/// Aggregate evidence that recovery-host shutdown did not complete cleanly.
#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) struct RecoveryHostShutdownError {
    runtime: Option<RuntimeStopReason>,
    blocking: Option<BlockingPortDriverShutdownError>,
}

impl fmt::Debug for RecoveryHostShutdownError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RecoveryHostShutdownError([REDACTED])")
    }
}

impl fmt::Display for RecoveryHostShutdownError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let _failed_stages =
            usize::from(self.runtime.is_some()) + usize::from(self.blocking.is_some());
        formatter.write_str("the recovery host did not shut down cleanly")
    }
}

impl Error for RecoveryHostShutdownError {}

#[cfg(test)]
mod tests {
    use riffdb_observability::ObservabilityBuildError;

    use super::*;

    const SOURCE: &str = include_str!("recovery_host.rs");

    fn production_source() -> &'static str {
        SOURCE
            .split_once("#[cfg(test)]")
            .expect("recovery host has an architecture-test boundary")
            .0
    }

    #[test]
    fn recovery_composition_has_no_normal_graph_or_transport_authority() {
        let source = production_source();
        for forbidden in [
            "dyn ApplicationService",
            "CurrentPolicyPort",
            "CredentialAuthenticator",
            "SharedRedbOperationalPorts",
            "RedbMaintenanceStorage",
            "RunningProductionGraph",
            "ProductionGraphBuilder",
            "HostedMcp",
        ] {
            assert!(
                !source.contains(forbidden),
                "recovery host gained forbidden authority {forbidden}"
            );
        }
        assert_eq!(
            source.matches(".recovery_coordinator(&blocking)").count(),
            1
        );
        assert_eq!(source.matches(".install_recovery(service)").count(), 1);
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
        let error = RecoveryHostStartError::Observability(ObservabilityBuildError::TraceCapacity);
        assert_eq!(format!("{error:?}"), "RecoveryHostStartError([REDACTED])");
    }
}
