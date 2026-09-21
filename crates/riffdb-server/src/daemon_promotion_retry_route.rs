//! Authentication and promotion only; no ordinary graph or derived workers.
use super::*;
use riffdb_api_grpc::{
    CheckedGrpcRestoreRetrySecurityContext, CheckedGrpcSecurityContext, GrpcBootstrapCompletion,
    GrpcDeploymentCompletion, GrpcOfflineMaintenanceOperation,
};
use riffdb_auth::AuthenticationContext;
use riffdb_observability::{MAX_TRACE_RECORDS, Observability};
use riffdb_service::{
    ApplicationService, FollowerPromotionApplication, FollowerPromotionService, HealthRequest,
    HealthResult, RecoveryOfflineMaintenanceApplication, RestoreRetryOfflineMaintenanceApplication,
    ServiceFuture,
};
use riffdb_types::{
    OfflineMaintenanceInputHash, OfflineMaintenanceOperationId, ServiceOperationV1,
};
use std::sync::Mutex;

type Admission = (
    Arc<dyn FollowerPromotionApplication>,
    CheckedGrpcSecurityContext,
);

pub(super) struct RetryRoute {
    admission: Mutex<Option<Admission>>,
}

impl RetryRoute {
    pub(super) fn new(
        snapshot: riffdb_storage_redb::RedbOwnedSnapshot,
        config: &ServerConfig,
        database: &DatabaseConfig,
        keys: &ProductionDigestKeys,
        clocks: &ProductionWallClocks,
        controller: Arc<crate::promotion_admission::PromotionController>,
    ) -> Result<Self, DaemonError> {
        let source = database.follower().ok_or(DaemonError::MaintenanceDriver)?;
        let database_id = source.lineage.database_id();
        let ids = ProductionIdentifierSources::new();
        let observability = Arc::new(
            Observability::new(Arc::new(ids.incident_ids()), MAX_TRACE_RECORDS)
                .map_err(|_| DaemonError::MaintenanceDriver)?,
        );
        let capability_keys = keys.shared_capability();
        let authenticator = Arc::new(crate::auth_adapters::ServerCredentialAuthenticator::new(
            snapshot.clone(),
            capability_keys.clone(),
            clocks.authentication(),
            observability.clone(),
        ));
        let policy = Arc::new(crate::auth_adapters::ServerCurrentPolicyPort::new(
            snapshot,
            clocks.authorization(),
            database_id,
            database.environment().clone(),
            TrustedAudienceCatalog::new(vec![config.audience().clone()])
                .map_err(|_| DaemonError::MaintenanceDriver)?,
            observability,
        ));
        let security = CheckedGrpcSecurityContext::new(
            authenticator,
            AuthenticationContext::new(
                database_id,
                database.environment().clone(),
                config.audience().clone(),
            ),
            capability_keys,
        );
        let service = Arc::new(FollowerPromotionService::new(
            database_id,
            database.environment().clone(),
            policy,
            controller,
        ));
        Ok(Self {
            admission: Mutex::new(Some((service, security))),
        })
    }

    pub(super) fn close(&self) {
        if let Ok(mut admission) = self.admission.lock() {
            admission.take();
        }
    }
}

impl GrpcLifecycleRoute for RetryRoute {
    fn admit_follower_promotion(&self) -> Option<Arc<dyn FollowerPromotionApplication>> {
        self.admission
            .lock()
            .ok()?
            .as_ref()
            .map(|value| value.0.clone())
    }
    fn security_context(&self) -> Option<CheckedGrpcSecurityContext> {
        self.admission
            .lock()
            .ok()?
            .as_ref()
            .map(|value| value.1.clone())
    }
    fn admit_authenticated(&self, _: ServiceOperationV1) -> Option<Arc<dyn ApplicationService>> {
        None
    }
    fn admit_offline_maintenance(
        &self,
        _: GrpcOfflineMaintenanceOperation,
    ) -> Option<Arc<dyn ApplicationService>> {
        None
    }
    fn admit_restore_retry(
        &self,
        _: OfflineMaintenanceOperationId,
        _: OfflineMaintenanceInputHash,
    ) -> Option<Arc<dyn RestoreRetryOfflineMaintenanceApplication>> {
        None
    }
    fn admit_recovery_restore(
        &self,
        _: OfflineMaintenanceOperationId,
        _: OfflineMaintenanceInputHash,
    ) -> Option<Arc<dyn RecoveryOfflineMaintenanceApplication>> {
        None
    }
    fn restore_retry_security_context(&self) -> Option<CheckedGrpcRestoreRetrySecurityContext> {
        None
    }
    fn server_generation(&self) -> Option<[u8; 16]> {
        None
    }
    fn history_incarnation(&self) -> Option<u64> {
        None
    }
    fn restricted_health(&self, _: HealthRequest) -> Option<ServiceFuture<'_, HealthResult>> {
        None
    }
    fn bootstrap_available(&self) -> bool {
        false
    }
    fn begin_bootstrap(&self) -> Option<Arc<dyn ApplicationService>> {
        None
    }
    fn finish_bootstrap(&self, _: GrpcBootstrapCompletion) {}
    fn finish_deployment(&self, _: GrpcDeploymentCompletion) {}
}
