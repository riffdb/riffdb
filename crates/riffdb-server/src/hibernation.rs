//! Scale-to-zero idle database hibernation and single-flight wake-on-request routing.

use std::fmt;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use riffdb_api_grpc::{
    CheckedGrpcRestoreRetrySecurityContext, CheckedGrpcSecurityContext,
    GrpcApplicationExportOperation, GrpcApplicationReimportOperation, GrpcBootstrapCompletion,
    GrpcContractMigrationOperation, GrpcDeploymentCompletion, GrpcLifecycleRoute,
    GrpcOfflineMaintenanceOperation,
};
use riffdb_auth::{
    AuthenticatedPrincipal, AuthenticationContext, AuthenticationFailure, CredentialAuthenticator,
    OpaqueCredential,
};
use riffdb_service::{
    ApplicationExportApplication, ApplicationReimportApplication, ApplicationService,
    ContractMigrationApplication, FollowerPromotionApplication, HealthRequest, HealthResult,
    RecoveryOfflineMaintenanceApplication, ReplicationApplication,
    RestoreRetryOfflineMaintenanceApplication, RiffDbService, ServiceFuture, ServiceTelemetry,
};
use riffdb_types::{
    OfflineMaintenanceInputHash, OfflineMaintenanceOperationId, ServiceOperationV1, Timestamp,
};
use tokio::task::JoinHandle as TokioJoinHandle;

use crate::config::{DatabaseConfig, ServerConfig};
use crate::daemon::{
    DaemonError, GenerationInputs, MultiDatabaseEvent, MultiDatabaseGraph, build_info,
    spawn_multi_runtime_monitor,
};
use crate::lifecycle::ProductionLifecycleRoute;
use crate::lifecycle_service::LifecycleApplicationService;
use crate::maintenance_adapter::MaintenanceController;
use crate::maintenance_lifecycle::MaintenanceLifecycle;
use crate::process_graph::{HostedMcpDependencies, ProductionGraphBuilder, RunningProductionGraph};
use crate::runtime_support::RuntimeRoutingState;
use crate::server_generation::SERVER_GENERATION_BYTES;
use crate::startup::open_redb_startup_with_extent_profile;

/// Active per-generation transport handles held while a database is awake.
#[derive(Clone)]
pub(crate) struct ActiveGenerationRoutes {
    pub(crate) lifecycle: Arc<ProductionLifecycleRoute>,
    pub(crate) hosted_authenticator: Arc<dyn CredentialAuthenticator>,
}

/// Newly opened database generation produced by a single-flight wake-on-request.
pub(crate) struct AwakenedDatabaseGeneration {
    pub(crate) graph: RunningProductionGraph,
    pub(crate) lifecycle: Arc<ProductionLifecycleRoute>,
    pub(crate) routing: RuntimeRoutingState,
}

pub(crate) type WakeDatabaseFn =
    Arc<dyn Fn() -> Result<(ActiveGenerationRoutes, AwakenedDatabaseGeneration), ()> + Send + Sync>;

pub(crate) type HibernationClock = Arc<dyn Fn() -> Instant + Send + Sync>;

enum HibernationSlot {
    Active(ActiveGenerationRoutes),
    Hibernating,
    Hibernated,
    Waking,
    Stopped,
}

struct HibernationState {
    slot: HibernationSlot,
    last_activity: Instant,
    wake_epoch: u64,
    waking_waiters: usize,
}

/// Transport-facing route wrapper that hibernates idle databases and wakes them
/// transparently in a single flight on the next gRPC or MCP request.
pub(crate) struct HibernatableDatabaseRoute {
    state: Mutex<HibernationState>,
    wake_condvar: Condvar,
    clock: HibernationClock,
    runtime_handle: tokio::runtime::Handle,
    wake_fn: WakeDatabaseFn,
    awakened_generation: Mutex<Option<AwakenedDatabaseGeneration>>,
}

struct WakeResetGuard<'a> {
    route: &'a HibernatableDatabaseRoute,
    armed: bool,
}

impl Drop for WakeResetGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            let mut state = self.route.lock_state();
            if matches!(state.slot, HibernationSlot::Waking) {
                state.wake_epoch = state.wake_epoch.wrapping_add(1);
                state.slot = HibernationSlot::Hibernated;
                self.route.wake_condvar.notify_all();
            }
        }
    }
}

impl HibernatableDatabaseRoute {
    pub(crate) fn new(
        active: ActiveGenerationRoutes,
        clock: HibernationClock,
        runtime_handle: tokio::runtime::Handle,
        wake_fn: WakeDatabaseFn,
    ) -> Arc<Self> {
        let now = (clock)();
        Arc::new(Self {
            state: Mutex::new(HibernationState {
                slot: HibernationSlot::Active(active),
                last_activity: now,
                wake_epoch: 0,
                waking_waiters: 0,
            }),
            wake_condvar: Condvar::new(),
            clock,
            runtime_handle,
            wake_fn,
            awakened_generation: Mutex::new(None),
        })
    }

    #[cfg(test)]
    pub(crate) fn is_hibernated(&self) -> bool {
        matches!(self.lock_state().slot, HibernationSlot::Hibernated)
    }

    #[cfg(test)]
    pub(crate) fn wait_for_waking_waiters(&self, expected: usize) {
        let mut state = self.lock_state();
        while state.waking_waiters < expected {
            state = self.wait_condvar(state);
        }
    }

    /// Transitions an active generation into `Hibernating` and drops its route
    /// references if `idle_timeout` has elapsed since `last_activity`.
    pub(crate) fn try_begin_hibernation(&self, idle_timeout: Duration) -> bool {
        let mut state = self.lock_state();
        if !matches!(state.slot, HibernationSlot::Active(_)) {
            return false;
        }
        let now = (self.clock)();
        if now.saturating_duration_since(state.last_activity) < idle_timeout {
            return false;
        }
        state.wake_epoch = state.wake_epoch.wrapping_add(1);
        state.slot = HibernationSlot::Hibernating;
        true
    }

    /// Completes the transition from `Hibernating` to `Hibernated` once the
    /// underlying `RunningProductionGraph` has drained and released its file lock.
    pub(crate) fn finish_hibernation(&self) {
        let mut state = self.lock_state();
        if matches!(state.slot, HibernationSlot::Hibernating) {
            state.wake_epoch = state.wake_epoch.wrapping_add(1);
            state.slot = HibernationSlot::Hibernated;
            self.wake_condvar.notify_all();
        }
    }

    /// Stops admission and releases any active route references without waking
    /// a hibernated database.
    pub(crate) fn stop(&self) {
        let mut state = self.lock_state();
        while matches!(
            state.slot,
            HibernationSlot::Waking | HibernationSlot::Hibernating
        ) {
            state = self.wait_condvar(state);
        }
        state.wake_epoch = state.wake_epoch.wrapping_add(1);
        state.slot = HibernationSlot::Stopped;
        self.wake_condvar.notify_all();
    }

    /// Installs a freshly constructed active generation after offline maintenance.
    pub(crate) fn install_active(&self, active: ActiveGenerationRoutes) {
        let mut state = self.lock_state();
        state.last_activity = (self.clock)();
        state.wake_epoch = state.wake_epoch.wrapping_add(1);
        state.slot = HibernationSlot::Active(active);
        self.wake_condvar.notify_all();
    }

    /// Takes any generation awakened by an on-demand request so the supervisor
    /// can monitor and manage its lifecycle.
    pub(crate) fn take_awakened_generation(&self) -> Option<AwakenedDatabaseGeneration> {
        self.awakened_generation
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }

    /// Wraps `HostedMcpDependencies` so MCP authentication and service calls
    /// route through this hibernatable wrapper without retaining direct
    /// references to a single generation's storage ports.
    pub(crate) fn wrap_hosted_mcp_dependencies(
        self: &Arc<Self>,
        mut dependencies: HostedMcpDependencies,
    ) -> HostedMcpDependencies {
        let route: Arc<dyn GrpcLifecycleRoute> = self.clone();
        let authenticator: Arc<dyn CredentialAuthenticator> = self.clone();
        dependencies.authenticator = authenticator;
        dependencies.service = Arc::new(LifecycleApplicationService::new(route));
        dependencies
    }

    fn ensure_active(&self) -> Option<ActiveGenerationRoutes> {
        let mut state = self.lock_state();
        loop {
            match &state.slot {
                HibernationSlot::Active(active) => {
                    let routes = active.clone();
                    state.last_activity = (self.clock)();
                    return Some(routes);
                }
                HibernationSlot::Stopped => return None,
                HibernationSlot::Hibernating => {
                    state = self.wait_condvar(state);
                }
                HibernationSlot::Waking => {
                    let observed_epoch = state.wake_epoch;
                    state.waking_waiters = state.waking_waiters.saturating_add(1);
                    self.wake_condvar.notify_all();
                    while matches!(state.slot, HibernationSlot::Waking)
                        && state.wake_epoch == observed_epoch
                    {
                        state = self.wait_condvar(state);
                    }
                    state.waking_waiters = state.waking_waiters.saturating_sub(1);
                    if let HibernationSlot::Active(active) = &state.slot {
                        let routes = active.clone();
                        state.last_activity = (self.clock)();
                        return Some(routes);
                    }
                    return None;
                }
                HibernationSlot::Hibernated => {
                    state.slot = HibernationSlot::Waking;
                    drop(state);
                    let mut reset_guard = WakeResetGuard {
                        route: self,
                        armed: true,
                    };
                    let wake_result = run_blocking_in_place(|| {
                        let _enter = self.runtime_handle.enter();
                        (self.wake_fn)()
                    });
                    match wake_result {
                        Ok((active, awakened)) => {
                            *self
                                .awakened_generation
                                .lock()
                                .unwrap_or_else(PoisonError::into_inner) = Some(awakened);
                            let mut state = self.lock_state();
                            state.last_activity = (self.clock)();
                            state.wake_epoch = state.wake_epoch.wrapping_add(1);
                            state.slot = HibernationSlot::Active(active.clone());
                            reset_guard.armed = false;
                            self.wake_condvar.notify_all();
                            return Some(active);
                        }
                        Err(()) => {
                            let mut state = self.lock_state();
                            state.wake_epoch = state.wake_epoch.wrapping_add(1);
                            state.slot = HibernationSlot::Hibernated;
                            reset_guard.armed = false;
                            self.wake_condvar.notify_all();
                            return None;
                        }
                    }
                }
            }
        }
    }

    fn lock_state(&self) -> MutexGuard<'_, HibernationState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn wait_condvar<'a>(
        &'a self,
        guard: MutexGuard<'a, HibernationState>,
    ) -> MutexGuard<'a, HibernationState> {
        run_blocking_in_place(|| {
            self.wake_condvar
                .wait(guard)
                .unwrap_or_else(PoisonError::into_inner)
        })
    }
}

impl fmt::Debug for HibernatableDatabaseRoute {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("HibernatableDatabaseRoute([CAPABILITY])")
    }
}

impl CredentialAuthenticator for HibernatableDatabaseRoute {
    fn authenticate(
        &self,
        credential: OpaqueCredential<'_>,
        context: &AuthenticationContext,
    ) -> Result<AuthenticatedPrincipal, AuthenticationFailure> {
        let active = self
            .ensure_active()
            .ok_or(AuthenticationFailure::Internal)?;
        active
            .hosted_authenticator
            .authenticate(credential, context)
    }
}

impl GrpcLifecycleRoute for HibernatableDatabaseRoute {
    fn admit_follower_promotion(&self) -> Option<Arc<dyn FollowerPromotionApplication>> {
        self.ensure_active()?.lifecycle.admit_follower_promotion()
    }

    fn admit_replication(&self) -> Option<Arc<dyn ReplicationApplication>> {
        self.ensure_active()?.lifecycle.admit_replication()
    }

    fn admit_authenticated(
        &self,
        operation: ServiceOperationV1,
    ) -> Option<Arc<dyn ApplicationService>> {
        self.ensure_active()?
            .lifecycle
            .admit_authenticated(operation)
    }

    fn admit_offline_maintenance(
        &self,
        operation: GrpcOfflineMaintenanceOperation,
    ) -> Option<Arc<dyn ApplicationService>> {
        self.ensure_active()?
            .lifecycle
            .admit_offline_maintenance(operation)
    }

    fn admit_contract_migration(
        &self,
        operation: GrpcContractMigrationOperation,
    ) -> Option<Arc<dyn ContractMigrationApplication>> {
        self.ensure_active()?
            .lifecycle
            .admit_contract_migration(operation)
    }

    fn admit_application_export(
        &self,
        operation: GrpcApplicationExportOperation,
    ) -> Option<Arc<dyn ApplicationExportApplication>> {
        self.ensure_active()?
            .lifecycle
            .admit_application_export(operation)
    }

    fn admit_application_reimport(
        &self,
        operation: GrpcApplicationReimportOperation,
    ) -> Option<Arc<dyn ApplicationReimportApplication>> {
        self.ensure_active()?
            .lifecycle
            .admit_application_reimport(operation)
    }

    fn admit_restore_retry(
        &self,
        operation_id: OfflineMaintenanceOperationId,
        input_hash: OfflineMaintenanceInputHash,
    ) -> Option<Arc<dyn RestoreRetryOfflineMaintenanceApplication>> {
        self.ensure_active()?
            .lifecycle
            .admit_restore_retry(operation_id, input_hash)
    }

    fn admit_recovery_restore(
        &self,
        operation_id: OfflineMaintenanceOperationId,
        input_hash: OfflineMaintenanceInputHash,
    ) -> Option<Arc<dyn RecoveryOfflineMaintenanceApplication>> {
        self.ensure_active()?
            .lifecycle
            .admit_recovery_restore(operation_id, input_hash)
    }

    fn security_context(&self) -> Option<CheckedGrpcSecurityContext> {
        self.ensure_active()?.lifecycle.security_context()
    }

    fn restore_retry_security_context(&self) -> Option<CheckedGrpcRestoreRetrySecurityContext> {
        self.ensure_active()?
            .lifecycle
            .restore_retry_security_context()
    }

    fn server_generation(&self) -> Option<[u8; SERVER_GENERATION_BYTES]> {
        self.ensure_active()?.lifecycle.server_generation()
    }

    fn history_incarnation(&self) -> Option<u64> {
        self.ensure_active()?.lifecycle.history_incarnation()
    }

    fn read_stage_telemetry(&self) -> Option<Arc<dyn ServiceTelemetry>> {
        self.ensure_active()?.lifecycle.read_stage_telemetry()
    }

    fn restricted_health(&self, request: HealthRequest) -> Option<ServiceFuture<'_, HealthResult>> {
        self.ensure_active()?
            .lifecycle
            .restricted_health_owned(request)
    }

    fn bootstrap_available(&self) -> bool {
        self.ensure_active()
            .is_some_and(|active| active.lifecycle.bootstrap_available())
    }

    fn begin_bootstrap(&self) -> Option<Arc<dyn ApplicationService>> {
        self.ensure_active()?.lifecycle.begin_bootstrap()
    }

    fn finish_bootstrap(&self, completion: GrpcBootstrapCompletion) {
        if let Some(active) = self.ensure_active() {
            active.lifecycle.finish_bootstrap(completion);
        }
    }

    fn finish_deployment(&self, completion: GrpcDeploymentCompletion) {
        if let Some(active) = self.ensure_active() {
            active.lifecycle.finish_deployment(completion);
        }
    }
}

fn run_blocking_in_place<F, R>(f: F) -> R
where
    F: FnOnce() -> R,
{
    if tokio::runtime::Handle::try_current()
        .is_ok_and(|handle| handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread)
    {
        tokio::task::block_in_place(f)
    } else {
        f()
    }
}

/// Builds the production wake closure that re-opens and validates the `redb`
/// database and starts a fresh `RunningProductionGraph` generation on demand.
pub(crate) fn make_production_wake_fn(
    config: ServerConfig,
    database: DatabaseConfig,
    started_at: Timestamp,
    maintenance_lifecycle: Arc<MaintenanceLifecycle>,
    maintenance: MaintenanceController,
) -> WakeDatabaseFn {
    Arc::new(move || {
        wake_production_database(
            &config,
            &database,
            started_at,
            &maintenance_lifecycle,
            &maintenance,
        )
        .map_err(|_| ())
    })
}

pub(crate) fn wake_production_database(
    config: &ServerConfig,
    database: &DatabaseConfig,
    started_at: Timestamp,
    maintenance_lifecycle: &Arc<MaintenanceLifecycle>,
    maintenance: &MaintenanceController,
) -> Result<(ActiveGenerationRoutes, AwakenedDatabaseGeneration), DaemonError> {
    let prepared = GenerationInputs::load(config)?;
    let startup = open_redb_startup_with_extent_profile(
        database.database_path(),
        prepared.startup_inputs,
        &prepared.identifiers.database_ids(),
        config.redb_commit_profile(),
        database.limits().extent_profile(),
    )
    .map_err(DaemonError::Startup)?;
    let (initializing, activator, issuer) = RiffDbService::begin_initialization();
    let routing = RuntimeRoutingState::new();
    let lifecycle = Arc::new(ProductionLifecycleRoute::new_with_maintenance(
        initializing,
        issuer,
        routing.clone(),
        Arc::clone(maintenance_lifecycle),
    ));
    let build = build_info(&startup)?;
    let graph = ProductionGraphBuilder::new(
        startup,
        activator,
        prepared.digest_keys,
        config,
        database,
        started_at,
        build,
        prepared.identifiers,
        prepared.clocks,
        Arc::clone(&lifecycle),
        maintenance.clone(),
    )
    .build()
    .map_err(DaemonError::GraphBuild)?;
    let active = graph.active_generation_routes();
    Ok((
        active,
        AwakenedDatabaseGeneration {
            graph,
            lifecycle,
            routing,
        },
    ))
}

/// Transfers any on-demand awakened generation into `MultiDatabaseGraph` and
/// registers its runtime monitor with the supervisor.
pub(crate) fn sync_awakened_generation(
    database_index: usize,
    generation: &mut MultiDatabaseGraph,
    monitors: &mut Vec<TokioJoinHandle<()>>,
    event_sender: &tokio::sync::mpsc::Sender<MultiDatabaseEvent>,
) {
    let Some(hibernatable) = generation.hibernatable.as_ref() else {
        return;
    };
    if let Some(awakened) = hibernatable.take_awakened_generation() {
        generation.generation = generation.generation.saturating_add(1);
        generation.graph = Some(awakened.graph);
        generation.lifecycle = awakened.lifecycle;
        generation.routing = awakened.routing;
        monitors.push(spawn_multi_runtime_monitor(
            database_index,
            generation.generation,
            generation.routing.clone(),
            event_sender.clone(),
        ));
    }
}

/// Synchronizes any awakened generation and suspends the hibernatable wrapper
/// before offline maintenance, quarantine, or process shutdown.
pub(crate) fn prepare_for_offline_or_shutdown(generation: &mut MultiDatabaseGraph) {
    let Some(hibernatable) = generation.hibernatable.as_ref().cloned() else {
        return;
    };
    hibernatable.stop();
    if let Some(awakened) = hibernatable.take_awakened_generation() {
        generation.generation = generation.generation.saturating_add(1);
        generation.graph = Some(awakened.graph);
        generation.lifecycle = awakened.lifecycle;
        generation.routing = awakened.routing;
    }
}

/// Checks whether a database generation has been idle for its configured
/// `idle_hibernate` duration and, if so, drains its workers, commits a clean
/// close checkpoint, and drops its `RunningProductionGraph` and `redb` lock.
pub(crate) async fn try_hibernate_if_idle(
    database_index: usize,
    generation: &mut MultiDatabaseGraph,
    monitors: &mut Vec<TokioJoinHandle<()>>,
    event_sender: &tokio::sync::mpsc::Sender<MultiDatabaseEvent>,
) -> Result<bool, DaemonError> {
    sync_awakened_generation(database_index, generation, monitors, event_sender);
    let (Some(idle_timeout), Some(hibernatable)) =
        (generation.idle_hibernate, generation.hibernatable.as_ref())
    else {
        return Ok(false);
    };
    if !generation
        .maintenance_lifecycle
        .ordinary_admission_available()
    {
        return Ok(false);
    }
    let Some(running_graph) = generation.graph.as_ref() else {
        return Ok(false);
    };
    if !running_graph.is_idle_for_hibernation() {
        return Ok(false);
    }
    if !hibernatable.try_begin_hibernation(idle_timeout) {
        return Ok(false);
    }
    let hibernatable = Arc::clone(hibernatable);
    generation.generation = generation
        .generation
        .checked_add(1)
        .ok_or(DaemonError::MaintenanceDriver)?;
    generation.lifecycle.stop();
    let (offline_service, _offline_activator, offline_issuer) =
        RiffDbService::begin_initialization();
    let offline_lifecycle = Arc::new(ProductionLifecycleRoute::new_with_maintenance(
        offline_service,
        offline_issuer,
        RuntimeRoutingState::new(),
        Arc::clone(&generation.maintenance_lifecycle),
    ));
    generation.lifecycle = offline_lifecycle;
    let Some(graph) = generation.graph.take() else {
        hibernatable.finish_hibernation();
        return Ok(false);
    };
    let notify_result = graph.begin_transport_shutdown();
    let shutdown_result = graph.shutdown().await;
    hibernatable.finish_hibernation();
    notify_result.map_err(|_| DaemonError::NotificationShutdown)?;
    shutdown_result.map_err(DaemonError::GraphShutdown)?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::maintenance_adapter::{
        MigrationProcessExclusion, maintenance_trigger_channel, shared_maintenance_storage,
    };
    use crate::real_storage_support::temporary_database_scope;
    use riffdb_storage_redb::RedbMaintenanceStorage;
    use riffdb_types::DatabaseAlias;
    use std::ffi::OsString;
    use std::fs;
    use std::sync::Barrier;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    struct ManualClock {
        base: Instant,
        offset_ms: AtomicU64,
    }

    impl ManualClock {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                base: Instant::now(),
                offset_ms: AtomicU64::new(0),
            })
        }

        fn advance(&self, duration: Duration) {
            let ms = u64::try_from(duration.as_millis()).expect("duration fits in u64 ms");
            self.offset_ms.fetch_add(ms, Ordering::SeqCst);
        }

        fn as_hibernation_clock(self: &Arc<Self>) -> HibernationClock {
            let clock = Arc::clone(self);
            Arc::new(move || {
                clock.base + Duration::from_millis(clock.offset_ms.load(Ordering::SeqCst))
            })
        }
    }

    fn write_protected_key_file(path: &std::path::Path, content: &str) {
        fs::write(path, content).expect("write key file");
        #[cfg(unix)]
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))
            .expect("set 0600 permissions on key file");
    }

    fn build_hibernation_test_config(
        scope_dir: &std::path::Path,
        db_path: &std::path::Path,
    ) -> ServerConfig {
        let backup_root = scope_dir.join("backups");
        fs::create_dir_all(&backup_root).expect("create backup root");
        #[cfg(unix)]
        fs::set_permissions(&backup_root, fs::Permissions::from_mode(0o700))
            .expect("protect backup root");

        let projections_root = scope_dir.join("projections");
        fs::create_dir_all(&projections_root).expect("create projections root");
        #[cfg(unix)]
        fs::set_permissions(&projections_root, fs::Permissions::from_mode(0o700))
            .expect("protect projections root");

        let cap_key = scope_dir.join("cap.key");
        let idem_key = scope_dir.join("idem.key");
        write_protected_key_file(
            &cap_key,
            "riffdb-capability-digest-keys-v1\n1:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\n",
        );
        write_protected_key_file(
            &idem_key,
            "riffdb-idempotency-digest-keys-v1\n1:1f1e1d1c1b1a191817161514131211100f0e0d0c0b0a09080706050403020100\n",
        );

        let config_toml = format!(
            r#"[server]
database = "{}"
grpc_listen = "127.0.0.1:7443"
environment = "local"
audience = "riffdb-grpc-loopback"
capability_keys = "{}"
idempotency_keys = "{}"

[maintenance]
backup_root = "{}"
projections_root = "{}"

[maintenance.limits]
idle_hibernate_seconds = 10
"#,
            db_path.display(),
            cap_key.display(),
            idem_key.display(),
            backup_root.display(),
            projections_root.display(),
        );
        let config_file = scope_dir.join("riffdbd.toml");
        fs::write(&config_file, config_toml).expect("write config toml");
        ServerConfig::parse([OsString::from("--config"), config_file.into_os_string()])
            .expect("parse hibernation ServerConfig")
    }

    // req: NET-005, REC-001, REC-002, REC-004, PERF-007, PERF-019
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn idle_database_hibernates_releases_lock_and_wakes_transparently_on_request() {
        let (scope, db_path) = temporary_database_scope("hibernation-lifecycle");
        let config = build_hibernation_test_config(scope.path(), &db_path);
        let database = config.databases()[0].clone();
        let idle_timeout = database
            .limits()
            .idle_hibernate()
            .expect("configured idle_hibernate_seconds");
        assert_eq!(idle_timeout, Duration::from_secs(10));

        let prepared = GenerationInputs::load(&config).expect("load generation inputs");
        let started_at = Timestamp::new(1_700_000_000, 0).expect("valid started_at");
        let (maintenance_storage, _reconciliation) =
            RedbMaintenanceStorage::open(database.database_path(), database.backup_root())
                .expect("open maintenance storage");
        let maintenance_lifecycle = Arc::new(MaintenanceLifecycle::ready());
        let (maintenance_triggers, maintenance_receiver) = maintenance_trigger_channel();
        let maintenance = MaintenanceController::new_with_migration_exclusion(
            shared_maintenance_storage(maintenance_storage),
            Arc::clone(&maintenance_lifecycle),
            maintenance_triggers,
            Arc::new(MigrationProcessExclusion::new()),
        );

        let (initial_active, initial_awakened) = wake_production_database(
            &config,
            &database,
            started_at,
            &maintenance_lifecycle,
            &maintenance,
        )
        .expect("start initial database generation");

        let clock = ManualClock::new();
        let wake_count = Arc::new(AtomicUsize::new(0));
        let base_wake = make_production_wake_fn(
            config.clone(),
            database.clone(),
            started_at,
            Arc::clone(&maintenance_lifecycle),
            maintenance.clone(),
        );
        let wake_count_clone = Arc::clone(&wake_count);
        let counted_wake: WakeDatabaseFn = Arc::new(move || {
            wake_count_clone.fetch_add(1, Ordering::SeqCst);
            base_wake()
        });

        let hibernatable = HibernatableDatabaseRoute::new(
            initial_active,
            clock.as_hibernation_clock(),
            tokio::runtime::Handle::current(),
            counted_wake,
        );

        let mut multi_graph = MultiDatabaseGraph::new_primary_for_test(
            DatabaseAlias::new("default").expect("default alias"),
            initial_awakened.graph,
            initial_awakened.lifecycle,
            initial_awakened.routing,
            Arc::clone(&maintenance_lifecycle),
            maintenance.clone(),
            maintenance_receiver,
            Arc::clone(&hibernatable),
            Some(idle_timeout),
        );

        let mut monitors = Vec::new();
        let (event_sender, _event_receiver) = tokio::sync::mpsc::channel(8);

        let gen_before = hibernatable
            .server_generation()
            .expect("active database exposes server_generation");
        assert!(!hibernatable.is_hibernated());

        // While active, the exclusive redb lock is held by RunningProductionGraph.
        let lock_probe = open_redb_startup_with_extent_profile(
            database.database_path(),
            prepared.startup_inputs.clone(),
            &prepared.identifiers.database_ids(),
            config.redb_commit_profile(),
            database.limits().extent_profile(),
        );
        assert!(
            lock_probe.is_err(),
            "active database must hold the exclusive redb lock"
        );

        // Before idle_timeout elapses, try_hibernate_if_idle is a no-op.
        clock.advance(Duration::from_secs(5));
        let hibernated = try_hibernate_if_idle(0, &mut multi_graph, &mut monitors, &event_sender)
            .await
            .expect("idle check before timeout");
        assert!(!hibernated);
        assert!(!hibernatable.is_hibernated());

        // Advance past the 10-second idle threshold (5s + 10s = 15s >= 10s).
        clock.advance(Duration::from_secs(10));
        let hibernated = try_hibernate_if_idle(0, &mut multi_graph, &mut monitors, &event_sender)
            .await
            .expect("hibernate idle database");
        assert!(hibernated);
        assert!(hibernatable.is_hibernated());
        assert!(multi_graph.graph.is_none());

        // Prove that the exclusive redb file lock has been released and CleanCloseState::Clean was committed.
        let reopened = open_redb_startup_with_extent_profile(
            database.database_path(),
            prepared.startup_inputs.clone(),
            &prepared.identifiers.database_ids(),
            config.redb_commit_profile(),
            database.limits().extent_profile(),
        )
        .expect("hibernated database must release the exclusive redb lock cleanly");
        let (_id, _retained, _history, _lifecycle, _capacity, ports) = reopened.into_parts();
        assert!(
            ports.clean_close_fast_startup(),
            "hibernation must commit a clean close state so wake uses bounded startup"
        );
        let close_receipt = ports.complete_graceful_close();
        assert_eq!(
            close_receipt.lifecycle(),
            riffdb_storage_redb::GracefulLifecycleOutcomeV1::CleanCommitted
        );
        drop(ports);

        // Now send a request through the hibernatable route: it wakes transparently!
        assert_eq!(wake_count.load(Ordering::SeqCst), 0);
        let gen_after = hibernatable
            .server_generation()
            .expect("request transparently wakes hibernated database");
        assert_eq!(wake_count.load(Ordering::SeqCst), 1);
        assert!(!hibernatable.is_hibernated());
        assert_ne!(
            gen_before, gen_after,
            "awakened database generation must sample a fresh server generation"
        );

        let health = hibernatable
            .restricted_health(HealthRequest)
            .expect("restricted health on awakened database")
            .await
            .expect("health response");
        assert!(matches!(
            health,
            HealthResult::PreBootstrap(report) if report.liveness()
        ));

        prepare_for_offline_or_shutdown(&mut multi_graph);
        for monitor in monitors {
            monitor.abort();
        }
        multi_graph
            .shutdown()
            .await
            .expect("clean shutdown of awakened database");
    }

    // req: NET-005, REC-001, REC-002, REC-004, PERF-007, PERF-019
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_wake_requests_coalesce_single_flight_and_shutdown_skips_hibernated_database()
     {
        let (scope, db_path) = temporary_database_scope("hibernation-single-flight");
        let config = build_hibernation_test_config(scope.path(), &db_path);
        let database = config.databases()[0].clone();
        let idle_timeout = database
            .limits()
            .idle_hibernate()
            .expect("configured idle_hibernate_seconds");

        let started_at = Timestamp::new(1_700_000_000, 0).expect("valid started_at");
        let (maintenance_storage, _reconciliation) =
            RedbMaintenanceStorage::open(database.database_path(), database.backup_root())
                .expect("open maintenance storage");
        let maintenance_lifecycle = Arc::new(MaintenanceLifecycle::ready());
        let (maintenance_triggers, maintenance_receiver) = maintenance_trigger_channel();
        let maintenance = MaintenanceController::new_with_migration_exclusion(
            shared_maintenance_storage(maintenance_storage),
            Arc::clone(&maintenance_lifecycle),
            maintenance_triggers,
            Arc::new(MigrationProcessExclusion::new()),
        );

        let (initial_active, initial_awakened) = wake_production_database(
            &config,
            &database,
            started_at,
            &maintenance_lifecycle,
            &maintenance,
        )
        .expect("start initial database generation");

        let clock = ManualClock::new();
        let wake_count = Arc::new(AtomicUsize::new(0));
        let wake_entered = Arc::new(Barrier::new(2));
        let wake_release = Arc::new(Barrier::new(2));
        let base_wake = make_production_wake_fn(
            config,
            database,
            started_at,
            Arc::clone(&maintenance_lifecycle),
            maintenance.clone(),
        );
        let wake_count_clone = Arc::clone(&wake_count);
        let wake_entered_clone = Arc::clone(&wake_entered);
        let wake_release_clone = Arc::clone(&wake_release);
        let gated_wake: WakeDatabaseFn = Arc::new(move || {
            let call_index = wake_count_clone.fetch_add(1, Ordering::SeqCst);
            if call_index == 0 {
                wake_entered_clone.wait();
                wake_release_clone.wait();
            }
            base_wake()
        });

        let hibernatable = HibernatableDatabaseRoute::new(
            initial_active,
            clock.as_hibernation_clock(),
            tokio::runtime::Handle::current(),
            gated_wake,
        );

        let mut multi_graph = MultiDatabaseGraph::new_primary_for_test(
            DatabaseAlias::new("default").expect("default alias"),
            initial_awakened.graph,
            initial_awakened.lifecycle,
            initial_awakened.routing,
            maintenance_lifecycle,
            maintenance,
            maintenance_receiver,
            Arc::clone(&hibernatable),
            Some(idle_timeout),
        );

        let mut monitors = Vec::new();
        let (event_sender, _event_receiver) = tokio::sync::mpsc::channel(8);

        // Hibernate the initial generation.
        clock.advance(Duration::from_secs(10));
        assert!(
            try_hibernate_if_idle(0, &mut multi_graph, &mut monitors, &event_sender)
                .await
                .expect("initial hibernation")
        );
        assert!(hibernatable.is_hibernated());

        // Launch 4 concurrent callers while the database is hibernated.
        let start_barrier = Arc::new(Barrier::new(4));
        let mut handles = Vec::with_capacity(4);
        for _ in 0..4 {
            let route = Arc::clone(&hibernatable);
            let barrier = Arc::clone(&start_barrier);
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                route
                    .server_generation()
                    .expect("coalesced wake returns active server generation")
            }));
        }

        // Wait until the single-flight leader has entered gated_wake.
        wake_entered.wait();
        // Wait until all 3 follower threads are queued on `wake_condvar` (state remains Waking).
        hibernatable.wait_for_waking_waiters(3);
        wake_release.wait();

        let mut generations = Vec::with_capacity(4);
        for handle in handles {
            generations.push(handle.join().expect("caller thread joined"));
        }
        assert_eq!(
            wake_count.load(Ordering::SeqCst),
            1,
            "concurrent wake requests must coalesce into a single wake execution"
        );
        for observed in &generations[1..] {
            assert_eq!(
                &generations[0], observed,
                "all coalesced callers must observe the same awakened generation"
            );
        }

        // Advance the clock again and hibernate a second time.
        clock.advance(Duration::from_secs(10));
        assert!(
            try_hibernate_if_idle(0, &mut multi_graph, &mut monitors, &event_sender)
                .await
                .expect("second hibernation")
        );
        assert!(hibernatable.is_hibernated());
        assert!(multi_graph.graph.is_none());

        // Shut down the server while the database is hibernated: it must NOT reopen the database.
        prepare_for_offline_or_shutdown(&mut multi_graph);
        for monitor in monitors {
            monitor.abort();
        }
        multi_graph
            .shutdown()
            .await
            .expect("shutdown while hibernated succeeds in O(1)");
        assert_eq!(
            wake_count.load(Ordering::SeqCst),
            1,
            "daemon shutdown must skip a hibernated database without waking it"
        );
        assert!(
            hibernatable.server_generation().is_none(),
            "stopped hibernatable route must fail closed after shutdown"
        );
        assert_eq!(wake_count.load(Ordering::SeqCst), 1);
    }
}
