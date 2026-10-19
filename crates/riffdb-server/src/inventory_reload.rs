//! Online dynamic multi-database inventory provisioning and deprovisioning (`ADR-0319`).

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use riffdb_api_grpc::{GrpcDatabaseRoutes, GrpcLifecycleRoute};
use riffdb_service::RiffDbService;
use riffdb_types::{DatabaseAlias, MAX_DATABASES_PER_PROCESS, Timestamp};
use tokio::task::JoinHandle as TokioJoinHandle;

use crate::config::{DatabaseConfig, ServerConfig, ServerMode};
use crate::daemon::{
    DaemonError, GenerationInputs, InitialDatabaseAction, MultiDatabaseEvent, MultiDatabaseGraph,
    TransportFileIdentity, build_info, incomplete_contract_migration, initial_database_action,
    replication_roles, spawn_multi_runtime_monitor, transport_file_identity,
};
use crate::hibernation::{HibernatableDatabaseRoute, make_production_wake_fn};
use crate::hosted_mcp::HostedMcp;
use crate::lifecycle::ProductionLifecycleRoute;
use crate::maintenance_adapter::{
    MaintenanceController, MigrationProcessExclusion, maintenance_trigger_channel,
    shared_maintenance_storage,
};
use crate::maintenance_lifecycle::MaintenanceLifecycle;
use crate::process_graph::ProductionGraphBuilder;
use crate::runtime_support::RuntimeRoutingState;
use crate::startup::open_redb_startup_with_extent_profile;

/// Closed failure reason for a rejected online multi-database inventory reload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InventoryReloadError {
    /// The configuration file could not be read or failed validation.
    ConfigParse,
    /// Either the current or candidate configuration is not a primary multi-database inventory.
    IncompatibleMode,
    /// A process-global listener, audience, or key-custody path changed across the reload.
    ProcessGlobalChanged,
    /// A retained database alias changed its immutable storage paths, environment, or topology.
    RetainedDatabaseChanged,
    /// A removed database alias is currently executing an offline maintenance or migration transition.
    DeprovisionBusy,
    /// A newly added database alias failed startup validation or graph construction.
    ProvisioningFailed,
    /// Dynamic route table insertion or removal failed.
    RouteMutationFailed,
    /// Graceful close of a deprovisioned database graph failed.
    DeprovisionFailed,
}

impl InventoryReloadError {
    /// Stable, non-secret reason code logged when a configuration reload is rejected.
    #[must_use]
    pub(crate) const fn kind(self) -> &'static str {
        match self {
            Self::ConfigParse => "config_parse",
            Self::IncompatibleMode => "incompatible_mode",
            Self::ProcessGlobalChanged => "process_global_changed",
            Self::RetainedDatabaseChanged => "retained_database_changed",
            Self::DeprovisionBusy => "deprovision_busy",
            Self::ProvisioningFailed => "provisioning_failed",
            Self::RouteMutationFailed => "route_mutation_failed",
            Self::DeprovisionFailed => "deprovision_failed",
        }
    }
}

/// Summary of aliases provisioned and deprovisioned by one online reconciliation pass.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct InventoryReconciliationSummary {
    pub(crate) added: Vec<DatabaseAlias>,
    pub(crate) removed: Vec<DatabaseAlias>,
}

/// Monitors the server's TOML configuration file and reconciles `[databases.<alias>]` deltas.
pub(crate) struct ReloadableDatabaseInventory {
    config_path: PathBuf,
    current_file: TransportFileIdentity,
    rejected_file: Option<TransportFileIdentity>,
    unavailable_reported: bool,
}

impl ReloadableDatabaseInventory {
    /// Initializes file-identity tracking when `config` was loaded from a named-database TOML file
    /// in `ServerMode::Primary`.
    pub(crate) fn load(config: &ServerConfig) -> Option<Self> {
        if !config.uses_named_databases() || config.mode() != ServerMode::Primary {
            return None;
        }
        let config_path = config.config_file_path()?.to_path_buf();
        let current_file = transport_file_identity(&config_path, false).ok()?;
        Some(Self {
            config_path,
            current_file,
            rejected_file: None,
            unavailable_reported: false,
        })
    }

    /// Checks whether the configuration file's `(dev, ino, mtime, bytes)` identity has changed
    /// and reconciles any added or removed `[databases.<alias>]` sections.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn poll_and_reconcile(
        &mut self,
        current_config: &mut ServerConfig,
        started_at: Timestamp,
        migration_exclusion: &Arc<MigrationProcessExclusion>,
        routes: &GrpcDatabaseRoutes,
        hosted_mcp: &Option<HostedMcp>,
        graphs: &mut Vec<MultiDatabaseGraph>,
        monitors: &mut Vec<TokioJoinHandle<()>>,
        event_sender: &tokio::sync::mpsc::Sender<MultiDatabaseEvent>,
    ) {
        let Ok(observed) = transport_file_identity(&self.config_path, false) else {
            if !self.unavailable_reported {
                eprintln!(
                    "riffdbd config reload rejected kind=config_file_unavailable retained_previous=true"
                );
                self.unavailable_reported = true;
            }
            return;
        };
        self.unavailable_reported = false;
        if observed == self.current_file || self.rejected_file.as_ref() == Some(&observed) {
            return;
        }
        let candidate = match ServerConfig::reload_from_config_file(&self.config_path) {
            Ok(candidate) => candidate,
            Err(_) => {
                eprintln!(
                    "riffdbd config reload rejected kind={} retained_previous=true",
                    InventoryReloadError::ConfigParse.kind()
                );
                self.rejected_file = Some(observed);
                return;
            }
        };
        match reconcile_multi_database_inventory(
            current_config,
            candidate,
            started_at,
            migration_exclusion,
            routes,
            hosted_mcp,
            graphs,
            monitors,
            event_sender,
        )
        .await
        {
            Ok(_) => {
                self.current_file = observed;
                self.rejected_file = None;
            }
            Err(error) => {
                eprintln!(
                    "riffdbd config reload rejected kind={} retained_previous=true",
                    error.kind()
                );
                self.rejected_file = Some(observed);
            }
        }
    }
}

/// Validates and applies the delta between `current_config` and `candidate_config` onto a running
/// multi-database server without interrupting retained peer databases.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn reconcile_multi_database_inventory(
    current_config: &mut ServerConfig,
    candidate_config: ServerConfig,
    started_at: Timestamp,
    migration_exclusion: &Arc<MigrationProcessExclusion>,
    routes: &GrpcDatabaseRoutes,
    hosted_mcp: &Option<HostedMcp>,
    graphs: &mut Vec<MultiDatabaseGraph>,
    monitors: &mut Vec<TokioJoinHandle<()>>,
    event_sender: &tokio::sync::mpsc::Sender<MultiDatabaseEvent>,
) -> Result<InventoryReconciliationSummary, InventoryReloadError> {
    validate_reload_compatibility(current_config, &candidate_config, graphs)?;

    let current_by_alias: BTreeMap<&DatabaseAlias, &DatabaseConfig> = current_config
        .databases()
        .iter()
        .map(|db| (db.alias(), db))
        .collect();
    let candidate_by_alias: BTreeMap<&DatabaseAlias, &DatabaseConfig> = candidate_config
        .databases()
        .iter()
        .map(|db| (db.alias(), db))
        .collect();

    let added_configs: Vec<&DatabaseConfig> = candidate_config
        .databases()
        .iter()
        .filter(|db| !current_by_alias.contains_key(db.alias()))
        .collect();
    let removed_aliases: Vec<DatabaseAlias> = current_config
        .databases()
        .iter()
        .filter(|db| !candidate_by_alias.contains_key(db.alias()))
        .map(|db| db.alias().clone())
        .collect();

    if added_configs.is_empty() && removed_aliases.is_empty() {
        *current_config = candidate_config;
        return Ok(InventoryReconciliationSummary::default());
    }

    // Phase 1: Open, validate, and build every newly added database graph before touching
    // any public route or deprovisioning any existing database.
    let mut prepared_additions = Vec::with_capacity(added_configs.len());
    for database in &added_configs {
        match build_provisioned_database_graph(
            &candidate_config,
            database,
            started_at,
            migration_exclusion,
        ) {
            Ok(built) => prepared_additions.push(built),
            Err(_) => {
                for graph in prepared_additions {
                    stop_and_shutdown_graph(graph).await;
                }
                return Err(InventoryReloadError::ProvisioningFailed);
            }
        }
    }

    // Phase 2: Publish route additions and unpublish/drain removals while preserving
    // 1 <= active_routes <= MAX_DATABASES_PER_PROCESS at every step.
    let active_before = current_config.databases().len();
    let remove_first =
        active_before.saturating_add(prepared_additions.len()) > MAX_DATABASES_PER_PROCESS;

    let mut deprovision_error = None;
    if remove_first {
        for alias in &removed_aliases {
            if let Err(err) = deprovision_database_alias(alias, routes, hosted_mcp, graphs).await {
                deprovision_error = Some(err);
            }
        }
    }

    let mut added_aliases = Vec::with_capacity(prepared_additions.len());
    for graph in prepared_additions {
        let alias = graph.alias.clone();
        install_provisioned_database_graph(
            graph,
            routes,
            hosted_mcp,
            graphs,
            monitors,
            event_sender,
        )?;
        added_aliases.push(alias);
    }

    if !remove_first {
        for alias in &removed_aliases {
            if let Err(err) = deprovision_database_alias(alias, routes, hosted_mcp, graphs).await {
                deprovision_error = Some(err);
            }
        }
    }

    *current_config = candidate_config;
    if let Some(err) = deprovision_error {
        return Err(err);
    }

    Ok(InventoryReconciliationSummary {
        added: added_aliases,
        removed: removed_aliases,
    })
}

fn validate_reload_compatibility(
    current: &ServerConfig,
    candidate: &ServerConfig,
    graphs: &[MultiDatabaseGraph],
) -> Result<(), InventoryReloadError> {
    if !current.uses_named_databases()
        || !candidate.uses_named_databases()
        || current.mode() != ServerMode::Primary
        || candidate.mode() != ServerMode::Primary
    {
        return Err(InventoryReloadError::IncompatibleMode);
    }
    if candidate.databases().is_empty() || candidate.databases().len() > MAX_DATABASES_PER_PROCESS {
        return Err(InventoryReloadError::ConfigParse);
    }
    if current.application_listener() != candidate.application_listener()
        || current.audience() != candidate.audience()
        || current.mcp_listener() != candidate.mcp_listener()
        || current.mcp_origins() != candidate.mcp_origins()
        || current.metrics_listen_address() != candidate.metrics_listen_address()
        || current.telemetry_flight_recorder_path() != candidate.telemetry_flight_recorder_path()
        || current.telemetry_flight_interval() != candidate.telemetry_flight_interval()
        || current.capability_key_path() != candidate.capability_key_path()
        || current.idempotency_key_path() != candidate.idempotency_key_path()
        || current.redb_commit_profile() != candidate.redb_commit_profile()
    {
        return Err(InventoryReloadError::ProcessGlobalChanged);
    }

    let current_by_alias: BTreeMap<&DatabaseAlias, &DatabaseConfig> = current
        .databases()
        .iter()
        .map(|db| (db.alias(), db))
        .collect();
    let candidate_aliases: BTreeSet<&DatabaseAlias> =
        candidate.databases().iter().map(|db| db.alias()).collect();

    for candidate_db in candidate.databases() {
        if candidate_db.follower().is_some() {
            return Err(InventoryReloadError::IncompatibleMode);
        }
        if let Some(existing_db) = current_by_alias.get(candidate_db.alias())
            && !retained_database_compatible(existing_db, candidate_db)
        {
            return Err(InventoryReloadError::RetainedDatabaseChanged);
        }
    }

    for existing_db in current.databases() {
        if !candidate_aliases.contains(existing_db.alias()) {
            let Some(graph) = graphs
                .iter()
                .find(|g| !g.deprovisioned && &g.alias == existing_db.alias())
            else {
                return Err(InventoryReloadError::DeprovisionBusy);
            };
            if !graph.maintenance_lifecycle.ordinary_admission_available() {
                return Err(InventoryReloadError::DeprovisionBusy);
            }
        }
    }

    Ok(())
}

fn retained_database_compatible(existing: &DatabaseConfig, candidate: &DatabaseConfig) -> bool {
    existing.database_path() == candidate.database_path()
        && existing.backup_root() == candidate.backup_root()
        && existing.projections_root() == candidate.projections_root()
        && existing.environment() == candidate.environment()
        && existing.projections() == candidate.projections()
        && existing.outbox() == candidate.outbox()
        && existing.archives() == candidate.archives()
        && existing.follower().is_none()
        && candidate.follower().is_none()
}

pub(crate) fn build_provisioned_database_graph(
    config: &ServerConfig,
    database: &DatabaseConfig,
    started_at: Timestamp,
    migration_exclusion: &Arc<MigrationProcessExclusion>,
) -> Result<MultiDatabaseGraph, DaemonError> {
    let prepared = GenerationInputs::load(config)?;
    let replication_roles::PreparedMaintenanceStartup {
        owner: mut maintenance_storage,
        reconciliation,
        promoted: mut promoted_startup,
    } = replication_roles::prepare(
        database.database_path(),
        database.backup_root(),
        None,
        prepared.startup_inputs.clone(),
        config.redb_commit_profile(),
    )?;

    if incomplete_contract_migration(&reconciliation)?.is_some() {
        return Err(DaemonError::MaintenanceDriver);
    }
    let target_requires_recovery = if promoted_startup.is_some() {
        false
    } else {
        maintenance_storage
            .configured_target_requires_recovery()
            .map_err(DaemonError::MaintenanceStorage)?
    };
    let initial_action = initial_database_action(
        &mut maintenance_storage,
        &reconciliation,
        target_requires_recovery,
    )?;
    if !matches!(initial_action, InitialDatabaseAction::OpenCurrent) {
        return Err(DaemonError::MaintenanceDriver);
    }

    let (maintenance_triggers, maintenance_receiver) = maintenance_trigger_channel();
    let maintenance_lifecycle = Arc::new(MaintenanceLifecycle::ready());
    let maintenance = MaintenanceController::new_with_migration_exclusion(
        shared_maintenance_storage(maintenance_storage),
        Arc::clone(&maintenance_lifecycle),
        maintenance_triggers,
        Arc::clone(migration_exclusion),
    );

    let startup = match promoted_startup.take() {
        Some(startup) => startup,
        None => open_redb_startup_with_extent_profile(
            database.database_path(),
            prepared.startup_inputs,
            &prepared.identifiers.database_ids(),
            config.redb_commit_profile(),
            database.limits().extent_profile(),
        )
        .map_err(DaemonError::Startup)?,
    };

    let (initializing, activator, issuer) = RiffDbService::begin_initialization();
    let routing = RuntimeRoutingState::new();
    let lifecycle = Arc::new(ProductionLifecycleRoute::new_with_maintenance(
        initializing,
        issuer,
        routing.clone(),
        Arc::clone(&maintenance_lifecycle),
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

    if !routing.is_routing_allowed() {
        return Err(DaemonError::RuntimeStopped);
    }

    let idle_hibernate = database.limits().idle_hibernate();
    let hibernatable = idle_hibernate.map(|_| {
        HibernatableDatabaseRoute::new(
            graph.active_generation_routes(),
            Arc::new(Instant::now),
            tokio::runtime::Handle::current(),
            make_production_wake_fn(
                config.clone(),
                database.clone(),
                started_at,
                Arc::clone(&maintenance_lifecycle),
                maintenance.clone(),
            ),
        )
    });

    Ok(MultiDatabaseGraph::new_provisioned(
        database.alias().clone(),
        graph,
        lifecycle,
        routing,
        maintenance_lifecycle,
        maintenance,
        maintenance_receiver,
        hibernatable,
        idle_hibernate,
    ))
}

fn install_provisioned_database_graph(
    mut graph: MultiDatabaseGraph,
    routes: &GrpcDatabaseRoutes,
    hosted_mcp: &Option<HostedMcp>,
    graphs: &mut Vec<MultiDatabaseGraph>,
    monitors: &mut Vec<TokioJoinHandle<()>>,
    event_sender: &tokio::sync::mpsc::Sender<MultiDatabaseEvent>,
) -> Result<(), InventoryReloadError> {
    let route: Arc<dyn GrpcLifecycleRoute> = match graph.hibernatable.as_ref() {
        Some(hibernatable) => hibernatable.clone(),
        None => graph.lifecycle.clone(),
    };
    let mcp_dependencies = if hosted_mcp.is_some() {
        Some(
            graph
                .hosted_mcp_dependencies()
                .ok_or(InventoryReloadError::RouteMutationFailed)?,
        )
    } else {
        None
    };

    routes
        .insert(graph.alias.clone(), route)
        .map_err(|_| InventoryReloadError::RouteMutationFailed)?;
    if let (Some(hosted), Some(dependencies)) = (hosted_mcp.as_ref(), mcp_dependencies)
        && hosted.insert(graph.alias.clone(), dependencies).is_err()
    {
        let _ = routes.remove(&graph.alias);
        return Err(InventoryReloadError::RouteMutationFailed);
    }

    let database_index = if let Some(existing_index) = graphs
        .iter()
        .position(|slot| slot.deprovisioned && slot.alias == graph.alias)
    {
        let next_generation = graphs[existing_index].generation.wrapping_add(1);
        graph.generation = next_generation;
        graphs[existing_index] = graph;
        existing_index
    } else {
        let index = graphs.len();
        graphs.push(graph);
        index
    };

    let slot = &mut graphs[database_index];
    if let Some(mut receiver) = slot.maintenance_receiver.take() {
        let sender = event_sender.clone();
        monitors.push(tokio::spawn(async move {
            while let Some(trigger) = receiver.recv().await {
                if sender
                    .send(MultiDatabaseEvent::Maintenance {
                        database_index,
                        trigger,
                    })
                    .await
                    .is_err()
                {
                    return;
                }
            }
            let _ = sender
                .send(MultiDatabaseEvent::MaintenanceClosed { database_index })
                .await;
        }));
    }
    monitors.push(spawn_multi_runtime_monitor(
        database_index,
        slot.generation,
        slot.routing.clone(),
        event_sender.clone(),
    ));

    Ok(())
}

async fn deprovision_database_alias(
    alias: &DatabaseAlias,
    routes: &GrpcDatabaseRoutes,
    hosted_mcp: &Option<HostedMcp>,
    graphs: &mut [MultiDatabaseGraph],
) -> Result<(), InventoryReloadError> {
    let Some(generation) = graphs
        .iter_mut()
        .find(|slot| !slot.deprovisioned && &slot.alias == alias)
    else {
        return Err(InventoryReloadError::RouteMutationFailed);
    };

    routes
        .remove(alias)
        .map_err(|_| InventoryReloadError::RouteMutationFailed)?;
    if let Some(hosted) = hosted_mcp.as_ref() {
        hosted
            .remove(alias)
            .map_err(|_| InventoryReloadError::RouteMutationFailed)?;
    }

    crate::hibernation::prepare_for_offline_or_shutdown(generation);
    generation.deprovisioned = true;
    generation.generation = u64::MAX;
    generation.lifecycle.stop();
    let (offline_service, _offline_activator, offline_issuer) =
        RiffDbService::begin_initialization();
    generation.lifecycle = Arc::new(ProductionLifecycleRoute::new_with_maintenance(
        offline_service,
        offline_issuer,
        RuntimeRoutingState::new(),
        Arc::clone(&generation.maintenance_lifecycle),
    ));
    generation.hibernatable = None;
    generation.maintenance = None;
    let _ = generation.maintenance_receiver.take();
    let _ = generation.promotion_receiver.take();

    let mut clean_close = Ok(());
    if let Some(graph) = generation.graph.take() {
        if graph.begin_transport_shutdown().is_err() {
            clean_close = Err(InventoryReloadError::DeprovisionFailed);
        }
        if graph.shutdown().await.is_err() {
            clean_close = Err(InventoryReloadError::DeprovisionFailed);
        }
    }
    clean_close
}

async fn stop_and_shutdown_graph(mut generation: MultiDatabaseGraph) {
    crate::hibernation::prepare_for_offline_or_shutdown(&mut generation);
    generation.lifecycle.stop();
    generation.hibernatable = None;
    generation.maintenance = None;
    if let Some(graph) = generation.graph.take() {
        let _ = graph.begin_transport_shutdown();
        let _ = graph.shutdown().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;

    use crate::real_storage_support::temporary_database_scope;

    fn write_protected_key_file(path: &Path, content: &str) {
        fs::write(path, content).expect("write key file");
        #[cfg(unix)]
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))
            .expect("set 0600 permissions on key file");
    }

    fn ensure_protected_dir(path: &Path) {
        fs::create_dir_all(path).expect("create protected dir");
        #[cfg(unix)]
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .expect("set 0700 permissions on dir");
    }

    fn write_multi_database_toml(
        scope_dir: &Path,
        audience: &str,
        aliases: &[(&str, &Path)],
    ) -> PathBuf {
        let cap_key = scope_dir.join("cap.key");
        let idem_key = scope_dir.join("idem.key");
        if !cap_key.exists() {
            write_protected_key_file(
                &cap_key,
                "riffdb-capability-digest-keys-v1\n1:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\n",
            );
        }
        if !idem_key.exists() {
            write_protected_key_file(
                &idem_key,
                "riffdb-idempotency-digest-keys-v1\n1:1f1e1d1c1b1a191817161514131211100f0e0d0c0b0a09080706050403020100\n",
            );
        }

        let mcp_port_file = scope_dir.join("mcp.port");
        let mcp_port: u16 = if mcp_port_file.exists() {
            fs::read_to_string(&mcp_port_file)
                .expect("read mcp.port")
                .trim()
                .parse()
                .expect("parse mcp.port")
        } else {
            let probe =
                std::net::TcpListener::bind("127.0.0.1:0").expect("allocate ephemeral mcp port");
            let port = probe.local_addr().expect("probe local_addr").port();
            drop(probe);
            fs::write(&mcp_port_file, port.to_string()).expect("write mcp.port");
            port
        };

        let mut document = format!(
            r#"[server]
grpc_listen = "127.0.0.1:7443"
audience = "{audience}"
mcp_listen = "127.0.0.1:{mcp_port}"
mcp_origins = ["http://127.0.0.1:3000"]
capability_keys = "{}"
idempotency_keys = "{}"
"#,
            cap_key.display(),
            idem_key.display(),
        );
        for (alias, db_path) in aliases {
            let backup_root = scope_dir.join(format!("backups-{alias}"));
            let projections_root = scope_dir.join(format!("projections-{alias}"));
            ensure_protected_dir(&backup_root);
            ensure_protected_dir(&projections_root);
            document.push_str(&format!(
                r#"
[databases.{alias}]
path = "{}"
backup_root = "{}"
projections_root = "{}"
environment = "local"

[databases.{alias}.limits]
extent_profile = "compact"
"#,
                db_path.display(),
                backup_root.display(),
                projections_root.display(),
            ));
        }
        let config_path = scope_dir.join("riffdbd.toml");
        fs::write(&config_path, document).expect("write multi-database config");
        config_path
    }

    async fn start_multi_database_harness(
        config: &ServerConfig,
        started_at: Timestamp,
        migration_exclusion: &Arc<MigrationProcessExclusion>,
    ) -> (
        Arc<GrpcDatabaseRoutes>,
        Option<HostedMcp>,
        Vec<MultiDatabaseGraph>,
        Vec<TokioJoinHandle<()>>,
        tokio::sync::mpsc::Sender<MultiDatabaseEvent>,
        tokio::sync::mpsc::Receiver<MultiDatabaseEvent>,
    ) {
        let mut graphs = Vec::with_capacity(config.databases().len());
        let mut route_entries = Vec::with_capacity(config.databases().len());
        let mut mcp_entries = Vec::with_capacity(config.databases().len());

        for database in config.databases() {
            let graph =
                build_provisioned_database_graph(config, database, started_at, migration_exclusion)
                    .expect("build initial database graph");
            let route: Arc<dyn GrpcLifecycleRoute> = match graph.hibernatable.as_ref() {
                Some(hibernatable) => hibernatable.clone(),
                None => graph.lifecycle.clone(),
            };
            route_entries.push((database.alias().clone(), route));
            mcp_entries.push((
                database.alias().clone(),
                graph
                    .hosted_mcp_dependencies()
                    .expect("hosted mcp dependencies"),
            ));
            graphs.push(graph);
        }

        let routes = Arc::new(GrpcDatabaseRoutes::new(route_entries).expect("grpc routes"));
        let hosted_mcp = match config.mcp_listener() {
            Some(listener) => Some(
                HostedMcp::bind_listener_routes(listener, config.mcp_origins(), mcp_entries)
                    .await
                    .expect("bind hosted mcp"),
            ),
            None => None,
        };

        let (event_sender, event_receiver) =
            tokio::sync::mpsc::channel(MAX_DATABASES_PER_PROCESS.saturating_mul(2));
        let mut monitors = Vec::new();
        for (database_index, graph) in graphs.iter_mut().enumerate() {
            let mut receiver = graph
                .maintenance_receiver
                .take()
                .expect("maintenance receiver");
            let sender = event_sender.clone();
            monitors.push(tokio::spawn(async move {
                while let Some(trigger) = receiver.recv().await {
                    if sender
                        .send(MultiDatabaseEvent::Maintenance {
                            database_index,
                            trigger,
                        })
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }));
            monitors.push(spawn_multi_runtime_monitor(
                database_index,
                graph.generation,
                graph.routing.clone(),
                event_sender.clone(),
            ));
        }

        (
            routes,
            hosted_mcp,
            graphs,
            monitors,
            event_sender,
            event_receiver,
        )
    }

    async fn shutdown_harness(
        mut hosted_mcp: Option<HostedMcp>,
        mut graphs: Vec<MultiDatabaseGraph>,
        monitors: Vec<TokioJoinHandle<()>>,
    ) {
        for monitor in monitors {
            monitor.abort();
        }
        if let Some(hosted) = hosted_mcp.as_mut() {
            let _ = hosted.drain_after_signal().await;
        }
        for graph in &mut graphs {
            if graph.deprovisioned {
                continue;
            }
            crate::hibernation::prepare_for_offline_or_shutdown(graph);
            graph.lifecycle.stop();
            if let Some(running) = graph.graph.as_ref() {
                let _ = running.begin_transport_shutdown();
            }
        }
        for graph in graphs {
            if graph.deprovisioned {
                continue;
            }
            graph.shutdown().await.expect("clean shutdown");
        }
    }

    // req: NET-005, REC-001, REC-002, REC-004, PERF-007, PERF-019
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn online_multi_database_reload_provisions_and_deprovisions_aliases_without_interrupting_peers()
     {
        let (scope, alpha_path) = temporary_database_scope("online-reload-alpha");
        let beta_path = scope.path().join("beta.redb");
        let gamma_path = scope.path().join("gamma.redb");

        let config_path = write_multi_database_toml(
            scope.path(),
            "riffdb-grpc-loopback",
            &[("alpha", &alpha_path), ("beta", &beta_path)],
        );
        let mut current_config =
            ServerConfig::parse_config_file(&config_path, scope.path()).expect("initial config");
        let started_at = Timestamp::new(1_700_000_000, 0).expect("valid started_at");
        let migration_exclusion = Arc::new(MigrationProcessExclusion::new());

        let (routes, hosted_mcp, mut graphs, mut monitors, event_sender, _event_receiver) =
            start_multi_database_harness(&current_config, started_at, &migration_exclusion).await;

        let alpha_alias = DatabaseAlias::new("alpha").expect("alpha alias");
        let beta_alias = DatabaseAlias::new("beta").expect("beta alias");
        let gamma_alias = DatabaseAlias::new("gamma").expect("gamma alias");

        assert!(routes.contains(&alpha_alias));
        assert!(routes.contains(&beta_alias));
        assert!(!routes.contains(&gamma_alias));
        assert!(hosted_mcp.as_ref().unwrap().contains(&alpha_alias));
        assert!(hosted_mcp.as_ref().unwrap().contains(&beta_alias));

        // Update the TOML configuration to retain `alpha`, remove `beta`, and add `gamma`.
        write_multi_database_toml(
            scope.path(),
            "riffdb-grpc-loopback",
            &[("alpha", &alpha_path), ("gamma", &gamma_path)],
        );
        let candidate_config =
            ServerConfig::parse_config_file(&config_path, scope.path()).expect("candidate config");

        let summary = reconcile_multi_database_inventory(
            &mut current_config,
            candidate_config,
            started_at,
            &migration_exclusion,
            &routes,
            &hosted_mcp,
            &mut graphs,
            &mut monitors,
            &event_sender,
        )
        .await
        .expect("reconcile multi-database inventory");

        assert_eq!(summary.added, vec![gamma_alias.clone()]);
        assert_eq!(summary.removed, vec![beta_alias.clone()]);
        assert!(routes.contains(&alpha_alias));
        assert!(!routes.contains(&beta_alias));
        assert!(routes.contains(&gamma_alias));
        assert!(hosted_mcp.as_ref().unwrap().contains(&alpha_alias));
        assert!(!hosted_mcp.as_ref().unwrap().contains(&beta_alias));
        assert!(hosted_mcp.as_ref().unwrap().contains(&gamma_alias));

        // Verify that retained `alpha` and newly provisioned `gamma` remain active on their routes.
        assert!(!graphs[0].deprovisioned);
        assert!(graphs[0].graph.is_some());
        assert!(graphs[1].deprovisioned);
        assert!(graphs[1].graph.is_none());
        assert!(!graphs[2].deprovisioned);
        assert!(graphs[2].graph.is_some());

        // Verify that deprovisioned `beta` released its redb lock and wrote CleanCloseState::Clean.
        let prepared = GenerationInputs::load(&current_config).expect("load generation inputs");
        let reopened_beta = open_redb_startup_with_extent_profile(
            &beta_path,
            prepared.startup_inputs,
            &prepared.identifiers.database_ids(),
            current_config.redb_commit_profile(),
            riffdb_storage_redb::JournalExtentProfile::Compact,
        )
        .expect("reopen deprovisioned beta database immediately without lock contention");
        let (_id, _retained, _history, _lifecycle, _capacity, ports) = reopened_beta.into_parts();
        assert!(
            ports.clean_close_fast_startup(),
            "deprovisioned database must record CleanCloseState::Clean"
        );
        let _ = ports.complete_graceful_close();
        drop(ports);

        // Re-adding `beta` online reuses the deprovisioned slot cleanly.
        write_multi_database_toml(
            scope.path(),
            "riffdb-grpc-loopback",
            &[
                ("alpha", &alpha_path),
                ("beta", &beta_path),
                ("gamma", &gamma_path),
            ],
        );
        let readd_config =
            ServerConfig::parse_config_file(&config_path, scope.path()).expect("readd config");
        let readd_summary = reconcile_multi_database_inventory(
            &mut current_config,
            readd_config,
            started_at,
            &migration_exclusion,
            &routes,
            &hosted_mcp,
            &mut graphs,
            &mut monitors,
            &event_sender,
        )
        .await
        .expect("re-provision beta online");
        assert_eq!(readd_summary.added, vec![beta_alias.clone()]);
        assert!(routes.contains(&beta_alias));
        assert!(hosted_mcp.as_ref().unwrap().contains(&beta_alias));

        shutdown_harness(hosted_mcp, graphs, monitors).await;
    }

    // req: NET-005, REC-001, REC-002, REC-004
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn invalid_or_incompatible_multi_database_reload_fails_closed_and_retains_active_inventory()
     {
        let (scope, alpha_path) = temporary_database_scope("online-reload-fail-closed");
        let beta_path = scope.path().join("beta.redb");

        let config_path = write_multi_database_toml(
            scope.path(),
            "riffdb-grpc-loopback",
            &[("alpha", &alpha_path), ("beta", &beta_path)],
        );
        let mut current_config =
            ServerConfig::parse_config_file(&config_path, scope.path()).expect("initial config");
        let started_at = Timestamp::new(1_700_000_000, 0).expect("valid started_at");
        let migration_exclusion = Arc::new(MigrationProcessExclusion::new());

        let (routes, hosted_mcp, mut graphs, mut monitors, event_sender, _event_receiver) =
            start_multi_database_harness(&current_config, started_at, &migration_exclusion).await;

        let alpha_alias = DatabaseAlias::new("alpha").expect("alpha alias");
        let beta_alias = DatabaseAlias::new("beta").expect("beta alias");

        // 1. Mutating a process-global setting (audience) fails closed with ProcessGlobalChanged.
        write_multi_database_toml(
            scope.path(),
            "mutated-audience",
            &[("alpha", &alpha_path), ("beta", &beta_path)],
        );
        let mutated_global =
            ServerConfig::parse_config_file(&config_path, scope.path()).expect("parse mutated");
        assert_eq!(
            reconcile_multi_database_inventory(
                &mut current_config,
                mutated_global,
                started_at,
                &migration_exclusion,
                &routes,
                &hosted_mcp,
                &mut graphs,
                &mut monitors,
                &event_sender,
            )
            .await
            .unwrap_err(),
            InventoryReloadError::ProcessGlobalChanged
        );
        assert_eq!(current_config.audience().as_str(), "riffdb-grpc-loopback");

        // 2. Mutating a retained database's storage path fails closed with RetainedDatabaseChanged.
        let relocated_beta = scope.path().join("beta-relocated.redb");
        write_multi_database_toml(
            scope.path(),
            "riffdb-grpc-loopback",
            &[("alpha", &alpha_path), ("beta", &relocated_beta)],
        );
        let mutated_retained = ServerConfig::parse_config_file(&config_path, scope.path())
            .expect("parse relocated beta");
        assert_eq!(
            reconcile_multi_database_inventory(
                &mut current_config,
                mutated_retained,
                started_at,
                &migration_exclusion,
                &routes,
                &hosted_mcp,
                &mut graphs,
                &mut monitors,
                &event_sender,
            )
            .await
            .unwrap_err(),
            InventoryReloadError::RetainedDatabaseChanged
        );

        // 3. Provisioning a newly added database whose path is a directory fails closed with
        // ProvisioningFailed before mutating any routes or deprovisioning peers.
        let invalid_gamma_dir = scope.path().join("gamma-is-a-directory.redb");
        fs::create_dir_all(&invalid_gamma_dir).expect("create directory at database path");
        write_multi_database_toml(
            scope.path(),
            "riffdb-grpc-loopback",
            &[("alpha", &alpha_path), ("gamma", &invalid_gamma_dir)],
        );
        let failing_candidate = ServerConfig::parse_config_file(&config_path, scope.path())
            .expect("parse candidate with directory path");
        assert_eq!(
            reconcile_multi_database_inventory(
                &mut current_config,
                failing_candidate,
                started_at,
                &migration_exclusion,
                &routes,
                &hosted_mcp,
                &mut graphs,
                &mut monitors,
                &event_sender,
            )
            .await
            .unwrap_err(),
            InventoryReloadError::ProvisioningFailed
        );

        // Both `alpha` and `beta` remain active and routable on gRPC and Hosted MCP.
        assert!(routes.contains(&alpha_alias));
        assert!(routes.contains(&beta_alias));
        assert!(hosted_mcp.as_ref().unwrap().contains(&alpha_alias));
        assert!(hosted_mcp.as_ref().unwrap().contains(&beta_alias));
        assert_eq!(
            current_config
                .databases()
                .iter()
                .map(|db| db.alias().as_str())
                .collect::<Vec<_>>(),
            vec!["alpha", "beta"]
        );

        shutdown_harness(hosted_mcp, graphs, monitors).await;
    }
}
