//! Closed operator events. No constructor accepts caller prose or raw values.

use riffdb_types::exact_provider_budget_v1::{BudgetObservation, EXACT_PROVIDER_BUDGET_BYTES};
use serde_json::{Value, json};
use tracing::Level;

use crate::inventory_reload::InventoryReloadError;

pub(super) const TARGET: &str = "riffdb_server::operational";
pub(super) const MAX_RECORD_BYTES: usize = 16 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) enum Component {
    Server,
    Configuration,
    Projection,
    Outbox,
    Replication,
    Retention,
    Telemetry,
    Storage,
    Transport,
}

impl Component {
    pub(crate) fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "server" => Self::Server,
            "configuration" => Self::Configuration,
            "projection" => Self::Projection,
            "outbox" => Self::Outbox,
            "replication" => Self::Replication,
            "retention" => Self::Retention,
            "telemetry" => Self::Telemetry,
            "storage" => Self::Storage,
            "transport" => Self::Transport,
            _ => return None,
        })
    }

    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::Server => "server",
            Self::Configuration => "configuration",
            Self::Projection => "projection",
            Self::Outbox => "outbox",
            Self::Replication => "replication",
            Self::Retention => "retention",
            Self::Telemetry => "telemetry",
            Self::Storage => "storage",
            Self::Transport => "transport",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum Lifecycle {
    Starting,
    Stopped,
    Failed,
}

pub(crate) enum OperationalEvent<'a> {
    Failure(&'a crate::daemon::DaemonError),
    Diagnostic(super::diagnostics::Diagnostic<'a>),
    ShutdownProgress {
        stage: usize,
        complete: bool,
        elapsed_us: u64,
    },
    StorageRelease {
        complete: bool,
        elapsed_us: u64,
    },
    StartupSelection(&'a riffdb_storage_redb::RedbStructuralEvidenceSession),
    TransportReloadRejected {
        material: bool,
    },
    RuntimeStopped(crate::runtime_support::RuntimeStopReason),
    ArchiveUnavailable(&'a riffdb_types::ArchiveNameV1),
    MaintenanceDrainFailure,
    ArchiveFailure {
        name: &'a riffdb_types::ArchiveNameV1,
        error: riffdb_storage_api::ArchiveConsumerErrorV1,
    },
    RetentionRefused {
        site: crate::retention_worker::RetentionRefusalSite,
        kind: riffdb_storage_api::StorageErrorKind,
    },
    Lifecycle(Lifecycle),
    ReloadUnavailable,
    ReloadRejected {
        reason: InventoryReloadError,
        database: Option<&'a riffdb_types::DatabaseAlias>,
    },
    ReloadApplied {
        added: usize,
        removed: usize,
    },
    Provider(Option<BudgetObservation>),
    WorkerUnavailable(Component),
}

impl OperationalEvent<'_> {
    pub(super) fn description(&self) -> (&'static str, &'static str, Component, Level) {
        match self {
            Self::Diagnostic(super::diagnostics::Diagnostic::DebugProfile) => (
                "RDB-LOG-0901",
                "debug build cannot qualify release performance",
                Component::Server,
                Level::WARN,
            ),
            Self::Diagnostic(super::diagnostics::Diagnostic::PromotionRetryReady) => (
                "RDB-LOG-0902",
                "promotion retry ready",
                Component::Replication,
                Level::INFO,
            ),
            Self::Diagnostic(_) | Self::ShutdownProgress { .. } | Self::StorageRelease { .. } => (
                "RDB-LOG-0900",
                "bounded process evidence",
                Component::Server,
                Level::INFO,
            ),
            Self::StartupSelection(_) => (
                "RDB-LOG-0500",
                "startup validation selected",
                Component::Storage,
                Level::INFO,
            ),
            Self::TransportReloadRejected { .. } => (
                "RDB-LOG-0103",
                "TLS reload rejected; previous configuration remains active",
                Component::Transport,
                Level::WARN,
            ),
            Self::RuntimeStopped(_) => (
                "RDB-LOG-0005",
                "service runtime stopped",
                Component::Server,
                Level::ERROR,
            ),
            Self::ArchiveUnavailable(_) => (
                "RDB-LOG-0303",
                "archive source unavailable",
                Component::Replication,
                Level::WARN,
            ),
            Self::MaintenanceDrainFailure => (
                "RDB-LOG-0304",
                "maintenance background workers did not drain",
                Component::Server,
                Level::WARN,
            ),
            Self::ArchiveFailure { .. } => (
                "RDB-LOG-0301",
                "archive worker failed",
                Component::Replication,
                Level::WARN,
            ),
            Self::RetentionRefused { .. } => (
                "RDB-LOG-0302",
                "retention lane refused",
                Component::Retention,
                Level::ERROR,
            ),
            Self::Failure(_) => (
                "RDB-LOG-0004",
                "server lifecycle failed",
                Component::Server,
                Level::ERROR,
            ),
            Self::Lifecycle(Lifecycle::Starting) => (
                "RDB-LOG-0001",
                "server starting",
                Component::Server,
                Level::INFO,
            ),
            Self::Lifecycle(Lifecycle::Stopped) => (
                "RDB-LOG-0002",
                "server stopped",
                Component::Server,
                Level::INFO,
            ),
            Self::Lifecycle(Lifecycle::Failed) => (
                "RDB-LOG-0003",
                "server stopped without a clean boundary",
                Component::Server,
                Level::ERROR,
            ),
            Self::ReloadUnavailable => (
                "RDB-LOG-0100",
                "configuration reload file unavailable; previous configuration remains active",
                Component::Configuration,
                Level::WARN,
            ),
            Self::ReloadRejected { .. } => (
                "RDB-LOG-0101",
                "configuration reload rejected; previous configuration remains active",
                Component::Configuration,
                Level::WARN,
            ),
            Self::ReloadApplied { .. } => (
                "RDB-LOG-0102",
                "configuration reload applied",
                Component::Configuration,
                Level::INFO,
            ),
            Self::Provider(Some(_)) => (
                "RDB-LOG-0200",
                "exact provider process accounting",
                Component::Projection,
                Level::DEBUG,
            ),
            Self::Provider(None) => (
                "RDB-LOG-0201",
                "exact provider process accounting unavailable",
                Component::Projection,
                Level::WARN,
            ),
            Self::WorkerUnavailable(component) => (
                "RDB-LOG-0300",
                "background worker unavailable",
                *component,
                Level::WARN,
            ),
        }
    }

    pub(super) fn attributes(&self) -> Value {
        match self {
            Self::Diagnostic(snapshot) => json!({"evidence": snapshot.render()}),
            Self::ShutdownProgress {
                stage,
                complete,
                elapsed_us,
            } => {
                let stage = crate::process_graph::PRODUCTION_SHUTDOWN_STAGE_LABELS_V1
                    .get(*stage)
                    .copied()
                    .unwrap_or("unknown");
                let phase = if *complete { "complete" } else { "begin" };
                json!({"evidence": format!("riffdb-shutdown-progress-v1\tstage={stage}\tphase={phase}\telapsed_us={elapsed_us}")})
            }
            Self::StorageRelease {
                complete,
                elapsed_us,
            } => {
                let phase = if *complete { "complete" } else { "begin" };
                json!({"evidence": format!("riffdb-shutdown-progress-v1\tstage=graph_storage_release\tphase={phase}\telapsed_us={elapsed_us}")})
            }
            Self::StartupSelection(session) => {
                json!({"bounded_dirty": session.bounded_dirty_startup_path(), "clean_close_fast": !session.bounded_dirty_startup_path() && session.clean_close_declined_reason().is_none(), "decline": session.clean_close_declined_reason().map(|reason|reason.to_string())})
            }
            Self::TransportReloadRejected { material } => {
                json!({"cause": if *material { "tls_material" } else { "tls_configuration" }, "retained_previous": true, "fix": "correct_protected_tls_configuration"})
            }
            Self::RuntimeStopped(reason) => json!({"cause": format!("{reason:?}")}),
            Self::ArchiveUnavailable(name) => {
                json!({"archive": name.as_str(), "cause": "source_unavailable"})
            }
            Self::MaintenanceDrainFailure => json!({"joined": false}),
            Self::ArchiveFailure { name, error } => {
                json!({"archive": name.as_str(), "cause": error.to_string()})
            }
            Self::RetentionRefused { site, kind } => {
                json!({"diagnostic": crate::retention_worker::format_retention_refusal(*site, *kind)})
            }
            Self::Failure(error) => {
                use crate::daemon::DaemonError;
                let mut attributes = json!({"cause": error.kind()});
                if let DaemonError::GraphBuild(source) = error {
                    attributes["class"] = json!(source.class());
                }
                if let DaemonError::Startup(crate::startup::RedbStartupError::Storage(source)) =
                    error
                {
                    attributes["class"] = json!(format!("{:?}", source.kind()));
                }
                attributes
            }
            Self::ReloadUnavailable => {
                json!({"cause": "config_file_unavailable", "retained_previous": true, "fix": "restore_protected_configuration_file"})
            }
            Self::ReloadRejected { reason, database } => {
                let fix = match reason {
                    InventoryReloadError::RetainedDatabaseChanged
                    | InventoryReloadError::ProcessGlobalChanged
                    | InventoryReloadError::IncompatibleMode => {
                        "restart_server_to_apply_configuration"
                    }
                    InventoryReloadError::DeprovisionBusy => "wait_for_maintenance_to_complete",
                    _ => "correct_configuration_and_retry",
                };
                let mut attributes = json!({"cause": reason.kind(), "retained_previous": true, "fix": fix,
                "setting_role": match reason {
                    InventoryReloadError::RetainedDatabaseChanged => "database_configuration",
                    InventoryReloadError::ProcessGlobalChanged => "server_configuration",
                    _ => "configuration_document",
                }});
                if let Some(alias) = database {
                    attributes["database_alias"] = json!(alias.as_str());
                }
                attributes
            }
            Self::ReloadApplied { added, removed } => {
                json!({"added_databases": added, "removed_databases": removed})
            }
            Self::Provider(None) => json!({"available": false}),
            Self::Provider(Some(s)) => json!({
                "available": true, "limit_bytes": EXACT_PROVIDER_BUDGET_BYTES,
                "fixed_owner_bytes": s.fixed_owner_bytes, "owned_bytes": s.owned_bytes,
                "reserved_bytes": s.reserved_bytes, "reserved_work": s.reservations,
                "allocations": s.allocations, "released_allocations": s.released_allocations,
                "active_owners": s.active_owners, "retiring_owners": s.retiring_owners,
                "pinned_owners": s.pinned_owners, "pins": s.pins,
                "released_owners": s.released_owners,
            }),
            _ => json!({}),
        }
    }
}

/// The enum and domain types are the redaction boundary, before any subscriber.
pub(crate) fn emit(event: OperationalEvent<'_>) {
    let (code, message, component, level) = event.description();
    let attributes = event.attributes().to_string();
    macro_rules! record {
        ($level:expr) => {
            tracing::event!(target: TARGET, $level,
                event_code = code, component = component.name(), message = message,
                attributes_json = attributes.as_str())
        }
    }
    match level {
        Level::ERROR => record!(Level::ERROR),
        Level::WARN => record!(Level::WARN),
        Level::INFO => record!(Level::INFO),
        Level::DEBUG => record!(Level::DEBUG),
        Level::TRACE => record!(Level::TRACE),
    }
}
