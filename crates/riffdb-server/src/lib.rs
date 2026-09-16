#![forbid(unsafe_code)]
#![cfg_attr(
    not(test),
    deny(
        clippy::expect_used,
        clippy::panic,
        clippy::unreachable,
        clippy::unwrap_used
    )
)]

//! Production composition and process providers for `riffdbd`.

/// Fixed stack reservation for dedicated production threads started after the
/// process-memory baseline. Tokio runtime workers use their separately owned
/// builder configuration.
pub(crate) const PRODUCTION_THREAD_STACK_BYTES: usize = 384 * 1024;

mod application_export_adapter;
mod application_reimport_adapter;
mod archive_worker;
mod auth_adapters;
mod clocks;
mod columnar_adapter;
mod columnar_worker;
mod config;
mod consumer_adapter;
mod consumer_token;
mod cursor;
mod daemon;
mod exact_text_adapter;
#[cfg(feature = "test-fixtures")]
mod exact_text_probe;
mod hosted_mcp;
mod identifiers;
mod installation_adapter;
mod lifecycle;
mod lifecycle_service;
mod maintenance_adapter;
mod maintenance_driver;
mod maintenance_lifecycle;
mod maintenance_migration;
mod maintenance_recovery_controller;
mod notifications;
mod operational_status;
mod outbox_adapter;
mod port_driver;
mod process_graph;
mod projection_adapter;
mod projection_read_source;
mod projection_worker;
mod read_adapters;
#[cfg(test)]
mod real_storage_support;
mod recovery_host;
pub mod replication_bootstrap;
pub mod replication_publication;
mod replication_source;
mod restore_retry_host;
mod runtime_support;
mod server_generation;
mod shutdown_census;
mod startup;
mod startup_census;
mod storage;

pub use daemon::riffdbd_main;

#[cfg(feature = "test-fixtures")]
pub mod test_fixtures {
    //! Closed process-level recovery fixtures unavailable to normal builds.

    pub use crate::archive_worker::install_archive_progress_probe;
    pub use crate::exact_text_probe::{ExactProviderTestPoint, install_exact_provider_probe};
    pub use crate::maintenance_recovery_controller::MaintenanceRecoveryTestPoint;

    use std::process::ExitCode;

    /// Runs the full daemon with exactly one explicitly injected abort point.
    ///
    /// The normal `riffdbd` entrypoint has no route to this function.
    #[must_use]
    pub fn riffdbd_main_with_maintenance_abort(point: MaintenanceRecoveryTestPoint) -> ExitCode {
        crate::daemon::riffdbd_test_fixture_main(point)
    }
}

/// Verified administrative source connection used by follower composition.
pub use daemon::replication_peer::VerifiedReplicationPeer;
