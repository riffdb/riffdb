#![forbid(unsafe_code)]

//! Production composition and process providers for `riffdbd`.

mod application_export_adapter;
mod application_reimport_adapter;
mod auth_adapters;
mod clocks;
mod columnar_adapter;
mod columnar_worker;
mod config;
mod consumer_adapter;
mod consumer_token;
mod cursor;
mod daemon;
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
mod projection_worker;
mod read_adapters;
#[cfg(test)]
mod real_storage_support;
mod recovery_host;
mod restore_retry_host;
mod runtime_support;
mod server_generation;
mod startup;
mod storage;

pub use daemon::riffdbd_main;

#[cfg(feature = "test-fixtures")]
pub mod test_fixtures {
    //! Closed process-level recovery fixtures unavailable to normal builds.

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
