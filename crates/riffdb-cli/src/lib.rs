#![forbid(unsafe_code)]

//! Public-only command-line workflows for a standalone RiffDB server.

mod agent;
mod app;
mod batch;
mod cli;
mod config;
mod config_diagnostics;
mod contract_explain;
mod credential;
mod input;
mod local_diagnostics;
mod local_outbox;
mod lsp;
mod output;
mod project;
mod readiness_diagnostics;
mod runner;
pub mod saga;
mod scaffold;
mod telemetry;
mod value;
mod version;

pub use app::run;
pub use version::early_version_from_env;

#[cfg(feature = "test-fixtures")]
pub mod test_fixtures {
    //! Closed credential-retention fixtures unavailable to normal builds.

    pub use crate::credential::{
        BootstrapRetentionFixtureError, BootstrapRetentionTestPoint,
        run_bootstrap_retention_fixture,
    };
}
