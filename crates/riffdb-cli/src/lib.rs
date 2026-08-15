#![forbid(unsafe_code)]

//! Public-only command-line workflows for a standalone RiffDB server.

mod app;
mod batch;
mod cli;
mod config;
mod credential;
mod input;
mod output;
mod project;
mod runner;
mod scaffold;
mod value;

pub use app::run;

#[cfg(feature = "test-fixtures")]
pub mod test_fixtures {
    //! Closed credential-retention fixtures unavailable to normal builds.

    pub use crate::credential::{
        BootstrapRetentionFixtureError, BootstrapRetentionTestPoint,
        run_bootstrap_retention_fixture,
    };
}
