#![forbid(unsafe_code)]

//! Bounded at-least-once delivery over atomically committed outbox intents.
//!
//! This crate owns worker policy and external delivery orchestration. It can
//! mutate only the derived delivery-status overlay exposed by
//! [`riffdb_storage_api::OutboxRepository`]. It cannot create or repair an
//! authoritative event or outbox intent.

mod connector;
mod error;
mod failpoint;
mod local_archive;
mod local_archive_custody;
mod local_archive_hooks;
mod policy;
mod recovery;
mod status;
mod telemetry;
mod worker;

pub use connector::*;
pub use error::*;
pub use failpoint::*;
pub use local_archive::*;
#[cfg(feature = "test-fixtures")]
pub use local_archive_hooks::{LocalArchiveCrashHooks, LocalArchiveCrashPoint};
pub use policy::*;
pub use recovery::*;
pub use status::*;
pub use telemetry::*;
pub use worker::*;
