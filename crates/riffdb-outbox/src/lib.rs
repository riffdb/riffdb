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
mod policy;
mod recovery;
mod status;
mod telemetry;
mod worker;

pub use connector::*;
pub use error::*;
pub use failpoint::*;
pub use policy::*;
pub use recovery::*;
pub use status::*;
pub use telemetry::*;
pub use worker::*;
