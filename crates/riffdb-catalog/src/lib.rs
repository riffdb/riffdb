#![forbid(unsafe_code)]

//! Immutable contract validation, activation preparation, and startup history proof.
//!
//! This crate interprets checked contract IR and prepares the typed catalog
//! intent consumed by the commit coordinator. Its only direct mutation
//! capability is the move-only, pre-readiness index-migration driver; normal
//! operation observes only the coordinator's durable result.

mod bundle;
mod capability_partition;
mod deployment;
mod error;
mod event_materialization;
mod history;
mod lineage;
mod materialization;
mod notification;
mod projection_materialization;
mod query_module;

pub use bundle::*;
pub use capability_partition::*;
pub use deployment::*;
pub use error::*;
pub use event_materialization::*;
pub use history::*;
pub use materialization::*;
pub use notification::*;
pub use projection_materialization::*;
pub use query_module::*;
