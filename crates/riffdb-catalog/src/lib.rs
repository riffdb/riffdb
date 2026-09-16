#![forbid(unsafe_code)]

//! Immutable contract validation, activation preparation, and startup history proof.
//!
//! This crate interprets checked contract IR and prepares the typed catalog
//! intent consumed by the commit coordinator. Its only direct mutation
//! capability is the move-only, pre-readiness index-migration driver; normal
//! operation observes only the coordinator's durable result.

mod application_catalog;
mod bundle;
mod capability_partition;
mod command_prefix;
mod command_prefix_entity;
mod command_prefix_vector;
mod deployment;
mod error;
mod event_materialization;
mod event_replay;
mod history;
mod lineage;
mod materialization;
mod migration;
mod notification;
mod projection_materialization;
mod projection_provider;
mod query_module;
mod reactive_module;

pub use application_catalog::*;
pub use bundle::*;
pub use capability_partition::*;
pub use command_prefix::*;
pub use command_prefix_vector::*;
pub use deployment::*;
pub use error::*;
pub use event_materialization::*;
pub use event_replay::*;
pub use history::*;
pub use materialization::*;
pub use migration::*;
pub use notification::*;
pub use projection_materialization::*;
pub use projection_provider::*;
pub use query_module::*;
pub use reactive_module::*;
