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
mod history;
mod lineage;
mod materialization;
mod notification;

pub use bundle::*;
pub use capability_partition::*;
pub use deployment::*;
pub use error::*;
pub use history::*;
pub use materialization::*;
pub use notification::*;
