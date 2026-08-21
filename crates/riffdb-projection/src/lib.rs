#![forbid(unsafe_code)]

//! Event-derived projection orchestration over semantic storage ports.
//!
//! Authoritative entity and commit state never depends on this crate. Projection
//! rows, markers, and frontiers are derived state and can be rebuilt.

mod control;
mod error;
mod evaluator;
mod exact_predicate;
mod exact_text;
mod hooks;
mod migration;
mod notification;
mod query;
mod recovery;
mod registry;
mod result_set_epoch;

pub use control::*;
pub use error::*;
pub use evaluator::*;
pub use exact_predicate::*;
pub use exact_text::*;
pub use hooks::*;
pub use migration::*;
pub use notification::*;
pub use query::*;
pub use recovery::*;
pub use registry::*;
pub use result_set_epoch::*;
