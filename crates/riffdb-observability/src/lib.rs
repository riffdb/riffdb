#![forbid(unsafe_code)]

//! Bounded, redaction-safe process telemetry and health aggregation.
//!
//! This crate consumes closed hooks from semantic crates. It never receives
//! credentials, capability tokens, command values, entity keys, source text, or
//! arbitrary metric labels. Server composition remains responsible for wiring
//! these adapters into the one application-service graph.

mod auth;
mod catalog;
mod commit;
mod conflict;
mod health;
mod mcp;
mod metrics;
mod policy;
mod service;
mod telemetry;
mod tracing_layer;

pub use auth::*;
pub use catalog::*;
pub use commit::*;
pub use conflict::*;
pub use health::*;
pub use mcp::*;
pub use metrics::*;
pub use policy::*;
pub use service::*;
pub use telemetry::*;
pub use tracing_layer::*;
