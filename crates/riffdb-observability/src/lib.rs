#![forbid(unsafe_code)]

//! Bounded, redaction-safe process telemetry and health aggregation.
//!
//! This crate consumes closed hooks from semantic crates. It never receives
//! credentials, capability tokens, command values, entity keys, source text, or
//! arbitrary metric labels. Server composition remains responsible for wiring
//! these adapters into the one application-service graph.

mod health;
mod metrics;
mod telemetry;
mod tracing_layer;

pub use health::*;
pub use metrics::*;
pub use telemetry::*;
pub use tracing_layer::*;
