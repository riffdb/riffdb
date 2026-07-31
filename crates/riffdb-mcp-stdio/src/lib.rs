#![forbid(unsafe_code)]

//! Process bootstrap for the public-client MCP stdio bridge.
//!
//! MCP protocol adaptation belongs to `riffdb-api-mcp`. This crate owns only
//! process configuration, the hard telemetry boundary, and construction of the
//! ordinary public gRPC client used by the stdio transport.

mod backend;
mod config;
mod observer_backend;
mod response;
mod startup;
mod telemetry;
mod wire;

pub use backend::PublicGrpcMcpBackend;
pub use observer_backend::PublicGrpcMcpObserverBackend;
pub use startup::{StdioBootstrap, StdioRunError, StdioStartupError, bootstrap, doctor, run};
