#![forbid(unsafe_code)]

//! Tonic transport bindings for RiffDB's API-neutral application service.
//!
//! The crate owns protocol framing, bounded Protobuf conversion, credential
//! handoff, and gRPC status mapping. It has no storage, catalog, policy,
//! command-runtime, or commit authority.

/// The one accepted details-free INTERNAL message before a public error exists.
pub const EMERGENCY_INTERNAL_MESSAGE: &str = "an internal error occurred";

#[cfg(feature = "server")]
mod authentication;
#[cfg(any(feature = "client", feature = "server"))]
mod codec;
#[cfg(feature = "server")]
mod conversion;
#[cfg(feature = "server")]
mod error;
#[cfg(feature = "server")]
mod server;

/// Generated Tonic clients, server traits, and server adapters for `riffdb.v1`.
///
/// Message types are not generated a second time. Every generated RPC uses the
/// checked-in [`riffdb_proto::v1`] messages through an external package mapping.
#[allow(missing_docs, clippy::large_enum_variant)]
pub mod generated {
    include!(concat!(env!("OUT_DIR"), "/riffdb.v1.rs"));
}

/// Generated Tonic bindings for the symbolic `riffdb.app.v1` service.
#[allow(missing_docs, clippy::large_enum_variant)]
pub mod generated_app {
    include!(concat!(env!("OUT_DIR"), "/riffdb.app.v1.rs"));
}

#[cfg(feature = "server")]
pub use authentication::*;
#[cfg(feature = "server")]
pub use conversion::*;
#[cfg(feature = "server")]
pub use error::*;
#[cfg(feature = "server")]
pub use server::*;
