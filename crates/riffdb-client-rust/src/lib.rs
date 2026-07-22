#![forbid(unsafe_code)]

//! Typed public gRPC client for RiffDB.
//!
//! This crate contains transport ergonomics only. Contract validation,
//! authorization, idempotency, command execution, and durable recovery remain
//! server-owned semantics.

mod client;
mod command;
mod ids;
mod metadata;
mod status;

/// Boundaries implemented by contract-generated ergonomic modules.
pub mod generated;

pub use client::{
    CommitNotificationStream, GeneratedExecution, GeneratedExecutionError, RiffDbClient,
};
pub use command::{AttemptBudget, CommandShapeError, IdempotentCommand};
pub use ids::{
    IdentifierGenerationError, SystemIdSource, generate_agent_session_id, generate_capability_id,
    generate_request_id,
};
pub use metadata::{
    BearerCredential, BootstrapCallMetadata, BootstrapCredential, CallMetadata, MetadataError,
    TraceParent,
};
pub use status::{
    ClientError, DetailsFreeStatus, OutcomeUnknown, ProtocolFailure, ProtocolFailureKind,
};
