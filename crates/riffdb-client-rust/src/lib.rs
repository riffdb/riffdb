#![forbid(unsafe_code)]

//! Typed public gRPC client for RiffDB.
//!
//! This crate contains transport ergonomics only. Contract validation,
//! authorization, idempotency, command execution, and durable recovery remain
//! server-owned semantics.

mod capability;
mod client;
mod command;
mod credential_file;
mod ids;
mod maintenance;
mod metadata;
mod status;

/// Boundaries implemented by contract-generated ergonomic modules.
pub mod generated;

/// Exact public wire types used by the typed transport client.
///
/// Re-exporting the generated package lets public-API consumers construct
/// requests without taking a second direct dependency on the Proto owner.
pub use riffdb_proto::v1;

pub use capability::{
    BootstrapCapabilityCreateTemplate, CapabilityCreateTemplateError,
    NormalCapabilityCreateTemplate,
};
pub use client::{
    CommitNotificationStream, GeneratedExecution, GeneratedExecutionError, RiffDbClient,
};
pub use command::{AttemptBudget, CommandShapeError, IdempotentCommand};
pub use credential_file::{BearerCredentialFileError, load_protected_bearer_credential};
pub use ids::{
    IdentifierGenerationError, SystemIdSource, generate_agent_session_id, generate_capability_id,
    generate_offline_maintenance_operation_id, generate_request_id,
};
pub use maintenance::{CreateOfflineBackup, RestoreOfflineBackup};
pub use metadata::{
    BearerCredential, BootstrapCallMetadata, BootstrapCredential, CallMetadata, MetadataError,
    TraceParent,
};
pub use status::{
    ClientError, DetailsFreeStatus, OutcomeUnknown, ProtocolFailure, ProtocolFailureKind,
};

pub use riffdb_errors::{
    ErrorClass, PublicError, PublicErrorDetails, PublicErrorKind, RecoveryAction, ValidationCode,
    ValidationIssue, ValidationIssues, ValidationPath, ValidationPathSegment,
};
pub use riffdb_types::{
    BackupNameV1, BackupNameV1Error, OfflineMaintenanceOperationId,
    OfflineMaintenanceReplacementConfirmation,
};
