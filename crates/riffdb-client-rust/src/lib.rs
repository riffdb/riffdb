#![forbid(unsafe_code)]

//! Typed public gRPC client for RiffDB.
//!
//! This crate contains transport ergonomics only. Contract validation,
//! authorization, idempotency, command execution, and durable recovery remain
//! server-owned semantics.

mod application;
mod capability;
mod client;
mod command;
mod credential_file;
mod ids;
mod maintenance;
mod metadata;
mod projected;
mod reactive;
mod status;

/// Boundaries implemented by contract-generated ergonomic modules.
pub mod generated;

/// Exact symbolic application wire types used by the typed transport client.
pub use riffdb_proto::app::v1 as app_v1;
/// Exact public wire types used by the typed transport client.
///
/// Re-exporting the generated package lets public-API consumers construct
/// requests without taking a second direct dependency on the Proto owner.
pub use riffdb_proto::v1;

pub use application::{
    ApplicationCardinality, ApplicationClientError, ApplicationCommand, ApplicationCommandResult,
    ApplicationContract, ApplicationRecord, ApplicationResultField, ApplicationUuid,
    ApplicationValue, GeneratedBatchError, GeneratedBatchItem, GeneratedBatchOptions,
    GeneratedBatchProgress, GeneratedBatchResult, IdempotentTransportBatchError, NamedQuery,
    NamedQueryResult, QueryOptions, QueryResponseIdentity, StableApplicationClient,
    TypedCommandResult, TypedQueryResult,
};
pub use capability::{
    BootstrapCapabilityCreateTemplate, CapabilityCreateTemplateError,
    NormalCapabilityCreateTemplate,
};
pub use client::{
    CommitNotificationStream, EventConsumerResponseStream, GeneratedExecution,
    GeneratedExecutionError, LiveQueryUpdateStream, RiffDbClient,
};
pub use command::{AttemptBudget, CommandShapeError, IdempotentCommand};
pub use credential_file::{BearerCredentialFileError, load_protected_bearer_credential};
pub use ids::{
    IdentifierGenerationError, SystemIdSource, generate_agent_session_id, generate_capability_id,
    generate_contract_migration_operation_id, generate_offline_maintenance_operation_id,
    generate_request_id,
};
pub use maintenance::{
    ApplyContractMigration, CheckContractMigration, ContractMigrationSubmissionError,
    CreateOfflineBackup, RestoreOfflineBackup,
};
pub use metadata::{
    BearerCredential, BootstrapCallMetadata, BootstrapCredential, CallMetadata, MetadataError,
    TraceParent,
};
pub use projected::{
    ProjectedDegradedReason, ProjectedOrder, ProjectedPredicate, ProjectedQuery,
    ProjectedQueryOutcome, ProjectedReadyRow, ProjectedRebuildingReason, ProjectedResponseEncoding,
    ProjectedSortDirection,
};
pub use reactive::{
    ApplicationContextualBatch, ApplicationContextualHydration, ApplicationContextualReaction,
    ApplicationContextualWorkItem, ApplicationEvent, ApplicationEventBatch,
    ApplicationEventCheckpoint, ApplicationEventConsumer, ApplicationEventConsumerStatus,
    ApplicationEventDelivery, ApplicationEventId, ApplicationEventLeaseEvidence,
    ApplicationEventMutationResult, ApplicationEventResponseStream, ApplicationLiveQueryStream,
    ApplicationLiveQueryUpdate, ApplicationReactiveOperation, EventConsumerOptions,
    LiveQueryCheckpoint, LiveQueryCursor, LiveQueryPatch, LiveQueryPatchOperation,
    LiveQueryTerminal, TypedContextualBatch, TypedContextualWorkItem, TypedEventBatch,
    TypedEventDelivery, TypedLiveQueryReset, TypedLiveQuerySnapshot, TypedLiveQueryStream,
};
/// Freshness policy and commit token types used by projected queries.
pub use riffdb_types::{CommitToken, FreshnessPolicy, ProjectionFrontier};
pub use status::{
    ClientError, DetailsFreeStatus, OutcomeUnknown, ProtocolFailure, ProtocolFailureKind,
};

pub use riffdb_errors::{
    ApplicationError, ApplicationErrorCategory, ApplicationErrorCode, ApplicationErrorContext,
    ApplicationFixCode, ApplicationOperation, ApplicationRecoveryAction, ApplicationSourceSpan,
    ErrorClass, PublicError, PublicErrorDetails, PublicErrorKind, RecoveryAction, ValidationCode,
    ValidationIssue, ValidationIssues, ValidationPath, ValidationPathSegment,
};
pub use riffdb_types::{
    BackupNameV1, BackupNameV1Error, ContractMigrationOperationId, DEFAULT_DATABASE_ALIAS,
    DatabaseAlias, MigrationBundleHash, OfflineMaintenanceOperationId,
    OfflineMaintenanceReplacementConfirmation, RequestId,
};
