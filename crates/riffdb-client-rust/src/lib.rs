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
#[cfg(feature = "exclusive-diagnostic")]
mod exclusive_diagnostic;
mod export;
mod ids;
mod installation;
mod maintenance;
mod metadata;
mod projected;
mod reactive;
mod reimport;
mod session;
mod status;
mod tls;

/// Boundaries implemented by contract-generated ergonomic modules.
pub mod generated;

/// Exact symbolic application wire types used by the typed transport client.
pub use riffdb_proto::app::v1 as app_v1;
/// Exact public wire types used by the typed transport client.
///
/// Re-exporting the generated package lets public-API consumers construct
/// requests without taking a second direct dependency on the Proto owner.
pub use riffdb_proto::v1;
#[doc(hidden)]
pub use riffdb_proto::{canonical_value_from_proto, canonical_value_to_proto};
#[doc(hidden)]
pub use riffdb_types::{CanonicalValue, decode_canonical_value};

pub use application::{
    ApplicationCardinality, ApplicationCatalogFeature, ApplicationCatalogFeatureState,
    ApplicationCatalogFeatureView, ApplicationCatalogPreflight, ApplicationClientError,
    ApplicationCommand, ApplicationCommandResult, ApplicationContract, ApplicationRecord,
    ApplicationResultField, ApplicationUuid, ApplicationValue, GeneratedBatchError,
    GeneratedBatchItem, GeneratedBatchOptions, GeneratedBatchProgress, GeneratedBatchResult,
    IdempotentTransportBatchError, MAX_GENERATED_BATCH_CONCURRENCY, NamedQuery, NamedQueryResult,
    QueryOptions, QueryResponseIdentity, StableApplicationClient, TypedCommandResult,
    TypedQueryResult, VectorInspectionPage, VectorModelVersionItem, VectorModelVersionSummary,
    VectorStalenessItem, VectorStalenessSummary, VectorStateInspection, VectorStateInspectionKind,
    VectorStateInspectionResult,
};
#[doc(hidden)]
pub use application::{raise_query_result, raise_value};
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
#[cfg(feature = "exclusive-diagnostic")]
#[doc(hidden)]
pub use exclusive_diagnostic::{
    ExclusiveDiagnosticConfiguration, ExclusiveDiagnosticEvidence,
    ExclusiveDiagnosticOperationEvidence,
};
pub use export::StartApplicationExport;
pub use ids::{
    IdentifierGenerationError, SystemIdSource, generate_agent_session_id,
    generate_application_export_operation_id, generate_application_installation_campaign_id,
    generate_capability_id, generate_contract_migration_operation_id,
    generate_offline_maintenance_operation_id, generate_request_id,
};
pub use installation::StartApplicationInstallation;
pub use maintenance::{
    ApplyContractMigration, CheckContractMigration, ContractMigrationSubmissionError,
    CreateOfflineBackup, RestoreOfflineBackup, RetireOfflineBackup,
};
pub use metadata::{
    BearerCredential, BootstrapCallMetadata, BootstrapCredential, CallMetadata, MetadataError,
    TraceParent,
};
pub use projected::{
    ProjectedAggregate, ProjectedAggregateGroup, ProjectedAggregateValue, ProjectedDegradedReason,
    ProjectedOrder, ProjectedPredicate, ProjectedQuery, ProjectedQueryOutcome, ProjectedReadyRow,
    ProjectedRebuildingReason, ProjectedResponseEncoding, ProjectedSortDirection,
    raise_projected_response_for_query,
};
pub use reactive::{
    ApplicationContextualBatch, ApplicationContextualHydration, ApplicationContextualReaction,
    ApplicationContextualWorkItem, ApplicationEvent, ApplicationEventBatch,
    ApplicationEventCheckpoint, ApplicationEventConsumer, ApplicationEventConsumerPublicStatus,
    ApplicationEventConsumerStatus, ApplicationEventDelivery, ApplicationEventId,
    ApplicationEventLeaseEvidence, ApplicationEventMutationResult, ApplicationEventProgressCursor,
    ApplicationEventPullDisposition, ApplicationEventResponseStream, ApplicationLiveQueryStream,
    ApplicationLiveQueryUpdate, ApplicationProtectedEventConsumerStatus,
    ApplicationReactiveOperation, EventConsumerOptions, LiveQueryCheckpoint, LiveQueryCursor,
    LiveQueryPatch, LiveQueryPatchOperation, LiveQueryTerminal, TypedContextualBatch,
    TypedContextualWorkItem, TypedEventBatch, TypedEventDelivery, TypedLiveQueryReset,
    TypedLiveQuerySnapshot, TypedLiveQueryStream,
};
pub use reimport::StartApplicationReimport;
/// Freshness policy and commit token types used by projected queries.
pub use riffdb_types::{CanonicalVector, CommitToken, FreshnessPolicy, ProjectionFrontier};
pub use session::{
    APPLICATION_SESSION_PROTOCOL_V1, ApplicationSessionConfigurationError,
    ApplicationSessionIdentity, MAX_APPLICATION_SESSION_IN_FLIGHT,
};
pub use status::{
    ClientError, DetailsFreeStatus, OutcomeUnknown, ProtocolFailure, ProtocolFailureKind,
};
pub use tls::{TlsClientFailure, TlsTrustReloadStatus, VerifiedTlsConnector};

pub use riffdb_errors::{
    ApplicationError, ApplicationErrorCategory, ApplicationErrorCode, ApplicationErrorContext,
    ApplicationFixCode, ApplicationOperation, ApplicationRecoveryAction, ApplicationSourceSpan,
    ErrorClass, PublicError, PublicErrorDetails, PublicErrorKind, RecoveryAction, ValidationCode,
    ValidationIssue, ValidationIssues, ValidationPath, ValidationPathSegment,
};
pub use riffdb_types::{
    ApplicationExportOperationId, ApplicationExportSelectionV1, ApplicationInstallationCampaignId,
    BackupNameV1, BackupNameV1Error, CapabilityApplicationExportScopeV1,
    CapabilityApplicationReimportScopeV1, ContractMigrationOperationId, DEFAULT_DATABASE_ALIAS,
    DatabaseAlias, MigrationBundleHash, OfflineMaintenanceOperationId,
    OfflineMaintenanceReplacementConfirmation, RequestId,
};
