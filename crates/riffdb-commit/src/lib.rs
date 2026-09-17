#![forbid(unsafe_code)]
#![cfg_attr(
    not(test),
    deny(
        clippy::expect_used,
        clippy::panic,
        clippy::unreachable,
        clippy::unwrap_used
    )
)]

//! Coordinator-owned semantic ports and orchestration values for RiffDB.
//!
//! This crate owns consumer interfaces for authoritative orchestration. It does
//! not provide operating-system clock or identifier sources, expose a storage
//! mutation handle, or define a transport-facing service API.

mod audit;
mod audit_executor;
mod clock;
mod command_admission;
mod command_attempt;
mod command_execution;
mod command_execution_failure;
mod command_index;
mod command_preparation;
mod command_records;
mod command_validation;
mod control_plane;
mod coordinator_time;
mod idempotency_inspection;
mod initialization;
mod migration;
mod notification;
mod outcome;
mod provenance;
mod read_only_execution;
mod read_only_preparation;
mod service_values;
mod telemetry;
#[cfg(test)]
mod test_support;
mod writer_census;

pub use audit::{AdministrationAuditInputView, BootstrapCompoundAuditProof};
pub use audit_executor::{
    AdministrationAuditAdmissionError, AdministrationAuditCapacityPermit,
    AdministrationAuditExecutionError, AdministrationAuditExecutor, AdministrationAuditReceipt,
    CapabilityBootstrapReceipt, CapabilityBootstrapTerminalReceipt, CapabilityCreateReceipt,
    CapabilityRevokeReceipt, CatalogDeploymentReceipt, CommandExecutionAdmissionError,
    CommandExecutionCapacityPermit, CommandExecutionReceipt, CommandExecutor,
    CommandIdempotencyInspector, ControlPlaneExecutionAdmissionError,
    ControlPlaneExecutionCapacityPermit, ControlPlaneExecutor, CoordinatorLifecycleState,
    CoordinatorShutdownError, CoordinatorStartError, CoordinatorWorkloadCapacity,
    QueryModuleDeploymentReceipt, ReactiveModulePublicationReceipt, ReadOnlyExecutionReceipt,
    ReplicationAdministrationReceipt, ReplicationRegistrationMaintenanceReceipt,
    RunningCommandCoordinator,
};
pub use clock::{
    AdministrationClock, AdministrationClockError, AdmissionClock, AdmissionClockError,
};
pub use command_execution::{
    CommandExecutionError, CommandExecutionErrorKind, CommandExecutionResult,
    CoordinatorDurability, ExecutionFailedOutcome,
};
pub use command_preparation::{
    CommandCancellationHandle, CommandExecutionPreparation, CommandExecutionPreparationError,
    CommandRequestControl, PostEvaluationAuthorizationError, PostEvaluationCommandAuthorizer,
    queued_preparation_units,
};
pub use control_plane::{
    ActivatedCatalog, ActivatedQueryModule, CapabilityBootstrapCompletion,
    CapabilityBootstrapExecutionResult, CapabilityBootstrapOutcome, CapabilityBootstrapPreparation,
    CapabilityBootstrapTerminalPreparation, CapabilityCreateExecutionResult,
    CapabilityCreateOutcome, CapabilityCreatePreparation, CapabilityIdentity,
    CapabilityRevokeExecutionResult, CapabilityRevokeOutcome, CapabilityRevokePreparation,
    CapabilityTransition, CatalogDeploymentOutcome, CatalogDeploymentPreparation,
    CatalogDeploymentResult, ControlPlaneExecutionError, ControlPlaneExecutionErrorKind,
    ControlPlanePreparationError, ControlPlaneTerminalAudit, PublishedReactiveModule,
    QueryModuleDeploymentOutcome, QueryModuleDeploymentPreparation, QueryModuleDeploymentResult,
    ReactiveModulePublicationExecutionResult, ReactiveModulePublicationOutcome,
    ReactiveModulePublicationPreparation, ReplicationAdministrationExecutionResult,
    ReplicationAdministrationOutcome, ReplicationAdministrationRefusal,
    ReplicationAdministrationResultReceipt,
};
#[doc(hidden)]
pub use coordinator_time::CoordinatorMonotonicClock;
pub use idempotency_inspection::{
    CommandIdempotencyConfirmationError, CommandIdempotencyInspectionError,
    CommandIdempotencyInspectionErrorKind, CommandIdempotencyInspectionRequest,
    CommandIdempotencyPlanSelection, InspectedCommandIdempotency,
};
pub use initialization::{
    DatabaseInitializationCompletion, DatabaseInitializationDecision,
    DatabaseInitializationExecutor, DatabaseInitializationPermit, InitializedDatabase,
};
pub use migration::{
    MigrationApplyReport, MigrationCheckReport, MigrationCoordinator,
    MigrationProjectionBuildObservation, MigrationProjectionBuildPort, MigrationProjectionError,
};
pub use notification::{ApplicationCommitNotificationError, ApplicationCommitNotificationSink};
pub use outcome::{CommittedOutcome, CommittedOutcomeDisposition, CommittedOutcomeDurabilityError};
pub use provenance::{ProvenanceIdSource, ProvenanceIdSourceError};
pub use read_only_execution::{ReadOnlyExecuted, ReadOnlyExecutionResult};
pub use read_only_preparation::{ReadOnlyExecutionPreparation, ReadOnlyExecutionPreparationError};
pub use service_values::{ServiceUuidV7Source, ServiceUuidV7SourceError};
pub use telemetry::{
    CommandPipelineStage, CommitCallTerminal, CommitCommandTerminal, CommitExecutionFailureKind,
    CommitGroupDispatchReason, CommitIdempotencyObservation, CommitTelemetry, CommitTelemetryEvent,
    CommitUncertaintyResolution, CommitUncertaintyStage, CompletionLanePhase, NoopCommitTelemetry,
    PreparedEpochRollbackReason,
};
#[doc(hidden)]
pub use writer_census::{
    WRITER_BATCH_STAGE_LABELS_V1, WRITER_BATCH_WINDOW_COUNT_V1, WRITER_BATCH_WINDOW_WIDTH_V1,
    WriterBatchCensusV1, WriterBatchWindowV1, format_writer_batch_stages_v1_line,
    writer_batch_stage_census_v1,
};
