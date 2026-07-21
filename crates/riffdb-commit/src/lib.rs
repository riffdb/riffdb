#![forbid(unsafe_code)]

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
mod idempotency_inspection;
mod initialization;
mod outcome;
mod provenance;
mod read_only_execution;
mod read_only_preparation;
#[cfg(test)]
mod test_support;

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
    ReadOnlyExecutionReceipt, RunningCommandCoordinator,
};
pub use clock::{
    AdministrationClock, AdministrationClockError, AdmissionClock, AdmissionClockError,
};
pub use command_execution::{
    CommandExecutionError, CommandExecutionErrorKind, CommandExecutionResult, CoordinatorDurability,
};
pub use command_preparation::{
    CommandCancellationHandle, CommandExecutionPreparation, CommandExecutionPreparationError,
    CommandRequestControl,
};
pub use control_plane::{
    ActivatedCatalog, CapabilityBootstrapCompletion, CapabilityBootstrapExecutionResult,
    CapabilityBootstrapOutcome, CapabilityBootstrapPreparation,
    CapabilityBootstrapTerminalPreparation, CapabilityCreateExecutionResult,
    CapabilityCreateOutcome, CapabilityCreatePreparation, CapabilityIdentity,
    CapabilityRevokeExecutionResult, CapabilityRevokeOutcome, CapabilityRevokePreparation,
    CapabilityTransition, CatalogDeploymentOutcome, CatalogDeploymentPreparation,
    CatalogDeploymentResult, ControlPlaneExecutionError, ControlPlaneExecutionErrorKind,
    ControlPlanePreparationError, ControlPlaneTerminalAudit,
};
pub use idempotency_inspection::{
    CommandIdempotencyConfirmationError, CommandIdempotencyInspectionError,
    CommandIdempotencyInspectionErrorKind, CommandIdempotencyInspectionRequest,
    CommandIdempotencyPlanSelection, InspectedCommandIdempotency,
};
pub use initialization::{
    DatabaseInitializationCompletion, DatabaseInitializationDecision,
    DatabaseInitializationExecutor, DatabaseInitializationPermit, InitializedDatabase,
};
pub use outcome::{CommittedOutcome, CommittedOutcomeDisposition};
pub use provenance::{ProvenanceIdSource, ProvenanceIdSourceError};
pub use read_only_execution::{ReadOnlyExecuted, ReadOnlyExecutionResult};
pub use read_only_preparation::{ReadOnlyExecutionPreparation, ReadOnlyExecutionPreparationError};
