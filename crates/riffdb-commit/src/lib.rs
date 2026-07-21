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
mod initialization;
mod outcome;
mod provenance;
#[cfg(test)]
mod test_support;

pub use audit::{AdministrationAuditInputView, BootstrapCompoundAuditProof};
pub use audit_executor::{
    AdministrationAuditAdmissionError, AdministrationAuditCapacityPermit,
    AdministrationAuditExecutionError, AdministrationAuditExecutor, AdministrationAuditReceipt,
    CommandExecutionAdmissionError, CommandExecutionCapacityPermit, CommandExecutionReceipt,
    CommandExecutor, CoordinatorLifecycleState, CoordinatorShutdownError, CoordinatorStartError,
    CoordinatorWorkloadCapacity, RunningCommandCoordinator,
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
pub use initialization::{
    DatabaseInitializationCompletion, DatabaseInitializationDecision,
    DatabaseInitializationExecutor, DatabaseInitializationPermit, InitializedDatabase,
};
pub use outcome::{CommittedOutcome, CommittedOutcomeDisposition};
pub use provenance::{ProvenanceIdSource, ProvenanceIdSourceError};
