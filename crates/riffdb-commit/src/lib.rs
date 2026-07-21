#![forbid(unsafe_code)]

//! Coordinator-owned semantic ports and orchestration values for RiffDB.
//!
//! This crate owns consumer interfaces for authoritative orchestration. It does
//! not provide operating-system clock or identifier sources, expose a storage
//! mutation handle, or define a transport-facing service API.

mod audit;
mod audit_executor;
mod clock;
#[allow(dead_code)] // Private semantic slice consumed by the later command executor.
mod command_admission;
#[allow(dead_code)] // Private semantic slice consumed by the later command executor.
mod command_attempt;
#[allow(dead_code)] // Private semantic slice consumed by the later command executor.
mod command_index;
mod command_preparation;
#[allow(dead_code)] // Private semantic slice consumed by the later command executor.
mod command_validation;
mod initialization;
mod outcome;
mod provenance;

pub use audit::{AdministrationAuditInputView, BootstrapCompoundAuditProof};
pub use audit_executor::{
    AdministrationAuditAdmissionError, AdministrationAuditCapacityPermit,
    AdministrationAuditExecutionError, AdministrationAuditExecutor, AdministrationAuditReceipt,
    CoordinatorLifecycleState, CoordinatorShutdownError, CoordinatorStartError,
    CoordinatorWorkloadCapacity, RunningCommandCoordinator,
};
pub use clock::{
    AdministrationClock, AdministrationClockError, AdmissionClock, AdmissionClockError,
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
