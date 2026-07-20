#![forbid(unsafe_code)]

//! Coordinator-owned semantic ports and orchestration values for RiffDB.
//!
//! This crate owns consumer interfaces for authoritative orchestration. It does
//! not provide operating-system clock or identifier sources, expose a storage
//! mutation handle, or define a transport-facing service API.

mod clock;
mod initialization;
mod outcome;
mod provenance;

pub use clock::{
    AdministrationClock, AdministrationClockError, AdmissionClock, AdmissionClockError,
};
pub use initialization::{
    DatabaseInitializationCompletion, DatabaseInitializationDecision,
    DatabaseInitializationExecutor, DatabaseInitializationPermit, InitializedDatabase,
};
pub use outcome::{CommittedOutcome, CommittedOutcomeDisposition};
pub use provenance::{ProvenanceIdSource, ProvenanceIdSourceError};
