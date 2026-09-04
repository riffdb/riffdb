#![forbid(unsafe_code)]

//! Deterministic in-memory implementation of RiffDB's semantic storage ports.

mod administration;
mod application;
mod application_export;
mod composite_view;
mod consumer;
mod derived;
mod gate;
mod integrity_administration;
mod integrity_command;
mod integrity_projection;
mod migration;
mod owned_snapshot;
#[cfg(test)]
mod query;
mod startup;
mod state;
mod store;

pub use migration::{
    MemoryMigrationHistoryWitness, MemoryMigrationJournal, MemoryMigrationRecord,
    MemoryMigrationSnapshot, MemoryMigrationStage,
};
pub use startup::{
    MemoryCompletionAuthority, MemoryDormantPorts, MemoryHistoricalEvidenceEnd,
    MemoryStartupIndexMigrationPort, MemoryStructuralEvidenceEnd, MemoryStructuralEvidenceSession,
};
pub use store::{MemoryOperationalPorts, MemoryStore};
