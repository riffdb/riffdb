#![forbid(unsafe_code)]

//! Deterministic in-memory implementation of RiffDB's semantic storage ports.

mod administration;
mod application;
mod derived;
mod gate;
mod integrity_administration;
mod integrity_command;
mod integrity_projection;
mod startup;
mod state;
mod store;

pub use startup::{
    MemoryCompletionAuthority, MemoryDormantPorts, MemoryHistoricalEvidenceEnd,
    MemoryStructuralEvidenceEnd, MemoryStructuralEvidenceSession,
};
pub use store::{MemoryOperationalPorts, MemoryStore};
