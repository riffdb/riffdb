#![forbid(unsafe_code)]

//! Durable redb implementation of RiffDB's semantic storage ports.

mod administration;
mod application;
mod codec;
mod derived;
mod error;
mod gate;
mod hooks;
mod keys;
mod layout;
mod reads;
mod startup;
mod store;

#[doc(hidden)]
pub use hooks::{RedbTestController, RedbTestEvent, RedbTestOperation, RedbTestPhase};
pub use startup::{
    RedbCompletionAuthority, RedbHistoricalEvidenceEnd, RedbStructuralEvidenceEnd,
    RedbStructuralEvidenceSession,
};
pub use store::{RedbDormantPorts, RedbOperationalPorts, RedbStore};
