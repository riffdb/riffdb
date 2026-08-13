#![forbid(unsafe_code)]

//! Exact, resumable application installation campaigns.
//!
//! This crate owns the API-neutral plan, stage, observation, and receipt
//! vocabulary for application installation. It deliberately owns no storage,
//! transport, authority, filesystem access, or application callbacks.

mod adapter;
mod campaign;
mod diff;
mod plan;
mod portability;
mod reimport_campaign;

pub use adapter::*;
pub use campaign::*;
pub use diff::*;
pub use plan::*;
pub use portability::*;
pub use reimport_campaign::*;
