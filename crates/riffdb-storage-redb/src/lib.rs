#![forbid(unsafe_code)]

//! Durable redb implementation of RiffDB's semantic storage ports.

mod application;
mod codec;
mod error;
mod gate;
mod hooks;
mod keys;
mod layout;
mod reads;
mod store;

#[doc(hidden)]
pub use hooks::{RedbTestController, RedbTestEvent, RedbTestOperation, RedbTestPhase};
pub use store::{RedbDormantPorts, RedbOperationalPorts, RedbStore};
