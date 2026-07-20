#![forbid(unsafe_code)]

//! Durable redb implementation of RiffDB's semantic storage ports.

mod application;
mod codec;
mod error;
mod gate;
mod keys;
mod layout;
mod reads;
mod store;

pub use store::{RedbDormantPorts, RedbOperationalPorts, RedbStore};
