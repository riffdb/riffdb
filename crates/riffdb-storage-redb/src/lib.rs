#![forbid(unsafe_code)]

//! Durable redb implementation of RiffDB's semantic storage ports.

mod codec;
mod error;
mod keys;
mod layout;
mod store;

pub use store::RedbStore;
