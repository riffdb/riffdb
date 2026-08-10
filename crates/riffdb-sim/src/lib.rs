//! Deterministic simulation testing (DST) foundation for the durable engine.
//!
//! This crate is the ADR-0113 Phase-1 composition root: a simulated storage
//! medium ([`SimDisk`]) with a seeded fault schedule, a [`redb::StorageBackend`]
//! adapter ([`SimBackend`]), and a versioned execution trace hash whose digest
//! pins determinism (`SIM-001`): the same seed and trace format version must
//! reproduce a byte-identical digest.
//!
//! Everything in this crate is a pure function of its seed. There are no
//! clocks, no ambient entropy, no threads, and no hash-randomized collections;
//! `tests/architecture.rs` pins those absences, and it also pins ADR-0012's
//! rule that no production crate may depend on this one.

#![forbid(unsafe_code)]

mod backend;
mod disk;
mod rng;
mod trace;

pub use backend::SimBackend;
pub use disk::{FaultConfig, FaultCounters, SimDisk};
pub use rng::SplitMix64;
pub use trace::{TRACE_FORMAT_VERSION, TraceHash, fnv1a64};
