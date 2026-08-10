#![forbid(unsafe_code)]

//! SIM-C2 (ADR-0113 Phase 1 items 5-7, SPEC SIM-004 and SIM-006): the seeded
//! exploration campaign over the simulated store.
//!
//! - [`generator`]: `WorkloadPlan::generate(seed, config)` — a deterministic,
//!   pure-value command sequence over the compiled fixture contract family
//!   (command mix, admission shapes, contention profile, value-size
//!   distribution, batch grouping), versioned by
//!   [`generator::WORKLOAD_GENERATOR_VERSION`] and pinned by same-seed
//!   equality, cross-seed inequality, and a golden-digest determinism test.
//!
//! The campaign, corpus, and subsumption modules land with the following
//! commits.

mod generator;
