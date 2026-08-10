#![forbid(unsafe_code)]

//! SIM-C2 (ADR-0113 Phase 1 items 5-7, SPEC SIM-004 and SIM-006): the seeded
//! exploration campaign over the simulated store.
//!
//! **PACKAGE STOPPED ON AN ENGINE FINDING**: the campaign's first exploration
//! beyond the hand-fixed fixtures surfaced a reachable torn-crash durable
//! state that panics redb 4.1.0 on every subsequent open (see
//! `campaign::finding_redb_reopen_panic_reproducer`). Per the standing rule a
//! real engine failure is a stop-and-report event, so the standing sweep, the
//! covered-row replays, and the corpus population are frozen
//! (`#[ignore]`-with-reason) rather than tuned around the bug; SIM-004 and
//! SIM-006 are NOT discharged by this tree. The generator and its determinism
//! pins, the SIM-006 classification guard, and the corpus mechanism are
//! complete and active.
//!
//! - [`generator`]: `WorkloadPlan::generate(seed, config)` — a deterministic,
//!   pure-value command sequence over the compiled fixture contract family
//!   (command mix, admission shapes, contention profile, value-size
//!   distribution, batch grouping), versioned by
//!   [`generator::WORKLOAD_GENERATOR_VERSION`] and pinned by same-seed
//!   equality, cross-seed inequality, and a golden-digest determinism test.
//! - [`campaign`]: `run_campaign(seed, config)` — drives the simulated store
//!   through the plan with `AuthoritativeCommandModel` in lockstep, a seeded
//!   crash schedule active across the whole run (bring-up, steady state, and
//!   recovery windows), full startup validation plus structural inspection and
//!   a model comparison at the recovered frontier after every recovery, and
//!   two-state crash-boundary acceptance for every interrupted transition.
//! - [`corpus`]: the SIM-004 found-seed regression corpus replayed per merge.
//! - [`subsumption`]: the SIM-006 classification of every `RECOVERY_SCENARIOS`
//!   row as campaign-covered, excluded with a reason, or out of the storage
//!   layer's scope — closed in both directions so a new row reds the guard
//!   until classified.
//!
//! Deep exploration (nightly-scale budget, no CI wiring on this machine):
//!
//! ```text
//! RIFFDB_SIM_EXPLORE_SEEDS=500 cargo test -p riffdb-sim --test seeded_campaign \
//!     -- --ignored explore --nocapture
//! ```

mod campaign;
mod corpus;
mod generator;
mod harness;
mod subsumption;
