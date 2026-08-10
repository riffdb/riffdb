#![forbid(unsafe_code)]

//! SIM-C2 (ADR-0113 Phase 1 items 5-7, SPEC SIM-004 and SIM-006): the seeded
//! exploration campaign over the simulated store.
//!
//! FIRST REAL ENGINE CATCH: the campaign's first exploration beyond the
//! hand-fixed fixtures surfaced a reachable torn-crash durable state that
//! panics redb 4.1.0 on every subsequent open — file growth is not durable
//! until the one-phase commit's single fsync, so a torn crash can keep the
//! in-commit header while losing the extension, and 4.1.0 asserts instead of
//! repairing. Fixed upstream in `fd82ced` ("Make file growth durable to
//! avoid an unopenable database after a crash"), unreleased; the pinned
//! `=4.1.0` predates it. Until the pin advances the wedge is a TYPED,
//! pin-guarded placement exclusion (`subsumption::CAMPAIGN_PLACEMENT_EXCLUSIONS`,
//! flipped by `campaign::REDB_PIN_CONTAINS_FD82CED`), the corpus's inaugural
//! entry must keep reproducing it, and
//! `campaign::finding_redb_reopen_panic_reproducer` demonstrates it on
//! demand.
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
