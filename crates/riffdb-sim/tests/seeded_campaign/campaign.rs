//! D2 (SIM-C2): the seeded campaign — generate a plan, drive the simulated
//! store through it with `AuthoritativeCommandModel` in lockstep, keep the
//! seeded crash schedule armed across the WHOLE run (bring-up, steady state,
//! and the recovery windows themselves), and after every recovery run the
//! full startup-validation and structural-inspection pass plus the oracle
//! comparison at the recovered frontier before continuing the plan.
//!
//! STORE-LEVEL DETERMINISM BLOCKERS (deliberate scope limit, named per the
//! arc's SEMANTIC-TRACE-ONLY constraint): this module asserts semantic
//! outcomes only and MUST NOT assert trace-digest equality across store runs,
//! for the two independent reasons pinned since SIM-B:
//!
//! 1. redb 4.1.0 drains `pending_table_updates` from a
//!    `std::collections::HashMap` during commit, so the backend write order —
//!    and therefore the trace digest and the per-seed torn-write outcomes —
//!    varies run to run until redb orders that drain upstream.
//! 2. the journal worker thread's operations interleave with the writer's
//!    under the shared seeded-RNG lock, so fault-schedule draws are not
//!    seed-stable at store level and crash placement cannot be pinned to one
//!    operation window.
//!
//! What IS deterministic and pinned: the generator's command sequence for a
//! seed (generator determinism ≠ execution-trace determinism), versioned by
//! `WORKLOAD_GENERATOR_VERSION` — see `generator.rs`.
//!
//! Crash-boundary acceptance (the SIM-A two-state discipline carried through
//! SIM-C1, extended to batches and to bring-up transitions): after a crash,
//! an interrupted batch is present IN FULL (the frontier advanced by the
//! whole batch and the complete effect graphs compare model-equal) or absent
//! IN FULL — never partial; an interrupted phase-one admission left its
//! pending row durable or absent; an interrupted database initialization or
//! catalog activation either landed durably or rolled back. Interrupted work
//! resolved absent is re-admitted and re-attempted from the recovered
//! frontier; work resolved present is folded into the model and never
//! re-attempted.

use riffdb_sim::{FaultConfig, SimDisk};
use riffdb_storage_api::DatabaseInitializationResult;
use riffdb_storage_redb::RedbOperationalPorts;
use riffdb_testkit::inspection::DurableInspection;
use riffdb_testkit::model::{
    AuthoritativeCommandModel, StoreAgreement, verify_model_against_inspection,
};
use riffdb_types::FrontierPosition;

use crate::generator::{
    AdmissionShape, GeneratorConfig, PlannedStep, WORKLOAD_GENERATOR_VERSION, WorkloadPlan,
};
use crate::harness::{
    CatalogOutcome, CommandFixture, CommitAttempt, build_plan_fixtures, database_id,
    inspection_request, try_activate_catalog, try_admit_audited, try_commit_group, try_initialize,
    try_inspect, try_open_operational, try_open_simulated,
};

/// Whether the pinned redb contains upstream commit `fd82ced` ("Make file
/// growth durable to avoid an unopenable database after a crash",
/// 2026-06-13), the fix for the SIM-C2 finding: redb 4.1.0's one-phase
/// commit orders `set_len` growth against the in-commit header write only
/// through the commit's final fsync, so a torn crash inside a file-growing
/// commit's single-fsync window can durably keep the header while losing the
/// extension — a state 4.1.0 panics on at every subsequent open
/// (`page_manager.rs:231`) instead of repairing. The pinned `=4.2.0`
/// CONTAINS the fix: `PageManager::grow` calls `Storage::sync_file` to make
/// the extension durable before the larger layout can reach the on-disk
/// header, and an actually truncated file now returns
/// `StorageError::Corrupted("File truncated below stored layout: ...")`
/// from the header check instead of tripping an open-time assert.
///
/// Flipped to `true` when the pin advanced to `=4.2.0`. Everything
/// keyed on it flips together: [`run_campaign_outcome`] stops classifying
/// the wedge panic as an excluded placement (a wedge would again fail
/// loudly), and the corpus's inaugural entry flips from
/// must-reproduce-the-wedge to must-recover-cleanly. The manifest guard in
/// `subsumption.rs` reds if the pin moves while this constant still says
/// `false`, so the flip cannot be forgotten silently.
pub(crate) const REDB_PIN_CONTAINS_FD82CED: bool = true;

/// The panic text of the redb 4.1.0 wedge assert (the whole-expression
/// message of `assert!(storage.raw_file_len()? >= header.layout().len())`).
const REDB_WEDGE_ASSERT_TEXT: &str = "storage.raw_file_len()? >= header.layout().len()";

/// One campaign's complete configuration. `Copy` and const-constructible so
/// corpus entries can pin it verbatim.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CampaignConfig {
    /// Workload generator bounds and mix ratios.
    pub generator: GeneratorConfig,
    /// Half-open `operations-until-crash` range the schedule redraws from
    /// after every recovery — the crash schedule stays armed through
    /// bring-up, steady state, and recovery windows alike.
    pub crash_operations: (u64, u64),
    /// Crash budget: once this many crashes occurred, the schedule is
    /// disarmed so the campaign deterministically drives the plan to
    /// completion (the `SimDisk` termination seam).
    pub max_crashes: u64,
    /// Torn-write granularity handed to the fault schedule.
    pub torn_write_granularity: u64,
}

/// Counters proving what one campaign actually exercised. A campaign that
/// "passed" without crashing, tearing, or resolving a two-state boundary
/// would be vacuous evidence — sweep tests assert these aggregates stay live.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct CampaignReport {
    /// Seed the campaign ran under.
    pub seed: u64,
    /// Generator version the plan was drawn under.
    pub generator_version: u32,
    /// The plan's versioned identity digest.
    pub plan_digest: u64,
    /// Commands durably applied (equals the plan length on success).
    pub commands_applied: u64,
    /// Batches durably committed (including re-attempts that succeeded).
    pub batches_committed: u64,
    /// Crashes taken (scheduled; the campaign never crashes explicitly).
    pub crashes: u64,
    /// Completed recoveries.
    pub recoveries: u64,
    /// Crashes that landed inside a recovery window — after a previous crash
    /// but before its bring-up, inspection, and oracle comparison completed
    /// (crash during recovery, recovery of a recovery).
    pub recovery_window_crashes: u64,
    /// Seeded keep/drop/prefix-truncate decisions taken across recoveries.
    pub torn_decisions: u64,
    /// Largest single-recovery torn-decision count.
    pub max_torn_in_one_recovery: u64,
    /// Recoveries that resolved an interrupted batch commit as durably
    /// present in full (the crash landed after the engine commit).
    pub in_flight_commit_present: u64,
    /// Recoveries that resolved an interrupted batch commit as absent in
    /// full (the crash landed before the engine commit).
    pub in_flight_commit_absent: u64,
    /// Recoveries that resolved an interrupted phase-one admission as
    /// durably present.
    pub in_flight_admit_present: u64,
    /// Recoveries that resolved an interrupted phase-one admission as absent.
    pub in_flight_admit_absent: u64,
    /// Crash-interrupted database initializations later observed rolled back
    /// (the retry re-proved emptiness and installed).
    pub initialization_rolled_back: u64,
    /// Post-crash bring-ups that observed the one installed `DatabaseId`
    /// preserved (the crash landed after the initialization engine commit).
    pub initialization_survived: u64,
    /// Crash-interrupted catalog activations later observed rolled back.
    pub catalog_activation_rolled_back: u64,
    /// Crash-interrupted catalog activations later observed durably active.
    pub catalog_activation_survived: u64,
    /// Full startup-validation + inspection + oracle comparisons performed.
    pub oracle_comparisons: u64,
    /// The quiesced final verification's recovered frontier.
    pub final_frontier: u64,
}

/// The failure context every campaign panic carries: everything needed to
/// replay the run.
fn replay_context(seed: u64, config: &CampaignConfig, step: usize) -> String {
    format!(
        "replay: seed={seed:#x} generator_version={WORKLOAD_GENERATOR_VERSION} step_index={step} config={config:?}"
    )
}

fn frontier_value(frontier: FrontierPosition) -> u64 {
    match frontier {
        FrontierPosition::BeforeFirst => 0,
        FrontierPosition::AppliedThrough(sequence) => sequence.get(),
    }
}

/// Panics unless the failure was caused by a simulated crash: with transient
/// errors disabled and capacity unbounded, a refusal without a crash is a
/// genuine store defect, never schedule noise.
fn ensure_crashed(disk: &SimDisk, seed: u64, config: &CampaignConfig, step: usize, error: &str) {
    assert!(
        disk.is_crashed(),
        "operation failed without a simulated crash — a genuine store defect: \
         {error}\n{}",
        replay_context(seed, config, step)
    );
}

/// Applies one committed batch to the model at its acknowledgement point:
/// fused commands admit-and-apply, two-phase commands (already admitted at
/// their own acknowledgement point) only apply.
fn apply_batch_to_model(
    model: &mut AuthoritativeCommandModel,
    plan: &WorkloadPlan,
    fixtures: &[CommandFixture],
    members: &[u32],
) {
    for member in members {
        let index = usize::try_from(*member).expect("bounded command index");
        let fixture = &fixtures[index];
        if plan.commands[index].shape == AdmissionShape::VacantTerminal {
            model
                .admit_pending(fixture.pending.clone())
                .expect("model admits the fused pending");
        }
        model
            .apply_command(&fixture.records)
            .expect("model applies the acknowledged command");
    }
}

/// Asserts the agreement's per-family counts equal the model's own population
/// — the request covered the complete compared surface and the comparison
/// proved a non-trivial one.
fn assert_agreement_matches_model(
    agreement: &StoreAgreement,
    model: &AuthoritativeCommandModel,
    requested_ranges: usize,
    context: &str,
) {
    let commits = model.commit_count();
    assert_eq!(agreement.commits(), commits, "commits ({context})");
    assert_eq!(agreement.outcomes(), commits, "outcomes ({context})");
    assert_eq!(agreement.events(), commits, "events ({context})");
    assert_eq!(agreement.provenance(), commits, "provenance ({context})");
    assert_eq!(
        agreement.outbox_intents(),
        commits,
        "outbox intents ({context})"
    );
    assert_eq!(
        agreement.entities(),
        model.entities().count(),
        "entities ({context})"
    );
    assert_eq!(
        agreement.index_entries(),
        model.index_entries().count(),
        "index entries ({context})"
    );
    let epochs = model.index_epochs().count();
    assert_eq!(
        agreement.index_epochs(),
        epochs,
        "present index epochs ({context})"
    );
    assert_eq!(
        agreement.index_epochs_absent(),
        requested_ranges - epochs,
        "absent index epochs ({context})"
    );
    assert_eq!(
        agreement.admissions(),
        model.admissions().count(),
        "admissions ({context})"
    );
}

/// Outcome of one campaign under the pin-aware placement exclusion.
#[derive(Clone, Copy, Debug)]
pub(crate) enum CampaignOutcome {
    /// The campaign completed its plan with the oracle holding at every
    /// recovery and the quiesced final verification passing.
    Completed(CampaignReport),
    /// EXCLUDED PLACEMENT (redb 4.1.0 pin only): the drawn crash landed
    /// inside a file-growing commit's single-fsync window and the seeded torn
    /// recovery kept the in-commit header write while dropping the covering
    /// extension — the durable state redb 4.1.0 panics on at every open
    /// instead of repairing (fixed upstream in `fd82ced`, unreleased). See
    /// `subsumption::CAMPAIGN_PLACEMENT_EXCLUSIONS` for the typed exclusion
    /// this variant realizes; it exists only while
    /// [`REDB_PIN_CONTAINS_FD82CED`] is `false` — after the pin advances the
    /// panic propagates again, so a regressed wedge fails loudly.
    WedgedByRedb410FileGrowth {
        /// Crashes taken before the wedge state formed.
        crashes: u64,
        /// Torn decisions taken across the run's recoveries.
        torn_decisions: u64,
    },
}

/// Runs the campaign, classifying the known redb 4.1.0 file-growth wedge
/// panic as the typed excluded placement while the pin predates `fd82ced`.
/// Every OTHER panic — oracle divergence, partial batch, unexplained refusal,
/// or an unexpected engine panic — propagates unchanged.
pub(crate) fn run_campaign_outcome(seed: u64, config: CampaignConfig) -> CampaignOutcome {
    let disk = SimDisk::new(FaultConfig {
        seed,
        torn_write_granularity: config.torn_write_granularity,
        crash_after_operations: None,
        transient_error_denominator: 0,
        capacity_bytes: None,
    });
    if REDB_PIN_CONTAINS_FD82CED {
        return CampaignOutcome::Completed(run_campaign_on(seed, config, &disk));
    }
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        run_campaign_on(seed, config, &disk)
    })) {
        Ok(report) => CampaignOutcome::Completed(report),
        Err(payload) => {
            let message = payload
                .downcast_ref::<&str>()
                .map(|text| (*text).to_owned())
                .or_else(|| payload.downcast_ref::<String>().cloned())
                .unwrap_or_default();
            if message.contains(REDB_WEDGE_ASSERT_TEXT) {
                let counters = disk.counters();
                CampaignOutcome::WedgedByRedb410FileGrowth {
                    crashes: counters.crashes,
                    torn_decisions: counters.torn_kept
                        + counters.torn_dropped
                        + counters.torn_truncated,
                }
            } else {
                std::panic::resume_unwind(payload)
            }
        }
    }
}

/// The campaign body over a caller-held disk (the diagnostic seam: a probing
/// test can keep the disk handle to examine durable state after a panic).
/// Panics — with the full replay context — on any oracle divergence, partial
/// crash-boundary state, or refusal that no simulated crash explains. The
/// schedule arms AFTER the first store creation completes: a crash inside
/// the very first format creation leaves a half-created inventory that the
/// format preflight refuses BY DESIGN (fail-closed; the remedy is operator
/// recreation, which has no in-process recovery path to explore); everything
/// after that first creation — initialization, catalog activation, steady
/// state, and every recovery reopen — runs under the armed schedule, with
/// the durability premise asserted at arming time.
pub(crate) fn run_campaign_on(seed: u64, config: CampaignConfig, disk: &SimDisk) -> CampaignReport {
    let plan = WorkloadPlan::generate(seed, config.generator);
    let fixtures = build_plan_fixtures(seed, &plan);
    let disk = disk.clone();
    let mut model = AuthoritativeCommandModel::new();
    let mut report = CampaignReport {
        seed,
        generator_version: WORKLOAD_GENERATOR_VERSION,
        plan_digest: plan.identity_digest(),
        ..CampaignReport::default()
    };

    // Plan cursor and crash-boundary state.
    let mut cursor = 0_usize;
    let mut acknowledged = 0_u64;
    let mut touched = 0_usize;
    let mut in_flight: Option<usize> = None;
    let mut init_acknowledged = false;
    let mut init_attempted = false;
    let mut catalog_acknowledged = false;
    let mut catalog_attempted = false;
    let mut in_recovery_window = false;
    let mut armed = false;
    let mut disarmed = false;

    'campaign: loop {
        // ---- crash resolution -------------------------------------------
        if disk.is_crashed() {
            report.crashes += 1;
            if in_recovery_window {
                report.recovery_window_crashes += 1;
            }
            let before = disk.counters();
            let torn_before = before.torn_kept + before.torn_dropped + before.torn_truncated;
            let unsynced = disk.unsynced_mutation_count();
            disk.recover_after_crash();
            let after = disk.counters();
            let torn = after.torn_kept + after.torn_dropped + after.torn_truncated - torn_before;
            assert_eq!(
                torn,
                unsynced,
                "every unsynced mutation at the crash faces exactly one seeded decision\n{}",
                replay_context(seed, &config, cursor)
            );
            report.torn_decisions += torn;
            report.max_torn_in_one_recovery = report.max_torn_in_one_recovery.max(torn);
            report.recoveries += 1;
            in_recovery_window = true;
            if !disarmed && report.crashes >= config.max_crashes {
                disk.set_crash_after_operations(None);
                disarmed = true;
            }
        }

        // ---- bring-up phase 1: open + initialize ------------------------
        let mut store = match try_open_simulated(&disk) {
            Ok(store) => store,
            Err(error) => {
                ensure_crashed(&disk, seed, &config, cursor, &error);
                continue 'campaign;
            }
        };
        if !armed {
            // Premise check for the arming boundary: the completed first
            // creation left nothing unsynced, so no later crash can tear the
            // format inventory the preflight depends on.
            assert_eq!(
                disk.unsynced_mutation_count(),
                0,
                "store creation must be durably synced before the schedule \
                 arms\n{}",
                replay_context(seed, &config, cursor)
            );
            disk.set_crash_after_operations(Some(config.crash_operations));
            armed = true;
        }
        match try_initialize(&mut store) {
            Ok(DatabaseInitializationResult::Installed(id)) => {
                assert_eq!(id, database_id(), "installed identity is the campaign's");
                if init_attempted {
                    // The interrupted initialization's engine commit did NOT
                    // survive: the retry re-proved emptiness (the
                    // startup.initialization.before-engine-commit state).
                    report.initialization_rolled_back += 1;
                }
                init_attempted = false;
                init_acknowledged = true;
            }
            Ok(DatabaseInitializationResult::ConcurrentWinner(id)) => {
                assert_eq!(
                    id,
                    database_id(),
                    "a surviving database identity must be the campaign's one \
                     installed DatabaseId\n{}",
                    replay_context(seed, &config, cursor)
                );
                if in_recovery_window {
                    // The one installed DatabaseId survived the crash (the
                    // startup.initialization.after-engine-commit state —
                    // whether the initializing attempt was acknowledged or
                    // itself the interrupted one).
                    report.initialization_survived += 1;
                }
                init_attempted = false;
                init_acknowledged = true;
            }
            Err(error) => {
                ensure_crashed(&disk, seed, &config, cursor, &error);
                if !init_acknowledged {
                    init_attempted = true;
                }
                drop(store);
                continue 'campaign;
            }
        }

        // ---- recovery verification (after every crash) ------------------
        if in_recovery_window {
            let attempted_fixtures: Vec<&CommandFixture> = fixtures[..touched].iter().collect();
            let request = inspection_request(&attempted_fixtures);
            let inspection = match try_inspect(store, &request) {
                Ok(inspection) => inspection,
                Err(error) => {
                    ensure_crashed(&disk, seed, &config, cursor, &error);
                    continue 'campaign;
                }
            };
            let frontier = frontier_value(inspection.recovered_application_frontier());

            // Two-state resolution of the interrupted step, if any.
            match in_flight.take() {
                None => {
                    assert_eq!(
                        frontier,
                        acknowledged,
                        "with no in-flight work the recovered frontier must equal \
                         the acknowledged frontier\n{}",
                        replay_context(seed, &config, cursor)
                    );
                }
                Some(step) => match &plan.steps[step] {
                    PlannedStep::AdmitPending { command } => {
                        assert_eq!(
                            frontier,
                            acknowledged,
                            "an interrupted admission never moves the commit \
                             frontier\n{}",
                            replay_context(seed, &config, step)
                        );
                        let index = usize::try_from(*command).expect("bounded command index");
                        let fixture = &fixtures[index];
                        // Non-vacuity: the two acceptance states must differ.
                        let mut admitted = model.clone();
                        admitted
                            .admit_pending(fixture.pending.clone())
                            .expect("model admits the in-flight pending");
                        assert!(
                            model.first_divergence_from(&admitted).is_some(),
                            "the two admission acceptance states must actually differ"
                        );
                        let key = fixture
                            .pending
                            .identity()
                            .storage_key()
                            .expect("identity storage key");
                        let present = inspection
                            .admissions()
                            .iter()
                            .find(|admission| {
                                admission
                                    .identity()
                                    .storage_key()
                                    .expect("inspected identity storage key")
                                    == key
                            })
                            .is_some_and(|admission| admission.state().is_some());
                        if present {
                            // Present IN FULL: fold into the model; the oracle
                            // comparison below proves field-exact equality.
                            model = admitted;
                            report.in_flight_admit_present += 1;
                            cursor = step + 1;
                        } else {
                            // Absent IN FULL: re-admit by retrying the step.
                            report.in_flight_admit_absent += 1;
                            cursor = step;
                        }
                    }
                    PlannedStep::CommitBatch { commands } => {
                        let batch_len = commands.len() as u64;
                        let attempted = acknowledged + batch_len;
                        // Non-vacuity: baseline and in-flight states differ.
                        let mut applied = model.clone();
                        apply_batch_to_model(&mut applied, &plan, &fixtures, commands);
                        assert!(
                            model.first_divergence_from(&applied).is_some(),
                            "the two commit acceptance states must actually differ"
                        );
                        if frontier == attempted {
                            // The interrupted batch's durability fence
                            // completed: its COMPLETE effect graph must be
                            // present (proved by the comparison below).
                            model = applied;
                            acknowledged = attempted;
                            report.in_flight_commit_present += 1;
                            report.commands_applied += batch_len;
                            report.batches_committed += 1;
                            cursor = step + 1;
                        } else if frontier == acknowledged {
                            // Absent IN FULL: the whole batch is re-attempted.
                            report.in_flight_commit_absent += 1;
                            cursor = step;
                        } else {
                            panic!(
                                "recovered frontier {frontier} is neither the \
                                 acknowledged frontier {acknowledged} nor the \
                                 attempted frontier {attempted}: a PARTIAL batch \
                                 survived the crash — a genuine engine defect\n{}",
                                replay_context(seed, &config, step)
                            );
                        }
                    }
                },
            }

            // The oracle comparison at the recovered frontier.
            let agreement =
                verify_model_against_inspection(&model, &inspection).unwrap_or_else(|divergence| {
                    panic!(
                        "ORACLE DIVERGENCE at the recovered frontier {frontier}: \
                         {divergence}\n{}",
                        replay_context(seed, &config, cursor)
                    )
                });
            assert_agreement_matches_model(
                &agreement,
                &model,
                request.index_ranges().len(),
                "recovery",
            );
            report.oracle_comparisons += 1;
            in_recovery_window = false;

            // The inspection consumed its store; reopen for phase 2.
            store = match try_open_simulated(&disk) {
                Ok(store) => store,
                Err(error) => {
                    ensure_crashed(&disk, seed, &config, cursor, &error);
                    continue 'campaign;
                }
            };
        }

        // ---- bring-up phase 2: operational ports + catalog --------------
        let mut ports: RedbOperationalPorts = match try_open_operational(store) {
            Ok(ports) => ports,
            Err(error) => {
                ensure_crashed(&disk, seed, &config, cursor, &error);
                continue 'campaign;
            }
        };
        match try_activate_catalog(&mut ports) {
            Ok(CatalogOutcome::Activated) => {
                if catalog_attempted {
                    // The interrupted activation rolled back; this attempt
                    // durably activated.
                    report.catalog_activation_rolled_back += 1;
                }
                catalog_attempted = false;
                catalog_acknowledged = true;
            }
            Ok(CatalogOutcome::AlreadyActive) => {
                if catalog_attempted {
                    // The interrupted activation landed durably.
                    report.catalog_activation_survived += 1;
                } else {
                    assert!(
                        catalog_acknowledged,
                        "AlreadyActive without an acknowledged or interrupted \
                         activation\n{}",
                        replay_context(seed, &config, cursor)
                    );
                }
                catalog_attempted = false;
                catalog_acknowledged = true;
            }
            Err(error) => {
                ensure_crashed(&disk, seed, &config, cursor, &error);
                if !catalog_acknowledged {
                    catalog_attempted = true;
                }
                continue 'campaign;
            }
        }

        // ---- steady state: continue the plan from the recovered frontier
        while cursor < plan.steps.len() {
            if disk.is_crashed() {
                // An asynchronous crash (the journal worker consumed the
                // countdown between steps): nothing was in flight.
                continue 'campaign;
            }
            match &plan.steps[cursor] {
                PlannedStep::AdmitPending { command } => {
                    let index = usize::try_from(*command).expect("bounded command index");
                    touched = touched.max(index + 1);
                    match try_admit_audited(&ports, &fixtures[index]) {
                        Ok(()) => {
                            model
                                .admit_pending(fixtures[index].pending.clone())
                                .expect("model admits the acknowledged pending");
                            cursor += 1;
                        }
                        Err(error) => {
                            ensure_crashed(&disk, seed, &config, cursor, &error);
                            in_flight = Some(cursor);
                            continue 'campaign;
                        }
                    }
                }
                PlannedStep::CommitBatch { commands } => {
                    let last = *commands.last().expect("non-empty batch");
                    touched =
                        touched.max(usize::try_from(last).expect("bounded command index") + 1);
                    let members: Vec<&CommandFixture> = commands
                        .iter()
                        .map(|member| {
                            &fixtures[usize::try_from(*member).expect("bounded command index")]
                        })
                        .collect();
                    match try_commit_group(&ports, &members) {
                        CommitAttempt::Committed => {
                            apply_batch_to_model(&mut model, &plan, &fixtures, commands);
                            acknowledged += commands.len() as u64;
                            report.commands_applied += commands.len() as u64;
                            report.batches_committed += 1;
                            cursor += 1;
                        }
                        CommitAttempt::Refused { stage, detail } => {
                            ensure_crashed(
                                &disk,
                                seed,
                                &config,
                                cursor,
                                &format!("batch commit refused at {stage}: {detail}"),
                            );
                            in_flight = Some(cursor);
                            continue 'campaign;
                        }
                    }
                }
            }
        }

        // ---- quiesced termination: plan exhausted -----------------------
        drop(ports);
        if disk.is_crashed() {
            // The crash landed during the session close; recover, verify,
            // and come back here with nothing left to apply.
            continue 'campaign;
        }
        break 'campaign;
    }

    // ---- final verification: a clean shutdown is also a simulated recovery
    if !disarmed {
        disk.set_crash_after_operations(None);
    }
    assert_eq!(
        acknowledged,
        u64::from(plan.config.commands),
        "an exhausted plan acknowledged every command\n{}",
        replay_context(seed, &config, cursor)
    );
    let all_fixtures: Vec<&CommandFixture> = fixtures.iter().collect();
    let request = inspection_request(&all_fixtures);
    let store = try_open_simulated(&disk)
        .unwrap_or_else(|error| panic!("quiesced final open failed: {error}"));
    let inspection: DurableInspection = try_inspect(store, &request)
        .unwrap_or_else(|error| panic!("quiesced final inspection failed: {error}"));
    let frontier = frontier_value(inspection.recovered_application_frontier());
    assert_eq!(
        frontier,
        acknowledged,
        "a clean shutdown loses nothing\n{}",
        replay_context(seed, &config, cursor)
    );
    let agreement =
        verify_model_against_inspection(&model, &inspection).unwrap_or_else(|divergence| {
            panic!(
                "ORACLE DIVERGENCE at the quiesced final frontier {frontier}: \
                 {divergence}\n{}",
                replay_context(seed, &config, cursor)
            )
        });
    assert_agreement_matches_model(&agreement, &model, request.index_ranges().len(), "final");
    report.oracle_comparisons += 1;
    report.final_frontier = frontier;

    // Cross-check the disk's own counters: every crash was observed and
    // every recovery completed through this loop.
    let counters = disk.counters();
    assert_eq!(report.crashes, counters.crashes, "crash bookkeeping");
    assert_eq!(
        report.recoveries, counters.recoveries,
        "recovery bookkeeping"
    );
    report
}

// ---------------------------------------------------------------------------
// The standing per-merge sweep and the env-gated deep exploration.
// ---------------------------------------------------------------------------

/// The per-merge broad-sweep configuration: deliberately the common
/// commit-arms schedule (18 commands, crash range (1, 140), budget 14) —
/// dense enough that every campaign crashes repeatedly and recovery-window
/// crashes are common. The rare commit-PRESENT side is covered by the
/// separately scouted targeted witness in the same standing test below.
pub(crate) const SWEEP_CONFIG: CampaignConfig = CampaignConfig {
    generator: GeneratorConfig {
        commands: 18,
        max_targets: 8,
        two_phase_percent: 30,
        reuse_percent: 40,
        batch_percent: 40,
        max_batch_len: 3,
        note_len_max: 200,
    },
    crash_operations: (1, 140),
    max_crashes: 14,
    torn_write_granularity: 512,
};

/// First seed of the standing broad sweep. If the store's internal operation
/// stream shifts, wedged placements are counted openly and the aggregate
/// assertions are the falsifier. Commit-PRESENT has its own stable targeted
/// coordinate because its physical window is substantially rarer.
pub(crate) const SWEEP_SEED_BASE: u64 = 0x51C2_C0D0;
/// Number of seeds the standing sweep replays per merge.
pub(crate) const SWEEP_SEEDS: u64 = 24;

/// The standing per-merge regression battery: every sweep seed's campaign
/// completes with the oracle holding at every recovery, and the aggregate
/// counters prove the swept territory is real — crashes landed, torn
/// decisions were taken, crashes landed INSIDE recovery windows, and the
/// targeted companion exercises both sides of commit two-state acceptance.
/// A quiet aggregate is a reportable finding, never silence.
///
/// Seeds whose drawn placement lands in the redb 4.1.0 file-growth wedge
/// window are counted OPENLY as excluded placements (the typed exclusion in
/// `subsumption::CAMPAIGN_PLACEMENT_EXCLUSIONS`, upstream fix `fd82ced`
/// unreleased) — never silently skipped, and bounded so exclusions cannot
/// hollow out the sweep. After the pin advances and
/// [`REDB_PIN_CONTAINS_FD82CED`] flips, a wedged seed fails the sweep again.
/// WP-725 diagnostic: sweep the commit-PRESENT configuration across many seeds
/// and report how many resolve an interrupted commit as present.
///
/// The question this answers is whether the after-commit window narrowed or
/// closed. If no seed in a wide sweep produces a present resolution, the
/// pinned witness did not merely drift and re-pinning would hide an engine
/// change rather than track one.
/// WP-725 diagnostic: is the after-commit window unaddressable by the current
/// operation-counted injection, or absent?
///
/// The sim places crashes by store-operation ordinal. Clean-close fast startup
/// removed storage work from recovery, so the same ordinal budget now covers
/// more logical progress and may simply step over the window. Widening and
/// shifting the crash range distinguishes "cannot address it" from "not there".
#[test]
#[ignore = "WP-725 diagnostic sweep; run explicitly"]
fn wp725_commit_present_window_shape() {
    const PROBE_SEEDS: u64 = 24;
    for (label, ops, crashes) in [
        ("narrow-early", (1_u64, 48_u64), 32_u64),
        ("baseline", (1, 128), 32),
        ("wide", (1, 512), 32),
        ("late", (64, 512), 32),
        ("dense", (1, 128), 64),
    ] {
        let config = CampaignConfig {
            crash_operations: ops,
            max_crashes: crashes,
            ..crate::subsumption::COMMIT_PRESENT_ARMS_CONFIG
        };
        let mut present = 0_u64;
        let mut absent = 0_u64;
        let mut present_seeds = 0_u64;
        for offset in 0..PROBE_SEEDS {
            if let CampaignOutcome::Completed(report) =
                run_campaign_outcome(SWEEP_SEED_BASE + offset, config)
            {
                present += report.in_flight_commit_present;
                absent += report.in_flight_commit_absent;
                if report.in_flight_commit_present > 0 {
                    present_seeds += 1;
                }
            }
        }
        println!(
            "wp725-shape\t{label}\tops={ops:?}\tcrashes={crashes}\t\
             present={present}\tpresent_seeds={present_seeds}\tabsent={absent}"
        );
    }
}

/// Decides the question the random draw cannot: is there ANY store-operation
/// ordinal at which a crash resolves an interrupted batch PRESENT?
///
/// `wp725_commit_present_window_shape` widens, shifts, and densifies a
/// *randomly drawn* countdown. A zero there is ambiguous — the draw may simply
/// never land on a narrow window. This pins the countdown to exactly one
/// ordinal (`(k, k+1)` is half-open, so `draw_crash_countdown` returns `k`) and
/// walks every ordinal in turn, with `max_crashes: 1` so the run carries one
/// crash at one known place.
///
/// The result is exhaustive over placement for a given seed. If no ordinal
/// resolves PRESENT, the window has zero width and the arm tests an
/// unreachable state; if some ordinal does, the window exists and the fixture's
/// draw is what fails to address it.
#[test]
#[ignore = "WP-725 diagnostic sweep; run explicitly"]
fn wp725_commit_present_window_ordinal_walk() {
    const ORDINAL_MAX: u64 = 768;
    const PROBE_SEEDS: u64 = 4;
    let mut present_placements = Vec::new();
    let mut absent_placements = 0_u64;
    let mut uncrashed = 0_u64;
    for offset in 0..PROBE_SEEDS {
        let seed = SWEEP_SEED_BASE + offset;
        for ordinal in 0..ORDINAL_MAX {
            let config = CampaignConfig {
                crash_operations: (ordinal, ordinal + 1),
                max_crashes: 1,
                ..crate::subsumption::COMMIT_PRESENT_ARMS_CONFIG
            };
            let CampaignOutcome::Completed(report) = run_campaign_outcome(seed, config) else {
                continue;
            };
            if report.crashes == 0 {
                // The plan finished before the countdown elapsed: every later
                // ordinal is also unreachable for this seed.
                uncrashed += 1;
                break;
            }
            absent_placements += report.in_flight_commit_absent;
            if report.in_flight_commit_present > 0 {
                present_placements.push((seed, ordinal, report.in_flight_commit_present));
            }
        }
    }
    println!(
        "wp725-walk\tseeds={PROBE_SEEDS}\tordinals<={ORDINAL_MAX}\t\
         present_placements={}\tabsent_placements={absent_placements}\t\
         seeds_exhausted_before_max={uncrashed}",
        present_placements.len()
    );
    for (seed, ordinal, count) in present_placements.iter().take(32) {
        println!("wp725-walk-present\tseed={seed:#x}\tordinal={ordinal}\tpresent={count}");
    }
}

#[test]
#[ignore = "WP-725 diagnostic sweep; run explicitly"]
fn wp725_commit_present_window_sweep() {
    const PROBE_SEEDS: u64 = 96;
    let mut present_seeds = Vec::new();
    let mut absent_total = 0_u64;
    let mut completed = 0_u64;
    let mut wedged = 0_u64;
    for offset in 0..PROBE_SEEDS {
        let seed = SWEEP_SEED_BASE + offset;
        match run_campaign_outcome(seed, crate::subsumption::COMMIT_PRESENT_ARMS_CONFIG) {
            CampaignOutcome::Completed(report) => {
                completed += 1;
                absent_total += report.in_flight_commit_absent;
                if report.in_flight_commit_present > 0 {
                    present_seeds.push((seed, report.in_flight_commit_present));
                }
            }
            CampaignOutcome::WedgedByRedb410FileGrowth { .. } => wedged += 1,
        }
    }
    println!(
        "wp725-sweep\tseeds={PROBE_SEEDS}\tcompleted={completed}\twedged={wedged}\t\
         present_seeds={}\tabsent_total={absent_total}",
        present_seeds.len()
    );
    for (seed, count) in &present_seeds {
        println!("wp725-present\tseed={seed:#x}\tpresent={count}");
    }
}

#[test]
// req: SIM-004, REC-001
fn per_merge_sweep_holds_the_oracle_and_reaches_the_swept_territory() {
    let mut total = CampaignReport::default();
    let mut completed = 0_u64;
    let mut wedged_excluded = 0_u64;
    for offset in 0..SWEEP_SEEDS {
        let seed = SWEEP_SEED_BASE + offset;
        let report = match run_campaign_outcome(seed, SWEEP_CONFIG) {
            CampaignOutcome::Completed(report) => report,
            CampaignOutcome::WedgedByRedb410FileGrowth { .. } => {
                // Only reachable while the pin predates fd82ced:
                // run_campaign_outcome re-raises the panic once the
                // exclusion deactivates.
                wedged_excluded += 1;
                continue;
            }
        };
        completed += 1;
        assert_eq!(
            report.final_frontier,
            u64::from(SWEEP_CONFIG.generator.commands),
            "seed {seed:#x} did not drive the plan to completion"
        );
        total.crashes += report.crashes;
        total.recoveries += report.recoveries;
        total.recovery_window_crashes += report.recovery_window_crashes;
        total.torn_decisions += report.torn_decisions;
        total.in_flight_commit_present += report.in_flight_commit_present;
        total.in_flight_commit_absent += report.in_flight_commit_absent;
        total.in_flight_admit_present += report.in_flight_admit_present;
        total.in_flight_admit_absent += report.in_flight_admit_absent;
        total.initialization_rolled_back += report.initialization_rolled_back;
        total.initialization_survived += report.initialization_survived;
        total.catalog_activation_rolled_back += report.catalog_activation_rolled_back;
        total.catalog_activation_survived += report.catalog_activation_survived;
        total.oracle_comparisons += report.oracle_comparisons;
        total.commands_applied += report.commands_applied;
        total.max_torn_in_one_recovery = total
            .max_torn_in_one_recovery
            .max(report.max_torn_in_one_recovery);
    }
    // The targeted interrupted-commit witness, rotated to 0x51C2_C406 when
    // fa5d906c moved the stream, to 0x51C2_C404 when a29312ff activated V3,
    // to 0x51C2_C41D when ADR-0236 removed affine fresh-locator coverage,
    // and to 0x51C2_C420 when 9aa49dabf initialized V2 sources atomically.
    // All retain the exact interrupted-commit/admission territory. It is not a
    // commit-PRESENT witness: that territory was retired rather than rotated,
    // because it is unreachable at every crash placement rather than merely
    // moved. See `corpus::REGRESSION_CORPUS` for every receipt.
    let interrupted_commit =
        match run_campaign_outcome(0x51C2_C420, crate::subsumption::COMMIT_PRESENT_ARMS_CONFIG) {
            CampaignOutcome::Completed(report) => report,
            CampaignOutcome::WedgedByRedb410FileGrowth { .. } => {
                panic!("targeted interrupted-commit witness wedged instead of completing")
            }
        };
    assert_eq!(
        interrupted_commit.final_frontier,
        u64::from(
            crate::subsumption::COMMIT_PRESENT_ARMS_CONFIG
                .generator
                .commands
        ),
        "targeted interrupted-commit witness did not drive the plan to completion"
    );
    assert!(
        interrupted_commit.in_flight_commit_absent > 0,
        "targeted recovery did not resolve an interrupted commit as absent"
    );
    assert!(
        interrupted_commit.in_flight_admit_present + interrupted_commit.in_flight_admit_absent > 0,
        "targeted recovery did not resolve an interrupted phase-one admission"
    );
    assert_eq!(
        interrupted_commit.in_flight_commit_present, 0,
        "the rotated witness must not reopen retired commit-PRESENT territory"
    );
    assert!(
        completed + wedged_excluded == SWEEP_SEEDS,
        "every sweep seed is accounted for"
    );
    assert!(
        completed * 4 >= SWEEP_SEEDS * 3,
        "excluded wedge placements ({wedged_excluded}) hollowed out more \
         than a quarter of the sweep — retune before trusting the aggregate"
    );
    assert!(total.crashes > 0, "no swept campaign crashed at all");
    assert!(total.recoveries > 0, "no swept campaign recovered");
    assert!(
        total.torn_decisions > 0,
        "no swept recovery resolved torn unsynced state"
    );
    assert!(
        total.recovery_window_crashes > 0,
        "no crash landed inside a recovery window (crash-during-recovery \
         went unexercised) — a reportable finding, do not widen the sweep \
         without understanding why"
    );
    assert!(
        total.in_flight_commit_absent > 0,
        "no swept recovery resolved an interrupted commit as absent"
    );
    // The assertion whose absence let this change go unnoticed. The sweep
    // asserted the ABSENT direction and never asserted the PRESENT one, so
    // fa5d906c closing the in-doubt interval was invisible here even though
    // it is the more dangerous direction to change silently.
    //
    // It is now asserted in the direction that is true: a batch's engine
    // commit is the last fault-eligible operation of its step, so no crash
    // leaves a batch durable but unacknowledged. That is proved exhaustively
    // over crash placement by `wp725_commit_present_window_ordinal_walk` and
    // receipted as a retirement in the corpus and the SIM-006 classification.
    assert_eq!(
        total.in_flight_commit_present, 0,
        "a swept recovery resolved an interrupted commit as PRESENT. The \
         in-doubt interval -- durable but unacknowledged -- has been closed \
         since fa5d906c, so this means it reopened. That is a change in when \
         durability becomes observable and belongs in ADR-0156/ADR-0157: \
         restore the retired corpus expectation and SIM-006 row rather than \
         relaxing this assertion. Totals: {total:?}"
    );
    assert!(
        total.in_flight_admit_present + total.in_flight_admit_absent > 0,
        "no swept recovery resolved an interrupted phase-one admission"
    );
    // Exact comparison accounting: every recovery either completes its full
    // validation + oracle comparison or is superseded by a crash INSIDE its
    // own recovery window (counted in recovery_window_crashes; the next
    // recovery's comparison then covers the combined state), and every
    // completed campaign adds one quiesced final verification.
    assert_eq!(
        total.oracle_comparisons,
        total.recoveries - total.recovery_window_crashes + completed,
        "every recovery must be closed by a full validation and oracle \
         comparison (or by the recovery that superseded it)"
    );
}

/// Env-gated deep exploration (the SIM-004 open-ended budget). Off the
/// per-merge path; run it as documented in the binary's module comment:
///
/// ```text
/// RIFFDB_SIM_EXPLORE_SEEDS=500 cargo test -p riffdb-sim --test seeded_campaign \
///     -- --ignored explore --nocapture
/// ```
#[test]
#[ignore = "deep exploration on a nightly-scale budget; set RIFFDB_SIM_EXPLORE_SEEDS and run with --ignored"]
fn explore_seeded_campaigns_on_the_deep_budget() {
    let seeds: u64 = std::env::var("RIFFDB_SIM_EXPLORE_SEEDS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(0);
    assert!(
        seeds > 0,
        "set RIFFDB_SIM_EXPLORE_SEEDS to the number of seeds to explore"
    );
    let config = CampaignConfig {
        generator: GeneratorConfig {
            commands: 60,
            max_targets: 20,
            two_phase_percent: 35,
            reuse_percent: 50,
            batch_percent: 50,
            max_batch_len: 4,
            note_len_max: 512,
        },
        crash_operations: (1, 700),
        max_crashes: 16,
        torn_write_granularity: 512,
    };
    let mut interesting = 0_u64;
    let mut wedged = 0_u64;
    for offset in 0..seeds {
        let seed = 0x51C2_E000_0000 + offset;
        let report = match run_campaign_outcome(seed, config) {
            CampaignOutcome::Completed(report) => report,
            CampaignOutcome::WedgedByRedb410FileGrowth { .. } => {
                wedged += 1;
                eprintln!("wedged seed {seed:#x} (excluded placement, fd82ced)");
                continue;
            }
        };
        if report.max_torn_in_one_recovery >= 8
            || report.in_flight_commit_present > 0
            || report.recovery_window_crashes > 1
        {
            interesting += 1;
            eprintln!("interesting seed {seed:#x}: {report:?}");
        }
    }
    eprintln!("explored {seeds} seeds; {interesting} interesting; {wedged} wedged-excluded");
}

/// Development scout: prints reports for a seed range under a named config so
/// pinned seeds can be chosen honestly (and so the finding's prevalence can
/// be sized). `RIFFDB_SIM_SCOUT_CONFIG` selects `commit`, `init`, or the
/// sweep config; `RIFFDB_SIM_SCOUT_BASE` (hex) and `RIFFDB_SIM_SCOUT_COUNT`
/// select the seed range.
#[test]
#[ignore = "development seed scout; env-driven, run with --ignored --nocapture"]
fn scout_seed_reports() {
    let which = std::env::var("RIFFDB_SIM_SCOUT_CONFIG").unwrap_or_default();
    let base: u64 = u64::from_str_radix(
        std::env::var("RIFFDB_SIM_SCOUT_BASE")
            .unwrap_or_else(|_| "51C25000".to_owned())
            .as_str(),
        16,
    )
    .expect("hex base");
    let count: u64 = std::env::var("RIFFDB_SIM_SCOUT_COUNT")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(16);
    let config = match which.as_str() {
        "commit" => crate::subsumption::COMMIT_ARMS_CONFIG,
        "init" => crate::subsumption::INITIALIZATION_ARMS_CONFIG,
        // Narrow-window mode: sweep a pinned crash offset (the oracle's
        // dense-window technique through the campaign) — `count` is the
        // number of windows, seeds stay at `base`.
        "window" => {
            for window in 0..count {
                let window = 1 + window * 2;
                let config = CampaignConfig {
                    crash_operations: (window, window + 1),
                    max_crashes: 5,
                    ..crate::subsumption::COMMIT_ARMS_CONFIG
                };
                match run_campaign_outcome(base, config) {
                    CampaignOutcome::Completed(report) => eprintln!(
                        "window {window}: crashes={} rwc={} torn={} cp={} ca={} ap={} aa={} ir={} is={}",
                        report.crashes,
                        report.recovery_window_crashes,
                        report.torn_decisions,
                        report.in_flight_commit_present,
                        report.in_flight_commit_absent,
                        report.in_flight_admit_present,
                        report.in_flight_admit_absent,
                        report.initialization_rolled_back,
                        report.initialization_survived,
                    ),
                    CampaignOutcome::WedgedByRedb410FileGrowth { crashes, .. } => {
                        eprintln!("window {window}: WEDGED (crashes={crashes})");
                    }
                }
            }
            return;
        }
        _ => SWEEP_CONFIG,
    };
    for offset in 0..count {
        let seed = base + offset;
        let report = match run_campaign_outcome(seed, config) {
            CampaignOutcome::Completed(report) => report,
            CampaignOutcome::WedgedByRedb410FileGrowth {
                crashes,
                torn_decisions,
            } => {
                eprintln!("seed {seed:#x}: WEDGED (crashes={crashes} torn={torn_decisions})");
                continue;
            }
        };
        eprintln!(
            "seed {seed:#x}: crashes={} rec={} rwc={} torn={} maxtorn={} cp={} ca={} ap={} aa={} ir={} is={} car={} cas={} cmds={} final={}",
            report.crashes,
            report.recoveries,
            report.recovery_window_crashes,
            report.torn_decisions,
            report.max_torn_in_one_recovery,
            report.in_flight_commit_present,
            report.in_flight_commit_absent,
            report.in_flight_admit_present,
            report.in_flight_admit_absent,
            report.initialization_rolled_back,
            report.initialization_survived,
            report.catalog_activation_rolled_back,
            report.catalog_activation_survived,
            report.commands_applied,
            report.final_frontier,
        );
    }
}

/// FINDING REPRODUCER (SIM-C2's stop-and-report event): seed `0x51C2_C003`
/// under the commit-arms config deterministically reaches, on its fourth
/// crash's torn recovery, a durable engine file whose newest valid god header
/// (recovery_required set, one-phase commit) records a layout length of
/// 233472 bytes while the durable file length is 118784 bytes — the torn
/// recovery kept the in-commit header write and dropped the covering file
/// extension, a physically reachable no-fsync-completed crash state. redb
/// 4.1.0 then panics on EVERY subsequent open at
/// `tree_store/page_store/page_manager.rs:231`
/// (`assert!(storage.raw_file_len()? >= header.layout().len())`) BEFORE its
/// own repair path — which recalculates the layout from the actual file
/// length and runs `pick_primary_for_repair` — can execute: the store is
/// permanently unopenable. Fixed upstream in `fd82ced` ("Make file growth
/// durable to avoid an unopenable database after a crash", 2026-06-13,
/// unreleased); the corpus's inaugural entry pins the reproduction until the
/// pin advances. Run with:
///
/// ```text
/// cargo test -p riffdb-sim --test seeded_campaign finding_redb_reopen_panic \
///     -- --ignored --nocapture
/// ```
#[test]
#[ignore = "reproduces the redb 4.1.0 reopen panic; run explicitly with --ignored --nocapture"]
fn finding_redb_reopen_panic_reproducer() {
    let seed = u64::from_str_radix(
        std::env::var("RIFFDB_SIM_PROBE_SEED")
            .unwrap_or_else(|_| "51C2C003".to_owned())
            .as_str(),
        16,
    )
    .expect("hex seed");
    let runs: u64 = std::env::var("RIFFDB_SIM_PROBE_RUNS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(10);
    let config = crate::subsumption::COMMIT_ARMS_CONFIG;
    let mut panics = 0_u64;
    for run in 0..runs {
        let disk = SimDisk::new(FaultConfig {
            seed,
            torn_write_granularity: config.torn_write_granularity,
            crash_after_operations: None,
            transient_error_denominator: 0,
            capacity_bytes: None,
        });
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_campaign_on(seed, config, &disk)
        }));
        match outcome {
            Ok(report) => {
                eprintln!(
                    "run {run}: completed; crashes={} torn={} cp={} ca={}",
                    report.crashes,
                    report.torn_decisions,
                    report.in_flight_commit_present,
                    report.in_flight_commit_absent
                );
            }
            Err(_) => {
                panics += 1;
                let durable = disk.durable_bytes("/sim/campaign.redb");
                let read_u32 = |offset: usize| {
                    u32::from_le_bytes(durable[offset..offset + 4].try_into().expect("u32"))
                };
                let page_size = u64::from(read_u32(12));
                let region_header_pages = u64::from(read_u32(16));
                let full_regions = u64::from(read_u32(24));
                let trailing_pages = u64::from(read_u32(28));
                // Single-trailing-region approximation of layout().len():
                // superheader page + region header pages + data pages.
                let approx_layout_len =
                    page_size * (1 + region_header_pages + trailing_pages) * full_regions.max(1);
                eprintln!(
                    "run {run}: PANIC. durable_len={} god_byte={:#04x} page_size={page_size} \
                     region_header_pages={region_header_pages} full_regions={full_regions} \
                     trailing_pages={trailing_pages} approx_layout_len={approx_layout_len} \
                     counters={:?} epoch={} crashed={}",
                    durable.len(),
                    durable[9],
                    disk.counters(),
                    disk.epoch(),
                    disk.is_crashed(),
                );
            }
        }
    }
    eprintln!("probe: {panics}/{runs} runs panicked (seed {seed:#x})");
}
