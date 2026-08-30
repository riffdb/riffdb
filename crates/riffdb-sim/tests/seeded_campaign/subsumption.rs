//! D3 (SIM-C2, SPEC SIM-006): corpus subsumption of the crash-matrix rows.
//!
//! Every `RECOVERY_SCENARIOS` row is classified here, exactly once, into one
//! of the typed classes below. The storage-layer process-crash arms (the
//! `DedicatedCrashChild` rows owned by `storage_recovery_matrix`) are either
//! reproduced as pinned campaign schedules — a fixed `(seed, config)` whose
//! campaign provably reaches the row's crash point and recovers with oracle
//! agreement — or recorded as typed exclusions with a verifiable reason.
//! Rows outside the storage layer are out of scope BY NAME: daemon-level
//! `IntegratedRiffdbd` rows, other crates' closed owner-package failpoints,
//! and the `DedicatedCrashChild` rows over file adapters that ADR-0113
//! Phase 1 deliberately keeps on the real filesystem (maintenance, backup,
//! CLI bootstrap).
//!
//! The classification is closed in BOTH directions against the inventory, so
//! adding a `RECOVERY_SCENARIOS` row reds
//! `every_recovery_scenario_row_is_classified_exactly_once` until the row is
//! classified — the standing guard SIM-006 requires. The fork+exec matrix
//! itself remains untouched process-level evidence; the simulator owns
//! exploration.

use riffdb_testkit::failpoint::{RECOVERY_SCENARIOS, RecoveryEvidenceKind};

use crate::campaign::{
    CampaignConfig, CampaignOutcome, CampaignReport, REDB_PIN_CONTAINS_FD82CED,
    run_campaign_outcome,
};
use crate::generator::GeneratorConfig;

/// riffdb-sim's own manifest, for the redb-pin guard below.
const SIM_MANIFEST: &str = include_str!("../../Cargo.toml");

/// One crash-placement exclusion active for the current engine pin. These
/// narrow the campaign's EXPLORED placement space — never silently: every
/// exclusion is enumerated here, guarded for pin consistency, and realized as
/// a typed campaign outcome the sweep counts openly.
pub(crate) struct PlacementExclusion {
    /// Stable exclusion name.
    pub name: &'static str,
    /// Verifiable reason, citing the defect and its upstream disposition.
    pub reason: &'static str,
    /// Whether the exclusion is active under the current pin.
    pub active: bool,
    /// What removes the exclusion.
    pub removal: &'static str,
}

/// The complete placement-exclusion inventory (SIM-006 exclusion machinery —
/// a reader of the coverage tests sees every narrowed placement here).
pub(crate) const CAMPAIGN_PLACEMENT_EXCLUSIONS: &[PlacementExclusion] = &[PlacementExclusion {
    name: "redb-4.1.0-file-growth-single-fsync-wedge",
    reason: "crash placements inside a file-growing commit's single-fsync \
             window can durably keep the in-commit god-header write while \
             losing the covering set_len extension; redb 4.1.0 panics on \
             every subsequent open of that state (page_manager.rs:231) \
             instead of running its own repair path — fixed upstream in \
             commit fd82ced (\"Make file growth durable to avoid an \
             unopenable database after a crash\", 2026-06-13, unreleased); \
             the pinned =4.1.0 predates the fix. Reproducer: \
             campaign::finding_redb_reopen_panic_reproducer (seed \
             0x51C2_C003); regression pin: the corpus's inaugural entry.",
    active: !REDB_PIN_CONTAINS_FD82CED,
    removal: "advance the redb pin past fd82ced and flip \
              campaign::REDB_PIN_CONTAINS_FD82CED — the wedge panic then \
              propagates again and the corpus entry flips to asserting clean \
              recovery",
}];

/// The redb-pin guard: the exclusion inventory cannot drift from the actual
/// pin. If the pin moves while `REDB_PIN_CONTAINS_FD82CED` still says
/// `false`, this reds until a human determines whether the new pin contains
/// the fix and flips the constant (with the corpus-entry expectation).
#[test]
fn placement_exclusions_are_consistent_with_the_redb_pin() {
    let pinned_4_1_0 = SIM_MANIFEST.contains("redb = { version = \"=4.1.0\"");
    if REDB_PIN_CONTAINS_FD82CED {
        assert!(
            !pinned_4_1_0,
            "redb 4.1.0 cannot contain fd82ced; the constant is wrong"
        );
    } else {
        assert!(
            pinned_4_1_0,
            "the redb pin moved off =4.1.0: determine whether the new pin \
             contains upstream fd82ced and flip \
             campaign::REDB_PIN_CONTAINS_FD82CED accordingly — the \
             file-growth placement exclusion and the corpus's inaugural \
             entry flip with it"
        );
    }
    for exclusion in CAMPAIGN_PLACEMENT_EXCLUSIONS {
        assert!(!exclusion.name.trim().is_empty());
        assert!(
            !exclusion.reason.trim().is_empty() && !exclusion.removal.trim().is_empty(),
            "{}: a placement exclusion carries a verifiable reason and a \
             removal condition",
            exclusion.name
        );
        assert_eq!(
            exclusion.active, !REDB_PIN_CONTAINS_FD82CED,
            "{}: exclusion activation must key on the pin constant",
            exclusion.name
        );
    }
}

/// Evidence a pinned campaign schedule must exhibit to reproduce one covered
/// row's crash point.
#[derive(Clone, Copy, Debug)]
pub(crate) enum CoveredEvidence {
    /// A recovery resolved an interrupted command/batch commit ABSENT in
    /// full: the crash landed before the engine commit.
    InFlightCommitAbsent,
    /// A recovery resolved an interrupted command/batch commit PRESENT in
    /// full: the crash landed after the engine commit, before the
    /// acknowledgement.
    InFlightCommitPresent,
    /// A crash-interrupted database initialization was observed rolled back:
    /// the retry re-proved emptiness and installed the identity.
    InitializationRolledBack,
    /// A post-crash bring-up observed the one installed `DatabaseId`
    /// preserved: the crash landed after the initialization engine commit.
    InitializationSurvived,
}

impl CoveredEvidence {
    /// Whether `report` exhibits this evidence.
    #[must_use]
    pub(crate) fn holds(self, report: &CampaignReport) -> bool {
        match self {
            Self::InFlightCommitAbsent => report.in_flight_commit_absent > 0,
            Self::InFlightCommitPresent => report.in_flight_commit_present > 0,
            Self::InitializationRolledBack => report.initialization_rolled_back > 0,
            Self::InitializationSurvived => report.initialization_survived > 0,
        }
    }
}

/// The typed classification of one inventory row.
#[derive(Clone, Copy, Debug)]
pub(crate) enum ScenarioClass {
    /// Storage-layer process-crash arm reproduced as a pinned campaign
    /// schedule: the fixed `(seed, config)` campaign reaches the row's crash
    /// point (per `evidence`) and every one of its recoveries passes the full
    /// startup validation and oracle comparison (a campaign panics
    /// otherwise).
    CoveredByCampaign {
        /// Pinned campaign seed.
        seed: u64,
        /// Pinned campaign configuration.
        config: CampaignConfig,
        /// The crash-point evidence the report must exhibit.
        evidence: CoveredEvidence,
    },
    /// Storage-layer process-crash arm the campaign cannot express yet; the
    /// reason names the missing generator capability.
    ExcludedStorageInexpressible {
        /// Verifiable reason for the exclusion.
        reason: &'static str,
    },
    /// Storage-layer process-crash arm whose state the engine can no longer
    /// enter, after an intentional change that narrowed the interval to
    /// nothing. This is NOT a rotation: there is no successor witness to pin,
    /// because there is no territory left to witness. Re-pinning such a row
    /// would manufacture a green suite around a guarantee that changed, and a
    /// campaign asserting an unreachable state is unfalsifiable rather than
    /// passing.
    ///
    /// The bar for this class is exhaustive proof over crash placement, not a
    /// sample: a random schedule that fails to reach a window is indis-
    /// tinguishable from a window that is gone. `proof` names the test that
    /// walks every reachable placement.
    ///
    /// The row does not stop being executable when it retires. Its last
    /// witness is retained and still replayed, and the assertion inverts: the
    /// campaign must complete AND must no longer exhibit `retired_evidence`.
    /// A retirement that turns out to be wrong therefore reds in the standard
    /// suite rather than waiting for someone to run the walk.
    ExcludedStorageStateUnreachable {
        /// Verifiable reason the state can no longer be entered.
        reason: &'static str,
        /// The commit that made it unreachable.
        unreachable_since: &'static str,
        /// The test that proves it exhaustively over crash placement.
        proof: &'static str,
        /// The last witness that reached this crash point, retained so the
        /// retirement stays falsifiable.
        retired_witness_seed: u64,
        /// That witness's campaign configuration.
        retired_witness_config: CampaignConfig,
        /// The evidence the retained witness must no longer exhibit.
        retired_evidence: CoveredEvidence,
    },
    /// Storage-owned row whose evidence is owner typestate proof, not a
    /// process crash: there is no crash point to schedule.
    ExcludedStorageNoCrashPoint {
        /// Verifiable reason for the exclusion.
        reason: &'static str,
    },
    /// Daemon-level `IntegratedRiffdbd` row: public riffdbd composition,
    /// outside ADR-0113 Phase 1's storage-engine scope.
    OutOfScopeDaemon,
    /// Another crate's closed owner-package failpoint above or beside the
    /// simulated store.
    OutOfScopeOwnerPackage {
        /// The owning crate.
        owner: &'static str,
    },
    /// `DedicatedCrashChild` row over file adapters that stay on the real
    /// filesystem in Phase 1 (ADR-0113 decision item 2: maintenance, backup,
    /// retention, and CLI file I/O are out of the seeded exploration's
    /// scope).
    OutOfScopeRealFilesystemAdapter,
}

/// The pinned schedule shared by the covered command-commit rows (also the
/// corpus and reproducer schedule).
pub(crate) const COMMIT_ARMS_CONFIG: CampaignConfig = CampaignConfig {
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

/// The pinned schedule for the rare commit-PRESENT arm after the exact
/// checkpoint-at-S table moved the physical storage-operation stream. The
/// narrower operation range and larger crash budget were scouted together;
/// seed `0x51C2_C001` reproduces PRESENT, ABSENT, and interrupted-admission
/// recovery with identical counters in 12/12 runs.
pub(crate) const COMMIT_PRESENT_ARMS_CONFIG: CampaignConfig = CampaignConfig {
    crash_operations: (1, 128),
    max_crashes: 32,
    ..COMMIT_ARMS_CONFIG
};

/// The pinned schedule shared by the covered initialization rows: a tight
/// crash window aims the first crashes into the bring-up sequence.
pub(crate) const INITIALIZATION_ARMS_CONFIG: CampaignConfig = CampaignConfig {
    generator: GeneratorConfig {
        commands: 6,
        max_targets: 4,
        two_phase_percent: 25,
        reuse_percent: 30,
        batch_percent: 30,
        max_batch_len: 2,
        note_len_max: 100,
    },
    crash_operations: (1, 120),
    max_crashes: 12,
    torn_write_granularity: 512,
};

/// The complete classification, sorted by row name (asserted). Every
/// `RECOVERY_SCENARIOS` row appears exactly once; the guard test closes the
/// mapping in both directions.
pub(crate) const SCENARIO_CLASSIFICATION: &[(&str, ScenarioClass)] = &[
    (
        "catalog.deployment.after-active-pointer",
        ScenarioClass::OutOfScopeOwnerPackage {
            owner: "riffdb-catalog",
        },
    ),
    (
        "catalog.deployment.before-active-pointer",
        ScenarioClass::OutOfScopeOwnerPackage {
            owner: "riffdb-catalog",
        },
    ),
    (
        "catalog.deployment.before-bundle-persist",
        ScenarioClass::OutOfScopeOwnerPackage {
            owner: "riffdb-catalog",
        },
    ),
    (
        "cli.bootstrap.after-credential-file-sync",
        ScenarioClass::OutOfScopeRealFilesystemAdapter,
    ),
    (
        "cli.bootstrap.after-parent-directory-sync",
        ScenarioClass::OutOfScopeRealFilesystemAdapter,
    ),
    (
        "command.capacity.before-reservation",
        ScenarioClass::ExcludedStorageNoCrashPoint {
            reason: "owner typestate tests prove sequence-free reservation and \
                     exact excess rejection; the row names no process crash \
                     point, so there is nothing for a crash schedule to reach",
        },
    ),
    (
        // Covered by a pinned witness until fa5d906c (clean-close fast
        // startup, ADR-0156/ADR-0157). Twice rotated before that, as the
        // checkpoint-at-S layout and then the redb 4.2.0 pin moved the
        // physical operation stream; the territory was already rare (1 of 234
        // seeds under 4.2.0). fa5d906c removed it entirely rather than moving
        // it, so the chain ends here instead of rotating a third time.
        "command.commit.after-engine-commit",
        ScenarioClass::ExcludedStorageStateUnreachable {
            reason: "a batch's engine commit is now the last fault-eligible \
                     operation of its step, so a caller-visible commit failure \
                     implies non-durability; the in-doubt interval in which a \
                     batch was durable but unacknowledged no longer exists. \
                     Confirmed benign rather than silent loss: \
                     verify_model_against_inspection checks every family in \
                     both directions and diverged on none of the 823 \
                     interrupted commits the walk resolved ABSENT, so the \
                     store genuinely lacked those effects",
            unreachable_since: "fa5d906c3abc47af15590810676f27615233aef5",
            proof: "campaign::wp725_commit_present_window_ordinal_walk",
            retired_witness_seed: 0x51C2_C147,
            retired_witness_config: COMMIT_PRESENT_ARMS_CONFIG,
            retired_evidence: CoveredEvidence::InFlightCommitPresent,
        },
    ),
    (
        // The same pinned campaign resolves interrupted batches ABSENT in
        // full (crashes before the engine commit).
        "command.commit.before-engine-commit",
        ScenarioClass::CoveredByCampaign {
            seed: 0x51C2_C147,
            config: COMMIT_PRESENT_ARMS_CONFIG,
            evidence: CoveredEvidence::InFlightCommitAbsent,
        },
    ),
    (
        "command.envelope.before-staging",
        ScenarioClass::ExcludedStorageNoCrashPoint {
            reason: "owner typestate tests prove canonical-envelope \
                     verification precedes staging; no process crash point",
        },
    ),
    (
        "command.public-response.kill-riffdbd",
        ScenarioClass::OutOfScopeDaemon,
    ),
    (
        "command.public-response.partial-frame",
        ScenarioClass::OutOfScopeDaemon,
    ),
    (
        "command.sequence.after-assignment",
        ScenarioClass::ExcludedStorageNoCrashPoint {
            reason: "owner typestate tests prove transaction-local assignment \
                     is invisible before commit; no process crash point (the \
                     campaign's refused mid-batch attempts exercise the same \
                     invisibility semantically, asserted by frontier equality \
                     after every interrupted-absent recovery)",
        },
    ),
    (
        "maintenance.backup.after-publication",
        ScenarioClass::OutOfScopeRealFilesystemAdapter,
    ),
    (
        "maintenance.daemon.after-database-close",
        ScenarioClass::OutOfScopeDaemon,
    ),
    (
        "maintenance.daemon.after-drain",
        ScenarioClass::OutOfScopeDaemon,
    ),
    (
        "maintenance.daemon.after-fresh-validation",
        ScenarioClass::OutOfScopeDaemon,
    ),
    (
        "maintenance.daemon.after-staged-authorization",
        ScenarioClass::OutOfScopeDaemon,
    ),
    (
        "maintenance.public.backup-restore-rewind",
        ScenarioClass::OutOfScopeDaemon,
    ),
    (
        "maintenance.receipt.after-file-sync",
        ScenarioClass::OutOfScopeRealFilesystemAdapter,
    ),
    (
        "maintenance.receipt.after-parent-sync",
        ScenarioClass::OutOfScopeRealFilesystemAdapter,
    ),
    (
        "maintenance.receipt.after-rename",
        ScenarioClass::OutOfScopeRealFilesystemAdapter,
    ),
    (
        "maintenance.receipt.before-file-sync",
        ScenarioClass::OutOfScopeRealFilesystemAdapter,
    ),
    (
        "maintenance.receipt.before-rename",
        ScenarioClass::OutOfScopeRealFilesystemAdapter,
    ),
    (
        "maintenance.restore.after-stage",
        ScenarioClass::OutOfScopeRealFilesystemAdapter,
    ),
    (
        "maintenance.restore.after-target-parent-sync",
        ScenarioClass::OutOfScopeRealFilesystemAdapter,
    ),
    (
        "maintenance.restore.after-target-publication",
        ScenarioClass::OutOfScopeRealFilesystemAdapter,
    ),
    (
        "maintenance.restore.before-target-publication",
        ScenarioClass::OutOfScopeRealFilesystemAdapter,
    ),
    (
        "migration.batch.after-engine-commit",
        ScenarioClass::ExcludedStorageInexpressible {
            reason: "the workload generator has no migration-batch commands \
                     (the campaign fixture contract never requires index \
                     migration and try_open_operational panics on \
                     MigrationRequired); expressible once a generator arm \
                     drives the migration control state",
        },
    ),
    (
        "migration.batch.before-engine-commit",
        ScenarioClass::ExcludedStorageInexpressible {
            reason: "the workload generator has no migration-batch commands \
                     (the campaign fixture contract never requires index \
                     migration and try_open_operational panics on \
                     MigrationRequired); expressible once a generator arm \
                     drives the migration control state",
        },
    ),
    (
        "outbox.delivery.after-claim",
        ScenarioClass::OutOfScopeOwnerPackage {
            owner: "riffdb-outbox",
        },
    ),
    (
        "outbox.delivery.after-connector-success",
        ScenarioClass::OutOfScopeOwnerPackage {
            owner: "riffdb-outbox",
        },
    ),
    (
        "outbox.recovery.after-normalize",
        ScenarioClass::OutOfScopeOwnerPackage {
            owner: "riffdb-outbox",
        },
    ),
    (
        "projection.apply.after-marker",
        ScenarioClass::OutOfScopeOwnerPackage {
            owner: "riffdb-projection",
        },
    ),
    (
        "projection.apply.before-frontier",
        ScenarioClass::OutOfScopeOwnerPackage {
            owner: "riffdb-projection",
        },
    ),
    (
        "projection.rebuild.before-publish",
        ScenarioClass::OutOfScopeOwnerPackage {
            owner: "riffdb-projection",
        },
    ),
    (
        // Scouted over 64 seeds; 0x51C2_1025 observes the installed
        // DatabaseId preserved across seven post-crash bring-ups AND one
        // interrupted initialization rolled back, rerun 10/10 with identical
        // counters before pinning.
        "startup.initialization.after-engine-commit",
        ScenarioClass::CoveredByCampaign {
            seed: 0x51C2_1025,
            config: INITIALIZATION_ARMS_CONFIG,
            evidence: CoveredEvidence::InitializationSurvived,
        },
    ),
    (
        "startup.initialization.before-engine-commit",
        ScenarioClass::CoveredByCampaign {
            seed: 0x51C2_1025,
            config: INITIALIZATION_ARMS_CONFIG,
            evidence: CoveredEvidence::InitializationRolledBack,
        },
    ),
    (
        "startup.repeated-complete-validation",
        ScenarioClass::OutOfScopeDaemon,
    ),
];

/// The standing SIM-006 guard: the classification and the inventory agree in
/// both directions, exactly once per row, and every class is consistent with
/// its row's evidence kind — a new or reclassified `RECOVERY_SCENARIOS` row
/// reds this test until it is honestly classified.
#[test]
fn every_recovery_scenario_row_is_classified_exactly_once() {
    // Sorted and unique, mirroring the inventory's own canonical order.
    for pair in SCENARIO_CLASSIFICATION.windows(2) {
        assert!(
            pair[0].0 < pair[1].0,
            "classification rows must be strictly sorted: {} then {}",
            pair[0].0,
            pair[1].0
        );
    }
    // Both directions: every inventory row classified, every classified name
    // in the inventory.
    for scenario in RECOVERY_SCENARIOS {
        assert!(
            SCENARIO_CLASSIFICATION
                .iter()
                .any(|(name, _)| *name == scenario.name),
            "RECOVERY_SCENARIOS row {:?} is not classified for SIM-006 — \
             classify it in SCENARIO_CLASSIFICATION (covered pinned schedule, \
             typed exclusion, or out-of-scope with the kind-consistency rules \
             below) before merging",
            scenario.name
        );
    }
    for (name, _) in SCENARIO_CLASSIFICATION {
        assert!(
            RECOVERY_SCENARIOS
                .iter()
                .any(|scenario| scenario.name == *name),
            "classification names a row {name:?} that RECOVERY_SCENARIOS no \
             longer contains"
        );
    }

    // Kind consistency: the out-of-scope buckets cannot quietly absorb a
    // storage-layer crash arm.
    for (name, class) in SCENARIO_CLASSIFICATION {
        let scenario = RECOVERY_SCENARIOS
            .iter()
            .find(|scenario| scenario.name == *name)
            .expect("both-directions check above");
        let storage_owned = scenario.evidence_target.contains("storage_recovery_matrix");
        match class {
            ScenarioClass::CoveredByCampaign { .. } => {
                assert_eq!(
                    scenario.evidence,
                    RecoveryEvidenceKind::DedicatedCrashChild,
                    "{name}: campaign coverage applies to process-crash arms"
                );
                assert!(storage_owned, "{name}: not a storage_recovery_matrix row");
            }
            ScenarioClass::ExcludedStorageInexpressible { reason } => {
                assert_eq!(
                    scenario.evidence,
                    RecoveryEvidenceKind::DedicatedCrashChild,
                    "{name}: storage exclusions apply to process-crash arms"
                );
                assert!(storage_owned, "{name}: not a storage_recovery_matrix row");
                assert!(
                    !reason.trim().is_empty(),
                    "{name}: a typed exclusion carries a verifiable reason"
                );
            }
            ScenarioClass::ExcludedStorageStateUnreachable {
                reason,
                unreachable_since,
                proof,
                ..
            } => {
                assert_eq!(
                    scenario.evidence,
                    RecoveryEvidenceKind::DedicatedCrashChild,
                    "{name}: storage exclusions apply to process-crash arms"
                );
                assert!(storage_owned, "{name}: not a storage_recovery_matrix row");
                assert!(
                    !reason.trim().is_empty(),
                    "{name}: a typed exclusion carries a verifiable reason"
                );
                // The commit that closed the state, so the claim is auditable
                // against a diff rather than taken on the reason's word.
                assert_eq!(
                    unreachable_since.len(),
                    40,
                    "{name}: unreachable_since must be a full 40-character \
                     commit id, not an abbreviation"
                );
                assert!(
                    unreachable_since.chars().all(|c| c.is_ascii_hexdigit()),
                    "{name}: unreachable_since must be a commit id"
                );
                // A retired row's standing proof must name a test in this
                // crate, so the claim stays executable rather than becoming a
                // comment that outlives its evidence.
                assert!(
                    proof.starts_with("campaign::")
                        || proof.starts_with("corpus::")
                        || proof.starts_with("subsumption::"),
                    "{name}: proof {proof:?} must name a seeded_campaign test"
                );
            }
            ScenarioClass::ExcludedStorageNoCrashPoint { reason } => {
                assert_eq!(
                    scenario.evidence,
                    RecoveryEvidenceKind::OwnerPackage,
                    "{name}: the no-crash-point exclusion is for owner \
                     typestate rows"
                );
                assert!(storage_owned, "{name}: not a storage_recovery_matrix row");
                assert!(
                    !reason.trim().is_empty(),
                    "{name}: a typed exclusion carries a verifiable reason"
                );
            }
            ScenarioClass::OutOfScopeDaemon => {
                assert_eq!(
                    scenario.evidence,
                    RecoveryEvidenceKind::IntegratedRiffdbd,
                    "{name}: only daemon-level rows are daemon-out-of-scope"
                );
            }
            ScenarioClass::OutOfScopeOwnerPackage { owner } => {
                assert_eq!(
                    scenario.evidence,
                    RecoveryEvidenceKind::OwnerPackage,
                    "{name}: only owner-package rows belong here"
                );
                assert!(
                    !storage_owned,
                    "{name}: storage-owned owner rows must use the typed \
                     storage exclusion, not the out-of-scope bucket"
                );
                assert!(
                    scenario.evidence_target.contains(owner),
                    "{name}: claimed owner {owner:?} does not appear in the \
                     row's evidence target {:?}",
                    scenario.evidence_target
                );
            }
            ScenarioClass::OutOfScopeRealFilesystemAdapter => {
                assert_eq!(
                    scenario.evidence,
                    RecoveryEvidenceKind::DedicatedCrashChild,
                    "{name}: only crash-child rows over real-filesystem \
                     adapters belong here"
                );
                assert!(
                    !storage_owned,
                    "{name}: a storage_recovery_matrix crash arm cannot hide \
                     in the real-filesystem bucket"
                );
            }
        }
    }

    // Every storage-layer DedicatedCrashChild arm is covered or excluded —
    // and the covered set is non-empty (subsumption with zero coverage would
    // be vacuous).
    let covered = SCENARIO_CLASSIFICATION
        .iter()
        .filter(|(_, class)| matches!(class, ScenarioClass::CoveredByCampaign { .. }))
        .count();
    // Retirements are counted separately and bounded, so "excluded because
    // unreachable" cannot become a quiet drain on the covered set.
    let retired = SCENARIO_CLASSIFICATION
        .iter()
        .filter(|(_, class)| matches!(class, ScenarioClass::ExcludedStorageStateUnreachable { .. }))
        .count();
    assert!(
        retired <= 1,
        "more than one storage arm has been retired as unreachable ({retired}); \
         each retirement removes a crash point from the covered set, so a \
         second one needs its own review rather than this floor being lowered \
         again"
    );
    assert!(
        covered + retired >= 4,
        "the storage-arm set shrank below four: {covered} covered plus \
         {retired} retired. A row may leave the covered set only by becoming \
         unreachable (proved exhaustively over crash placement), never by \
         being dropped"
    );
}

/// SIM-006: a retired row stays falsifiable. Its last witness is replayed and
/// must still complete with the oracle holding, but must NOT reach the crash
/// point any more — the assertion of `covered_rows_replay_as_pinned_campaign_
/// schedules`, inverted.
///
/// Without this, retiring a row would be indistinguishable from deleting it,
/// and a change that reopened the interval would go unnoticed exactly the way
/// `fa5d906c` closing it did.
#[test]
fn retired_rows_replay_without_reaching_their_crash_point() {
    for (name, class) in SCENARIO_CLASSIFICATION {
        let ScenarioClass::ExcludedStorageStateUnreachable {
            unreachable_since,
            proof,
            retired_witness_seed,
            retired_witness_config,
            retired_evidence,
            ..
        } = class
        else {
            continue;
        };
        let CampaignOutcome::Completed(report) =
            run_campaign_outcome(*retired_witness_seed, *retired_witness_config)
        else {
            panic!(
                "retired witness (seed {retired_witness_seed:#x}) for {name} \
                 wedged instead of completing; a retired row must still replay \
                 cleanly, or the retirement is hiding a second failure"
            );
        };
        assert_eq!(
            report.final_frontier,
            u64::from(retired_witness_config.generator.commands),
            "retired witness (seed {retired_witness_seed:#x}) for {name} did \
             not drive its plan to completion"
        );
        assert!(
            !retired_evidence.holds(&report),
            "retired row {name} reached its crash point again \
             ({retired_evidence:?}) on seed {retired_witness_seed:#x}. The \
             state was retired as unreachable since {unreachable_since}; if it \
             is reachable once more, restore the row to CoveredByCampaign \
             rather than deleting this assertion, and re-run {proof} to \
             confirm the window's width. Report: {report:?}"
        );
    }
}

/// SIM-006: every covered row's pinned campaign schedule reaches the row's
/// crash point and recovers with oracle agreement (the campaign panics on any
/// validation or oracle failure, so a returned report IS the recovery
/// evidence; the counter assertion is the crash-point reachability proof). A
/// pinned schedule that wedges on the 4.1.0 placement exclusion fails — the
/// pinned seeds are chosen to complete, with the exclusion documented in
/// [`CAMPAIGN_PLACEMENT_EXCLUSIONS`], never silently skipped.
#[test]
fn covered_rows_replay_as_pinned_campaign_schedules() {
    let mut cache: Vec<(u64, CampaignConfig, CampaignReport)> = Vec::new();
    for (name, class) in SCENARIO_CLASSIFICATION {
        let ScenarioClass::CoveredByCampaign {
            seed,
            config,
            evidence,
        } = class
        else {
            continue;
        };
        let report = match cache
            .iter()
            .find(|(cached_seed, cached_config, _)| cached_seed == seed && cached_config == config)
        {
            Some((_, _, report)) => *report,
            None => {
                let CampaignOutcome::Completed(report) = run_campaign_outcome(*seed, *config)
                else {
                    panic!(
                        "pinned schedule (seed {seed:#x}) for {name} wedged on \
                         the redb 4.1.0 file-growth exclusion; pin a seed that \
                         completes"
                    );
                };
                cache.push((*seed, *config, report));
                report
            }
        };
        assert!(
            evidence.holds(&report),
            "pinned schedule (seed {seed:#x}) for {name} no longer reaches \
             the row's crash point ({evidence:?}); report: {report:?}"
        );
    }
}
