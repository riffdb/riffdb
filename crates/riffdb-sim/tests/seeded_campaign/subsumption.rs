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

use crate::campaign::{CampaignConfig, CampaignReport, run_campaign};
use crate::generator::GeneratorConfig;

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

/// The pinned schedule shared by the covered command-commit rows.
const COMMIT_ARMS_CONFIG: CampaignConfig = CampaignConfig {
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

/// The pinned schedule shared by the covered initialization rows: a tight
/// crash window aims the first crashes into the bring-up sequence.
const INITIALIZATION_ARMS_CONFIG: CampaignConfig = CampaignConfig {
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
        "command.commit.after-engine-commit",
        ScenarioClass::CoveredByCampaign {
            seed: 0x51C2_C001,
            config: COMMIT_ARMS_CONFIG,
            evidence: CoveredEvidence::InFlightCommitPresent,
        },
    ),
    (
        "command.commit.before-engine-commit",
        ScenarioClass::CoveredByCampaign {
            seed: 0x51C2_C001,
            config: COMMIT_ARMS_CONFIG,
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
        "startup.initialization.after-engine-commit",
        ScenarioClass::CoveredByCampaign {
            seed: 0x51C2_C002,
            config: INITIALIZATION_ARMS_CONFIG,
            evidence: CoveredEvidence::InitializationSurvived,
        },
    ),
    (
        "startup.initialization.before-engine-commit",
        ScenarioClass::CoveredByCampaign {
            seed: 0x51C2_C002,
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
    assert!(
        covered >= 4,
        "the covered storage-arm set shrank below four"
    );
}

/// SIM-006: every covered row's pinned campaign schedule reaches the row's
/// crash point and recovers with oracle agreement (the campaign panics on any
/// validation or oracle failure, so a returned report IS the recovery
/// evidence; the counter assertion is the crash-point reachability proof).
///
/// STOPPED (SIM-C2 finding): the pinned seeds above are PLACEHOLDERS — seed
/// selection was interrupted when campaign development surfaced the redb
/// 4.1.0 reopen panic (`finding_redb_reopen_panic_reproducer`); this test
/// reds or panics until the engine finding is resolved and honest seeds are
/// pinned. Un-ignoring it without that resolution would either normalize the
/// bug (dodging seeds) or fail.
#[test]
#[ignore = "SIM-C2 stopped: pinned seeds unselected, blocked on the redb reopen-panic finding"]
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
                let report = run_campaign(*seed, *config);
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

/// Scout/reproducer re-export of the commit-arms schedule.
pub(crate) const COMMIT_ARMS_CONFIG_FOR_SCOUT: CampaignConfig = COMMIT_ARMS_CONFIG;
/// Scout re-export of the initialization-arms schedule.
pub(crate) const INITIALIZATION_ARMS_CONFIG_FOR_SCOUT: CampaignConfig = INITIALIZATION_ARMS_CONFIG;
