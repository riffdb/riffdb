//! D4 (SIM-C2, SPEC SIM-004): the found-seed regression corpus.
//!
//! Every seed that ever caught a real defect or exercised interesting
//! territory — a non-trivial two-state acceptance, a large torn-decision
//! recovery, a crash inside a recovery window — is pinned here with its full
//! replay coordinates (seed, generator version, campaign config, what it
//! caught, pin date) and replayed per merge. Entries grow append-only and are
//! never deleted to "keep passing": a replay that stops reproducing its
//! territory is a finding. When an intentional durable-layout change moves a
//! physical crash window, the historical entry receives a typed rotation
//! annotation and an active successor witness is appended. The old entry must
//! prove that its exact territory moved, the successor must reproduce the same
//! expected territory, and the causal layout commit remains part of the
//! checked corpus.
//!
//! The inaugural entry is the campaign's first real engine catch: the redb
//! 4.1.0 file-growth torn-crash wedge (fixed upstream in `fd82ced`,
//! unreleased — see `campaign::REDB_PIN_CONTAINS_FD82CED`). While the pin
//! predates the fix, the entry must REPRODUCE the wedge; once the pin
//! advances and the constant flips, the same replay must instead assert a
//! CLEAN recovery — both expectations are encoded, so the flip obligation
//! cannot rot.

use crate::campaign::{
    CampaignConfig, CampaignOutcome, CampaignReport, REDB_PIN_CONTAINS_FD82CED,
    run_campaign_outcome,
};
use crate::generator::WORKLOAD_GENERATOR_VERSION;
use crate::subsumption::{COMMIT_ARMS_CONFIG, COMMIT_PRESENT_ARMS_CONFIG};

/// Minimum evidence one completing corpus replay must reproduce. Every
/// variant maps to a `CampaignReport` counter, so a replay that "passes"
/// without reaching the pinned territory reds instead of rotting silently.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CorpusExpectation {
    /// At least this many seeded torn decisions across the run's recoveries.
    TornDecisionsAtLeast(u64),
    /// At least one recovery resolved an interrupted batch commit present.
    InFlightCommitPresent,
    /// At least one recovery resolved an interrupted batch commit absent.
    InFlightCommitAbsent,
    /// At least one recovery resolved an interrupted phase-one admission
    /// (either acceptance state).
    InFlightAdmitResolved,
    /// At least one crash landed inside a recovery window.
    RecoveryWindowCrash,
    /// The initialization boundary was exercised (either acceptance state).
    InitializationBoundary,
}

impl CorpusExpectation {
    /// Whether `report` reproduces this expectation.
    #[must_use]
    pub(crate) fn holds(self, report: &CampaignReport) -> bool {
        match self {
            Self::TornDecisionsAtLeast(bound) => report.torn_decisions >= bound,
            Self::InFlightCommitPresent => report.in_flight_commit_present > 0,
            Self::InFlightCommitAbsent => report.in_flight_commit_absent > 0,
            Self::InFlightAdmitResolved => {
                report.in_flight_admit_present + report.in_flight_admit_absent > 0
            }
            Self::RecoveryWindowCrash => report.recovery_window_crashes > 0,
            Self::InitializationBoundary => {
                report.initialization_rolled_back + report.initialization_survived > 0
            }
        }
    }
}

/// What one corpus entry's replay must demonstrate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CorpusOutcome {
    /// The campaign must complete (oracle holding at every recovery, plan
    /// exhausted, quiesced final verification green) AND reproduce the listed
    /// evidence.
    Completes(&'static [CorpusExpectation]),
    /// The entry pins the redb 4.1.0 file-growth wedge: while
    /// [`REDB_PIN_CONTAINS_FD82CED`] is `false` the replay must REPRODUCE the
    /// wedge (the bug demonstrably still exists under the pin); once the pin
    /// advances past `fd82ced` and the constant flips, the same replay must
    /// COMPLETE with a clean recovery — the upstream fix demonstrably took.
    WedgesUntilRedbFileGrowthFix,
}

/// One pinned regression seed: full replay coordinates plus the outcome the
/// replay must demonstrate again.
pub(crate) struct CorpusEntry {
    /// Campaign seed.
    pub seed: u64,
    /// Generator version the entry was pinned under; the replay test refuses
    /// entries from other versions instead of replaying a different plan
    /// under the same name.
    pub generator_version: u32,
    /// Complete campaign configuration.
    pub config: CampaignConfig,
    /// What the seed caught or exercised when it was pinned.
    pub caught: &'static str,
    /// Pin date (UTC).
    pub pinned: &'static str,
    /// The outcome the replay must demonstrate.
    pub outcome: CorpusOutcome,
    /// A reviewed rotation to a successor witness after an intentional
    /// durable-layout change moved this physical crash window. This is never
    /// a silent waiver: the historical coordinate remains replayed and its
    /// successor must appear later in the append-only corpus; repeated layout
    /// changes may form a forward-only chain whose terminal entry is active.
    pub rotation: Option<CorpusWitnessRotation>,
}

/// Receipted replacement of one schedule-sensitive corpus witness.
#[derive(Clone, Copy, Debug)]
pub(crate) struct CorpusWitnessRotation {
    /// Later corpus entry that now reaches the same expected territory.
    pub successor_seed: u64,
    /// Exact RiffDB commit whose intentional layout change moved the window.
    pub invalidated_by_commit: &'static str,
    /// Review date (UTC).
    pub rotated: &'static str,
}

/// The corpus. Append-only.
pub(crate) const REGRESSION_CORPUS: &[CorpusEntry] = &[
    CorpusEntry {
        seed: 0x51C2_C003,
        generator_version: 1,
        config: COMMIT_ARMS_CONFIG,
        caught: "redb 4.1.0 file-growth torn-crash permanent wedge: the \
                 fourth crash's torn recovery keeps the in-commit god-header \
                 write (layout length 233472) while dropping the covering \
                 set_len extension (durable length 118784), and every \
                 subsequent open panics at page_manager.rs:231 before redb's \
                 own repair path can run. Fixed upstream in commit fd82ced \
                 (\"Make file growth durable to avoid an unopenable database \
                 after a crash\", 2026-06-13), unreleased; the pinned =4.1.0 \
                 predates it.",
        pinned: "2026-08-10",
        outcome: CorpusOutcome::WedgesUntilRedbFileGrowthFix,
        rotation: Some(CorpusWitnessRotation {
            successor_seed: 0x51C2_C022,
            invalidated_by_commit: "c2bba5ce043d9b8933e432c6d2a49e5adf618985",
            rotated: "2026-08-11",
        }),
    },
    CorpusEntry {
        seed: 0x51C2_C067,
        generator_version: 1,
        config: COMMIT_ARMS_CONFIG,
        caught: "heaviest torn-decision territory of the 256-seed development \
                 scout: 55 seeded torn decisions across 14 recoveries (max 10 \
                 in one), eight interrupted batches resolved absent in full, \
                 and crashes inside three recovery windows — all with the \
                 oracle holding at every recovered frontier.",
        pinned: "2026-08-10",
        outcome: CorpusOutcome::Completes(&[
            CorpusExpectation::TornDecisionsAtLeast(30),
            CorpusExpectation::InFlightCommitAbsent,
            CorpusExpectation::RecoveryWindowCrash,
            CorpusExpectation::InitializationBoundary,
        ]),
        rotation: Some(CorpusWitnessRotation {
            successor_seed: 0x51C2_C000,
            invalidated_by_commit: "c2bba5ce043d9b8933e432c6d2a49e5adf618985",
            rotated: "2026-08-11",
        }),
    },
    CorpusEntry {
        seed: 0x51C2_C0E1,
        generator_version: 1,
        config: COMMIT_ARMS_CONFIG,
        caught: "the only commit-PRESENT resolution in the 256-seed \
                 development scout (the crash landed after the engine commit \
                 and before the acknowledgement; the recovered frontier \
                 included the unacknowledged batch and its complete effect \
                 graph compared model-equal), alongside four absent \
                 resolutions and four interrupted-admission resolutions; \
                 rerun 12/12 with identical counters before pinning.",
        pinned: "2026-08-10",
        outcome: CorpusOutcome::Completes(&[
            CorpusExpectation::InFlightCommitPresent,
            CorpusExpectation::InFlightCommitAbsent,
            CorpusExpectation::InFlightAdmitResolved,
        ]),
        rotation: Some(CorpusWitnessRotation {
            successor_seed: 0x51C2_C001,
            invalidated_by_commit: "a725142385f26326e4af014414511b71e485033e",
            rotated: "2026-08-14",
        }),
    },
    CorpusEntry {
        seed: 0x51C2_C006,
        generator_version: 1,
        config: COMMIT_ARMS_CONFIG,
        caught: "deepest crash-during-recovery chain of the development \
                 scout: six of fourteen crashes landed inside recovery \
                 windows (recovery of a recovery), with interrupted \
                 admissions resolved on both retries and the oracle holding \
                 throughout.",
        pinned: "2026-08-10",
        outcome: CorpusOutcome::Completes(&[
            CorpusExpectation::RecoveryWindowCrash,
            CorpusExpectation::TornDecisionsAtLeast(10),
            CorpusExpectation::InFlightAdmitResolved,
        ]),
        rotation: Some(CorpusWitnessRotation {
            successor_seed: 0x51C2_C0DF,
            invalidated_by_commit: "a725142385f26326e4af014414511b71e485033e",
            rotated: "2026-08-14",
        }),
    },
    CorpusEntry {
        seed: 0x51C2_C022,
        generator_version: 1,
        config: COMMIT_ARMS_CONFIG,
        caught: "active successor witness for the redb 4.1.0 file-growth \
                 torn-crash wedge after c2bba5c added durable entity-chain \
                 heads and moved the physical write schedule: the fifth \
                 crash keeps a god header requiring a roughly 200704-byte \
                 layout while the durable image is 135168 bytes, and every \
                 subsequent open panics at page_manager.rs:231. Reproduced \
                 identically in 12/12 runs before pinning.",
        pinned: "2026-08-11",
        outcome: CorpusOutcome::WedgesUntilRedbFileGrowthFix,
        rotation: Some(CorpusWitnessRotation {
            successor_seed: 0x51C2_C06D,
            invalidated_by_commit: "a725142385f26326e4af014414511b71e485033e",
            rotated: "2026-08-14",
        }),
    },
    CorpusEntry {
        seed: 0x51C2_C000,
        generator_version: 1,
        config: COMMIT_ARMS_CONFIG,
        caught: "active successor for the heavy torn-recovery territory \
                 after c2bba5c added durable entity-chain heads and moved \
                 the physical operation schedule: 48 torn decisions across \
                 14 recoveries, five interrupted batches resolved absent, \
                 four recovery-window crashes, and ten initialization \
                 survivals with the oracle holding throughout.",
        pinned: "2026-08-11",
        outcome: CorpusOutcome::Completes(&[
            CorpusExpectation::TornDecisionsAtLeast(30),
            CorpusExpectation::InFlightCommitAbsent,
            CorpusExpectation::RecoveryWindowCrash,
            CorpusExpectation::InitializationBoundary,
        ]),
        rotation: None,
    },
    CorpusEntry {
        seed: 0x51C2_C001,
        generator_version: 1,
        config: COMMIT_PRESENT_ARMS_CONFIG,
        caught: "active successor for the commit-PRESENT recovery arm after \
                 a7251423 added exact checkpoint-at-S entity heads and moved \
                 the physical operation stream: one interrupted commit \
                 resolved present, eight resolved absent, and two \
                 interrupted admissions resolved absent across 32 \
                 recoveries; rerun 12/12 with identical counters before \
                 pinning.",
        pinned: "2026-08-14",
        outcome: CorpusOutcome::Completes(&[
            CorpusExpectation::InFlightCommitPresent,
            CorpusExpectation::InFlightCommitAbsent,
            CorpusExpectation::InFlightAdmitResolved,
        ]),
        rotation: None,
    },
    CorpusEntry {
        seed: 0x51C2_C0DF,
        generator_version: 1,
        config: COMMIT_ARMS_CONFIG,
        caught: "active successor for crash-during-recovery plus interrupted \
                 admission after a7251423 moved the physical operation \
                 stream: five of fourteen crashes landed in recovery \
                 windows, 47 torn decisions were resolved, and one \
                 interrupted admission resolved present; rerun 12/12 with \
                 identical counters before pinning.",
        pinned: "2026-08-14",
        outcome: CorpusOutcome::Completes(&[
            CorpusExpectation::RecoveryWindowCrash,
            CorpusExpectation::TornDecisionsAtLeast(10),
            CorpusExpectation::InFlightAdmitResolved,
        ]),
        rotation: None,
    },
    CorpusEntry {
        seed: 0x51C2_C06D,
        generator_version: 1,
        config: COMMIT_ARMS_CONFIG,
        caught: "active successor witness for the redb 4.1.0 file-growth \
                 torn-crash wedge after a7251423 moved the physical \
                 operation stream: the seventh crash leaves redb's durable \
                 file shorter than the layout named by its retained header, \
                 and every subsequent open panics in page_manager.rs:231. \
                 Reproduced identically in 12/12 runs before pinning.",
        pinned: "2026-08-14",
        outcome: CorpusOutcome::WedgesUntilRedbFileGrowthFix,
        rotation: None,
    },
];

fn outcome_holds(entry: &CorpusEntry, outcome: &CampaignOutcome) -> bool {
    match (entry.outcome, outcome) {
        (CorpusOutcome::Completes(expectations), CampaignOutcome::Completed(report)) => {
            expectations
                .iter()
                .all(|expectation| expectation.holds(report))
        }
        (
            CorpusOutcome::WedgesUntilRedbFileGrowthFix,
            CampaignOutcome::WedgedByRedb410FileGrowth { .. },
        ) => !REDB_PIN_CONTAINS_FD82CED,
        (CorpusOutcome::WedgesUntilRedbFileGrowthFix, CampaignOutcome::Completed(report)) => {
            REDB_PIN_CONTAINS_FD82CED
                && report.final_frontier == u64::from(entry.config.generator.commands)
        }
        _ => false,
    }
}

/// SIM-004: every corpus entry replays per merge and demonstrates its pinned
/// outcome again.
#[test]
fn regression_corpus_replays_and_reproduces_its_territory() {
    assert!(
        !REGRESSION_CORPUS.is_empty(),
        "the corpus must retain at least the inaugural engine catch"
    );
    for (entry_index, entry) in REGRESSION_CORPUS.iter().enumerate() {
        assert!(
            REGRESSION_CORPUS[..entry_index]
                .iter()
                .all(|prior| prior.seed != entry.seed),
            "corpus seed {:#x} appears more than once",
            entry.seed
        );
        assert_eq!(
            entry.generator_version, WORKLOAD_GENERATOR_VERSION,
            "corpus entry for seed {:#x} was pinned under generator version \
             {}; re-validate and re-pin it under the current version instead \
             of silently replaying a different plan",
            entry.seed, entry.generator_version
        );
        let outcome = run_campaign_outcome(entry.seed, entry.config);
        if let Some(rotation) = entry.rotation {
            let Some((successor_index, successor)) = REGRESSION_CORPUS
                .iter()
                .enumerate()
                .find(|(_, candidate)| candidate.seed == rotation.successor_seed)
            else {
                panic!(
                    "corpus seed {:#x} rotation names absent successor {:#x}",
                    entry.seed, rotation.successor_seed
                );
            };
            assert!(
                successor_index > entry_index,
                "corpus seed {:#x} rotation successor {:#x} must be appended later",
                entry.seed,
                rotation.successor_seed
            );
            assert_eq!(
                successor.outcome, entry.outcome,
                "corpus seed {:#x} rotation successor {:#x} must preserve the exact expected territory",
                entry.seed, rotation.successor_seed
            );
            assert!(
                rotation.invalidated_by_commit.len() == 40
                    && rotation
                        .invalidated_by_commit
                        .bytes()
                        .all(|byte| byte.is_ascii_hexdigit()),
                "corpus seed {:#x} rotation must name an exact Git commit",
                entry.seed
            );
            assert!(
                !rotation.rotated.is_empty(),
                "corpus seed {:#x} rotation date is required",
                entry.seed
            );
            if REDB_PIN_CONTAINS_FD82CED
                && matches!(entry.outcome, CorpusOutcome::WedgesUntilRedbFileGrowthFix)
            {
                assert!(
                    outcome_holds(entry, &outcome),
                    "historical engine-defect seed {:#x} must also recover cleanly after the redb fix: {outcome:?}",
                    entry.seed
                );
            } else {
                assert!(
                    !outcome_holds(entry, &outcome),
                    "historical corpus seed {:#x} still reaches its exact expected territory, so its rotation receipt is stale or unnecessary: {outcome:?}",
                    entry.seed
                );
            }
            continue;
        }
        match entry.outcome {
            CorpusOutcome::Completes(expectations) => {
                let CampaignOutcome::Completed(report) = outcome else {
                    panic!(
                        "corpus seed {:#x} (pinned {} for: {}) wedged instead \
                         of completing: {outcome:?}",
                        entry.seed, entry.pinned, entry.caught
                    );
                };
                for expectation in expectations {
                    assert!(
                        expectation.holds(&report),
                        "corpus seed {:#x} (pinned {} for: {}) no longer \
                         reproduces {expectation:?}; report: {report:?}",
                        entry.seed,
                        entry.pinned,
                        entry.caught
                    );
                }
            }
            CorpusOutcome::WedgesUntilRedbFileGrowthFix => {
                if REDB_PIN_CONTAINS_FD82CED {
                    // The pin advanced: the fix must hold — clean recovery.
                    let CampaignOutcome::Completed(report) = outcome else {
                        panic!(
                            "corpus seed {:#x}: the redb pin claims to \
                             contain fd82ced but the file-growth wedge still \
                             reproduces — the fix regressed or the constant \
                             was flipped wrongly",
                            entry.seed
                        );
                    };
                    assert_eq!(
                        report.final_frontier,
                        u64::from(entry.config.generator.commands),
                        "corpus seed {:#x}: post-fix replay must recover \
                         cleanly to the full plan frontier",
                        entry.seed
                    );
                } else {
                    // The pin predates the fix: the wedge must reproduce, or
                    // the regression pin has silently lost its bug.
                    assert!(
                        matches!(outcome, CampaignOutcome::WedgedByRedb410FileGrowth { .. }),
                        "corpus seed {:#x} no longer reproduces the redb \
                         4.1.0 file-growth wedge under the =4.1.0 pin; the \
                         inaugural catch has rotted: {outcome:?}",
                        entry.seed
                    );
                }
            }
        }
    }
}

/// The expectation-to-counter mapping is itself pinned: each variant holds
/// exactly when its counter is live, so a corpus entry's expectations mean
/// what they say.
#[test]
fn corpus_expectations_map_to_their_report_counters() {
    let quiet = CampaignReport::default();
    let live = CampaignReport {
        torn_decisions: 3,
        in_flight_commit_present: 1,
        in_flight_commit_absent: 1,
        in_flight_admit_absent: 1,
        recovery_window_crashes: 1,
        initialization_survived: 1,
        ..CampaignReport::default()
    };
    for expectation in [
        CorpusExpectation::TornDecisionsAtLeast(3),
        CorpusExpectation::InFlightCommitPresent,
        CorpusExpectation::InFlightCommitAbsent,
        CorpusExpectation::InFlightAdmitResolved,
        CorpusExpectation::RecoveryWindowCrash,
        CorpusExpectation::InitializationBoundary,
    ] {
        assert!(expectation.holds(&live), "{expectation:?} holds on live");
        assert!(!expectation.holds(&quiet), "{expectation:?} quiet is quiet");
    }
}
