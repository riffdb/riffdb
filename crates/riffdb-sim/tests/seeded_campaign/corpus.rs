// req: SIM-004, REC-001
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
    /// A reviewed retirement of part of this entry's territory, when an
    /// intentional change made that state unreachable rather than moving it.
    /// A chain of rotations ends in a retirement when the window closes.
    pub retirement: Option<CorpusTerritoryRetirement>,
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
    /// Exact change that later moved the window BACK, if one has. A rotated
    /// entry normally must stop reaching its territory; once a named change
    /// restores it the entry must reach it again, and both receipts are kept
    /// so the corpus records the whole history rather than dropping the
    /// rotation that was true in between.
    pub restored_by: Option<&'static str>,
    /// A later layout change moved a restored window away again. Keep the
    /// original rotation AND restoration receipts; the successor chain still
    /// has to end in a live witness for exactly the same territory.
    pub restoration_moved_by: Option<&'static str>,
    /// A second restoration, retaining the first restoration and its later
    /// displacement rather than overwriting either historical receipt.
    pub restoration_returned_by: Option<&'static str>,
}

impl CorpusWitnessRotation {
    fn currently_restored(self) -> bool {
        self.restoration_returned_by.is_some()
            || (self.restored_by.is_some() && self.restoration_moved_by.is_none())
    }
}

#[test]
fn moved_restoration_preserves_both_historical_receipts() {
    let original = CorpusWitnessRotation {
        successor_seed: 0x51C2_C000,
        invalidated_by_commit: "c2bba5ce043d9b8933e432c6d2a49e5adf618985",
        rotated: "2026-08-11",
        restored_by: Some("fa5d906c3abc47af15590810676f27615233aef5"),
        restoration_moved_by: None,
        restoration_returned_by: None,
    };
    assert!(original.currently_restored());
    let moved = CorpusWitnessRotation {
        restoration_moved_by: Some("a29312ffbfd3cd464f4803b1a32ce8e294279d83"),
        restoration_returned_by: None,
        ..original
    };
    assert!(!moved.currently_restored());
    assert_eq!(moved.restored_by, original.restored_by);
    assert_eq!(moved.invalidated_by_commit, original.invalidated_by_commit);
    assert_eq!(moved.successor_seed, original.successor_seed);
    let returned = CorpusWitnessRotation {
        restoration_returned_by: Some("d32e1182889cc338b44808448c13f4539ee9253e"),
        ..moved
    };
    assert!(returned.currently_restored());
    assert_eq!(returned.restored_by, original.restored_by);
    assert_eq!(returned.restoration_moved_by, moved.restoration_moved_by);
    assert_eq!(
        returned.invalidated_by_commit,
        original.invalidated_by_commit
    );
    assert_eq!(returned.successor_seed, original.successor_seed);
}

/// Receipted retirement of one expectation whose territory an intentional
/// change made unreachable.
///
/// This is not a rotation. A rotation says "the window moved, here is the
/// successor that reaches it"; a retirement says "there is no window left, and
/// therefore no successor to pin". Rotating instead would demand a witness for
/// a state the engine cannot enter, and the only way to satisfy that demand is
/// to stop asserting anything.
///
/// The entry stays in the append-only corpus and keeps replaying. Its
/// surviving expectations must still hold, and `retired_expectations` must NOT
/// hold — so a change that reopens the territory reds here.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CorpusTerritoryRetirement {
    /// Exact RiffDB commit whose intentional change closed the territory.
    pub closed_by_commit: &'static str,
    /// Review date (UTC).
    pub retired: &'static str,
    /// Expectations this entry must no longer reproduce.
    pub retired_expectations: &'static [CorpusExpectation],
    /// Test proving the closure exhaustively over crash placement, rather
    /// than a sample that merely failed to find the window.
    pub proof: &'static str,
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
            restored_by: Some("c7ec046edb68fd489a7ca00f7c356f6265f979c7"),
            restoration_moved_by: None,
            restoration_returned_by: None,
        }),
        retirement: None,
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
            // Clean-close fast startup removed storage work from recovery,
            // which moved the operation stream again and landed this window
            // back on the original seed: 42 torn decisions, an interrupted
            // commit resolved absent, six recovery-window crashes, ten
            // initialization survivals. Verified against the diff rather than
            // assumed -- the whole corpus replay passes at fa5d906c~1, so this
            // entry did not reach its territory there. The 2026-08-11 rotation
            // receipt is retained because it was true in between.
            restored_by: Some("fa5d906c3abc47af15590810676f27615233aef5"),
            restoration_moved_by: Some("a29312ffbfd3cd464f4803b1a32ce8e294279d83"),
            restoration_returned_by: None,
        }),
        retirement: None,
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
            restored_by: None,
            restoration_moved_by: None,
            restoration_returned_by: None,
        }),
        retirement: None,
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
            // The redb 4.2.0 pin carries upstream fd82ced, whose extra
            // pre-header growth sync moved this window back onto the original
            // seed: the replay again lands three crashes inside recovery
            // windows with an interrupted admission resolved. The 2026-08-14
            // rotation receipt is retained because it was true under the
            // 4.1.0 pin; the successor witness stays in the corpus.
            restored_by: Some("c7ec046edb68fd489a7ca00f7c356f6265f979c7"),
            restoration_moved_by: None,
            restoration_returned_by: None,
        }),
        retirement: None,
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
            restored_by: Some("c7ec046edb68fd489a7ca00f7c356f6265f979c7"),
            restoration_moved_by: None,
            restoration_returned_by: None,
        }),
        retirement: None,
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
        rotation: Some(CorpusWitnessRotation {
            successor_seed: 0x51C2_C307,
            invalidated_by_commit: "fa5d906c3abc47af15590810676f27615233aef5",
            rotated: "2026-08-30",
            // Adding affine fresh-locator coverage moved the physical
            // operation stream back onto this coordinate: the replay again
            // reaches the exact four-predicate territory. Retain both the
            // truthful 2026-08-30 rotation receipt and its active successor
            // witness.
            restored_by: Some("5a136021db21a4a84b781d5cdc77d7f9d022c013"),
            restoration_moved_by: Some("a29312ffbfd3cd464f4803b1a32ce8e294279d83"),
            restoration_returned_by: None,
        }),
        retirement: None,
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
        rotation: Some(CorpusWitnessRotation {
            successor_seed: 0x51C2_C147,
            invalidated_by_commit: "c7ec046edb68fd489a7ca00f7c356f6265f979c7",
            rotated: "2026-08-18",
            restored_by: None,
            restoration_moved_by: None,
            restoration_returned_by: None,
        }),
        retirement: None,
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
        rotation: Some(CorpusWitnessRotation {
            successor_seed: 0x51C2_C200,
            invalidated_by_commit: "c7ec046edb68fd489a7ca00f7c356f6265f979c7",
            rotated: "2026-08-18",
            // Clean-close fast startup moved the operation stream a third time
            // and landed this window back on the original seed: five
            // recovery-window crashes, 32 torn decisions, an interrupted
            // admission resolved. The corpus replay passes at fa5d906c~1, so
            // this entry did not reach its territory there. Both receipts are
            // retained; the successor stays in the corpus.
            restored_by: Some("fa5d906c3abc47af15590810676f27615233aef5"),
            restoration_moved_by: Some("a29312ffbfd3cd464f4803b1a32ce8e294279d83"),
            // Complete schema-bound images in d32e1182 restore this coordinate:
            // 45 torn decisions, four recovery-window crashes, one interrupted
            // admission and four absent commits, identical through 12 reruns.
            restoration_returned_by: Some("d32e1182889cc338b44808448c13f4539ee9253e"),
        }),
        retirement: None,
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
        retirement: None,
    },
    CorpusEntry {
        seed: 0x51C2_C147,
        generator_version: 1,
        config: COMMIT_PRESENT_ARMS_CONFIG,
        caught: "active successor for the commit-PRESENT recovery arm after \
                 the redb pin advanced to =4.2.0: upstream fd82ced syncs a \
                 file extension before its layout reaches the header, which \
                 moved the physical operation stream and made this territory \
                 markedly rarer (three of ninety seeds under the 4.1.0 pin, \
                 one of two hundred thirty-four under 4.2.0). One \
                 interrupted commit resolves present, seven resolve absent, \
                 and an interrupted admission resolves across the run; rerun \
                 12/12 with identical counters before pinning.",
        pinned: "2026-08-18",
        outcome: CorpusOutcome::Completes(&[
            CorpusExpectation::InFlightCommitPresent,
            CorpusExpectation::InFlightCommitAbsent,
            CorpusExpectation::InFlightAdmitResolved,
        ]),
        rotation: Some(CorpusWitnessRotation {
            successor_seed: 0x51C2_C406,
            invalidated_by_commit: "fa5d906c3abc47af15590810676f27615233aef5",
            rotated: "2026-08-30",
            restored_by: None,
            restoration_moved_by: None,
            restoration_returned_by: None,
        }),
        retirement: None,
    },
    CorpusEntry {
        seed: 0x51C2_C200,
        generator_version: 1,
        config: COMMIT_ARMS_CONFIG,
        caught: "active successor for crash-during-recovery plus interrupted \
                 admission after the redb pin advanced to =4.2.0 and upstream \
                 fd82ced moved the physical operation stream again: four of \
                 fourteen crashes land in recovery windows, 34 torn decisions \
                 resolve, and one interrupted admission resolves; rerun 12/12 \
                 with identical counters before pinning.",
        pinned: "2026-08-18",
        outcome: CorpusOutcome::Completes(&[
            CorpusExpectation::RecoveryWindowCrash,
            CorpusExpectation::TornDecisionsAtLeast(10),
            CorpusExpectation::InFlightAdmitResolved,
        ]),
        rotation: Some(CorpusWitnessRotation {
            successor_seed: 0x51C2_C504,
            invalidated_by_commit: "fa5d906c3abc47af15590810676f27615233aef5",
            rotated: "2026-08-30",
            restored_by: None,
            restoration_moved_by: None,
            restoration_returned_by: None,
        }),
        retirement: None,
    },
    // ---- successors appended for fa5d906c, clean-close fast startup -------
    //
    // That commit removed storage work from recovery, moving the physical
    // operation stream for the third time in this corpus's history. It also
    // moved two older windows BACK onto their original seeds (0x51C2C067 and
    // 0x51C2C0DF, receipted above), so the same change both invalidated and
    // restored witnesses -- which is why each direction is recorded separately
    // rather than as one blanket re-pin.
    //
    // Scouted by `wp725_scout_successor_witnesses` and held to the bar the
    // existing receipts record: identical counters across 12 reruns before
    // pinning.
    CorpusEntry {
        seed: 0x51C2_C307,
        generator_version: 1,
        config: COMMIT_ARMS_CONFIG,
        caught: "active successor for the heavy torn-recovery territory after \
                 fa5d906c clean-close fast startup moved the physical \
                 operation stream: 44 torn decisions across 14 recoveries, \
                 three interrupted batches resolved absent, six \
                 recovery-window crashes, and nine initialization boundary \
                 resolutions with the oracle holding throughout; rerun 12/12 \
                 with identical counters before pinning.",
        pinned: "2026-08-30",
        outcome: CorpusOutcome::Completes(&[
            CorpusExpectation::TornDecisionsAtLeast(30),
            CorpusExpectation::InFlightCommitAbsent,
            CorpusExpectation::RecoveryWindowCrash,
            CorpusExpectation::InitializationBoundary,
        ]),
        rotation: Some(CorpusWitnessRotation {
            successor_seed: 0x51C2_C301,
            invalidated_by_commit: "5a136021db21a4a84b781d5cdc77d7f9d022c013",
            rotated: "2026-09-06",
            // Streaming checkpoint-head updates restored this window before
            // V3 activation: f595885a gives 23 torn decisions, its immediate
            // successor 630e1a42 gives 34 with all four predicates holding.
            // Activation subsequently gives 31 and keeps the same territory.
            restored_by: Some("630e1a42bf005c7e7c52b2585d822f0f3ea3b88a"),
            // Corrected schema-bound fixture d32e1182 resolves only 25 torn
            // decisions here, below the unchanged 30-decision predicate.
            // The existing forward successor chain still proves that territory.
            restoration_moved_by: Some("d32e1182889cc338b44808448c13f4539ee9253e"),
            restoration_returned_by: None,
        }),
        retirement: None,
    },
    CorpusEntry {
        seed: 0x51C2_C406,
        generator_version: 1,
        config: COMMIT_PRESENT_ARMS_CONFIG,
        caught: "active successor for the interrupted-commit and \
                 interrupted-admission territory after fa5d906c. It is the \
                 terminal entry of the commit-PRESENT chain: that resolution \
                 is not merely rarer here, it is unreachable at every crash \
                 placement, so this entry carries a retirement receipt instead \
                 of a fourth successor. One interrupted batch resolves absent, \
                 three interrupted admissions resolve, 55 torn decisions \
                 resolve, and 15 crashes land inside recovery windows; rerun \
                 12/12 with identical counters before pinning.",
        pinned: "2026-08-30",
        outcome: CorpusOutcome::Completes(&[
            CorpusExpectation::InFlightCommitAbsent,
            CorpusExpectation::InFlightAdmitResolved,
        ]),
        rotation: Some(CorpusWitnessRotation {
            successor_seed: 0x51C2_C404,
            invalidated_by_commit: "a29312ffbfd3cd464f4803b1a32ce8e294279d83",
            rotated: "2026-09-14",
            restored_by: None,
            restoration_moved_by: None,
            restoration_returned_by: None,
        }),
        // Only the live admission/absent territory moves. The original
        // commit-PRESENT retirement remains asserted even on this rotated
        // entry: V3 does not reopen the durable-but-unacknowledged interval.
        retirement: Some(CorpusTerritoryRetirement {
            closed_by_commit: "fa5d906c3abc47af15590810676f27615233aef5",
            retired: "2026-08-30",
            retired_expectations: &[CorpusExpectation::InFlightCommitPresent],
            proof: "campaign::wp725_commit_present_window_ordinal_walk",
        }),
    },
    CorpusEntry {
        seed: 0x51C2_C504,
        generator_version: 1,
        config: COMMIT_ARMS_CONFIG,
        caught: "active successor for crash-during-recovery plus interrupted \
                 admission after fa5d906c moved the physical operation stream: \
                 six of fourteen crashes land in recovery windows, 46 torn \
                 decisions resolve, two interrupted admissions resolve, and \
                 two interrupted batches resolve absent; rerun 12/12 with \
                 identical counters before pinning.",
        pinned: "2026-08-30",
        outcome: CorpusOutcome::Completes(&[
            CorpusExpectation::RecoveryWindowCrash,
            CorpusExpectation::TornDecisionsAtLeast(10),
            CorpusExpectation::InFlightAdmitResolved,
        ]),
        rotation: Some(CorpusWitnessRotation {
            successor_seed: 0x51C2_C500,
            invalidated_by_commit: "a29312ffbfd3cd464f4803b1a32ce8e294279d83",
            rotated: "2026-09-14",
            restored_by: None,
            restoration_moved_by: None,
            restoration_returned_by: None,
        }),
        retirement: None,
    },
    // ---- successor appended for affine fresh-locator coverage ------------
    //
    // Commit 5a136021 added affine fresh-locator coverage and moved the
    // physical operation stream again. The earlier 0x51C2C000 coordinate was
    // restored, while its active successor moved below the heavy-torn
    // threshold. The old receipts and both historical coordinates remain
    // replayed; this forward-only successor keeps the exact four-predicate
    // territory active under the current layout.
    CorpusEntry {
        seed: 0x51C2_C301,
        generator_version: 1,
        config: COMMIT_ARMS_CONFIG,
        caught: "active successor for the heavy torn-recovery territory after \
                 affine fresh-locator coverage moved the physical operation \
                 stream: 36 torn decisions \
                 across 14 recoveries, one interrupted batch resolved absent, \
                 five recovery-window crashes, and ten initialization \
                 boundary resolutions with the oracle holding throughout; \
                 rerun 12/12 with identical counters before pinning.",
        pinned: "2026-09-06",
        outcome: CorpusOutcome::Completes(&[
            CorpusExpectation::TornDecisionsAtLeast(30),
            CorpusExpectation::InFlightCommitAbsent,
            CorpusExpectation::RecoveryWindowCrash,
            CorpusExpectation::InitializationBoundary,
        ]),
        rotation: Some(CorpusWitnessRotation {
            successor_seed: 0x51C2_C30A,
            invalidated_by_commit: "a29312ffbfd3cd464f4803b1a32ce8e294279d83",
            rotated: "2026-09-14",
            restored_by: None,
            restoration_moved_by: None,
            restoration_returned_by: None,
        }),
        retirement: None,
    },
    // V3 activation changes physical storage work, not the acceptance states.
    // Each successor below retained identical reports through twelve reruns
    // of the existing scout. All old coordinates and predicates remain.
    CorpusEntry {
        seed: 0x51C2_C30A,
        generator_version: 1,
        config: COMMIT_ARMS_CONFIG,
        caught: "V3 successor for heavy torn recovery: 76 torn decisions, three \
                 interrupted commits absent, five recovery-window crashes and \
                 thirteen initialization survivals; oracle holds throughout, \
                 identical counters in 12/12 reruns.",
        pinned: "2026-09-14",
        outcome: CorpusOutcome::Completes(&[
            CorpusExpectation::TornDecisionsAtLeast(30),
            CorpusExpectation::InFlightCommitAbsent,
            CorpusExpectation::RecoveryWindowCrash,
            CorpusExpectation::InitializationBoundary,
        ]),
        rotation: None,
        retirement: None,
    },
    CorpusEntry {
        seed: 0x51C2_C404,
        generator_version: 1,
        config: COMMIT_PRESENT_ARMS_CONFIG,
        caught: "V3 successor for interrupted commit and admission: two commits \
                 resolve absent, one interrupted admission resolves, 89 torn \
                 decisions, twelve recovery-window crashes and 24 initialization \
                 survivals; oracle holds throughout, identical in 12/12 reruns.",
        pinned: "2026-09-14",
        outcome: CorpusOutcome::Completes(&[
            CorpusExpectation::InFlightCommitAbsent,
            CorpusExpectation::InFlightAdmitResolved,
        ]),
        rotation: None,
        retirement: None,
    },
    CorpusEntry {
        seed: 0x51C2_C500,
        generator_version: 1,
        config: COMMIT_ARMS_CONFIG,
        caught: "V3 successor for crash-during-recovery and admission: five \
                 recovery-window crashes, 23 torn decisions, two interrupted \
                 admissions and twelve initialization survivals; oracle holds \
                 throughout, identical counters in 12/12 reruns.",
        pinned: "2026-09-14",
        outcome: CorpusOutcome::Completes(&[
            CorpusExpectation::RecoveryWindowCrash,
            CorpusExpectation::TornDecisionsAtLeast(10),
            CorpusExpectation::InFlightAdmitResolved,
        ]),
        rotation: None,
        retirement: None,
    },
];

/// Whether a rotation's successor still covers the territory the rotated entry
/// was pinned for.
///
/// Normally that is exact outcome equality. A successor that has itself been
/// partly retired is the one exception: the territory it covered when pinned is
/// what it still asserts PLUS what it now asserts is gone, and that
/// reconstruction is what must match. Comparing only the live half would let a
/// retirement silently shrink every ancestor in the chain.
fn successor_preserves_territory(entry: &CorpusEntry, successor: &CorpusEntry) -> bool {
    let (CorpusOutcome::Completes(wanted), CorpusOutcome::Completes(live)) =
        (entry.outcome, successor.outcome)
    else {
        return successor.outcome == entry.outcome;
    };
    let retired = successor
        .retirement
        .map_or(&[][..], |retirement| retirement.retired_expectations);
    if retired.is_empty() {
        return successor.outcome == entry.outcome;
    }
    // Set equality across the live and retired halves. `CorpusExpectation` is
    // Copy + Eq and these lists hold a handful of entries, so a linear scan is
    // the whole implementation.
    wanted
        .iter()
        .all(|needle| live.contains(needle) || retired.contains(needle))
        && live.iter().all(|held| wanted.contains(held))
        && retired.iter().all(|gone| wanted.contains(gone))
}

fn assert_corpus_expectation(
    entry: &CorpusEntry,
    expectation: CorpusExpectation,
    report: &CampaignReport,
) {
    assert!(
        expectation.holds(report),
        "corpus seed {:#x} (pinned {} for: {}) no longer reproduces \
         {expectation:?}; observed CampaignReport: {report:?}",
        entry.seed,
        entry.pinned,
        entry.caught
    );
}

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
            assert!(
                successor_preserves_territory(entry, successor),
                "corpus seed {:#x} rotation successor {:#x} must preserve the \
                 exact expected territory: {:?} against {:?} plus retirement \
                 {:?}",
                entry.seed,
                rotation.successor_seed,
                entry.outcome,
                successor.outcome,
                successor.retirement.map(|r| r.retired_expectations)
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
            if let Some(commit) = rotation.restoration_moved_by {
                assert!(
                    rotation.restored_by.is_some()
                        && commit.len() == 40
                        && commit.bytes().all(|byte| byte.is_ascii_hexdigit()),
                    "a moved restoration retains its restoring and causal layout commits"
                );
            }
            if let Some(commit) = rotation.restoration_returned_by {
                assert!(
                    rotation.restored_by.is_some()
                        && rotation.restoration_moved_by.is_some()
                        && commit.len() == 40
                        && commit.bytes().all(|byte| byte.is_ascii_hexdigit()),
                    "a returned restoration retains every earlier receipt"
                );
            }
            // Retirement still applies if the surviving territory moves.
            // Rotating an admission witness must never reopen commit-PRESENT.
            if let Some(retirement) = entry.retirement {
                let CampaignOutcome::Completed(report) = &outcome else {
                    panic!("a partly retired witness must still complete");
                };
                assert!(!retirement.retired_expectations.is_empty());
                assert!(
                    retirement.closed_by_commit.len() == 40
                        && retirement
                            .closed_by_commit
                            .bytes()
                            .all(|byte| byte.is_ascii_hexdigit())
                );
                assert!(!retirement.retired.is_empty() && !retirement.proof.is_empty());
                let CorpusOutcome::Completes(expectations) = entry.outcome else {
                    panic!("only completing territory can be partly retired");
                };
                for retired in retirement.retired_expectations {
                    assert!(!expectations.contains(retired));
                    assert!(
                        !retired.holds(report),
                        "retired territory reopened: {report:?}"
                    );
                }
            }
            if rotation.currently_restored() {
                assert!(
                    outcome_holds(entry, &outcome),
                    "corpus seed {:#x} records a restoring change, so it must reach its \
                     expected territory again: {outcome:?}",
                    entry.seed
                );
            } else if REDB_PIN_CONTAINS_FD82CED
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
                    assert_corpus_expectation(entry, *expectation, &report);
                }
                if let Some(retirement) = entry.retirement {
                    assert!(
                        !retirement.retired_expectations.is_empty(),
                        "corpus seed {:#x} carries an empty retirement; drop \
                         the annotation rather than recording a receipt for \
                         nothing",
                        entry.seed
                    );
                    assert!(
                        retirement.closed_by_commit.len() == 40
                            && retirement
                                .closed_by_commit
                                .bytes()
                                .all(|byte| byte.is_ascii_hexdigit()),
                        "corpus seed {:#x} retirement must name an exact Git \
                         commit",
                        entry.seed
                    );
                    assert!(
                        !retirement.retired.is_empty() && !retirement.proof.is_empty(),
                        "corpus seed {:#x} retirement needs a date and a \
                         standing proof",
                        entry.seed
                    );
                    for retired in retirement.retired_expectations {
                        assert!(
                            !expectations.contains(retired),
                            "corpus seed {:#x} both requires and retires \
                             {retired:?}",
                            entry.seed
                        );
                        // The inverted assertion. A retirement claims the
                        // territory is gone; if the replay reaches it again
                        // the claim is false, and the entry must be restored
                        // rather than the receipt amended.
                        assert!(
                            !retired.holds(&report),
                            "corpus seed {:#x} reproduces {retired:?} again, \
                             which was retired {} as unreachable since {}. \
                             Restore it to the entry's expectations and re-run \
                             {} to re-measure the window, rather than editing \
                             this receipt; report: {report:?}",
                            entry.seed,
                            retirement.retired,
                            retirement.closed_by_commit,
                            retirement.proof
                        );
                    }
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

/// Replays every corpus entry and reports which expectations still hold,
/// instead of stopping at the first that does not.
///
/// `regression_corpus_replays_and_reproduces_its_territory` is a gate and
/// rightly fails fast. That makes it a poor instrument when an intentional
/// change moves the physical operation stream, because the first failure hides
/// how many entries moved and in which direction — and treating them one at a
/// time invites re-pinning each in turn without ever seeing the shape. Running
/// this first is what showed `fa5d906c` both invalidated three witnesses and
/// restored two others.
#[test]
#[ignore = "WP-725 diagnostic audit; run explicitly"]
fn wp725_corpus_territory_audit() {
    for entry in REGRESSION_CORPUS {
        let outcome = run_campaign_outcome(entry.seed, entry.config);
        let CampaignOutcome::Completed(report) = outcome else {
            println!("corpus-audit\tseed={:#x}\tWEDGED", entry.seed);
            continue;
        };
        let expectations = match entry.outcome {
            CorpusOutcome::Completes(expectations) => expectations,
            CorpusOutcome::WedgesUntilRedbFileGrowthFix => {
                println!(
                    "corpus-audit\tseed={:#x}\tcompleted (pinned as a wedge)",
                    entry.seed
                );
                continue;
            }
        };
        let retired = entry
            .retirement
            .map_or(&[][..], |retirement| retirement.retired_expectations);
        let verdicts: Vec<String> = expectations
            .iter()
            .map(|expectation| {
                let mark = if expectation.holds(&report) { "+" } else { "-" };
                format!("{mark}{expectation:?}")
            })
            .chain(retired.iter().map(|expectation| {
                // A retired expectation is inverted: holding is the failure.
                let mark = if expectation.holds(&report) { "!" } else { "=" };
                format!("{mark}retired:{expectation:?}")
            }))
            .collect();
        println!(
            "corpus-audit\tseed={:#x}\trotation={}\ttorn={}\tabsent={}\t\
             present={}\tadmit={}\twindow={}\tinit={}\t{}",
            entry.seed,
            entry
                .rotation
                .map_or("none", |rotation| if rotation.currently_restored() {
                    "restored"
                } else {
                    "rotated"
                }),
            report.torn_decisions,
            report.in_flight_commit_absent,
            report.in_flight_commit_present,
            report.in_flight_admit_present + report.in_flight_admit_absent,
            report.recovery_window_crashes,
            report.initialization_rolled_back + report.initialization_survived,
            verdicts.join(" ")
        );
    }
}

/// Scouts successor witnesses for territories an intentional change moved.
///
/// The corpus convention is to rotate: keep the historical coordinate, prove
/// it no longer reaches its territory, and append a successor that does. This
/// finds candidates for that append and applies the bar the existing receipts
/// record — a candidate must reproduce identical counters across repeated runs
/// before it is worth pinning, or it is schedule luck rather than a witness.
#[test]
#[ignore = "WP-725 successor scout; run explicitly"]
fn wp725_scout_successor_witnesses() {
    const SCAN: u64 = 4_096;
    const WANTED: usize = 3;
    const RERUNS: u32 = 12;
    for (label, base, config, required) in [
        (
            "heavy-torn (successor for 0x51C2C000)",
            0x51C2_C300_u64,
            COMMIT_ARMS_CONFIG,
            &[
                CorpusExpectation::TornDecisionsAtLeast(30),
                CorpusExpectation::InFlightCommitAbsent,
                CorpusExpectation::RecoveryWindowCrash,
                CorpusExpectation::InitializationBoundary,
            ][..],
        ),
        (
            "commit-absent + admission (successor for 0x51C2C147)",
            0x51C2_C400,
            COMMIT_PRESENT_ARMS_CONFIG,
            &[
                CorpusExpectation::InFlightCommitAbsent,
                CorpusExpectation::InFlightAdmitResolved,
            ][..],
        ),
        (
            "recovery-window + admission (successor for 0x51C2C200)",
            0x51C2_C500,
            COMMIT_ARMS_CONFIG,
            &[
                CorpusExpectation::RecoveryWindowCrash,
                CorpusExpectation::TornDecisionsAtLeast(10),
                CorpusExpectation::InFlightAdmitResolved,
            ][..],
        ),
    ] {
        let mut found = 0;
        for offset in 0..SCAN {
            let seed = base + offset;
            let CampaignOutcome::Completed(report) = run_campaign_outcome(seed, config) else {
                continue;
            };
            if report.final_frontier != u64::from(config.generator.commands) {
                continue;
            }
            if !required.iter().all(|wanted| wanted.holds(&report)) {
                continue;
            }
            let stable = (0..RERUNS).all(|_| {
                matches!(
                    run_campaign_outcome(seed, config),
                    CampaignOutcome::Completed(rerun) if rerun == report
                )
            });
            println!(
                "corpus-scout\t{label}\tseed={seed:#x}\tstable={stable}\t\
                 torn={}\tabsent={}\tpresent={}\tadmit={}\twindow={}\tinit={}",
                report.torn_decisions,
                report.in_flight_commit_absent,
                report.in_flight_commit_present,
                report.in_flight_admit_present + report.in_flight_admit_absent,
                report.recovery_window_crashes,
                report.initialization_rolled_back + report.initialization_survived,
            );
            if stable {
                found += 1;
                if found == WANTED {
                    break;
                }
            }
        }
        if found == 0 {
            println!("corpus-scout\t{label}\tNO CANDIDATE in {SCAN} seeds");
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

/// SIM-004: a deliberately drifted corpus pin is rejected with enough context
/// to distinguish the coordinate, territory, and observed counters without
/// rerunning under a debugger.
// req: SIM-004
#[test]
fn deliberately_drifted_pin_is_rejected_with_a_diagnosis() {
    let entry = REGRESSION_CORPUS
        .iter()
        .find(|entry| {
            entry.rotation.is_none() && matches!(entry.outcome, CorpusOutcome::Completes(_))
        })
        .expect("the corpus retains an active pin");
    let drifted = CampaignReport::default();

    let panic = std::panic::catch_unwind(|| {
        assert_corpus_expectation(entry, CorpusExpectation::InFlightCommitAbsent, &drifted);
    })
    .expect_err("the deliberately drifted pin must fail");
    let diagnosis = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .expect("the pin failure carries a text diagnosis");

    assert!(diagnosis.contains(&format!("{:#x}", entry.seed)));
    assert!(diagnosis.contains("InFlightCommitAbsent"));
    assert!(diagnosis.contains(entry.caught));
    assert!(diagnosis.contains("CampaignReport"));
}
