//! D4 (SIM-C2, SPEC SIM-004): the found-seed regression corpus.
//!
//! Every seed that ever exercised interesting territory — a non-trivial
//! two-state acceptance, a large torn-decision recovery, a crash inside a
//! recovery window — is pinned here with its full replay coordinates (seed,
//! generator version, campaign config, what it caught, pin date) and replayed
//! per merge. A future simulator-found ENGINE failure would be a
//! STOP-and-report event first; only after the defect is fixed does its seed
//! land here as the regression fixture SIM-004 requires.

use crate::campaign::{CampaignConfig, CampaignReport, run_campaign};
use crate::generator::WORKLOAD_GENERATOR_VERSION;

/// Minimum evidence one corpus replay must reproduce. Every variant maps to a
/// `CampaignReport` counter, so a replay that "passes" without reaching the
/// pinned territory reds instead of rotting silently.
#[derive(Clone, Copy, Debug)]
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

/// One pinned regression seed: full replay coordinates plus the minimum
/// territory the replay must reach again.
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
    /// Minimum evidence the replay must reproduce.
    pub expect: &'static [CorpusExpectation],
}

/// The corpus. Grows append-only: entries are never edited to "keep passing";
/// a replay that stops reproducing its territory is a finding.
///
/// DELIBERATELY EMPTY at the SIM-C2 stop point: development surfaced an
/// actual engine failure (the redb 4.1.0 reopen panic — see
/// `finding_redb_reopen_panic_reproducer` in the campaign module) and the
/// standing rule for a real bug is STOP and report, never a corpus entry.
/// The first entries land when the finding is resolved and the interesting
/// development seeds can replay to completion.
pub(crate) const REGRESSION_CORPUS: &[CorpusEntry] = &[];

/// SIM-004: every corpus entry replays per merge and reproduces at least the
/// territory it was pinned for.
#[test]
#[ignore = "SIM-C2 stopped before corpus population: a real engine finding is a bug report, not a corpus entry"]
fn regression_corpus_replays_and_reproduces_its_territory() {
    assert!(
        !REGRESSION_CORPUS.is_empty(),
        "the corpus must retain at least the development-found seeds"
    );
    for entry in REGRESSION_CORPUS {
        assert_eq!(
            entry.generator_version, WORKLOAD_GENERATOR_VERSION,
            "corpus entry for seed {:#x} was pinned under generator version \
             {}; re-validate and re-pin it under the current version instead \
             of silently replaying a different plan",
            entry.seed, entry.generator_version
        );
        let report = run_campaign(entry.seed, entry.config);
        for expectation in entry.expect {
            assert!(
                expectation.holds(&report),
                "corpus seed {:#x} (pinned {} for: {}) no longer reproduces \
                 {expectation:?}; report: {report:?}",
                entry.seed,
                entry.pinned,
                entry.caught
            );
        }
    }
}

/// The expectation-to-counter mapping is itself pinned: each variant holds
/// exactly when its counter is live, so a future corpus entry's expectations
/// mean what they say.
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
