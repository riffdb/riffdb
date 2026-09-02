//! SIM-009 retained Phase-2 coordinator schedules, replayed per merge.

use std::num::NonZeroU8;

use riffdb_sim::{COORDINATOR_TRACE_FORMAT_VERSION, CoordinatorPhase, CoordinatorSchedule};

struct CoordinatorCorpusEntry {
    seed: u64,
    lanes: NonZeroU8,
    expected_digest: u64,
    expected_crash_phase: CoordinatorPhase,
    caught: &'static str,
    pinned: &'static str,
}

const FOUR_LANES: NonZeroU8 = NonZeroU8::new(4).expect("nonzero corpus lane count");

const COORDINATOR_CORPUS: &[CoordinatorCorpusEntry] = &[
    CoordinatorCorpusEntry {
        seed: 0x7660_0000_0000_0001,
        lanes: FOUR_LANES,
        expected_digest: 11_754_762_321_006_919_251,
        expected_crash_phase: CoordinatorPhase::Preparation,
        caught: "Phase-2 composition witness across SimDisk and production conflict checkpoints",
        pinned: "2026-09-01",
    },
    CoordinatorCorpusEntry {
        seed: 0x7660_0000_0000_0002,
        lanes: FOUR_LANES,
        expected_digest: 11_009_497_774_924_184_732,
        expected_crash_phase: CoordinatorPhase::Admission,
        caught: "distinct four-lane interleaving and pre-fence crash placement",
        pinned: "2026-09-01",
    },
    CoordinatorCorpusEntry {
        seed: 0x7660_0000_0000_0003,
        lanes: FOUR_LANES,
        expected_digest: 2_037_531_432_158_838_727,
        expected_crash_phase: CoordinatorPhase::ChangelogEmission,
        caught: "late completion-to-changelog recovery witness",
        pinned: "2026-09-01",
    },
];

#[test]
fn phase_two_coordinator_corpus_replays_per_merge() {
    assert_eq!(
        COORDINATOR_TRACE_FORMAT_VERSION, 1,
        "review corpus on format change"
    );
    let actual = COORDINATOR_CORPUS
        .iter()
        .map(|entry| {
            assert!(!entry.caught.is_empty());
            assert_eq!(entry.pinned, "2026-09-01");
            let schedule =
                CoordinatorSchedule::generate(entry.seed, entry.lanes).expect("corpus schedule");
            let crash_phase = schedule
                .decisions()
                .iter()
                .find(|decision| decision.crash_after())
                .expect("one retained crash")
                .phase();
            (schedule.digest(), crash_phase)
        })
        .collect::<Vec<_>>();
    let expected = COORDINATOR_CORPUS
        .iter()
        .map(|entry| (entry.expected_digest, entry.expected_crash_phase))
        .collect::<Vec<_>>();
    assert_eq!(actual, expected, "retained coordinator corpus drifted");
}
