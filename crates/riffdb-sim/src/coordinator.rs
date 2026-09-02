//! Seed-only coordinator lane and crash scheduling (ADR-0113 Phase 2).

use std::fmt;
use std::num::NonZeroU8;

use crate::{SplitMix64, TraceHash};

/// Version of the coordinator decision encoding and PRNG draw order.
pub const COORDINATOR_TRACE_FORMAT_VERSION: u32 = 1;

/// Maximum number of simultaneously explored command lanes.
pub const MAX_COORDINATOR_LANES: u8 = 8;

const PHASE_COUNT: usize = 7;
const TRACE_COORDINATOR_DOMAIN: u64 = 0x434f_4f52_445f_5631;

/// One coordinator boundary at which another lane may run or the process may crash.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum CoordinatorPhase {
    /// Admission and authorization have accepted the command.
    Admission = 0,
    /// Deterministic command preparation has completed.
    Preparation = 1,
    /// The command's epoch has been sealed for the writer.
    EpochSeal = 2,
    /// The authoritative transaction has crossed its durable fence.
    DurableFence = 3,
    /// The durable result has been published to readers and waiters.
    Publication = 4,
    /// Command completion has been delivered.
    Completion = 5,
    /// Changelog emission for the command has been observed.
    ChangelogEmission = 6,
}

impl CoordinatorPhase {
    /// All coordinator phases, in per-lane dependency order.
    pub const ALL: [Self; PHASE_COUNT] = [
        Self::Admission,
        Self::Preparation,
        Self::EpochSeal,
        Self::DurableFence,
        Self::Publication,
        Self::Completion,
        Self::ChangelogEmission,
    ];
}

/// One seed-selected scheduling decision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CoordinatorDecision {
    lane: u8,
    phase: CoordinatorPhase,
    eligible_lanes: u8,
    selected_eligible_index: u8,
    crash_after: bool,
}

impl CoordinatorDecision {
    /// Zero-based command lane selected by this decision.
    #[must_use]
    pub const fn lane(self) -> u8 {
        self.lane
    }

    /// Phase advanced in the selected lane.
    #[must_use]
    pub const fn phase(self) -> CoordinatorPhase {
        self.phase
    }

    /// Number of lanes eligible when this decision was made.
    #[must_use]
    pub const fn eligible_lanes(self) -> u8 {
        self.eligible_lanes
    }

    /// Index selected from the canonically ordered eligible-lane vector.
    #[must_use]
    pub const fn selected_eligible_index(self) -> u8 {
        self.selected_eligible_index
    }

    /// Whether the seed places the simulated crash after this boundary.
    #[must_use]
    pub const fn crash_after(self) -> bool {
        self.crash_after
    }

    /// Returns the same decision with the crash bit changed.
    #[doc(hidden)]
    #[must_use]
    pub const fn with_crash_after(mut self, crash_after: bool) -> Self {
        self.crash_after = crash_after;
        self
    }

    /// Returns the same decision with the selected lane changed.
    #[doc(hidden)]
    #[must_use]
    pub const fn with_lane(mut self, lane: u8) -> Self {
        self.lane = lane;
        self
    }
}

/// A complete, bounded schedule derived from one seed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoordinatorSchedule {
    seed: u64,
    lane_count: NonZeroU8,
    decisions: Vec<CoordinatorDecision>,
    digest: u64,
}

impl CoordinatorSchedule {
    /// Generates every lane choice and the single crash placement from `seed`.
    pub fn generate(seed: u64, lane_count: NonZeroU8) -> Result<Self, CoordinatorScheduleError> {
        if lane_count.get() > MAX_COORDINATOR_LANES {
            return Err(CoordinatorScheduleError::TooManyLanes {
                supplied: lane_count.get(),
                maximum: MAX_COORDINATOR_LANES,
            });
        }
        let lanes = usize::from(lane_count.get());
        let decision_count = lanes * PHASE_COUNT;
        let mut rng = SplitMix64::new(seed);
        let crash_ordinal = usize::try_from(rng.next_below(decision_count as u64))
            .expect("bounded coordinator decision count fits usize");
        let mut progress = vec![0_usize; lanes];
        let mut decisions = Vec::with_capacity(decision_count);

        for ordinal in 0..decision_count {
            let eligible = progress
                .iter()
                .enumerate()
                .filter_map(|(lane, phase)| (*phase < PHASE_COUNT).then_some(lane))
                .collect::<Vec<_>>();
            let selected_index = usize::try_from(rng.next_below(eligible.len() as u64))
                .expect("bounded eligible-lane index fits usize");
            let lane = eligible[selected_index];
            let phase = CoordinatorPhase::ALL[progress[lane]];
            progress[lane] += 1;
            decisions.push(CoordinatorDecision {
                lane: u8::try_from(lane).expect("lane bound fits u8"),
                phase,
                eligible_lanes: u8::try_from(eligible.len()).expect("lane bound fits u8"),
                selected_eligible_index: u8::try_from(selected_index).expect("lane bound fits u8"),
                crash_after: ordinal == crash_ordinal,
            });
        }
        let digest = coordinator_decision_digest(seed, lane_count, &decisions);
        Ok(Self {
            seed,
            lane_count,
            decisions,
            digest,
        })
    }

    /// Seed that completely selected this schedule.
    #[must_use]
    pub const fn seed(&self) -> u64 {
        self.seed
    }

    /// Number of bounded command lanes in the schedule.
    #[must_use]
    pub const fn lane_count(&self) -> NonZeroU8 {
        self.lane_count
    }

    /// Ordered decisions made by the scheduler.
    #[must_use]
    pub fn decisions(&self) -> &[CoordinatorDecision] {
        &self.decisions
    }

    /// Versioned trace digest containing every scheduler decision.
    #[must_use]
    pub const fn digest(&self) -> u64 {
        self.digest
    }
}

/// Recomputes the versioned digest for decision-sensitivity proofs.
#[doc(hidden)]
#[must_use]
pub fn coordinator_decision_digest(
    seed: u64,
    lane_count: NonZeroU8,
    decisions: &[CoordinatorDecision],
) -> u64 {
    let mut trace = TraceHash::new();
    trace.fold_u64(TRACE_COORDINATOR_DOMAIN);
    trace.fold_u64(u64::from(COORDINATOR_TRACE_FORMAT_VERSION));
    trace.fold_u64(seed);
    trace.fold_u64(u64::from(lane_count.get()));
    trace.fold_u64(decisions.len() as u64);
    for (ordinal, decision) in decisions.iter().enumerate() {
        trace.fold_u64(ordinal as u64);
        trace.fold_u64(u64::from(decision.lane));
        trace.fold_u64(decision.phase as u64);
        trace.fold_u64(u64::from(decision.eligible_lanes));
        trace.fold_u64(u64::from(decision.selected_eligible_index));
        trace.fold_u64(u64::from(decision.crash_after));
    }
    trace.digest()
}

/// Invalid bounded coordinator schedule configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CoordinatorScheduleError {
    /// The requested lane count exceeds the fixed simulator bound.
    TooManyLanes {
        /// Requested lane count.
        supplied: u8,
        /// Fixed simulator maximum.
        maximum: u8,
    },
}

impl fmt::Display for CoordinatorScheduleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooManyLanes { supplied, maximum } => write!(
                formatter,
                "coordinator schedule has {supplied} lanes; maximum is {maximum}"
            ),
        }
    }
}

impl std::error::Error for CoordinatorScheduleError {}
