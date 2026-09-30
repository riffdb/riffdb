//! Bounded diagnostic counts at actual asynchronous checkpoint selection.

use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) struct CheckpointCensus {
    values: [AtomicU64; 15],
}

impl CheckpointCensus {
    const fn new() -> Self {
        Self {
            values: [
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(u64::MAX),
                AtomicU64::new(0),
                AtomicU64::new(u64::MAX),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
            ],
        }
    }

    fn add(&self, slot: usize, value: u64) {
        let _ = self.values[slot].fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
            Some(old.saturating_add(value))
        });
    }

    pub(crate) fn started(
        &self,
        transitions: usize,
        commands: usize,
        bytes: usize,
        triggers: [bool; 4],
    ) {
        self.add(0, 1);
        let transitions = u64::try_from(transitions).unwrap_or(u64::MAX);
        let commands = u64::try_from(commands).unwrap_or(u64::MAX);
        let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
        self.add(1, transitions);
        self.add(2, commands);
        self.add(3, bytes);
        // Reanchor-only selections may be empty; zero is a real minimum.
        for (min_slot, max_slot, value) in [(4, 5, transitions), (6, 7, bytes)] {
            self.values[min_slot].fetch_min(value, Ordering::Relaxed);
            self.values[max_slot].fetch_max(value, Ordering::Relaxed);
        }
        for (offset, crossed) in triggers.into_iter().enumerate() {
            if crossed {
                self.add(8 + offset, 1);
            }
        }
    }

    pub(crate) fn materialized(&self) {
        self.add(12, 1);
    }
    pub(crate) fn installed(&self) {
        self.add(13, 1);
    }
    pub(crate) fn failed(&self) {
        self.add(14, 1);
    }

    pub(crate) fn snapshot(&self) -> [u64; 15] {
        let mut values = std::array::from_fn(|slot| self.values[slot].load(Ordering::Relaxed));
        if values[0] == 0 {
            values[4] = 0;
            values[6] = 0;
        }
        values
    }
}

pub(crate) static CHECKPOINTS: CheckpointCensus = CheckpointCensus::new();

/// The peak overlay charge any selected checkpoint covered. Overlay memory is
/// bounded by it (ADR-0263 Decision 7).
static PEAK_OVERLAY_CHARGE: AtomicU64 = AtomicU64::new(0);

pub(crate) fn overlay_charge(bytes: usize) {
    PEAK_OVERLAY_CHARGE.fetch_max(u64::try_from(bytes).unwrap_or(u64::MAX), Ordering::Relaxed);
}

pub(crate) fn peak_overlay_charge() -> u64 {
    PEAK_OVERLAY_CHARGE.load(Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;

    // req: REP-007
    #[test]
    fn the_overlay_census_keeps_the_peak_charge() {
        overlay_charge(90_000_000);
        overlay_charge(60_000_000);
        assert!(peak_overlay_charge() >= 90_000_000);
    }

    // req: REP-007
    #[test]
    fn checkpoint_counts_retain_selected_sizes_and_nonexclusive_trigger_reasons() {
        let census = CheckpointCensus::new();
        census.started(4_100, 4_100, 15_000_000, [true, false, false, false]);
        census.materialized();
        census.installed();
        census.started(3_050, 3_050, 16_800_000, [false, true, false, false]);
        census.materialized();
        // Both bounds can be crossed at a group boundary: reason counts must
        // remain nonexclusive, never masquerade as additional starts.
        census.started(4_101, 4_101, 17_000_000, [true, true, false, false]);
        census.failed();
        assert_eq!(
            census.snapshot(),
            [
                3, 11_251, 11_251, 48_800_000, 3_050, 4_101, 15_000_000, 17_000_000, 2, 2, 0, 0, 2,
                1, 1,
            ]
        );
        census.started(0, 0, 0, [false, false, false, true]);
        census.started(1, 1, 1, [false, false, false, true]);
        assert_eq!(census.snapshot()[4], 0);
        assert_eq!(census.snapshot()[6], 0);
    }
}
