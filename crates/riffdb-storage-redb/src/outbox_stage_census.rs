//! Graph-owned, diagnostic-only outbox writer timings. No payload or authority.
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Closed private diagnostic stage labels, in snapshot order.
#[doc(hidden)]
pub const OUTBOX_NATIVE_STAGE_LABELS_V1: [&str; 8] = [
    "gate_wait",
    "prefix_publish",
    "checkpoint_wait",
    "journal_take",
    "engine_open",
    "suffix_apply",
    "begin_held",
    "commit_finish",
];

#[derive(Clone, Copy)]
pub(crate) enum Stage {
    GateWait,
    PrefixPublish,
    CheckpointWait,
    JournalTake,
    EngineOpen,
    SuffixApply,
    BeginHeld,
    CommitFinish,
}

pub(crate) struct Census {
    values: [[AtomicU64; 3]; 8],
}
impl Default for Census {
    fn default() -> Self {
        Self {
            values: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0))),
        }
    }
}
impl Census {
    pub(crate) fn snapshot(&self) -> [[u64; 3]; 8] {
        std::array::from_fn(|stage| {
            std::array::from_fn(|slot| self.values[stage][slot].load(Ordering::Relaxed))
        })
    }
    pub(crate) fn measure(
        &self,
        source: Option<riffdb_storage_api::ChangelogAttributionV3>,
        stage: Stage,
    ) -> Span<'_> {
        let active = matches!(
            source,
            Some(riffdb_storage_api::ChangelogAttributionV3::OutboxTransition)
        );
        Span {
            census: self,
            stage,
            began: active.then(Instant::now),
        }
    }
    fn record(&self, stage: Stage, elapsed_us: u64) {
        let [count, sum, maximum] = &self.values[stage as usize];
        maximum.fetch_max(elapsed_us, Ordering::Relaxed);
        for (counter, add) in [(sum, elapsed_us), (count, 1)] {
            let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
                Some(old.saturating_add(add))
            });
        }
    }
}
pub(crate) struct Span<'a> {
    census: &'a Census,
    stage: Stage,
    began: Option<Instant>,
}
impl Drop for Span<'_> {
    fn drop(&mut self) {
        if let Some(began) = self.began {
            self.census.record(
                self.stage,
                u64::try_from(began.elapsed().as_micros()).unwrap_or(u64::MAX),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};

    // req: STO-030, STO-031, END-008
    #[test]
    fn graph_census_keeps_concurrent_attempts_without_wrapping_or_cross_stage_leakage() {
        assert!(std::mem::size_of::<Census>() <= 256);
        let census = Arc::new(Census::default());
        let barrier = Arc::new(Barrier::new(3));
        let mut workers = Vec::new();
        for elapsed in [u64::MAX - 1, 7] {
            let census = Arc::clone(&census);
            let barrier = Arc::clone(&barrier);
            workers.push(std::thread::spawn(move || {
                barrier.wait();
                census.record(Stage::CommitFinish, elapsed);
                barrier.wait();
            }));
        }
        assert_eq!(census.snapshot(), [[0; 3]; 8]);
        barrier.wait();
        barrier.wait();
        for worker in workers {
            worker.join().unwrap();
        }
        let values = census.snapshot();
        assert_eq!(
            values[Stage::CommitFinish as usize],
            [2, u64::MAX, u64::MAX - 1]
        );
        assert_eq!(values[..7], [[0; 3]; 7]);
        assert_eq!(Census::default().snapshot(), [[0; 3]; 8]);
    }

    // req: STO-030, STO-031, END-008
    #[test]
    fn observation_selects_only_native_outbox_attribution_and_counts_dropped_spans() {
        let census = Census::default();
        for source in [
            None,
            Some(riffdb_storage_api::ChangelogAttributionV3::CatalogAdministration),
        ] {
            drop(census.measure(source, Stage::GateWait));
        }
        assert_eq!(census.snapshot(), [[0; 3]; 8]);
        drop(census.measure(
            Some(riffdb_storage_api::ChangelogAttributionV3::OutboxTransition),
            Stage::GateWait,
        ));
        let values = census.snapshot();
        assert_eq!(values[0][0], 1);
        assert_eq!(values[0][1], values[0][2]);
        assert_eq!(values[1..], [[0; 3]; 7]);
    }
}
