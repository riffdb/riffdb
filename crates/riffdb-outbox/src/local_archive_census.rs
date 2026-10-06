//! Process-local timings only; no archive content, durability or authority.
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Closed native diagnostic labels. Durations overlap: commit is inside accept,
/// and an equal retry's accept may call admission again. Counts are attempts.
#[doc(hidden)]
pub const LOCAL_ARCHIVE_NATIVE_STAGE_LABELS_V1: [&str; 3] =
    ["admission", "accept", "record_commit"];

struct Census {
    values: [[AtomicU64; 3]; 3],
}
const _: () = assert!(std::mem::size_of::<Census>() <= 128);

/// Read-only handle for the native worker's numeric diagnostic census.
/// It owns no archive, reader, writer lease or delivery capability.
#[doc(hidden)]
#[derive(Clone)]
pub struct LocalArchiveNativeCensus(Arc<Census>);

impl std::fmt::Debug for LocalArchiveNativeCensus {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("LocalArchiveNativeCensus")
            .field(&self.snapshot())
            .finish()
    }
}

#[derive(Clone, Copy)]
pub(crate) enum Stage {
    Admission,
    Accept,
    RecordCommit,
}

impl LocalArchiveNativeCensus {
    pub(crate) fn new() -> Self {
        Self(Arc::new(Census {
            values: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0))),
        }))
    }

    /// Returns attempt count, summed microseconds and maximum microseconds.
    /// A live sample may race a completed span; joined samples are complete.
    #[must_use]
    pub fn snapshot(&self) -> [[u64; 3]; 3] {
        std::array::from_fn(|stage| {
            std::array::from_fn(|slot| self.0.values[stage][slot].load(Ordering::Relaxed))
        })
    }

    pub(crate) fn measure(&self, stage: Stage) -> Span<'_> {
        Span {
            census: &self.0,
            stage,
            began: Instant::now(),
        }
    }
}

impl Census {
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
    began: Instant,
}
impl Drop for Span<'_> {
    fn drop(&mut self) {
        self.census.record(
            self.stage,
            u64::try_from(self.began.elapsed().as_micros()).unwrap_or(u64::MAX),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // req: EFF-002, STO-020, END-008
    #[test]
    fn archive_census_keeps_concurrent_attempts_without_wrap_or_cross_stage_leakage() {
        let census = LocalArchiveNativeCensus::new();
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let mut workers = Vec::new();
        for elapsed in [u64::MAX - 1, 7] {
            let census = census.clone();
            let barrier = Arc::clone(&barrier);
            workers.push(std::thread::spawn(move || {
                barrier.wait();
                census.0.record(Stage::RecordCommit, elapsed);
                barrier.wait();
            }));
        }
        assert_eq!(census.snapshot(), [[0; 3]; 3]);
        barrier.wait();
        barrier.wait();
        for worker in workers {
            worker.join().unwrap();
        }
        assert_eq!(
            census.snapshot(),
            [[0; 3], [0; 3], [2, u64::MAX, u64::MAX - 1]]
        );
        assert_eq!(LocalArchiveNativeCensus::new().snapshot(), [[0; 3]; 3]);
    }
}
