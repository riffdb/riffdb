//! Sample collection helpers.

use std::time::{Duration, Instant};

/// One named scenario's raw samples.
#[derive(Clone, Debug, Default)]
pub struct SampleSet {
    samples_ns: Vec<u64>,
}

impl SampleSet {
    /// Builds a sample set from explicit nanosecond samples (median reconstruction).
    #[must_use]
    pub fn from_nanos(samples_ns: Vec<u64>) -> Self {
        Self { samples_ns }
    }

    /// Records one sample.
    pub fn record(&mut self, elapsed: Duration) {
        let nanos = u64::try_from(elapsed.as_nanos()).expect("sample fits u64");
        assert!(nanos > 0, "zero-duration sample");
        self.samples_ns.push(nanos);
    }

    /// Returns sorted sample stats.
    #[must_use]
    pub fn summary(&self) -> SampleSummary {
        assert!(!self.samples_ns.is_empty(), "samples required");
        let mut sorted = self.samples_ns.clone();
        sorted.sort_unstable();
        let sum: u128 = sorted.iter().map(|sample| u128::from(*sample)).sum();
        SampleSummary {
            sample_count: sorted.len(),
            samples_ns: sorted.clone(),
            min_ns: sorted[0],
            p50_ns: nearest_rank(&sorted, 50),
            p95_ns: nearest_rank(&sorted, 95),
            p99_ns: nearest_rank(&sorted, 99),
            max_ns: *sorted.last().expect("nonempty"),
            mean_ns: u64::try_from(sum / sorted.len() as u128).expect("mean fits u64"),
        }
    }
}

/// Distribution summary.
#[derive(Clone, Debug)]
pub struct SampleSummary {
    /// Sample count.
    pub sample_count: usize,
    /// Sorted raw samples.
    pub samples_ns: Vec<u64>,
    /// Minimum.
    pub min_ns: u64,
    /// Nearest-rank p50.
    pub p50_ns: u64,
    /// Nearest-rank p95.
    pub p95_ns: u64,
    /// Nearest-rank p99.
    pub p99_ns: u64,
    /// Maximum.
    pub max_ns: u64,
    /// Arithmetic mean.
    pub mean_ns: u64,
}

fn nearest_rank(sorted: &[u64], percentile: usize) -> u64 {
    let numerator = sorted.len().saturating_mul(percentile);
    let rank = numerator.div_ceil(100).max(1);
    sorted[rank - 1]
}

/// Times `work` and returns elapsed duration plus the value.
pub fn time_call<T>(work: impl FnOnce() -> T) -> (T, Duration) {
    let started = Instant::now();
    let value = work();
    (value, started.elapsed())
}
