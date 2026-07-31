//! Fixed-bucket latency histogram for high-volume load windows.
//!
//! Each power-of-two octave is divided into 16 linear sub-buckets. This keeps
//! percentile quantization below 6.25% while remaining allocation-free and
//! bounded for millions of samples.

use std::time::Duration;

const LINEAR_BUCKETS: usize = 16;
const EXACT_BUCKETS: usize = 16;
const OCTAVES: usize = 60;
const BUCKETS: usize = EXACT_BUCKETS + OCTAVES * LINEAR_BUCKETS;

/// Log-linear latency histogram with 16 sub-buckets per octave.
#[derive(Clone, Debug)]
pub struct LatencyHistogram {
    counts: [u64; BUCKETS],
    total: u64,
    sum_ns: u128,
    min_ns: u64,
    max_ns: u64,
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self {
            counts: [0; BUCKETS],
            total: 0,
            sum_ns: 0,
            min_ns: u64::MAX,
            max_ns: 0,
        }
    }
}

impl LatencyHistogram {
    /// Records one observation.
    pub fn record(&mut self, elapsed: Duration) {
        let nanos = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX).max(1);
        let bucket = bucket_index(nanos);
        self.counts[bucket] = self.counts[bucket].saturating_add(1);
        self.total = self.total.saturating_add(1);
        self.sum_ns = self.sum_ns.saturating_add(u128::from(nanos));
        self.min_ns = self.min_ns.min(nanos);
        self.max_ns = self.max_ns.max(nanos);
    }

    /// Merges another histogram into this one.
    pub fn merge(&mut self, other: &Self) {
        for (dst, src) in self.counts.iter_mut().zip(other.counts.iter()) {
            *dst = dst.saturating_add(*src);
        }
        self.total = self.total.saturating_add(other.total);
        self.sum_ns = self.sum_ns.saturating_add(other.sum_ns);
        if other.total > 0 {
            self.min_ns = self.min_ns.min(other.min_ns);
            self.max_ns = self.max_ns.max(other.max_ns);
        }
    }

    /// Total observations.
    #[must_use]
    pub const fn total(&self) -> u64 {
        self.total
    }

    /// Nearest-rank percentile in nanoseconds.
    #[must_use]
    pub fn percentile_ns(&self, percentile: u8) -> u64 {
        if self.total == 0 {
            return 0;
        }
        let percentile = u64::from(percentile.min(100));
        let target = self.total.saturating_mul(percentile).div_ceil(100).max(1);
        let mut seen = 0_u64;
        for (index, count) in self.counts.iter().enumerate() {
            seen = seen.saturating_add(*count);
            if seen >= target {
                return bucket_upper_ns(index);
            }
        }
        self.max_ns
    }

    /// Mean latency in nanoseconds.
    #[must_use]
    pub fn mean_ns(&self) -> u64 {
        if self.total == 0 {
            return 0;
        }
        u64::try_from(self.sum_ns / u128::from(self.total)).unwrap_or(u64::MAX)
    }

    /// Compact JSON-friendly summary.
    #[must_use]
    pub fn summary_json(&self) -> serde_json::Value {
        serde_json::json!({
            "sample_count": self.total,
            "min_ns": if self.total == 0 { 0 } else { self.min_ns },
            "mean_ns": self.mean_ns(),
            "p50_ns": self.percentile_ns(50),
            "p95_ns": self.percentile_ns(95),
            "p99_ns": self.percentile_ns(99),
            "max_ns": self.max_ns,
        })
    }
}

fn bucket_index(nanos: u64) -> usize {
    if nanos <= EXACT_BUCKETS as u64 {
        return usize::try_from(nanos - 1).unwrap_or(0);
    }
    let exponent = 63_u32.saturating_sub(nanos.leading_zeros());
    let base = 1_u64 << exponent;
    let offset = nanos - base;
    let sub_bucket =
        usize::try_from((u128::from(offset) * LINEAR_BUCKETS as u128) / u128::from(base))
            .unwrap_or(LINEAR_BUCKETS - 1)
            .min(LINEAR_BUCKETS - 1);
    let octave = usize::try_from(exponent.saturating_sub(4))
        .unwrap_or(OCTAVES - 1)
        .min(OCTAVES - 1);
    EXACT_BUCKETS + octave * LINEAR_BUCKETS + sub_bucket
}

fn bucket_upper_ns(index: usize) -> u64 {
    if index < EXACT_BUCKETS {
        return u64::try_from(index + 1).unwrap_or(u64::MAX);
    }
    let relative = index - EXACT_BUCKETS;
    let octave = relative / LINEAR_BUCKETS;
    let sub_bucket = relative % LINEAR_BUCKETS;
    let exponent = octave + 4;
    if exponent >= 63 {
        u64::MAX
    } else {
        let base = 1_u64 << exponent;
        let width = base / LINEAR_BUCKETS as u64;
        base.saturating_add(
            width
                .saturating_mul(u64::try_from(sub_bucket + 1).unwrap_or(u64::MAX))
                .saturating_sub(1),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentiles_track_injected_latencies() {
        let mut histogram = LatencyHistogram::default();
        for _ in 0..90 {
            histogram.record(Duration::from_micros(100));
        }
        for _ in 0..9 {
            histogram.record(Duration::from_millis(1));
        }
        histogram.record(Duration::from_millis(10));
        assert!((100_000..=106_495).contains(&histogram.percentile_ns(50)));
        assert!(histogram.percentile_ns(99) >= 1_000_000);
        assert_eq!(histogram.total(), 100);
    }

    #[test]
    fn bucket_upper_bound_has_at_most_one_sixteenth_octave_error() {
        for nanos in [
            1_u64,
            16,
            17,
            31,
            32,
            100,
            1_000,
            100_000,
            1_000_000,
            1_000_000_000,
            u64::MAX / 2,
        ] {
            let upper = bucket_upper_ns(bucket_index(nanos));
            assert!(upper >= nanos, "{nanos} mapped below itself to {upper}");
            if nanos < u64::MAX / 2 {
                let relative_error = (upper - nanos) as f64 / nanos as f64;
                assert!(
                    relative_error <= 0.0625,
                    "{nanos} mapped to {upper} ({relative_error})"
                );
            }
        }
    }

    #[test]
    fn merge_preserves_sub_octave_percentiles() {
        let mut first = LatencyHistogram::default();
        let mut second = LatencyHistogram::default();
        first.record(Duration::from_micros(100));
        second.record(Duration::from_micros(105));
        first.merge(&second);
        assert_eq!(first.total(), 2);
        assert!((100_000..=106_495).contains(&first.percentile_ns(50)));
        assert!((105_000..=106_495).contains(&first.percentile_ns(100)));
    }
}
