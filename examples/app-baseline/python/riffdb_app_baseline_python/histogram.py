"""Fixed-bucket latency histogram matching the Rust load driver."""

from __future__ import annotations

LINEAR_BUCKETS = 16
EXACT_BUCKETS = 16
OCTAVES = 60
BUCKETS = EXACT_BUCKETS + OCTAVES * LINEAR_BUCKETS


class LatencyHistogram:
    def __init__(self) -> None:
        self.counts = [0] * BUCKETS
        self.total = 0
        self.sum_ns = 0
        self.min_ns = 2**64 - 1
        self.max_ns = 0

    def record_ns(self, nanos: int) -> None:
        nanos = max(int(nanos), 1)
        bucket = _bucket_index(nanos)
        self.counts[bucket] += 1
        self.total += 1
        self.sum_ns += nanos
        self.min_ns = min(self.min_ns, nanos)
        self.max_ns = max(self.max_ns, nanos)

    def merge(self, other: LatencyHistogram) -> None:
        for index, count in enumerate(other.counts):
            self.counts[index] += count
        self.total += other.total
        self.sum_ns += other.sum_ns
        if other.total > 0:
            self.min_ns = min(self.min_ns, other.min_ns)
            self.max_ns = max(self.max_ns, other.max_ns)

    def percentile_ns(self, percentile: int) -> int:
        if self.total == 0:
            return 0
        percentile = min(max(percentile, 0), 100)
        target = max((self.total * percentile + 99) // 100, 1)
        seen = 0
        for index, count in enumerate(self.counts):
            seen += count
            if seen >= target:
                return _bucket_upper_ns(index)
        return self.max_ns

    def summary(self) -> dict[str, int]:
        return {
            "sample_count": self.total,
            "min_ns": 0 if self.total == 0 else self.min_ns,
            "mean_ns": 0 if self.total == 0 else self.sum_ns // self.total,
            "p50_ns": self.percentile_ns(50),
            "p95_ns": self.percentile_ns(95),
            "p99_ns": self.percentile_ns(99),
            "max_ns": self.max_ns,
        }


def _bucket_index(nanos: int) -> int:
    if nanos <= EXACT_BUCKETS:
        return nanos - 1
    exponent = nanos.bit_length() - 1
    base = 1 << exponent
    offset = nanos - base
    sub_bucket = min((offset * LINEAR_BUCKETS) // base, LINEAR_BUCKETS - 1)
    octave = min(max(exponent - 4, 0), OCTAVES - 1)
    return EXACT_BUCKETS + octave * LINEAR_BUCKETS + sub_bucket


def _bucket_upper_ns(index: int) -> int:
    if index < EXACT_BUCKETS:
        return index + 1
    relative = index - EXACT_BUCKETS
    octave = relative // LINEAR_BUCKETS
    sub_bucket = relative % LINEAR_BUCKETS
    exponent = octave + 4
    if exponent >= 63:
        return 2**64 - 1
    base = 1 << exponent
    width = base // LINEAR_BUCKETS
    return base + width * (sub_bucket + 1) - 1
