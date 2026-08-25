/** Fixed-bucket latency histogram matching the Rust load driver. */

const LINEAR_BUCKETS = 16;
const EXACT_BUCKETS = 16;
const OCTAVES = 60;
const BUCKETS = EXACT_BUCKETS + OCTAVES * LINEAR_BUCKETS;

export class LatencyHistogram {
  counts = new Array<number>(BUCKETS).fill(0);
  total = 0;
  sumNs = 0;
  minNs = Number.MAX_SAFE_INTEGER;
  maxNs = 0;

  recordNs(nanos: number): void {
    const value = Math.max(Math.trunc(nanos), 1);
    const bucket = bucketIndex(value);
    this.counts[bucket] = (this.counts[bucket] ?? 0) + 1;
    this.total += 1;
    this.sumNs += value;
    this.minNs = Math.min(this.minNs, value);
    this.maxNs = Math.max(this.maxNs, value);
  }

  merge(other: LatencyHistogram): void {
    for (let index = 0; index < this.counts.length; index += 1) {
      this.counts[index] = (this.counts[index] ?? 0) + (other.counts[index] ?? 0);
    }
    this.total += other.total;
    this.sumNs += other.sumNs;
    if (other.total > 0) {
      this.minNs = Math.min(this.minNs, other.minNs);
      this.maxNs = Math.max(this.maxNs, other.maxNs);
    }
  }

  percentileNs(percentile: number): number {
    if (this.total === 0) return 0;
    const clamped = Math.min(Math.max(percentile, 0), 100);
    const target = Math.max(Math.trunc((this.total * clamped + 99) / 100), 1);
    let seen = 0;
    for (let index = 0; index < this.counts.length; index += 1) {
      seen += this.counts[index] ?? 0;
      if (seen >= target) return bucketUpperNs(index);
    }
    return this.maxNs;
  }

  summary(): Record<string, number> {
    return {
      sample_count: this.total,
      min_ns: this.total === 0 ? 0 : this.minNs,
      mean_ns: this.total === 0 ? 0 : Math.trunc(this.sumNs / this.total),
      p50_ns: this.percentileNs(50),
      p95_ns: this.percentileNs(95),
      p99_ns: this.percentileNs(99),
      max_ns: this.maxNs,
    };
  }
}

function bucketIndex(nanos: number): number {
  if (nanos <= EXACT_BUCKETS) return nanos - 1;
  const exponent = Math.floor(Math.log2(nanos));
  const base = 2 ** exponent;
  const offset = nanos - base;
  const subBucket = Math.min(Math.trunc((offset * LINEAR_BUCKETS) / base), LINEAR_BUCKETS - 1);
  const octave = Math.min(Math.max(exponent - 4, 0), OCTAVES - 1);
  return EXACT_BUCKETS + octave * LINEAR_BUCKETS + subBucket;
}

function bucketUpperNs(index: number): number {
  if (index < EXACT_BUCKETS) return index + 1;
  const relative = index - EXACT_BUCKETS;
  const octave = Math.trunc(relative / LINEAR_BUCKETS);
  const subBucket = relative % LINEAR_BUCKETS;
  const exponent = octave + 4;
  if (exponent >= 53) return Number.MAX_SAFE_INTEGER;
  const base = 2 ** exponent;
  const width = base / LINEAR_BUCKETS;
  return base + width * (subBucket + 1) - 1;
}
