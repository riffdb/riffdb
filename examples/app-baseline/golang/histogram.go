package main

const (
	linearBuckets = 16
	exactBuckets  = 16
	octaves       = 60
	histogramSize = exactBuckets + octaves*linearBuckets
)

type latencyHistogram struct {
	counts [histogramSize]uint64
	total  uint64
	sumNs  uint64
	minNs  uint64
	maxNs  uint64
}

func newLatencyHistogram() *latencyHistogram {
	return &latencyHistogram{minNs: ^uint64(0)}
}

func (h *latencyHistogram) recordNs(nanos uint64) {
	if nanos < 1 {
		nanos = 1
	}
	h.counts[bucketIndex(nanos)]++
	h.total++
	h.sumNs += nanos
	if nanos < h.minNs {
		h.minNs = nanos
	}
	if nanos > h.maxNs {
		h.maxNs = nanos
	}
}

func (h *latencyHistogram) merge(other *latencyHistogram) {
	for i := range h.counts {
		h.counts[i] += other.counts[i]
	}
	h.total += other.total
	h.sumNs += other.sumNs
	if other.total > 0 {
		if other.minNs < h.minNs {
			h.minNs = other.minNs
		}
		if other.maxNs > h.maxNs {
			h.maxNs = other.maxNs
		}
	}
}

func (h *latencyHistogram) percentileNs(percentile int) uint64 {
	if h.total == 0 {
		return 0
	}
	if percentile < 0 {
		percentile = 0
	}
	if percentile > 100 {
		percentile = 100
	}
	target := (h.total*uint64(percentile) + 99) / 100
	if target < 1 {
		target = 1
	}
	var seen uint64
	for index, count := range h.counts {
		seen += count
		if seen >= target {
			return bucketUpperNs(index)
		}
	}
	return h.maxNs
}

func (h *latencyHistogram) summary() map[string]uint64 {
	minNs := uint64(0)
	meanNs := uint64(0)
	if h.total > 0 {
		minNs = h.minNs
		meanNs = h.sumNs / h.total
	}
	return map[string]uint64{
		"sample_count": h.total,
		"min_ns":       minNs,
		"mean_ns":      meanNs,
		"p50_ns":       h.percentileNs(50),
		"p95_ns":       h.percentileNs(95),
		"p99_ns":       h.percentileNs(99),
		"max_ns":       h.maxNs,
	}
}

func bucketIndex(nanos uint64) int {
	if nanos <= exactBuckets {
		return int(nanos - 1)
	}
	exponent := 0
	for shifted := nanos >> 1; shifted > 0; shifted >>= 1 {
		exponent++
	}
	base := uint64(1) << exponent
	offset := nanos - base
	subBucket := int((offset * linearBuckets) / base)
	if subBucket > linearBuckets-1 {
		subBucket = linearBuckets - 1
	}
	octave := exponent - 4
	if octave < 0 {
		octave = 0
	}
	if octave > octaves-1 {
		octave = octaves - 1
	}
	return exactBuckets + octave*linearBuckets + subBucket
}

func bucketUpperNs(index int) uint64 {
	if index < exactBuckets {
		return uint64(index + 1)
	}
	relative := index - exactBuckets
	octave := relative / linearBuckets
	subBucket := relative % linearBuckets
	exponent := octave + 4
	if exponent >= 63 {
		return ^uint64(0)
	}
	base := uint64(1) << exponent
	width := base / linearBuckets
	return base + width*uint64(subBucket+1) - 1
}
