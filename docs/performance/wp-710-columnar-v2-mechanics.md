# WP-710 columnar V2 mechanics receipt

Status: **mechanics accepted**. The independently testable Segment V2 codec
passes every ADR-0160 mechanics threshold. Production remains on row-framed
layout V1; this receipt does not authorize V2 publication or query routing.

## Frozen method

The registered corpus and legacy comparison are implemented by
`segment_v2::tests::wp710_fixed_corpus_mechanics_receipt`. The harness uses
16,384 primary-key-ordered synthetic rows, four exact columns, five unrecorded
warmups, and 31 counterbalanced release-mode samples in one process.
Construction is outside each measured cell. The high-cardinality arm uses
deterministic incompressible text, integer, and byte values. The
low-cardinality arm uses four text values, packed Booleans, monotone integers,
and two exact byte values.

Measurements are single-threaded elapsed nanoseconds for CPU-bound work. They
are not hardware-counter CPU samples. Allocation values are the frozen logical
owned-allocation model exercised by the corpus; no unsafe or process-global
allocator instrumentation was introduced. The corpus and diagnostics contain
no application, organization, field, predicate, or protected value.

## Current mechanics result

The current candidate was measured once after correctness, fixture, fuzz,
topology, policy, formatting, and Clippy checks completed.

| Gate or observation | V1 | V2 candidate | Result |
|---|---:|---:|---|
| High-cardinality bytes | 1,851,404 | 1,609,476 (86.93% of V1) | Pass; maximum 110% |
| Low-cardinality bytes | 1,421,324 | 785,287 (55.25% of V1; 44.75% fewer) | Pass; at least 25% fewer |
| Clustered 1% segment rejection | — | 99/100, zero false negatives | Pass; at least 90% |
| Full-decode p50 | 4,851,901 ns | 4,811,571 ns (99.16% of V1) | **Pass**; maximum 105% |
| Full-decode p95 | 6,217,316 ns | 5,187,982 ns | Observation |
| Full-decode p99 | 6,556,688 ns | 5,735,055 ns | Observation |
| Examined rows | 16,384 | 16,384 | Matched |
| Modeled owned allocations | 98,304 | 98,320 | Observation |
| Projection lag | Not applicable | Not applicable | Codec-only; no activation |
| Recovery behavior | Existing V1 checkpoint/open | Complete corruption validation before any lane is returned | No production change |

The separately frozen validate-once matched-consumer cell uses one completely
validated immutable process-generation view and the same exact scalar digest on
both formats. It passed at 34.77% of the V1 p50:

| Observation | V1 | V2 candidate |
|---|---:|---:|
| Matched digest p50 | 7,944,134 ns | 2,762,332 ns |
| Matched digest p95 | 9,178,929 ns | 3,236,993 ns |
| Matched digest p99 | 9,615,461 ns | 3,280,424 ns |
| Cold V2 validation p50/p95/p99 | — | 4,683,780 / 5,863,655 / 6,284,067 ns |
| Output digest | `20942a162eec3145dfa77729d1250f2e` | same |
| Validation proofs per retained view | — | 1 |
| Modeled retained allocations | 98,304 | 98,321 |

## Closed V1 row-framed ledger

The independent V1 stage ledger uses the same 16,384-row low-cardinality
corpus, five warmups, and 31 release-mode samples. Physical bytes are 1,421,324;
logical cells are 65,536; modeled owned allocations are 98,304.

| Stage | p50 | p95 | p99 |
|---|---:|---:|---:|
| Canonical row decode and ordered-map reconstruction | 4,643,369 ns | 7,475,152 ns | 7,525,532 ns |
| Immutable row-map merge clone | 2,016,829 ns | 3,282,254 ns | 3,347,344 ns |
| Predicate | 43,070 ns | 84,470 ns | 96,691 ns |
| Exact aggregate | 44,160 ns | 48,910 ns | 50,360 ns |
| Candidate-key sort | 351,862 ns | 463,512 ns | 545,163 ns |
| Bounded 500-value output encoding | 14,270 ns | 23,190 ns | 23,990 ns |

The ledger attributes V1 costs; it is not substituted for the counterbalanced
V1/V2 full-decode gate.

## Retained rejected-candidate evidence

The earlier candidate at historical commit `92afdd2c` passed bytes and pruning
but failed the unchanged full-decode gate at 227.68% of V1 p50. The subsequent
validate-once candidate at historical commit `87d43a73` improved the unchanged
cell but still failed at 122.92% of V1 p50; its separately matched hot scan was
39.50% of V1. Both results remain negative evidence. Neither threshold, corpus,
sample count, nor interpretation was weakened, and neither historical result is
used as current qualification evidence.

The current implementation incorporates only the bounded single-pass decoder
mechanics from historical commit `655291df`: encoding-specific cursors build
final system and field lanes directly and accumulate exact statistics in the
same pass. The staged decoder remains test-only as an independent differential
oracle.

## Compatibility, recovery, and security evidence

- `LAYOUT_VERSION` remains V1; no production open, checkpoint, publication,
  compaction, recovery, or query path can select V2.
- Segment V2, manifest V2, and encoding-registry V1 are topology-registered as
  additive inactive identities. The checked compatibility fixture decodes and
  re-encodes byte-for-byte.
- Complete lengths and checksums, fixed bounds, logical types, validity states,
  canonical order and values, offsets, bit widths, row alignment, statistics,
  and end-of-input are checked before a lane is usable.
- Exact private statistics have zero false-negative pruning in the fixed and
  randomized corpora. They own no authority, policy, freshness, or public
  diagnostic, and no caller can select an encoding, statistic, pruning mode,
  validation frequency, fallback, or bound.
- The implementation is safe Rust and adds no external compression dependency.

## Checks executed

- `cargo +1.97.0 test -p riffdb-types -p riffdb-columnar -p riffdb-projection --all-features`
- `cargo +nightly-2026-07-12 fuzz run columnar_segment_v2 -- -max_total_time=60`
  (29,050,758 executions in 61 seconds; no crash)
- `cargo +1.97.0 test --release -p riffdb-columnar wp710_fixed_corpus_mechanics_receipt -- --ignored --nocapture --test-threads=1`
- `cargo +1.97.0 test --release -p riffdb-columnar wp710_validated_view_mechanics_receipt -- --ignored --nocapture --test-threads=1`
- `cargo +1.97.0 test --release -p riffdb-columnar wp710_v1_row_framed_workload_ledger -- --ignored --nocapture --test-threads=1`
- `./scripts/check-version-topology`
- `./scripts/check-workspace-policy`
- `./scripts/check-requirement-coverage`
- `cargo +1.97.0 fmt --all -- --check`
- `cargo +1.97.0 clippy -p riffdb-types -p riffdb-columnar -p riffdb-projection --all-targets --all-features -- -D warnings`

## Required PR description

```text
Package: WP-710
Tier: surface
Behavior added or changed: additive inactive bounded Segment V2 and manifest V2 codecs, closed physical encoding registry, exact private statistics and pruning oracle, validate-once view, and single-pass decoder; production remains V1
Checks run: package tests; 60-second decoder fuzz; topology; workspace policy; requirement coverage; formatting; Clippy; frozen full-decode, validated-view, and V1-ledger release receipts
Compatibility: additive inactive derived-format identities and a frozen byte fixture; existing production layout, manifest, query, projection, protocol, compiler, and authoritative identities are unchanged
Hazards and follow-ups: V2 remains inactive pending WP-711 disjoint rebuild, matched-frontier publication, recovery, compaction, cancellation, ENOSPC, policy-safe pruning, and rollback proofs; hardware CPU counters and actual allocator call counts were unavailable
Documentation: internal mechanics receipt only; no handbook change because no user-visible or operator-visible capability is activated
```
