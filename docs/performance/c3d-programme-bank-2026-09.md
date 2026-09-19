# Banked baseline: C3D, 2026-09-19

Receipts for the 2026-09 performance programme, taken on the C3D bench host
(`104.196.193.252`) with `~/tmp/c3d-probe2.sh`, one repetition per cell,
20-second measurement window after a 5-second warmup, RiffDB only, PostgreSQL
comparator skipped.

## Revisions

| tag | revision | what it is |
|---|---|---|
| `a54511b5` | `a54511b51960` | pre-programme main |
| `perf6` | `7bc0a4d95` | the programme's code changes, system allocator |
| `perf7-jemalloc` | `97b90f6e1` | the same code with jemalloc (ADR-0238) |

## write_only, throughput ops/s

| clients | main | perf6 | perf7 | vs main |
|---|---|---|---|---|
| 1 | 593 | 661 | 671 | +13.2% |
| 8 | 2,141 | 2,372 | 2,622 | +22.5% |
| 32 | 2,753 | 3,035 | 3,555 | +29.1% |
| 128 | 2,864 | 3,197 | 3,739 | +30.6% |

p95 at 128 clients fell from 88.1 ms on main to 54.5 ms.

## read_only, throughput ops/s

| clients | perf6 | perf7 | p50 | p95 |
|---|---|---|---|---|
| 1 | 3,824 | 3,832 | 0.26 ms | 0.33 ms |
| 8 | 18,981 | 19,454 | 0.39 ms | 0.66 ms |
| 32 | 28,963 | 29,509 | 1.02 ms | 1.90 ms |
| 128 | 31,087 | 31,585 | 3.93 ms | 7.08 ms |

## interactive, throughput ops/s

| clients | perf6 | perf7 | p50 | p95 |
|---|---|---|---|---|
| 8 | 10,303 | 10,888 | 0.38 ms | 2.62 ms |
| 32 | 16,121 | 17,348 | 0.75 ms | 7.60 ms |

## PostgreSQL comparison, gate cell

`interactive` at 32 clients against the safe-app PostgreSQL 18.4 comparator,
`--full`, three repetitions, counterbalanced phase order, revision `3f244c545`.

| | RiffDB | PostgreSQL | ratio | gate | |
|---|---|---|---|---|---|
| throughput ops/s | 15,818 | 15,711 | 1.007 | >= 0.90 | pass |
| p95 | 8.91 ms | 8.13 ms | 1.097 | <= 1.25 | pass |
| p50 | 0.72 ms | 1.18 ms | 0.611 | none | RiffDB 39% faster |

The harness reports this cell `eligible: true`, `non_evidentiary_window: false`,
`correctness_clean: true`, `same_device_comparable: true`. Repetition spread is
1.0027 on RiffDB and 1.0143 on PostgreSQL, both well inside ADR-0171's 1.20
stability rule. The comparator is genuinely durable: `fsync=on`,
`full_page_writes=on`, `synchronous_commit=on`, `wal_sync_method=fdatasync`.
Dataset is 19,220 rows across 10 organizations.

The previously banked figures for this cell were 0.88 throughput against the
0.90 gate and 1.38/1.23 p95 against the 1.25 ceiling. Throughput was the
failing criterion and now clears. **This clears the gate on C3D, which is not
where the gate is defined.** PERF-018 fixes the comparator on the N1 and E2
profiles, and ratios do not port between hosts, which is also why the 0.88
figure is not directly comparable to the 1.007 above. Confirming this requires
the same cell on N1 and E2.

### The smoke reading of the same cell was wrong by 2.3x

Run at `--smoke` first, the same cell reported a throughput ratio of 2.30
rather than 1.007, and `write_only` reported 2.55. Smoke seeds 404 rows against
full's 19,220, and at that size the comparison means nothing: the harness marks
smoke runs `eligible: false` for exactly this reason. Recorded here because the
smoke number looks like good news and is not, and because the gap is scale, not
host.

For reference, the smoke cells were: interactive minimal 1.91, interactive
safe-app 2.30, write_only minimal 2.36, write_only safe-app 2.55.

### Where the write-path advantage is real

On `write_only` the advantage is structural rather than an artifact of scale.
The harness measures this device at about 1,030 fdatasync per second.
PostgreSQL at `synchronous_commit=on` pays one WAL fsync per transaction, which
puts a ceiling near that figure, and it lands at 1,394 to 1,427 ops/s. RiffDB
group-commits roughly 16 commands per flush and is not bound by it. That is the
durable-group mechanism ADR-0183 describes. `interactive` is read-heavy, which
is why the two engines land level there while diverging on writes.

## What these numbers do and do not say

**No prior comparison exists for read_only or interactive.** Those loads had
never been run on this host before this programme, so their columns are a first
baseline, not an improvement. Only the write_only column can be compared to
main, because only write_only was measured there.

**Run-to-run variance on this host is 2 to 4 percent, not 1 percent.** Two
banks of identical write-path code (`perf5` and `perf6`) differed by up to 3.9
percent at the same client count. Any single-run delta smaller than about 4
percent should be treated as noise unless it reproduces. This is why the
cross-crate input-facts threading, measured at roughly 1 percent, was reverted
rather than kept.

**These are RiffDB-only receipts.** They carry no comparator ratio and are not
the dual-profile gate evidence ADR-0171 requires; they measure change against
our own prior revision on one host.

**Coverage is partial.** The ticketdesk contract declares no projection, no
text index, and no row policy, so the projection evaluator, the tokenized-text
provider, and row-policy evidence are not exercised by any cell above. See
`deferred-dormant-path-optimizations.md`.

## Host and method

Measured with `benchmarks/run-app-baseline --smoke`. The jemalloc comparison
against mimalloc and the system allocator was run twice per allocator per
level; jemalloc led at every level by 1 to 2 percent over mimalloc. The
allocator probe branch that carried a workspace-wide lint relaxation was never
a merge candidate and was not the branch measured here: `perf7-jemalloc` was
verified to carry `unsafe_code = "forbid"` at the workspace before measurement.
