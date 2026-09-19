# Banked baseline: C3D, 2026-09-19

Receipts for the 2026-09 performance programme, taken on the C3D bench host
(`bench-host-c3d`) with `~/tmp/c3d-probe2.sh`, one repetition per cell,
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
