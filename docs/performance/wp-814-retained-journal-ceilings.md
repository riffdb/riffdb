# WP-814: retained journal history at the Decision 7 ceilings

ADR-0263 moves journaled command history out of redb and into retained journal
extents, then raises the suffix ceilings (Decision 7): 65,536 transitions,
256 MiB of encoded suffix, a 320 MiB extent, and a 256 MiB overlay ceiling.
This page records the live-server comparison the ADR requires before the
package closes. It is measurement evidence, not a throughput promise.

## Setup

- Host: the ADR-0239 dev-standard host, c3d-standard-8 (8 vCPU, AMD EPYC 9B14),
  one shared persistent disk for database and journal.
- Load: `riffdb-app-baseline --full`, 32 clients, 60 s measured after 10 s of
  warm-up, write-only and interactive, one repetition per run.
- Order: base, candidate, candidate, base for each load, so drift shows as a
  base-to-base difference.
- Base: main at `485a40268`, before any ADR-0263 implementation, with the
  ADR-0104 ceilings (8,192 transitions, 32 MiB suffix, 40 MiB extent).
- Candidate: the committed Decision 7 build, the ceilings and every change
  listed below.

## Result

| Load | Arm | Throughput (ops/s) | p95 | p99 | Maximum |
|---|---|---|---|---|---|
| Write-only | base | 3,438 / 3,579 | 10.0 / 10.0 ms | 109.1 / 100.7 ms | 309 / 178 ms |
| Write-only | candidate | 3,416 / 3,546 | 8.9 / 8.4 ms | 13.6 / 13.1 ms | 718 / 742 ms |
| Interactive | base | 16,455 / 16,583 | 7.3 / 7.1 ms | 10.0 / 9.4 ms | 166 / 181 ms |
| Interactive | candidate | 16,023 / 16,056 | 7.3 / 7.6 ms | 10.0 / 10.0 ms | 696 / 696 ms |

Write-only p99 falls from about 105 ms to 13 ms and p95 from 10 ms to under
9 ms, at unchanged throughput. Interactive throughput is 2.9% lower, at the edge
of the host's 2–4% spread, with p95 and p99 unchanged. The maximum rises to
about 700 ms in both loads: journal syncs of up to 687 ms queue behind each
checkpoint's 320 MiB zero-fill. The peak overlay charge a checkpoint covered
was 72–73 MiB of the 256 MiB ceiling (`riffdb-writer-overlay-census-v1`).

## Bytes and memory

| Load | Arm | Durable growth per mutation | Process writes per mutation | Peak RSS |
|---|---|---|---|---|
| Write-only | base | 19,636 / 18,893 B | 112,444 / 111,672 B | 519 / 530 MiB |
| Write-only | candidate | 14,938 / 14,311 B | 108,923 / 107,431 B | 1,105 / 1,115 MiB |
| Interactive | base | 27,226 / 27,012 B | 200,753 / 205,127 B | 421 / 426 MiB |
| Interactive | candidate | 12,435 / 12,414 B | 197,228 / 195,281 B | 927 / 936 MiB |

Durable growth per mutation falls 24% (write-only) and 54% (interactive). It
includes the retained journal extents, so redb's own share falls further. Peak
resident memory roughly doubles: the uncheckpointed suffix a checkpoint covers
is eight times larger, and the writer keeps its frames and mutations in memory
until the checkpoint installs.

## What the first runs found

The first candidate lost 15% of write-only throughput and stalled for up to
1.2 s. Each cause was measured with a writer-thread profile or a stall trace
and removed:

- **Stubbed segments missed the payload cache.** The overlay holds a stub, the
  cache held the segment bytes, so the writer re-decoded every segment. The
  cache now matches a stub by its segment's first sequence and digest.
- **Checkpoint materialization re-decoded every segment** to build its stub.
  The writer now records each segment's summary when it builds it.
- **Installation waited for the sealer** on the writer's path. Retiring and
  sealing the installed extent now runs on the sealer thread; the next
  rotation waits for the checkpoint name instead.
- **Sealing fully decoded every frame.** It now checks each frame against the
  hash its header recorded and skips re-decoding its segments.
- **The 320 MiB spare's zero-fill is one writeback burst** that journal syncs
  queue behind, and it produces the remaining maximum. Syncing the fill every
  8 MiB cut the maximum to 283–587 ms but moved write-only p95 from under 9 ms
  to 10–19 ms and p99 to 31 ms, so it was reverted.
- **Sealing refused a direct commit between journaled frames** (fixed in
  `20e3657cb`), which fenced writes on the first sealed extent.

## Same-run PostgreSQL comparison

`run-app-baseline --full --load <load> --load-clients 32 --load-duration-secs 90
--load-warmup-secs 15 --postgres-comparator safe-app --reps 3 --require-stable`
(`--load-journeys` for the mixed load), on the same host. Ratios are RiffDB over
PostgreSQL medians. PERF-008 gates the mixed load at throughput ≥ 0.90× and
p95 ≤ 1.25×.

| Load | Build | Throughput | p95 | p99 |
|---|---|---|---|---|
| Mixed (interactive) | base | 1.085× | 0.871× | 0.818× |
| Mixed (interactive) | final | 1.033× | 0.937× | 0.909× |
| Write-only | base | 0.702× | 0.950× | 8.276× |
| Write-only | final | 0.740× | 0.857× | 1.000× |

The mixed-load gate passes before and after on this host. On write-only, the
final build removes the p99 stalls (8.3× to 1.0× PostgreSQL) and keeps p95
below PostgreSQL. Write-only throughput stays near 0.74×: the ordered writer's
CPU per command bounds it, and this package does not change that. The final
write-only report is marked unstable only because PostgreSQL's p99 varied 2.14×
across repetitions; every RiffDB metric varied at most 1.07×.

## Limits

- Retained extents are not yet recycled (WP-815), so every checkpoint still
  zero-fills a new 320 MiB extent. That doubles the sequential bytes written
  for journal growth and produces the remaining maximum, about 700 ms.
- Write-only throughput remains about 0.74× PostgreSQL.
- Two repetitions per arm show direction, not a distribution.
- Test builds keep the 40 MiB extent; only release builds use 320 MiB.
- Peak resident memory is about 1.1 GiB under write-only load, against about
  0.5 GiB at the previous ceilings.
