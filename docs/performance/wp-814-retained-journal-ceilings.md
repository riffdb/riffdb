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
- Candidate: main at `20e3657cb` plus the Decision 7 ceilings and the changes
  listed below, in one tree.

## Result

| Load | Arm | Throughput (ops/s) | p99 | Maximum |
|---|---|---|---|---|
| Write-only | base | 3,446 / 3,548 | 109.1 / 96.5 ms | 219 / 196 ms |
| Write-only | candidate | 3,621 / 3,656 | 31.5 / 31.5 ms | 587 / 283 ms |
| Interactive | base | 16,586 / 16,675 | 10.0 / 9.4 ms | 167 / 170 ms |
| Interactive | candidate | 16,876 / 16,288 | 10.5 / 11.0 ms | 224 / 252 ms |

Write-only throughput is 4.1% higher and its p99 is a third of the base.
Interactive throughput is unchanged within the host's 2–4% spread, with p99
about 1 ms higher. The maximum latency is higher in both loads, up to 2.8× the
base in one write-only run. The worst journal sync fell from 686 ms, before the
zero-fill was paced, to 171–275 ms. The peak overlay charge a checkpoint
covered was 71–73 MiB of the 256 MiB ceiling (`riffdb-writer-overlay-census-v1`).

## Bytes and memory

| Load | Arm | Durable growth per mutation | Process writes per mutation | Peak RSS |
|---|---|---|---|---|
| Write-only | base | 19,612 / 18,983 B | 111,169 / 111,887 B | 516 / 531 MiB |
| Write-only | candidate | 14,038 / 13,911 B | 107,439 / 107,253 B | 1,104 / 1,131 MiB |
| Interactive | base | 19,474 / 19,529 B | 199,783 / 206,958 B | 420 / 427 MiB |
| Interactive | candidate | 12,766 / 11,583 B | 195,154 / 193,377 B | 978 / 963 MiB |

Durable growth per mutation falls 28% (write-only) and 38% (interactive). It
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
- **The 320 MiB spare's zero-fill was one burst** that journal syncs queued
  behind. The background fill now syncs every 8 MiB.
- **Sealing refused a direct commit between journaled frames** (fixed in
  `20e3657cb`), which fenced writes on the first sealed extent.

## Limits

- Retained extents are not yet recycled (WP-815), so every checkpoint still
  zero-fills a new 320 MiB extent. That doubles the sequential bytes written
  for journal growth, and it is the likely source of the remaining maximum.
- Two repetitions per arm show direction, not a distribution.
- Test builds keep the 40 MiB extent; only release builds use 320 MiB.
- Peak resident memory is about 1.1 GiB under write-only load, against about
  0.5 GiB at the previous ceilings.
