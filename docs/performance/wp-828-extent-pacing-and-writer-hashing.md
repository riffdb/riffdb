# WP-828: extent pacing and writer hashing

Status: complete (2026-10-08). Implements ADR-0280 (accepted 2026-10-07).
Base: `4489fa2f3`; candidate: `966492cbc`. Banked results come from the
ADR-0239 C3D host (8 vCPU, AMD EPYC 9B14, ext4 on local NVMe). Earlier
development figures come from a second machine (8 vCPU, AMD EPYC 7B12, ext4 on
a network persistent disk) that runs TicketDesk at roughly half the C3D's
throughput; they are retained as mechanism evidence only.

## C3D results

Production release builds, diagnostics disabled, TicketDesk `--full`, 32
clients. The comparison runs held the host exclusively: a driver waited for no
other build, test or daemon process and load below 1 before each arm, and
recorded any other process seen during an arm: a short foreign process for 1
and 4 seconds in the first two write-only arms, none in the others.

Alternating base/candidate/candidate/base, RiffDB only (60 s after 10 s warmup,
and a 90 s write-only repeat after 15 s warmup):

| Load | Base arms (ops/s) | Candidate arms (ops/s) | Median change | Base / candidate p95 | Base / candidate p99 | Base / candidate maximum |
|---|---|---|---:|---|---|---|
| Write-only, 60 s | 3,613; 3,671 | 4,253; 4,134 | +15.1% | 8.13–8.39 / 8.91–9.44 ms | 12.06–12.58 / 14.68–15.20 ms | 697–704 / 280–297 ms |
| Write-only, 90 s | 3,622; 3,588 | 4,135; 4,145 | +14.8% | 8.39 / 8.91 ms | 13.63–14.16 / 14.68 ms | 702–1,024 / 281–283 ms |
| Interactive (journeys), 60 s | 16,110; 16,106 | 17,615; 17,557 | +9.2% | 7.08 / 7.08–7.34 ms | 9.44 / 9.44–9.96 ms | 696–729 / 233–249 ms |
| Read-only, 60 s | 24,319; 24,274 | 24,316; 24,294 | 0.0% | 2.36 / 2.36 ms | 3.41–3.54 / 3.41 ms | 9 / 9 ms |

Every write-only and interactive candidate arm exceeds every base arm. The
recurring 0.7–1.0 s maximum stall is gone. Write-only p95 rises 6–11% and p99
5–21% as more commands complete per second at the same concurrency; this is a
real tail cost of the change at c32 and is disclosed rather than averaged away.
Process CPU per committed command falls 3.4% write-only (0.0622 to 0.0600
ticks) and 1.8% interactive; peak sampled RSS rises about 2.6% write-only. Most
of the gain is the ordered writer idling less: its idle time falls from about 14
to 9 s in the 60 s write-only runs.

Same-run PostgreSQL 18 `safe-app` qualification of the candidate: three 90 s
repetitions after 15 s warmup, `--require-stable`, run from a clean candidate
tree. Both reports are evidence-eligible (stable, correctness clean, resource
attribution complete, same device, idle host).

| Load | RiffDB median | PostgreSQL median | Throughput ratio | p95 ratio | p99 ratio |
|---|---:|---:|---:|---:|---:|
| Write-only | 4,191 ops/s | 4,678 ops/s | 0.896 | 0.81 | 0.97 |
| Interactive (journeys) | 17,569 ops/s | 15,338 ops/s | 1.145 | 0.87 | 0.86 |

The previous same-run qualifications (WP-818, WP-819) measured write-only at
0.758–0.768 and interactive at 1.021–1.048 of PostgreSQL. Comparing ratios
across sessions is indicative, not a measurement of this change; the A/B above
is. Write throughput remains below PostgreSQL.

## Changes

1. **Paced zero-fill.** Each checkpoint prepares a fresh 320 MiB journal extent
   beside the live journal. Preparation buffered the whole extent and synced it
   once, queueing all of it on the device ahead of the next journal `fdatasync`.
   It now syncs after every 2 MiB; the final sync and spare publication order are
   unchanged.
2. **Lazy insert digests.** The receipt accumulator hashed every put value. An
   insert can never be an exact restoration, so its digest is computed only if a
   later observation of the same key needs it.
3. **One receipt encoding.** History advancement encoded and hashed the full
   receipt, then encoded it again to persist it. A caller persisting the bytes
   now takes the history hash from the trailing checksum of that one encoding.
4. **Single-descent staging.** Overlay staging searched the persistent map once
   for the memory charge and again to insert. When the replacement fits even
   with nothing released, it now inserts once and charges from the replaced
   entry; near the bound it keeps the exact pre-check.

No durable byte, hash value, bound, ordering, publication or recovery rule
changes.

## Isolated zero-fill interference (development machine)

A 136 KiB write plus `fdatasync` loop on a preallocated file, measured while a
320 MiB extent is zero-filled on the same disk, three rounds each:

| Fill syncs | Worst journal sync | p99 journal sync |
|---|---:|---:|
| Once at the end | 175.9–177.4 ms | 51–132 ms |
| Every 8 MiB | 27.2–46.6 ms | 26 ms |
| Every 2 MiB | 7.6–7.8 ms | 6.9–7.0 ms |
| Every 1 MiB | 3.9–4.8 ms | 3.4 ms |

Fill time rises from about 1.2 s to 1.3 s at 2 MiB, off the writer's path.

## Development-machine comparisons

TicketDesk `--full`, 32 clients, 60 s measured after 10 s warmup, RiffDB only,
production release builds, alternating base and candidate arms (B/C/C/B twice).

Zero-fill alone, write-only:

| Metric | Base (4 arms) | Candidate (4 arms) |
|---|---|---|
| Throughput | 1,865–2,037 ops/s | 1,806–2,058 ops/s |
| Journal queue wait | 6.3–10.4 s | 1.7–2.1 s |
| Writer idle | 7.8–9.0 s | 4.6–5.1 s |
| Worst journal sync | 177–343 ms | 155–175 ms |
| Maximum latency | 355–530 ms | 306–347 ms |

At 64 clients (two arms each) zero-fill raised throughput about 10% and cut p99
from 168–193 ms to 40–42 ms.

All four changes, against the same base:

| Load | Base median | Candidate median | Change | Base / candidate p99 |
|---|---:|---:|---:|---|
| Write-only | 1,947 ops/s | 2,171 ops/s | +11.5% | 48 / 23 ms (medians) |
| Interactive (journeys) | 8,796 ops/s | 9,388 ops/s | +6.7% | 19.4 / 18.9 ms |

Every write-only candidate arm exceeded every base arm. Interactive arms overlap
(one candidate arm, 8,524, is below one base arm) and both series drift down
over the session, so its change is directional. Maximum latency fell from
354–525 ms to 280–355 ms write-only and from 357–525 ms to 254–280 ms
interactive. Process CPU per committed command fell about 3%; most of the
write-only gain is the writer idling less.

## Rejected in the same investigation

- **Persistent B+tree overlay** with node-local prefixes and key heads: about
  2x faster lookups at 300K keys in isolation, but -1.1% write-only and -2.1%
  interactive end to end with unchanged writer busy time. Per-table overlays
  stay small between checkpoints on this workload.
- **Authentication principal cache:** an unsafe skip-everything ceiling cut
  authentication from about 5 us to 1 us per request and changed no throughput.
  The capability reader already serves lookups from an in-memory view.
- **Bounded writeback for the redb checkpoint commit** (a data sync through a
  second file handle every 2 MiB written to the primary database): write-only
  fell from 2,052–2,179 to 1,782–1,816 ops/s and p99 rose from 22–27 ms to
  50 ms. The checkpoint's writes are small and random, so each 2 MiB sync is
  slow; journal syncs over 15 ms went from 12 to 380 per run, the worst falling
  from about 150 ms to 47 ms while the journal queue wait rose from about 2 s to
  8–11 s. One large burst per checkpoint is cheaper here than many small ones.

## Review note

Package: WP-828. Tier: guarantee; ADR-0280 was accepted on 2026-10-07 at its
committed text. Checks: package acceptance (15 of 16 steps; 1,775 nextest tests
passed, 2 skipped; `check-adr-obligations` fails identically on the base for
ADR-0275 to ADR-0277), clippy with warnings denied, rustfmt, and the storage
all-feature suites on the development machine; the zero-fill tests fail on the
single-sync implementation. Compatibility: no public, durable or configuration
change. Hazards and follow-ups: write-only c32 p95 and p99 rise modestly; write
throughput remains below PostgreSQL; redb checkpoint commits remain the largest
journal stall source; extent recycling remains WP-815; `./scripts/ci-all` was
not run before merge.
