# WP-828: extent pacing and writer hashing

Status: open. Implements ADR-0280 (accepted 2026-10-07). Base: `4489fa2f3`. The figures
below come from a development machine (8 vCPU, AMD EPYC 7B12, ext4 on a
network persistent disk), which runs
TicketDesk at roughly half the ADR-0239 C3D host's throughput. They show
direction only: no C3D comparison or same-run PostgreSQL qualification has been
run, and nothing here is banked.

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

## Isolated zero-fill interference

A 136 KiB write plus `fdatasync` loop on a preallocated file, measured while a
320 MiB extent is zero-filled on the same disk, three rounds each:

| Fill syncs | Worst journal sync | p99 journal sync |
|---|---:|---:|
| Once at the end | 175.9–177.4 ms | 51–132 ms |
| Every 8 MiB | 27.2–46.6 ms | 26 ms |
| Every 2 MiB | 7.6–7.8 ms | 6.9–7.0 ms |
| Every 1 MiB | 3.9–4.8 ms | 3.4 ms |

Fill time rises from about 1.2 s to 1.3 s at 2 MiB, off the writer's path.

## End-to-end comparisons

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
committed text. Checks: storage-api and storage-redb all-feature suites
(1,820 passed, 0 failed, 2 ignored), clippy with warnings denied and rustfmt on
the bench machine; the zero-fill tests fail on the single-sync implementation.
Compatibility: no public, durable or configuration change. Hazards and
follow-ups: C3D confirmation and same-run PostgreSQL qualification; read-only
comparison; redb checkpoint commits remain a large write burst; extent recycling
remains WP-815.
