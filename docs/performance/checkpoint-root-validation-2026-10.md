# Checkpoint root validation once per batch

Status: implemented (2026-10-08) on `fc357d14e`, base `607e9b6de` (WP-828
closure). Internal tier; no package. No durable byte, protocol, public
interface, ordering, publication, or acknowledgement rule changes.

## Finding

A c32 write-only TicketDesk profile of the profiling build (frame pointers,
25 s of user-space samples on the measured daemon, development workstation:
32 threads, AMD Ryzen 9 7950X) put the asynchronous journal checkpoint
threads at 20.7% of daemon CPU. Inside them:

| Checkpoint stage | Share of checkpoint samples |
|---|---:|
| Full changelog root validation (`read_checkpoint_roots_for_write`) | 64.2% |
| of which SHA-256 compression | 44.4% |
| Mutation application into redb | 24.8% |
| Receipt re-derivation and encoding | about 6% |
| redb commit | 2.2% |

The materializer validated the roots three times per frame: before the
frame, inside history-advance preparation, and after staging. Each full
validation resolves the terminal receipt through its history stub, which
re-reads the previous frame from the journal extent, verifies its physical
and logical frame hashes, re-derives the receipt and hashes it again. That
was the largest redundant hashing in the process; the receipt re-derivation
first suspected was about 6% of the checkpoint.

## Change

A checkpoint batch is one redb transaction, so no state between two of its
frames is durable or visible. The roots, including the terminal receipt
resolved through the history index, are now validated in full once before
the first frame and once after the last. Between them the history state is
the successor the previous frame staged in the same transaction.
`PreparedHistoryAdvance::prepare_chained` still refuses, per frame: a stored
history-state or allocator row that differs from that state, a physical
application/administration frontier that differs from the chain tail, a
receipt the admission roots refuse, a mismatched allocation, and an occupied
history slot. Each receipt is still derived from its frame and bound to the
recorded binding, as ADR-0263 Decision 2 requires.

What moves: the registry, lineage, follower and admission-root checks run at
batch start and end rather than per frame (frame mutations cannot write those
rows), and the history stubs of frames before the last are not resolved
against the extent at checkpoint time. Readers and the background scrub still
resolve every stub and refuse a mismatch as typed corruption. The maintainer
reviewed and accepted this in session.

Recovery and the direct-write journal-suffix drain
(`materialize_native_frame`) are unchanged and still validate per frame.

`multi_frame_checkpoint_validates_roots_once_per_batch_and_matches_sequential_receipts`
materializes three chained frames, requires each receipt to equal sequential
recovery-path application and the final history state to match, refuses a
corrupted middle binding or middle mutation set without committing, and
requires exactly two root validations. It records nine on the base.

## Mechanism evidence (development workstation)

The same profile on the candidate put checkpoint threads at 8.6% of daemon
CPU, with root validation no longer visible. Alternating release arms on the
shared workstation measured daemon CPU per committed command at 418–476 µs
(base) and 337–407 µs (candidate); its throughput varied too much with
neighbouring load to compare.

## C3D results

Production release builds, diagnostics disabled, TicketDesk `--full`, 32
clients, 60 s measured after 10 s warmup, RiffDB only, three rounds of
base/candidate/candidate/base per load on the ADR-0239 C3D host (8 vCPU, AMD
EPYC 9B14, ext4 on local NVMe), otherwise idle. Medians of six arms, ranges in
brackets.

| Load | Base ops/s | Candidate ops/s | Change | CPU µs per command | p95 / p99 | Peak sampled RSS |
|---|---|---|---:|---|---|---|
| Write-only | 4,222 [4,142–4,237] | 4,457 [4,293–4,513] | +5.6% | 598 → 501 | 8.91 / 14.68 ms both | 1,154 → 1,006 MiB |
| Interactive (journeys) | 17,672 [17,550–17,792] | 18,604 [18,484–18,691] | +5.3% | 1,727 → 1,606 | 10.49 / 13.11 → 9.96 / 12.58 ms | 960 → 845 MiB |

Every candidate arm exceeds every base arm in both loads. Median maximum
latency was 288 ms in both write-only arms and rose from 264 to 281 ms
interactive.

## Same-run PostgreSQL qualification

PostgreSQL 18 `safe-app` comparator on the same C3D host, run from a clean
checkout of `fc357d14e`: three 90 s repetitions after 15 s warmup per load,
`--require-stable`, interactive with `--load-journeys`. Both reports are
evidence-eligible (stable, correctness clean, resource attribution complete,
same device, idle host). Ratios divide the medians in `load_rep_summaries`.

| Load | RiffDB median | PostgreSQL median | Throughput ratio | p95 ratio | p99 ratio |
|---|---:|---:|---:|---:|---:|
| Write-only | 4,393 ops/s | 4,642 ops/s | 0.946 | 0.85 | 0.97 |
| Interactive (journeys) | 18,586 ops/s | 15,362 ops/s | 1.210 | 0.81 | 0.82 |

WP-828's same-run qualification measured 0.896 write-only and 1.145
interactive. Comparing ratios across sessions is indicative; the alternating
comparison above is the measurement of this change. Write throughput remains
below PostgreSQL.

## Second development machine

On the 8 vCPU AMD EPYC 7B12 machine with a network persistent disk (eight
write-only and four interactive arms per side, same alternation): interactive
rose 2.9% (6,652 → 6,844 ops/s) with CPU per command 5.6% lower; write-only
throughput was unchanged (1,468 → 1,460, overlapping ranges) with CPU per
command 4% lower. That host is bound by its disk under write-only load, so the
saved CPU does not become write throughput there. Median maximum latency fell
from 371 to 270 ms write-only and from 362 to 268 ms interactive.

## Item not taken: journal and writer hash consolidation

The same profile sized the other hashes. The journal worker's extent-frame
checksum, which re-hashes the padded physical frame, costs about 40 µs per
group; the writer's receipt and frame hashes are about 5% of command-thread
samples. Removing either changes durable bytes (a new extent-frame version
under ADR-0103, or a receipt history-hash change on the V3 changelog export
surface under ADR-0186) and is not justified by these numbers.

## Review note

Package: none. Tier: internal. Checks: the new test fails on the base; the
riffdb-storage-redb all-feature suite (1,217 tests) passes; acceptance fmt,
clippy, file-size and panic-allowance guards pass; one ci-profile test timed
out at 120 s under workstation load and passes alone in 30.7 s (base 31.4 s).
Compatibility: no durable, protocol, or public change. Hazards and follow-ups:
recovery and the journal-suffix drain under the writer gate keep three root
validations per frame and are the next candidate; write throughput remains
below PostgreSQL (0.946).
