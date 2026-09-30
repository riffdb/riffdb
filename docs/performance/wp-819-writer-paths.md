# WP-819: current writer reads and temporary allocations

Status: closed with a negative finding. Frozen base: `9fa3bfb28`, after
WP-818 and the integrated archive-selector work. Machine: 8 vCPU, AMD EPYC
9B14. Rejected candidate: `67b81a23f6`; restored runtime: `8d4f5fe47f`.
No throughput optimization is retained. Structured sanitized evidence is in
`release/evidence/wp-819/closure-20260930.json`.

## Frozen-base CPU profile

The base production daemon and benchmark runner were rebuilt, then a separate
profiling daemon with frame pointers sampled the busiest command thread for
25 seconds at 997 Hz. The diagnostic workload was TicketDesk write-only,
32 clients, 60 seconds measured, 10 seconds warmup. It holds an exclusive
benchmark reservation through build and sampling. No other remote connection
was made during the load. There were 18,128 recorded samples and no lost
samples. Profiling builds are diagnostic evidence only.

| Caller or symbol | Inclusive samples | Self samples |
|---|---:|---:|
| Apply command record set | 15.72% | 0.58% |
| Checkpoint point reads | 8.85% | 0.18% |
| Writer-private snapshot materialization | 6.27% | 0.09% |
| Prepare changelog successor | 6.02% | 0.01% |
| Insert overlay node | 5.41% | 1.09% |
| Overlay node `Arc::make_mut` | 3.76% | 3.05% |
| Rebase published composite read view | 3.48% | 0.01% |
| Rebase frozen overlay | 2.77% | 0.68% |
| Capture writer-private snapshots | 0.67% | 0.01% |

These callers are nested and their percentages must not be added. SHA-256
compression is 8.11% self; allocator allocation/free and libc symbols also
remain visible. Symbol names alone do not prove that any proposed replacement
will improve throughput. A concurrent task-clock counter reported 33.32 CPU
seconds for one thread during 25.00 wall seconds; that utilization figure is
invalid and is discarded. Direct per-thread CPU observations replace it for occupancy; raw scheduling
fields are retained separately and are not interpreted as blocked time.

## Bounded measurement probes

These probes existed at `c75d923ef` and were removed before production A/B.
The following describes historical diagnostic code, rather than current
interfaces.

The read probe emits numeric thread ordinal, role, physical table tag, source,
call count, present count, absent count, errors, copied payload bytes, and wall
nanoseconds. Roles distinguish published snapshot materialization,
writer-private snapshots, command record staging, mutation before-image
validation, and unclassified reads. Logical reads and their checkpoint
fallbacks are different sources and have nested intervals. Backend read
requests are not physical disk reads. No key, value, hash, or principal is
logged. A fixed-size thread-local table and restored nested scopes bound
measurement state.

The overlay probe counts fresh node allocation and actual clone ownership
transitions, key and value allocations, owned value-read copies, lineage key
copies, and rebase key copies. It reports only allocations at these named
sites and their payload sizes, excluding allocator/Arc headers, B-tree nodes,
vector growth, and unrelated allocations. It never discounts authoritative
memory accounting. Published predecessor snapshots remain immutable in the
counter tests.

Receipt probes separate source construction/coalescing, the enclosing writer
successor, and history advancement. Their intervals overlap. Input key/value
copies and owned payload allocations are distinguished from coalesced output
rows and canonical receipt size. Failed attempts remain failures, and a
zero-length boxed value is not counted as a payload allocation.

These temporary probes use the existing diagnostics switch. They select it
once for their own bounded counters. They do not change read answers, errors,
validation, canonical bytes, authorization, sequencing, publication, or
acknowledgement. No batching limit or delay changes. The 15-field checkpoint
census remains unchanged. Extent recycling remains WP-815.

## Live site census and first candidate

Diagnostic release run at `c75d923ef`, 32 clients, 60 seconds plus 10 seconds
warmup, counted 259,647 write commands including warmup. The busiest writer
used 18.26 CPU seconds during 25.015 wall seconds. This is CPU occupancy,
not per-command CPU or a durability-wait attribution.

| Writer site | Count per command | Allocated or copied payload bytes per command |
|---|---:|---:|
| Fresh overlay nodes | 13.453 | 753.394 |
| Cloned shared overlay nodes | 52.052 | 2,914.932 |
| Overlay keys | 9.049 | 525.425 |
| Overlay values | 8.979 | 4,589.468 |
| Owned overlay point values | 3.383 | 686.714 |
| Lineage keys | 8.986 | 524.921 |
| Rebase keys | 5.918 | 345.432 |

Receipt construction copied 1,328,002,253 key/value bytes and allocated
4,634,738 owned payloads on the writer. Its construction interval was
7.150 microseconds per command; enclosing successor preparation was 11.661,
including 4.428 for history advancement. These intervals are nested.

Writer entity reads outside private snapshot capture took 8.941 microseconds
per command (5.233 requests); 3.614 requests fell back to the checkpoint,
including 2.191 absences. Private snapshot entity reads added 0.618 requests
and 1.311 microseconds. Index epochs mostly resolved from current private
state: 0.951 requests, only 36 checkpoint fallbacks across the generation.
Idempotency, pending admission, and locator requests each numbered 2.007 per
command, all absent; their logical intervals were 0.714, 0.902, and 4.812
microseconds respectively. Staging index before-images took 6.599 microseconds
(1.617 requests), and entity-chain heads took 5.525 (1.400 requests).
No role-4 fallback occurred: current before-images were already supplied by
the typed writer path. Unclassified role-0 calls include transaction-current
validation; they must not be presented as all preparation work.

In the write-only diagnostic run, process physical read bytes were zero. Backend requests, nested point-read
wall time, and sampled CPU are separate measurements. Prefetch is deferred:
checkpoint entity fallback alone is a small opportunity, while adding exact
pinning, deduplication, unused reads, and memory ownership is more expensive
than the first allocation candidate. No checkpoint values may supersede the
current writer overlay.

Interactive diagnostic load finished, but shutdown evidence failed because
252 mirrored stderr lines since startup exceeded the runner's 200-line stderr ring and evicted
the startup census. It is not qualification evidence. Its writer counted
170,358 commands, 61.741 shared-node clones and 4.595 rebase key copies per
command. Removing the temporary probes restores the production output size;
no parser bound or failure check was weakened.

The first candidate borrows immutable lineage keys during rebase deduplication
and shares the surviving map key storage. It targets the measured 5.918
allocations per write command without a tree replacement, new surface, or
format change. Complete entry charges remain unchanged. The augmented rebase
test first failed on shared-key identity and also checks the retained charge,
old snapshot, post-checkpoint value, and fallback to the exact new checkpoint.
Temporary diagnostics are removed before production A/B. The production comparisons below reject this candidate.

## Initial production comparison

Candidate `67b81a23f6` versus base `9fa3bfb28`, diagnostics disabled,
32 clients, alternating base/candidate/candidate/base, 60 seconds measured
and 10 seconds warmup. All twelve arms completed successfully.

| Load | Base ops/s | Candidate ops/s | Change | Base / candidate p95 | Base / candidate p99 |
|---|---:|---:|---:|---:|---:|
| Write-only | 3,655.5 | 3,626.5 | -0.79% | 8.258 / 8.258 ms | 11.534 / 11.534 ms |
| Interactive | 16,174 | 16,098 | -0.47% | 7.078 / 7.078 ms | 9.437 / 9.437 ms |
| Read-only | 24,915.5 | 25,011.5 | +0.39% | 2.359 / 2.359 ms | 3.408 / 3.408 ms |

Write-only sampled writer CPU was 198.72 versus 200.92 microseconds per
process-generation command including warmup. Interactive was 260.12 versus
257.89; read-only has no command normalization. Sampling uses the busiest
command thread's accumulated process-generation ticks and can omit the final
sampling interval; it is not a sum of overlapping stage wall intervals.

Maximum write latencies were 717 versus 715 ms; interactive 1,039 versus
1,037 ms; read-only 10.38 versus 9.62 ms. Median peak sampled RSS was 1,177
versus 1,151 MB write-only, 1,028 versus 1,010 MB interactive, and 88 versus
87 MB read-only. Median heap envelopes were 1,208 versus 1,207 MB, 1,099
versus 1,092 MB, and 48 versus 49 MB respectively (decimal MB).

The candidate does not demonstrate a write-throughput improvement and is
removed. The direct frozen-base implementation is restored; only requirement
tags identify the existing exact-lineage regression coverage. No cause is
assigned to the small throughput differences. Longer alternating write runs and the writer stage census are reported below.
The same-run PostgreSQL comparison refreshes the remaining throughput gap on
the unchanged base.

## Longer comparison and pipeline census

The 90-second write-only repeat uses the same production binaries and
base/candidate/candidate/base order, with 15 seconds warmup. Base arms measured
3,195 and 3,390 ops/s; candidate arms 3,207 and 3,405. The common within-round
throughput drift is substantial; no cause is assigned without further
instrumentation. Median throughput is 3,292.5 versus 3,306 (+0.41%), p95 is
9.699 ms for both, and p99 is 14.418 versus 15.729 ms (+9.09%). Maximum
latencies are 1,027 versus 1,031 ms. Sampled writer CPU is 224.79 versus
224.17 microseconds per generation command. Median peak RSS is 1,296 versus
1,307 MB and heap envelope 1,430 versus 1,436 MB. Together with the negative
60-second comparison, this does not establish a reproducible improvement.
No candidate runtime code remains.

A separate frozen-base diagnostic run enables the existing writer stage
census. Its disjoint level-0 windows distinguish waiting for the next work
unit, completion drains, and execution; nested stages must not be added to
those windows. Logical command attempts include warmup and can differ from
committed-command counts.

| Metric | Write-only | Interactive |
|---|---:|---:|
| Observed command attempts | 258,801 | 167,554 |
| Whole writer-loop wall seconds | 70.007 | 75.735 |
| Wait for next work, microseconds/attempt | 45.843 | 57.023 |
| Pre-execution completion drain, microseconds/attempt | 8.156 | 11.917 |
| Execution wall, microseconds/attempt | 215.271 | 379.964 |
| Commands per journal flush | 15.919 | 6.998 |
| Maximum completion depth | 5 | 16 |
| Completion drain events | 1,014 | 658 |
| Command flush I/O wall seconds | 23.966 | 25.482 |

The serial journal encode/write/sync windows and the writer execution windows
are contained in this successful writer lifetime. Inclusion-exclusion gives
at least 9.671 and 13.412 seconds of overlapping **wall windows** respectively:
execution plus I/O minus the complete writer-loop duration. Execution includes
awaited evaluation/preparation and any synchronous fence completion, so this is not an estimate of useful CPU
running concurrently with I/O. Exact CPU overlap and cause-specific drain
counts were not collected. The existing drain conditions (non-deferrable
work, bounded suffix/live-root budget, completion capacity, and required
post-seal drain) are unchanged. Aggregate publication latency is not blocked
writer time and must not be added to I/O or execution.

Grouped published snapshots already run in preparation workers against one
captured root/frontier. Writer-private captures carry earlier unpublished
changes and complete dependent discovery through the existing bounded path.
Current entity observations and index generations are cached in the batch and
updated by staged changes. Admission and mutation-before-image checks still
resolve transaction-current state. A prefetched checkpoint value could only
serve an overlay miss; a current value or tombstone must take precedence, and
checkpoint identity, errors, absence, and epoch validation must remain exact.
No such prefetch is introduced here.

Overlay nodes already share immutable key/value payloads. The census's actual
clones are copies of shared search-path nodes, with reference-count ownership
changes, rather than duplicate key/value payload copies. Mutation-to-overlay
and lineage payload copies remain. Temporary receipt construction owns and
coalesces mutations, validates intervening predecessors, and encodes exact
history checksums even when its caller retains only binding/history state.
Removing these representations requires more design and semantic work than
the rejected rebase prototype. No borrowed receipt contract or canonical
codec changes are made.

## Refreshed PostgreSQL comparison

A clean checkout of restored runtime `8d4f5fe47f` builds the default production
release daemon, with diagnostics disabled. Its runtime implementation matches
`9fa3bfb28`; the only storage source difference is a requirement tag. The
safe-application comparator runs PostgreSQL 18.4, 32 clients, three 90-second
repetitions with 15-second warmup, and requires stability. Both workloads
complete successfully, with clean correctness, complete resource attribution,
comparable devices, idle-host checks, and eligible evidence.

| Load | RiffDB ops/s | PostgreSQL ops/s | Throughput ratio | p95 ratio | p99 ratio |
|---|---:|---:|---:|---:|---:|
| Write-only | 3,569 | 4,709 | 0.758 | 0.800 | 0.931 |
| Interactive with journeys | 15,835 | 15,516 | 1.021 | 0.903 | 0.864 |

The development-machine interactive PERF-008 gate passes. Write throughput
remains below PostgreSQL. These runs qualify the restored base, rather than
the rejected candidate. Maximum RiffDB latencies are 748 ms write-only and
1,041 ms interactive. Median peak sampled RSS is 1,338 and 1,134 MB, and heap
envelopes are 1,435 and 1,307 MB respectively. Whole-process CPU/resource
counters are retained in the evidence; writer CPU per command comes from the
separate A/B observations above. The common drift across series is disclosed;
no cause is inferred from code or these counters.

## Review note

Package: WP-819. Tier: guarantee. Behavior: bounded measurement and a rejected
immutable rebase-key sharing experiment; no retained runtime change. Checks:
candidate and restored-runtime `./scripts/ci-all` pass with four test threads;
final scoped acceptance passes all fifteen steps and all 541 storage-api tests;
probe acceptance passed fourteen steps, including 1,636 storage tests with
one existing scale-test skip. The ordinary parent crash harnesses exercise
recovery, overlap, absence, checkpoint rebase, cancellation, and failure.
The comparator repetitions are stable and eligible. Preexisting runner
warnings concern unused default-feature gRPC codec items and four missing
scrub-field comments; the handbook retains its existing large-index warning.
Legacy ADR length warnings are unchanged. Workspace all-feature clippy passes
without warnings. Closure acceptance passes all fifteen steps.

Compatibility: public surfaces, durable bytes, hashes, errors, bounds,
validation, ordering, and supported export are unchanged. No acceptance
check or oracle was weakened. Updated handbook pages are this report and
`docs/SUMMARY.md`; operational and application behavior cannot change because
no runtime optimization remains. Hazards and follow-ups: write throughput
remains below PostgreSQL; long allocation stalls and extent recycling belong
to WP-815; offline upgrades remain WP-816; the alternate-machine gate is not
run. WP-818's additional lifecycle authority-oracle RepairAborted gap remains.
Exact CPU overlap and drain causes need instrumentation before changing
pipeline policy. The next profiling target is intake/preparation during the
measured 45.843 microseconds per write-command work-receive wait, while
preserving private-overlay precedence and every admitted-group bound.
TicketDesk does not qualify projections, tokenized text, or row-policy
performance. Borrowed receipts and bounded checkpoint prefetch are evaluated
opportunities, rather than available interfaces.
