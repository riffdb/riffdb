# WP-819: current writer reads and temporary allocations

Status: open. Frozen base: `9fa3bfb28`, after WP-818 and the integrated
archive-selector work. Machine: 8 vCPU, AMD EPYC 9B14. No performance
improvement is claimed before production alternating comparisons and same-run
safe-application PostgreSQL qualification.

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
invalid and is discarded. Direct per-thread CPU and scheduling observations
will replace it before any blocked-time or pipeline claim.

## Bounded measurement probes

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

| Writer site | Count per command | Copied payload bytes per command |
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

Process physical read bytes were zero. Backend requests, nested point-read
wall time, and sampled CPU are separate measurements. Prefetch is deferred:
checkpoint entity fallback alone is a small opportunity, while adding exact
pinning, deduplication, unused reads, and memory ownership is more expensive
than the first allocation candidate. No checkpoint values may supersede the
current writer overlay.

Interactive diagnostic load finished, but shutdown evidence failed because
283 lines since startup exceeded the runner's 200-line stderr ring and evicted
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
Temporary diagnostics are removed before production A/B. Benefit remains
unproven until alternating comparisons.

## Review note

Package: WP-819. Tier: guarantee. Behavior: measurement and candidate immutable
key reuse during checkpoint rebase. Production comparisons and final checks
remain pending. Compatibility: no public surface, durable format, bounds, or
validation change. This page and the handbook index document the measurement
scope. Hazards: write throughput remains below PostgreSQL, long extent
allocation stalls remain, and WP-818's lifecycle authority-oracle qualification
gap is unchanged. TicketDesk does not qualify projections, tokenized text,
or row-policy performance.
