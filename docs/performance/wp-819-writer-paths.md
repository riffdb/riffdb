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

## Review note

Package: WP-819. Tier: guarantee. Behavior: investigation and opt-in numeric
measurement; no retained optimization yet. Checks and live allocation/read
results are pending. Compatibility: no public surface or durable format
change. This page and the handbook index document the measurement scope.
Hazards: write throughput remains below PostgreSQL, long extent-allocation
stalls remain, and WP-818's lifecycle authority-oracle qualification gap is
unchanged. This TicketDesk workload does not qualify projections, tokenized
text, or row-policy performance.
