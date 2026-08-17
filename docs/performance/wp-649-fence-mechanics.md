# WP-649 journal-fence mechanics gate

Date: 2026-08-17

Status: reject-first mechanics complete; production dispatch unchanged.

## Question

ADR-0132 asks whether a bounded append/fence pipeline or concurrent same-file
durability calls can remove the measured ordered-completion tail without
weakening RiffDB's gap-free durable and published frontiers.

The production journal currently drains every immediately ready submission,
writes its frames positionally into one pre-zeroed extent, and performs one
`fdatasync` for the complete ready prefix. The mechanics gate compares that
`coalesced_ready` control with:

- one `fdatasync` per logical group (`serial_per_group`);
- a bounded producer plus one coalescing durability worker
  (`pipelined_coalesced_fence`);
- persistent workers issuing concurrent writes and `fdatasync` calls against
  cloned handles for the same inode (`concurrent_same_file`); and
- the same workers using separate files (`concurrent_separate_files`).

Every file is completely zero-filled and fenced before measurement. Prefix
sizes are 4 KiB, 64 KiB, and 256 KiB. Depths are one, two, and four, with 128
iterations per cell. The probe uses real disk roots under `~/tmp/perf-db` and
refuses `/tmp`.

Activation required at least a 20% p95 improvement on both cloud profiles and
no more than a 5% throughput loss. No candidate passes.

## Receipts

| Profile | Host | vCPU | Raw receipt SHA-256 |
|---|---|---:|---|
| Workstation | local development host | 32 | `a90527563416054ea4e96f73ffb5839f8e384bee14d3787d7153d7cc4214b652` |
| N1 | `34.138.100.108` | 8 | `4c3612994ac044a90d41c37ee201d8b26e210d41de29eef97b79f43c960d5c5b` |
| E2 | `35.231.16.106` | 8 | `ba8df98c12d5ff578feef0ddfa0c806cbb42a1e7b68595d78cd367f8fc632253` |

The raw receipts remain in `/home/kevin/tmp/perf-db/wp649/` on the originating
hosts. They use schema `riffdb-journal-fence-pipeline-probe-v1`.

## Cloud comparison against ready-drain coalescing

Values are candidate changes versus `coalesced_ready`. Positive p95 is worse;
negative throughput is worse.

### N1

| Bytes | Depth | Candidate | p95 | Throughput |
|---:|---:|---|---:|---:|
| 4 KiB | 2 | pipelined/coalesced | +108% | -50% |
| 4 KiB | 2 | concurrent same file | +48% | -31% |
| 4 KiB | 4 | pipelined/coalesced | +73% | -46% |
| 4 KiB | 4 | concurrent same file | +77% | -38% |
| 64 KiB | 2 | pipelined/coalesced | +33% | -28% |
| 64 KiB | 2 | concurrent same file | +40% | -31% |
| 64 KiB | 4 | pipelined/coalesced | +39% | -30% |
| 64 KiB | 4 | concurrent same file | +41% | -22% |
| 256 KiB | 2 | pipelined/coalesced | +29% | -26% |
| 256 KiB | 2 | concurrent same file | +18% | -21% |
| 256 KiB | 4 | pipelined/coalesced | -11% | -15% |
| 256 KiB | 4 | concurrent same file | -5% | -15% |

### E2

| Bytes | Depth | Candidate | p95 | Throughput |
|---:|---:|---|---:|---:|
| 4 KiB | 2 | pipelined/coalesced | +44% | -33% |
| 4 KiB | 2 | concurrent same file | +40% | -28% |
| 4 KiB | 4 | pipelined/coalesced | +88% | -46% |
| 4 KiB | 4 | concurrent same file | +134% | -58% |
| 64 KiB | 2 | pipelined/coalesced | +48% | -43% |
| 64 KiB | 2 | concurrent same file | +22% | -29% |
| 64 KiB | 4 | pipelined/coalesced | +47% | -33% |
| 64 KiB | 4 | concurrent same file | +59% | -42% |
| 256 KiB | 2 | pipelined/coalesced | +45% | -36% |
| 256 KiB | 2 | concurrent same file | +30% | -30% |
| 256 KiB | 4 | pipelined/coalesced | -13% | -15% |
| 256 KiB | 4 | concurrent same file | -10% | -15% |

## Decision

Concurrent same-file durability and the tested append/fence pipeline are
rejected. The closest result improves p95 by only 13% and loses 15% throughput;
it misses both predeclared thresholds. The current ready-drain coalescing is the
best tested journal-media shape for the production frame range.

This falsifies the claim that the 6.6--24.7 ms ordered completion tail can be
removed merely by issuing more `fdatasync` calls. The physical journal lane is
not changed. WP-649 proceeds only with a closed production ledger separating
coordinator group residence, journal queue/write/sync, receipt readiness,
ordered publication, notification, and acknowledgement. Any later production
candidate must be sized from that ledger and requires exact ADR-0132 acceptance.

## Current-HEAD interactive c32 completion ledger

Commit `e26cefda` added fixed-cardinality process-generation evidence without
changing dispatch, journal, publication, or acknowledgement. Each cell is the
same public gRPC interactive workload with 32 clients, two seconds of warmup,
and ten measured seconds. These short cells select the candidate; they are not
release evidence.

`group residence` is the age of the oldest selected groupable transition when
the next group is dispatched. It includes time spent queued behind the prior
writer unit. It is not an active batching-window timer. Journal queue begins at
submission and ends when the journal worker begins encoding. Durable-to-publish
begins when the successful fence receipt exists and ends when the ordered
publisher installs that frame. Publication work is the actual ordered install.

| Profile | Throughput | Aggregate p95 | CreateComment p50/p95 | Group residence mean | Journal queue mean | `fdatasync` mean | Durable-to-publish mean | Publish work mean |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Workstation | 34,138 ops/s | 4.98 ms | 4.72 / 7.60 ms | 0.99 ms | 0.54 ms | 1.28 ms | 0.50 ms | 0.03 ms |
| N1 | 7,307 ops/s | 20.97 ms | 18.87 / 29.36 ms | 4.82 ms | 0.16 ms | 1.30 ms | 4.04 ms | 0.16 ms |
| E2 | 8,631 ops/s | 16.25 ms | 14.16 / 23.07 ms | 3.20 ms | 0.15 ms | 1.16 ms | 2.61 ms | 0.12 ms |

The public service-stage means close to 96.6% of client-observed mean
CreateComment latency on the workstation, 90.9% on N1, and 86.9% on E2. The
remaining 0.17/1.78/1.95 ms is the same-process client, tonic, scheduling, and
histogram-boundary residual outside server service-stage clocks. The ledger
retains that residual explicitly; it does not attribute it to the database.

The cloud mechanism is therefore not publication computation and not a slow
durability syscall. The sole writer continues deterministic work for successor
groups while an earlier receipt becomes durable. It can poll and publish the
earlier FIFO unit only when it returns to the head of its loop. On N1 the
receipt waits 3.88 ms longer than the 0.16 ms required to publish it; on E2 it
waits 2.49 ms longer than the 0.12 ms required to publish it.

Raw receipt SHA-256:

- workstation: `36a2dce5150c175ec1f80691cf234f8d632db6df81fb809008e6175ebc74191b`
- N1: `168db664cca67fcef21f96cb4c8cda54f2a73d4f6fda98974282f0c7df38bde2`
- E2: `10878abe06044e894a15925d9284f7574f30335fac6520db6127f2e2584383c0`

## Completion-edge coalescing falsification

An isolated diagnostic build disabled only the existing two-millisecond
completion-edge collection window. It was never committed and is ineligible
for release evidence. The resulting group-residence age did not fall, proving
that the ledger field mostly measures queueing behind the busy writer rather
than the active collection window.

| Profile | Throughput change | Aggregate p95 change | CreateComment p50 change | Group-residence change |
|---|---:|---:|---:|---:|
| N1 | -1.9% | +5.0% | +5.6% | +1.1% |
| E2 | -3.4% | +3.2% | +3.7% | +2.2% |

Raw diagnostic SHA-256:

- N1: `7db9fe2e46b3479d8f14df00825cc2893b64e666d81d4e873adf42c237c47433`
- E2: `91a299c685e979ec76d7e8e9590bf36dd25e34eb49bbf7c29bec56b0b3176f6e`

The coalescing window remains enabled. Removing it is rejected: it makes every
measured public result worse and does not attack the observed residence.

## Candidate selected for exact-text review

The only evidence-sized branch remaining in ADR-0132 is an ordered completion
lane separate from the authoritative apply writer. The apply writer retains
all command evaluation, conflict ownership, sequence assignment, mutation, and
journal-submission authority. It hands a bounded FIFO of already-submitted
units to a completion owner that can wait for the oldest receipt, publish the
contiguous durable prefix, issue commit notifications, and release responses
while the apply writer prepares a successor.

The predeclared mean upper bound is removal of durable-to-publish wait above
actual publication work: 3.88 ms on N1 and 2.49 ms on E2. No throughput gain is
assumed. The branch proceeds only after exact ADR acceptance and only if its
deterministic schedules prove that publication never exceeds durability,
results never exceed publication, and failure of either lane fences all later
work.

A compile-only mechanics probe in an isolated `~/tmp` source tree added `Send`
as a supertrait of `DeferredCommandFence` and `CheckedCommandGroupFence`, then
required the complete `SubmittedWriterUnit: Send`. Rust 1.97 successfully
checked `riffdb-storage-api`, `riffdb-storage-redb`, and `riffdb-commit` with all
features and no unsafe code or representation change. This proves the existing
typed redb fence and captured read root can cross the proposed bounded lane; it
does not authorize the scheduling change.
