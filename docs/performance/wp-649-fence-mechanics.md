# WP-649 journal-fence mechanics gate

Date: 2026-08-17

Status: implementation complete; mixed-load tail gate passed with the bounded
session, but the conjunctive unary gate did not pass. No alpha performance or
`PERF-018` release activation is claimed.

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
| N1 | `bench-host-n1` | 8 | `4c3612994ac044a90d41c37ee201d8b26e210d41de29eef97b79f43c960d5c5b` |
| E2 | `bench-host-e2` | 8 | `ba8df98c12d5ff578feef0ddfa0c806cbb42a1e7b68595d78cd367f8fc632253` |

The raw receipts remain in `/home/user/tmp/perf-db/wp649/` on the originating
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

## Accepted implementation checkpoint

The maintainer accepted the exact ADR-0132 text on 2026-08-17. The production
coordinator now retains all storage, conflict, sequence, mutation, and journal
submission authority on the sole apply writer and transfers only an opaque
submitted unit to a separate bounded FIFO completion owner. The owner waits the
oldest checked command or audit fence, publishes the contiguous prefix, emits
notifications and terminal telemetry, and releases responses. A bounded
apply-side shadow queue retains only transition counts and compiler-proved
private-frontier eligibility; it carries no result or durability authority.

The channel and shadow queue are capped at 256 units and the pre-existing
256-transition journal-suffix ceiling remains authoritative. A full lane or a
barrier drains the oldest completion; no timer, caller setting, or public
protocol shape was added. Fixed-cardinality `riffdb-completion-lane-v1`
shutdown evidence reports submitted/published/drained/shutdown counts and
durations, maximum depth, and the FIFO reorder occupancy (zero by
construction).

The first local public-gRPC smoke cell (32 interactive clients, one-second
warmup, five measured seconds) completed with zero errors at 33,189 ops/s,
aggregate p95 5.24 ms, and CreateComment p50/p99 4.72/19.92 ms. This is a
directional checkpoint only, not paired release evidence. The paired N1/E2
safe-application comparisons are recorded below.

## Paired cloud decision

Commit `a9c4cefb` was evaluated on the inventoried N1 and E2 hosts with
safe-application PostgreSQL in sequential exclusive phases. Each mixed cell
used two seconds of warmup and ten measured seconds; each dedicated cell used
the full TicketDesk dataset and one repetition. These are candidate-selection
receipts, not the required 90-second release matrix.

The ordinary public unary transport improved enough for N1 to pass both mixed
gates. E2 missed throughput by 1.3 percentage points while landing exactly at
the tail boundary.

| Host | Transport | Safe PG ops/s | RiffDB ops/s | Throughput ratio | Safe PG p95 | RiffDB p95 | p95 ratio | Seed ratio |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| N1 | unary | 8,261 | 7,763 | 0.940x | 15.20 ms | 18.87 ms | 1.241x | 3.75x |
| E2 | unary | 7,649 | 6,786 | 0.887x | 16.78 ms | 20.97 ms | 1.250x | 2.43x |

The unchanged ADR-0127 bounded session then composed with the completion lane.
It passed the mixed c32 throughput and p95 gates on both hosts, with zero
errors, conflicts, unavailable results, or idempotency mismatches.

| Host | Transport | Safe PG ops/s | RiffDB ops/s | Throughput ratio | Safe PG p95 | RiffDB p95 | p95 ratio | Seed ratio |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| N1 | bounded session | 8,263 | 8,686 | 1.051x | 15.20 ms | 18.87 ms | 1.241x | 4.47x |
| E2 | bounded session | 7,629 | 8,258 | 1.082x | 16.78 ms | 19.92 ms | 1.188x | 2.96x |

The completion owner remained bounded. The ordinary mixed cells observed
maximum submitted depths of four on N1 and six on E2; the bounded-session cells
observed depths of three and four. Reorder occupancy remained zero by FIFO
construction, every submitted unit was published, and the writer frontier
equivalence counters reported no failure.

The dedicated session-shaped unary matrix remains the rejecting gate. The
table below lists the mutation p50 ratios; the accepted ceiling is 1.10x on
both hosts.

| Scenario | N1 ratio | E2 ratio |
|---|---:|---:|
| CreateComment | 1.53x | 1.23x |
| CloseTicketWithComment | 1.37x | 1.02x |
| SwapMemberRoles | 1.38x | 1.14x |
| OpenTicketWithLabels | 1.50x | 1.17x |

Reads also do not satisfy the conjunctive unary ceiling. The best larger N1
pages are competitive (`BoardPage50` 1.02x, `BoardPage200` 0.93x), but small
N1 reads range from 1.28x to 2.16x and E2 reads range from 1.10x to 2.41x;
`BoardPage450` remains a separate measured result-assembly defect at 2.91x N1
and 3.25x E2. The completion lane is not the owner of those costs.

The decision is therefore:

1. retain the bounded completion owner and its semantic/recovery coverage;
2. leave physical journal scheduling unchanged;
3. keep unary as the generated-client default and do not amend `PERF-018`;
4. close WP-649 without alpha release activation; and
5. assign the remaining unary/read gates to the pre-fence service/read fast
   path, with result-set windowing owning `BoardPage450`.

No additional journal-fence or coordinator candidate is justified by this
evidence. The mixed tail mechanism is closed; the remaining failures are
low-concurrency orchestration, service preparation, query execution, and
large-result assembly.

### Receipt hashes

| Receipt | SHA-256 |
|---|---|
| N1 unary mixed | `e6998ce85cc939de86ea368934ffed5136e0ec78296f5cfc6c5be6a968d119f3` |
| E2 unary mixed | `c341915bc6c3064c5c8bb2e1ac8a65d14c615da389b0e213012f54625821e8fc` |
| N1 bounded-session mixed | `16e7471fe49a2aeb47166247775ed421609c43d28e52206459c088808e0f88fe` |
| E2 bounded-session mixed | `a88bad5c6df86d8338b84f72dafa32b57ab24a4df6115d5947cb2485aa82aeb2` |
| N1 unary dedicated | `50a26ccd5e5eaec36d2052b32b83c9d5382ffaaf42b46d800d4670afa8ecc203` |
| E2 unary dedicated | `bd18a03889249e016fcc95ad5fbccef632db0072819a628c8280922340f1cd5b` |
| N1 session dedicated | `a57db669a0469013d89839909c6c1e6c8c143c973465ec73d564523c2fd56be1` |
| E2 session dedicated | `c83f2f42e62339f0afcec171138eea3485e41f4d83ebd146f2873e1a18ec7cd4` |
