# WP-640 honest closure and current-HEAD seed ledger

WP-640 is closed without a release activation claim. The bounded preparation,
admission-order proof, private-frontier, and pay-once increments that produced
portable gains remain in production. The complete ADR-0129 candidate did not
meet its ordered-lane mechanics threshold, seed target, mixed-c32 target,
unary target, or complete recovery gate. This report does not reinterpret a
partial implementation as an exit-gate pass.

After this closure, the maintainer amended `PERF-008` in SPEC revision 1.02.
The 1.10x seed row below remains the historical ADR-0129/WP-640 activation
criterion and explains why the complete candidate was rejected; it is not the
current alpha seed gate. Current alpha evidence must instead receipt the full
seed on both inventoried profiles under the 5.0x same-run regression ceiling.
The measured 3.65x workstation and 4.46x N1 results pass that new ceiling, but
do not retroactively activate WP-640 or waive its ordered-lane, mixed-c32,
unary, and recovery failures. Seed parity remains deferred to ADR-0129 section
5's conflict-domain-parallel batch-apply direction.

The exact runtime revision measured here is `02dbce7e` (runtime-identical to
`84c93a18`; the later commit documents a rejected sibling candidate). Seed
receipts use the full 19,220-command TicketDesk corpus, standard durability,
128 generated-client workers, public gRPC, and three repetitions. Mixed-load
ratios use the already-recorded 15-second current-runtime diagnostic and the
latest stable frozen safe-application PostgreSQL receipt; they remain
diagnostic rather than PERF-018 release evidence.

## ADR-0129 mechanics and activation table

The preparation charge is the observed deterministic evaluation plus the
mixed validation/encoding/staging charge. These are worker CPU charges and may
overlap writer wall time. The ordered-lane row deliberately uses only final
apply plus journal submission: it is a lower bound because it excludes
admission-order finalization. A lower bound above the threshold is sufficient
to reject the gate without inventing an allocation for the remaining mixed
stage.

| ADR-0129 gate | Workstation current | N1 current | Threshold | Result |
|---|---:|---:|---:|---|
| deterministic evaluation + validation/encoding preparation | 47.79 us/command | 168.64 us/command | N1 at most 180 us; workstation no slower than its 69.54 us baseline | pass |
| ordered final apply + journal submit, excluding finalization | 28.26 us/command | 130.95 us/command | workstation at most 25 us; N1 at most 60 us | **fail** on both hosts |
| full seed / safe-app PostgreSQL | 3.65x | 4.46x | at most 1.10x | **fail** |
| mixed interactive c32 / safe-app PostgreSQL | 0.81x | 0.86x | at least 0.90x | **fail** |
| representative unary write / safe-app PostgreSQL | no new same-run receipt; package made no unary claim | latest frozen cloud range 1.26--1.71x | at most 1.10x; no more than 5% candidate regression | **not proven; latest evidence fails** |
| frontier equivalence | 1,099 checks, zero failures across workstation seed repetitions | 1,113 checks, zero failures across N1 seed repetitions | exact equality after every epoch | pass |
| fixed failure-mode telemetry | pool depth, reorder occupancy, proof mismatch, rollback, and equivalence evidence present | same | fixed cardinality and redacted | pass |
| correctness/durability/recovery family | targeted crate, architecture, Shuttle, and 86-case storage recovery tests green | public seed cells zero-error | complete process recovery matrix and no unresolved reliability finding | **fail**: process recovery readiness ordering remains red |

The c32 numerators are 31,292 operations/s on the workstation and 7,482 on N1.
Their stable safe-application PostgreSQL denominators are 38,573 and 8,672
operations/s. These are the current diagnostic ratios, not a substitute for
the missing same-run 90-second release matrix. The current seed denominators
remain the frozen 0.501-second workstation and 1.586-second N1 safe-app
receipts used by WP-639. No threshold or comparator shape changed.

## What survived and what did not

The following design elements survived cloud evidence and remain enabled:

- the authority-free `PreparedCommandBody` and the pay-once command-index,
  capsule, entity-postimage, and live-checkpoint mutation proofs;
- a fixed bounded command-evaluation/preparation pool, FIFO ordinal assembly,
  one bounded reorder surface, and backpressure before unbounded retention;
- writer-private snapshot capture and evaluation against the exact unpublished
  predecessor rather than an older public root;
- admission-order current-state checks, conflict ownership, sequence
  assignment, final authoritative apply, fence, publication, and uncertainty;
- frontier-equivalence checks at each committed group and fixed-cardinality
  depth, occupancy, mismatch, and rollback evidence; and
- byte-identical durable formats plus independent startup/recovery validation.

The independent storage command-segment preparation pool did **not** survive
cloud evidence. On hosts with at most eight logical CPUs its production worker
count is now zero, so N1 and E2 encode that portion inline. The bounded command
evaluation pool remains active at `available_parallelism - 1` (three workers
on the four-vCPU N1). This distinction matters: cloud retained the useful
parallel deterministic preparation but rejected a second pool that competed
for the same cores.

Two other experimental branches were removed rather than hidden in the final
number: a decoded columnar-consumer segment cache had no credible cloud win,
and shared overlay-lineage ownership measured 1.797 seconds against the 1.792-
second checkpoint baseline with unchanged final-apply and journal-submit cost.
Neither representation remains. The complete `CheckedPreparedCommandGroup`
vision is therefore not claimed as activated; the retained pieces are bounded
pay-once improvements whose individual safety and regression tests passed.

## Current-HEAD closed seed ledger

Writer busy plus writer idle closes to public seed wall within the same two-
percent standard as WP-639.

| wall component | Workstation | Share | N1 | Share |
|---|---:|---:|---:|---:|
| public seed wall | 1,829.14 ms | 100.0% | 7,074.07 ms | 100.0% |
| writer busy | 1,444.66 ms | 79.0% | 5,482.69 ms | 77.5% |
| writer idle, including every possible prefix-drain wait | 366.08 ms | 20.0% | 1,633.13 ms | 23.1% |
| busy + idle | 1,810.74 ms | 99.0% | 7,115.82 ms | 100.6% |
| closure error | -18.40 ms | -1.01% | +41.75 ms | +0.59% |

The disjoint public-wall ledger is busy, idle, and closure error. The following
stage counters decompose work inside or concurrent with those wall components;
they are not summed a second time:

| mean charge per command | Workstation | N1 | Relationship to wall |
|---|---:|---:|---|
| admission | 5.65 us | 17.42 us | writer work |
| compatibility/conflict ownership | 1.07 us | 4.14 us | writer work |
| deterministic evaluation | 8.27 us | 36.47 us | bounded preparation CPU; overlaps writer |
| validation/encoding/staging | 39.52 us | 132.17 us | mixed preparation/finalization CPU; overlaps writer |
| final authoritative apply | 12.72 us | 53.66 us | writer work |
| journal construction/submission | 15.55 us | 77.29 us | writer work |
| publication | 0.01 us | 0.10 us | writer work |
| **total writer busy** | **75.16 us** | **285.26 us** | wall-critical serialized lane |
| **writer idle** | **19.05 us** | **84.97 us** | feeding, fence drain, and cap-drain upper bound |
| physical flush I/O | 26.33 us | 26.04 us | asynchronous overlap; not added to wall |

Subtracting final apply and journal submission from writer busy leaves 901.45
ms on the workstation and 2,965.83 ms on N1 in admission, conflict,
finalization, preparation handoff, and writer-loop work. Physical I/O totals
506.14 and 500.43 ms respectively and runs concurrently with the writer. The
N1's remaining time is therefore predominantly CPU/ordered-lane service, not a
slow persistent-disk fence.

| frame/fence census | Workstation | N1 |
|---|---:|---:|
| mean command frames | 366.33 | 371.00 |
| mean physical flushes | 365.33 | 371.00 |
| mean frames per physical flush | 1.003 | 1.000 |
| maximum frames in one flush | 2 | 1 |
| mean commands per frame | 52.47 | 51.81 |
| mean complete frame bytes | 141.23 KiB | 139.47 KiB |
| mean physical I/O | 1.385 ms/flush | 1.349 ms/flush |
| maximum physical I/O | 11.384 ms | 15.070 ms |

## ADR-0104 amendment decision

No ADR-0104 amendment is drafted. The unpublished-prefix ceiling can block the
writer only while it drains submitted work, which is recorded inside writer
idle. Even the deliberately impossible attribution that assigns **all** writer
idle to the 256-transition ceiling gives it an upper bound of 20.0% of local
wall and 23.1% of N1 wall. Writer busy is 79.0% and 77.5% respectively. The
ceiling therefore cannot be the dominant share of the current seed on either
host.

The frame census reinforces the decision: the N1 issued one roughly 139-KiB
frame per physical flush, spent only 0.500 seconds in physical I/O, and spent
5.483 seconds writer-busy. Raising the unpublished prefix would permit more
in-flight frames but would not remove the dominant serialized construction,
finalization, apply, and submission CPU.

If later cap-specific instrumentation overturns this bound, a proposed
amendment must first retain an interactive guard derived from WP-620: command-
bearing interactive flush aggregation may not cross 256 KiB without separate
evidence, because N1 `fdatasync` rises from about 0.8--0.9 ms at 256 KiB to
3.1 ms at 1 MiB, 14.3 ms at 4 MiB, and 57.4 ms at 16 MiB. It must additionally
hold public c32 write p95 within the existing five-percent no-regression gate.
Those are evidence prerequisites, not a drafted or accepted amendment.

## Reliability ownership

The three disclosed reliability findings now have explicit packages:

| Finding | Owner | Required disposition |
|---|---|---|
| cold E2/N1 seed can stop with `AuditUnavailable` before checkpoint activation; exact baseline reproduced it | **WP-641**, service-audit/readiness lane | deterministic reproduction and fail-closed repair with no unaudited work |
| one zero-operation-error workstation run could not produce the mandatory final authoritative table inventory | **WP-642**, storage/app-baseline conformance lane | reproduce the epilogue boundary and keep an unprovable inventory as a failed cell |
| `recovery_full` sees shutdown evidence before its required ready line on current and exact baseline | **WP-643**, server/recovery process lane | repair explicit lifecycle ordering without weakening a crash or maintenance assertion |

## Receipts

| Host | Path | SHA-256 |
|---|---|---|
| workstation | `/home/kevin/tmp/wp640-current-head-seed-workstation.json` | `b0bce5910d9abf7dcdf35f15da45e0c4bb45ab9c12354ba027e960642e8da26f` |
| N1 | `/home/kevin/tmp/wp640-current-head-seed-n1.json` | `b1400f374401d2ffc9c2dfab3ec57c2eadb6e878b22edfee2de3a67ab21c2e1a` |

Requirement coverage: PERF-001, PERF-004, PERF-005, PERF-008, PERF-018.
