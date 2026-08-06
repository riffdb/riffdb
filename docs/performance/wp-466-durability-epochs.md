# WP-466: Redb physical commit amplification

WP-466 targets physical work per durable redb fence. Its first production
change is a completion-edge collection window: after a busy writer leaves
2--32 commands queued, the coordinator may spend at most 2 milliseconds
collecting the next FIFO prefix and commit it as one larger Immediate
transaction. Idle singletons, prefixes above 32 commands, and barriers never
pay this delay.

The retained c32 evidence contains one physical commit per coordinator
dispatch, with every dispatch ending because the queue was drained. The
completion-edge window allows clients released by the preceding commit to join
the already-queued cohort before the next transaction starts. Only workloads
that still require multiple physical subgroups after this coalescing are
candidates for unpublished durability epochs.

The evidence harness reports staging order, deferred-tail mechanics, staging
time, commit time, database growth, and per-table tree inventory. redb 4.1 does
not expose page-size selection in its production API, so RiffDB does not fork
the engine for that experiment. Table-major staging measured within 0.4% of
command-major staging and is not retained as a production optimization.

RiffDB never trades correctness for throughput. Every command keeps its own
authorization, idempotency identity, sequence, outcome, audit, provenance,
events, retry classification, and uncertainty recovery. A commit error still
fences writes, persistent encodings stay compatible, and the hardened profile
retains Immediate two-phase commits.

## Retained implementation

The coordinator uses its existing dedicated current-thread runtime for the
completion-edge deadline. It parks on either the next intake message or the
fixed two-millisecond deadline. This differs deliberately from the
sub-millisecond fresh-formation path: that 200-microsecond path remains a
timer-free bounded poll because Tokio's millisecond timer resolution would add
an unintended unary delay. The two-millisecond timer is armed one timer-wheel
tick early so rounding cannot intentionally extend the logical budget. The
completion-edge path is entered only after a busy commit and at least two
queued commands, so idle unary writes get no timer.

The first implementation repeatedly yielded until the deadline. It formed
larger groups, but consumed a coordinator CPU while waiting and failed the
throughput retain gate. It was replaced by the event-or-deadline park before
acceptance.

## Evidence

The command-growth smoke harness measured the complete 32-command synthetic
graph as follows on the recorded ext4/NVMe device:

| Candidate | Elapsed |
| --- | ---: |
| Immediate one-phase, command-major, groups of 16 | 21.55 ms |
| Immediate one-phase, table-major, groups of 16 | 21.31 ms |
| Non-durable groups of 16 plus one durable tail | 7.28 ms |

Table-major staging was only 1.1% faster in this smoke run (and 0.4% in the
earlier retained sample), so it was rejected as noise relative to its added
implementation surface. The deferred-tail result is 33.8% of ordinary
Immediate time and justifies retaining the epoch design, but epochs remain a
follow-up because the application workload still produces one physical group
per dispatch.

A paired local c32 A/B compared a zero-window control with the final guarded
window on the same device and 15-second closed-loop workload; both runs passed
the same correctness checks:

| Metric | Window disabled | Retained parked window | Change |
| --- | ---: | ---: | ---: |
| Throughput | 13,742 ops/s | 14,265 ops/s | +3.8% |
| `create_comment` p50 | 14.68 ms | 13.11 ms | -10.7% |
| Physical commits | 2,338 | 1,851 | -20.8% |
| Commands/physical commit | 15.98 | 20.89 | +30.8% |
| Process writes/committed command | 34,919 B | 31,330 B | -10.3% |
| Durable growth/committed command | 7,589 B | 7,304 B | -3.8% |

Focused guardrails remained clean: c1 produced only singleton groups (1,182
ops/s, 4.98 ms write p50 on that device run). At c128, already-amortized
prefixes retained their direct path; the run reached 27,275 ops/s with 63.91
commands per physical commit. All focused runs reported zero errors, conflicts,
idempotency mismatches, unavailable responses, or overloads. These short runs
are retain-or-revert engineering evidence, not published comparative claims.

No durable key, value, record, public protocol, acknowledgement boundary, or
read-visibility rule changed. Deferred durability epochs are not enabled by
WP-466's production change.
