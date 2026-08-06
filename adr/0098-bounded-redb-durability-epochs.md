# ADR-0098: Bounded Redb Durability Epochs

- **Status:** Accepted
- **Date:** 2026-08-06
- **Decision owners:** RiffDB maintainers
- **Related requirements:** `PERF-004`, `PERF-005`, `PERF-007`, `PERF-008`,
  `PERF-009`, `PERF-010`, `PERF-015`, `REC-002`, `TXN-040`, `TXN-041`,
  `TXN-042`, `TXN-043`, `TXN-044`
- **Related work packages:** `WP-466`, `WP-467`
- **Amends:** ADR-0058, ADR-0060, ADR-0061, ADR-0095, ADR-0096, ADR-0097

## Context

The retained tri-backend interactive run `tri-20260806T133128Z` shows that
RiffDB's remaining mid-concurrency deficit is durable-commit frequency. At 32
clients, 4,387 physical redb commits consumed about 26.7 seconds of the
35-second process window. At 128 clients, larger groups reduced that to about
2,172 commits and 19.2 seconds, and RiffDB closed materially more of the
PostgreSQL gap. Request-cache and immutable-artifact candidates did not remove
the deficit.

redb 4.1 defines `Durability::None` commits as visible to later transactions
but not persistent unless followed by `Durability::Immediate`. That mechanism
can amortize one durable flush across separately evaluated FIFO subgroups, but
ADR-0061 correctly rejected using it without an exact visibility boundary,
tail fence, independent uncertainty handling, and crash proof.

## Decision

### Closed standard-profile durability epochs

The coordinator MUST first prefer extending one ordinary atomic command group.
After a busy writer completion edge leaves at least two commands queued, it MAY
collect additional FIFO commands for at most 2 milliseconds and stage the
complete compatible or transaction-local serial prefix in one Immediate redb
transaction. The window closes over an initial prefix of 2--32 commands. Idle
singletons and already-amortized prefixes above 32 commands retain the direct
path and never pay this window.

The dedicated coordinator runtime MAY enable Tokio time and park on the next
intake message or this real two-millisecond deadline. This does not amend
ADR-0060's sub-millisecond rule: fresh 200-microsecond formation remains a
timer-free poll and MUST NOT be rounded onto Tokio's millisecond timer wheel.
The completion-edge timer MUST be armed early enough that timer-wheel rounding
does not intentionally extend the logical two-millisecond budget.

Only work that still requires at least two physical transactions after that
coalescing MAY use one bounded durability epoch.
An epoch contains one or more ordinary FIFO command subgroups. Each subgroup
retains the existing compatible or transaction-local serial rules and commits
one complete atomic command graph through redb `Durability::None`. A final
redb `Durability::Immediate` fence transaction makes the complete prefix
durable before any result or effect is released.

An epoch is bounded by all of:

- at most 256 commands in total;
- at most 16 MiB of conservatively reserved command graphs in total;
- at most 2 milliseconds from the oldest selected command to fence start;
- the existing per-subgroup compatibility, conflict, validation, and retry
  rules; and
- every catalog, capability, administration, readiness, shutdown, and fencing
  barrier.

The epoch deadline is never extended or restarted. A singleton command uses
the existing direct Immediate path and incurs no epoch wait. The public
transport batch remains capped at 16 and the coordinator admission queue
remains independently bounded at 512.

The hardened profile and synchronous recovery oracle do not use deferred
commits. They retain one Immediate two-phase commit for each existing physical
group.

### Last-durable read frontier

One activated database owns a process-local immutable redb read transaction at
the last known durable frontier. Every operational application read, command
preparation read, outbox/projection/consumer read, and subscription hydration
MUST begin from that frontier rather than redb's newest visible root.

Deferred subgroup commits do not publish a replacement frontier. After the
Immediate tail succeeds, storage opens the successor read transaction and
atomically publishes it before commit outcomes, transient indexes,
notifications, projections, subscribers, or responses are released. A reader
that raced publication sees either the predecessor durable snapshot or the
successor durable snapshot, never an unfenced subgroup.

Writer-private transactions may observe prior deferred subgroups in the same
epoch so FIFO retries and transaction-current validation remain serially
correct. That private visibility grants no application or derived read access.

### Response, effect, and transient publication

Deferred subgroup success is an internal applied result, not a committed
application outcome. The writer retains every completion and first-commit
notification until the tail fence succeeds. Transient outbox indexes and any
other process-local accelerator are accumulated per subgroup and applied only
after the durable frontier is published.

On a proven tail success, each command is released with its existing
independent identity, sequence, outcome, audit, provenance, event, outbox, and
idempotency semantics. The epoch is not a public transaction and one caller
cannot make sibling commands part of its business atomic unit.

### Failure and recovery

A failure before any deferred subgroup commit follows the existing absent
path. A failure after a deferred subgroup but before a known-successful tail
fence releases no result and fences authoritative writes. Recovery must observe
either the previous durable frontier or the complete fenced epoch; it validates
and resolves every selected command by exact identity and MUST NOT infer one
command from another.

The last-durable read frontier remains unchanged on any tail error. Unknown
tail status uses the existing uncertainty classification, keeps responses
withheld, and requires complete bounded identity resolution before admission
can resume. A partial durable epoch is an integrity failure.

Process-crash coverage MUST kill the server before the tail, during the tail,
and after the tail but before response release. It must prove absent-or-complete
recovery, no unfenced read visibility, stable sequence order, and idempotent
retry outcomes.

## Consequences

- Once the deferred-epoch follow-up is implemented, burst and import commands
  can share one fsync when dependency or conflict rules produce multiple
  already-accepted redb transactions.
- That follow-up keeps reads concurrent by using redb MVCC at the last durable
  frontier rather than blocking behind the writer epoch.
- Standard-profile crash semantics and acknowledgement durability are
  unchanged, but storage and coordinator typestate gain an unpublished applied
  state.
- The 2-millisecond epoch deadline is a new bounded contention-only latency
  policy. Direct unary work retains the old path.
- No public protocol, command language, durable record encoding, or storage
  engine is replaced.
- The retained c32 closed-loop workload forms one physical subgroup per
  dispatch. Its first lever is therefore completion-edge coalescing into a
  larger single transaction, not unpublished roots.

## Rejected alternatives

- **Expose redb's newest root to readers.** Deferred state could be observed and
  later roll back.
- **Acknowledge each deferred subgroup.** This is weaker durability and remains
  prohibited.
- **Block all readers for the whole epoch.** It preserves correctness but
  destroys the concurrent read behavior the application workload requires.
- **Use epochs in the hardened profile.** The hardened oracle remains the
  simpler independently durable comparison path.
- **Make bounds operator-configurable.** Count, bytes, and time remain compiled
  safety invariants.
- **Add a separate WAL before proving redb epochs.** redb already provides the
  required ordered non-durable prefix and durable tail mechanism.

## Post-acceptance evidence

The retained `tri-20260806T133128Z` server evidence was re-examined before
implementation. At 32 clients, each of 4,360--4,414 coordinator dispatches
became exactly one physical commit, every dispatch ended `queue_drained`, and
the mean group contained about 16 commands. At 128 clients the same one-to-one
relationship held with groups of about 64 commands. The workload's alternating
client cohorts can supply more commands after the prior cohort receives its
completion. WP-466 therefore tests the single-transaction completion-edge
window before introducing unpublished roots.

The selected event-or-deadline implementation reduced the focused c32
physical-commit count from 2,338 to 1,851, increased the mean commands per
physical commit from 15.98 to 20.89, improved throughput from 13,742 to 14,265
ops/s, and reduced `create_comment` p50 from 14.68 to 13.11 milliseconds in a
paired local A/B. A cooperative busy-yield implementation was measured and
rejected before acceptance. c1 retained singleton dispatch, and c128 retained
the direct path for already-amortized prefixes.

WP-466 enables only the single-transaction completion-edge window. Deferred
subgroup commits and the last-durable frontier remain unavailable until a
separate work package supplies their storage typestate and crash proof.

WP-467 supplies that storage typestate and crash proof. It introduces the
unpublished applied result, epoch-owned mutation lease, predecessor read
frontier, accumulated transient-index publication, and Immediate tail seal.
Coordinator epoch selection and response retention remain disabled until a
later package composes this mechanism through the ordinary writer lane.

## Acceptance reference

The maintainer explicitly approved the bounded redb durability-epoch design on
2026-08-06 after review of the tri-backend results and the proposed
last-durable MVCC visibility boundary, deferred response/publication rules,
Immediate tail fence, crash behavior, and retained hardened oracle.
