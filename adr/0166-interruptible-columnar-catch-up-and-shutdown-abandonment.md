# ADR-0166: Interruptible Columnar Catch-Up and Graceful-Stop Abandonment

- **Status:** Proposed
- **Direction approved:** No
- **Exact text accepted:** No
- **Decision deadline:** Before any package bounds, interrupts, or abandons
  columnar catch-up at a graceful stop
- **Requires:** ADR-0007, ADR-0010, ADR-0049, ADR-0085, ADR-0086, ADR-0156
- **Amends if accepted:** ADR-0049's derived-worker stop sequence, by fixing the
  unit of work a graceful stop waits for. It changes no authority, freshness
  class, policy, or rebuildability semantics in ADR-0086.
- **Defines or blocks:** the columnar-worker bounding package

This record is planning input only until its exact text is accepted. The number
Numbers 0163 and 0164 are reserved by compiler-sealed command decisions and
admission-head fenced query consistency, drafted concurrently on another
branch; 0165 is the accepted durable command-derived locator tables record.
0166 is this record's number.

## Context

`RunningColumnarWorker` runs one thread that polls every 25 ms and calls
`run_columnar_pass`. The pass calls `ColumnarEngine::apply_available`, whose
contract is "Pulls and applies **all** commits currently available from
`reader`": it loops over bounded 64-commit pages until the scan reaches its
frozen `ExactEnd` fence. One pass is therefore **unbounded in duration**.

`RunningColumnarWorker::shutdown` sets a stop flag and joins the thread. The
flag is only observed between passes — `stop_requested` at the top of the loop
and `wait_for_stop` at the bottom. A graceful stop consequently waits for the
*remaining duration of whatever pass is in flight*, which is a sample from an
unbounded distribution rather than a bounded drain.

Measured on the app-baseline production seed at 20 organizations (115,690
commands) and 64 organizations (359,318 commands):

| | 20 orgs | 64 orgs |
|---|---|---|
| commits applied | 114,584 | 351,956 |
| seed wall clock | 13.58 s | 41.03 s |
| worker apply time | 34.61 s | 118.49 s |
| worker checkpoint time | 0.25 s | 0.57 s |
| final pass duration | 23.64 s | 80.14 s |
| shutdown `columnar_worker` stage | 23.14 s | 79.22 s |

The shutdown stage equals the final pass duration at both sizes. The write lane
produces commits at roughly 8,600/s while the worker applies at roughly
3,300/s under concurrent write load, so the backlog grows monotonically during
any sustained ingest and the stop pays whatever is outstanding. Two stops at
identical data size previously measured 36.63 s and 0.137 s — a 267x spread
that is explained entirely by how far into an unbounded pass the stop request
arrived.

No accepted record requires a complete columnar drain at stop. ADR-0007 states
that graceful shutdown "stops admission, drains or cancels bounded non-durable
work according to its owner, closes the one graph, and writes no readiness or
clean-shutdown proof." ADR-0049 states that shutdown "stops derived workers."
ADR-0156 §3 enumerates what must be drained before the clean-close certificate —
"all application, administration, outbox, projection-control, migration, and
maintenance writers" — and columnar catch-up is not among them; it further
requires that "Shutdown must not scan all entities, indexes, or retained history
merely to produce the certificate."

What the current implementation does is therefore not a decision that was taken.
It is an accident of the stop flag being checked only at pass boundaries, and it
is the dominant term in a 12.3-minute graceful stop of a 1.1M-command database.

### The head-probe fix does not remove this, and the size dependence is the point

A separate behaviour-preserving fix removed a 500-row journal rescan that the
worker performed once per 25 ms poll, cutting per-commit apply cost about
fourfold. That is enough for the worker to outrun the write lane at 115,690
commands, where the columnar shutdown stage fell to single-digit milliseconds in
three of three daemons.

It is **not** enough at 359,318 commands. Three daemons in one post-fix run,
same binary, measured per-commit apply of 82, 250, and 380 µs and columnar
shutdown stages of **0.188 ms, 4.57 s, and 55.79 s**. The two slow daemons had
applied only 274,564 and 181,350 of ~358,500 commits when stopped: the worker
was still behind, and the unbounded pass still charged the stop for the backlog.
Why apply degrades from 82 to 380 µs per commit between daemons in one run is
not yet isolated.

The conclusion this record rests on is therefore not "the worker is slow". It is
that **a graceful stop's cost must not be a function of how far behind a derived
worker happens to be**, whatever its speed. A constant-factor speedup moves the
size at which the unbounded pass becomes visible; it does not bound it.

## Proposed Decision

### 1. Columnar catch-up becomes cooperatively interruptible

`apply_available` gains a caller-supplied continuation signal checked **only at
a commit-page boundary**, never inside a commit. A pass that observes a stop
request stops requesting further pages, publishes the exact fully applied prefix
it already holds, and returns normally with `caught_up: false`.

A commit page boundary is already a legal publication point: the existing
catch-up publishes the accumulated prefix at the end of a call and, under
supersession holdback, mid-call before mutating working state with a held
commit. Interruption adds no new publication shape and cannot split a commit.

### 2. A graceful stop attempts one final checkpoint, then abandons the rest

After the interrupted pass returns, the worker attempts exactly one
`checkpoint()` and then exits, whether or not it succeeds. `HoldbackActive`
is accepted as a normal outcome and the delta beyond the last durable frontier
is discarded.

The stop therefore costs one commit page plus one checkpoint, not the backlog.

### 3. Abandoned columnar catch-up is a recoverable state, not a loss

Undrained columnar ingest at a graceful stop is abandoned. This is admissible
because the columnar plane holds nothing authoritative:

- ADR-0086 §1 classifies projected state as "derived and non-authoritative",
  entity-row projections as snapshot-rebuildable, and states that "Segments are
  excluded from backup identity."
- `SPEC.md` `PRJ-004` requires that "Projection state MUST be rebuildable from
  the authoritative commit log."
- ADR-0086 §4 already fixes the recovery semantics this decision relies on:
  "after restart the projection may regress to it and replay. Crash invariants:
  the durable frontier never overclaims recoverable state; applying a commit
  twice is idempotent; a crash before checkpoint advancement causes replay; a
  crash after it cannot lose the associated projection changes."
- For entity-row projections the worker writes no control row into the
  authoritative database; the plane's entire durable footprint is the
  projections directory.

An interrupted stop therefore leaves the database in a state the accepted crash
invariants already cover, and produces no state a restart cannot reconstruct.

### 4. The cost is relocated, not removed, and that is the decision being taken

This decision does **not** make the catch-up work disappear. It moves it from
the stop to the next start. After a graceful stop under backlog, the next
process must apply the abandoned range before the projection is `Ready`, and
projected queries return the already-specified typed outcomes
`ProjectionBuilding` and `ProjectionLagging` (ADR-0086 §7) until it is.

A deployment that serves projected reads therefore trades bounded stop latency
for post-restart projection staleness. A deployment that does not serve
projected reads pays nothing. Because the trade is operator-visible, it requires
explicit acceptance even though no accepted record promises the current
behaviour.

This decision does not reduce durable authoritative work, does not weaken any
freshness class, and must not be cited to improve a benchmark number: the
authoritative commit path, the journal, and the clean-close ceremony are
untouched.

### 5. Lag at the stop is reported

The stop records the abandoned sequence distance so an operator can see what the
next start owes, rather than discovering it as unexplained post-restart
staleness.

## Options Considered

1. **Leave the pass unbounded (status quo):** rejected. Stop latency is
   unbounded and 267x variable at fixed data size, and no accepted record asks
   for it.
2. **Interrupt without a final checkpoint:** rejected as the primary shape. It
   is simpler and still crash-equivalent, but it discards durable progress the
   pass had already earned and makes every stop maximally expensive for the next
   start.
3. **Force a complete drain and a final checkpoint at stop, and declare the
   latency correct:** rejected. It makes stop latency a function of ingest
   backlog with no bound, and ADR-0156 already refuses to move bulk work into
   shutdown.
4. **Block the write lane until the projection keeps up (ingest backpressure):**
   not chosen here. It would bound the backlog at its source, but it makes
   authoritative write throughput a function of derived-plane speed, which
   ADR-0086 §9's "never permitted to starve the apply consumer" addresses from
   the opposite direction only. It belongs to the replay-budget conformance work
   below, not to a shutdown decision.
5. **Interrupt at a page boundary and checkpoint once:** proposed. Bounded stop,
   no lost durable progress, no new publication shape, crash-equivalent worst
   case.

## Consequences

- Graceful stop cost for the columnar stage becomes one commit page plus one
  checkpoint instead of the outstanding backlog.
- Post-restart projection lag after a graceful stop under backlog can be larger
  than it is today; typed lifecycle outcomes already express it.
- Total work across stop and start is unchanged or marginally increased.
- `ColumnarEngine::apply_available` gains a continuation parameter, so every
  caller including rebuild is affected.
- Does not address why the worker cannot keep up in the first place; that is a
  separate performance matter and this ADR makes no claim about it.

## Compatibility

No public API, wire protocol, RiffQL surface, query IR, durable authoritative
format, backup identity, or clean-close certificate changes. The columnar
segment and manifest formats are untouched, so this is independent of ADR-0160's
V2 layout proposal and composes with it either way. `apply_available`'s
signature is internal to the workspace.

## Security

No trust boundary, authorization, redaction, or policy surface changes.
Abandoned catch-up cannot expose a partial commit: publication remains atomic at
a page boundary and the durable frontier still never overclaims. Reported lag is
a sequence distance and carries no application data.

## Standing Design Tests

- **Interface safety (AGENTS.md boundary 11):** no. The continuation signal is
  internal to the server crate; no application developer or agent surface gains
  a way to request an unbounded drain, suppress catch-up, opt out of the
  frontier guarantee, or observe a partially applied commit. Freshness policies
  remain the only client-visible control and their semantics are unchanged.
- **Scale:** this decision removes a full-backlog rewind from the stop path and
  assumes nothing about co-located storage or single-node memory. It does not
  foreclose the billion-row tier. It also does not advance it: the per-commit
  apply cost and the whole-organization checkpoint rewrite both remain, and both
  are named as separate deliberate constraints below.

## Testing

- A catch-up interrupted at a page boundary publishes exactly the applied
  prefix, never a partial commit, and reports `caught_up: false`.
- An interrupted catch-up under supersession holdback never publishes a held
  commit and never advances the durable frontier past the published frontier.
- Stop during an interrupted pass under holdback discards the delta without
  overclaiming: reopening reports the prior durable frontier.
- Reopen after an interrupted stop replays the abandoned range and reaches a
  frontier and query result identical to an uninterrupted run at the same head.
- Crash injection at the interruption boundary yields no overclaiming durable
  frontier.
- A shutdown stage receipt bounds the columnar stage under a synthetic backlog.

## Requirements and Work Packages

- **Requirements:** `PRJ-004`, `REC-*` restart behaviour, `PERF-008`
- **Defines or blocks:** the columnar-worker bounding package
- **Final evidence:** shutdown stage receipts and columnar worker receipts at
  two bracketing dataset sizes

## Decision Deadline

Exact acceptance is required before any package changes what a graceful stop
delivers for the columnar plane. It is not required for the separable internal
performance fixes to the head probe and the idle checkpoint, which preserve
current observable behaviour exactly.
