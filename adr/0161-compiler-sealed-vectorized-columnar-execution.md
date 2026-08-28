# ADR-0161: Compiler-Sealed Vectorized Columnar Execution and Bounded Parallel Scans

- **Status:** Proposed
- **Direction approved:** No
- **Exact text accepted:** No
- **Decision deadline:** Before WP-712 adds a batch executor, selection vector,
  late-materialization phase, or parallel segment scheduler
- **Requires:** ADR-0051, ADR-0053, ADR-0070, ADR-0071, ADR-0086, ADR-0087,
  ADR-0111, ADR-0129, ADR-0130, ADR-0152, ADR-0160
- **Defines or blocks:** WP-712 and WP-713

This record is planning input only until its exact text is accepted.

## Context

ADR-0160 gives the derived columnar plane immutable typed lanes and exact
segment-pruning evidence. The current executor remains row-at-a-time: it merges
rows, reconstructs canonical cell values, evaluates predicates per row,
materializes candidates, then sorts or folds them. Merely changing file layout
would leave much of the CPU and allocation cost in that execution shape.

Batch-at-a-time evaluation can amortize type dispatch, apply predicates to
contiguous lanes, delay unrelated-column decode, and merge exact aggregate
states efficiently. Parallel segment scans can use available cores on bounded
analytical work. Both mechanisms are safety-sensitive. A different task order
must not change exact arithmetic, total ordering, row-policy behavior, cursor
identity, freshness, error precedence, or resource admission. Parallelism must
not become an application knob or an unbounded task fan-out.

## Proposed Decision

### 1. Compile one closed column-use and execution-phase program

For each columnar plan, the compiler/provider descriptor seals the fields and
operations needed for these ordered phases:

1. segment eligibility and exact zone-map pruning;
2. policy evidence and row-policy evaluation;
3. query predicates;
4. grouping, aggregate input, or total-order keys;
5. bounded top-K/window selection when requested; and
6. final selected output materialization.

The program contains stable field IDs, exact types, predicate and aggregate
identities, comparison/order semantics, missing/null behavior, fixed work and
byte charges, and a compiler maximum batch width. It is immutable plan data,
not request structure. Callers cannot submit a field, expression, operator,
batch width, execution phase, physical lane, or fallback.

Fields required by policy are read before a row can contribute to predicates,
order, aggregation, counts, or output. Fields needed only for selected output
are decoded after the final selection. Late materialization therefore narrows
work but never postpones authorization.

### 2. Execute over bounded selection vectors

The V2 executor processes a segment in fixed-cardinality batches. One batch
contains borrowed or bounded decoded lane slices, validity bitmaps, and a
selection vector whose length never exceeds the plan maximum. Initial selection
contains the segment's rows in primary-key order. Policy and predicate stages
may only clear positions; they cannot introduce, duplicate, or reorder rows.

The initial closed batch-width set is `64`, `128`, `256`, `512`, and `1024`.
The provider chooses one width at activation from the compiler maximum and
checked worst-case row/intermediate bytes. The chosen width is recorded in the
provider generation proof and remains fixed for that generation. It is not
selected per request or from observed values.

Each operator has an independent scalar reference implementation. Vectorized
results must be bit-for-bit or value-for-value identical to that reference for
every admitted type, optional state, direction, overflow, and boundary. No
floating point or CPU-dependent arithmetic enters exact decimal, money, count,
mean, min/max, or Boolean folds.

### 3. Preserve deterministic aggregate and ordering semantics

Exact aggregate partial states use ADR-0152's checked merge laws. Parallel or
batched execution merges them in canonical segment-ID then batch-ordinal order.
An overflow, distinct-state bound, group bound, or invalid input returns the
same closed failure independent of worker schedule.

For a compiler-declared bounded top-N query, the executor may maintain a heap
bounded by the immutable maximum result cardinality plus continuation probe.
Comparison uses the complete declared total order and unique entity-key
tie-breaker. Final output is sorted exactly once by that order. A query without
an admitted bounded top-N algorithm may use only its existing bounded candidate
sort; it cannot silently materialize an unbounded population.

Cursors bind the same logical result set, provider epoch, snapshot, order, and
position as before. Physical batch width, segment task assignment, pruning
count, and lane encoding never enter public cursor semantics.

### 4. Parallelize segments under one finite scheduler

One process-global columnar scan scheduler owns a fixed worker ceiling, bounded
ready queue, bounded per-query task count, and per-query cancellation token. A
query may enqueue at most the compiler/provider segment ceiling and receives a
fair-share admission bounded independently of tenant population. There is no
thread or task per row, group, dictionary value, or result.

The production worker maximum is the lesser of a reviewed static ceiling and
available parallelism. The scheduler may reduce active workers under service
pressure, but cannot exceed that ceiling, change query semantics, admit more
work, or suppress a typed saturation result. Query deadlines and cancellation
are checked between batches and before result publication.

Parallel workers receive immutable validated segments, a nonserializable
compiled program, an exact policy-evidence handle, and bounded output slots.
They own no authoritative storage, capability mutation, projection frontier,
publication, or fallback authority. The coordinator of one query validates one
provider epoch, waits for its finite task set, canonically merges results, runs
the existing pre-release authorization safe point, and only then releases the
response.

### 5. Keep freshness and publication atomic

One query captures one published columnar snapshot and one provider epoch
before scheduling. Every worker reads only that immutable snapshot. Compaction
or a newer projection publication may proceed concurrently but cannot replace
the query's retained segment handles. The response reports the captured
frontier; it never combines batches from different frontiers or generations.

Causal and bounded freshness waits remain outside scan execution and retain
post-wake authorization. Revocation during execution is caught by the existing
pre-release safe point. Cancellation releases tasks, buffers, snapshot handles,
and provider proofs within a fixed deadline.

### 6. Constrain policy-sensitive physical optimization

Row policy is evaluated before any selected row contributes to result-shaping
state. For policy shapes where physical pruning, task count, early top-K stop,
or aggregate work could reveal a protected distribution, the compiler/provider
must choose one of:

- policy-aligned segment/provider state;
- a fixed admitted work class independent of protected membership; or
- typed unavailability for that optimized plan.

It may not run a wider shared optimization and redact afterward. Public
telemetry has fixed stage and work-class labels only; it carries no segment,
field, predicate, tenant-cardinality, skip-count, or policy-result dimension.

### 7. Retain a differential scalar oracle and fail closed

The scalar reference evaluator remains test/runtime-diagnostic code until V2
activation is complete. Production does not silently retry through V1 or a
scalar scan after a V2 integrity, policy, bound, or lifecycle failure. A plan
may name an existing compiler-proved equivalent fallback under ADR-0086, but
the fallback decision is immutable plan policy and produces the existing typed
lifecycle evidence.

The implementation activates in two gates:

- WP-712 proves single-threaded V2 batch execution and late materialization;
- WP-713 adds bounded parallel scheduling and public projected-query use.

WP-712 must reduce CPU per examined row by at least 35 percent on the registered
full-scan aggregate corpus or improve its complete query throughput by at least
1.5 times, with no more than five percent regression on selective queries after
zone-map pruning. WP-713 must improve the registered 100k-row parallel scan by
at least 1.5 times from one to four admitted workers while keeping p95 no more
than 1.10 times the single-worker p95 under simultaneous interactive load.
Failure leaves the prior production executor selected and retains value-free
mechanics evidence.

## Options Considered

1. **SIMD-specialize the row evaluator:** rejected as the first step because
   row objects and type dispatch remain the dominant representation boundary.
2. **Spawn one task per segment without a shared scheduler:** rejected because
   a query could monopolize runtime and memory by segment count.
3. **Vectorize typed batches and schedule finite segment tasks:** proposed
   because work remains compiler-bounded and deterministic while amortizing
   dispatch and using available cores.
4. **Return partial results when cancellation or one task fails:** rejected;
   ordinary RiffDB query outcomes remain atomic.

## Consequences

- Full and partially selective columnar scans consume less CPU and allocate far
  fewer row objects.
- Output-only fields are decoded only for authorized selected rows.
- Parallel analytical work gains an explicit admission and fairness surface.
- The engine must maintain scalar/vector differential tests and deterministic
  merge rules.
- CPU-specific SIMD, GPU execution, distributed scans, runtime plan changes,
  and caller-selected parallelism remain deferred.

## Compatibility

This decision changes no RiffQL, query IR, module, plan, cursor, public wire,
generated result, or authoritative format. Batch width and scheduling are
private provider-generation facts. If a compiler/provider descriptor needs new
physical capability metadata, it uses a least-sufficient successor registered
under ADR-0124 while preserving old artifacts and semantics.

## Security

Workers operate only on immutable authorized projection state and cannot access
authoritative storage or widen policy. Policy fields are evaluated before
result shaping, and inference-sensitive plans require policy-aligned state or a
fixed work class. Cancellation, saturation, internal failures, and diagnostics
release no partial protected rows or data-dependent labels.

## Standing Design Tests

- **Interface safety (AGENTS.md boundary 11):** applications still select one
  finite compiled operation and typed values. They cannot select vectorization,
  width, workers, pruning, policy order, fallback, partial results, freshness,
  or budgets. Every failure remains typed and fail closed.
- **Scale:** batches, selection vectors, partial states, top-K heaps, segment
  tasks, workers, queues, retained snapshots, result buffers, waits, and
  diagnostics are bounded before execution. Work is partition/segment scoped
  and assumes neither database-wide memory nor co-located authoritative state.

## Testing

- Scalar/vector differential properties for every predicate, type, optional
  state, aggregate, group, order, cursor, and overflow boundary.
- Late-materialization spies proving output-only fields are untouched for
  rejected rows while policy fields are always evaluated first.
- Deterministic-schedule tests permuting segment and batch completion order and
  asserting identical rows, ordering, exact states, failures, and digests.
- Cancellation, saturation, slow-worker, panic containment, revocation,
  compaction, publication, rebuild, and shutdown matrices.
- Policy/inference tests proving hidden rows cannot affect released totals,
  top-K, work classes, diagnostics, or partial output.
- Architecture checks for one scheduler, no per-row task, no worker authority,
  no public tuning input, and no unapproved scalar fallback.
- Registered single/four-worker CPU, throughput, p50/p95/p99, allocation,
  examined-row, skipped-segment, and projection-lag receipts.

## Requirements and Work Packages

- **Requirements:** `PRJ-001` through `PRJ-004`, `QRY-001` through `QRY-005`,
  `OQ-017` through `OQ-024`, `OQ-044` through `OQ-055`, `PERF-001`,
  `PERF-007`, `PERF-008`, `PERF-018`
- **Vectorized executor:** `WP-712`
- **Parallel scheduling and activation:** `WP-713`
- **Final evidence:** `WP-715`

## Decision Deadline

Exact human acceptance is required before WP-712 adds a production batch
program, selection-vector contract, batch-width identity, late-materialization
path, or scan scheduler. Any change to row-policy ordering, freshness,
authorization safe points, public results, or cursor semantics returns for
separate review.
