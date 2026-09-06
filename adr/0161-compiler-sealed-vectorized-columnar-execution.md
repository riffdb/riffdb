---
adr: "0161"
title: Compiler-Sealed Vectorized Columnar Execution and Bounded Parallel Scans
status: proposed
tier: guarantee
date: 2026-09-05
accepted: null
requires: [ADR-0051, ADR-0053, ADR-0070, ADR-0071, ADR-0086, ADR-0087,
  ADR-0111, ADR-0129, ADR-0130, ADR-0152, ADR-0160, ADR-0183, ADR-0187,
  ADR-0190, ADR-0192, ADR-0195, ADR-0196, ADR-0200]
amends: []
supersedes: []
requirements: [PRJ-001, PRJ-002, PRJ-003, PRJ-004, QRY-001, QRY-002,
  QRY-003, QRY-004, QRY-005, OQ-017, OQ-019, OQ-020, OQ-021, OQ-022,
  OQ-024, OQ-044, OQ-045, OQ-046, OQ-047, OQ-048, OQ-049, OQ-050,
  OQ-051, OQ-052, OQ-055, PERF-001, PERF-007, PERF-008, PERF-018]
packages: [WP-712, WP-713, WP-715]
obligations:
  - id: OBL-0161-1
    package: WP-712
    proof: vectorized_columnar_batches_match_independent_scalar_oracle
    says: Every admitted V2 batch result and closed failure equals an independent scalar evaluation under the same logical order, bounds, and optional-value rules.
  - id: OBL-0161-2
    package: WP-712
    proof: columnar_policy_precedes_selection_and_output_lane_materialization
    says: Policy admission precedes every contribution to filtering or result shape, and rejected rows never decode output-only lanes.
  - id: OBL-0161-3
    package: WP-712
    proof: columnar_batch_resources_and_failure_precedence_are_bounded
    says: Widths, lane views, validity maps, selection vectors, partials, heaps, output, and deterministic failure precedence remain independently bounded.
  - id: OBL-0161-4
    package: WP-712
    proof: scripts/columnar-vectorized-qualification
    says: A fixed single-threaded V1, V2 scalar, and V2 batch receipt satisfies the predeclared mechanics gate before batch execution is selected.
  - id: OBL-0161-5
    package: WP-713
    proof: parallel_columnar_scans_capture_one_active_generation
    says: Every parallel query retains one validated Active generation and provider epoch and canonically merges bounded partials without mixed-frontier results.
  - id: OBL-0161-6
    package: WP-713
    proof: parallel_columnar_scheduler_is_bounded_fair_and_shutdown_safe
    says: The scan scheduler bounds workers, queues, tasks, buffers, fairness, cancellation, panic containment, and shutdown without partial results or storage authority.
  - id: OBL-0161-7
    package: WP-713
    proof: cold_columnar_demand_never_enters_scan_scheduler
    says: Cold and Activating demand returns the existing rowless Building outcome and only an already Active validated view can enter the query scheduler.
  - id: OBL-0161-8
    package: WP-713
    proof: scripts/columnar-parallel-qualification
    says: A fixed one/four-worker and simultaneous-interactive receipt satisfies the scaling and tail gates before parallel execution is selected.
  - id: OBL-0161-9
    package: WP-715
    proof: scripts/columnar-program-b-referee
    says: Cross-plane matched-frontier acceptance proves scalar, vectorized, parallel, policy, lifecycle, and public-surface equivalence on activated behavior.
review_triggers:
  - A caller or configuration value could select a lane, width, worker count, scheduler, pruning path, fallback, policy order, physical identity, or partial result.
  - Policy could run after protected influence, an unauthorized row could affect a predicate, aggregate, order, top-N, work class, diagnostic, or timing class, or output-only data could be decoded for a rejected row.
  - A query could mix generations or frontiers, bypass freshness or authorization safe points, expose a worker-owned handle, or fall back after a V2 failure.
  - Batch or parallel execution would change exact arithmetic, optional-value meaning, total order, cursor identity, error precedence, saturation, cancellation, or public result bytes.
  - A worker, queue, task, selection, partial, heap, buffer, wait, or diagnostic would lose its static global and per-query bound.
  - The scan scheduler could activate, rebuild, catch up, publish, checkpoint, select, retain, or reclaim columnar state, or interfere with ADR-0187 shutdown abandonment.
  - A query IR, module, descriptor, cursor, protocol, durable control, generation root, manifest, segment, provider-state identity, or topology window would change.
  - WP-712, WP-713, or WP-715 would advance while barred by ADR-0183's performance-package freeze or would alter a frozen threshold or receipt identity.
---
# ADR-0161: Compiler-Sealed Vectorized Columnar Execution and Bounded Parallel Scans

## Context

ADR-0160 and WP-711 provide validated immutable V2 lane data, exact pruning,
one durable generation selector, and atomic Active-view capture. The current
query path still reconstructs row values before predicate, aggregate, order,
and output work. Batch evaluation can avoid that cost, but its physical order,
late decoding, and later parallel schedule must not change policy, exact
results, failure precedence, freshness, cursor identity, or resource admission.

Later accepted decisions also close boundaries the legacy proposal predates.
ADR-0190/0192/0200 freeze V2 artifact and control identities; ADR-0195/0196
keep sources cold until one activation worker installs a validated view and
return rowless Building before then; ADR-0187 alone governs catch-up shutdown;
ADR-0183 may bar these packages from advancing. This decision adds query
mechanics only and grants none of those authorities to a scan executor.

## Decision

1. WP-712 adds one nonserializable, fields-private V2 batch program lowered
   from the already checked compiler plan, provider descriptor, registered
   definition, and Active generation. It seals exact fields, types, optional
   states, predicates, aggregates, total order, result shape, bounds, and
   charges. Requests and configuration supply none of that physical program.

2. An Active V2 view retains its already validated immutable segment owners,
   primary-key and entity-version lanes, decoded directories, typed lane views,
   and exact pruning evidence. Batch execution borrows those views; it does not
   reread, rehash, reopen, or revalidate files per query and does not first
   clone the complete generation into a second row-object population. V1 and a
   database without selected V2 remain on their accepted scalar path.

3. Execution order is: capture one Active view and provider epoch; apply only
   ADR-0160-authorized exact segment pruning; decode policy lanes and perform
   policy admission; evaluate predicates; update grouping, aggregate, or order
   state; select the bounded window/top-N; then decode selected output-only
   lanes. Pruning before policy is allowed only for the already accepted
   policy-aligned or fixed-work cases. A denied row influences no later stage.

4. One batch owns borrowed or bounded decoded lane slices, validity maps, and
   a monotone selection vector. Selection begins in canonical segment row order
   and may only clear positions; it cannot add, duplicate, or reorder them.
   Every lane length equals the batch length before evaluation. No row owns a
   task, proof, allocation, descriptor validation, or retained capability.

5. The closed widths are 64, 128, 256, 512, and 1024. The process-private
   provider/program chooses one width no greater than the compiler maximum only
   after checking worst-case lane, validity, selection, partial, and output
   bytes. It remains fixed for that opened provider/program identity. Width is
   neither caller-selected nor recorded in a durable generation, root,
   manifest, segment, control, descriptor, query IR, module, plan, or cursor.

6. Each operator has an independent scalar oracle using the same canonical
   values and logical order. Vector results and closed failure classes equal it
   for every admitted type, Missing/Null/Value state, direction, bound,
   cancellation edge, and overflow. Batch width and completion order cannot
   change error precedence. No floating point, CPU-specific arithmetic,
   unsafe code, native dependency, or approximate substitute enters an exact
   predicate, count, sum, mean, extrema, Boolean, distinct, or group operation.

7. Aggregate partials use ADR-0152's exact checked states and merge in canonical
   root-inventory, segment-ID, then batch-ordinal order. Group keys retain
   canonical typed order. A bounded top-N heap holds at most the compiler result
   maximum plus its one continuation probe, compares the complete declared
   total order plus unique entity-key tie-breaker, and sorts selected output
   exactly once. Exhaustion returns no partial result.

8. WP-712 is single-threaded and may select V2 batch execution only after the
   differential, policy, allocation, and mechanics gates pass. The gate is at
   least 35 percent less CPU per examined row on the registered full-scan
   aggregate corpus or at least 1.5 times complete-query throughput, with no
   more than five percent regression after selective zone-map pruning. A miss
   leaves V2 scalar selected and publishes only value-free evidence. Once batch
   execution is selected, its integrity, policy, bound, or lifecycle failure
   never silently retries through scalar or V1.

9. WP-713 may add one process-global query-scan scheduler with a reviewed static
   worker ceiling, bounded ready queue, bounded per-query tasks and buffers,
   and finite fair-share admission. Its worker count is the lesser of that
   ceiling and available parallelism and is not externally selectable. It owns
   no storage, activation, catch-up, rebuild, control, frontier, publication,
   checkpoint, retention, or fallback authority and cannot call an ADR-0187
   worker-only apply entry point.

10. Only a request that already captured an Active validated view may submit
    scan tasks. Cold or Activating demand follows ADR-0195 as corrected by
    ADR-0196: it returns the existing rowless Building outcome immediately and
    wakes only the sole activation worker. The scan scheduler neither opens a
    source nor retains a cold request. Startup and no-demand close preserve
    PERF-019's zero artifact-open and zero population-walk observations.

11. A parallel query retains one exact immutable generation, provider epoch,
    and bounded task set. Compaction or publication may create a successor but
    cannot replace captured handles. Workers return bounded partials tagged by
    canonical segment and batch ordinal; the coordinator rejects missing,
    duplicate, foreign, late, or excessive output and merges only after every
    required task succeeds. No result combines generations, frontiers, or
    policies, and a worker panic stops admission and returns no partial rows.

12. Causal/bounded freshness waiting remains before Active-view capture and
    keeps its post-wake authorization. The existing pre-release safe point
    catches revocation after execution. Deadlines and cancellation are checked
    between batches and before response release, release all query-owned tasks,
    buffers, handles, and proofs, and preserve typed saturation. Shutdown closes
    scan admission, cancels and joins accepted query work before its retained
    storage handles close, and causes no projection publication or checkpoint.

13. For inference-sensitive policy, task shape, pruning, early stop, aggregate
    work, and telemetry use only policy-aligned state or one fixed admitted work
    class independent of protected membership; otherwise the optimized plan is
    unavailable. Public observations use fixed stage/work-class labels and no
    source, tenant, segment, field, predicate, cardinality, skip count, key,
    value, digest, path, or data-dependent dimension.

14. WP-713 selects parallel execution only after a registered 100k-row receipt
    shows at least 1.5 times throughput from one to four admitted workers and
    four-worker p95 at most 1.10 times single-worker p95 under simultaneous
    interactive load, with unchanged correctness, write, catch-up, freshness,
    lifecycle, and no-projection gates. A miss retains single-threaded batch
    execution and value-free evidence.

15. This decision changes no authoritative or derived durable byte, generation
    selection, artifact identity, retention fence, public protocol, generated
    method, query result, cursor, freshness class, authorization safe point, or
    acknowledgement. A need for serialized capability metadata or a new
    topology node stops WP-712/WP-713 for separate review and scope. This record
    does not lift ADR-0183; while its freeze is in force, WP-712, WP-713, and
    WP-715 remain inert unless a later accepted lifting ADR names them.

## Options considered

1. **Optimize reconstructed row objects:** rejected because it preserves the
   allocation and dispatch boundary the V2 lane format was built to remove.
2. **Put width or scheduling in generation evidence:** rejected because those
   are execution mechanics and would rotate frozen durable identities.
3. **Let each query spawn segment tasks:** rejected because segment count would
   become uncoordinated global work and memory.
4. **Use a sealed lane program, then a separate bounded scheduler:** selected;
   it permits independent semantic and scheduling gates without granting query
   workers projection-lifecycle authority.

## Consequences

- Selected V2 scans can avoid full-row reconstruction and output-only decoding.
- Scalar, batch, and parallel implementations remain independently testable,
  and failure leaves the last qualified executor selected.
- Active-query CPU and buffers gain explicit fixed bounds; no scheduler work is
  added to cold startup or no-demand clean close.
- SIMD, GPU execution, distributed scans, caller tuning, partial results, and
  incremental provider structures remain outside this decision.

## Standing design tests

- **Interface safety:** Applications and agents retain only compiled typed
  operations and values. They cannot select physical lanes, width, workers,
  scheduling, pruning, policy order, fallback, freshness, bounds, or partial
  results, and no failure becomes success with fewer guarantees.
- **Scale:** Batches, lane views, validity maps, selections, partials, heaps,
  tasks, workers, queues, retained snapshots, outputs, waits, and diagnostics
  have independent global and per-query bounds. The POC uses one process-local
  scheduler over immutable derived segments but assumes neither co-located
  authoritative storage, database-wide memory, nor a full-state rewrite.

## Checks

- The nine front-matter obligations name the differential, policy-order,
  resource, lifecycle, scheduler, performance, and cross-plane proofs.
- Architecture checks freeze nonserializable program ownership, Active-only
  scheduler admission, no worker lifecycle/storage authority, no physical
  caller input, and no scalar fallback after selected V2 failure.
- Deterministic schedules cover task permutations, cancellation, saturation,
  panic, publication/compaction races, revocation, and shutdown without sleeps.
