---
adr: 0187
title: Bounded Columnar Shutdown Abandonment
status: accepted
tier: guarantee
date: "2026-09-02"
accepted: "2026-09-02"
requires: [ADR-0007, ADR-0049, ADR-0086, ADR-0156]
amends:
  - ADR-0049's derived-worker stop sequence by bounding only the in-flight
    columnar worker's response to a graceful stop
supersedes: [ADR-0166]
requirements: [PRJ-001, PRJ-002, PRJ-003, PRJ-004, PERF-007, PERF-008, PERF-019]
packages: [WP-773]
obligations:
  - id: OBL-0187-1
    package: WP-773
    proof: ordinary_columnar_apply_publication_and_checkpoint_are_unchanged
    says: With no stop request, columnar catch-up retains its exact scan fence,
      publication, notification, checkpoint, and result semantics.
  - id: OBL-0187-2
    package: WP-773
    proof: shutdown_abandons_only_unpublished_columnar_work_at_a_page_boundary
    says: A graceful stop is observed after at most one existing 64-record page,
      causes no publication, notification, checkpoint, or durable-frontier advance,
      and discards only unpublished derived work.
  - id: OBL-0187-3
    package: WP-773
    proof: abandoned_columnar_shutdown_replays_from_the_durable_frontier
    says: Reopen after page-boundary abandonment replays from the last durable
      frontier to the same projection result and frontier as uninterrupted catch-up.
review_triggers:
  - A stop request would be observed inside a commit, or any partial commit could
    become visible or durable.
  - Ordinary no-stop catch-up would gain a different publication, notification,
    checkpoint, scan-fence, freshness, or retention behavior.
  - Shutdown abandonment would discard authoritative state, advance a durable
    frontier, or weaken restart replay and idempotency.
  - An application, transport, operator option, or configuration value could select
    interruption, page size, publication, checkpoint, or restart behavior.
---
# ADR-0187: Bounded Columnar Shutdown Abandonment

## Context

The production columnar worker checks its stop flag only between catch-up calls,
while one call drains every 64-record page through a frozen exact end. WP-667
proved a bounded-turn engine but removed it after every tested production
schedule missed a lifecycle, latency, throughput, or resource gate. WP-668 then
measured columnar shutdown as a distinct variable owner. A shutdown bound may not
reactivate WP-667's ordinary scheduling change or alter the publication contract.

ADR-0086 already separates visible and durable projection frontiers and permits a
restart to regress to the durable frontier and replay. ADR-0156 does not bind the
derived columnar plane into the clean-close certificate. The safe boundary is
therefore to abandon only unpublished derived work when shutdown is requested,
without manufacturing a publication or checkpoint at the stop edge.

## Decision

1. The ordinary no-stop `apply_available` path retains its existing frozen exact
   end, all-or-none commit application, safe-prefix holdback, final publication,
   and returned progress semantics; the ordinary worker retains its notification
   and checkpoint cadence. No bounded turn, polling window, public tuning value,
   or caller-selected continuation is added to ordinary catch-up.

2. The production worker alone may call one workspace-internal shutdown-aware
   catch-up entry point owned by `riffdb-columnar` and not re-exported through an
   application, SDK, transport, or operator surface. The server worker owns the
   monotonic stop token and may supply an internal read-only closure that observes
   it only after processing a complete existing page of at most 64 authoritative
   commit records and before requesting another page. It never observes
   interruption inside a commit and adds no clock, sleep, randomness, application
   callback, application input, or unbounded retained state.

3. When that page-boundary observation sees no stop request, execution is
   semantically identical to the ordinary path. When it sees a stop request, it
   returns an internal abandoned result without the ordinary final publication.
   The worker emits no notification, performs no final columnar checkpoint, and
   advances no durable frontier because of the stop.

4. Any maximal-safe-prefix publication completed before the stop observation
   under the existing supersession-holdback rule remains published. Everything
   after the last such publication exists only in the in-memory working delta and
   is dropped with the worker. No published or durable state is rolled back, and
   no unpublished state is made visible merely to retain shutdown progress.

5. Reopen uses the existing durable projection checkpoint and authoritative log.
   It may report the already accepted typed building or lagging state while it
   idempotently replays the abandoned range. It must converge to the same rows and
   frontier as uninterrupted catch-up, and the durable frontier continues to fence
   retention without overclaiming recoverable projection state.

6. Shutdown observation is one fixed redaction-safe internal value with exactly
   `between_passes`, `abandoned_unpublished`, and `failed`. It may appear only in
   bounded shutdown diagnostics; it carries no database, projection, organization,
   path, frontier, distance, row count, key, value, hash, credential, or incident
   detail and cannot be supplied back as control input. Existing public projection
   health and freshness values do not change.

7. The columnar shutdown stage is bounded by the work of one already-bounded
   64-record page plus fixed worker join and status reporting. It never pays the
   remaining catch-up backlog or a whole-projection checkpoint. Failure retains
   the existing fail-closed nonzero shutdown outcome.

## Standing design tests

- **Interface safety (AGENTS.md boundary 11):** The server owns the stop token;
  the shutdown-aware entry point and result are workspace-internal and are not
  re-exported by any application, SDK, transport, or operator surface.
  Applications and agents gain no operation, option, configuration, freshness
  override, storage handle, or way to request partial publication, skip replay,
  or opt out of a frontier guarantee.
- **Scale:** Stop observation is once per existing page and retains no history.
  Shutdown work is bounded by 64 already-bounded commits rather than backlog or
  projection population; replay remains driven by the authoritative retained log
  and existing projection budgets.

## Checks

- `ordinary_columnar_apply_publication_and_checkpoint_are_unchanged` compares the
  no-stop path with the pre-change publication observer, progress, notification,
  checkpoint, and snapshot behavior.
- `shutdown_abandons_only_unpublished_columnar_work_at_a_page_boundary` uses an
  explicit barrier to request stop between pages and proves no stop-caused publish,
  notify, checkpoint, or durable-frontier advance, including holdback.
- `abandoned_columnar_shutdown_replays_from_the_durable_frontier` reopens the
  abandoned plane and compares its final rows and frontier with an uninterrupted
  independent oracle.
- Process evidence exercises a production-scale backlog, pins the closed shutdown
  result and stage bound, and retains the existing no-regression gates for ordinary
  catch-up, application latency and throughput, CPU, and memory.
