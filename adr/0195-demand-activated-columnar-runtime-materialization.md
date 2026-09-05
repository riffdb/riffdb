---
adr: 0195
title: Demand-Activated Columnar Runtime Materialization
status: accepted
tier: guarantee
date: 2026-09-04
accepted: 2026-09-04
requires: [ADR-0086, ADR-0156, ADR-0187, ADR-0190, ADR-0192]
amends:
  - ADR-0086 query availability by treating a valid but process-cold selected columnar generation as typed Building until its validated immutable view is installed
  - ADR-0156 clean lifecycle scope by keeping configured columnar artifacts cold through readiness and a no-demand graceful close
  - ADR-0187 worker ownership by permitting one demand-activated owner whose shutdown retains the same page-boundary abandonment rule
  - ADR-0190 and ADR-0192 runtime installation timing without changing durable selection, publication, retention, or artifact semantics
supersedes: []
requirements: [PERF-019, PRJ-002, PRJ-004, PRJ-008, PRJ-009, OQ-019, OQ-020, OQ-022, PERF-007, PERF-008]
packages: [WP-777]
obligations:
  - id: OBL-0195-1
    package: WP-777
    proof: configured_columnar_artifacts_remain_cold_through_readiness_and_clean_close
    says: Startup and a no-demand graceful close resolve bounded registrations but never open, decode, apply, publish, or checkpoint configured columnar artifacts.
  - id: OBL-0195-2
    package: WP-777
    proof: first_columnar_demand_coalesces_one_activation_and_returns_building_without_rows
    says: Concurrent first demand installs one server-owned activation and every triggering request returns the existing typed Building outcome with no rows.
  - id: OBL-0195-3
    package: WP-777
    proof: columnar_activation_installs_only_the_validated_selected_immutable_view
    says: Activation validates current control and artifact identity before one atomic view installation, then ordinary freshness and catch-up semantics resume.
  - id: OBL-0195-4
    package: WP-777
    proof: failed_columnar_activation_never_serves_rows_or_falls_back
    says: Corruption, mismatch, resource refusal, and storage failure remain typed and rowless without another generation, layout, source, or authoritative fallback.
  - id: OBL-0195-5
    package: WP-777
    proof: columnar_activation_shutdown_abandons_without_stop_caused_publication
    says: Shutdown cancels activation only at an existing page boundary, joins its sole owner, and creates no stop-caused publication, checkpoint, or frontier advance.
  - id: OBL-0195-6
    package: WP-777
    proof: scripts/wp705-lifecycle-referee
    says: Exact release-process evidence proves the canonical configured production graph keeps zero columnar activations and zero projection population walks through clean readiness and no-demand close.
review_triggers:
  - A public or operator input could choose eager versus demand activation, skip validation, force readiness, select an artifact, or alter an activation bound.
  - A cold, activating, failed, mismatched, corrupt, partial, stale, excessive, or unknown source could serve rows or fall back to another source, generation, layout, partition set, or frontier.
  - More than one activation owner or thread could exist, first demand could block until population work completes, or request cancellation could cancel shared activation.
  - Startup or no-demand graceful close could open or decode a generation artifact, apply authoritative records, publish a view, write a checkpoint, or walk projection population.
  - Shutdown could interrupt inside a commit/page, publish or checkpoint because of stop, or fail to join the activation owner before storage closes.
  - A durable control, generation-root, manifest, segment, retention fence, public protocol, lifecycle enum, freshness outcome, or canonical identity would change.
---
# ADR-0195: Demand-Activated Columnar Runtime Materialization

## Context

ADR-0156 and `PERF-019` require clean-certificate startup and graceful
certificate production to retain bounded heap and perform no projection-
population walk. The canonical production graph nevertheless opens every
configured columnar engine before readiness. Opening a retained 1.1-million-row
checkpoint decodes its complete published snapshot into process heap: measured
clean startup retained about 126 MiB of `VmData` above the pre-open baseline,
while the identical database with no columnar artifact open retained about
52 MiB. The accepted lifecycle ceiling is 64 MiB and may not be raised or
evaded by omitting the configured projection from evidence.

Columnar state is derived and optional to core command readiness, and ADR-0086
already requires unavailable projections to return a typed closed outcome with
no rows. Deferring materialization is therefore safe only if registration,
first demand, concurrent ownership, validation, failure, freshness, retention,
and shutdown are exact. An implementation package cannot independently change
when a selected artifact becomes process-visible or reinterpret the
`PERF-019` evidence window.

## Decision

1. Production startup resolves the complete at-most-256 schema-bound columnar
   source set against the checked active bundle and performs only the bounded
   control/retention operations already required for those sources. It creates
   one fields-private `Cold` slot per admitted source containing its immutable
   registration and least-authority activation handle. Before server readiness
   it MUST NOT open a generation directory, decode a manifest/segment/checkpoint,
   construct a published snapshot, scan or apply projection population, or
   start catch-up. Invalid configuration or bounded control corruption still
   refuses startup; laziness is not deferred configuration validation.

2. Each slot has the closed process-local state machine `Cold -> Activating ->
   Active | Failed`, plus terminal `Stopped`. Exactly one server-owned columnar
   worker serializes activation transitions. The first projected query, vector
   execution, or freshness wait against `Cold` atomically changes it to
   `Activating` and wakes that worker. Concurrent demand coalesces onto the same
   transition; it creates no task, thread, engine, retry loop, or retained
   request per caller. Status and aggregate-health observations are read-only
   and do not activate a source.

3. A request that observes `Cold` or `Activating` MUST return immediately with
   the existing typed Building/unavailable result and no rows. Its current
   frontier is the exactly selected durable frontier when one is valid and
   `BeforeFirst` otherwise. For the ADR-0086 projected-query result this is
   `Degraded { reason: Building }`; vector execution uses its existing
   `Building` error. A transient lower storage failure uses the existing typed
   storage-unavailable error. No new public enum, field, retry hint, option, or
   success-with-empty-data interpretation is added. Projection control status
   continues to report exact durable lifecycle; aggregate columnar health is
   degraded while any admitted source remains cold, activating, or failed.

4. Activation rereads current durable selection and authority rather than
   trusting startup observations. When a selected generation exists, it opens
   only that exact source/generation/layout and validates every required
   control, root, manifest, segment/checkpoint, checksum, incarnation,
   definition/spec, frontier, and bound before atomically installing one
   immutable query view. When no selected generation exists, it begins the
   already accepted authoritative snapshot-plus-retained-tail build under its
   existing fence and bounds. It MUST NOT import legacy state as new control,
   infer selection from a path, or fall back to another generation, layout,
   source, partition set, or frontier.

5. After a validated view is installed, the slot becomes `Active`, wakes the
   existing notifier, and participates in ordinary catch-up, publication,
   checkpoint, freshness, cursor, policy, and query semantics without a second
   path. A selected view that is valid but behind remains subject to the
   existing causal/bounded wait outcome. Activation itself never advances a
   durable or visible frontier; only the accepted all-or-none apply and
   publication operations may do so.

6. Corruption, identity mismatch, incompatible bytes, hard-limit refusal, or
   storage failure records only the existing closed process/durable failure
   allowed for that condition and moves the slot to `Failed`. Every query stays
   rowless and typed. Reattempt is owned only by an already accepted rebuild or
   recovery transition; requests do not spin, reopen, clear failure, or consume
   an attempt budget. Application, SDK, transport, MCP, CLI, and configuration
   surfaces gain no activation or validation control.

7. The triggering request does not own the shared activation, so request
   completion or cancellation cannot cancel it. Daemon shutdown monotonically
   stops new demand, wakes the one worker, and joins it before columnar storage
   closes. A `Cold` slot closes without opening or checkpointing. An
   `Activating` or `Active` slot observes the ADR-0187 stop token only at the
   existing complete-page boundary and may abandon only unpublished work; stop
   causes no publication, notification, checkpoint, durable-frontier advance,
   or clean-certificate dependency.

8. `PERF-019` clean lifecycle evidence uses the exact default-feature release
   server with the canonical configured production graph and performs no
   projected query, vector execution, or freshness wait between process spawn
   and graceful close. It records admitted cold-source count, activation count,
   and every prohibited projection-population counter; activation count and all
   population counters MUST be zero. The accepted 65,536-row and production-
   scale heap/time ceilings remain unchanged. Post-demand materialization is
   measured by the existing `PERF-007`/`PERF-008` workload and resource gates,
   not represented as clean lifecycle work.

9. This decision changes no authoritative mutation, acknowledgement,
   transaction order, durable encoding, public protocol, projection identity,
   selection/publication CAS, artifact bytes, retention fence, or freshness
   guarantee. WP-776 and WP-711 MUST preserve this cold registration and demand
   boundary when common control and V2 activation replace V1 mechanics.

## Options considered

1. **Raise the 64 MiB lifecycle ceiling:** rejected because it makes startup
   memory proportional to derived population and weakens accepted `PERF-019`.
2. **Exclude projections from the evidence configuration:** rejected because
   it does not measure the production graph and hides the retained allocation.
3. **Eagerly open then release the checkpoint:** rejected because readiness
   still pays population work and peak heap, and allocator retention remains.
4. **Demand-activated materialization:** selected because it keeps core
   readiness population-independent while preserving fail-closed projection
   semantics and all durable authority.

## Consequences

- Clean startup and no-demand close remain bounded independently of columnar
  population even when production configuration names retained projections.
- The first demand receives a typed Building/unavailable result and must retry
  under its ordinary application policy; it never receives stale or empty
  success. Materialization latency and memory move to actual projection use.
- A never-demanded configured source remains visibly degraded and consumes only
  its bounded registration/control state. It cannot silently appear healthy.
- Predictive prewarming, operator-triggered activation, configurable eager
  mode, parallel activation, and new public progress are deferred. Any of them
  requires a later accepted ADR rather than an implementation option.

## Standing design tests

- **Interface safety (AGENTS.md boundary 11):** Activation is automatic on
  first semantic demand and wholly server-owned. No application or agent can
  select bytes, generations, layouts, fallback, validation strength, worker
  count, scheduling, or bounds, and no cold/failing path can return rows.
- **Scale:** Startup retains at most 256 bounded registrations and one worker,
  independent of entity or projection population. Activation is serialized,
  uses existing page/segment/query bounds, and installs at most one engine per
  admitted source. Runtime memory after actual demand remains governed by
  `PERF-007`/`PERF-008`; this record adds no full-database rewrite or duplicate
  in-memory generation.

## Checks

- Req-tagged state-machine tests use barriers to prove first-demand coalescing,
  immediate rowless outcomes, single installation, cancellation independence,
  and shutdown at a page boundary without sleeps.
- Corruption and resource matrices prove current-selection reread, exact
  validation, and absence of every fallback.
- Architecture tests prove there is one worker, no public activation control,
  no request-owned task, and no engine/artifact open on process-graph startup.
- The WP-705 process referee proves both exact clean lifecycle checkpoints with
  the production configuration and zero activation/population work.
