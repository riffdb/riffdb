# ADR-0049: P2 Derived Recovery, Telemetry, and Hosted Composition

- **Status:** Accepted
- **Direction approved:** 2026-07-24
- **Exact text accepted:** 2026-07-24
- **Accepted:** 2026-07-24
- **Acceptance reference:** Human maintainer confirmation in the current Codex
  session on 2026-07-24
- **Requires:** ADR-0004, ADR-0007, ADR-0010, ADR-0017, ADR-0027,
  ADR-0035, ADR-0043, and ADR-0044
- **Amends:** SPEC Sections 5.2 through 5.4, 10.6, 14, 15, 16.2 through
  16.5, 17.4 through 17.5, 19.4 through 19.5, 20.4, and the ADR register;
  and the affected WP-050, WP-060, WP-070, WP-100, WP-120, WP-140,
  WP-160, WP-170, WP-180, WP-185, WP-190, and WP-200 package boundaries
- **Decision deadline:** Before WP-160, WP-170, or WP-180 is accepted and
  before WP-185 production composition begins

The human maintainer accepted this exact corrective direction in the current
Codex session on 2026-07-24. This record supplies the missing owner interfaces
needed to join the independently implemented P2 workers and observability
components. It also freezes the optional hosted-MCP process composition needed
by WP-185. It changes no public Protobuf field, MCP schema or URI, durable
semantic record, storage key, contract bundle, IR encoding, plan hash, command
semantics, commit order, or authoritative readiness rule.

## Context

The P2 packages exposed four interface gaps:

1. `OutboxRepository::scan_pending_outbox` cannot enumerate an explicit
   `Delivering` row because the process-local pending accelerator intentionally
   excludes it. A worker therefore cannot prove that every interrupted
   delivery was normalized before declaring recovery complete.
2. The storage projection query returns no generation with degraded state and
   no generation or frontier with invalid state. The service's already accepted
   projection fence requires those facts to validate one atomic observation.
   Reading status separately would create a mixed-snapshot result.
3. The specification assigns compatible event-payload normalization to catalog
   but has no complete catalog operation that binds an immutable event to its
   enclosing writer plan and the exact projection plan. Projection also owns
   filter/group/count/sum execution but lacks the narrow IR and pure evaluator
   dependencies required to execute the already checked expressions.
4. WP-180 cannot reconstruct required tracing and metrics after the fact.
   Owners must emit bounded, payload-free semantic events before arbitrary
   tracing subscribers, and WP-185 must wire them into one in-process registry.

WP-140 already implements the hosted MCP Tower service. WP-185 still needs an
approved listener, audience, lifecycle, shutdown, and dependency boundary to
serve it from `riffdbd` without creating a second service or authorization
path.

## Decision

### Storage owns complete bounded derived recovery observations

`riffdb-storage-api` adds a worker-only outbox recovery scan distinct from the
ordinary pending-dispatch scan. Its first request freezes the greatest
reciprocal authoritative `EventId` visible in that read transaction, or an
explicit before-first empty fence. Every continuation carries that same upper
fence and an exclusive last `EventId`. Pages are in strict `EventId` order,
contain at most 500 complete items and 4 MiB of checked encoded content, and
end only with an explicit exact-end result.

Each item contains the exact equal durable event/outbox-intent pair and the
source observation of its status:

- absent status, canonically meaning never-attempted `Pending`;
- explicit `Pending`, preserving retry metadata;
- explicit `Delivering`;
- `DeadLetter`; or
- `Delivered`.

The recovery consumer may request all reciprocal items or only undelivered
items. An undelivered scan includes absent/explicit `Pending`, `Delivering`,
and `DeadLetter`, and excludes `Delivered`. The engine, not the worker, proves
event/intent/status reciprocity. Missing, orphaned, duplicate, unequal, or
mis-keyed rows are typed derived integrity findings and are never synthesized
or repaired by the scan.

The process-local pending accelerator remains non-durable. Memory and redb
rebuild it from reciprocal authoritative rows on every open. It may accelerate
ordinary pending dispatch but is not evidence that no `Delivering`,
`DeadLetter`, or orphan status exists. Recovery always consumes the complete
bounded storage scan through exact end. WP-160 alone applies retry policy and
uses the existing compare-and-transition operation to normalize every observed
`Delivering` row before its readiness becomes true. A race yields
`StateChanged` and is reread; it is never overwritten blindly.

If no production destination is configured, WP-185 does not install a
discarding connector. Reciprocal pending events remain pending, no attempt is
invented, and derived health reports a bounded outbox degradation. No event is
silently marked delivered or dead-lettered merely because configuration is
absent.

### Projection storage results carry one atomic internal fence

`riffdb-storage-api` enriches only its process-local `ProjectionQueryResult`:

- `Degraded` carries the exact `ProjectionIdentity`, an optional retained
  `ProjectionGeneration`, its `FrontierPosition`, and the closed reason from
  one read transaction. Generation may be absent only for a known identity
  with no control record, at `BeforeFirst`, with the `Building` reason.
- `Invalid` carries the exact identity, failed nonzero generation,
  `FrontierPosition`, and closed failure from that same transaction.

Memory and redb derive these values from the one control/query snapshot already
used for the result. The mechanical WP-185 adapter constructs the accepted
service `ProjectionStateFence` or `ProjectionPageFence` directly from that
result. It does not issue a second status read. The service continues to expose
only the already accepted public degraded/invalid result fields after it has
validated the internal fence; no public wire or MCP schema changes.

### Catalog is the sole compatible event-materialization authority

`riffdb-catalog` owns two fields-private, nonserializable, process-local values:

- a resolved projection plan bound to the exact active catalog snapshot,
  lineage, projection identity, bundle, and checked projection plan; and
- an opaque event-materialization view bound to that resolved plan, the
  enclosing commit's complete `ExecutablePlanRef`, and one immutable
  `StoredDurableEventV1`.

The catalog operation:

1. resolves the exact projection bundle and plan;
2. resolves and verifies the enclosing writer-plan reference through the
   catalog-proved active-to-genesis lineage;
3. verifies event type, writer relationship, canonical payload shape, event
   identity, and immutable hash relationship;
4. permits omission only for a strict-ancestor optional field whose
   introduction is proved by the existing lineage ledger, materializing that
   field as canonical null;
5. preserves unknown fields but makes them invisible to the selected
   projection schema; and
6. rejects foreign, descendant, same-version malformed, genesis, required-field
   omission, wrong-type, hard-limit, and integrity cases without modifying the
   event.

The view exposes only checked schema-directed field access needed by projection
evaluation. It exposes no raw payload bytes, unknown-field iterator, omission
mask, ancestry proof, bundle constructor, storage handle, callback, or mutation
authority. Raw event bytes and `EventHash` remain unchanged and are never
persisted as a normalized copy. Live catch-up and rebuild must use the identical
catalog operation.

`riffdb-projection` may depend directly, with default features disabled, on
`riffdb-contract-ir` and `riffdb-invariant` solely to execute checked projection
filter, key/group, and measure expressions over that opaque catalog-normalized
view. It may not decode a durable event, derive ancestry, fill omissions,
resolve a bundle, evaluate command or commit-check plans, or access storage
implementation types. Catalog remains the sole normalizer and
`riffdb-invariant` remains the sole pure expression evaluator.

### Semantic owners emit closed telemetry before subscribers

Telemetry event vocabularies stay with the component that knows the semantic
transition:

- service owns API operation, authorization, audit, response, wait, and cursor
  classes;
- commit owns admission, evaluation, idempotency, commit, replay, uncertainty,
  storage-queue, and durable-flush classes;
- conflict owns bounded lock wait, timeout, queue-depth, and hot-key
  cardinality observations;
- storage API/redb own storage, recovery, page, commit-log, and capacity
  observations;
- MCP owns session, risk-class tool call, schema failure, authorization denial,
  and list-change observations;
- outbox and projection own their worker attempt, retry, frontier, rebuild, and
  failure observations; and
- catalog and auth/policy retain their existing closed owner hooks.

Each owner defines a least-authority injected sink trait and a closed event
enum. The enums accept only stable closed tags, bounded numeric quantities, and
already-redacted fixed-size hashes expressly listed in SPEC Section 16.4. They
accept no arbitrary map, label name, error text, business value, entity key,
credential, raw tenant/principal/request/session identifier, event payload, or
`Debug` rendering. A no-op sink remains available for isolated tests.

First-party code validates and redacts before invoking any sink or `tracing`
macro. A subscriber layer is not considered a redaction boundary because a
sibling subscriber could observe the original event. `riffdb-observability`
adapts only those closed events into a frozen in-process span/metric inventory
and safe tracing target. It has no business decision or mutation authority.
Tests install an independent permissive sibling subscriber/exporter and prove
that canary values cannot be observed.
WP-180 therefore retains WP-120 and adds WP-140, WP-160, and WP-170 as hard
dependencies; the semantic owners land their hooks first, and observability
then freezes the complete inventory rather than guessing interfaces in
parallel.

Metrics use fixed closed label registries and checked counters, gauges, and
histograms. Cardinality limits and overflow/drop counters are explicit.
WP-180 provides the complete Section 16.5 inventory, including current gauges,
histogram bucket behavior, capacity/drop behavior, and stable label fixtures.
The POC registry remains in-process. This decision does not add a network
metrics listener or exporter.

### WP-185 composes the accepted hosted endpoint and lifecycle

`riffdbd` adds these optional flags:

```text
--mcp-listen <loopback-ip-literal:nonzero-port>
--mcp-origin <exact-http-loopback-origin>   # repeatable
```

Absence of `--mcp-listen` disables hosted MCP. A present value must be a literal
IPv4 or IPv6 loopback address and nonzero port. Each origin is independently
validated by WP-140's existing bounded exact-origin rules; absence permits only
requests without a browser `Origin` header. Origins are never inferred from
untrusted requests.

The MCP authentication audience is exactly
`http://<canonical-listen-authority>/mcp`. Server composition adds it to the
trusted audience set alongside the unchanged gRPC audience and constructs
`HostedMcpHttpConfiguration` with that exact value. It does not accept an
operator-supplied audience for this endpoint.

`riffdb-server` directly uses the already locked
`axum = 0.8.9` with default features disabled and only `http1` and `tokio` to
serve WP-140's accepted Tower service on a separate listener. This is HTTP/1
loopback hosting only; it adds no TLS, HTTP/2, proxy trust, OAuth, remote bind,
or alternate route. Tokio's direct server features may add only `net` and
`signal` to the already accepted process runtime for this composition.

Hosted MCP receives the same shared lifecycle proxy and activated
`RiffDbService` as gRPC. It never captures a permanently ready service,
constructs a service, or bypasses startup, maintenance, draining, or failed-
closed routing. Authentication remains auth-owned and every operation continues
through WP-140's service backend and repeated current authorization.

SIGINT, SIGTERM, or the exact existing `shutdown\n` test-control line initiates
graceful shutdown. Stdin EOF merely disables the test-control watcher; it does
not stop a daemon launched with closed stdin. Shutdown stops new admission,
drains and closes hosted MCP sessions/listener, stops derived workers, drains
the shared service/coordinator, closes providers/storage, and finally exits,
all under the existing bounded grace policy. A timeout fails closed and exits
nonzero; no clean-shutdown durable marker is written.

## Consequences

- WP-160 can prove complete `Delivering` normalization without trusting an
  incomplete accelerator.
- WP-170 can satisfy the service's one-snapshot fence and compatible event
  evolution without duplicating catalog or expression semantics.
- Upstream components remain independent of `riffdb-observability`; only closed
  sink interfaces cross ownership boundaries.
- Hosted MCP is an optional transport over the same service and lifecycle, not
  a second server core.
- The server gains one reviewed direct Axum edge and two Tokio features already
  present in the lock graph.
- Derived recovery faults can degrade their component without weakening or
  rewriting authoritative source state.

## Rejected Alternatives

1. **Recover from the pending accelerator:** rejected because it cannot prove
   the absence of explicit `Delivering` or orphan status rows.
2. **Join a projection query to a later status read:** rejected because it can
   construct a generation/frontier pair that never existed atomically.
3. **Let projection decode raw events or infer null fills:** rejected because it
   creates a second compatibility authority.
4. **Move all projection expression execution into catalog:** rejected because
   catalog owns schema resolution/materialization, while projection owns
   derived-state execution; both can share the one pure checked evaluator.
5. **Redact only in a tracing layer:** rejected because sibling subscribers see
   events before that layer can sanitize them.
6. **Let observability accept arbitrary labels or text:** rejected because it
   creates unbounded cardinality and data-exfiltration surfaces.
7. **Expose a network metrics endpoint in WP-180/WP-185:** deferred because it
   adds another listener and deployment/security boundary not needed for the
   semantic POC.
8. **Give MCP a direct storage or readiness handle:** rejected by API-001 and
   the accepted hosted adapter boundary.
9. **Treat stdin EOF as shutdown:** rejected because normal service managers
   launch daemons with closed stdin.

## Compatibility

All storage, catalog, projection, telemetry, and lifecycle values in this
record are process-local Rust interfaces. The projection and outbox scans read
existing records and keys. No durable envelope or public message changes.
The MCP flags are additive local process configuration; absence retains the
existing gRPC-only server behavior. Moving the already locked Axum row into the
production server graph requires the normal lock/feature review but introduces
no new version or source.

Changing scan ordering/fences, event materialization authority, projection
normalization/evaluator ownership, public projection fields, telemetry label
vocabularies, listener trust, audience derivation, or shutdown order requires
new human review.

## Security

Recovery scans never expose event payloads to health or telemetry. Catalog's
opaque view prevents a worker from interpreting hidden unknown fields.
Telemetry producers accept no caller-controlled text. Hosted MCP is loopback
only, validates exact authority and origin before authentication, uses the
auth-owned retained credential, and invokes only the shared service. Raw
configuration, origins, bearer values, and connector errors are redacted.

## Testing

- Storage API, memory, and redb tests freeze outbox first/continuation/exact-end
  fences, 500-item/4-MiB/equal-plus-one bounds, every effective state, reciprocal
  corruption, reopen accelerator rebuild, and CAS races.
- WP-160 recovery tests prove every `Delivering` row at the frozen fence is
  normalized before readiness and no-destination composition preserves pending
  rows.
- Storage projection tests freeze generation/frontier shapes for absent,
  building, catching-up, rebuilding, degraded, and invalid control.
- Catalog/projection fixtures cover strict ancestor optional-null
  materialization, exact/descendant/foreign/genesis/required omission, unknown
  fields, wrong types, raw byte/hash immutability, hard bounds, and identical
  live/rebuild results.
- Telemetry inventory, canary, sibling-subscriber, label-cardinality,
  histogram/gauge, overflow/drop, and source architecture tests cover every
  Section 16.4/16.5 owner.
- WP-185 process tests cover disabled/enabled MCP configuration, loopback and
  origin rejection, exact audience, service lifecycle changes, authorization,
  worker startup/degradation, SIGINT/SIGTERM, stdin EOF, bounded drain order,
  and retained WP-130 restart behavior.
- WP-190 repeats derived crash/restart and hosted shutdown cases at process
  level; WP-200 consumes the final reports.

## Requirements and Work Packages

- **Requirements:** `API-001`, `ID-005`, `EFF-001`, `EFF-002`, `EFF-003`,
  `PRJ-001`, `PRJ-002`, `PRJ-003`, `PRJ-004`, `REC-001`, `REC-002`,
  `REC-003`, `MCP-010`, `MCP-011`, `MCP-043`, `POC-005`, `POC-006`,
  `POC-007`, `POC-008`, `POC-009`, and `POC-010`
- **Interfaces corrected or blocked:** `WP-050`, `WP-060`, `WP-070`,
  `WP-100`, `WP-120`, `WP-140`, `WP-160`, `WP-170`, `WP-180`, and
  `WP-185`
- **Final evidence:** `WP-190` and `WP-200`

Implementation must stop if these process-local changes require a public or
durable field, a second normalization/evaluation authority, a non-loopback MCP
bind, an arbitrary telemetry field, or a changed authoritative readiness rule.
