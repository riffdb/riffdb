# ADR-0053: Composite Query Snapshots and Current-View Publication

- **Status:** Accepted
- **Direction approved:** 2026-07-28
- **Exact text accepted:** 2026-07-28
- **Acceptance reference:** Maintainer authorization in the current Codex
  session to make the implementation decisions required through WP-270
- **Requires:** ADR-0003, ADR-0004, ADR-0007, ADR-0009, ADR-0012,
  ADR-0016, ADR-0025, ADR-0026, ADR-0027, ADR-0033, ADR-0034, ADR-0035,
  ADR-0038, ADR-0042, ADR-0049, ADR-0051, and ADR-0052
- **Amends:** SPEC Sections 4.1, 5.2, 9, 12, 13, 16, 19,
  and 20.7
- **Implementation boundary:** Before WP-205 accepts a latency optimization or WP-240
  publishes the executable query-read boundary

The maintainer accepted this exact one-snapshot, rebuildable-view, and
unary-gRPC-first boundary on 2026-07-28 and authorized the implementation
decisions required through WP-270.

## Context

The TicketDesk baseline on the same loopback host measured roughly 43--47 ms for
one point read, 133--230 ms for small list reads, about 477--507 ms for a detail
page, about 70--73 ms for one command, and about 17 seconds to seed 276 rows.
The client reuses one HTTP/2 channel. Lists perform one scan plus N sequential
point RPCs, while one point request repeatedly reads and validates current
catalog and capability state around the authoritative read.

Changing transports cannot remove N+1 service operations or repeated semantic
reads. Optimization must preserve fail-closed current authorization, immutable
catalog evidence, coordinator ownership, audit durability, and storage
encapsulation.

## Decision

### Composite query snapshot

Introduce a versioned, closed, storage-neutral `QueryAccessProgram` produced only
by the accepted RiffQL planner. It contains exact internal IDs, encoded key/range
templates, dependency edges, field requirements, order, cardinality, and every
accepted work bound. Callers and transports cannot construct it.

Introduce a narrow query-execution port below the application service. Its
concrete engine adapter opens one read transaction, executes the complete
program, copies bounded observations into an owned `QuerySnapshot`, and closes
the engine transaction before returning. Dependent point targets may be derived
only by the closed program from earlier observed keys/fields. The adapter cannot
call network, policy, clock, randomness, command runtime, commit coordinator, or
transport code.

One snapshot records its authoritative application head and every relevant
index epoch. All returned bindings for one request come from that snapshot.
No live engine handle, iterator, callback, or arbitrary transaction escapes the
port.

Pagination is snapshot-per-request. A cursor binds principal, exact contract and
module/plan identities, canonical parameter hash, ordering, continuation,
authorization constraint, and observed index epochs. A later request opens a
new snapshot and rejects if a bound epoch changed. RiffDB does not retain or
durably persist a user snapshot between pages.

Covering index values are not added in the first execution milestone. The
composite snapshot scans index keys and performs dependent point reads inside
the same engine view. A covering-format proposal requires separate profiling
evidence and an accepted durable-format ADR.

### Rebuildable current views

Replace repeated production reads of active catalog and current capability state
with two immutable, revisioned, process-local views:

- the active catalog view contains the fully validated active bundle and exact
  historical lookup material required by admitted service operations; and
- the capability view contains active/inactive capability records indexed by
  stable ID and nonsecret configured digest, with the exact durable
  administration revision/frontier from which it was built.

These views are derived, bounded, non-authoritative, and rebuilt from complete
validated authoritative state at startup before readiness. They contain no raw
credential or digest-key material.

The commit coordinator remains the sole writer. After a catalog/capability
mutation is durable, its owner publishes the exact resulting immutable view
before the successful public operation is released. Publication is monotonic,
gap checked, and identity checked. Failure, uncertainty, gap, overflow, or
revision mismatch closes application readiness; it never serves the old view as
current. Restart performs a complete rebuild rather than persisting cache state.

Authentication still performs a digest lookup on every new RPC. Each initial,
pre-execution safe point required after an admission wait, and final
pre-release authorization still resolves the current capability by stable ID
and samples its owned clock. The resolver may use the current immutable view,
but no session proof changes expiration or revocation latency.

This record does not authorize removing a safe point. WP-205 must enumerate the
exact operation-specific safe points and prove that any consolidation preserves
the accepted revoke/deploy race outcomes before implementation.

### Performance method and gates

WP-205 adds redaction-safe stage measurements for client validation, Tonic
transport, authentication, catalog selection, each authorization safe point,
port scheduling, storage, audit, response conversion, and client validation.
It benchmarks in-process service and reused-channel loopback gRPC with identical
requests.

The initial reference-machine gates are:

- warmed kernel point read p50 at or below 5 ms;
- warmed named TicketDesk list/detail query p50 at or below 15 ms;
- committed and replayed command p50 at or below 20 ms;
- 276-row development seed at or below 3 seconds; and
- reused-channel unary gRPC overhead over the in-process service at or below
  1 ms p50.

These are engineering gates on the recorded local NVMe profile, not portable
marketing claims or correctness substitutes. CI runs deterministic structural
checks; the release evidence records hardware, filesystem, raw samples, and
methodology.

Unary gRPC remains the application wire transport through WP-270. A session
stream or separate protocol requires evidence that framing exceeds the 1 ms
gate after service/storage optimization and a separate accepted ADR.

Development seeding uses bounded concurrent ordinary command invocations.
Optional coordinator group commit may amortize durability across independently
atomic commands while preserving per-command sequence, outcome, provenance,
audit, idempotency, and cancellation semantics. No bulk mutation or transaction
callback API is introduced.

## Options Considered

1. **New text wire protocol first:** rejected because measurements already show
   structural service-call amplification and repeated semantic reads.
2. **Long-lived authenticated sessions:** rejected because they change
   revocation/expiration semantics.
3. **No current-view acceleration:** rejected because it preserves repeated
   durable read/decode work on every request.
4. **Covering indexes immediately:** deferred until composite snapshots are
   measured.
5. **Return a storage transaction to the service:** rejected as a direct
   violation of the semantic storage boundary.

## Consequences

- One application page incurs one public request, one authorization lifecycle,
  and one engine snapshot rather than N public reads.
- Catalog and capability mutations gain a mandatory derived-publication step and
  readiness failure mode.
- Storage engines implement a closed bounded query access program rather than a
  general query callback.
- Exact transaction/auth ordering changes require concurrency and crash evidence
  before acceptance.

## Compatibility

The first milestone changes no existing entity, index, command, audit, or
capability durable encoding. `QueryAccessProgram`, query snapshots, and cursors
use new independent versions. Current views are not persisted and are excluded
from backup/restore formats.

## Security

Current views grant no authority and cannot outlive a detected gap. Query
snapshots contain only fields already included in the compiler-derived
requirement; final authorization occurs before release. Stage telemetry uses
closed enums and durations only and contains no query text, parameters, symbols,
credentials, keys, or result values.

## Testing

- Barrier-controlled revoke/deploy/query schedules with no sleeps.
- Loom or Shuttle tests for view publication, gap handling, and readiness close.
- Memory/redb snapshot parity, cardinality, bounds, cancellation, and cursor
  epoch properties.
- Process-kill tests before/after durable mutation and before/after view
  publication, followed by complete rebuild.
- In-process versus gRPC stage and semantic parity benchmarks.
- TicketDesk point/list/detail/command/seed raw performance evidence.

## Requirements and Work Packages

- **Requirements:** New post-POC snapshot, publication, and application-latency
  requirements assigned in WP-205
- **Defines or blocks:** WP-205, WP-220, WP-230, WP-240, WP-250, and WP-270
- **Final evidence:** WP-280

## Decision Deadline

Exact acceptance is required before authorization safe-point ordering, current
catalog/capability resolution, readiness publication, query snapshot traits,
cursor semantics, or production performance claims change.
