# ADR-0010: Event-Derived Projection and Frontier Semantics

- **Status:** Accepted
- **Direction approved:** 2026-07-12
- **Exact text accepted:** 2026-07-13
- **Decision deadline:** Persistence semantics before WP-070; full record before WP-170

The human maintainer accepted this exact text on 2026-07-13.

## Context

Projection state is derived and rebuildable, while the commit log and entity
state are authoritative. Consistency claims require a durable frontier that never
gets ahead of applied state, never skips a commit, and survives crashes without
double application.

## Proposed Decision

Each projection consumes committed records in strictly contiguous increasing
`CommitSequence` order. Applying all relevant events for sequence `N`, recording
the `(projection_id, N)` idempotency marker or equivalent, updating derived state,
and advancing `applied_through` to `N` occur atomically in one storage
transaction. A frontier never decreases or advances across a missing sequence.

Projection state and frontier are namespaced by stable projection identity and
plan/version. Reapplying an already applied sequence is a no-op with equality
checks. On incompatible plan change or corruption, state is discarded and rebuilt
from the authoritative commit log; no projection repairs authoritative state.

Queries return data plus the observed frontier. `after_sequence` waits return
typed ready, timeout, degraded, or invalid results under bounded deadlines.
Lifecycle is durable and monotonic only where the specified state transition
allows it. WP-130 proves protocol mapping with an injected stub; WP-170 provides
real semantics; WP-185 composes the worker.

## Options Considered

1. **Atomic state plus frontier per projection:** Approved correctness boundary.
2. **Advance frontier separately:** Can expose a frontier ahead of derived state.
3. **Shared global worker cursor:** Conflates independently failing/rebuilding
   projections.
4. **Treat projections as authoritative:** Contradicts rebuildability and commit
   log ownership.

## Consequences

- Storage APIs/tables for projections must be designed before WP-070 freezes
  durable layout.
- Rebuild cost is accepted in the POC; advanced backfill controls are later work.
- Projection waits do not block commit and are bounded/cancellable.
- Every sequence, including one with no relevant event, is accounted for before
  frontier advancement.

## Compatibility

Projection identity/version, derived key encoding, aggregate arithmetic, frontier
record, lifecycle, and query/wait result types require versioned fixtures.

## Security

Projection queries use the shared application service and reauthorize. Derived
rows, group keys, errors, and telemetry apply the same redaction and bounds as
authoritative queries.

## Testing

Reference-prefix comparison, frontier monotonic properties, duplicate apply,
irrelevant-event sequence, gap rejection, atomic failpoints, process kill/reopen,
rebuild equality, typed waits, concurrent reader/frontier tests, and end-to-end
winning-commit visibility.

## Requirements and Work Packages

- **Requirements:** `PRJ-001` through `PRJ-004`, `STO-022`, `REC-001`
- **Defines or blocks:** storage ports in `WP-060`/`WP-070`; `WP-130`, `WP-170`,
  `WP-185`
- **Final evidence:** `WP-190`, `WP-200`

## Decision Deadline

Accept atomic persistence and identity semantics before WP-070. Accept the full
query, lifecycle, and rebuild record before WP-170 implementation.
