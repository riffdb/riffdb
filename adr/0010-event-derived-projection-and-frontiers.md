# ADR-0010: Event-Derived Projection and Frontier Semantics

- **Status:** Accepted
- **Direction approved:** 2026-07-12
- **Exact text accepted:** 2026-07-13, amended 2026-07-13
- **Amended by:** ADR-0017 for exact projection identity, generations, group/apply
  keys, apply-hash equality, frontier position, lifecycle, query, and rebuild
- **Decision deadline:** Persistence semantics before WP-070; full record before WP-170

The human maintainer accepted this exact text and the companion ADR-0017
amendment below on 2026-07-13.

## Context

Projection state is derived and rebuildable, while the commit log and entity
state are authoritative. Consistency claims require a durable frontier that never
gets ahead of applied state, never skips a commit, and survives crashes without
double application.

## Decision

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

### ADR-0017 companion amendment

The projection identity referenced above is exactly contract lineage, nonzero
`ProjectionId`, and `ProjectionPlanHash`; it excludes contract version and bundle
hash. Each identity owns nonzero, never-reused `ProjectionGeneration` values and
an explicit `FrontierPosition::{BeforeFirst, AppliedThrough(nonzero
CommitSequence)}`. A same-plan rebuild allocates a disjoint generation; a changed
plan hash creates a disjoint identity. Neither operation lowers an existing
published frontier.

The earlier illustrative `(projection_id, N)` marker is replaced exactly by
`(ProjectionIdentity, ProjectionGeneration, CommitSequence)` plus equality of
the canonical `ProjectionApplyHash`. Every applied sequence, including an
irrelevant one, has a marker. Row post-images, marker, and the matching frontier
advance commit atomically. Equal historical retry is a no-op only when the exact
marker hash matches.

Control owns highest allocated generation, optional published and candidate
generation/frontier, published apply mode, the closed lifecycle, and optional
closed failure. Initial build and rebuild write candidate rows in an unpublished
namespace and publish only in one transaction after reaching the
transaction-current authoritative head. Replaced generations, including failed
candidates once explicitly replaced during recovery, are retired and inert: they
may remain durable but are never queried, resumed, reused, or accepted as apply
targets. A failed candidate still retained by `Degraded` control is suspended and
inert but not yet retired.

Queries and status use the closed ADR-0017 lifecycle mapping and
`FrontierPosition`. A known identity with no control is building at
`BeforeFirst`; `Rebuilding` exposes no retained published rows. One storage read
transaction returns control, selected published rows, frontier, and the lower
continuation. Any identity, generation, prefix, or frontier change invalidates a
continuation and returns no rows. These rules supersede the earlier shorthand
that state is simply discarded on rebuild; authoritative state remains
unchanged, while derived generations follow the reviewed lifecycle.

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
- **Defines or blocks:** projection schema in `WP-040`; storage ports in
  `WP-060`; durable schema in `WP-065`; `WP-070`, `WP-075`, `WP-120`; public
  schema in `WP-127`; `WP-130`, `WP-140`, `WP-170`, and `WP-185`
- **Final evidence:** `WP-190`, `WP-200`

## Decision Deadline

Accept atomic persistence and identity semantics before WP-070. Accept the full
query, lifecycle, and rebuild record before WP-170 implementation.
