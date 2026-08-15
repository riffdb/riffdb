# Vectors Pillar — Projection Plane Recon Map

**Work package:** WP-590
**ADR:** ADR-0091 (as amended 2026-08-10)
**Projection-plane ADR:** ADR-0086

This document maps the existing projection-plane infrastructure that the vectors
ladder (WP-591..WP-595) builds on, identifying the file:line locations of each
ADR-0086 obligation transfer point.

## 1. Projection identity, lifecycle, and control

The vector projection is a new projection kind. It reuses the existing:

| Obligation | Location | Notes |
|---|---|---|
| Projection identity tuple | `crates/riffdb-types/src/projection.rs` — `ProjectionIdentity` | (lineage, stable ID, plan hash); unchanged for vectors |
| Lifecycle state machine | `crates/riffdb-projection/src/control.rs:28` — `ProjectionController` | Building → CatchingUp → Ready / Degraded / Invalid |
| Generation allocation | `crates/riffdb-storage-api/src/projection.rs` — `ProjectionGeneration` | Never-reused; rebuild uses disjoint generation |
| Control record | `crates/riffdb-storage-api/src/projection.rs` — `StoredProjectionControlV1` | Carries lifecycle, frontier, failure, apply mode |

## 2. Frontier and consumption

| Obligation | Location | Notes |
|---|---|---|
| Frontier position | `crates/riffdb-types/src/frontier.rs` — `FrontierPosition` | `BeforeFirst` or `AppliedThrough(CommitSequence)` |
| Monotonic frontier (PRJ-001) | `crates/riffdb-projection/src/control.rs` — publish path | Never decreases; asserted in control transitions |
| Contiguous apply (PRJ-002) | `crates/riffdb-projection/src/evaluator.rs` — apply loop | Scans strictly increasing commit sequence |
| Apply marker + hash | `crates/riffdb-projection/src/evaluator.rs` — `ProjectionApplyHash` | Atomic: rows + marker + frontier advance |
| Idempotent replay (PRJ-003) | `crates/riffdb-projection/src/evaluator.rs` — duplicate detection | (identity, generation, sequence, hash) |
| Frontier-reporting queries | `crates/riffdb-projection/src/query.rs:28` — `ProjectionQueryResult::Ready { frontier }` | Every response carries frontier |

**Vectors obligation:** VEC-011 requires this frontier contract unchanged. The
vector projection's apply must be the same atomic (rows + marker + frontier)
pattern; a committed embedding acknowledged to the client must be reflected at
the reported frontier.

## 3. Organization partitioning

| Obligation | Location | Notes |
|---|---|---|
| Contract-level `partition_by` | `crates/riffdb-contract-ir/src/entity.rs` | Entity declaration; org partition is a contract language construct |
| Query-time org scope enforcement | `crates/riffdb-query-executor/src/lib.rs` | All queries require org scope in the plan |
| Projection group keys carry org | `crates/riffdb-projection/src/evaluator.rs` — group key prefix | Keys are `0x47 0x01` + identity + generation + group values |

**Vectors obligation:** VEC-008 requires per-organization index statistics.
WP-594 constructs its deterministic HNSW graph only from the single requested
organization after scalar predicates and principal admission. The graph entry
point, links, and reported statistics therefore have no cross-organization or
denied-row input. `nearest()` retains the mandatory org scope (VEC-006).

## 4. Freshness / read-after-commit

| Obligation | Location | Notes |
|---|---|---|
| Read-after-commit with `after_sequence` | `crates/riffdb-projection/src/query.rs:159` | Waits for frontier ≥ required |
| Wait timeout | `crates/riffdb-projection/src/query.rs` — `WaitTimedOut` variant | Typed response, never silent stale |
| Degraded / Invalid typed responses | `crates/riffdb-projection/src/query.rs` — enum variants | Lifecycle prevents silent stale results |

**Vectors obligation:** VEC-005 requires the same freshness service for vector
queries. nearest() results carry the frontier; `after_sequence` support ensures
causal reads.

## 5. Snapshot rebuild

| Obligation | Location | Notes |
|---|---|---|
| Rebuild from authoritative state | `crates/riffdb-projection/src/recovery.rs` | Generation swap; rebuild scans full commit log |
| Candidate → publish gate | `crates/riffdb-projection/src/control.rs` — publish validation | Only when candidate frontier = current head |
| PRJ-004 (rebuildable) | SPEC.md §15.2 | "Projection state MUST be rebuildable from the authoritative commit log" |

**Vectors obligation:** VEC-005 inherits snapshot-rebuildable. Since embeddings
are authoritative entity state (not derived by an outbox worker), the vector
projection rebuilds by scanning current entity state with its stored
embeddings — simpler than the original ADR-0091 design.

## 6. Replay budget

| Obligation | Location | Notes |
|---|---|---|
| Budget enforcement | `crates/riffdb-projection/src/evaluator.rs` — hard limits | MAX_PROJECTION_ROW_UPDATES, content ceilings |
| Detach on budget breach | `crates/riffdb-projection/src/control.rs` — Degraded transition | Stuck index degrades rather than blocking |

**Vectors obligation:** VEC-005 requires replay-budget detachment. The vector
projection must degrade on budget breach rather than blocking the commit path.

## 7. Authorization transfer (policy before ranking)

| Obligation | Location | Notes |
|---|---|---|
| Row-level policy evaluation | `crates/riffdb-policy/src/` | Principal capabilities checked per request |
| Service-layer authorization | `crates/riffdb-service/src/` — pre-query auth | Authorization happens before storage access |
| Projection query policy point | `crates/riffdb-projection/src/query.rs:20` | "between service policy safe points" |

**Vectors obligation:** VEC-007 strengthens this: row-level policy must apply
BEFORE distance computation and ranking, not just before result return. This
means the vector projection's exact scan (and later ANN traversal) must receive
only policy-visible rows as candidates. This is a tighter integration point than
existing aggregate projections where policy filters results post-aggregation.

## 8. Outbox machinery (NOT reused)

Per ADR-0091 Amendment 1, the embedding loop is removed. The outbox
(`crates/riffdb-outbox/`) is NOT part of the vectors path. Embeddings arrive
through normal typed commands, not outbox intents.

## 9. Query grammar extension point

| Obligation | Location | Notes |
|---|---|---|
| Grammar extension | `crates/riffdb-riffql-syntax/src/` | The RiffQL query parser (contract-syntax parses contracts, not queries) |
| Query plan compilation | `crates/riffdb-query-compiler/` + `crates/riffdb-query-ir/` | Plan generation from parsed queries (no riffdb-query-planner crate exists) |
| Budget enforcement | ADR-0087 surface | Nearest-neighbor falls under ad-hoc grammar governance |

**Vectors obligation:** VEC-006 adds `nearest(field, $vector, k)` to this
surface with mandatory K and org scope.

## 10. Requirement-to-package assignment summary

| Requirement | Primary WP | Evidence WP |
|---|---|---|
| VEC-001 (contract declaration) | WP-591 | — |
| VEC-002 (typed embedding write) | WP-592 | — |
| VEC-003 (staleness tracking) | WP-592 | WP-595 |
| VEC-004 (staleness SLO health) | WP-592 | — |
| VEC-005 (0086 obligations) | WP-593 | WP-595 |
| VEC-006 (nearest grammar) | WP-593 | — |
| VEC-007 (policy before ranking) | WP-593 | WP-595 |
| VEC-008 (per-org statistics) | WP-594 | WP-595 |
| VEC-009 (exact default + threshold) | WP-593, WP-594 | — |
| VEC-010 (recall contract) | WP-594 | WP-595 |
| VEC-011 (frontier unchanged) | WP-593 | WP-595 |
| VEC-012 (model version observability) | WP-592 | — |

## 11. Human-review decisions recorded

1. **ANN structure (WP-594):** Exact-only first at WP-593; first-party HNSW at
   WP-594. No external dependency. `#![forbid(unsafe_code)]` preserved.

2. **Embedding boundary (WP-592):** Client-supplied embeddings via typed
   command. No database-owned loop, no model endpoint configuration, no
   credential storage. ADR-0091 amended accordingly.
