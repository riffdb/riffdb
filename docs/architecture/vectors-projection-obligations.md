# Vector Projection — ADR-0086 Obligation Map

**Work package:** WP-593
**Status:** Structural proof that the vector projection inherits every unamended
ADR-0086 obligation through the existing projection plane machinery.

## Inherited by construction

The vector projection uses the **identical** projection plane infrastructure:

| ADR-0086 Obligation | Mechanism | Test location |
|---|---|---|
| Derived and non-authoritative | Same `ProjectionController` lifecycle | `crates/riffdb-projection/src/control.rs` |
| Org-partitioned segments | Same partition_by contract semantics | Entity key includes org; nearest() requires org scope (VEC-006) |
| Frontier-reporting | Same `FrontierPosition` on every query result | `crates/riffdb-projection/src/query.rs` |
| Monotonic frontier (PRJ-001) | Same publish-path validation | `crates/riffdb-projection/src/control.rs` |
| Freshness (Causal/Bounded/Available) | Same `after_sequence` + wait timeout | `crates/riffdb-projection/src/query.rs` |
| Snapshot-rebuildable | Vectors are authoritative entity state; rebuild scans current state | ADR-0091 §2 |
| Replay-budget detach | Same hard-limit → Degraded transition | `crates/riffdb-projection/src/control.rs` |
| Idempotent apply (PRJ-003) | Same (identity, generation, sequence, hash) duplicate detection | `crates/riffdb-projection/src/evaluator.rs` |

## New (vector-specific)

| Property | Implementation |
|---|---|
| Exact KNN | `crates/riffdb-columnar/src/nearest.rs::exact_knn()` |
| Policy before ranking (VEC-007) | Candidates are pre-filtered; adversarial test proves no leakage |
| Per-org statistics (VEC-008) | Exact scan is org-scoped; no shared statistics exist in exact tier |
| Frontier unchanged (VEC-011) | Uses same FrontierPosition; no weakening |
| Mandatory K and org scope (VEC-006) | `QueryAccessKind::Nearest` validation: k > 0, non-empty |

## What this means

**No new projection-plane code is needed for VEC-005/VEC-011.** The vector
projection is a new *consumer* of the same lifecycle, frontier, and rebuild
machinery. Its novelty is in the query execution path (nearest instead of
group-key scan), which is handled by the columnar nearest module.
