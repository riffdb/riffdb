---
adr: "0313"
title: Projected group-by canonical ordering, follower ANN graph cache parity, and transactional installer binary swap
status: accepted
tier: guarantee
date: 2026-10-15
accepted: 2026-10-15
acceptance: 'user, in session, 2026-10-15: "Yes" (approve next batch of known-limitations fixes)'
requires: [ADR-0086, ADR-0087, ADR-0091, ADR-0182, ADR-0229, ADR-0312]
amends: []
supersedes: []
requirements: [PRJ-002, PRJ-005, OQ-045, VEC-007, VEC-008, VEC-009, EFF-002, STO-010, DX-002, DX-014]
packages: [WP-868]
obligations: []
review_triggers:
  - Follower ANN graph caching reuses a graph across different LocalView ordinals, frontiers, or policy bindings.
  - Binary rollback deletes or overwrites server configuration, digest keys, or database files.
---
# ADR-0313: Projected group-by canonical ordering, follower ANN graph cache parity, and transactional installer binary swap

## Context

After `ADR-0312` (`WP-865`–`WP-867`), four actionable items remain in
`docs/known-limitations.md`:

1. **Ad-hoc projected `group by` ordering (`docs/known-limitations.md` line 47)**:
   `execute_group_by` in `crates/riffdb-columnar/src/query.rs` accumulates groups
   in a `BTreeMap` keyed by length-prefixed `encode_group_key_into` bytes and
   returns them in map iteration order. For variable-length `String`/`Bytes` and
   signed `I64` keys, encoded-byte order differs from canonical `compare_values`
   order (e.g. `"b"` precedes `"aa"` under length-prefixed bytes).
2. **Follower HNSW (`NearestGraphCache`) parity (`docs/known-limitations.md` line 52)**:
   `FollowerColumnarRuntime` in `crates/riffdb-server/src/columnar_follower.rs`
   passes `None` for the `NearestGraphCache` argument to `vector_view::execute`,
   so follower vector queries above `ann_threshold` never warm or reuse an
   in-memory HNSW graph under `ADR-0229`.
3. **Transactional three-binary replacement in `scripts/release-source-install` (`docs/known-limitations.md` line 24)**:
   `publish_user_binaries` and `publish_system_binaries` replace `riffdbd`,
   `riffdb`, and `riffdb-mcp` sequentially and run `systemd-analyze verify`
   afterward, without a rollback bundle if unit verification or an interrupted
   binary swap fails mid-flight.
4. **Outbox delivery and online retention documentation (`docs/known-limitations.md` lines 29 & 73, `docs/configuration.md` line 293)**:
   `docs/known-limitations.md` and `docs/configuration.md` still state that the
   POC delivers no outbox events and never advances outbox retention
   automatically, whereas `[maintenance.outbox]` / `[databases.<alias>.outbox]`
   (`kind = "durable_local"`, `docs/operations/LOCAL-OUTBOX-ARCHIVE.md`) runs a
   server-owned local archive dispatcher that completes source deliveries and
   advances the outbox retention fence.

## Decision

1. **Canonical `compare_values` group ordering (`WP-868`)**:
   - In `execute_group_by` (`crates/riffdb-columnar/src/query.rs`), sort the
     materialized `(key_cells, aggregates)` groups by element-wise
     `compare_values` over `key_cells` before returning `QueryResult::Groups`.
   - Update `docs/known-limitations.md` and `docs/reference/CLI.md` to reflect
     canonical `compare_values` group ordering across both named RiffQL and the
     ad-hoc projected CLI.

2. **Follower `NearestGraphCache` parity (`WP-868`)**:
   - Attach an `Arc<riffdb_columnar::NearestGraphCache>` to each materialized
     `LocalView` in `crates/riffdb-server/src/columnar_follower.rs`.
   - In `FollowerColumnarRuntime`'s `VectorProjectionPort::execute`
     implementation, capture the `LocalView`'s `ann_cache` and
     `ProjectionGeneration::new(local.ordinal.get())` alongside the
     `ColumnarObservation` under the same inventory lock and pass them to
     `super::vector_view::execute`. When a newer `LocalView` replaces the slot,
     the previous `LocalView` and its `NearestGraphCache` are dropped.

3. **Rollback-safe three-binary replacement (`WP-868`)**:
   - Before replacing `riffdbd`, `riffdb`, and `riffdb-mcp`, record any
     pre-existing installed binaries in a sibling rollback directory
     (`.riffdb-binary-rollback-v1` inside the target `bin` directory) with an
     explicit manifest (`absent-<binary>` and `COMPLETE`) of which binaries
     existed before the swap, and commit/remove `.riffdb-binary-rollback-v1`
     only after all three binary moves and `systemd-analyze verify` succeed.
   - If any binary move or `systemd-analyze verify` fails, or if a prior run was
     interrupted mid-swap, restore all three binaries to their pre-swap state
     before removing the rollback directory.

4. **Outbox retention documentation reconciliation (`WP-868`)**:
   - Update `docs/known-limitations.md` (lines 29 and 73) and
     `docs/configuration.md` (line 293) so they accurately state that emitted
     outbox events hold online retention until delivered to a configured
     `durable_local` outbox archive (`[maintenance.outbox]` or
     `[databases.<alias>.outbox]`), while databases with no configured outbox
     destination retain undelivered events from the first emitted event onward.

## Consequences

- Keeping `BTreeMap<Vec<u8>, ...>` for O(log G) group lookup during scan and
  sorting the bounded output vector (`<= max_group_cardinality`) at the end of
  `execute_group_by` preserves zero-allocation scratch lookup per row while
  guaranteeing canonical value order on output.
- Giving each follower `LocalView` its own `Arc<NearestGraphCache>` bound to
  `local.ordinal` satisfies `ADR-0229`'s exact immutable population binding
  without sharing mutable cache state across view replacements.

## Standing design tests

- **Interface safety:** Follower ANN graph caching runs after full scalar
  predicate and principal-policy admission under `ADR-0229` and is scoped to a
  single immutable `LocalView`. Binary rollback touches only the three product
  binaries in the target `bin` directory.
- **Scale:** Group sorting is bounded by `max_group_cardinality` (at most 10,000
  groups), and follower ANN caching shares the process-wide 64 MiB `ADR-0229`
  budget.
- **Recovery:** An interrupted binary swap in `scripts/release-source-install`
  is rolled back on failure or recovered at the start of the next installer
  invocation before staging new binaries.

## Checks

- `projected_group_by_orders_groups_by_canonical_value_order` in `crates/riffdb-columnar`.
- `follower_vector_projection_warms_and_reuses_ann_graph_cache` in `crates/riffdb-server`.
- `./scripts/release-source-install-smoke` and `./scripts/handbook check`.
