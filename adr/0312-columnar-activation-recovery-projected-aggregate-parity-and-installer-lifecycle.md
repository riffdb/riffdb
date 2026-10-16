---
adr: "0312"
title: Columnar Activation Recovery, Projected Aggregate Parity, and Installer Lifecycle
status: accepted
tier: guarantee
date: 2026-10-15
accepted: 2026-10-15
acceptance: 'maintainer, in session, 2026-10-15: "okay, lets start with the first 5"'
requires: [ADR-0085, ADR-0086, ADR-0087, ADR-0092, ADR-0152, ADR-0195]
amends: [ADR-0087, ADR-0195]
supersedes: []
requirements: [PRJ-002, PRJ-004, PRJ-008, PRJ-009, OQ-019, OQ-020, OQ-022, OQ-045, OQ-046, OQ-047, OQ-048, STO-010, STO-012, DX-002, DX-014]
packages: [WP-865, WP-866, WP-867]
obligations: []
review_triggers:
  - Columnar startup opens or decodes projection artifacts before semantic demand or violates PERF-019 cold-source startup bounds.
  - Ad-hoc projected aggregate wire decoding accepts mismatched descriptor/value arms or unbounded distinct state.
  - Installer uninstall deletes authoritative database files, backups, digest keys, or operator credentials.
  - Codex MCP registration runs without an exclusive lockfile or overwrites an incompatible existing server entry.
---
# ADR-0312: Columnar Activation Recovery, Projected Aggregate Parity, and Installer Lifecycle

## Context

Five remaining entries in `docs/known-limitations.md` can be resolved or reconciled cleanly:

1. **Columnar activation failure recovery (`docs/known-limitations.md:55`)**: While `ADR-0195` requires configured columnar projections to remain cold at startup until first semantic demand (`PERF-019`), `activate_requested_slot_controlled` in `crates/riffdb-server/src/columnar_worker.rs` transitions a slot to `ColumnarSlotState::Failed` even when `current_activation_spec` returns a transient `ColumnarPortError::Unavailable` storage read failure before any artifact is opened. When `control.servable_generation()` is `Some`, `maintain_common_generations` skips a `Failed` slot unless `servable_generation()` is `None`, wedging the slot for the process generation on a transient control read failure. Conversely, when a published artifact fails validation, `record_selected_open_failure` and `allocate_unservable_rebuild_candidate` already rebuild and promote a replacement generation within the same process generation.
2. **Online retention & startup checkpoint / cold projection fencing (`docs/known-limitations.md:74-75`)**: `docs/known-limitations.md` still states that the first online retention step forces a full-population startup scan on the next restart, whereas `Completed` registry bindings preserve bounded startup and `RedbOnlineRetentionLane::refresh_validated_prefix` (`crates/riffdb-storage-redb/src/retention/online/prefix_refresh.rs`) refreshes the optional validated-prefix checkpoint off-gate (`docs/operations/STARTUP-INTEGRITY.md`). In addition, operators can unpin an unqueried projection fence via audited `riffdb retention projection-detach`.
3. **Ad-hoc projected query aggregate parity (`docs/known-limitations.md:47`)**: `crates/riffdb-columnar` already implements the complete `BOUNDED_EXACT_CORE` aggregate set (`Count`, `CountPresent`, `CountDistinct`, `CountDistinctPresent`, `Sum`, `Mean`, `Min`, `Max`, `Any`, `All`), and `numeric_as_i128` can also fold fixed-scale `Decimal` and `Money` coefficients when scale/currency is uniform across the column, yet `ProjectedAggregateOp` in `riffdb.app.v1`, `riffdb-service`, `riffdb-api-grpc`, `riffdb-client-rust`, and `riffdb-cli` exposes only `count`, `sum`, `min`, and `max`.
4. **Non-atomic Codex MCP registration (`docs/known-limitations.md:25`)**: `scripts/release-source-bootstrap --register-codex` inspects `codex mcp list --json` and invokes `codex mcp add` without file locking, racing if two bootstrap processes register concurrently, and fails if the exact target entry is already registered.
5. **No source convenience uninstall mode (`docs/known-limitations.md:23`)**: `cargo riffdb` provides `install` and `bootstrap` but no `uninstall` command to stop/disable the unit and remove installed binaries and unit files while preserving authoritative data, backups, digest keys, and credentials.

## Decision

1. **Columnar Transient Activation Retry & Retention Continuity (`WP-865`)**:
   - In `crates/riffdb-server/src/columnar_worker.rs`, when `runtime.current_activation_spec(name, slot)` fails with `ColumnarPortError::Unavailable`, keep the slot in `ColumnarSlotState::Activating` so the worker retries activation on the next pass rather than wedging in `Failed`. Only `ColumnarPortError::Integrity` transitions the slot to `Failed` before artifact open.
   - Update `docs/known-limitations.md` to reflect transient activation retry and automatic candidate rebuild recovery on corrupt published artifacts, audited `riffdb retention projection-detach` for unqueried projections, and bounded restart preservation + background `refresh_validated_prefix` after online retention.
2. **Ad-Hoc Projected Query Aggregate Parity (`WP-866`)**:
   - Extend `ProjectedAggregateOp` in `proto/riffdb/app/v1/application.proto`, `crates/riffdb-service/src/projected_query.rs`, `crates/riffdb-api-grpc/src/projected_query_conversion.rs`, `crates/riffdb-client-rust/src/projected.rs`, and `crates/riffdb-cli` (`fixtures/compatibility/operation-adapters-v1/riffdb-cli/app.rs.in`) with `Mean`, `Any`, `All`, `CountPresent`, `CountDistinct`, and `CountDistinctPresent`, and add `ProjectedAggregateExactMean` (`total`, `count`) and `boolean` arms to `ProjectedAggregateValue`.
   - Extend `crates/riffdb-columnar` `Accumulator` / `numeric_as_i128` to support `Decimal` and `Money` columns (requiring uniform scale/currency across contributing rows) in `Sum` and `Mean` alongside `I64` and `U64`.
3. **Locked Idempotent Codex MCP Registration & Source Convenience Uninstall (`WP-867`)**:
   - In `scripts/release-source-bootstrap`, guard `--register-codex` with an exclusive `flock` on a private user-owned lockfile (`$config_dir/.codex-mcp.lock`), treat an existing Codex MCP entry with the identical command/arguments as an idempotent no-op success, and refuse conflicting existing entries.
   - Add `scripts/release-source-uninstall` and wire `cargo riffdb uninstall (--user|--system) [--no-stop]` (and `cargo riffdb install (--user|--system) --uninstall [--no-stop]`) in `xtask/src/main.rs` and `scripts/release-source-install` to stop/disable `riffdbd.service` and remove installed binaries and systemd unit/sysusers/tmpfiles files while preserving database, backup, digest-key, configuration, and credential directories.

## Options considered

1. **Eagerly activate all columnar projections before server readiness:** rejected; violates `ADR-0195` (`OBL-0195-1`, `OBL-0195-6`) and `PERF-019` startup heap bounds.
2. **Have `cargo riffdb uninstall` delete database, backup, and key directories:** rejected; violates `AGENTS.md` boundary 11 by exposing an irreversible data/key destruction path on convenience CLI invocation.
3. **Preserve cold startup while retrying transient activation errors, bring projected aggregates to RiffQL parity, and add locked Codex registration plus data-preserving uninstall:** selected.

## Consequences

- Transient storage unavailability during columnar activation no longer wedges a projection slot for the process lifetime, while corrupt artifacts continue to rebuild automatically.
- Ad-hoc projected queries (`riffdb query projected`) support the full bounded exact aggregate vocabulary (`count`, `count_present`, `count_distinct`, `count_distinct_present`, `sum`, `mean`, `min`, `max`, `any`, `all`) including `Decimal`/`Money` sums and means.
- Concurrent `cargo riffdb bootstrap --register-codex` runs serialize safely via `flock`, and `cargo riffdb uninstall` cleanly removes binaries and units without risking data or keys.

## Standing design tests

- **Interface safety:** Columnar activation remains server-owned and fail-closed on corruption. Projected aggregate wire conversion cross-checks every response cell arm against its request descriptor. `cargo riffdb uninstall` never deletes databases, backups, keys, or credentials.
- **Scale:** Distinct projected aggregates remain capped by `MAX_AGGREGATE_DISTINCT_VALUES_V1` (256) and `MAX_AGGREGATE_STATE_BYTES_V1` (1 MiB).

## Checks

- `transient_control_unavailability_during_activation_retries_and_recovers_on_next_pass` in `crates/riffdb-server`.
- `projected_query_aggregates_support_mean_any_all_distinct_and_decimal_money_sums` in `crates/riffdb-service` and `crates/riffdb-api-grpc`.
- `./scripts/release-source-bootstrap-smoke` and `./scripts/release-source-install-smoke`.
