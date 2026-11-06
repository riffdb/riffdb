---
adr: "0338"
title: Cold Columnar Projection Retention Advancement and Default Outbox Spooling
status: accepted
tier: guarantee
date: 2026-11-06
accepted: 2026-11-06
acceptance: 'maintainer, in session, 2026-11-06: approved implementation_plan.md (ADR-0338)'
requires: [ADR-0049, ADR-0118, ADR-0124, ADR-0158, ADR-0181, ADR-0195]
amends: [ADR-0181, ADR-0195]
supersedes: []
requirements: [STO-030, STO-031, PRJ-002, PRJ-004, DRV-001, DRV-002, DRV-004, PERF-019]
packages: [WP-907, WP-908]
obligations: []
review_triggers:
  - "Online history retention prunes authoritative commits before a non-detached columnar projection or outbox intent has durably advanced past the prune frontier."
  - "Advancing a cold columnar projection for online retention leaves a materialized ColumnarEngine in process heap without semantic query demand (PERF-019)."
  - "Automatic local outbox spooling drops or acknowledges an outbox intent before its durable spool segment is fsynced on disk."
---
# ADR-0338: Cold Columnar Projection Retention Advancement and Default Outbox Spooling

## Context

Under `ADR-0181`, online history retention (`retained_sequences = N`) is fail-closed behind four durable fences (`RetentionFenceBinding`):
1. `ProjectionDurableFrontier`: the minimum `retention_frontier()` across all rows in `COLUMNAR_PROJECTION_CONTROLS`.
2. `UndeliveredOutboxLowWater`: the lowest commit sequence with an undelivered `OUTBOX` intent.
3. `ChangelogArchiveConsumerLowWater`: the minimum acknowledged sequence across registered replication/archive consumers.
4. `ActiveSnapshotPin`: the minimum active reader/prepared-retention snapshot sequence.

Two interactions caused `retained_sequences = N` to stall indefinitely at `BeforeFirst` (`0`) or at an earlier session's frontier unless operators took manual action (documented in `docs/known-limitations.md#retention`):
- **Demand-activated columnar projections (`ADR-0195`):** Columnar projection slots open in `ColumnarSlotLifecycle::Cold` so `riffdbd` does not materialize published snapshots into process heap before semantic query demand (`PERF-019`). Because `maintain_common_generations` skipped `Cold` slots, unqueried projections never advanced `retention_frontier()` in `COLUMNAR_PROJECTION_CONTROLS`, permanently holding `ProjectionDurableFrontier` unless queried or manually detached (`riffdb retention projection-detach`).
- **Unconfigured outbox workers (`[maintenance.outbox]`):** Every event-emitting command records a `StoredOutboxIntentV1` in `OUTBOX`. When an operator configured `[maintenance.retention]` (`retained_sequences = N`) without configuring `[maintenance.outbox]`, no `RunningLocalOutbox` worker ran, leaving `OUTBOX_STATUS` empty and holding `UndeliveredOutboxLowWater` at the first event-emitting commit forever.

## Decision

1. **Cold-Slot Durable Columnar Retention Advancement Without Heap Activation (`WP-907`):**
   - `ColumnarRuntime` tracks a monotonic `retention_target_frontier: AtomicU64`. When `RunningRetentionLane` observes `OnlineRetentionHold::Fenced(RetentionFenceBinding::ProjectionDurableFrontier)` (or computes an online prune target `head.saturating_sub(retain.get())` above a cold projection's durable frontier), it sets `retention_target_frontier` and wakes `RunningColumnarWorker`.
   - `maintain_common_generations` in `crates/riffdb-server/src/columnar_worker.rs` includes `ColumnarSlotLifecycle::Cold` slots whenever `runtime.retention_catchup_needed(control.retention_frontier())` holds (`control.retention_frontier() < retention_target_frontier`). Under `RetentionTarget::Hold` (`retention_target_frontier == 0`), `Cold` slots remain untouched at startup and after writes (`OBL-0195-1`, `OBL-0195-6`).
   - When `publish_prepared_under_capture_gate_controlled` or `advance_published_v1_under_capture_gate_controlled` publishes a new durable generation and advances `COLUMNAR_PROJECTION_CONTROLS` while the slot state under `capture_gate` is still `ColumnarSlotState::Cold(spec)`, the slot updates `spec` (`directory`, `generation`, `controlled_manifest`, `expected_durable_frontier`, `v2_selection`) in place and drops the transient `ColumnarEngine` immediately instead of transitioning to `ColumnarSlotState::Active`. The slot remains `Cold` with zero retained in-memory snapshot heap while `ProjectionDurableFrontier` advances on disk.
   - If a V1 delta catch-up encounters `ColumnarError::HistoryPruned`, `advance_published_v1` falls back to building a fresh snapshot-backed candidate from `capture_application_export_snapshot`.

2. **Automatic Default Durable-Local Outbox Spooling When Online Retention Is Configured (`WP-908`):**
   - When a primary database (`follower.is_none()`) is configured with `RetentionTarget::RetainSequences(_)` and `outbox` is omitted (`None`), `DatabaseConfig::effective_outbox()` automatically supplies `ConfiguredOutbox::DurableLocal` rooted at `<database_path>.riffoutbox` (`DatabaseConfig::default_outbox_root()`) with a default archive quota of `256 MiB` (`DEFAULT_AUTO_OUTBOX_ARCHIVE_QUOTA_BYTES = 268_435_456`).
   - `ServerConfig::validate_disjoint_paths` reserves `<database_path>.riffoutbox` alongside `.riffreplication` and `.riffreceiver`.
   - `RunningLocalOutbox` drains pending `OUTBOX` intents into durable, checksummed local spool segments under `<database_path>.riffoutbox` and marks them `Delivered` in `OUTBOX_STATUS` only after durable file sync (`DRV-004`), allowing `UndeliveredOutboxLowWater` to advance without manual `[maintenance.outbox]` boilerplate.
   - When `retention` is `RetentionTarget::Hold` (default) and `outbox` is omitted, no automatic outbox worker is started.

## Options considered

1. **Automatically detach (`DetachedByOperator`) cold columnar projections or discard unconfigured outbox intents when online retention runs:** rejected because silently discarding outbox intents violates `DRV-004` and detaching unqueried projections forces offline rebuilds on first query.
2. **Eagerly activate all columnar projections into `ColumnarSlotLifecycle::Active` whenever `RetainSequences` is configured:** rejected because keeping every projection resident in process heap violates `ADR-0195` (`PERF-019`).
3. **Cold-slot durable generation publication with immediate engine drop + automatic `<database_path>.riffoutbox` durable-local spooling under `RetainSequences`:** chosen because it preserves every fail-closed retention fence (`STO-030`, `STO-031`), keeps cold projections unmaterialized in heap (`PERF-019`), and preserves durable outbox delivery (`DRV-004`).

## Consequences

- Online history retention (`retained_sequences = N`) automatically advances past cold columnar projections and drains outbox intents to `<database_path>.riffoutbox` without requiring a prior semantic query or manual `riffdb retention projection-detach`.
- Cold columnar projections remain in `ColumnarSlotLifecycle::Cold` (`cold_slot_count` unchanged, zero retained snapshot heap) until an actual query or warm-up demands activation.
- The `Retention` limitation in `docs/known-limitations.md` is resolved and removed.

## Standing design tests

- **Interface safety:** Fail-closed retention fences (`ProjectionDurableFrontier`, `UndeliveredOutboxLowWater`) remain strictly enforced; no commit is pruned before durable projection publication and durable outbox spool sync.
- **Scale:** Transient catch-up engines for cold slots are dropped immediately after durable publication, bounding steady-state heap by actively queried projections (`PERF-019`).

## Checks

- Unit and integration tests proving cold columnar projections advance `COLUMNAR_PROJECTION_CONTROLS` and unblock online history pruning while remaining `Cold` (`OBL-0338-1`).
- End-to-end server retention test proving `retained_sequences` with unconfigured `[maintenance.outbox]` and cold columnar projections prunes history and spools outbox records under `<database_path>.riffoutbox` (`OBL-0338-2`).
