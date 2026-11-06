---
adr: "0335"
title: Two-Phase Checkpoint Commits and Checkpointed Command-Locator Snapshots
status: accepted
tier: guarantee
date: 2026-11-05
accepted: 2026-11-05
acceptance: 'maintainer, in session, 2026-11-05: approved implementation_plan.md (ADR-0335 through ADR-0337)'
requires: [ADR-0102, ADR-0104, ADR-0244, ADR-0263, ADR-0327]
amends: [ADR-0102, ADR-0327]
supersedes: []
requirements: [REC-001, REC-002, REC-005, STO-032, TXN-042]
packages: [WP-899, WP-900]
obligations:
  - id: OBL-0335-1
    package: WP-899
    description: "Two-phase redb checkpoint commits with quick_repair enabled allow dirty-restart recovery to open database.redb without full-file page-scanning repair while preserving hot-path journal commit semantics."
    proof: checkpoint_two_phase_commit_avoids_dirty_restart_engine_repair
  - id: OBL-0335-2
    package: WP-900
    description: "Checkpointed locator.riffsnapshot persistence and startup suffix fold over (C, P] publish LocatorCapability::Complete synchronously at readiness without scanning the historical segment prefix."
    proof: checkpoint_locator_snapshot_fold_publishes_complete_at_startup
  - id: OBL-0335-3
    package: WP-900
    description: "Missing, truncated, CRC-invalid, or lineage-mismatched locator.riffsnapshot files are rejected fail-safe and fall back to background segment-manifest reconstruction."
    proof: corrupt_or_mismatched_locator_snapshot_falls_back_to_segment_rebuild
review_triggers:
  - "A hot-path command commit reintroduces synchronous redb transactions instead of committing through the authoritative journal and overlay."
  - "A corrupt, truncated, or lineage-mismatched locator.riffsnapshot is accepted during startup instead of falling back to segment-manifest reconstruction."
  - "LocatorCapability::Complete is published before folding all uncheckpointed journal transitions in (C, P]."
---
# ADR-0335: Two-Phase Checkpoint Commits and Checkpointed Command-Locator Snapshots

## Context

Under `ADR-0104` and `ADR-0263`, foreground command commits under `Standard` and `Relaxed` durability profiles write exclusively to the preallocated authoritative journal (`journal.riffextent`) and in-memory state overlay; `redb` (`database.redb`) is written only by background journal checkpoints (once per 65,536 transitions or 256 MiB) and rare administrative barriers. However, two legacy behaviors remain in `riffdb-storage-redb`:

1. Background journal checkpoints in `store_journal_checkpoint.rs` and `composite_view.rs` still set `transaction.set_two_phase_commit(false)` (1-phase commit), and `open_redb_database_with_repair_telemetry` disables `quick_repair` because pre-`ADR-0104` `Standard` commits executed 1-phase `redb` transactions on the hot path. When a process terminates abruptly (`SIGKILL`) after any 1-phase `redb` commit, `redb::Database::open` must walk every page in `database.redb` to rebuild regional allocator state, making dirty-restart recovery proportional to total database file size (~53s at 1.1M rows).
2. Under `ADR-0244` and `ADR-0327`, exact command locators (`idempotency`, `provenance`, `audit_seq`, `audit_req`, `event_route`, `pending_outbox`) reside in `PublishedChangelogSnapshotV3.locator_generation`. On `Bounded` startup (`startup.rs`), `LocatorCapability` starts as `Pending` while `spawn_background_history_scrub` scans all retained command segment manifests from the retention watermark to head `P`, delaying command writes and locator-backed reads after restart.

## Decision

1. **Two-Phase `redb` Checkpoint Commits and Quick-Repair Open:**
   - All background journal checkpoint transactions (`materialize_checkpoint_batch_with_locator`, `composite_view.rs`, and administrative checkpoint barriers) MUST use `transaction.set_two_phase_commit(true)`.
   - `open_redb_database_with_repair_telemetry` in `store_open.rs` MUST enable `builder.set_quick_repair(true)` so `redb` maintains persisted two-phase regional allocator state tables and recovers from abrupt process termination in $O(1)$ / $O(\text{allocator tables})$ time without scanning `database.redb` pages.
   - Foreground command commits remain unchanged on the `ADR-0104` journal + overlay path.

2. **Checkpointed Command-Locator Snapshot (`locator.riffsnapshot`):**
   - Whenever a journal checkpoint advances the durable `redb` checkpoint frontier `C` (and on clean close or backup staging), `RedbStore` MUST atomically write a versioned, CRC32C-checksummed sidecar file `locator.riffsnapshot` (via temporary file + `fsync` + atomic rename) in the database directory.
   - `locator.riffsnapshot` binds:
     - `format_version: u32` (`1`),
     - `database_id: String`,
     - `history_incarnation: u64`,
     - `registry_digest: [u8; 32]`,
     - `retention_watermark: u64`,
     - `checkpoint_sequence: u64` (`C`),
     - `checkpoint_frame_hash: [u8; 32]`,
     - Canonical length-prefixed sorted entries of all six `LocatorGeneration` maps (`idempotency`, `provenance`, `audit_seq`, `audit_req`, `event_route`, `pending_outbox`) at `C`,
     - Trailing CRC32C checksum over the complete payload.

3. **Bounded Suffix Fold for Instant Locator Readiness:**
   - During `Bounded` startup (`startup.rs`), after replaying the uncheckpointed journal suffix `(C, P]` into the authoritative overlay:
     - If `locator.riffsnapshot` exists and its header (`database_id`, `history_incarnation`, `registry_digest`, `retention_watermark`, `checkpoint_sequence == C`, `checkpoint_frame_hash`) and CRC32C digest match the verified checkpoint watermark `RedbCheckpointWatermarkV1` at `C`, startup loads `LocatorGeneration` at `C`, folds the `LocatorDeltaBatch` entries extracted from the replayed journal suffix `(C, P]`, and publishes `LocatorCapability::Complete` **before** readiness.
     - If `locator.riffsnapshot` is absent, truncated, checksum-invalid, or header-mismatched, startup logs a structured diagnostic, ignores the sidecar, and falls back to the existing `LocatorCapability::Pending -> Complete` background segment-manifest reconstruction while background history scrub runs as normal.

## Options considered

1. **Reintroduce durable locator B-tree tables inside `database.redb`:** rejected because `ADR-0244` retired durable command-derived locator tables to eliminate write amplification and multi-table B-tree churn during checkpoints.
2. **Keep 1-phase `redb` checkpoint commits:** rejected because background checkpoints occur once per 65,536 transitions off the foreground commit path, so 1-phase commits save negligible background I/O at the cost of $O(\text{database file size})$ `SIGKILL` recovery.
3. **Two-phase `redb` checkpoints plus a checksummed `locator.riffsnapshot` sidecar with `(C, P]` journal suffix fold:** chosen because it keeps foreground writes untouched, eliminates full-file `redb` repair on dirty restart, and makes `LocatorCapability::Complete` immediate at startup readiness.

## Consequences

- Abrupt process termination (`SIGKILL`) no longer triggers a full-file `database.redb` page scan on reopen; dirty-restart time is bounded by the uncheckpointed journal suffix `(C, P]`.
- `LocatorCapability` is immediately `Complete` at startup readiness whenever a valid `locator.riffsnapshot` is present at `C`.
- Background history scrub (`REC-007`) continues to verify retained history and structural integrity asynchronously after readiness.

## Standing design tests

- **Interface safety:** No public flag or caller option weakens recovery validation; a mismatched or corrupted `locator.riffsnapshot` never admits stale or cross-incarnation locators and falls back fail-closed to authoritative segment reconstruction.
- **Scale:** Startup locator reconstruction processes at most the checkpointed snapshot plus the bounded uncheckpointed suffix ($\le 65,536$ transitions / $256\text{ MiB}$).

## Checks

- Process-kill (`SIGKILL`) dirty-restart test verifying bounded reopen without full-file page scan and immediate `LocatorCapability::Complete` at readiness.
- Corruption/truncation/lineage-mismatch tests on `locator.riffsnapshot` verifying safe fallback to background reconstruction and eventual `LocatorCapability::Complete`.
