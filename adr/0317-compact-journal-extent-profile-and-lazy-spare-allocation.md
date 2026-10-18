---
adr: "0317"
title: Compact Journal Extent Profile and Lazy Spare Allocation
status: accepted
tier: guarantee
date: 2026-10-18
accepted: 2026-10-18
acceptance: 'user, in session, 2026-10-18: "I accept"'
requires: [ADR-0103, ADR-0157, ADR-0263, ADR-0271, ADR-0316]
amends: [ADR-0263, ADR-0316]
supersedes: []
requirements: [STO-032, REC-001, REC-002, REC-004, PERF-019]
packages: [WP-877]
obligations:
  - id: OBL-0317-1
    package: WP-877
    summary: Compact journal extent profile (8 MiB) enforces 4 MiB checkpoint thresholds and transitions losslessly to and from standard extents across restarts without breaking history stubs
    proof: compact_extent_profile_rotates_recovers_and_transitions_capacity_without_losing_history_stubs
  - id: OBL-0317-2
    package: WP-877
    summary: Compact journal extent profile defers spare extent creation on initial low-volume writes and prepares the spare at 25% utilization or on-demand checkpoint rotation
    proof: compact_extent_profile_defers_spare_until_utilization_threshold_or_checkpoint_rotation
review_triggers:
  - Changing extent_profile drops uncheckpointed suffix frames or invalidates sealed history-stub locators.
  - Lazy spare allocation permits a checkpoint rotation to proceed without a validated, generation-monotonic replacement active extent.
---
# ADR-0317: Compact Journal Extent Profile and Lazy Spare Allocation

## Context

Under `ADR-0263` Decision 7, every production database allocates an active `.riffjournal` extent of `PRODUCTION_EXTENT_DATA_BYTES = 320 MiB` (`320 MiB + 8 KiB` header) and, on the first writer initialization, immediately pre-allocates and zero-fills a second `.riffjournal.spare` extent of `320 MiB`. Consequently, a newly created or low-volume database consumes **640 MiB** of disk space and performs **640 MiB** of synchronous/background zero-fill I/O upon its first write.

For multi-database developer hosting (`[databases.<alias>]` under `ADR-0271` and `[databases.<alias>.limits]` under `ADR-0316`), pre-allocating `640 MiB` per database prevents packing hundreds of small developer or preview databases onto a shared NVMe volume and makes `max_storage_bytes` ceilings below `1 GiB` impractical. Meanwhile, `ExtentHeader` already stamps each extent's exact `data_bytes` capacity per file, and `normalize_active_capacity_to` already supports lossless capacity transitions at startup by either replacing an empty unanchored extent or sealing and retiring an extent that retains materialized history frames (`StubLocator`).

## Decision

1. **Compact Journal Extent Profile (`crates/riffdb-storage-redb`, `crates/riffdb-server`)**:
   - Introduce `JournalExtentProfile` (`Standard` | `Compact`, default `Standard`) in `riffdb-storage-redb` and expose optional `extent_profile = "standard" | "compact"` in `[maintenance.limits]` and `[databases.<alias>.limits]`.
   - `JournalExtentProfile::Standard` preserves existing behavior unchanged (`PRODUCTION_EXTENT_DATA_BYTES = 320 MiB` in release builds, `LEGACY_EXTENT_DATA_BYTES = 40 MiB` in test builds, `MAX_JOURNAL_FRAME_BYTES = 16 MiB`, and eager spare preparation on writer initialization).
   - `JournalExtentProfile::Compact` sets extent data capacity to `COMPACT_EXTENT_DATA_BYTES = 8 * 1024 * 1024` (**8 MiB**, file size `8,396,800` bytes with the 8 KiB dual-slot header), sets the per-frame physical ceiling to `COMPACT_MAX_JOURNAL_FRAME_BYTES = 4 * 1024 * 1024` (**4 MiB**), and triggers asynchronous checkpoint rotation when physical extent utilization reaches `COMPACT_EXTENT_DATA_BYTES / 2` (**4 MiB**).
   - Register `COMPACT_EXTENT_DATA_BYTES` in `known_extent_data_bytes` alongside `LEGACY_EXTENT_DATA_BYTES` and `PRODUCTION_EXTENT_DATA_BYTES`. When a database reopens under a changed `extent_profile` (`compact` $\leftrightarrow$ `standard`), startup recovery runs `normalize_active_capacity_to( configured_data_bytes )`: if the recovered active extent holds no materialized history (`anchor_position == EXTENT_DATA_OFFSET`), it is atomically replaced at `generation + 1` with the newly configured capacity; if it retains materialized history frames before the anchor, it is sealed to `.ext.<gen>` (retaining its stamped `data_bytes` for all `StubLocator` reads) and a new active extent is created at `generation + 1` with the newly configured capacity.

2. **Lazy Spare Extent Allocation in Compact Profile (`crates/riffdb-storage-redb`)**:
   - In `JournalExtentProfile::Compact`, `initialize_journal_runtime(JournalRuntimePurpose::Writer)` does **not** spawn `.riffjournal.spare` preparation when the active extent's physical utilization is below 25% (`physical_bytes < COMPACT_EXTENT_DATA_BYTES / 4`, i.e., `< 2 MiB`) and no existing `.riffjournal.spare` is present.
   - During foreground frame admission on a `Compact` store, once the active extent reaches `>= 25%` physical utilization (`>= 2 MiB`) or `suffix_transitions >= JOURNAL_CHECKPOINT_START_TRANSITIONS / 4`, the store triggers background `ensure_journal_spare` off the writer gate so the spare is ready before the 50% (`4 MiB`) checkpoint threshold.
   - If a checkpoint rotation (`start_async_checkpoint`) is triggered before `.riffjournal.spare` exists on disk (for example, by a direct administrative write or explicit checkpoint while utilization is `< 2 MiB`), `start_async_checkpoint` joins any in-flight sealer and prepares `.riffjournal.spare` on demand with the store's configured `extent_data_bytes` before rotating `active -> checkpoint` and `spare -> active`.

## Standing design tests

- **Interface safety:** Durability, crash recovery, generation monotonicity, and `StubLocator` history resolution are identical between `standard` and `compact` extent profiles; switching `extent_profile` across restarts is lossless and fail-closed.
- **Scale:** `compact` databases allocate a single `8 MiB` active journal extent on initial writes instead of `640 MiB` (`2 x 320 MiB`), an **80x** reduction in initial journal disk footprint and zero-fill I/O.

## Checks

- `cargo test -p riffdb-storage-redb compact_extent_profile_rotates_recovers_and_transitions_capacity_without_losing_history_stubs`
- `cargo test -p riffdb-storage-redb compact_extent_profile_defers_spare_until_utilization_threshold_or_checkpoint_rotation`
