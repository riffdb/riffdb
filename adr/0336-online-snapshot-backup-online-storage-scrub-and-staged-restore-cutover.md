---
adr: "0336"
title: Online Snapshot Backup Online Storage Scrub and Staged Restore Cutover
status: accepted
tier: guarantee
date: 2026-11-05
accepted: 2026-11-05
acceptance: 'maintainer, in session, 2026-11-05: approved implementation_plan.md (ADR-0335 through ADR-0337)'
requires: [ADR-0050, ADR-0178, ADR-0203, ADR-0263, ADR-0335]
amends: [ADR-0050, ADR-0203]
supersedes: []
requirements: [REC-004, REC-007, REC-008, REP-007, STO-032]
packages: [WP-901, WP-902, WP-903]
obligations: []
review_triggers:
  - "Online backup create or storage scrub blocks foreground command admission or closes the live RedbStore."
  - "A backup bundle is published before strict physical and logical verification succeeds on the staged directory."
  - "Backup restore swaps the active data directory before staging, archive suffix replay, strict verification, and staged bearer authorization complete."
---
# ADR-0336: Online Snapshot Backup Online Storage Scrub and Staged Restore Cutover

## Context

`ADR-0050` and `ADR-0203` implemented `riffdb backup create`, `riffdb backup restore`, and `riffdb storage scrub` through an exclusive offline maintenance lifecycle (`Running -> Draining -> OfflineMaintenance -> Starting -> Ready`) that stops gRPC and MCP admission, drains all in-flight work, closes `RedbStore`, executes the maintenance task offline, and reopens the database.

However, `riffdb-storage-redb` now provides two online snapshot primitives:
1. `begin_replication_bootstrap_v3` (`bootstrap_source.rs`) pins one `PublishedChangelogSnapshotV3` (`AuthoritativeStateCursorV3`) and streams a consistent point-in-time snapshot without holding the writer gate.
2. `RedbBackgroundScrubSession` (`background_history.rs`) pins a `redb::ReadTransaction` and an extent `ScrubPin` to validate retained history, structural pages, and catalog history while the writer remains online.

Stopping the process for full backup creation, storage scrub, and the staging/validation phases of restore causes avoidable service downtime.

## Decision

1. **Online Full Snapshot Backup (`riffdb backup create`):**
   - When `StartOfflineBackupCreate` is invoked on a `Ready` database, `riffdbd` MUST execute the backup online without stopping transport admission or closing `RedbStore`:
     1. Acquire the per-database maintenance mutex (preventing concurrent backup/restore/scrub mutations on the same database while keeping ordinary command/query admission `Ready`).
     2. Pin the current `PublishedChangelogSnapshotV3` and an extent `ScrubPin` (or materialize a consistent snapshot image in `<data-dir>/.maintenance/backup-stage-<id>` using the pinned `AuthoritativeStateCursorV3` / checkpoint barrier).
     3. Run full strict offline verification (`verify_existing_store_strict`) and write `manifest.riffdb` and `locator.riffsnapshot` on the staged backup directory while live reads and writes continue uninterrupted.
     4. Atomically publish `<backup-root>/<backup-name>` and record the `CompletedMaintenanceJob`.

2. **Online Storage Scrub (`riffdb storage scrub`):**
   - When `StartOfflineStorageScrub` is invoked on a `Ready` database, `riffdbd` MUST execute the scrub online while remaining `Ready`:
     1. Acquire the per-database maintenance mutex while keeping client admission `Ready`.
     2. Materialize or open a consistent pinned snapshot image in `<data-dir>/.riffdb-offline-verification-v1` (pinned via `PublishedChangelogSnapshotV3` + `ScrubPin`) and execute the complete exact-end physical B-tree (`check_integrity`) and logical/semantic scrub (`verify_existing_store_strict` / `RedbBackgroundScrubSession`) against that pinned snapshot.
     3. Return the versioned `StorageScrubReceiptV1` bound to the scrubbed snapshot's `(history_incarnation, commit_sequence)`. If corruption is detected, transition storage health to `Failed` with an incident ID (`REC-007`, `REC-008`).

3. **Online Restore Staging and Sub-Second Atomic Cutover (`riffdb backup restore`):**
   - When `StartOfflineBackupRestore` is invoked on a `Ready` database, `riffdbd` MUST perform all staging and verification **while the active database remains `Ready` and serving traffic**:
     1. Copy the source backup into the private staging directory, replay any configured incremental archive suffix (`REP-007`), run strict physical and logical verification (`verify_existing_store_strict`), persist `locator.riffsnapshot` (`ADR-0335`), and verify caller authorization against the staged bearer authenticator (`build_restore_stage_bearer_authenticator`) **before** draining the live database.
     2. Only after staging, verification, and authorization succeed does `riffdbd` enter a brief cutover barrier: drain in-flight requests, close the live `RedbStore`, perform the atomic filesystem rename swap between `<data-dir>` and the verified stage directory, reopen in `Bounded` mode with the pre-built `locator.riffsnapshot`, and return to `Ready`.
   - Unhealthy/unmountable startup recovery (`FailedStartup` restore mode) continues to restore directly into the unmounted directory.

## Options considered

1. **Keep full backup, scrub, and restore staging offline (`Draining -> OfflineMaintenance`):** rejected because taking the database offline during routine backups and scrubs violates high-availability expectations.
2. **Copy live `database.redb` and `journal.riffextent` files directly without snapshot pinning:** rejected because concurrent checkpoints and journal writes can tear unpinned file copies.
3. **Materialize and strictly verify backups, scrubs, and restore candidates from pinned `PublishedChangelogSnapshotV3` snapshots online, limiting restore downtime to the final atomic swap:** chosen because it preserves byte-exact verification guarantees with zero downtime for backup/scrub and sub-second cutover for restore.

## Consequences

- `riffdb backup create` and `riffdb storage scrub` execute with zero request downtime on a `Ready` server.
- `riffdb backup restore` performs all I/O-intensive copy, archive replay, and full scrub work online before a brief atomic directory swap and bounded reopen.
- The existing gRPC/CLI maintenance polling contracts (`StartOfflineBackupCreate`, `StartOfflineStorageScrub`, `StartOfflineBackupRestore`, `GetMaintenanceStatus`) remain wire-compatible.

## Standing design tests

- **Interface safety:** No unverified backup is ever published, and no unverified or unauthorized restore candidate ever replaces the active data directory.
- **Scale:** Online snapshot streaming and verification run in bounded batches without holding the writer lock across disk I/O.

## Checks

- Concurrent foreground command/query execution during `backup create` and `storage scrub` with zero dropped requests and valid restored state.
- Online restore staging test proving live reads/writes succeed throughout staging and verification up to the final atomic swap.
