---
adr: "0320"
title: Wall-Clock Point-in-Time Recovery Resolution and Cloud Archive Frame Summary
status: accepted
tier: surface
date: 2026-10-19
accepted: 2026-10-19
acceptance: "User selected Option C (ADR-0320 wall-clock point-in-time recovery resolution and cloud archive frame summary)"
requires: ["0071", "0072", "0178", "0207", "0248", "0263"]
amends: []
supersedes: []
requirements: ["REC-001", "REC-002", "REC-003", "REC-006", "REP-005", "REP-007", "AFC-007"]
packages: ["WP-880"]
obligations:
  - id: OBL-0320-1
    package: WP-880
    description: "RedbVerifiedArchiveBackup::resolve_archive_stop_at_timestamp deterministically resolves a wall-clock Timestamp to ArchiveRestoreStopV1::AtApplicationSequence across the backup fence, physical frame boundaries, and interior grouped-commit segments, and prepare_restore restores the exact prefix"
    proof: archive_pitr_timestamp_resolution_restores_exact_fence_frame_and_interior_group_stops
  - id: OBL-0320-2
    package: WP-880
    description: "RedbVerifiedArchiveBackup::resolve_archive_stop_at_timestamp fails closed on timestamps preceding the backup fence or first commit and on non-monotonic clock regressions straddling the target timestamp, and RedbArchiveRepository::frame_summaries reports confirmed frame and manifest pairs with sequence and timestamp bounds"
    proof: archive_pitr_refuses_pre_fence_or_non_monotonic_timestamps_and_summarizes_confirmed_frames
review_triggers:
  - "Wall-clock PITR resolution silently accepts a target timestamp earlier than the verified backup fence or admits a non-monotonic clock regression that straddles the target timestamp."
  - "Archive frame summary inspection or sync notification exposes unconfirmed staging files or mutates durable archive or receipt formats."
---
# ADR-0320: Wall-Clock Point-in-Time Recovery Resolution and Cloud Archive Frame Summary

## Context

`ADR-0178`, `ADR-0207`, and `ADR-0248` established verified physical backup binding (`RedbVerifiedArchiveBackup`), append-only changelog V3 archives (`RedbArchiveRepository`), and exact sequence-targeted archive restore (`ArchiveRestoreStopV1::AtApplicationSequence(CommitSequence)`), including private command-prefix reconstruction inside grouped commit receipts (`RedbReplayedArchiveRestore::rebuild_to_stop`). Every committed command record and archived `AuthoritativeNamespaceV1::Commits` segment already carries the coordinator's authoritative commit `LogicalTime` (`Timestamp` + `LogicalCounter`), and `RedbArchiveRepository` persists immutable `(frame-<16-hex-seq>.v3, manifest-<16-hex-seq>.v1)` pairs advanced via `CURRENT`.

However, operators and serverless control planes recovering from accidental data modification specify recovery targets in wall-clock time (`Timestamp`) rather than internal `CommitSequence` numbers, and external cloud archive shippers (shipping immutable archive frames to S3/GCS/R2) lack a typed inspection and notification surface for confirmed `(frame, manifest)` pairs and their covered sequence and commit-timestamp ranges.

## Decision

1. **Deterministic Wall-Clock Timestamp-to-Sequence PITR Resolution (`crates/riffdb-storage-redb`):**
   - `RedbVerifiedArchiveBackup` records the backup fence commit timestamp (`fence_commit_timestamp: Option<Timestamp>`) when opening and verifying a physical backup whose `last_commit_sequence()` is present.
   - `RedbVerifiedArchiveBackup::resolve_archive_stop_at_timestamp(&self, archive: &RedbArchiveRepository, target_timestamp: Timestamp, cancellation: &AtomicBool) -> Result<ArchivePitrResolutionV1, ArchivePitrResolutionError>` verifies the backup and archive lineage/fence/digest binding and streams the selected archive frames in order.
   - For the backup fence (when present) and each decoded `AuthoritativeNamespaceV1::Commits` segment member in `ChangelogFrameV3`, it inspects `(commit_sequence, commit.logical_time().timestamp())`:
     - If `target_timestamp` precedes the verified backup fence's commit timestamp (or if the backup head was pruned and no archived commit has `timestamp <= target_timestamp`), resolution fails closed with `ArchivePitrResolutionError::TargetBeforeBackupFence`.
     - If the backup has no application commits and no archived commit has `timestamp <= target_timestamp`, resolution fails closed with `ArchivePitrResolutionError::TargetBeforeFirstCommit`.
     - If any later commit after the first commit with `timestamp > target_timestamp` has `timestamp <= target_timestamp` (a non-monotonic clock regression straddling `target_timestamp`), resolution fails closed with `ArchivePitrResolutionError::NonMonotonicTimestampAcrossTarget`.
     - Otherwise it returns `ArchivePitrResolutionV1` containing `stop: ArchiveRestoreStopV1::AtApplicationSequence(resolved_sequence)`, `resolved_sequence`, `resolved_commit_timestamp`, `next_excluded_sequence`, and `next_excluded_commit_timestamp`, which feeds directly into `RedbArchiveRestoreStage::prepare_restore` without changing `.proto` wire messages or `OfflineMaintenanceReceiptV3` durable encodings.

2. **Confirmed Archive Frame Summaries and Sync Hook (`crates/riffdb-storage-redb`, `crates/riffdb-server`):**
   - `RedbArchiveRepository::frame_summaries(&self, cancellation: &AtomicBool) -> Result<Vec<ArchiveFrameSummaryV1>, ArchiveConsumerErrorV1>` validates the selected archive prefix and returns an ordered `Vec<ArchiveFrameSummaryV1>` for every confirmed frame through `CURRENT`, exposing `manifest: ArchiveManifestV1`, `frame_file_name: String`, `manifest_file_name: String`, `first_application_sequence: Option<CommitSequence>`, `last_application_sequence: Option<CommitSequence>`, `first_commit_timestamp: Option<Timestamp>`, `last_commit_timestamp: Option<Timestamp>`, and `command_count: u64`.
   - `crates/riffdb-server/src/archive_worker.rs` supports an optional bounded confirmed-frame observer callback invoked only after `Pump::step` durably advances the archive sink, enabling external cloud object-storage shippers and PITR indexers to observe confirmed archive progress without touching unconfirmed `.tmp` staging files.

## Standing design tests

- **Interface safety:** Wall-clock PITR resolution never guesses across a pruned backup fence or a non-monotonic clock inversion straddling the target timestamp; it resolves to an exact verified `ArchiveRestoreStopV1::AtApplicationSequence` that undergoes full archive replay, interior group reconstruction, integrity scrub, and staged authorization.
- **Scale:** Resolution and frame summarization stream one bounded `ChangelogFrameV3` (`<= 8 MiB`) at a time with per-frame and per-receipt cancellation checks.

## Checks

- `archive_pitr_timestamp_resolution_restores_exact_fence_frame_and_interior_group_stops` (`OBL-0320-1`).
- `archive_pitr_refuses_pre_fence_or_non_monotonic_timestamps_and_summarizes_confirmed_frames` (`OBL-0320-2`).
- `./scripts/acceptance --wp WP-880`.
