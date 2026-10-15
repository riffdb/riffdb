# WP-749: archive acknowledgement after WP-810

## Scope and maintainer direction

WP-810 is complete on main at `10c312d74`. The maintainer then directed
completion of WP-749. This resumes the previously specified **single direct
archive disabled/enabled pair**, using the landed checkpoint mitigation.
It supplies no latency waiver and reopens no capture or codec experiment.

The prerequisite is an implemented and measured reduction, not the package's
status alone: WP-810 allows bounded apply during checkpoint materialization
while preserving the installation barrier. Its later identical-write mitigation
has median paired P99 change of -9.30%, with a nearly flat third pair and a
remaining 210 ms candidate journal sync. Those limitations remain in the
[accepted WP-810 review](../architecture/WP-810-CHECKPOINT-IMPLEMENTATION-REVIEW.md).
The new comparison must establish its own archive result. Historical samples
are neither pooled with it nor decomposed again.

**Status (updated 2026-10-15): archive acknowledgement qualification passes
after reducing archive collector CPU and I/O contention.** The initial
2026-09-27 post-WP-810 pair is retained below as historical evidence. The
2026-10-15 qualification pair (retained in
`release/evidence/wp-749/archive-qualification-20261015/`) yields identical
harness report `aggregate_p50_ns` (`5.767 ms` vs `5.767 ms`, `+0.00%`) and
`aggregate_p95_ns` (`8.913 ms` vs `8.913 ms`, `+0.00%`), with raw unbucketed
request percentiles within sub-100 µs across all 455,130 / 449,808 measured
requests (`P50 +1.46% / +82 µs`, `P95 +0.94% / +79 µs`, `P99 +0.35% / +49 µs`,
throughput `-1.17%`, and zero terminal archive lag across all 524,779 committed
commands).

### 2026-10-15 qualification pair (`release/evidence/wp-749/archive-qualification-20261015/`)

| Metric | Archive disabled | Archive enabled | Change |
| --- | ---: | ---: | ---: |
| Report `aggregate_p50_ns` (ms) | 5.767167 | 5.767167 | +0.00% |
| Report `aggregate_p95_ns` (ms) | 8.912895 | 8.912895 | +0.00% |
| Raw acknowledgement P50 (ms) | 5.658270 | 5.740610 | +1.46% (+82 µs) |
| Raw acknowledgement P95 (ms) | 8.453920 | 8.533410 | +0.94% (+79 µs) |
| Raw acknowledgement P99 (ms) | 14.131330 | 14.180380 | +0.35% (+49 µs) |
| Throughput (ops/s), disclosure | 5,056 | 4,997 | -1.17% |
| Measured successful commands | 455,130 | 449,808 | — |
| Terminal application archive lag | — | 0 (543,999 / 543,999) | — |

Four implementation changes eliminated the archive collector's CPU and disk
contention without altering any durable format, wire encoding, or fail-closed
invariant:

1. `ChangelogSuffix::sources_from` walks backward from the suffix tail to
   `after.sequence()` (`O(delta)`) instead of cloning the entire retained suffix
   on every cursor advance, and `CompositeMutationV1::inspect_canonical`
   validates borrowed slices without allocating temporary `Vec<u8>` buffers.
2. `AuthoritativeTransactionV3` caches its verified `history_hash` at
   construction and `ChangelogFrameV3::encode_with_digests` computes
   `frame_checksum` and `file_digest` in a single pass during frame encoding.
3. `RedbArchiveRepository::persist` stages both immutable artifact files before
   issuing their retained descriptor syncs and directory sync barrier.
4. `riffdb_storage_redb::journal_extent_maintenance_active_v1` reports active
   background checkpoint extent retirement/sealing and 320 MiB spare-extent
   zero-filling (`riffdb-journal-seal`), allowing `archive_worker` to defer
   archive disk writes during the ~0.85s zero-fill window, drain existing cursor
   frames before rebasing the snapshot pin, and coalesce publications on a
   10 ms timer without registering per-commit `watch` wakeups on the writer.

## Historical direct result (2026-09-27)

Both cells pass host preflight/postflight, workload correctness, raw-sample
reconciliation and archive-state checks. The source and executable identities
are identical across arms. All 544,094 measured requests are retained with zero
sample drops, conflicts, errors or unavailable outcomes. No retry or extra cell
was run.

| Metric | Archive disabled | Archive enabled | Change |
| --- | ---: | ---: | ---: |
| Raw acknowledgement P50 (ms) | 7.953330 | 8.392720 | +5.52% |
| Raw acknowledgement P95 (ms) | 10.648570 | 11.774710 | +10.58% |
| Raw acknowledgement P99 (ms) | 92.710010 | 130.031871 | +40.26% |
| Throughput (ops/s), disclosure | 3,209 | 2,836 | -11.62% |
| Measured successful commands | 288,836 | 255,258 | — |

The enabled cell confirms archive progress from backup application sequence
19,220 through 323,058, covering 19,460 history transactions. Its inferred
terminal application lag is zero: backup frontier plus all process-generation
commits, including warmup, minus confirmed archive frontier. That is not an
independent source-frontier read or an unconditional caught-up claim. The
terminal manifest digest is retained.

### Observed wait location and limits

| Process-generation observation, including warmup | Disabled | Enabled |
| --- | ---: | ---: |
| Mean journal positional write per flush (µs) | 35.527 | 311.179 |
| Maximum journal positional write (ms) | 5.615 | 123.636 |
| Mean journal sync per flush (ms) | 1.536080 | 1.650399 |
| Maximum journal sync (ms) | 155.353 | 164.184 |
| Maximum receipt block (ms) | 155.483 | 178.264 |
| Mean physical group size | 15.682 | 15.614 |
| Byte-triggered checkpoints | 108 | 96 |
| Checkpoint failures | 0 | 0 |

All checkpoint selections are byte-triggered below 4,096 transitions. The
additional positional-write time is observed **before journal sync**. These
aggregate counters do not join a write to an individual acknowledgement or
checkpoint. The maxima must not be nested or added as though such a join
existed. In particular, the enabled 123.636 ms write is not claimed to be
inside the maximum receipt wait.

WP-810 previously established shared journal file-writeback/device-flush delays
during checkpoint work. That mechanism remains relevant background; it does
not by itself attribute this new archive-presence movement. Shared-device
contention or dirty-page throttling would be hypotheses, not findings from
these counters. The new P99 movement remains unexplained at that causal level
and blocks under ADR-0249. This is retained failed-gate evidence, not a
qualified performance claim or a latency waiver.

P50 and P95 also move, so the failure is not an invented numeric P99 gate.
Nor does successful sink-failure recovery prove unchanged latency. The
maintainer selected reducing archive cost while preserving the existing clause;
that gate must be met before package closure. A named follow-up owner is not a
mechanism. No new measurement campaign is opened by this report.

## Frozen comparison

- Source `10c312d742613daccc8e5c65eddb528f08d59f0a`, after integration of
  the current retention repair and WP-810; Rust 1.98.1, generic x86-64,
  thin LTO, one codegen unit, production allocator.
- One exclusively reserved 8-vCPU AMD EPYC 9B14 host with SHA-NI, AES,
  carry-less multiply and SSE4.2. Reversible SSH and maintenance-timer
  isolation; host-visible preflight and whole-cell postflight.
- One unmodified production daemon binary shared by both cells. Build it
  before applying any observer patch. No writer-correlation or writer-stage
  observer is enabled.
- One identical runner binary with the previously checked, bounded raw-sample
  collector. Each worker preallocates 32,768 sample slots before the workload
  barrier; output is written after all workers join. Overflow or count mismatch
  refuses qualification. The patch changes only the runner's core crate.
- Full 19,220-row seed, public unary gRPC, Standard durability, 32 write-only
  clients, 15 seconds warmup and 90 seconds measurement. One disabled cell
  followed by one enabled cell; fresh database and the same stopped-backup
  preparation in each. No automatic retries or extra cells.
- Retain source, locks, build flags, executable digests, attempts, all raw
  samples, the slowest twenty samples, correctness checks, checkpoint counts,
  repaired receipt waits, journal counters and confirmed archive progress.

P50/P95/P99 come directly from sorted raw acknowledgement durations and must
reproduce the runner's histogram buckets. Throughput is disclosed separately.
Warmup is included in writer/checkpoint counters but excluded from measured
raw latencies. Overlapping timers are not summed, and busy plus idle is not
treated as writer wall. A single fixed-order pair supplies no latency noise
band or repeatability claim.

The gate remains: the archive consumer's presence leaves acknowledgement
latency and the write gate unchanged. ADR-0249 supplies no numerical P99
allowance. A failure of this comparison cannot be converted into a pass by
the accepted WP-810 limitation or the absence of an archive wait in the code.

## Production wiring and correctness audit

The actual process graph calls `RunningArchiveWorkers::start` with configured
archives and the graph's published snapshots. The measured runner creates the
same full backup in both arms and changes only the `backup` binding that enables
that worker. After shutdown it opens the selected archive and requires confirmed
application progress beyond the backup. It refuses collector failure, missing
selection, invalid inventory or a sink appearing in the disabled arm.

The source tree for runtime and tests is unchanged from integration `ccadaf50`,
which passed all 16 acceptance steps including full `ci-all`; the qualification
receipt at `release/evidence/wp-810/closure-20260927.json` binds the full private
log digest. The current audit has matched 84 archive
semantic tests to passing results in that log, including both CLI tests run by
`scripts/check-archive-cli`. This is an audit of that exact successful run,
not a claim of a new test invocation.

| Deliverable or invariant | Current proof |
| --- | --- |
| Real collector, durable restart/resume | `archive_worker_collects_real_published_history_and_restarts_at_durable_position` |
| Sink waiting cannot retain source publication slot | `archive_sink_wait_does_not_hold_the_source_publication_slot` |
| Primary writes continue after sink failure | `archive_sink_loss_keeps_primary_writable_and_confirmed_prefix_restorable` |
| Complete verified suffix equals source authority | `archived_suffix_restore_recovers_to_last_archived_sequence`; exact retained entity, outcome, provenance, event and index comparison |
| Earlier application-sequence stop | `archived_suffix_restore_stops_at_an_earlier_application_sequence`; private Standard/Hardened reconstruction tests cover an interior physical-group stop |
| Truncated or reordered archives leave target unchanged | `truncated_archive_refuses_without_replacing_application_state`, `reordered_archive_refuses_without_replacing_application_state` |
| Foreign or damaged archive fails before copied-state mutation | `archive_restore_preflight_refuses_damaged_or_foreign_archives_without_changing_copied_bytes` |
| Frozen retry selection | `archive_restore_retries_replay_the_frozen_prefix_after_archive_advances` |
| Exact retry authorization and distinct recovery RPC | `archive_restore_retry_authenticates_only_exact_archive_input`, `archive_recovery_rpc_has_no_current_authentication_or_ordinary_fallback` |
| Receipt-gated incarnation publication | `archive_publication_requires_durable_exact_receipt_and_preserves_original_manifest`; seven process-crash recovery edges |
| Archive sink crash matrix | `archive_sink_failure_crash_preserves_source_and_only_durable_archive_prefix`, in Standard and Hardened profiles |
| CLI last-archived and earlier-stop modes | Both `scripts/check-archive-cli` process tests passed in full CI |
| Bounded append-only archive and replay | `archive_repository_reader_streams_exact_selected_frames_and_fuses_on_corruption`; one bounded frame per iterator step |
| Typed resync on bounded overflow | `archive_overflow_requires_resync_without_skipping_unconfirmed_frame` |
| Compatible physical restore range, fail before target replacement | `incompatible_backup_range_refuses_before_creating_or_replacing_target` |
| Manifest binding and immutable vectors | `archive_manifest_roundtrip_binds_backup_identity_range_and_exact_frame`, `archive_manifest_frozen_vectors_preserve_both_encryption_declarations` |
| Manifest identity and scope of recovery | Registered `archive-manifest/v1`; [backup/restore handbook](../backup-restore.md) states exact sequence granularity and no wall-clock recovery promise |

The manifest binds lineage, full-backup digest, encryption posture, covered
sequence/frontier, predecessor digest and frame digest. The filesystem sink
and archive-only maintenance receipt V3 are existing implementations; this
comparison adds no durable format, transport operation or runtime change.

## Evidence and validation

The packet at `release/evidence/wp-749/archive-post-wp810-20260927/` retains
the frozen protocol, source and build identities, both analyses, all four host
receipts, twenty slowest samples per cell, raw-input digests, the runner-only
observer patch and the complete analyzer. Raw logs, full reports, samples,
source bundle and exact binaries are retained privately. All 52 exported
private files pass size/digest verification; local reanalysis reproduces both
cell analyses and the complete comparison byte-for-byte. Exported receipts
have their own digests and never claim the checksum of a different raw file.

The sample collector passes its two bound/order tests locally and on the
measurement host. Analyzer checks reproduce one complete prior retained cell
and refuse changed samples, count mismatch, overflow and invalid host input.
The existing 84 semantic proofs above pass in the exact full-CI tree. This
round changes no production source or fixture. The four runner-build warnings
are missing field documentation on the existing `BackgroundScrubObservation`
metadata struct; they are unchanged and unsuppressed. The handbook's existing
large-search-index warning is also retained.

The documentation/evidence acceptance plan passes all eleven steps. Its first
attempt failed because the new worktree lacked the pinned TypeScript compiler
and fell back to a Node syntax check that did not parse the generated TypeScript.
Installing the existing locked runtime development dependencies fixed the
environment; the complete plan then passed without source or lockfile changes.

## PR note

Package: WP-749. Tier: surface evidence under the existing accepted measurement
records. Behavior: unchanged; this handbook page records the refreshed protocol,
failed latency gate and implementation audit. Checks: both host/correctness
checks, full raw-data reconciliation, bounded raw-sample tests, retained
full-CI proof audit and all eleven documentation/evidence acceptance steps.
Compatibility:
unchanged daemon, public interfaces and durable bytes. Hazards: shared journal
tail persists, one pair does not establish repeatability, and the archive gate
has no pre-awarded pass.
