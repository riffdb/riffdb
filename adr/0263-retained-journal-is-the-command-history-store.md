---
adr: "0263"
title: Retained Journal Is The Command History Store
status: proposed
tier: guarantee
date: 2026-09-28
accepted: null
acceptance: null
requires: [ADR-0101, ADR-0102, ADR-0103, ADR-0104, ADR-0182, ADR-0186, ADR-0261]
amends:
  - ADR-0101 and ADR-0103's recycling of a fully checkpointed journal extent, which
    becomes sealing and retention of that extent
  - ADR-0102's placement of command segments in the redb COMMITS table
  - ADR-0104 section 4's suffix ceilings, and the overlay's coverage of COMMITS
  - ADR-0182 section 6's online prune, which becomes whole-extent retirement
  - ADR-0186's materialization of each V3 receipt into the redb HISTORY table
supersedes: [ADR-0261]
requirements: [STO-030, STO-031, STO-032, REP-002, REP-003, REC-005]
packages: []
# One entry per deferred obligation. `proof` is a test function name or a
# scripts/<name>; ./scripts/check-adr-obligations requires it to exist as a
# definition once the owning package is complete.
obligations: []
review_triggers:
  - A command segment or V3 receipt would again be written into a redb table, or
    into any store other than the journal extent that made it durable.
  - Acknowledgement would depend on any sync other than the ordered writer's group
    sync, including an extent seal.
  - A sealed extent below the retention watermark could be missing or corrupt
    without a typed refusal, or an extent could be deleted before the retained views
    for its commands are committed.
  - A reader could resolve history from a source other than the history index and
    the sealed or live extent it names.
---
# ADR-0263: Retained Journal Is The Command History Store

## Context

Write throughput is capped by the ordered writer's CPU on both reference hosts: the
writer thread is 98% busy at the tmpfs ceiling. The write tail on a single cloud
persistent disk comes from redb checkpoint writeback. In 2026-09-28 kernel traces on
the ADR-0239 dev-standard host, every slow journal sync spent its time in the device
flush behind about 90 MiB of writes issued by the checkpoint thread, and none waiting
on the filesystem journal. A census of what checkpoints materialize shows why the
writeback is so large. Under write-only load at 32 clients, the checkpoint writes
10.6 KB per command into redb: V3 receipts are 52% (one about 88 KB receipt per
16-command group), command segments are 33%, and current state (entities, indexes,
chain heads, locators, epochs, metadata) is 15%. Receipts and segments are
append-shaped history. The journal frame that made each command durable already holds
both: the segment is a COMMITS mutation in the frame, and the checkpoint derives the
receipt from the frame's validated mutations and its recorded changelog binding.

Replaying those captured checkpoints on the same host, with a probe standing in for
the journal's group syncs (a 90 KB write plus `fdatasync` at 230 Hz), isolated the
cost:

| Checkpoints | History in | redb MiB written | Probe p99 | Probe p999 | Syncs ≥100 ms |
|---|---|---|---|---|---|
| 1×, today | redb | 4,857 | 65.7 ms | 114 ms | 18–25 |
| 1× | journal | 2,002 | 64.6 ms | 66 ms | 0–1 |
| 8× batches | redb | 7,444 | 15–31 ms | ~330 ms | 38–48 |
| 8× batches | journal | 907 | 0.7 ms | 52–70 ms | 9 |

Two further results constrain the design. Rewriting receipts and segments into a
second append file erased most of the 1× gain, so the journal itself must be the
store. And raising the ceilings alone on the live server cut p99 from 107 to 11 ms,
but raised the worst case from about 190 to 670 ms, because the larger checkpoint
syncs grow. ADR-0182 retention also exists largely to relocate those same history
rows out of redb under the writer gate (WP-761, WP-811, ADR-0261).

## Decision

1. The journal extent that made a command durable is the durable store for that
   command's segment and its V3 receipt. redb holds only current state: entities,
   secondary indexes, index epochs, entity-chain heads, idempotency and audit
   pending rows, locators, metadata, and the history index in item 3. No COMMITS
   segment row and no HISTORY receipt row is written to redb.
2. The checkpoint materializes current-state mutations only. It still derives each
   receipt from the frame and its recorded binding, checks it, and advances the
   history chain state in META in the same redb transaction. The receipt bytes are
   not stored; a reader re-derives them from the frame, and a mismatch against the
   chain is a typed corruption refusal.
3. A fully checkpointed extent is sealed rather than recycled. Sealing writes a
   bounded footer (commit, administration, and receipt sequence ranges; frame
   offsets and hashes; an extent hash) and syncs it once, off the writer gate. The
   checkpoint that completes an extent inserts its history-index row: extent
   identity, sequence ranges, and footer hash. The live and sealed extents together
   hold every frame from the retention watermark to the head.
4. Every reader of segments or receipts resolves through the history index to one
   extent and one frame, then decodes that frame. This covers transient-index
   rebuild, command, idempotency, provenance and audit resolution, changelog
   cursors, follower shipping, backup, the scrub, retention, and validated-prefix
   work. Frame decoding stays behind the byte-checked payload cache. Uncheckpointed
   frames are read from the live extent, so the ADR-0104 overlay no longer carries
   COMMITS rows.
5. Acknowledgement and durability are unchanged. A command is acknowledged after
   the ordered writer's group sync of its frame, as in ADR-0101. Sealing, index
   insertion, and extent allocation are never on the acknowledgement path.
6. Online retention retires whole sealed extents. When the ADR-0182 fences permit a
   target covering an entire sealed extent, one bounded administration transition
   materializes the ADR-0182 retained outcome, provenance, audit, and route views for
   that extent's commands, then advances the watermark and removes the index row.
   The extent file is deleted or recycled only after that transition is durable. At
   most 256 sequences per step (STO-031) and the five-percent p95 bound (ADR-0182
   section 7) still hold. A step spanning part of an extent waits for the extent to
   seal.
7. ADR-0104's suffix ceilings rise to 65,536 transitions and 256 MiB of encoded
   frames, with a 320 MiB extent. The overlay memory ceiling rises from 128 MiB to at
   most 256 MiB. The package must show that the measured overlay charge at the new
   ceilings stays within it, since segments leave the overlay.
8. On open, sealed extents are checked against their index rows and footer hashes.
   The existing background scrub fully validates frames and the receipt chain. A
   sealed extent that is missing, truncated, or mismatched at or above the watermark
   is a typed recovery refusal, never silently absent history.
9. The journal and redb formats take new versions: the extent footer, the history
   index table, and the absence of COMMITS and HISTORY. A binary that does not know
   them refuses to open. An existing database migrates offline through a
   maintenance command that writes its COMMITS and HISTORY rows into sealed extents
   and verifies them before removing the tables. The POC offers no online upgrade.

## Options considered

1. **Raise the ceilings and keep history in redb:** p99 107 → 11 ms on the live
   server, but the worst case rose to about 670 ms, and the replay shows redb writes
   growing to 7.4 GiB per run. Retention still relocates rows under the gate.
   Rejected as the end state; its ceiling change is kept in item 7.
2. **Move history to a second append store:** the replay's separate-file variant
   kept the 1× probe p99 at 65 ms and added a second copy of every byte. Rejected.
3. **Pace or throttle checkpoint writeback:** chunked `fdatasync`, `fadvise`, and
   `sync_file_range`, both asynchronous and waited, were each measured. They reduced
   throughput or left the ~335 ms journal stalls unchanged, and two of them needed
   unsafe code. Rejected.
4. **Replace redb with an LSM engine (Fjall):** a 20-minute soak held 10.3k
   commands/s with p99 1.9 ms, but p999 was 130 ms and the worst stall 900 ms, from
   compaction RiffDB does not schedule. It would also cede recovery and format
   control, and ADR-0113 simulation coverage. Rejected.
5. **Put the journal on its own volume:** real deployments have one disk. Rejected.
6. **Journal as history store (chosen):** it removes 85% of checkpoint bytes, makes
   retention a file operation, and adds no sync to acknowledgement.

## Consequences

- redb absorbs about 15% of today's checkpoint bytes. Journal syncs stop queueing
  behind history writeback, and larger checkpoints become affordable.
- Retention stops relocating rows under the writer gate. ADR-0261 and WP-812 are
  superseded before implementation.
- Cost: about 11 modules that read HISTORY and 22 that read COMMITS move to
  history-index resolution. New work covers the extent footer, the history index, the
  migration command, and compatibility fixtures.
- Cost: every history read decodes a frame, bounded by the payload cache. Old
  history still costs disk: extents hold the same frames the journal already wrote,
  without B-tree overhead.
- Cost: when retention holds, the journal grows by new zero-filled extents, which
  doubles sequential bytes for that growth. Recycling retired extents avoids that in
  steady state.
- Deferred: the live-server confirmation of the replay numbers, which gates the
  first package; compaction of retained history; and online migration.

## Standing design tests

- **Interface safety:** nothing public changes. The history, idempotency, audit,
  provenance, and changelog surfaces return the same typed results, and missing
  history below the watermark stays `HistoryPruned`. No principal can address an
  extent, a footer, or the history index.
- **Scale:** checkpoint work scales with current-state change, not with history.
  History reads are one index lookup and one frame read. Retention cost per step is
  bounded by 256 sequences and one file operation.

## Checks

- `checkpoint_materializes_no_commits_or_history_rows`: redb table census after a
  load contains no segment or receipt rows.
- `sealed_extent_crash_windows_recover_to_a_consistent_history_index`: process kills
  and seeded simulation across seal, index insert, retirement, and deletion.
- `history_readers_resolve_through_the_history_index`: every listed reader against
  live, sealed, and retired extents.
- `missing_or_corrupt_sealed_extent_is_a_typed_refusal`.
- Compatibility fixtures for the footer, the history index, and the new versions,
  including refusal by an older binary and the offline migration round trip.
- A live-server A/B on the dev-standard host, write-only and interactive at 32
  clients: throughput, p99, and maximum against the current build, and a keep-up
  measurement for extent retirement under ADR-0182 section 7.
