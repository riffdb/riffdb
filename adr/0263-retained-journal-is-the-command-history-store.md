---
adr: "0263"
title: Retained Journal Is The Command History Store
status: accepted
tier: guarantee
date: 2026-09-28
accepted: '2026-09-28'
acceptance: 'maintainer, in session, 2026-09-28: "approved, build it" (ADR-0263 as written);
  Amendment 1 accepted by the maintainer, in session, 2026-09-28: "Accepted";
  Amendment 2 accepted by the maintainer, in session, 2026-09-28: "Approved";
  Amendment 3 accepted by the maintainer, in session, 2026-09-29: "i accepted it";
  Amendment 4 accepted by the maintainer, in session, 2026-09-30: "approved"'
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
packages: [WP-814, WP-815, WP-816]
# One entry per deferred obligation. `proof` is a test function name or a
# scripts/<name>; ./scripts/check-adr-obligations requires it to exist as a
# definition once the owning package is complete.
obligations: []
review_triggers:
  - A command segment or V3 receipt made durable by a standard-profile journal frame
    would be written into a redb table, or into any store other than that journal
    extent (Amendment 1).
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

Two further results constrain the design. Rewriting receipts and segments into a second append file erased most of the 1×
gain, so the journal itself must be the store. And raising the ceilings alone on the live server cut p99 from 107 to 11 ms,
but raised the worst case from about 190 to 670 ms, because the larger checkpoint syncs grow. ADR-0182 retention also exists
largely to relocate those same history rows out of redb under the writer gate (WP-761, WP-811, ADR-0261).

## Decision

1. The journal extent that made a command durable is the durable store for that command's segment and its V3 receipt. redb
   holds only current state: entities, secondary indexes, index epochs, entity-chain heads, idempotency and audit pending
   rows, locators, metadata, and the history index in item 3. No COMMITS segment row and no HISTORY receipt row is written
   to redb.
2. The checkpoint materializes current-state mutations only. It still derives each receipt from the frame and its recorded
   binding, checks it, and advances the history chain state in META in the same redb transaction. The receipt bytes are not
   stored; a reader re-derives them from the frame, and a mismatch against the chain is a typed corruption refusal.
3. A fully checkpointed extent is sealed rather than recycled. Sealing writes a bounded footer (commit, administration, and
   receipt sequence ranges; frame offsets and hashes; an extent hash) and syncs it once, off the writer gate. The checkpoint
   that completes an extent inserts its history-index row: extent identity, sequence ranges, and footer hash. The live and
   sealed extents together hold every frame from the retention watermark to the head.
4. Every reader of segments or receipts resolves through the history index to one extent and one frame, then decodes that
   frame. This covers transient-index rebuild, command, idempotency, provenance and audit resolution, changelog cursors,
   follower shipping, backup, the scrub, retention, and validated-prefix work. Frame decoding stays behind the byte-checked
   payload cache. Uncheckpointed frames are read from the live extent, so the ADR-0104 overlay no longer carries COMMITS
   rows.
5. Acknowledgement and durability are unchanged. A command is acknowledged after the ordered writer's group sync of its
   frame, as in ADR-0101. Sealing, index insertion, and extent allocation are never on the acknowledgement path.
6. Online retention retires whole sealed extents. When the ADR-0182 fences permit a target covering an entire sealed extent,
   one bounded administration transition materializes the ADR-0182 retained outcome, provenance, audit, and route views for
   that extent's commands, then advances the watermark and removes the index row. The extent file is deleted or recycled
   only after that transition is durable. At most 256 sequences per step (STO-031) and the five-percent p95 bound (ADR-0182
   section 7) still hold. A step spanning part of an extent waits for the extent to seal.
7. ADR-0104's suffix ceilings rise to 65,536 transitions and 256 MiB of encoded frames, with a 320 MiB extent. The overlay
   memory ceiling rises from 128 MiB to at most 256 MiB. The package must show that the measured overlay charge at the new
   ceilings stays within it, since segments leave the overlay.
8. On open, sealed extents are checked against their index rows and footer hashes. The existing background scrub fully
   validates frames and the receipt chain. A sealed extent that is missing, truncated, or mismatched at or above the
   watermark is a typed recovery refusal, never silently absent history.
9. The journal and redb formats take new versions: the extent footer, the history index table, and the absence of COMMITS
   and HISTORY. A binary that does not know them refuses to open. An existing database migrates offline through a
   maintenance command that writes its COMMITS and HISTORY rows into sealed extents and verifies them before removing the
   tables. The POC offers no online upgrade.

### Amendment 1 (accepted 2026-09-28): journaled history only

WP-814's inventory found history that never passes through a journal frame, so Decision 1 cannot hold as written. The
hardened profile commits each group to redb directly (ADR-0101 section 1). So do standard-profile commands outside the
deferred journal epoch, and every direct lane: administration, catalog, modules, capabilities, migrations, exports, consumer
and outbox transitions, retention, and the control ceremonies. Each of these seals its receipt, and any segment, straight
into redb. Followers have no journal (REP-002) and materialize COMMITS from receipts. The measured cost is in
standard-profile journaled command groups. This amendment scopes the decision to them and replaces the parts of Decisions 1,
2, 3, 4, 6, and 9 that it contradicts.

A1. Only history made durable by a standard-profile journal frame is stored in journal extents. That is each command segment
   the frame carries and the V3 receipt its checkpoint derives. History from every other path keeps today's redb placement
   in COMMITS and HISTORY, unchanged. Each commit sequence and each receipt sequence lives in exactly one of the two stores.
A2. The checkpoint that materializes a journal frame writes one history-index row for it in redb. The row carries the
   frame's extent, offset, and hash; its commit, administration, and receipt sequence ranges; and the receipt's complete
   binding (history incarnation, predecessor sequence, prior history hash, catalog digest). The binding is not in the frame,
   so the receipt can be re-derived only from the frame plus this row. Sealing adds one per-extent row with the footer hash.
A3. Every reader of segments or receipts looks up the history index first and falls back to COMMITS or HISTORY. A sequence
   found in both, or in neither while above the retention watermark, is a typed corruption refusal. Changelog cursors and
   full-history walks merge the two stores in sequence order.
A4. redb records the checkpointed application and administration frontiers in META, advanced in the checkpoint transaction.
   The frontier is no longer derived from the COMMITS tail. META also keeps running counts and tails across both stores, so
   bounded startup and clean close (REC-005) stay O(1).
A5. Backup, restore, and archive copy and validate the sealed and live extents that hold retained history, alongside the
   redb file. The empty-journal requirement no longer means that no history exists outside redb.
A6. Retention retires whole sealed extents under Decision 6, and prunes redb-held history as today. It never rewrites extent
   bytes. Offline retention rounds its target down so no segment held in an extent straddles the watermark.
A7. Format migrations that rewrite COMMITS bytes apply only to redb-held rows. Extent frames are immutable and keep their
   own format version, which a reader decodes or refuses.
A8. Followers are unchanged: no journal, and history in redb. They remain the same database format, because the format
   admits both stores.

### Amendment 2 (accepted 2026-09-28): the history index is derived sidecar state

An authoritative index table would need a successor to the closed authority catalog, whose digest every V3 receipt binds.
The index is derivable from the retained extents and the history chain in META, so it is derived state under ADR-0240. This
replaces A2's placement.

B1. The history index lives in the derived sidecar redb file. It is written after the checkpoint commits, and each row
   carries A2's fields. It is never evidence that a commit occurred, and a row whose frame hash does not match its extent is
   discarded.
B2. On open, index rows past the index high-water mark are rebuilt by scanning only the retained frames after it and
   recomputing bindings forward from META. This is journal recovery under REC-005. A missing or discarded index is rebuilt
   after readiness; until then, reads that need it answer `Unavailable`, never absence.
B3. A4's frontiers, counts, and tails stay authoritative in the main file, through the existing META allocators and
   retention watermark, with no new META key: the checkpointed frontier is the committed allocator, and one command per
   sequence makes the counts derivable.
B4. Decision 3's footer also records each frame's complete receipt binding. A sealed extent then describes itself, and the
   index can be rebuilt from footers and the live extent after its predecessors are retired.

### Amendment 3 (accepted 2026-09-29): history stubs replace the sidecar index

Moving every reader of COMMITS and HISTORY to a separate index would rewrite about 130 read sites that depend on these
tables' keys, ranges, tails, and row counts, and a sidecar index must be reconciled after every crash. The two tables can
carry the index instead. This replaces A2, A3, B1, and B2; B3 and B4 stand.

C1. For each segment and receipt a journal frame made durable, the checkpoint transaction that materializes the frame writes
   a stub under the same COMMITS or HISTORY key. A stub is a fixed-size locator: the extent generation, the frame's position
   and hash, and the summary fields readers need without the value. The segment and receipt bytes are never written to redb.
C2. Key order, ranges, tails, and row counts of COMMITS and HISTORY are unchanged, so bounded startup, retention accounting,
   and every structural reader keep working. Reading a value resolves a stub by reading its frame from the extent and
   extracting the segment or re-deriving the receipt. A frame whose hash does not match its stub, or a missing extent, is a
   typed corruption refusal.
C3. A stub is a representation, not replicated state. Every hash of a COMMITS or HISTORY value that a receipt, follower,
   backup comparison, or validation binds is computed over the resolved value, so a primary and its followers agree byte for
   byte on history.
C4. Stubs are a new versioned encoding, which a binary without this amendment refuses.

### Amendment 4 (accepted 2026-09-30): bounded pruning and whole-file reclamation

Decision 6 and A6 distinguish logical pruning from physical reclamation:

1. Online logical pruning uses administration transitions covering at most 256 application sequences each. Each transition
   atomically materializes retained outcome, provenance, audit, and route views, deletes whole command rows and the covered
   event/outbox rows, appends its tombstone, and advances the application retention watermark under every existing fence.
   The first transition invalidates the validated-prefix checkpoint as before. A logical pruning boundary may lie inside a
   sealed extent; extent bytes remain immutable. Commands in an unsealed extent wait for sealing.
2. Physical reclamation deletes or recycles only an entire sealed extent, after durable logical pruning has covered every
   application command in it and the existing source checkpoint/reclamation protocol has durably removed every V3 receipt
   reference into it. The retained resume-anchor receipt and every source/follower/projection hold retain their existing
   meaning. No acknowledgement, checkpoint, detach, or receipt deletion is manufactured to make a file eligible. Unreadable
   or inconsistent evidence refuses reclamation.
3. File reclamation runs off the ordered writer gate with a bounded pass and at most one extent operation per pass. It
   cannot advance an allocator, watermark, or consumer frontier. A crash before or after the file operation must leave
   durable authoritative state sufficient to retry safely. Reuse must install a fresh extent header and generation without
   exposing old bytes as new frames. No command segment or journaled receipt is copied elsewhere.
4. A target inside an extent may make bounded logical progress while physical space remains allocated. Diagnostics and
   evidence report logical pruning, physically reclaimed/recycled extents, and receipts or other fences still retaining
   files separately. The five-percent p95 interference gate and keep-up fraction greater than 0.0131 remain mandatory;
   logical progress alone does not establish sustainable physical storage.
5. Offline retention remains available. It preserves references and immutable extent bytes; its existing journal rebase
   remains required. This amendment adds no public operation, durable identity, or relaxed application guarantee.

 ## Options considered

1. **Raise the ceilings and keep history in redb:** p99 107 → 11 ms on the live server, but the worst case rose to about 670
   ms, and the replay shows redb writes growing to 7.4 GiB per run. Retention still relocates rows under the gate. Rejected
   as the end state; its ceiling change is kept in item 7.
2. **Move history to a second append store:** the replay's separate-file variant kept the 1× probe p99 at 65 ms and added a
   second copy of every byte. Rejected.
3. **Pace or throttle checkpoint writeback:** chunked `fdatasync`, `fadvise`, and `sync_file_range`, both asynchronous and
   waited, were each measured. They reduced throughput or left the ~335 ms journal stalls unchanged, and two of them needed
   unsafe code. Rejected.
4. **Replace redb with an LSM engine (Fjall):** a 20-minute soak held 10.3k commands/s with p99 1.9 ms, but p999 was 130 ms
   and the worst stall 900 ms, from compaction RiffDB does not schedule. It would also cede recovery and format control, and
   ADR-0113 simulation coverage. Rejected.
5. **Put the journal on its own volume:** real deployments have one disk. Rejected.
6. **Journal as history store (chosen):** it removes 85% of checkpoint bytes, makes retention a file operation, and adds no
   sync to acknowledgement.

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

