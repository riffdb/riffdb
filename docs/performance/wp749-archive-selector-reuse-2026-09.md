# WP-749: reuse the durable archive selector bytes

## Decision and current status

The maintainer directed keeping WP-749's unchanged-acknowledgement-latency gate
and reducing archive overhead after the [post-WP-810 comparison](wp749-archive-post-wp810-2026-09.md)
failed it. This candidate removes redundant archive-side I/O. Its latency
benefit is unmeasured; WP-749 remains incomplete. Remote work is paused until
the maintainer releases the measurement host.

The integrated source baseline is `be4e1a25`, which incorporates main's
`c8d388b0` retention instrumentation. The archive repository is unchanged
between the measured baseline `10c312d74` and this integrated baseline.
Future comparisons must use the integrated source in both arms and retain
separate executable identities when comparing the implementation change.

## Redundant work observed

The exact-retry/reopen test writes two new archive frames. A local syscall trace
of the production sink at `10c312d74` records two successful syncs each of
`frame.tmp`, `manifest.tmp` and `current.tmp`. The selector repeats the same
386-byte manifest already written and synced before `artifacts-synced`.
This is a count of real sink operations, not a latency measurement.

## Implementation and durability argument

The filesystem sink retains the already-synced manifest descriptor, links its
immutable name to `current.tmp`, and applies the existing selector rename and
directory sync. Both linked names are checked against the retained descriptor
before publication. A replaced regular file or symlink cannot substitute for
that descriptor, even when its bytes and length match.

The publication order remains:

1. Write and sync the frame and manifest; install their immutable names.
2. Sync the archive directory before any selector can name the new pair.
3. Stage the selector, check the previous selection and atomically rename it.
4. Verify the selected descriptor and sync the directory before confirming
   archive progress.

Only the duplicate selector payload write and its file sync are removed.
The already-durable manifest supplies the exact same selector bytes. Both
directory barriers, all ten crash/uncertainty boundaries, bounded cleanup,
private-directory ownership and exact retry checks remain. Retry after an
uncertain publication still compares and syncs the complete selected pair.

Selector names and manifest/frame encodings are unchanged. Old independent
selectors remain readable and appendable. Advancing a selector unlinks its
stale staging name without mutating an older manifest or an open descriptor;
frozen archive selections continue to read their original prefix.

This is an implementation choice under D-003 and accepted ADR-0178 §7:
it adds no durable identity, public surface or guarantee. It changes neither
primary capture nor command transaction ordering. Capture and codec work remain
closed. The code uses the same checked directory hard-link mechanism already
used to install immutable archive artifacts.

The production path is connected: `process_graph.rs` starts
`RunningArchiveWorkers`; `archive_worker.rs::collect` opens a
`RedbArchiveRepository` from the verified backup, and the pump appends through
`ArchiveConsumerV1` to that sink's `persist`. The changed selector path is
therefore used by automatic production collection. It is not a test-only sink.

## Validation required before measurement

- Exact retry and reopen syscall comparison: two selector file syncs must
  become zero, with frame/manifest syncs retained.
- Existing process-crash and uncertain-I/O matrices at all ten boundaries.
- Legacy independent selectors, stale linked staging files, open descriptors
  and frozen-prefix readers across advancement.
- Equal-byte and symlink source substitution refused before publication.
- Archive recovery/restore tests and package acceptance on the integrated tree.

The 39 archive-focused storage tests pass, including the new compatibility
and source-substitution proofs and both ten-edge matrices. The candidate
syscall trace removes exactly the two `current.tmp` syncs; all other archive
file and directory sync counts match the control, including retries and reopen
validation. Both traces retain ten directory syncs. The evidence packet at
`release/evidence/wp-749/archive-selector-reuse-20260927/` binds the source,
counts and private trace/test-log digests. Clippy passes with warnings denied
after replacing a duplicate test-fixture module inclusion with an import of
the existing module. The syscall census was repeated on that corrected test
source with identical counts. Full package acceptance is running; there is no
timed result.

The source change alone cannot satisfy the acknowledgement gate. Its subsequent
measurement requires a recorded bounded protocol and an available exclusive
host. Retain direct raw acknowledgements, the full workload, checkpoint/wait
counters, archive progress, source and executable digests. Disclose throughput
separately; a tail movement still needs a named mechanism under ADR-0249.

## PR note

Package: WP-749. Tier: guarantee, implementing accepted ADR-0178. Behavior:
reuse durable manifest bytes for selector publication. Checks: 39 archive
tests, Clippy and syscall census pass; broader acceptance is in progress. No performance
verdict or completion claim. Compatibility: existing names,
bytes, readers and retry semantics remain. Hazards: the latency gate is still
unmet and sharing an inode requires retaining immutable-file discipline.
Handbook: [backup and restore](../backup-restore.md) describes the unchanged
operator contract and selector compatibility.
