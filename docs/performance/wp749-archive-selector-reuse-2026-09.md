# WP-749: reuse the durable archive selector bytes

## Decision and current status

The maintainer directed keeping WP-749's unchanged-acknowledgement-latency gate
and reducing archive overhead after the [post-WP-810 comparison](wp749-archive-post-wp810-2026-09.md)
failed it. This change removes redundant archive-side I/O. Its latency
benefit is unmeasured; WP-749 remains incomplete. The maintainer approved landing
the implementation and historical evidence on 2026-09-30. Integrated source
`6d26f3b93`, including main `49fb4c291`, passes all thirteen full-acceptance steps
and full CI. The shared-effect experiment remains separate.

The original syscall census used integrated baseline `be4e1a25`, incorporating
main's `c8d388b0` retention instrumentation. The archive repository was unchanged
between measured baseline `10c312d74` and that baseline. Those source-bound
results remain historical; they are not measurements of current main. Future
comparisons must use the same current baseline and retain separate executable
identities when comparing the implementation change. No benchmark ran during
this landing qualification.

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

## Local validation

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
source with identical counts.

The full plan `./scripts/acceptance --wp WP-749 --base c8d388b0 --full`
passes all thirteen steps on `f77159c0`, including the complete CI battery.
The preceding scoped run passed all 1,051 storage tests; its only failed step
was the subsequently corrected fixture-import lint. Its one ignored test is
the existing manual `shutdown_checkpoint_build_scale_evidence` timing experiment.
The complete qualification also verifies all 84 existing archive/restore
proofs and the two new selector proofs, including both dedicated CLI restore
tests. `qualification.json` and `full-proof-audit.json` retain the source/tree
identities, successful steps, 86 named proofs, 24 source digests and exact
private-log digests. This is correctness qualification; there is no timed result.

The source change alone cannot satisfy the acknowledgement gate. Its subsequent
measurement requires a recorded bounded protocol and an available exclusive
host. Retain direct raw acknowledgements, the full workload, checkpoint/wait
counters, archive progress, source and executable digests. Disclose throughput
separately; a tail movement still needs a named mechanism under ADR-0249.

## September 30 integration qualification

The landing packet at
`release/evidence/wp-749/archive-selector-landing-20260930/` records full qualification of `6d26f3b93`, including main `49fb4c291`. The
archive runtime and selector proofs are unchanged from `f77159c0`. All thirteen
steps of `./scripts/acceptance --wp WP-749 --base 49fb4c291 --full` pass with
Rust 1.98.1 and four test-harness workers. The audit retains all 86 named archive
proofs, both real CLI restore tests, and hashes of the current proof sources.

Integration exposed a fixture watchdog mismatch: normal debug daemon startup
allowed 60 seconds, while two bootstrap attachment waits allowed 30. Two full
attempts hit the first attachment cutoff. A diagnostic run passed all 25
replication tests and reached the checked attachment at 27,015 ms; it does not
prove the cause of the earlier timeouts. Commit `b9b5ac260` shares the existing
60-second startup budget with those two waits. Explicit channels still select
the crash boundary, and the exact prefix, lineage and recovery assertions remain.
Command progress, process-kill, RPC and product deadlines are unchanged. The
probe was removed before final full CI. Failed logs and the diagnostic are
retained by digest, alongside three runs stopped when main changed.

A separate intermediate run exposed the old cache architecture assertion after
main reduced the primary cache budget. Main `29933f6e` already corrected it to
check both explicit primary and derived budgets; final qualification includes
that repair. The existing handbook search-index size warning remains informational.

Main then added `5c3dfd9f3`, an opt-in numeric error probe in the
`benchmark-support` feature, integrated as `e6c06a6a2`. The archive code and
all 26 audited proof sources remain byte-identical to the full-CI source.
The follow-up runs storage Clippy with every target and feature, all 13
benchmark-support unit tests, and the authority-snapshot corruption/refusal
integration test. The landing packet records this separately: full CI ran on
`6d26f3b93`; targeted checks cover the later benchmark-only change.

This lands a correctness-qualified implementation and its historical evidence.
It does not satisfy or amend WP-749's acknowledgement-latency gate, mark the
package complete, or merge the shared-effect experiment.

## PR note

Package: WP-749. Tier: guarantee, implementing accepted ADR-0178. Behavior:
reuse durable manifest bytes for selector publication. Checks: all thirteen
full-acceptance steps and complete CI pass on the integrated source, including
86 named archive proofs and the repaired bootstrap recovery harness. The earlier
1,051-test storage run and syscall census remain bound to their original source.
The later benchmark-only update has separate Clippy and targeted test results.
No performance verdict or completion claim.
Compatibility: existing names,
bytes, readers and retry semantics remain. Hazards: the latency gate is still
unmet and sharing an inode requires retaining immutable-file discipline.
Handbook: [backup and restore](../backup-restore.md) describes the unchanged
operator contract and selector compatibility.
