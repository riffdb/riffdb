# WP-825 restore recovery decision review

Status: the maintainer accepted the exact text of ADR-0290, ADR-0291 and
ADR-0292 in session on 2026-10-12. Implementation and verification are in progress.

The saved populated fixture and ordinary binaries at `9a795b034` reproduce an
ordinary restore component of 162.707925093 seconds. The unchanged combined
60-second gate failed during restore after 17.481282666 seconds of backup
creation. Its failures and original media remain preserved. Neither the actual
eight-hour rehearsal nor the 24-hour campaign has started.

The maintainer supplied an independent reproduction of about 165 seconds and
a frame-pointer profile of the same revision. Its approximate phase attribution
is 42 seconds copying/preparing history, 34 seconds in the first full semantic
walk, 40 seconds rehashing custody seals, eight seconds publishing, and another
34 seconds in ordinary reopen validation. These are profile observations, not
new qualification receipts or timing lower bounds.

## Accepted decisions

1. [ADR-0290](../../adr/0290-restore-content-seals-at-mutation-boundaries.md)
   replaces full hashing at unchanged private handoffs with descriptor and native
   mutation continuity checks. Full hashes remain at mutation and publication
   boundaries. Metadata is not claimed to prove byte equality, and a fresh seal
   cannot legitimize an unexpected mutation.
2. [ADR-0291](../../adr/0291-bounded-restore-readiness-and-background-validation.md)
   extends the existing ordinary dirty-startup model to ordinary self-contained
   restore. It keeps artifact hashes, nonrepairing private physical verification,
   journal/history recovery, bounded roots and fresh staged authorization before
   publication. Population semantic validation moves to first access and the
   existing background scrub. A successful ordinary restore consequently ceases
   to assert a complete semantic scrub before publication. Explicit offline
   scrub, archive restore, upgrades and follower paths retain their full proofs.
3. [ADR-0292](../../adr/0292-recovery-clock-begins-at-primary-loss.md)
   measures the unchanged 60-second target from loss of the primary, with a
   previously completed durable backup. Target setup, transfers, validation,
   authorization, publication and first successful write all count. The focused
   gate keeps replay and full reconciliation inside that same deadline. Backup
   preparation and its interruption remain separately visible and accounted.

The second decision is a deliberate assurance change, not proof that the staged
and reopened database are identical. Incarnation stamping and RestoreAnchor
construction mutate the staged database after validation. Reusing an old proof
or merely writing CLEAN would not establish the successor's readiness.

## Implementation and immediate verification

Implement each accepted change separately and test the previous failure first.
Use the preserved populated fixture with renewed public credentials, copied to
an owned scratch target. Preserve original failure media and independent hashes.
Only the required 8-vCPU, 32-GiB Linux SSD host performs builds, tests and
repository scripts; no local-disk test runs.
Keep access details and private build locations outside the repository.

1. Add deterministic seal-pass and mutation-generation coverage, reduce only
   unchanged-handoff hashes, build the affected ordinary binary and rerun the
   saved-backup restore. Report component timing; do not claim the whole gate.
2. Add native bounded-restore and corruption/authorization oracles, implement
   bounded staged and post-publication startup, and directly rerun the restore
   reproduction. Prove no hidden population counts, locator construction or
   complete reopen walk before readiness. Test valid-hash semantic corruption
   for fail-closed first access and the existing degraded scrub health state.
3. Amend clock/fault evidence with deterministic event-order tests. Bind a
   durable backup and acknowledgement frontier before injection. Reject a gap
   in acknowledged writes, a reset clock or a new backup selected after loss.
   Run the full public recovery gate directly against the saved population.
4. If the gate still fails, profile the remaining copy, history hydration,
   physical/journal checks, seals and publication. Optimize within accepted
   proof boundaries and rerun that failing check first. Do not repeat the load,
   language smoke or full CI merely to investigate a still-failing restore.
5. After the direct failure is resolved, run required semantic/crash/compatibility
   coverage, scoped acceptance, final full CI, public documentation and fresh
   frozen qualification on the final combined source. Then run the actual
   eight-hour rehearsal and, after WP-825 closes, the uninterrupted 24-hour run.

The two largest redundancies are actionable, but 25 seconds is an estimate to
test. The accepted policy still retains history hydration, physical verification
and publication. A fixed live ticket population does not bound retained outcomes,
provenance or backup size; the actual scheduled-fault population must qualify.
Background-scrub interference remains subject to the continuing-load gates.

## Review scope and checks

All three records are accepted. Their acceptance metadata and package
registration were recorded in a standalone governance commit. Existing package
scope includes storage, server, scripts, tests and documentation; these records
are the implementing package's own consequence decisions. ADR-0292 also belongs
to WP-826. Acceptance authorizes implementation; it does not qualify the new
behavior or revise any previous failed result.

Updated handbook pages after implementation must explain ordinary restore's
bounded assurance, first-access failure, background scrub and explicit offline
full-assurance operation. Qualification documentation must distinguish the old
combined clock from the new primary-loss clock. No old failed result becomes a
passing result by subtracting backup time.

## ADR-0290 implementation checkpoint

Package: WP-825. Tier: guarantee. WP-825 remains open.

Unchanged private handoffs now check descriptor identity, length, modification
and change times, native mutation generation, cancellation and repair latches.
They reuse the established seal; unsupported metadata falls back to hashing.
Full content verification remains before controlled engine close, incarnation
stamping and publication. Closed successors retain immutable journal/marker
checks and the exact bounded post-preparation extent inventory.

The first candidate incorrectly compared that inventory with the original
backup's extent count. Existing history hydration deliberately consumes copied
extents before validation. The populated reproduction exposed the refusal; a
small native regression now reproduces that lifecycle and proves its correction.
The original backup still passes complete artifact verification and is unchanged.

Checks on the required host: ordinary release daemon build; 28 focused native
materialization regressions; allowed paths and whitespace. Coverage includes
unchanged-handoff hash counts, unsupported-metadata fallback, same-length changes
with restored modification times, stale generations, mandatory full boundary
checks, added extents and the post-hydration inventory. The saved populated
restore then completed in 142.007646638 seconds with ordinary readiness observed.
The earlier ordinary component observation was 162.707925093 seconds; neither
component observation qualifies the whole recovery gate. The original combined
60-second reproduction still failed during restore and remains preserved.

Compatibility: no durable format, fixture, public selector, authorization,
ordering, acknowledgement, complete validator or recovery clock changed in this
checkpoint. This is an internal custody-protocol change under ADR-0290; handbook
assurance changes belong to the pending ADR-0291 implementation.

Hazards and follow-ups: implement ADR-0291 and ADR-0292, then rerun the direct
whole public gate. Scoped acceptance, full CI, required crash/compatibility checks
and fresh qualification remain pending on the final combined source. The actual
eight-hour rehearsal and 24-hour campaign have not started. Existing failed
attempts are not revised or reclassified.
