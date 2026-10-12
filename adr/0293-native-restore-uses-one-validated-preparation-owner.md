---
adr: "0293"
title: Native Restore Uses One Validated Preparation Owner
status: accepted
tier: guarantee
date: 2026-10-12
accepted: '2026-10-12'
acceptance: 'maintainer, in session, 2026-10-12: Accept ADR-0293 as written, in response to the exact-text review linked from WP-825-NATIVE-OWNER-REVIEW.md; proposed text SHA-256 b58d08cea2353aad8f1d1b701e12271ffb2b6b1e82752659f5cad8c80569b553'
requires: [ADR-0290, ADR-0291]
amends: [ADR-0290, ADR-0291]
supersedes: []
requirements: [REC-001, REC-004, REC-005, REC-006, REC-008, END-001]
packages: [WP-825]
obligations: []
review_triggers:
  - A reused history proof crosses an unclassified write or uncertain custody.
  - Authorization reads survive the stamp or an open engine grants publication.
  - A history reference survives removal of its source extent.
  - Artifact checks, crash reconciliation, assurance or a qualification gate change.
---
# ADR-0293: Native Restore Uses One Validated Preparation Owner

## Context

After ADR-0290 and ADR-0291, the saved populated ordinary restore still takes
75.815435474 seconds and the unchanged primary-loss recovery gate fails. Private
phase instrumentation on the required host attributes approximately 14 seconds
to history hydration, nine to watermark preparation, four to materialized
history verification, six to the stamp transaction, five to authorization-owner
close/sealing and nine to publication. These phases overlap at nested boundaries;
they are not additive lower bounds or qualification evidence.

The current lifecycle materializes HISTORY receipts that the required
RestoreAnchor subsequently deletes. It also repeats complete history checks
across one exclusively owned private transformation, then closes and reopens the
same engine between authorization and stamping. Removing these costs requires
explicit proof ownership and a combined mutation boundary, not a weaker parser,
a bare cached root, deferred required recovery or a relaxed deadline.

## Decision

1. Apply this protocol only to ordinary native self-contained restore. Preserve
   complete immutable backup verification, copy verification, nonrepairing
   private physical checking, complete journal recovery, bounded readiness,
   fresh staged authorization and final self-contained publication. Archive,
   follower, promotion, upgrades and explicit full-scrub paths retain their
   existing complete protocols. Unsupported states use the existing complete
   path or refuse; they cannot acquire this proof by claiming compatibility.
2. Keep one private engine under continuous native custody through preparation,
   validation, authorization and the receipt-authorized stamp. It exposes no
   operational ports. Before stamping, consume every authorization principal,
   bearer, snapshot, read pin and subordinate engine lease. Observe unique
   ownership; dropping a wrapper is not evidence that another reader is gone.
3. A complete retained-history validation may mint a private, nonserialized,
   operation-local proof for that exact engine, database, incarnation, catalog,
   lineage, tail/frontiers, native mutation generation and cancellation/repair
   state. Its constructor must perform every existing retained-chain,
   source-hold, registration-link, primary-admission and cutover check. A
   bounded root decode or a valid frame checksum cannot mint this proof.
4. Reuse that proof only while the native owner proves the same relevant state.
   Each transaction checks its current bounded roots and ownership/generation
   against the proof before use. The existing materialized-journal inspection
   still verifies complete framing, checksums, bindings and exact materialization.
   Release its read pins before any write. No proof grants replay authority or
   the retained decoder's semantic shortcuts.
5. A known watermark transaction may advance the proof only by the exact
   validated canonical receipt and state transition already required today.
   Preserve the complete original fencing checks, tombstone truth, rooting
   digest, permissible minimum, incarnation binding, Immediate two-phase commit
   and uncertainty. An equal-watermark retry may reuse the completed same-state
   proof; it cannot omit a previously unperformed required check. Journal replay,
   a compatibility change or any other relevant mutation invalidates reuse
   unless its exact successor is established by this protocol. Otherwise perform
   fresh complete validation or refuse, never silently refresh a stale token.
6. Hydrate every command/history value that must survive final publication. A
   copied HISTORY receipt may remain a stub in the private stage while its
   original copied sealed extent is retained and verified. Fully validate the
   resolved original receipt chain before authorization. The existing stamp
   must remove that predecessor HISTORY and install its canonical RestoreAnchor
   atomically. Do not serialize predecessor receipts solely to delete them.
   Retain all source extents until no surviving command or receipt references
   them; malformed, missing, mismatched or unresolved references refuse.
7. Immutable copied extents retain their own verified content seals and original
   descriptors under pinned directory/inventory custody. A primary-only native
   mutation does not advance their content generation. At each handoff and
   controlled boundary verify identity, length, modification/change times,
   inventory and cancellation; unsupported metadata uses full hashing. Full
   content verification still precedes first use. An unexpected extent change
   invalidates the operation and cannot be authorized by taking a new seal.
   This narrows ADR-0290's aggregate-generation rule only for these immutable
   copied source extents, which are removed before publication. If retaining
   descriptors cannot fit existing limits, use complete verification or refuse;
   do not raise the descriptor ceiling.
8. Establish the required full primary/journal/marker seal after private
   validation. After read release, fully verify it before one controlled span
   containing the unchanged receipt-authorized stamp and its engine close.
   Persist the chosen incarnation and manifest/database binding in the existing
   receipt before entering that span. Transaction-current history checks may
   consume Decision 3's same-state complete proof; recheck affected bounded
   roots, retention rebind, RestoreAnchor and replication reset. The open
   owner is never a closed publication proof. This replaces the separate
   pre-stamp engine close/reopen and its intermediate full seal in ADR-0290.
9. Inside that span permit only the existing stamp, observed engine-close
   effects and final source-extent retirement/journal renewal required to make
   the result self-contained. Check the immutable files against their prior
   proofs before retiring or replacing them. Verify any required journal
   successor's complete framing, bindings and checksum. Unclassified writes,
   repair, cancellation, unexpected identity changes or I/O uncertainty
   invalidate the owner. No final checksum launders such a failure.
10. After all controlled work and observed engine close, establish the final
    full seal of the exact published primary, journal and marker. Publication
    retains its complete immutable-backup revalidation, full input check,
    independent synchronized-destination readback, target exclusion,
    marker-last ordering, directory synchronization, rollback and uncertainty.
    Preserve the retained private stage used by existing crash reconciliation;
    this record does not replace publication copying with links or renames.
11. Keep fresh receipt-bound published startup, durable dirty consumption and
    ADR-0291's first-access/background-scrub assurance. Create no CLEAN record,
    checkpoint or registry-completion witness. Keep durable encodings, public
    selectors, authorization policy, command ordering and acknowledgement
    semantics unchanged. Application authors cannot supply or retain any proof.
12. Restart discards every ephemeral proof and rebuilds from the original
    immutable backup under existing receipt authority. Repeated or failed
    attempts retain the same operation identity and exactly-once incarnation
    choice. Preserve all original failed observations. Keep the 60-second clock,
    workload, resource bounds and stronger focused replay/reconciliation gate.
    Measure the smallest faithful failing gate first, then qualify the actual
    scheduled-fault population. This decision makes no timing qualification.

## Options considered

1. **Keep separate owners and repeated complete checks:** simplest ownership
   boundaries, but retains the measured redundant transformation work.
2. **Cache roots, omit history checks or fake a clean record:** cannot establish
   complete validation or preserve failure semantics; rejected.
3. **One native owner with typed same-state evidence:** selected. Explicitly
   changes intermediate proof boundaries while retaining final content checks.
4. **Publish by moving/linking the stage:** would require a different durable
   crash-reconciliation protocol; deferred rather than hidden in this change.

## Consequences

- Removes materialization of discarded predecessor receipts, repeated same-state
  history walks and a redundant engine close/reopen. Actual savings must be
  measured; no estimate replaces the whole recovery gate.
- Adds a narrow private proof lifecycle whose invalidation is mandatory. It is
  not a general mutation callback, process cache or application escape hatch.
- Copying, surviving-command hydration, complete required history/fencing and
  physical checks remain proportional to retained data. The final scheduled
  workload must qualify; fixed live ticket count is not a backup-size bound.
- No new durable record, compatibility fixture, crypto algorithm, critical
  dependency, worker pool, cache ceiling or paid infrastructure is introduced.

## Standing design tests

- **Interface safety:** only the native restore owner can originate and consume
  these proofs. All public recovery, authorization, idempotency, durability and
  scope rules remain; no caller chooses partial validation or skips a checksum.
- **Scale:** retain one bounded decoded history value/receipt and constant-size
  semantic proof state and the existing bounded artifact inventory. Keep
  existing hydration batches, recovery memory admission,
  queues, scans, waits, diagnostics and resource ceilings. No population cache.

## Checks

- First reproduce the saved populated failure with the ordinary daemon. Then
  rerun that exact gate after each change before full CI or fresh population.
- Count complete history walks per unchanged generation; corrupt middle rows,
  source holds/links, admission/cutover records and journals must still refuse.
- Exercise a no-op watermark, a genuine canonical watermark transition,
  compatibility/replay invalidation and an unclassified same-length mutation.
- Retain authorization readers deterministically: stamping must refuse until
  the last pin is released; an open owner cannot enter publication.
- Compare resolved receipts before the stamp and the final self-contained
  command values after it. Refuse missing extents or any surviving stub before
  retirement. Preserve command replay, provenance and full state reconciliation.
- Process-crash cuts cover authorization release, durable incarnation choice,
  stamp transaction, engine close, extent retirement/renewal, final sealing,
  publication, dirty consumption and terminal receipt. Retry advances once.
- Re-run existing source/target mutation, archive/follower, compatibility,
  uncertainty and corruption checks; update operational documentation. Scoped
  acceptance and full CI on the final source precede fresh qualification.
