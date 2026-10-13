---
adr: "0295"
title: Consumed Native Restore Uses Ordinary Engine Crash Recovery
status: accepted
tier: guarantee
date: 2026-10-12
accepted: '2026-10-13'
acceptance: 'maintainer, in session, 2026-10-13: I accept them all, in response to the exact ADR-0295 review; proposed text SHA-256 bc94b55348da24df345515b5f11e5a5de5818a525ae41928e21577e55957f6a6'
requires: [ADR-0294]
amends: [ADR-0294]
supersedes: []
requirements: [REC-001, REC-004, REC-005, REC-006, REC-008, END-001]
packages: [WP-825]
obligations: []
review_triggers:
  - Engine recovery is permitted before durable StartupConsumed or on a private stage.
  - Native crash recovery substitutes for complete current-state validation.
  - A receipt or intent permits backup republication over the consumed target.
  - Application admission, authorization, timing or resource guarantees change.
---
# ADR-0295: Consumed Native Restore Uses Ordinary Engine Crash Recovery

## Context

ADR-0294 Decision 9 requires a complete nonrepairing native startup after an
interrupted StartupConsumed operation. An actual process-abort test exposes a
conflict: complete startup validation commits its dirty-generation record while
the ordinary maintenance receipt remains Validating. Killing that process leaves
the actual published database requiring redb's ordinary engine crash recovery.
The nonrepairing reopen returns CorruptData before semantic validation can run,
even though the publication owner and database inode remain correct.

A diagnostic on the same crash cut uses the existing ordinary engine recovery
policy, then completes the entire current structural and catalog validation.
It preserves the published inode, database, incarnation, application frontier,
immutable backup and nonterminal receipt. This demonstrates a necessary recovery
step, not a timing qualification or permission to admit unvalidated state.

## Decision

1. Amend only ADR-0294 Decision 9's nonrepairing requirement for a restarted
   nonterminal operation whose StartupConsumed intent was already durable before
   the current attempt. Open its actual published target with the same native
   engine crash-recovery policy used by ordinary source startup. Do not introduce
   a second recovery algorithm, new dependency or public policy selector.
2. Before that open, retain the complete closed intent/receipt inventory checks,
   exact operation and immutable-backup binding, directory and file ownership,
   native exclusion, single-link identities and exactly one permitted published
   artifact set. The consumed target is the only recovery source. Never rebuild
   it from the original backup or demand its obsolete primary and journal seals.
3. A freshly Published intent transitioning to StartupConsumed in this attempt
   still uses the nonrepairing engine open after original artifact verification.
   Private preparation, hydration, authorization, stamping and physical evidence
   retain their existing nonrepairing policies. A repair callback in those paths
   remains a refusal; this amendment does not broaden their authority.
4. Ordinary engine crash recovery restores the native container's committed
   state; it supplies no semantic validation certificate. A restarted consumed
   operation must complete current structural, journal and catalog validation
   before terminal success. Clean-close, checkpoint and bounded dirty-startup
   shortcuts cannot replace that complete walk, even if an earlier attempt
   finished validation before crashing.
5. Validate exact database identity, incarnation, application frontier, restored
   history lineage and retention watermark against the consumed intent. Internal
   administrative progress may advance but must not precede the recorded
   frontier. No application write is admitted while recovery is nonterminal.
   Recheck the required bindings through completion using existing validation.
6. Unrecoverable native damage, malformed journal or rows, checksum failure,
   conflicting ownership, changed bindings, cancellation and incomplete evidence
   remain typed fail-closed outcomes. Preserve actual target artifacts, intent,
   original immutable backup and failure observations. Native engine recovery
   cannot waive a finding or authorize semantic repair, destructive reset,
   artifact replacement or fallback publication.
7. Preserve ADR-0294's admission sequence: complete validation, durable ordinary
   Succeeded receipt, matching intent removal and directory synchronization,
   then application admission. A crash before terminal success repeats recovery
   and complete validation on the actual target. After Succeeded, cleanup and
   ordinary startup cannot republish the original backup or overwrite later
   acknowledged writes.
8. Report actual native recovery and validation through existing bounded,
   redacted diagnostics. Their time remains inside the applicable recovery
   clock; no cost is hidden in setup or an unmeasured retry.
9. Preserve the frozen intent and receipt encodings, compatibility fixtures,
   authorization, ordering, atomicity, idempotency and acknowledged durability.
   Keep the 60-second gate, fixed campaign windows, scheduled population and
   resource ceilings unchanged. This record grants no timing or campaign claim.

## Options considered

1. **Keep nonrepairing consumed restart:** rejects a legitimate crash after a
   native startup commit; it cannot fulfill interrupted restore recovery.
2. **Recover and use a startup shortcut:** avoids the required complete
   validation of changed consumed content; rejected.
3. **Ordinary engine recovery plus complete current validation:** selected;
   separates necessary container recovery from semantic admission evidence.
4. **Rebuild the backup or defer validation:** could discard acknowledged state
   or weaken admission; rejected and outside this amendment.

## Consequences

- Legitimate nested startup crashes can recover their actual committed target
  without losing the operation's publication ownership or exactly-once stamp.
- Engine recovery may modify a previously consumed primary or journal. Those
  files remain the recovery source; obsolete pre-startup hashes grant no proof.
- Complete validation remains on the critical path and grows with retained
  state. This amendment makes no performance promise or deadline extension.
- No new dependency, unsafe Rust, cryptography, durable format, application
  interface, worker pool or cache is introduced. Production activation and
  Succeeded cleanup still need their own complete implementation and checks.

## Standing design tests

- **Interface safety:** applications cannot select recovery policy, create an
  intent, provide validation proof, override a finding or write before admission.
  Existing administrative authorization and private ownership boundaries apply.
- **Scale:** retain one bounded intent, three artifacts and existing descriptor,
  streaming validation, memory, scan and diagnostic bounds. No population cache,
  increased concurrency, raised resource ceiling or shortened walk is added.

## Checks

- The existing failing test
  `native_publication_startup_interrupted_dirty_generation_recovers_the_consumed_current_target`
  must pass through the native restore entry point after an actual process abort
  following the dirty-generation commit, preserving the target inode and bindings.
- The diagnostic
  `native_publication_startup_diagnostic_ordinary_engine_recovery_validates_the_consumed_target`
  establishes the existing recovery algorithm on that cut; it does not qualify
  the unmodified native entry point or substitute for the preceding regression.
- Retain actual process-abort coverage before engine open and while it is open,
  and add repeated interruption during recovery, before Succeeded, during intent
  cleanup and after the first acknowledged write. Recover the same operation
  and incarnation without original-backup republication or early admission.
- Prove current nonroot corruption, journal damage, conflicting ownership and
  unrecoverable native damage refuse; prove fresh and private opens still refuse
  engine repair. Retain full exact-end, catalog and frontier validation checks.
- Required-host scoped acceptance and final-source full CI precede activation;
  document operator behavior and rerun the saved failing recovery gate before
  broader qualification. WP-825 remains open until its unchanged exit gate.
