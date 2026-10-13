---
adr: "0294"
title: Native Restore Transfers One Sealed Artifact Set Into Publication
status: accepted
tier: guarantee
date: 2026-10-12
accepted: '2026-10-12'
acceptance: 'maintainer, in session, 2026-10-12: Accept ADR-0294 as written, in response to the exact-text review linked from WP-825-PUBLICATION-TRANSFER-REVIEW.md; proposed text SHA-256 1cb928dca0d928fe9bb11faf2fd0ec1d5576a747925afd69f5501413f60070d3'
requires: [ADR-0289, ADR-0290, ADR-0291, ADR-0293]
amends: [ADR-0289, ADR-0290, ADR-0293]
supersedes: []
requirements: [REC-001, REC-004, REC-005, REC-006, REC-008, END-001]
packages: [WP-825]
obligations: []
review_triggers:
  - Publication or restart trusts bytes absent from the durable artifact seal.
  - A startup or terminal receipt can authorize overwriting acknowledged writes.
  - A prior reader can remove intent-owned files before refusing the new protocol.
  - Full content verification, authorization, compatibility or a timing gate changes.
---
# ADR-0294: Native Restore Transfers One Sealed Artifact Set Into Publication

## Context

The saved populated restore still fails the unchanged 60-second gate. A separate
attribution run completes first write at 59.539 seconds and exact replays plus
all 65,536 ticket checks at 62.350 seconds. Its larger diagnostic budget grants
no recovery qualification. Complete predecessor validation takes about 12 seconds;
publication, including checks that remain required, previously takes about nine.
The campaign's fixed 45-second fault window also remains unqualified.

ADR-0293 deliberately retains a second complete publication copy and its private
stage as crash evidence. The maintenance receipt contains manifest, database and
incarnation bindings, but no final artifact content seals. Moving that stage
without new durable evidence would break restart reconciliation. The decision
below replaces the duplicate copy with explicit, recoverable ownership transfer;
it does not assume that all measured publication time disappears.

## Decision

1. Add a private native publication protocol only for an ordinary self-contained
   restore into an empty replacement, after complete required preparation,
   history/journal/physical checks, fresh authorization, read release, the
   receipt-authorized stamp and observed unique engine close. The three final
   artifacts remain primary, journal and format marker. No engine is open during
   transfer. Archive, follower, promotion, nonempty replacement, upgrades and
   unsupported filesystem states retain their existing complete copy protocol.
2. Amend ADR-0289 Decisions 8–9, ADR-0290 Decisions 7–8 and ADR-0293 Decisions
   10 and 12 only for this protocol: the retained byte-for-byte stage, second
   publication copy and backup-rebuild restart become a durable sealed-artifact
   intent plus recovery of the actual owned artifacts. Preserve complete immutable-backup
   revalidation, final full source seals, target exclusion, marker-last ordering,
   directory synchronization and publication uncertainty. No required history
   check moves to the background. ADR-0291's assurance boundary remains intact.
3. Before the first transfer, durably create a versioned, checksummed intent
   binding the original operation, backup manifest, staged database, chosen
   incarnation, restored frontiers and exactly three ordered artifact kinds,
   lengths and existing SHA-256 seals. Its closed encoding contains no ambient
   path, credential, user selector or serialized process proof. Bound one record
   to 4 KiB and one active operation. Reject duplicates, extra fields/artifacts,
   unknown versions, conflicting bindings and noncanonical encodings.
4. Place the intent and its temporary records under the existing receipts
   directory with distinct closed suffixes. Supported prior readers must refuse
   this inventory before cleanup, database open or mutation. Prove this with a
   genuine prior-reader fixture. Do not write the new protocol where this
   refusal cannot be established. Existing receipt, backup, journal, registry and
   catalog encodings remain unchanged. Fixture diffs require separate review.
5. The intent has monotone Prepared, Published and StartupConsumed states. Each
   replacement is synchronized before its successor can act. Prepared is durable
   before any artifact move; Published follows complete marker-last publication
   and directory synchronization; StartupConsumed is durable before a native
   engine may modify the published artifact set. An intent never grants
   application readiness or replaces the ordinary maintenance receipt.
6. Keep original descriptors, single-link regular-file identities, pinned source
   and destination directories, native exclusion and cancellation through the
   move. Admit only same-filesystem transfer with the target still empty. Use
   capability-relative operations and operation-bound temporary names. Recheck
   the exact source and destination identities around every move. Unknown
   replacement, link, write, repair or cancellation invalidates authority.
7. Transfer the three sealed artifacts into owned target temporaries without
   copying their contents. Synchronize the affected source and destination
   directories and retain exact artifact ownership. Before publishing any target
   artifact, perform a full content read of all three transferred files against
   the pre-transfer durable intent seals. A rename's metadata change cannot
   authorize a new checksum. Preserve file synchronization and marker-last
   publication. No hard links, writable aliases or relaxed native locks.
8. Reconciliation validates the whole intent/receipt inventory before cleanup.
   For Prepared or Published, locate each artifact only at its closed permitted
   stage, temporary or published position; require exactly one matching owned
   instance, verify its full content seal and all operation bindings, and resume
   only the recorded transfer. A missing, duplicated or changed artifact refuses
   and preserves evidence. Partial marker publication cannot grant normal source
   startup. Retry retains the same operation and exactly-once incarnation.
9. Once StartupConsumed is durable, the published database is the only possible
   recovery source for that operation. Do not demand the obsolete primary byte
   seal after permitted native startup writes, and never rebuild over it from
   the backup. A restarted nonterminal operation performs the existing complete
   nonrepairing native startup, journal recovery and semantic validation before
   terminal success, with exact database/incarnation/frontier binding. This
   transition does not itself certify those checks or permit a bounded restart
   from unverified changed content. Refusal remains fail-closed.
10. Keep the listener and application writer fenced until fresh published startup
    succeeds and the ordinary Succeeded receipt is durable. Then remove the
    matching intent and synchronize its directory before operational admission.
    A crash after Succeeded but before cleanup can finish that cleanup and normal
    startup; it can never republish the original backup. Once writes are
    acknowledged, later startup follows their actual durable journal and state.
11. Keep original immutable backup bytes throughout nonterminal recovery. Failure
    before uncertain publication may roll back only positively identified owned
    temporaries. An uncertain move, sync or receipt write preserves the intent,
    remaining artifacts and original failure observations, and uses the existing
    typed uncertainty/polling path. No automatic destructive reset or new public
    retry policy. Fallback to copying is allowed only before creating the intent;
    it cannot hide a partially executed transfer.
12. Preserve the 60-second gate, the stronger focused replay/full-state checks,
    the fixed campaign fault windows, scheduled population, resource ceilings
    and all authorization, ordering, durability and idempotency guarantees. Test
    the saved failing gate first, then qualify the actual scheduled population.
    This record grants no timing, rehearsal or 24-hour readiness claim.

## Options considered

1. **Retain publication copying:** existing restart evidence is simple, but moves
   every final artifact through another content copy before recovery can finish.
2. **Rename without durable seals or link the stage:** restart loses independent
   artifact evidence or retains writable aliases; rejected.
3. **Journal the sealed set and transfer ownership:** selected. It changes a
   bounded native publication protocol while retaining full content checking.
4. **Defer predecessor checks or relax timing:** separate guarantee decisions;
   neither is authorized by this record.

## Consequences

- Removes one content copy for eligible empty replacements. Backup revalidation,
  post-transfer hashing and complete required history validation still cost time.
- Introduces one small native durable encoding and explicit restart states;
  old readers refuse while its intent exists. Review compatibility fixtures
  before implementation can merge.
- Existing copied publication remains available for other restore states. This
  record does not promise the 45-second campaign window will pass.
- No new dependency, cryptographic algorithm, unsafe Rust, worker pool, cache,
  descriptor ceiling, database format or application interface is introduced.

## Standing design tests

- **Interface safety:** application callers cannot create an intent, select a
  weaker restore mode, supply a seal or acquire publication authority. Existing
  public backup/restore selectors and administrative authorization remain.
- **Scale:** exactly three artifact entries and one bounded intent per active
  operation; existing retained descriptors, memory admission and diagnostic
  bounds. No population cache or queue/scan/deadline increase.

## Checks

- Freeze canonical V1 intent and genuine prior-reader refusal vectors; refuse
  malformed versions, lengths, hashes, ordering, identities and transitions.
- Process-crash cuts cover intent durability, every transfer and directory sync,
  content readback, marker publication, Published, StartupConsumed, dirty startup,
  Succeeded, cleanup and the first acknowledged write. Retry advances once.
- Change or replace primary/journal/marker at each ownership boundary, including
  same-length mutation with restored modification time; no fresh seal laundering.
- Exercise partial sets, extra files, foreign owners, live readers, cancellation,
  cross-filesystem fallback and uncertain synchronization. Preserve original
  media and refusals; no early application admission or acknowledged overwrite.
- Retain original copied-path, archive/follower/promotion, legacy compatibility,
  authorization, provenance, idempotency and full state reconciliation tests.
- Required-host scoped acceptance and final-source full CI precede qualification;
  update the operational handbook and measure every unchanged timing gate.
