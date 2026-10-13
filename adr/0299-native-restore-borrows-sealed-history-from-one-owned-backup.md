---
adr: "0299"
title: Native Restore Borrows Sealed History From One Owned Backup
status: accepted
tier: guarantee
date: 2026-10-13
accepted: '2026-10-13'
acceptance: 'maintainer, in session, 2026-10-13: Accept ADR-0298 as written and its WP-825 registration; proposed text SHA-256 8d9c127a15c678d31cfad6199f0b39b47cff54fd15c31424732938db8900a78c'
requires: [ADR-0290, ADR-0291, ADR-0292, ADR-0293, ADR-0294, ADR-0295]
amends: [ADR-0290, ADR-0293, ADR-0294]
supersedes: []
requirements: [REC-001, REC-004, REC-005, REC-006, REC-008, END-001, END-008]
packages: [WP-825]
obligations: []
review_triggers:
  - Backup bytes can change without invalidating their retained owner.
  - Borrowed history survives publication or becomes a source write capability.
  - A bounded root decode replaces complete history or artifact validation.
  - A descriptor, memory, scan, worker or timing ceiling increases.
  - A durable encoding, application selector or compatibility boundary changes.
---
# ADR-0299: Native Restore Borrows Sealed History From One Owned Backup

## Context

The unchanged saved primary-loss gate passes at shipping revision `aacaaf54`:
first acknowledged write at 54.59 seconds, exact replays and all 65,536 ticket
checks at 58.07 seconds. The fixed 45-second campaign window and its larger
scheduled population remain unqualified. The passing fixture has 150,000
updates and is not a substitute for the scheduled 5,400-second population.

On the latest merged source `42a524b2`, private phase probes measure a 47.77-second
restore component. Materialization takes 18.95 seconds, including 8.82 seconds
staging sealed extents; complete retained-history capture takes 11.29 seconds.
The staging measurement includes copying, hashing and frame checks, so it is
not an estimate that all 8.82 seconds can disappear. Large-file hash passes read
18.76 GB and take 13.11 seconds. Fresh target rehashing accounts for about one
second. These diagnostic probes qualify no recovery deadline or campaign.

ADR-0293 retains verified copied sealed extents until history hydration and the
receipt-authorized RestoreAnchor make them unnecessary. Copying immutable
history only to read and discard it is avoidable work, but replacing those
copies with original backup files changes custody and needs this decision.

## Decision

1. Apply this protocol only to the existing eligible ordinary native,
   self-contained empty-replacement restore. Archive, follower, promotion,
   upgrades, explicit full scrub and unsupported states retain their existing
   complete protocols. No public caller may select a weaker mode.
2. Establish one private, operation-local owned backup image. Bind the original
   operation, canonical manifest, database, incarnation, frontiers and exact
   bounded artifact inventory to original read-only descriptors and pinned
   directories. Require regular single-link files and exact descriptor/name
   identity. Include primary, active journal, marker and every sealed extent;
   a manifest decode or ordinal inventory alone is not a complete content proof.
3. Before first use, fully verify every backup artifact against its existing
   manifest checksum, with the existing journal framing, footer, database,
   generation and canonicality checks. Check identities and metadata before and
   after verification. Do not open an engine on, write, rename, link, repair or
   otherwise mutate any original backup artifact.
4. Copy and synchronize the private primary, active journal and marker exactly
   as required today, and verify the copied bytes. For eligible sealed history,
   replace the physical private copies with retained original backup descriptors
   and their verified seals. Private resolution must bind each reference to its
   exact generation and recorded artifact; no ambient path or caller-supplied
   resolver may redirect a read. This amends ADR-0293 Decisions 6–7's copied-file
   requirement only for these temporary private history sources.
5. Reuse the existing immutable-extent continuity rules on those originals:
   verify exact inventory, descriptor/path identity, single-link status, length,
   modification and change times, cancellation and custody before and after
   reads and at every handoff. Read positions must be independently owned;
   shared descriptor offsets cannot race. Unsupported identity or metadata uses
   the existing complete-copy protocol before acquiring this authority.
6. Keep complete retained-chain, source-hold, registration-link, primary-admission
   and cutover validation on the critical path. Preserve every command/history
   value that must survive publication. Reuse only ADR-0293's exact same-state
   complete proof under its transaction-current roots and mutation checks.
   Neither a valid checksum nor the borrowed owner grants semantic validation,
   replay authority or permission to omit a previously unperformed check.
7. Keep fresh staged authentication and authorization, consume all principals,
   bearers, snapshots and engine leases before stamping, and retain the one
   observed private engine through the existing receipt-authorized stamp and
   close. Preserve watermark fencing and rebind, canonical RestoreAnchor,
   replication reset, Immediate two-phase transactions and uncertainty.
8. Before releasing borrowed extents, prove that no surviving command, receipt
   or published state references them. Malformed, missing, mismatched or
   unresolved references refuse. Drop only private leases and bookkeeping;
   never remove or alter the original backup files. The final published set
   remains exactly primary, active journal and marker and is self-contained.
9. Reuse backup seals at unchanged handoffs only under the actual continuously
   retained owner. Preserve a complete original-backup content revalidation at
   publication, including all sealed artifacts. A fresh same-process publication
   may retain its actual verified backup and three-artifact owners into startup
   consumption instead of reconstructing and rehashing them. Every continuity
   and closed receipt/intent inventory check still precedes consumption.
10. Preserve full final source seals, independent complete post-transfer
    readback, native exclusion, operation-bound no-overwrite moves, marker-last
    ordering, file/directory synchronization and typed publication uncertainty.
    The frozen intent still records the original three seals. No metadata check
    can authorize a replacement checksum or stand in for either full boundary.
11. Durable StartupConsumed still precedes engine open. Preserve ADR-0291's
    fresh readiness and ADR-0295's complete current-state consumed restart.
    Admission still requires fresh published startup, durable Succeeded,
    matching intent removal and directory sync. Retained file owners confer no
    readiness, clean-close, checkpoint or registry-completion certificate.
12. These owners and proofs are never serialized or reconstructed from a receipt.
    After a pre-publication restart, revalidate the complete original backup and
    acquire fresh custody under the same operation/incarnation authority. After
    Prepared or Published, recover only the actual intent-bound artifacts with
    full required original seals. After StartupConsumed, the actual target is
    the sole recovery source; never republish the backup over it or later writes.
13. An unexpected write, replacement, link, repair, inventory change, cancellation
    or uncertain I/O invalidates the owner and preserves failure evidence. No
    resealing, copying fallback or new checksum may launder that failure.
    Original backup retention and the existing recovery/polling policy remain.
14. Keep the existing descriptor, memory, scan, buffer, queue and worker ceilings.
    Borrowed descriptors replace temporary-copy ownership slots; implementation
    must account for all overlapping backup, private and publication owners.
    If custody cannot fit existing limits, retain complete copying or refuse.
    Add no population cache, worker pool, dependency or unsafe Rust.
15. Preserve public interfaces, authorization, ordering, atomicity, idempotency,
    acknowledged durability, durable encodings and compatibility fixtures.
    Keep the one primary-loss 60-second clock, exact replays/full-state checks,
    fixed 45-second campaign window, scheduled population and all resource
    ceilings. This record grants no performance, rehearsal or 24-hour claim.

## Options considered

1. **Keep temporary extent copies:** existing custody is simple but copies
   immutable history that the final restore intentionally removes.
2. **Read ambient backup paths or trust manifest hashes:** loses actual custody
   or complete validation; rejected.
3. **Borrow verified original descriptors under one owner:** selected; removes
   temporary copies while retaining content, semantic and publication checks.
4. **Only retain the fresh published target owner:** a smaller useful handoff,
   but the measured target rehash is about one second and leaves staging cost.
5. **Defer required history checks or relax deadlines:** rejected.

## Consequences

- Removes sealed-history copy work, not its required verification or decoding.
  A timing improvement and success on the larger population remain experiments.
- Requires closed read-only resolution and explicit inventory/lease retirement;
  original backup corruption must refuse even when private primary bytes match.
- No new on-disk state, application capability or persisted validation proof.

## Standing design tests

- **Interface safety:** applications cannot choose borrowing, supply a source
  descriptor, create proof, skip validation or write before ordinary admission.
- **Scale:** existing artifact and descriptor ceilings govern the whole lifetime;
  prove bounded overlap, cancellation and disposal without increasing them.

## Checks

- First add failing semantic tests proving eligible restore reads original sealed
  descriptors without creating private extent copies and publishes no references.
- Compare complete copied and borrowed restore results with an independent oracle:
  exact outcomes, provenance, events, retained history, roots and incarnation.
- Refuse same-length mutation with restored modification time, replacement,
  links, missing/extra files, checksum/footer/generation errors and shared-offset
  races at every handoff, including mutation after verification and publication.
- Count full content checks and actual ownership slots; read-only handoffs cannot
  add population walks, hide a required hash or exceed existing capacity.
- Exercise cancellation, unsupported fallback and process-crash cuts during
  preparation, stamp, extent release, transfer, consumed startup and post-ACK
  restart. Retain old copied, archive/follower and compatibility checks.
- Test the closest saved failure before broader work; then required-host scoped
  acceptance, final-source full CI, the actual scheduled population and every
  unchanged gate precede the eight-hour rehearsal and 24-hour campaign.
