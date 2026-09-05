---
adr: 0197
title: Fresh-Process Contiguous Idempotency Locator Coverage
status: proposed
tier: guarantee
date: 2026-09-05
accepted: null
requires: [ADR-0004, ADR-0006, ADR-0058, ADR-0100, ADR-0101, ADR-0104, ADR-0156, ADR-0157, ADR-0165]
amends: [ADR-0165]
supersedes: []
requirements: [OUT-001, OUT-002, TXN-042, REC-004, PERF-019]
packages: [WP-778]
obligations:
  - id: OBL-0197-1
    package: WP-778
    proof: fresh_locator_coverage_arms_at_first_command_write_only_from_exact_empty_authority
    says: Process-local locator coverage initializes at the first mutation-gated command write only from exact empty authority; None is empty and zero is invalid.
  - id: OBL-0197-2
    package: WP-778
    proof: fresh_locator_private_chain_and_fifo_publications_are_linear
    says: Distinct move-only private-chain and per-publication witnesses preserve every exact A-to-B link; a command FIFO advances public coverage only at its ADR-0100 edge.
  - id: OBL-0197-3
    package: WP-778
    proof: fresh_locator_write_miss_retains_current_semantics_and_gates_only_coverage
    says: Transaction-adjacent locator misses keep their existing result; only a matching private-chain witness permits the coverage induction to survive them.
  - id: OBL-0197-4
    package: WP-778
    proof: fresh_locator_miss_preserves_prior_identity_and_rejects_malformed_locators
    says: Before inferring absence, every read checks the exact durable locator and validates any located capsule's complete idempotency identity; prior locators remain visible and malformed or mismatched locators fail closed.
  - id: OBL-0197-5
    package: WP-778
    proof: fresh_locator_coverage_classifies_every_publication_rebase_failure_and_restart
    says: Every publication lane, checkpoint rebase, failure, uncertainty, invalidation, and restart preserves coverage only under the closed rules or disables it; Direct postcommit public-prefix operational-miss use and private-chain continuation share the exact successor, and wrong-role, root, frontier, or allocator evidence fails closed.
  - id: OBL-0197-6
    package: WP-778
    proof: cold_fresh_database_publications_complete_without_history_scans
    says: Bounded concurrent fresh-database command publication completes with dormant transient indexes and no history fallback scan.
review_triggers:
  - Coverage would initialize outside the first mutation-gated command-write entry, from a checkpoint, locator-table cardinality, nonempty frontier, retained history, or another process's evidence.
  - An operational miss could mean absence without its exact public-prefix proof, a transaction-adjacent miss could extend coverage without its exact private-chain witness, or a malformed, mismatched, or prior locator could avoid CorruptData.
  - Transaction ordering, conflict ownership, transaction-adjacent idempotency revalidation, command batching, acknowledgement, or outcome sequencing would change.
  - Queued coverage would advance before ADR-0100 publication, direct coverage before exact commit/root refresh, either role would cross a rebase without its matching predecessor, or coverage would survive an unclassified lane, uncertainty, or failed rebase; state would be unbounded/durable or a witness Clone, Copy, serializable, publicly constructible, or cross-role.
  - Public audit or SubscribeCommits first-demand reconstruction, any public or operator surface, or any durable, journal, Protobuf, storage-key, or registry byte would change.
---
# ADR-0197: Fresh-Process Contiguous Idempotency Locator Coverage

## Context

On a clean bounded start, ADR-0165 keeps command-derived population indexes
dormant. That operational-read baseline does not prove write-side absence: the
first command write still resolves exact identities through durable locator rows.
For a new identity, operational direct inspection scans preceding command
segments, while later transaction-adjacent revalidation point-reads the locator
and returns `Ok(None)` on a miss. At 213 of 65,536 rows, repeated operational
history work stopped progress within the diagnostic bound.

ADR-0165 does not authorize absence from a validated-prefix checkpoint. Locator
tables are empty for pre-ADR histories and old clean certificates remain valid,
so neither frontier nor cardinality proves every historical command has a
locator. Proving at the first command-write entry that no application command has ever existed does
give an induction, but changes admission validation and requires this decision.

ADR-0165 also contradicts itself: Decision 5 preserves registry digest and
eligible clean-close certificates, while its relationship paragraph says the
opposite. If accepted, this record applies the exact correction marked there.

## Decision

1. The sole initialization site is `RedbOperationalPorts::admit_or_resolve_group`,
   immediately after its existing `begin_write()` and before
   `stage_admission_group`. `begin_write` already owns the mutation gate, rejects
   a write fence, publishes the pending journal prefix, polls/takes any completed
   checkpoint, and applies that suffix before returning `RedbWriteAccess`; this
   decision adds no drain or ordering edge. The check uses the writer-private
   redb transaction in that access, not `BatchCore::open_with_access`, operational
   activation, or the later `begin_deferred_epoch` composite stage.

2. On only the first such admission entry, the transaction must prove the
   application frontier is `None`, `COMMITS`, `IDEMPOTENCY`,
   `IDEMPOTENCY_PENDING`, and `IDEMPOTENCY_LOCATORS` contain no command
   authority, the allocator is `Next(first)`, transient indexes are `Dormant`,
   and the handle remains unfenced. Failure sets process-lifetime `Disabled`.
   Frontiers use `None < Some(1) < ... < Some(u64::MAX)`; `None` is zero
   commands, raw `Some(0)` is corruption, and successor exhaustion disables and
   returns the existing typed sequence failure.

3. The fields-private state is `Uninitialized | Disabled |
   PublicPrefixThrough(P)` plus inaccessible rebind states and a process-local
   monotonic `CoverageEpoch`. Armed state retains both the public proof and one
   fixed-size `PrivateChainWitness` for the newest writer-private view. A public
   proof may answer only an operational miss from its exact published
   root/frontier; a private witness may only extend its exact writer chain.
   Neither stores a key, locator, segment, or history collection.

4. Sealing a queued command consumes its matching `PrivateChainWitness` and
   returns a replacement private witness plus a distinct
   `CommandPublicationWitness` moved into that frame's FIFO payload. A queued
   service audit consumes its matching `PrivateChainWitness` and returns a
   private replacement plus a distinct
   `PreservePublicationWitness`. Those roles and the direct witness in Decision
   9 implement no `Clone`, `Copy`, `Default`, serialization, public constructor,
   or cross-role conversion. Dropping, mismatching, or failing to consume one
   disables coverage.

5. Thus pipelined A, B, then C is linear: each consumes private P, A, then B,
   installs private A, B, then C, and queues separate P->A, A->B, then B->C
   publication witnesses. FIFO publication consumes those exact witnesses.
   Preserve frames compose the same way without advancing the application
   frontier. No private capability publishes, no publication capability extends
   a writer view, and no transition capability is duplicated or forged.

6. A queued command witness is constructible only from the canonical sealed
   command segment and the same ADR-0104 composite mutation stage. Arithmetic
   requires
   `first = successor(predecessor)` (`1` after `None`) and
   `count = last - first + 1`. Segment span, retained count, and stage count
   agree. Each sequence has exactly one canonical `IdempotencyIdentityKey`
   locator decoding to `StoredCommandLocatorV1(sequence)` and a capsule matching
   sequence plus database, environment, tenant scope, principal, contract
   lineage, command id, and keyed caller-key digest.

7. Transaction-adjacent behavior does not change. The current
   `command_outcome_from_write_indexes` order remains: transient, unpublished,
   and active-epoch member lookup; exact durable locator point read; complete
   located-capsule/identity validation; then `Ok(None)` on a locator miss. There
   is no write-side history fallback. When coverage is armed, only the matching
   private witness permits that miss to extend the induction; without it,
   coverage becomes `Disabled` before the existing admission result proceeds.

8. Operational pre-admission behavior also retains its result algebra. After
   its exact locator point read, a matching `PublicPrefixThrough(P)` may prove a
   miss only for the exact captured published view with frontier `<= P`, avoiding
   the current bounded history scan. Without that proof the existing operational
   scan and its existing result/failure remain. Found locators always resolve
   and validate; malformed, duplicate, wrong-key, wrong-sequence, wrong-capsule,
   or wrong-identity evidence remains `CorruptData`.

9. Standard command publication consumes its queued command witness only after
   the journal fence matches, at the existing
   `publish_pending_command` composite-successor edge; public coverage advances
   only after that successor is public, beside the ADR-0100 observation of the
   same snapshot. Direct/hardened uses a distinct `DirectCommandWitness`: its
   constructor requires a direct `RedbWriteAccess` with no composite stage, an
   empty publication FIFO, matching public/private predecessor root, frontier,
   and allocator, and the canonical audited segment, transaction-local count,
   locators, span, and identities from Decision 6. It consumes both predecessor
   roles before `Immediate` commit; exact commit/root refresh and expected
   frontier/allocator produce both successor roles. Neither role alone can mint
   the other, no ADR-0104 stage/order edge is repurposed, and failure disables.

10. Current root-refresh lanes are closed. Deferred service audit consumes its
    FIFO preserve witness. A closed `PreservingImmediateClass` names admission,
    execution failure, service audit, catalog, query/reactive module, capability
    bootstrap/grant/revoke, projection, outbox, consumer, columnar control,
    installation, and export. Before staging, its typed request creates a
    fields-private permit containing the exact allowed `(table, encoded key,
    insert/delete)` set; every mutation is registered and must be in that set,
    and architecture checks forbid an unregistered/raw mutation in these lanes.
    The set must exclude `COMMITS`, `IDEMPOTENCY_LOCATORS`,
    `PROVENANCE_LOCATORS`, `AUDIT_BY_REQUEST_LOCATORS`, and
    `META_APPLICATION_SEQUENCE`; pre/post point reads must prove byte-identical
    allocator and application frontier. Any extra mutation or mismatch aborts
    and disables before commit, with no population scan.

11. A preserving Immediate lane requires an empty publication FIFO and matching
    public/private predecessor root and frontier. It consumes both roles and its
    Decision 10 permit into one inaccessible pair token before commit. A definite
    precommit abort restores that same predecessor pair; exact commit/root
    refresh consumes the token and returns both roles bound to the exact
    successor. Neither role is cloned, converted, or reconstructed from the
    other; mismatch or uncertainty disables before a new-root read.
    Direct/hardened audited publication uses Decision 9. Storage-format/contract
    migration or import, uncapsulated or unwitnessed command writes, restore/root
    replacement, invalidation, unknown Immediate/queued lane, gap/duplicate,
    fence, uncertainty, or dropped witness disables before visibility. Every
    offline retention hold/prune opens an exclusive store without inherited
    proof and drops its state before the next store opens.
    Disabled is monotonic. Pre-arm control writes cannot arm and are assessed by
    the exact first-command transaction in Decision 2.

12. At an existing checkpoint/root-rebase edge with no pending publication, the
    short publication lock moves both roles into one pair token recording
    `CoverageEpoch`, both predecessor identities and Option frontiers, then is
    released for all I/O/waits. A definite pre-publication failure restores the
    same old-view pair. Exact success comparison consumes the token and restores
    both roles on the exact rebased successor; neither is minted from the other.
    Uncertainty, mismatch, partial handoff, lock poison, or post-publication
    failure disables and retains existing fencing. No lock or new drain crosses
    I/O, and old-view reads can use only the token's old public proof.

13. Crash or close drops state and all capabilities. A later process may arm
    only through Decisions 1-2; nonempty, restored, migrated, retained,
    dirty-recovered, upgraded, or pruned history disables it. Checkpoints, clean
    certificates, locator counts, and prior-process evidence never arm it.

14. Verification is bounded by the existing 256-transition and 16-MiB frame
    ceilings. One private witness and one fixed-size witness per already-bounded
    publication entry are retained; there is no population-, duration-, or
    restart-proportional state. Public audit and `SubscribeCommits` retain their
    accepted first-demand reconstruction. No public/operator API, durable byte, registry,
    storage key, journal, or protocol changes.

15. WP-778 implements only this decision. WP-705 separately reruns fixed-scale
    lifecycle qualification after WP-778 closes. WP-578 separately owns 72-hour
    evidence; neither evidence package is satisfied or closed here.

## Options considered

1. **Fresh empty-process induction:** selected; a scan-free complete base extends
   only across bounded atomic publications produced by the same process.
2. **Checkpoint or locator cardinality:** rejected; pre-locator databases can
   carry valid checkpoints, and a count is not an exact key-set proof.
3. **Rebuild or retain the derived index:** rejected; it restores
   population-linear work while the index is deliberately dormant.
4. **Persist a marker or backfill:** deferred as a durable-compatibility change.

## Consequences

- Fresh databases avoid O(history) absence scans while indexes remain dormant.
- Restart may rederive the optimization only from exactly empty authority.
- Lost proof loses only the optimization; it never manufactures absence.
- WP-705 must rerun exact qualification on the accepted implementation.

## Standing design tests

- **Interface safety:** The proof is storage-private and automatic. No caller can
  arm, preserve, reset, inspect, or bypass it, change the existing write-side
  miss result, or skip transaction-adjacent idempotency validation.
- **Scale:** State is fixed public/private identities, epoch, and closed tag. Fixed tokens are
  bounded by the existing epoch/publication queue and 256-command/16-MiB frame
  ceilings; no population-proportional collection is retained or scanned.

## Checks

- `fresh_locator_coverage_arms_at_first_command_write_only_from_exact_empty_authority`.
- `fresh_locator_private_chain_and_fifo_publications_are_linear`.
- `fresh_locator_write_miss_retains_current_semantics_and_gates_only_coverage`.
- `fresh_locator_miss_preserves_prior_identity_and_rejects_malformed_locators`.
- `fresh_locator_coverage_classifies_every_publication_rebase_failure_and_restart`.
- `cold_fresh_database_publications_complete_without_history_scans`.
- The storage recovery matrix and full recovery suite preserve crash,
  uncertainty, atomicity, and idempotent replay semantics.
- Architecture checks freeze the absence of durable/public coverage state and
  any call from public audit or `SubscribeCommits`.
