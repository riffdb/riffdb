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
    says: Every read checks the exact locator and complete idempotency identity first; prior locators remain visible and malformed or mismatched locators fail closed.
  - id: OBL-0197-5
    package: WP-778
    proof: fresh_locator_coverage_classifies_every_publication_rebase_failure_and_restart
    says: Every publication lane, checkpoint rebase, failure, uncertainty, invalidation, and restart preserves coverage only under the closed rules or disables it.
  - id: OBL-0197-6
    package: WP-778
    proof: cold_fresh_database_publications_complete_without_history_scans
    says: Bounded concurrent fresh-database command publication completes with dormant transient indexes and no history fallback scan.
review_triggers:
  - Coverage would initialize outside the first mutation-gated command-write entry, from a checkpoint, locator-table cardinality, nonempty frontier, retained history, or another process's evidence.
  - An operational miss could mean absence without its exact public-prefix proof, a transaction-adjacent miss could extend coverage without its exact private-chain witness, or a malformed, mismatched, or prior locator could avoid CorruptData.
  - Transaction ordering, conflict ownership, transaction-adjacent idempotency revalidation, command batching, acknowledgement, or outcome sequencing would change.
  - Queued coverage would advance before ADR-0100 publication, direct coverage before exact commit/root refresh, or either would survive an unclassified lane, uncertainty, or failed rebase; state would be unbounded/durable or a witness Clone, Copy, serializable, publicly constructible, or cross-role.
  - Public audit or SubscribeCommits first-demand reconstruction, any public or operator surface, or any durable, journal, Protobuf, storage-key, or registry byte would change.
---
# ADR-0197: Fresh-Process Contiguous Idempotency Locator Coverage

## Context

On a clean bounded start, ADR-0165 keeps command-derived population indexes
dormant. That is an operational-read baseline, not proof that a write-side
absence is complete: the first command write still opens under the mutation
gate and resolves exact committed identities through durable locator rows.
For a genuinely new idempotency identity, however, operational direct inspection
falls back to scanning preceding command segments. The later
transaction-adjacent revalidation instead point-reads the locator and returns
`Ok(None)` on a miss. At 213 committed rows of a fixed 65,536-row qualification,
repeated operational history work stopped progress within the diagnostic bound.

ADR-0165 does not authorize an absence inference from a validated-prefix
checkpoint. Its additive locator tables are empty for pre-ADR histories and
old clean certificates remain valid, so neither checkpoint frontier nor table
cardinality proves that every historical command owns a locator. The narrower
case in which activation proves there has never been an application command
has an exact inductive proof. Adopting that proof changes admission read
validation and therefore cannot be selected by an implementation package.

ADR-0165 also contradicts itself: Decision 5 and Consequences prove the
additive tables change no registry digest and preserve eligible clean-close
certificates, while its later relationship paragraph says this upgrade changes
the registry chain and invalidates those certificates. If accepted, this record
replaces that later sentence with the exact correction marked in ADR-0165.

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
   PublicPrefixThrough(P)` plus an inaccessible rebind-in-progress state. It
   retains one fixed-size `PrivateChainWitness` for the newest writer-private
   composite view. A public proof may answer only an operational locator miss
   captured from its exact published view/frontier; a private witness may only
   extend the exact writer chain. Neither stores a key, locator, segment, or
   history collection.

4. Sealing a command frame consumes the matching `PrivateChainWitness` and
   returns two different, fields-private capabilities: the replacement private
   witness for that successor, and one `CommandPublicationWitness` moved into
   that frame's `CommandPublication` FIFO payload. A service-audit frame instead
   returns a private replacement plus a `PreservePublicationWitness` proving its
   application frontier unchanged. These types implement no `Clone`, `Copy`,
   `Default`, serialization, or public constructor/conversion. Dropping,
   mismatching, queue-draining, or failing to consume one disables coverage.

5. Thus pipelined A then B is linear: A consumes private P and queues publish
   witness P->A while installing private A; B consumes private A and queues
   A->B while installing private B. Publication consumes the distinct FIFO
   witnesses P->A then A->B. A private witness cannot publish, a publication
   witness cannot extend a writer view, and no one transition capability is
   duplicated or forged.

6. A command witness is constructible only from the canonical sealed command
   segment and the same composite mutation stage. Checked arithmetic requires
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
   same snapshot. Direct/hardened audited-capsule publication consumes an
   equivalent nonqueued witness around its existing `Immediate` commit/refresh.
   Neither path moves batching, transaction, conflict, durability,
   acknowledgement, outcome, notification, or changelog ordering.

10. Current root-refresh lanes are closed. Deferred service audit consumes its
    FIFO preserve witness. Immediate admission reservation, execution-failure
    terminalization, service audit, catalog activation, capability bootstrap,
    grant, revoke, and query/reactive-module publication may rebind only after a
    named precommit allowlist proves their tables exclude command segments and
    locators and their application frontier/command-authority generation is
    unchanged. Projection/outbox/consumer, columnar control, installation, and
    export lanes use that same preserve rule.

11. Before any preserving Immediate commit, the public proof moves into an
    unusable rebind token; exact commit plus root refresh restores it, and any
    mismatch or uncertainty disables before a new-root read. Direct/hardened
    audited command publication uses Decision 9. Storage-format/contract
    migration or import, uncapsulated or unwitnessed command writes, restore/root
    replacement, invalidation, unknown Immediate/queued lane, gap/duplicate,
    fence, uncertainty, or dropped witness disables before visibility. Every
    offline retention hold/prune opens an exclusive store without inherited
    proof and drops its state before the next store opens.
    Disabled is monotonic. Pre-arm control writes cannot arm and are assessed by
    the exact first-command transaction in Decision 2.

12. Checkpoint rebase snapshots generation, public composite identity, and both
    Option frontiers under the short publication lock, then releases it for all
    checkpoint I/O/waits. Old-view reads retain only the old proof. At the
    existing rebase publication edge, exact old-view/generation/frontier equality
    atomically rebinds the proof; definite pre-publication failure leaves the old
    proof/result, while uncertainty, mismatch, partial handoff, poisoned lock, or
    post-publication failure disables and retains existing fencing. No transient
    lock crosses I/O.

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

1. **Fresh empty-process induction:** selected. It proves a complete base
   without scanning or durable evidence and extends only across bounded atomic
   publications produced by the same process.
2. **Seed from validated-prefix checkpoint or locator cardinality:** rejected.
   Pre-locator databases can carry valid checkpoints, and a count is not an
   exact key-set proof.
3. **Rebuild or retain the command-derived index:** rejected for this path. It
   restores population-linear time and memory and does not address repeated
   fallback work while the index is deliberately dormant.
4. **Persist a coverage marker or backfill history:** deferred. Either changes
   durable compatibility and requires a separate accepted decision.

## Consequences

- Fresh databases can publish large bounded command populations without an
  O(history) absence scan per candidate while retaining dormant indexes.
- The optimization may be derived anew after restart only while authority is
  still exactly empty; it does not apply to any nonempty history.
- Any inability to prove the induction loses only the optimization and remains
  fail closed; it never manufactures absence or repairs authority.
- WP-705 must rerun its exact qualification commands on the accepted
  implementation revision. Prior failed diagnostic runs remain non-evidence.

## Standing design tests

- **Interface safety:** The proof is storage-private and automatic. No caller can
  arm, preserve, reset, inspect, or bypass it, change the existing write-side
  miss result, or skip transaction-adjacent idempotency validation.
- **Scale:** State is one frontier, generation, and closed tag. Fixed tokens are
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
