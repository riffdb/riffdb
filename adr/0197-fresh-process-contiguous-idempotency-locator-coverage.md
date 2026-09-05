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
    proof: fresh_locator_public_prefix_advances_only_with_exact_published_successor
    says: Public-prefix coverage advances only after the exact contiguous locator-bearing successor becomes the ADR-0100 published durable frontier.
  - id: OBL-0197-3
    package: WP-778
    proof: sealed_successor_token_proves_only_its_exact_transaction_adjacent_view
    says: A sealed unpublished-successor token proves absence only for its exact writer view and cannot authorize a public or different-successor read.
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
  - A locator miss could mean absence without the matching public-prefix or exact sealed-successor token, or a malformed, mismatched, or prior locator could avoid CorruptData.
  - Transaction ordering, conflict ownership, transaction-adjacent idempotency revalidation, command batching, acknowledgement, or outcome sequencing would change.
  - Coverage would advance before ADR-0100 publication, survive an unclassified lane or uncertainty, retain unbounded state, cross a failed rebase comparison, or become durable.
  - Public audit or SubscribeCommits first-demand reconstruction, any public or operator surface, or any durable, journal, Protobuf, storage-key, or registry byte would change.
---
# ADR-0197: Fresh-Process Contiguous Idempotency Locator Coverage

## Context

On a clean bounded start, ADR-0165 keeps command-derived population indexes
dormant. That is an operational-read baseline, not proof that a write-side
absence is complete: the first command write still opens under the mutation
gate and resolves exact committed identities through durable locator rows.
For a genuinely new idempotency identity, however, the current admission read
falls back to scanning every preceding command segment. The direct inspection
batcher and the coordinator's transaction-adjacent revalidation both perform
that read. At 213 committed rows of a fixed 65,536-row qualification this
became repeated CPU-bound history work, not a lock wait, and command progress
stopped within the external diagnostic bound.

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

1. Initialization occurs exactly in `BatchCore::open_with_access`, after its
   `RedbWriteAccess` has acquired the exclusive mutation gate and completed the
   existing pending-publication/checkpoint barriers, but before allocator or
   admission reads. Operational activation does not arm it. On the first such
   command-write entry, one transaction view must prove: `COMMITS` has no
   application frontier, the application allocator is `Next(first)`, no command
   authority is present, the transient index is `Dormant`, and writes are not
   fenced. Otherwise it becomes permanently `Disabled` for that process.

2. Frontiers use the existing `Option<CommitSequence>` order:
   `None < Some(1) < ... < Some(u64::MAX)`. `None` means exactly zero published
   application commands. `CommitSequence` is nonzero, so raw `Some(0)` is
   corruption, never an alias for `None`; checked successor exhaustion disables
   the proof and returns the existing typed sequence failure. The fields-private
   state is `Uninitialized | Disabled | PublicPrefixThrough(frontier)` and holds
   no key, locator, command, segment, or history collection.

3. Two proofs are distinct. `PublicPrefixThrough(P)` applies only to operational
   reads whose captured published application frontier is `<= P`. A move-only
   `SealedSuccessorCoverage` applies only to the transaction-adjacent writer view
   whose fields-private composite-successor identity it binds; it names the
   public root, immediate predecessor, inclusive `first..=last`, nonzero count,
   and successor frontier. A next private seal consumes this token into one
   replacement token; tokens never form a retained collection. Neither token can
   be supplied to an operational read or a different successor.

4. A sealed token may be created only from the canonical command segment and
   same composite mutation stage after sealing. Checked arithmetic requires
   `first = successor(P)` (`first = 1` for `P=None`) and
   `count = last - first + 1`; segment first/last and independently retained
   command count must agree. For every sequence in that inclusive span, the
   successor must contain exactly one `idempotency_locators` row keyed by the
   canonical `IdempotencyIdentityKey` and decoding to
   `StoredCommandLocatorV1(sequence)`. The located capsule's sequence and full
   identity tuple—database, environment, tenant scope, principal, contract
   lineage, command id, and keyed caller-key digest—must equal the command.

5. The exact locator point read always precedes an absence proof. A found row is
   resolved and validated as ADR-0165 requires; malformed, duplicate, wrong-key,
   wrong-sequence, wrong-capsule, or wrong-identity evidence is `CorruptData`.
   A writer view must search the durable predecessor plus all staged mutations,
   so a locator in any prior public or private prefix cannot disappear. Only a
   point miss covered by the matching public-prefix token, or by the exact
   replacement sealed-successor token rooted in that public prefix, is absence.
   Otherwise the existing bounded fallback runs; if it cannot prove the answer, the
   existing typed failure is returned, never absence.

6. Publication consumes, rather than copies, a sealed token. The standard
   journaled command lane may advance public coverage only after its exact
   composite successor is public and its durable fence is verified. The
   existing ADR-0100 observer then receives that exact published snapshot. The
   direct/hardened audited-capsule command lane performs the same proof at its
   `Immediate` publication. Advancement remains inside the existing
   transient-index/unpublished-index publication critical section; no ordering,
   acknowledgement, batching, conflict, transaction, or changelog edge moves.

7. The remaining lanes are closed exhaustively. Service-audit-only publication
   preserves the application frontier and coverage only when its exact
   predecessor/successor comparison does too. The storage-only uncapsulated
   command path, migration command writes, any direct application-authority
   write without the sealed proof, an unknown future application-frontier lane,
   a gap, duplicate span, or proof mismatch disables coverage before that state
   can become visible. An execution-failure terminalization that advances no
   application frontier preserves coverage; any locator/transient invalidation,
   write-fence event, dropped fence/token, failed or uncertain commit or
   publication disables it. Disabled is monotonic until process death.

8. Checkpoint rebase changes physical roots and bytes, not logical application
   authority. It snapshots coverage generation, public composite identity and
   application frontier under the short publication lock, releases that lock
   before all checkpoint I/O or waits, then reacquires it. Coverage is rebound
   unchanged only if success proves the exact old view was rebased and generation
   and frontier are identical. A definite pre-publication failure with unchanged
   view leaves the proof unchanged while following the existing typed failure;
   an uncertain result, comparison failure, partial handoff, poisoned lock, or
   post-publication failure disables coverage and retains the existing write
   fence. No transient lock is held across I/O.

9. Crash or close drops all proof state and every unpublished token. A later
   process may derive a new `PublicPrefixThrough(None)` only by Decision 1; any
   nonempty, restored, migrated, retained, dirty-recovered, or upgraded history
   disables it. A checkpoint, clean certificate, locator cardinality, or prior
   process token never arms or restores coverage.

10. Verification is bounded by `MAX_GROUPED_WRITE_TRANSITIONS = 256` and the
   existing 16 MiB journal-frame bound. It uses the already bounded sealed
   segment/stage; no second unbounded decode is introduced. One fixed-size token
   may accompany each existing bounded pending-publication entry and one active
   epoch; publication consumes it, so retained memory is bounded independently
   of command population, duration, and restart count.

11. The proof is private to redb admission reads. Public audit and
   `SubscribeCommits` retain their accepted first-demand reconstruction.
   Applications, SDKs, transports, MCP, CLI, configuration, and operators gain
   no switch, status, receipt, warmup operation, fallback selector, storage
   handle, or authority.

12. WP-778 implements and proves this decision only. WP-705 depends on WP-778
   and separately owns the fixed-scale lifecycle qualification after it closes.
   WP-578 separately
   owns its uninterrupted 72-hour evidence; neither evidence package can be
   satisfied, relabelled, or closed by WP-778 tests.

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

- **Interface safety:** The proof is storage-private, automatic, and usable only
  for exact absence validation. No caller can arm, preserve, reset, inspect, or
  bypass it, select startup or reconstruction behavior, or skip the mandatory
  transaction-adjacent idempotency check.
- **Scale:** State is one frontier, generation, and closed tag. Fixed tokens are
  bounded by the existing epoch/publication queue and 256-command/16-MiB frame
  ceilings; no population-proportional collection is retained or scanned.

## Checks

- `fresh_locator_coverage_arms_at_first_command_write_only_from_exact_empty_authority`.
- `fresh_locator_public_prefix_advances_only_with_exact_published_successor`.
- `sealed_successor_token_proves_only_its_exact_transaction_adjacent_view`.
- `fresh_locator_miss_preserves_prior_identity_and_rejects_malformed_locators`.
- `fresh_locator_coverage_classifies_every_publication_rebase_failure_and_restart`.
- `cold_fresh_database_publications_complete_without_history_scans`.
- The storage recovery matrix and full recovery suite preserve crash,
  uncertainty, atomicity, and idempotent replay semantics.
- Architecture checks freeze the absence of durable/public coverage state and
  any call from public audit or `SubscribeCommits`.
