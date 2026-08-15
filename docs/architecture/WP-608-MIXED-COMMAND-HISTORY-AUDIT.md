# WP-608 mixed command-history persistence audit

Upstream revision: `25c4ac9a`

This is an internal storage-boundary audit. It does not add or change an
application-facing interface.

## Caller inventory

- The production commit coordinator calls
  `commit_with_service_audit_transitions` (or its deferred equivalent). It
  supplies the exact `Started` and terminal lifecycle needed to build a
  canonical command capsule.
- The redb simulation smoke, recovery oracle, seeded campaign, framework
  concurrency schedules, and adapter paths use that same audited completion
  path. The audit found older command-concurrency fixtures that still selected
  direct completion; WP-608 migrates those persistent entity fixtures to the
  production-shaped audited lifecycle.
- The in-memory backend retains calls to `NonEmptyCommandBatch::commit` for
  memory-only model tests. It has no redb entity-chain-head table and is not a
  durable-history caller.
- The only retained redb surface that can omit audit transitions is the storage
  trait's direct `NonEmptyCommandBatch::commit` implementation. It is a
  lower-level conformance fallback, not a public application command path.

## Durable-history finding

Staging an entity mutation creates an exact `CommittedEntityTransitionV1`,
updates the private entity post-image and `ENTITY_CHAIN_HEADS`, and retains the
transition in `BatchCore::capsule_entity_transitions`. The audited completion
path embeds those transitions in the authoritative V2 command capsule before
durability.

The direct non-audited completion path instead materialized the frozen complete
outcome, provenance, event, outbox, and V1 commit rows. A V1 commit contains
post-image references but not the transition hash needed to walk the entity
chain. Committing that path therefore advanced `ENTITY_CHAIN_HEADS` without
retaining its proof. A later audited command could validly extend the stored
head and then fail startup with `MissingCrossLink` when validation encountered
the unprovable predecessor.

## Selected compatibility disposition

Redb now refuses every entity-bearing direct non-audited batch with
`InvariantViolation` while the write transaction is still private. The
storage-only path remains available for entity-free legacy conformance records.
RiffDB does not synthesize an audit lifecycle, weaken startup validation, or
introduce a second transition format.

An isolated schedule proved that even a first entity-bearing direct completion
advances a chain head without a walkable transition and cannot pass the current
startup proof. There is therefore no valid legacy entity-history mode to
preserve or classify. Production audited command semantics are unchanged and
the refusal adds no work to their path.

## Evidence

- `uncapsulated_then_audited_entity_history_refuses_before_mutation_and_reopens_clean`
- `audited_then_uncapsulated_entity_history_refuses_before_mutation_and_reopens_clean`
- `entity_history_never_commits_without_walkable_transition_capsules`
- The existing startup entity-chain corruption corpus continues to require
  `MissingCrossLink` for genuinely absent transition evidence.

The both-order schedules prove that the refused attempt advances neither
entity state, idempotency authority, nor the application allocator, and that
the accepted audited history reopens through the normal structural validator.
