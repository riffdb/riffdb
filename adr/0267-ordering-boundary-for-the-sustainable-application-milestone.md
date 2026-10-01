---
adr: "0267"
title: Ordering Boundary For The Sustainable Application Milestone
status: accepted
tier: guarantee
date: 2026-10-01
accepted: '2026-10-01'
acceptance: 'maintainer, in session, 2026-10-01: Accept all three as written (ADR-0265, ADR-0266, ADR-0267)'
requires: [ADR-0003, ADR-0086, ADR-0186]
amends: []
supersedes: []
requirements: [LOG-001, REP-001]
packages: [WP-821]
obligations: []
review_triggers:
  - Database-wide ordering is weakened or existing tokens are reinterpreted.
  - Distribution, consensus, or replicated acknowledgement is implemented.
---
# ADR-0267: Ordering Boundary For The Sustainable Application Milestone

## Context

Compiler locality and database-wide sequencing are different guarantees. The
milestone preserves current semantics while making future partition-placement
assumptions explicit. This proposed record authorizes no runtime change.

## Decision

1. Keep LOG-001, current database-scoped tokens, and one writer per database.
   Existing asynchronous followers do not imply replicated acknowledgement.
   No consensus, sharding, cross-route transaction, or automatic failover.
2. R-13 stays open. Independently sequenced routes need a separately accepted
   ordering/compatibility design. Adding a route field to a scalar token alone
   cannot preserve its previous database-wide ordering meaning.
3. Route-local candidates are command atomicity, conflicts, and ordinary mutation
   dependencies. Catalog/capability changes, database freshness, idempotency
   namespaces, global changelog/export, backup cuts, and promotion require an
   explicit coordination or separately accepted compatibility decision.
4. Two-route case: a current token after an A command identifies its position
   in the one database history, including earlier B commits. Independent A/B
   counters need additional ordering or consistent-cut machinery to retain this
   meaning. The future design must select and prove that machinery first.
5. A command spanning routes A/B remains rejected. A common catalog/authority
   change must not let one owner admit a plan or capability already retired
   by the applicable authority; fresh authorization and exact identity survive.
6. Leader-failure case: a durable command losing its reply resolves the same
   idempotency identity and outcome. A future replicated mode must place its
   durable quorum decision before acknowledgement and apply the same canonical
   transition deterministically. Today's local journal plus async changelog
   does not establish this property.
7. Movement/promotion must fence the old owner before new authority and define
   token, outcome, and event continuity. Existing manual promotion remains under
   its accepted decisions. This record does not pre-approve a distributed design.
8. Continue application features within current ordering. Change no protocol,
   durable bytes, or public behavior. Do not claim route placement is proven
   semantics-neutral or expose per-route sequence arithmetic as current behavior.

## Options considered

- Build distribution now: exceeds the selected milestone.
- Treat locality as proof of scale: omits database-wide observation/control.
- Keep current order and enumerate future obligations: selected.

## Consequences

- Single-writer execution is intentional for this milestone.
- R-13 remains unresolved; accepting this record is not a replication design.

## Standing design tests

- **Interface safety:** no weaker acknowledgement, authority bypass, token
  reinterpretation, or cross-route write is introduced.
- **Scale:** identify rather than conceal global assumptions; make no new
  placement or throughput guarantee.

## Checks

- Record A/B ordering, catalog change, cross-route rejection, lost reply, and
  former-owner fencing cases in the architecture gate review.
- Preserve existing sequence, freshness, idempotency, and promotion tests.
- Milestone evidence cannot claim replicated acknowledgement or alpha release.
