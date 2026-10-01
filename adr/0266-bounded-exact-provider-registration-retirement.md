---
adr: "0266"
title: Bounded Exact Provider Registration Retirement
status: accepted
tier: guarantee
date: 2026-10-01
accepted: '2026-10-01'
acceptance: 'maintainer, in session, 2026-10-01: Accept all three as written (ADR-0265, ADR-0266, ADR-0267)'
requires: [ADR-0130, ADR-0164, ADR-0231]
amends: [ADR-0130]
supersedes: []
requirements: [PRJ-001, PRJ-004, OQ-040]
packages: [WP-824]
obligations: []
review_triggers:
  - State is shared across unequal plan, partition, role, or capability identities.
  - Continuations switch epochs or eviction bypasses minimum retention.
  - Accounting excludes retiring state, scratch, or incomplete background work.
---
# ADR-0266: Bounded Exact Provider Registration Retirement

## Context

Exact-provider state is keyed by plan, partition, role, and optional capability
identity/revision. Each registry refuses its 257th registration even when older
registrations are obsolete. The selected milestone reclaims this capacity
without weakening authorization or continuation semantics. This record is
proposed; runtime changes require exact human acceptance.

## Decision

1. Share one lifecycle/budget owner across existing exact-text, predicate,
   nullable-predicate, tokenized-text, and long-pattern registries. Preserve
   their physical implementations and exact identity keys; no cross-policy reuse.
2. Keep each 256-slot ceiling. Add a 1-GiB aggregate process-graph bound for
   provider ownership, decoded checkpoints, retained epochs, build/catch-up
   scratch, and retired referenced state. Existing smaller bounds still apply.
   The bound is compiled, not an application or operator tuning parameter.
3. Reserve conservative maximum incremental bytes before allocation. Charge
   capacities/ownership, not only payloads; shared allocations have one owner.
   Release reservations on failure and charged bytes only after final reference
   destruction. Checked arithmetic refuses overflow and prevents double release.
4. Owners transition active -> retiring -> released. Removal from discovery
   does not release bytes. Captured queries, workers, and retained epoch leases
   carry explicit pins. Retiring owners admit no new pin; worker publication
   rechecks retirement. Cancellation releases only the caller's resources.
5. Retire superseded plans and capability revisions after exact identity checks.
   Captured operations continue only under normal fresh authorization; obsolete
   state never revives revoked authority.
6. Under pressure evict the least-recently-used eligible owner, breaking ties
   by canonical key. Internal monotonic access ordinals are rebased under the
   registry lock before overflow, preserving order. Eligibility requires no
   in-flight operation/publication, protected cursor, or unexpired minimum
   retention obligation. Use the existing service clock owner for lease time.
7. When no eligible owner can release sufficient resources, return existing
   typed unavailable/capacity failure. Never evict protected epochs, increase
   scan ceilings, or expose an authoritative-scan fallback. Concurrent protected
   demand beyond 256 slots is still unsupported.
8. Later access to evicted state uses ordinary Building/rebuild admission and
   a fresh generation. A continuation keeps its exact epoch and authority or
   fails typed; it never negotiates a substitute snapshot.
9. Stop obsolete workers at bounded safe points. Reclaim derived artifacts
   using existing directory-anchored no-replace conventions after all captured
   references release. Crash recovery discards partial derived work. This cannot
   delete authoritative rows, command evidence, or history holds.
10. Expose bounded active/retiring/reserved/pinned/released counts and byte
    charges through operator diagnostics, never keys, tenant/capability values,
    paths, or payloads. Applications cannot select budgets, pins, or eviction.

## Options considered

- Larger caps postpone accumulation; policy sharing lacks an equivalence proof.
- Opportunistic cursor eviction weakens retention. Both are rejected.
- Eligible-owner retirement with explicit accounting is selected.

## Consequences

- Sequential churn can recover capacity; protected saturation remains a refusal.
- The aggregate budget may refuse demand that previously fit separate caps.
  This explicit availability change requires human acceptance.
- No new grammar, query family, durable provider format, or SDK method.

## Standing design tests

- **Interface safety:** preserve policy-before-observation, fresh authorization,
  exact snapshots, bounded work, and typed refusal; no public cache controls.
- **Scale:** include every retained owner and worker in one finite budget;
  reclamation work is bounded by existing registry ceilings.

## Checks

- More than 256 sequential registrations recycle capacity after retention;
  256 protected registrations refuse a successor without invalidating a cursor.
- Deterministic capture/retire/rebuild/cancel races and eventual byte release.
- Capability/plan churn, scratch admission, corrupted checkpoints, and restart.
- Independent row/order/count/authority/epoch oracle before and after eviction.
- Scoped acceptance, provider conformance, and full CI.
