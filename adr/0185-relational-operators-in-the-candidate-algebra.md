---
adr: 0185
title: Relational Operators in the Candidate Algebra, Not Provider Families
status: proposed
tier: surface
date: 2026-09-02
accepted: null
requires: [ADR-0051, ADR-0054, ADR-0108, ADR-0111, ADR-0134, ADR-0150, ADR-0164, ADR-0174, ADR-0175]
amends: [ADR-0178]
supersedes: []
requirements: [RQL-008, OQ-113, OQ-114, OQ-115, OQ-116, OQ-117, OQ-118]
packages: [WP-768, WP-769, WP-770, WP-771]
obligations:
  - id: OBL-0185-1
    package: WP-768
    proof: expansion_plan_is_least_sufficient_and_legacy_bytes_are_unchanged
    says: A query without an expansion or existence operator keeps its exact RiffQL, IR, module, plan-hash, and cursor bytes; a query with one selects the additive successor identities.
  - id: OBL-0185-2
    package: WP-768
    proof: refused_shapes_are_recorded_as_anonymized_refusal_classes
    says: Every compiler refusal of an operational read shape records an operator kind, cardinality class, and partition class with no application value, name, or identifier.
  - id: OBL-0185-3
    package: WP-769
    proof: expansion_completes_every_driver_before_release_or_refuses_whole
    says: Expansion hydrates the complete bounded target set for every driver row at one snapshot before any row, count, or cursor is released, and exceeding any ceiling returns only the declared refusal.
  - id: OBL-0185-4
    package: WP-769
    proof: expansion_memory_redb_parity_matches_independent_oracle
    says: Memory and redb produce byte-equal expansion results that match an independent model, including policy-denied targets, empty drivers, and maximum fan-out.
  - id: OBL-0185-5
    package: WP-770
    proof: exists_predicates_lower_to_identical_candidate_plan_bytes
    says: An exists or not exists predicate compiles to exactly the plan bytes of the equivalent explicit candidates binding, so it adds no execution path, cursor family, or authority surface.
  - id: OBL-0185-6
    package: WP-771
    proof: check-operator-acceptance
    says: The retained corpus of previously refused application shapes compiles and executes through every generated binding, and no new provider descriptor, epoch, or provider state was added.
review_triggers:
  - A shape would be admitted as a new provider family without a written operator refusal.
  - An operator would read across partitions, consume a caller-supplied plan, or release a partial result.
  - RiffQL, query IR, or module bytes for existing queries would change rather than selecting a successor identity.
---
# ADR-0185: Relational Operators in the Candidate Algebra, Not Provider Families

## Context

Since ADR-0130 every new operational read shape has landed as a provider
family with its own descriptor, epoch semantics, cursor family, provider-state
version, and service port: exact text, exact predicate, long pattern,
tokenized text, projection result sets. That pattern is how the workspace
reached 824 version-suffixed types and seven per-engine service ports, and it
makes each new shape a subsystem rather than a compiler change. Meanwhile the
shapes applications actually refuse most are relational: a page that needs the
children of each row it lists, a list filtered by the existence of a related
row, and its negation. `docs/known-limitations.md` names semijoins, correlated
existence, and one-to-many expansion as unavailable. ADR-0054 already proves
the N-to-1 direction as a bounded dependent key batch, and ADR-0174's candidate
algebra already completes intersection, union, and difference over declared
same-partition indexes before root ordering. The missing operators need no
physical state that those indexes do not already hold.

## Decision

1. **Operators before providers.** An operational read shape that can be
   expressed as an operator over existing declared indexes, relationships,
   providers, and the candidate algebra lands as an operator: one grammar
   addition, one additive IR tag, one executor step, and fixtures. A new
   provider family is admitted only for a shape that needs physical state no
   existing index or provider can supply, and only after a written refusal of
   the operator form. This amends ADR-0178 section 1: operators over existing
   physical state are exempt from its freeze; new provider families remain
   frozen until WP-750 closes.
2. **One-to-many expansion.** A later `many` binding may be driven by an
   earlier bounded binding: `for each ticket in tickets` names the driver, its
   predicate binds a declared same-partition index prefix to the driver row's
   key components, and `take N per ticket` bounds the targets per driver. The
   compiler proves one partition route, the index prefix, and the product
   bound: driver maximum times per-driver maximum is at most the 65,535-row
   scan ceiling and the query's result ceiling. Targets are ordered by the
   driver's order, then the index order with the primary-key tie-breaker. The
   outcome nests targets under their driver record; a flat carriage carries
   the driver key. Expansion depth is one in V1; a driver may not itself be an
   expansion.
3. **Complete before release, all or refusal.** Every driver's complete
   bounded target set is hydrated at the one authorized snapshot before any
   row, count, or cursor is released. Exceeding a per-driver, product, byte,
   probe, or work ceiling returns only the declared typed refusal. Paging is
   the driver binding's existing cursor; per-driver pages have no cursor.
4. **Policy before observation.** Row policy and field policy apply to every
   target before it can influence membership, count, order, or nesting. A
   denied target is indistinguishable from an absent one.
5. **Existence as lowering.** `exists Junction using index where ...` and
   `not exists ...` on a root binding compile to the ADR-0174 candidates
   binding: existence lowers to intersection with the projected junction keys
   and negation lowers to authorized root-universe difference. The plan bytes
   are identical to the explicit form, so the operators add syntax and
   diagnostics only.
6. **Additive identities.** Queries using an operator select RiffQL V14,
   query IR V18, and module V18 under ADR-0124's least-sufficient rule; every
   query without one keeps its exact bytes, plan hash, and cursors. No
   provider descriptor, epoch, provider state, service port, or durable
   format is added or changed.
7. **Refusals are demand data.** Every compiler refusal of an operational
   read shape records an anonymized refusal class (operator kind, cardinality
   class, partition class) carrying no application name, value, or identifier.
   `riffdb application check --refusals` and the alpha campaign event schema
   report the classes so the operator backlog is ranked by observed demand.

## Standing design tests

- **Interface safety:** callers submit only typed parameters to a compiled,
  named operation; driver, index, bounds, order, nesting, and existence
  sources are compiler identity. No surface accepts a plan, join order, or
  fan-out.
- **Scale:** V1 is single-node and partition-local by construction; the
  product bound and the existing scan ceiling cap memory, and nothing assumes a
  full-population read.

## Checks

- Compiler fixtures: least-sufficient identity selection, legacy byte
  equality, product-bound and depth refusals with `RDB-QP` diagnostics.
- Executor: memory and redb parity against an independent oracle, including
  empty drivers, maximum fan-out, denied targets, and cancellation.
- Lowering: plan-byte equality between existence syntax and explicit
  candidates.
- Acceptance: the refused-shape corpus (TicketDesk ticket page with comments
  per ticket, MLflow runs with tags per run, OpenFGA tuples with relations per
  object) compiles and executes through Rust, Go, TypeScript, Python, MCP, and
  CLI; the version topology shows no new provider domain.
