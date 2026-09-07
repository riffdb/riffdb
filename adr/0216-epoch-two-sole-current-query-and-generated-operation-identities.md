---
adr: "0216"
title: Epoch-Two Sole-Current Query and Generated-Operation Identities
status: proposed
tier: guarantee
date: 2026-09-07
accepted: null
requires: [ADR-0124, ADR-0181, ADR-0185, ADR-0194, ADR-0198, ADR-0204, ADR-0211]
amends: [ADR-0185, ADR-0194, ADR-0198]
supersedes: []
requirements: [GOV-001, GOV-002, GOV-003, OQ-116, DX-044, DX-047, DX-049,
  VER-001, VER-002, VER-003, VER-004, VER-008]
packages: [WP-757, WP-779]
obligations:
  - id: OBL-0216-1
    package: WP-757
    proof: epoch_two_query_identity_topology_is_sole_current
    says: Epoch two admits only RiffQL V14, query IR V18, and query module V18, refuses every retired reader and artifact, and preserves operator semantics without a version fallback.
  - id: OBL-0216-2
    package: WP-757
    proof: epoch_two_generated_application_operations_v5_is_sole_current
    says: Epoch two admits only generated application operations V5, deletes V2 through V4 readers, writers, and artifacts, and preserves pagination, SDK-only, and vector semantics within V5.
  - id: OBL-0216-3
    package: WP-779
    proof: generated_application_operations_v6_is_sole_current_for_all_registries
    says: While the external-database registry is empty, V6 succeeds sole-current V5 for empty and nonempty command registries and refuses every retired artifact without changing descriptor semantics.
review_triggers:
  - A RiffQL, query IR, query-module, generated-operation, application-lock, reader, writer, source locator, fixture, or topology identity would differ from the exact sole-current epoch-two sets.
  - An old artifact would be read, written, migrated, reinterpreted, aliased, negotiated, or retained as a compatibility fallback after its owning transition.
  - The external-database registry would become nonempty before the reviewed V6 activation, V5 retirement, and WP-779 closure all complete; V6 would be selected conditionally on command-registry population; or an MCP descriptor, schema, visibility, authorization, pagination, cursor, SDK-only, or vector semantic would change.
  - Operator admission, plan semantics, public query behavior, protocol, durable data, runtime authority, or any compatibility boundary other than the named artifact identities would change.
---
# ADR-0216: Epoch-Two Sole-Current Query and Generated-Operation Identities

## Context

ADR-0181 and ADR-0204 require the epoch-two repository to retain only current
format identities, but two accepted least-sufficient rules still authorize
multiple live identities. ADR-0185 keeps pre-operator query identities for
queries without expansion or existence, and ADR-0194 keeps generated operation
V2 through V5 according to feature predicates. Their completed proofs are exact
historical epoch-one evidence; carrying those readers, writers, and artifacts
into epoch two would violate the sole-current invariant.

ADR-0198 adds generated operation V6 only for a nonempty command registry and
otherwise emits a V2-through-V5 predecessor. WP-779 has not merged, and no
external database is registered, so its identity transition can still be made
complete and deterministic without preserving a third-party compatibility
window. This record reconciles identity selection only. It does not change the
features or semantics represented by those identities.

## Decision

1. In epoch two, every successfully compiled query emits exactly RiffQL V14,
   query IR V18, and query module V18. Expansion and existence remain governed
   by ADR-0185's grammar, bounds, lowering, authorization, and execution rules,
   but their presence no longer selects an identity. Feature admission remains
   a semantic compiler decision; version fallback is not an admission path.

2. WP-757 deletes every older RiffQL, query IR, and query-module reader, writer,
   fixture, generated artifact, topology entry, source locator, compatibility
   alias, and runtime dispatch. Each owning bounded parser, decoder, or compiler
   entry point refuses an old artifact identity before lowering or dispatch,
   and its reserved identity cannot be reused. Epoch-one databases separately
   refuse at ADR-0204's storage format gate. The exact completed epoch-one
   fixtures and commit history remain historical evidence; no current reader is
   retained to replay them.

3. SPEC OQ-116 is epoch-qualified after exact acceptance. Its expansion and
   existence semantics remain exact, including the absence of a provider
   descriptor, epoch, state, service port, or durable format. Its
   least-sufficient V14/V18/V18 identity rule remains the historical epoch-one
   contract; epoch two uses the sole-current identities in Decision 1 for every
   successful query.

4. ADR-0185 OBL-0185-1 is retired from the live obligation set after exact
   acceptance because its completed proof establishes the historical
   least-sufficient transition that Decision 1 ends. Its exact tuple remains
   historical evidence: ID `OBL-0185-1`; package `WP-768`; proof
   `expansion_plan_is_least_sufficient_and_legacy_bytes_are_unchanged`; says,
   "A query without an expansion or existence operator keeps its exact RiffQL,
   IR, module, plan-hash, and cursor bytes; a query with one selects the additive
   successor identities." It is not renamed, reassigned, or replaced with a
   false epoch-two substitute. ADR-0185 Decisions 1 through 5 and 7,
   OBL-0185-2 through OBL-0185-6, and all operator safety and execution
   semantics remain exact.

5. During WP-757, `riffdb-generated-application-operations/v5` becomes the sole
   current generated-operation identity. V5 represents every combination of
   pagination, SDK-only output, and vector inspection, including their absence.
   WP-757 deletes V2 through V4 readers, writers, fixtures, generated artifacts,
   topology entries, source locators, aliases, and dispatch. Old locks and
   artifacts refuse or are explicitly regenerated under existing application
   authority; they are never silently reinterpreted.

6. ADR-0194 OBL-0194-3 is retired whole from the live obligation set after exact
   acceptance. Its exact historical tuple remains: ID `OBL-0194-3`; package
   `WP-701`; proof `generated_application_operations_v5_topology_and_selection_are_exact`;
   says, "V5 is a strict V4 structural successor, composes pagination with exact
   SDK-only and vector registries, selects the least-sufficient V2 through V5
   identity, freezes every reader/writer window and reviewed rotation, and
   preserves every unpaged entry byte-exact." OBL-0216-2 solely owns the new V5
   topology while OBL-0194-1 and OBL-0194-2 retain pagination behavior and
   cursor authority. All SDK-only and vector composition, schema bounds,
   authorization, redaction, and public behavior remain exact inside V5.

7. The authoritative external-database registry must be empty when WP-779
   starts and remain empty through the reviewed V6 activation, V5 retirement,
   and WP-779 closure. WP-779 advances the entire
   generated-operation domain from sole-current V5 to sole-current V6 for both
   empty and nonempty post-reimport-exclusion command registries. V6 retains the
   exact V5 root and all applicable registries, records V5 as its predecessor
   metadata where ADR-0198 requires that field, and adds only ADR-0198's exact
   MCP identities and descriptors for present commands and queries. Empty
   registries produce a valid V6 artifact with no fabricated entry.

8. WP-779 deletes the V5 reader, writer, fixtures, generated artifacts,
   topology entries, source locators, aliases, and dispatch after its V6
   transition. Every old V5 lock or artifact refuses or is explicitly
   regenerated under existing application authority. Registration at any time
   before WP-779 closes stops the transition for a separately accepted
   compatibility ADR; this record grants no migration authority over an
   external binding. OBL-0216-3 freezes that completion-time gate.

9. ADR-0198 Decision 1's nonempty-command predicate and Compatibility language
   are replaced after exact acceptance by Decisions 7 and 8. OBL-0198-3 is
   retired whole before implementation because the pre-external consolidation
   supersedes its predicate. Its exact unproved tuple remains visible: ID
   `OBL-0198-3`; package `WP-779`; proof
   `generated_mcp_catalog_v6_topology_and_lock_migration_are_exact`; says, "The
   exact nonempty-command predicate, predecessor-profile preservation,
   dependency edge, topology, fixture rotation, V8 lock-identity migration, and
   old-artifact behavior are frozen and reviewed." It is not relabelled as a
   completed proof. OBL-0216-3 solely owns all-registry V6. OBL-0198-1 and
   OBL-0198-2, descriptor composition, hosted parity, policy visibility,
   ordering, pagination, bounds, schema ownership, and ADR-0211's trusted
   projection remain exact.

10. After exact acceptance, a prerequisite WP-757-only path-authority commit
    touches only `work_packages.yaml`, adds exactly the ADR-0185 and ADR-0194
    record paths to `WP-757.allowed_paths`, and becomes the base for the
    reconciliation; it changes no package semantic. The following WP-757-only
    governance commit updates SPEC OQ-116; retires only OBL-0185-1 and
    OBL-0194-3 from their live arrays while preserving their exact tuples here;
    adds this record to `required_adrs`; adds OQ-116, DX-044, DX-047, DX-049,
    VER-001, VER-002, VER-003, VER-004, VER-008, and OBL-0216-1/-2; and updates
    WP-757's objective, deliverables, exit gate, checks, and triggers for
    Decisions 1 through 6. After WP-757 closes, a separate WP-779-only
    governance commit retires OBL-0198-3 from ADR-0198's live array, makes
    WP-779 guarantee tier, depends on completed WP-757, requires ADR-0211 and
    this record, adds OBL-0216-3, and updates its objective, deliverables, exit
    gate, checks, and triggers for Decisions 7 through 9. WP-779's existing
    allowed paths already authorize ADR-0198 and `work_packages.yaml`; no path
    is silently widened. None of the three commits contains runtime, fixture,
    generated artifact, or implementation changes.

11. This decision changes only identity selection, retired-artifact refusal,
    and the exact order of the two repository-owned transitions. It adds no
    grammar, operator, plan, cursor, policy, capability, transport, protocol,
    durable database format, descriptor, SDK, vector, authorization, runtime
    fallback, or caller-selected compatibility control.

## Options considered

1. **Retain least-sufficient identities in epoch two:** rejected because live
   predecessor readers and writers violate the accepted sole-current invariant.
2. **Advance only artifacts whose optional feature is present:** rejected
   because it preserves population-dependent predecessor identities and makes
   one repository epoch encode multiple compatibility windows.
3. **Fold V6 into WP-757:** rejected because ADR-0198/ADR-0211 descriptor parity
   is a separate reviewed change and must follow the V5 consolidation.
4. **Use two complete sole-current transitions:** chosen because it preserves
   semantic proofs while giving each transition one exact predecessor.

## Consequences

- Historical least-sufficient proofs remain truthful evidence, while no old
  reader or writer survives as an epoch-two placeholder.
- Every epoch-two query has one language, IR, and module identity; every
  generated-operation artifact has V5 until WP-779 atomically advances the
  complete domain to V6.
- WP-779 becomes guarantee tier and must stop if the external-database registry
  becomes nonempty before its reviewed transition and closure complete.

## Standing design tests

- **Interface safety:** applications, agents, transports, roles, and
  configuration cannot choose a version, retain a predecessor, request a
  fallback, or opt out of the transition. Existing typed query, cursor,
  descriptor, SDK-only, vector, authorization, and lock behavior is unchanged.
- **Scale:** compilation and generation still process one bounded query,
  command, schema, or artifact entry at a time under existing ceilings. The
  transitions rotate bounded repository fixtures and add no population read,
  database rewrite, co-location assumption, or retained compatibility state.

## Checks

- `epoch_two_query_identity_topology_is_sole_current` freezes V14/V18/V18 as
  the only query identities and rejects old readers, writers, artifacts,
  locators, aliases, and dispatch without changing operator semantics.
- `epoch_two_generated_application_operations_v5_is_sole_current` freezes V5
  for every feature combination and rejects V2 through V4 artifacts while
  preserving pagination, SDK-only, and vector behavior.
- `generated_application_operations_v6_is_sole_current_for_all_registries`
  freezes the complete V5-to-V6 transition, including empty and nonempty
  command registries, the external-database precondition, exact predecessor
  metadata, old-artifact refusal, and unchanged ADR-0198 descriptors.
- Requirement, obligation, topology, generated-artifact, application-lock,
  handbook, allowed-path, workspace, clippy, and `ci-all` checks retain every
  unchanged interface and semantic guarantee.
