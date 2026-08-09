# ADR-0111: Compiled Principal-Aware Row Policies

- **Status:** Accepted
- **Direction approved:** 2026-08-09
- **Exact text accepted:** Yes, 2026-08-09
- **Decision deadline:** Before WP-570 changes contract/policy grammar or a
  Payload/MLflow adapter claims per-record authorization
- **Requires:** ADR-0007, ADR-0009, ADR-0038, ADR-0055, ADR-0065,
  ADR-0086, ADR-0087, ADR-0108, and ADR-0109
- **Closes:** The unimplemented row-policy premise in ADR-0007, ADR-0086,
  ADR-0087, ADR-0091, and ADR-0092
- **Defines or blocks:** WP-570, WP-572, WP-573, and WP-579

## Context

RiffDB currently authorizes application reads and writes through exact symbolic
operations, tenant/partition scope, field visibility, bounded work, and current
capability state. It does not have a contract-visible predicate that decides
whether one principal may observe or mutate one entity row.

That omission makes realistic alpha adapters unsafe. Payload requires
per-document owner/team/public visibility; MLflow commonly scopes experiments
and runs to owners or groups. Implementing those checks in application code
would permit a forgotten endpoint, aggregate, projection, live query, or agent
hydration path to leak a row. Accepted projection and search ADRs already say
row policy is applied before aggregation/ranking, but no compiled policy exists
to make that promise true.

The answer cannot be arbitrary policy code, request-supplied predicates, or a
post-query filter. Row authorization must remain symbolic, finite,
partition-local, transaction-current where it protects mutation, and shared by
every public surface.

## Proposed Decision

### Policies are declared contract programs, not callbacks

A contract may declare named row policies over one entity and attach them to
symbolic roles and operation classes. The first alpha supports only a closed
boolean grammar over:

- the entity's declared partition/key/scalar fields;
- authenticated stable principal ID and actor kind;
- bounded symbolic principal attributes issued as current capability facts;
- constants and declared enum values;
- equality/inequality, null/existence, bounded membership, conjunction, and
  compiler-capped disjunction; and
- one indexed partition-local `exists` relationship predicate whose target and
  key mapping are fixed in the policy source.

No policy can call application code, the network, filesystem, clock, random
source, another command/query, a dynamic field, an arbitrary function, an
unindexed scan, or a cross-partition relation. Recursion, nested relation
traversal, runtime predicate trees, and deny/allow plugins remain unavailable.

Illustrative source:

```riff
row policy DocumentVisible on Document {
  allow read when visibility == Public
    or owner_id == principal.id
    or exists DocumentGrant.by_document_subject(
      organization_id,
      document_id,
      principal.id
    )
}
```

This syntax is illustrative until WP-570 freezes grammar and IR. The compiler
must produce a closed policy plan, required principal facts, field union,
relationship/index dependencies, cost, and one-partition route. A missing safe
index or excessive boolean/attribute family is a source-spanned deployment
failure, never a runtime scan.

### Principal attributes are bounded current authorization facts

Principal attributes are not free-form request claims. They are a canonical
bounded set issued through the existing capability/role administration path,
covered by capability revision, expiry, audience, tenant, delegation, and
current-policy validation. Names and public value types are compiler-visible;
credentials and raw capability records remain hidden.

The alpha caps attribute count, values per attribute, encoded bytes, policy
nodes, disjunction width, and relationship probes. WP-570 freezes exact values.
Delegation can only narrow an attribute set and row-policy scope. A caller
cannot add an owner/group fact in an operation request.

### Reads enforce policy before shape, rank, or disclosure

Every entity source in named RiffQL, operational RiffQL, dependent hydration,
live query, contextual subscription, projected query, full-text search, vector
search, catalog-derived operation, and export is lowered with its required row
policy. Enforcement occurs before:

- selection into a returned collection or dependent relation;
- `take`, top-N, cursor advancement, count, grouping, or aggregation;
- relevance/vector scoring, corpus statistics, snippets, or result counts;
- live-query diffing and cached/subscribed result retention; and
- agent context hydration or available-command presentation.

Unauthorized rows have indistinguishable absence. A page may scan authorized
bounded candidates and return fewer rows, but cursor semantics must not reveal
the position/count of hidden rows beyond the accepted bounded-filter contract.
Post-filtering after limit/rank/aggregate is forbidden because it leaks and
produces incorrect pages.

Policy is revalidated before protected release. Revocation or narrowing closes
a stream and removes previously visible live-query values before further
delivery, following the existing authorization-change contract.

### Writes prove current-row and successor-row authority

Commands declare whether their row policy protects `create`, `read-current`,
`update`, `delete`, or a named transition. The compiler includes every required
current and proposed field in the command plan. The service authorizes current
principal facts before evaluation; the commit coordinator's narrow
transaction-current verifier rechecks the exact current row, policy-dependent
relationship evidence, capability revision, and successor row before
publication.

This prevents an owner from changing `owner_id`, visibility, project, or ACL
fields to escape the policy unless a separately declared transition permits
it. Create policy evaluates the proposed row. Update/delete policy evaluates
the transaction-current row and, for update, the successor. A stale or newly
revoked decision produces a declared/typed whole-command result and no effect.
There is no caller option to skip policy for bulk, scheduler, MCP, import, or
agent operations.

Administrative backup retains its separately accepted whole-database authority.
Application export may request either principal-filtered or separately
authorized whole-application scope; the selected scope is explicit in its
receipt.

### Roles and generated surfaces remain symbolic

An application role names policies/operations symbolically. Role compilation
derives the exact policy plans, principal-attribute schema, field visibility,
indexes/relationships, and maximum work. Generated clients carry ordinary
typed command/query inputs only; they do not carry row predicates, field IDs,
policy bytecode, or a client-side evaluator.

Diagnostics name the role, policy, entity, operation, missing principal fact,
and source span without revealing row values or confirming a hidden row. The
catalog may describe visible policy names and required attribute schemas, never
the capability's actual attributes or hidden policy branches.

## Options Considered

1. **Application middleware checks:** rejected because omission remains
   expressible and every read/mutation surface must reproduce the rule.
2. **Post-query filtering:** rejected because limits, aggregates, ranking,
   cursors, timing, and dependent reads can disclose hidden rows.
3. **General policy language or embedded WASM:** rejected because access,
   termination, locality, determinism, and cost cease to be compiler proofs.
4. **Compiled closed row policies:** proposed because principal-specific access
   becomes one shared fail-closed database invariant.

## Consequences

- Payload and MLflow adapter claims can include their defining access-control
  behavior instead of application-side imitation.
- Capability records gain bounded typed principal facts and role identities
  gain policy-plan dependencies.
- Query and mutation plans, module/role hashes, cursors, and generated schemas
  require additive versioned successors and the ADR-0108 rotation ceremony.
- Cross-partition ACLs, arbitrary ABAC, time-of-day policy, external policy
  engines, and policy-defined declassification remain unavailable.

## Security

Default is deny. Missing/unknown facts, policy versions, indexes, relationship
evidence, current capability revisions, or successor checks deny before
protected release or mutation. Policy inputs and diagnostics are bounded and
redacted. Timing and cardinality inference remain subject to the existing
stated policy boundary; tests must prove another tenant never affects result,
rank, aggregate, cursor, or observable work class.

## Standing Design Tests

- **Interface safety:** no public request contains a policy predicate or bypass.
  Every supported read/write surface consumes the same compiler-owned policy
  proof, making a forgotten application check unexpressible.
- **Scale:** policies are one-partition, finite, index-backed, and bounded in
  nodes, facts, probes, rows, memory, and output. They require neither global
  principal expansion nor full-state scans and remain routable to a future
  partition leader.

## Testing

- Grammar/IR/plan/role/hash old-current fixtures and source-span diagnostics.
- Pure-evaluator differential tests across owner/public/group/absent/revoked
  facts for every entity and operation class.
- Cross-surface equivalence for named/operational/projected/search/live/
  contextual/export reads and unary/bulk/workflow/import commands.
- Transaction-current races for owner/ACL/visibility changes and capability
  narrowing between evaluation and commit/release.
- Inference tests for limits, cursors, aggregates, FTS/vector rank, errors,
  timing classes, and hidden-row existence.
- Payload document ACL and MLflow experiment/run permission acceptance in every
  generated language.

## Requirements and Work Packages

- **Provisional requirements:** `RAP-001` through `RAP-016`, added to `SPEC.md`
  only after exact acceptance.
- **Defines or blocks:** WP-570, WP-572, WP-573, and WP-579.
- **Final evidence:** WP-573 and WP-579.

## Decision Deadline

Exact acceptance is required before row-policy grammar, capability attributes,
policy IR, role identity, query/mutation authorization, or adapter claims change.
