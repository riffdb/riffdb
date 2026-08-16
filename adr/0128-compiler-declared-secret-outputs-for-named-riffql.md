# ADR-0128: Compiler-Declared Secret Outputs for Named RiffQL

- **Status:** Proposed
- **Direction approved:** 2026-08-15
- **Exact text accepted:** No
- **Decision deadline:** Before WP-634 changes RiffQL grammar, query IR,
  compiled-role identity, installation authority diffs, or MCP query visibility
- **Requires:** ADR-0055, ADR-0108, ADR-0110, ADR-0111, ADR-0112,
  ADR-0117, ADR-0118, and ADR-0124
- **Amends if accepted:** ADR-0055's generated MCP presentation rule,
  ADR-0110's role-widening diff, and ADR-0118's dedicated secret-field naming
- **Defines or blocks if accepted:** WP-634 and WP-635; blocks WP-630's
  Better Auth lifecycle acceptance

## Context

ADR-0118 correctly makes secret field visibility default-deny. Ordinary field
visibility, including an enumeration that happens to contain every field, never
reveals a secret; only the dedicated `secret_fields` capability component may
release one. Named RiffQL query compilation already derives ordinary field
visibility for a generated application role from each selected query's complete
authorization union. It does not derive dedicated secret visibility.

That separation exposed a real Better Auth adapter failure. A named query may
project a secret-classified session, account, or verification field, but role
compilation places its field ID only in ordinary visibility. The resulting query
reaches the correct observed denial, `RDB-AUTH-0214`, even though its generated
role names the exact immutable module and query. Read-only compiled commands can
return the value, but using commands as reveal-shaped reads would compensate for
an authorization-model gap and blur the intended division: RiffQL owns reads;
compiled commands own writes and atomic state transitions.

The missing feature cannot be a raw `secret_fields` list in application source.
Such a list would let an author grant authority unrelated to an exact reviewed
query output, would be difficult to explain in an installation diff, and would
make application roles a second capability language. Nor can a source annotation
grant authority by itself: ad-hoc query text is caller-controlled, and ADR-0118
already establishes that a `reveals` annotation records static disclosure intent
rather than runtime authorization.

There is a second gap at installation. The current bounded role-widening diff
compares symbolic operations only. Adding a secret output to an already selected
query rotates its module and role hashes but may add no operation name. Treating
that change as identity churn rather than authority widening would silently
create a successor credential with broader disclosure authority, contrary to
ADR-0110 and `APE-006`.

## Proposed Decision

### 1. `reveals` is an exact RiffQL output declaration

RiffQL adds one contextual output annotation on a leaf field selection:

```riffql
query SessionByToken($organization_id: Organization.id, $digest: string<128>) {
  maybe session from Session
    where organization_id == $organization_id and token_digest == $digest
  return {
    session {
      id
      user_id
      token_digest reveals session.token_digest
    }
  }
}
```

The word `reveals` is contextual in this position and remains usable as a
contract field or output alias elsewhere. The annotation names the exact
resolved binding and entity field whose value the leaf releases. Output aliases
do not change the required source name.

The initial form is deliberately narrower than the contract-flow annotation:

- it is legal only on a leaf selection that directly resolves to one stored
  secret-classified entity field;
- that one exact source must be named once;
- a missing, duplicate, ordinary-field, unresolved, differently bound,
  whole-record, aggregate, parameter, cursor, or excess declaration is a
  source-spanned compilation error;
- nested output records declare each secret leaf independently; there is no
  wildcard, entity-wide, role-wide, recursive, inferred, or whole-record
  reveal; and
- one query may contain at most 1,024 declared secret leaves, within the
  existing RiffQL collection and artifact byte limits.

Selecting a secret leaf in newly compiled source without the exact annotation
is a compilation error. This is an intentional authoring-time tightening for a
shape that generated application roles cannot currently execute successfully.
Previously compiled query modules remain governed by their frozen decoder and
runtime field-visibility checks.

The annotation is disclosure intent, not authority. Parsing, checking,
explaining, or submitting source containing `reveals` never adds capability
permission or field visibility. In particular, an ad-hoc principal cannot mint
secret access by adding the word to submitted text.

### 2. The compiler carries exact secret-output requirements

For every declared secret leaf, resolution produces one immutable requirement
binding:

- the exact query name and module;
- the symbolic entity and field names used for safe review;
- the compiler-owned entity and field IDs used for authorization;
- the result path and source span used for generation and diagnostics; and
- the exact contract lineage, version, and bundle identity already owned by
  the query plan.

Requirements are canonically ordered by result path and source identity,
duplicate checked, bounded, included in the plan and module hashes, and visible
in source maps, safe explain output, catalogs, and generated schema metadata.
Those surfaces may name a field as secret-revealing but never contain its value.

The feature introduces RiffQL language V3, query IR V4, and query-module V4.
V4 can envelope any currently supported core, operational, or aggregate plan
family plus its exact secret-output requirement table; it does not create a
combinatorial version for each old plan family. The compiler uses V4 only when
at least one declaration is present. Sources without a declaration continue to
emit their least-sufficient V1, V2, or V3 IR/module identities byte-for-byte.

### 3. Selected named queries derive least secret authority

When an application role selects a V4 named query, role compilation verifies
the query module against the exact manifest, lock, contract, and plan identities
and derives two related artifacts:

1. the union of exact entity/field IDs required by its selected queries is
   placed in `EntityFieldVisibilityV1.secret_fields`; and
2. each selected `(query, entity, field)` becomes one safe symbolic
   `QuerySecretOutput` authority atom for review and installation.

Ordinary projected fields remain in the ordinary visibility list. Primary-key,
predicate, index, uniqueness, row-policy, and cost dependencies do not become
secret outputs merely because the runtime reads them. The compiler derives
secret authority from declared returned leaves only.

Application source, resolved manifests, and public role configuration gain no
raw field-ID list, symbolic `secret_fields` list, wildcard, inheritance switch,
or manual authority escape. Removing the selected query or its last declaration
removes the derived atom and, when no selected query still needs that source,
the dedicated capability visibility.

Compiled-role codec V4 canonically binds the complete current role semantics,
including length-delimited ordinary and secret visibility sets and the ordered
per-query secret-output atoms. It is selected only for a role with at least one
such atom. Roles without secret-output atoms preserve their existing V1, V2, or
V3 canonical bytes and hashes. Adding, removing, or changing an atom rotates the
role identity even if another selected query already causes the same field ID
to appear in the role-wide capability union.

The application role-definition shape remains V3: its exact query module and
plan hashes already bind the declaration. Application source V6, manifest V4,
and lock V7 can represent the new query/module and role identities without new
fields and therefore do not advance merely for this feature.

### 4. Runtime release remains fail closed and shared

The existing application-query authorization path remains the sole release
point. Before any protected result is serialized, it verifies the exact named
query permission, immutable module and plan identities, current capability,
tenant/partition and row policy, ordinary visibility, dedicated secret
visibility, and response budget. The declaration does not bypass any check.

A query executed under a capability missing one required dedicated secret field
returns the existing observed typed denial `RDB-AUTH-0214` and releases no
partial result, cursor, aggregate, count, or existence signal. Delegation may
only retain or narrow the parent's dedicated secret fields. Unknown V4
requirements, a module/role mismatch, incomplete requirement coverage, or an
old runtime that does not advertise V4 support fails before execution.

Old V1 through V3 modules continue to decode and use their existing runtime
authorization semantics. They do not acquire compiler-derived secret authority
retroactively.

### 5. Secret-revealing queries are SDK reads, not agent streams

An exact generated application role may invoke an accepted secret-revealing
named query through the shared application service, gRPC, and generated Rust,
Go, TypeScript, or Python SDK. Generated result schemas carry structural secret
metadata so driver diagnostics, debug helpers, and generated examples remain
redacted. The application receives the authorized typed value; RiffDB cannot
control application code after that explicit release.

The initial feature does not expose the query as an MCP tool and does not allow
it to feed a named-query watch, live query, contextual agent subscription, or
other reactive output. MCP catalogs and generated MCP schemas omit that query
as an invocable tool while safe catalog description may report that it is an
SDK-only secret-output query. Reactive compilation that references it fails at
the referencing source span. This is a static surface exclusion, not a
transport-owned authorization decision or runtime redaction substitute.

CLI rendering retains ADR-0118's rule: secret values stay redacted unless the
separately authorized explicit-reveal mode is selected and the principal has
the exact dedicated authority. Logs, public errors, diagnostics, provenance,
audit, telemetry, MCP text, examples, and documentation never echo the value.

Streaming or agent-visible secret output requires a later accepted ADR that
defines revocation, retained-value removal, replay, tool visibility, prompting,
and redaction semantics. It is not an implementation-package option.

### 6. Installation reviews the complete symbolic authority set

Installation role reconciliation uses a closed bounded authority vocabulary:

```text
Operation { kind, name }
QuerySecretOutput { query, entity, field }
```

Names are compiler-resolved symbols; no numeric IDs, values, source text, or
capability bytes enter the public diff. Each role is capped at 1,024 operation
atoms and 1,024 query-secret-output atoms, with at most 2,048 total atoms. The
desired and observed sets are independently sorted and duplicate checked.

For an existing role, every desired atom absent from the exact observed role is
an addition. Any nonempty addition set requires one approval bound to:

- the exact previous role hash; and
- the complete canonically ordered addition set.

This includes adding a secret output to a query whose `Operation` atom already
exists, and adding it to a query when another query already grants role-wide
visibility to the same field. Excess, missing, stale-predecessor, differently
named, or reordered noncanonical approval data fails before capability
creation. Removals are narrowing and need no widening approval. A role-hash
change with identical authority atoms is identity rotation, not widening, but
still uses exact successor credential creation and predecessor revocation; no
existing credential is mutated in place.

Initial role creation has no predecessor to widen. Its full authority set is
nevertheless shown in the read-only plan, bound into the plan identity, and
covered by the caller's explicit initial-install confirmation and receipt.

Application installation-plan V3 carries the authority atoms and an optional
ADR-0119 reimport binding. The least-sufficient writer continues to emit V1 or
V2 for a plan with no query-secret-output atoms, preserving their canonical
bytes; it emits V3 when any such atom exists. Old readers reject V3 before
mutation. Feature negotiation prevents an installer that cannot observe
secret-output atoms from reconciling a V4 role. The terminal receipt's existing
exact plan and role hashes bind the approved authority, so no receipt-format
change is required.

WP-635 registers the installation-plan domain in ADR-0124's version topology
with V1 through V3 and its exact reader/writer policy. Query language, query IR,
query module, and compiled-role codec topology entries advance in the same
implementation change that adds their fixtures.

### 7. Better Auth uses the intended read/write split

The Better Auth compiled profile uses named RiffQL for every read, including an
exact lookup whose result contains an authorized secret field. It uses compiled
commands for writes, bounded cascades, and atomic verification-token
consumption. No read-only command may be introduced solely to reveal stored
secret query data. This does not prohibit a real state-transition command from
returning its declared one-time secret outcome under ADR-0118 Amendment 1.

The integration remains external under ADR-0117. WP-630's fresh external
consumer and framework conformance run are the final evidence that the named
read works without generic CRUD, transaction callbacks, raw field grants, or a
framework-specific first-party crate.

## Options Considered

1. **Keep reveal-shaped read-only commands.** Rejected because it hides a query
   authorization gap and makes ordinary reads look like state transitions.
2. **Let application roles list arbitrary secret fields.** Rejected because it
   creates a second public capability language unrelated to reviewed outputs.
3. **Treat any selected secret projection as implicit authority.** Rejected
   because a source edit would silently widen a role and an output would not
   carry explicit disclosure intent.
4. **Make `reveals` itself a runtime grant.** Rejected because submitted source
   is untrusted input and annotations cannot replace current capability checks.
5. **Declare exact outputs, derive least authority, and review symbolic deltas.**
   Proposed because compiler intent, runtime authority, and installation
   approval remain separate and mechanically checkable.

## Consequences

- Better Auth and other auth-shaped adapters can perform secret-bearing reads
  through named RiffQL while commands retain writes and atomic transitions.
- Every secret output is locally visible in query source, module metadata,
  role identity, and installation review without exposing a value.
- Existing successful non-secret queries and roles retain exact identities.
- Authors must add an annotation to newly compile a secret projection, and
  existing manually provisioned secret-query source must migrate explicitly.
- Secret-revealing queries are initially unavailable to MCP and reactive
  consumers; that deliberate restriction may require separate non-secret query
  shapes for agent workflows.
- The installation diff becomes a complete authority diff rather than an
  operation-name diff, increasing fixture and compatibility work.

## Compatibility

This is a `new_domain_identity` under ADR-0124 for RiffQL language V3, query IR
V4, query-module V4, compiled-role codec V4, and installation-plan V3. Their old
readers and fixtures remain active. Writers use the least-sufficient old
identity whenever the new semantics are absent.

Application source V6, resolved manifest V4, application lock V7, application
role-definition V3, capability-grant encoding, public query result value
encoding, Protobuf field numbers, storage keys, durable rows, journal, backup,
export, and changelog formats do not change. Exact module, plan, role, lock,
generated-artifact, and installation-plan hashes rotate where their covered
content changes. Existing credentials are replaced, never widened in place.

An old compiler rejects the contextual syntax; an old runtime or installer
rejects the new immutable artifact/plan identity before data release or
mutation. No downgrade path strips declarations or authority atoms.

## Security

The default remains deny at three independent gates: source must declare the
exact secret output, the selected named role must derive the exact dedicated
field visibility, and current runtime authorization must validate that grant
before release. Neither another query's annotation nor an ad-hoc source string
can grant access. Row policy and tenant isolation apply before secret release.

Permission widening is visible even when operation names are unchanged. Values
never enter hashes, diffs, approvals, catalogs, source maps, diagnostics,
receipts, logs, or MCP content. Secret fields retain full-fidelity durable and
application-result behavior; this decision adds no encryption promise.

## Standing Design Tests

- **Interface safety (AGENTS.md boundary 11):** an application can select only
  a named compiler-checked query. It cannot name raw secret authority, use an
  annotation as a grant, skip current authorization, silently widen an existing
  role, expose the query to an agent/stream, or opt out of redaction. The only
  public widening is an exact symbolic addition approved against the exact
  predecessor.
- **Scale:** declaration and role derivation are compile-time bounded sets.
  Runtime adds no scan, join, retained row, queue, global lock, or full-state
  rewrite; it reuses the existing per-result field visibility check. Authority
  atoms and plans have fixed count and byte ceilings.

## Testing

- Parser, formatter, source-span, reserved-word, nesting, and fuzz coverage for
  exact/missing/duplicate/wrong/excess `reveals` declarations.
- Resolver and compiler semantic tests proving only direct returned secret
  leaves create requirements; predicate/index/row-policy dependencies do not.
- Frozen V1 through V3 query and role fixtures plus least-sufficient query/role
  V4 and installation-plan V3 identity tests and topology drift checks.
- Role tests proving dedicated secret fields and per-query atoms are exact,
  canonically hashed, removed on narrowing, and impossible to supply through
  application source.
- Cross-surface authorization tests for success, missing grant
  (`RDB-AUTH-0214`), delegation narrowing, revocation, row-policy denial,
  module substitution, partial-result suppression, and redaction canaries.
- Architecture tests proving generated MCP inventories omit the query and
  reactive compilers reject watches/subscriptions that reference it.
- Installation V1/V2 byte-freeze and V3 canonical tests for initial creation,
  same-operation secret widening, stale/wrong/excess approval, narrowing,
  identity-only rotation, old-client refusal, resume, and receipt binding.
- Generated Rust, Go, TypeScript, and Python query-result conformance plus the
  external Better Auth empty-database signup/read/session/delete/replay suite.

## Requirements and Work Packages

- **Provisional requirements:** `QSO-001` through `QSO-012`, added to
  `SPEC.md` only after exact acceptance.
- **Provisional WP-634:** RiffQL syntax, diagnostics, V4 IR/module,
  least-sufficient compatibility, compiler-derived role secret visibility,
  V4 role identity, runtime denial/success, and MCP/reactive exclusion;
  satisfies `QSO-001` through `QSO-006`.
- **Provisional WP-635:** bounded symbolic authority atoms, installation-plan
  V3, diff/approval/rotation semantics, topology registration, generated-client
  conformance, and documentation; satisfies `QSO-007` through `QSO-011`.
- **Amend on acceptance:** WP-630 depends on WP-634 and WP-635 and supplies
  final external Better Auth evidence for `QSO-012` and `DEL-012`.

## Decision Deadline

Exact acceptance is required before RiffQL grammar, query IR/module identity,
compiled-role secret authority, role hash, installation diff/plan, generated
client behavior, MCP visibility, or reactive admission changes.
