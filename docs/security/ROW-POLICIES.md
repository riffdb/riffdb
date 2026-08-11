# Compiled row policies

RiffDB row policies are closed contract programs attached symbolically to
application roles. They are intended to make a forgotten application-side row
check impossible: a request carries ordinary operation values only, while the
compiler and authorization path own the exact policy proof.

WP-570 freezes authoring, IR, identities, and capability-fact delegation. The
first WP-572 increment adds the shared pure evaluator: it denies missing rules,
facts, rows, and exact relationship evidence; evaluates read/create/delete
against the required row; evaluates update against both transaction-current and
successor state; and issues a move-only proof bound to the exact capability
revision and row hashes. The same evaluator filters rows before limits and
aggregates, and its final-safe-point recheck rejects capability or row drift.

Protected operations are still deliberately unavailable while role compilation
withholds their ordinary permission. Enabling them requires the reviewed
versioned durable capability successor that persists principal facts and exact
policy selection, followed by wiring the evaluator through every application
surface. Do not add fields to the schema-hash-bound V1 capability envelope,
implement a temporary middleware filter, or weaken the expected authorization
refusal.

## Contract declarations

A principal fact has a symbolic name and either a supported scalar type or a
bounded scalar set. A row policy names one entity and defines at most one rule
for each operation class:

```riff
principal fact team_ids: list<uuid, 32>

row policy DocumentAccess on Document {
  allow read when visibility == Public
    || owner_id == principal.id
    || team_id in principal.fact.team_ids
  allow create when owner_id == principal.id
  allow update when owner_id == principal.id
  allow delete when owner_id == principal.id
}
```

The closed expression language supports:

- fields of the protected row;
- `principal.id`, `principal.kind`, and declared `principal.fact.<name>`;
- typed constants and enum variants;
- equality, inequality, null/existence, bounded membership, `&&`, and `||`;
- one complete indexed, partition-local relationship-existence probe.

It does not support request-provided predicates, callbacks, arbitrary
functions, dynamic field names, recursion, nested relationship traversal,
unindexed scans, cross-partition probes, clocks, randomness, filesystem or
network access, or a client-side evaluator. Unsupported forms fail during
contract compilation with a source span.

## Exact limits

| Dimension | V1 maximum |
|---|---:|
| Principal fact schemas per contract/capability | 32 |
| Scalar members per set-valued fact | 64 |
| Complete canonical capability facts | 64 KiB |
| Named row policies | 1,024 |
| Rules per policy | 4 |
| Expression nodes per rule | 128 |
| Disjunctive leaves per rule | 16 |
| Indexed relationship probes per rule | 1 |
| Complete index arguments per probe | 16 |
| Complete policy catalog | 1 MiB |

Each relationship probe must name a declared index completely. Its target
partition prefix and the source row route must have the same symbolic field and
type. There is no fallback scan.

## Roles and exact identity

Application Source V6 makes `row_policies` a required array on each role. A
role compiling a protected read or mutation must select a policy containing
the corresponding `read`, `create`, or `update` rule. Unknown policies,
multiple policies for the same protected entity, and missing operation coverage
fail closed.

The compiler emits Application Manifest V4, Application Lock V7, and role
definition format V3. The lock safely names the selected policy, entity, and
operation classes. The contract bundle hash covers the complete executable
policy IR, and the role hash covers its policy selection and required public
fact schemas. Existing V1–V6 fixtures remain byte-exact; the current rotation
receipt is `fixtures/application-locks/row-policy-rotation-v1.json`.

Catalogs may expose a visible policy name, entity name, operation classes, fact
name, and symbolic fact type. They do not expose policy bytecode, stable numeric
IDs, hidden branches, row values, or a capability's fact values.

## Principal facts and delegation

Facts are authorization state, never request claims. The V1 fact set:

- sorts fact names and set members canonically;
- rejects duplicate names and members;
- supports only checked scalars or one bounded scalar set;
- has a strict versioned decoder that rejects non-canonical bytes; and
- redacts values from `Debug` output.

An authorization binding ties the set to one capability ID and nonzero
revision, database, environment, principal ID/kind, canonical audience set,
tenant scope, issue time, and exclusive expiry. Delegation inherits database
and environment and may only reduce audiences, reduce global scope to one
tenant (or retain the same tenant), shorten the lifetime, remove facts, retain
an equal scalar, or reduce a set-valued fact to a subset. Adding or changing a
scalar fact, adding a set member, changing a tenant, widening an audience, or
extending expiry is rejected.

The remaining WP-572 durable increment will persist these facts in a versioned
successor capability record and revalidate the exact capability revision before
protected release and commit. Until that successor and the cross-surface wiring
land, no protected application operation is executable.
