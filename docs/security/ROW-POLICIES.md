# Compiled row policies

RiffDB row policies are closed contract programs attached symbolically to
application roles. They are intended to make a forgotten application-side row
check impossible: a request carries ordinary operation values only, while the
compiler and authorization path own the exact policy proof.

WP-570 freezes authoring, IR, identities, and capability-fact delegation.
WP-572 applies the shared pure evaluator across authoritative, projected,
search, live, event, contextual, workflow, scheduler, and bulk safe points. It
denies missing rules, facts, rows, and exact relationship evidence; evaluates read/create/delete
against the required row; evaluates update against both transaction-current and
successor state; and issues a move-only proof bound to the exact capability
revision and row hashes. The same evaluator filters rows before limits and
aggregates, and its final-safe-point recheck rejects capability or row drift.

Protected operations remain unavailable from the ordinary unbound role grant.
Capability Record V4 persists the exact application role, canonical principal
facts, selected policy per protected entity, and closed operation classes.
Trusted role provisioning type-checks the complete fact set before returning
otherwise-withheld permissions in that V4 grant.

Authoritative named and ad hoc RiffQL reads now consume that V4 authority in
the storage-owned snapshot. Point reads become indistinguishable absence when
denied; dependent reads filter before cardinality and hydration; index scans
filter each authoritative candidate before it can consume the visible page
limit or cursor; and operational aggregates see only the resulting authorized
rows. The service resolves the selected policy against the exact active bundle
and reauthorizes the capability immediately before response release. A missing
binding, stale policy name, wrong operation class, invalid relationship plan,
or adapter without the policy-aware execution port fails closed.

Compiled commands now carry the same move-only V4 authority through admission
and deterministic evaluation. Before sequence assignment, the authoritative
write transaction reloads the exact capability revision and every bounded,
compiler-derived relationship lookup. Create checks the proposed row, update
checks both transaction-current and successor rows, and delete checks the
transaction-current row. Any stale capability, missing relationship evidence,
or denied transition rolls back without mutation and returns only the ordinary
authorization-denied class.

Live named queries execute their initial snapshot and every subsequent
re-evaluation through the same policy-aware RiffQL path. Protected watches
conservatively re-evaluate after any commit in the routed partition, including
ACL-only relationship changes, so a narrowing emits removals or a reset before
the next value is delivered. Capability changes close the watch instead of
reusing its prior authority.

Contextual hydration has a policy-aware grouped-query path that executes every
declared hydration for one work item inside one shared authoritative snapshot.
The service derives the protected entity union only from exact deployed
reactive and query modules, resolves it against current V4 authority, and
reauthorizes before cursor publication, hydration, and release. The trigger
event is independently checked through its compiler-owned current-row anchor
before it can be leased. Acknowledgement and negative acknowledgement repeat
that event check in the same authoritative transaction as consumer mutation.

Contextual reaction helpers validate the sealed causation token and exact live
lease, then repeat current capability, trigger-row, relationship, and event
policy evaluation in one storage safe point before invoking the ordinary
compiled-command service. They cannot submit an evaluated mutation or bypass
the transaction-current command verifier; a denied event, current row, or
successor row commits nothing. Old events without an anchor remain hidden.

Native projected queries now derive the complete candidate-key set from one
organization partition and one immutable projection snapshot. The configured
authoritative adapter reloads every current row and bounded indexed
relationship observation in one read snapshot, evaluates the same V4 policy,
and returns a move-only admission proof bound to the entity and exact candidate
set. The columnar engine verifies that proof and excludes denied rows before
request predicates, scan charging, limits, grouping, aggregates, or ranking.
The operation refuses a candidate set above the fixed 100,000-row admission
ceiling; it never post-filters or returns a partial aggregate.

Event replay/tail, durable streams, contextual trigger delivery and reactions,
consumer status, protected opaque seek, and acknowledgement now consume the
same current anchor authority. Filtering occurs before visible limits, leases,
hydration, cursor release, and reaction command admission. Protected replay
returns a typed `bounded_progress` result after at most 1,024 physical
candidates so hidden history cannot force unbounded work or disclose its
cardinality. Cursors bind capability revision and application-role policy
identity. The untargeted global reactive-wakeup resource remains denied for V4
credentials because it has no subscription identity with which to suppress
hidden commit timing.

Export additionally requires a distinct current Capability V5 grant; V4 row
policy alone never implies it. Event policy is never guessed from payload field
names. An event without the compiler-owned source entity/key and policy anchor
is indistinguishable hidden history for ordinary protected consumers.
Provisioning rejects missing, extra, or mistyped facts as `RDB-AR010`; it does
not silently drop an unknown fact or substitute a default value. A role whose
selected policy reads `principal.id` also rejects a principal that is not
canonical lowercase UUID text before capability creation.

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

Capability Record V4 is an additive successor: V1, migration-only V2, and
installation-only V3 remain byte-exact. V4 is selected only for a checked
row-policy extension, and its decoder rejects missing extensions, noncanonical
fact or binding order, duplicate entity selection, unknown operation tags, and
role-identity substitution. Current authorization reconstructs facts only from
the transaction-current retained record. Authoritative RiffQL, native projected
queries, compiled commands, live named RiffQL watches, compiler-anchored event
replay/streams, protected consumer state, and contextual subscriptions/reactions
are enabled protected surfaces. The generic database-wide reactive wakeup
remains unavailable because it cannot prove subscription-specific visibility.
Other protected surfaces remain unavailable until their shared policy and
final-safe-point enforcement lands.

## Binding a protected role

Principal facts are supplied only by an operator while binding the exact
compiled application role. They never appear in a generated query or command
method and cannot be overridden by an application request. Write one bounded
JSON object whose keys exactly match `riffdb role describe`'s
`principal_fact_schemas`, then bind it explicitly:

```json
{
  "team_ids": [
    { "$uuid": "00000000-0000-0000-0000-000000000008" }
  ]
}
```

```bash
riffdb role bind riffdb.application.json \
  --role DocumentReader \
  --principal 00000000-0000-0000-0000-000000000007 \
  --actor-kind service \
  --audience document-service \
  --principal-facts principal-facts.json \
  --credential-output .riffdb/document-reader.credential
```

The facts document uses the same natural symbolic JSON values as application
inputs. Missing, extra, wrongly typed, duplicate, or over-bound facts fail
before capability creation with `application_role_principal_facts_invalid`.
Enum facts use variant names; the compiler resolves them against that fact's
declared enum, for example `{ "$enum": "Internal" }`, so operator files never
contain stable numeric enum IDs. Lists of enum facts use the same tagged form
for each member.
When a selected policy reads `principal.id`, the bound principal must be a
canonical lowercase UUID. Diagnostics may name the required fact and public
type but never render its value.
