# ADR-0055: Safe Application Surface and Declared Integrity

- **Status:** Accepted
- **Direction approved:** 2026-07-29
- **Exact text accepted:** 2026-07-29
- **Acceptance reference:** Maintainer agreement in the current Codex session
  with the recommended safe-application product rule
- **Clarified:** 2026-07-30 for aggregate scan authority versus separately
  fuel-metered bounded hydration work
- **Requires:** ADR-0002, ADR-0003, ADR-0007, ADR-0009, ADR-0013,
  ADR-0016, ADR-0027, ADR-0035, ADR-0038, ADR-0051, ADR-0052,
  ADR-0053, and ADR-0054
- **Amends:** SPEC Sections 3, 13, and 24.3
- **Implementation boundary:** Before RiffDB claims that its ordinary
  application surface prevents common database-application bug patterns by
  construction

## Context

RiffDB already makes the most dangerous write mechanics unavailable through its
normal command path: there is no generic application insert/update/delete,
commands are deterministic and idempotent, declared dependencies are
revalidated, and mutation/outcome/event/provenance records commit atomically.
RiffQL similarly proves locality, cardinality, access paths, and per-step bounds
before one-snapshot execution.

The TicketDesk application review nevertheless found four ways for ordinary
application code to recreate familiar SQL-era mistakes:

1. entity relationships are plain scalar identifiers, so a command can create a
   dangling reference unless its author remembers an exact target read;
2. `ReadEntity` and `ScanIndex` authorize both compiler-derived RiffQL work and
   the public kernel-shaped RPCs, so an application capable of RiffQL can also
   build N+1 or multi-snapshot pages manually;
3. named and ad-hoc RiffQL share one execution permission, so a stable
   application cannot be restricted to its reviewed deployed query set; and
4. the compiler records aggregate row cost, but authorization and execution
   enforce primarily per-step ceilings rather than one complete request fuel
   budget.

These are not known mutation or authentication bypasses. They are product-
surface escape hatches that prevent the stronger claim that the normal way to
use RiffDB is safe by construction.

## Decision

### Aggregate scan authority and bounded hydration

The compatible capability field `max_scan_rows` is the whole-query aggregate
index-scan ceiling. Every index-scan step is summed into the plan cost; a
capability at 500 never authorizes two 500-row scans in one query.

Point reads, dependent-key hydration, retained intermediate rows, and projected
values are separate cost-vector dimensions. Their authorization ceilings are
derived from `max_scan_rows * MAX_APPLICATION_QUERY_STEPS` (and, for projected
values, the existing visible-field bound), while the exact compiled plan cost
is consumed as move-only execution fuel. Each compiler-derived entity access
must still fit `max_scan_rows`, every step is independently bounded, and the
fixed step/result-byte ceilings remain in force. This permits a bounded page to
hydrate several relationships without misclassifying every point read as an
index scan; it does not permit an unbounded or hidden access.

Role compilation derives `max_scan_rows` from the greatest aggregate
index-scan cost of its named queries. A query above 500 is rejected before lock
publication and role binding. Defaults do not narrow a `Limit` parameter's
500-row type range; multi-collection pages must use fixed `take` bounds when
their complete parameter ranges would exceed the aggregate ceiling.

### Closed application operation profile

RiffDB has three explicit interaction profiles:

1. **Stable application:** exact named compiled commands and exact named
   deployed queries only.
2. **Scoped agent/development:** named operations plus separately granted
   ad-hoc RiffQL check, explain, and execute authority.
3. **Kernel/administration:** raw entity/index operations for conformance,
   diagnosis, migration, and reviewed low-level tooling.

Stable application authority never implies agent/development or kernel
authority. Agent/development authority never implies kernel authority.

Compiled commands remain the only application mutation mechanism. A stable
application command permission remains exact to contract lineage and command
identity. A stable application query permission is exact to contract lineage,
query-module hash, and query name. Ad-hoc query authority is a separate
lineage-scoped permission and is never inferred from `ReadContract`.

### Private derived query authority

The shared service compiles or resolves the complete query before data access.
Policy evaluates one application-query request containing:

- exact database, environment, principal, capability revision, and ingress;
- exact contract lineage/version/bundle hash;
- named module hash and query name, or the explicit ad-hoc classification;
- query plan hash;
- exact partition route;
- complete entity, field, index, and output-shape requirements; and
- one compiler-derived whole-request cost vector.

An allow decision yields a private, non-cloneable, nonserializable,
process-local authorized-query proof. The query executor consumes that proof
with the exact plan and parameters. It cannot be converted into a public
`GetEntity` or `ScanIndex` request, retained in a cursor, or used for another
plan, partition, principal, capability revision, or module.

`ReadEntity` and `ScanIndex` remain exact kernel permissions for the compatible
`riffdb.v1` protocol. They do not authorize RiffQL. Application query
permissions do not authorize kernel RPCs. Existing kernel RPC bytes and
semantics remain supported; ordinary application presets, generated clients,
and application MCP catalogs do not expose them.

### Named and ad-hoc separation

Named execution requires the exact active or explicitly selected immutable
query module and the permission for its exact module hash and query name.
Substituting ad-hoc source, another module, another query name, or a different
plan fails closed.

Ad-hoc query execution requires its own explicit permission. It remains useful
for agents, exploration, migrations, and diagnosis, but stable generated
application clients never construct ad-hoc source. Query check and explain are
separate permissions from execution.

### Whole-request cost and execution fuel

The compiler emits a canonical cost vector covered by the query plan hash. At
minimum it contains:

- access-step count;
- maximum scanned index rows;
- maximum point reads;
- maximum dependent-batch keys;
- maximum intermediate rows;
- maximum projected field values; and
- maximum encoded result bytes.

Policy compares the complete vector with the capability budget once; repeated
steps cannot multiply a per-step allowance. The executor receives matching
decrementing fuel and validates backend-reported work. Exhaustion returns a
closed resource-limit failure and releases no partial result or cursor.
Existing fixed parser, step, row, response, deadline, and cursor ceilings remain
defense in depth.

### Declared relationships

The contract language gains a bounded same-partition relationship declaration
over existing stored fields:

```riff
reference project
  (organization_id, project_id)
  -> Project(organization_id, project_id)
```

The target components must be the complete target primary key in canonical
order, types must match, and the source and target must share the declared
partition route. Grammar v1 initially supports required relationships only.
Optional relationships, cascades, cross-partition references, polymorphic
references, and deferred validation require later accepted decisions.

No hidden target read is injected. Instead, the command compiler rejects every
create or mutation that establishes or changes a declared relationship unless
the command contains a dominating exact target read with a declared missing-
target outcome. That read enters the ordinary dependency set and commit-time
revalidation. This makes omission impossible without hiding dependencies from
review.

The query compiler may use the relationship symbol as typed navigation only
when it lowers to an already accepted bounded complete-key access. It gains no
unrestricted join.

### Declared same-partition uniqueness

The contract language gains a declared same-partition unique key over existing
fields:

```riff
unique user_email (organization_id, email)
```

The partition component must be complete and canonical. Every component needed
for a mutation must be computable within the accepted static command plan.
Compilation rejects a command that can establish or change the unique value
without acquiring the compiler-derived unique conflict capability and checking
the exact unique key. The authoritative transaction atomically validates and
updates the unique index with the entity mutation.

Global uniqueness, nullable multi-row uniqueness variants, deferrable
constraints, and collation-dependent equality are outside this milestone.

### Presentation and defaults

Generated application SDKs expose named commands and named queries only. The
normal MCP builder catalog exposes generated named operations plus explicitly
granted ad-hoc RiffQL tools; it does not expose raw entity/index tools. CLI
kernel commands remain available only under an explicitly selected kernel/admin
profile and credential.

`riffdb dev` issues separate named presets for stable applications, agents, and
kernel diagnosis. It never implements a single "full access" preset that
silently collapses those profiles.

### Safety claim and acceptance

The supported claim is:

> Through the stable application profile, RiffDB makes generic writes,
> unreviewed read shapes, raw storage-shaped page composition, violations of
> declared relationships and uniqueness, unbounded query work, and retry-unsafe
> command execution unavailable by construction.

The claim is limited to declared contract semantics and RiffDB interactions.
RiffDB cannot infer an undeclared business invariant, stop application code
from calling an external service before or after a command, or infer that a
sequence of separately named commands was intended to be one business action.
Every intermediate RiffDB commit still satisfies all declared invariants.
External effects must use durable event/outbox intent when atomic coupling is
required.

Acceptance includes a checked corpus of intentionally bad applications. Each
must fail at contract/query compilation, capability issuance, service
authorization, generated-client compilation, or execution before protected
output or mutation:

- generic write;
- raw kernel read under an application capability;
- ad-hoc query under a named-only capability;
- wrong module/query/plan substitution;
- dangling relationship creation;
- concurrent duplicate unique value;
- cumulative query-cost amplification;
- N+1 or multi-snapshot TicketDesk page construction; and
- field, partition, cursor, or revocation escape through a dependent batch.

## Compatibility

The `riffdb.v1` kernel RPC inventory and existing wire bytes remain compatible.
New capability permission tags and contract/query IR constructs are additive
and require exact generated compatibility fixtures before implementation.

Existing `ReadEntity` and `ScanIndex` capabilities remain kernel capabilities;
they are not silently upgraded into application-query permissions. Existing
development deployments must issue replacement application/agent presets before
the RiffQL authorization cutover. There is no automatic capability widening.

Relationship and unique declarations change contract and plan identity.
Activation follows ordinary compatibility and migration rules. WP-290 and
WP-295 must freeze exact grammar, IR tags, index semantics, and old/current
fixtures before production implementation.

## Consequences

- Stable application code becomes a closed set of reviewed operations rather
  than a table-permission programming environment.
- Agents retain flexible textual querying without forcing that authority into
  production application credentials.
- The compatible kernel protocol remains useful but is unmistakably privileged.
- Relationship and uniqueness safety become compiler obligations rather than
  conventions in application code.
- Capabilities and query execution gain more semantic types, but applications
  lose field masks, kernel permissions, and manual access planning.
- Query budgeting becomes conservative and predictable; some formerly accepted
  high-amplification queries will require decomposition or a reviewed budget.

## Rejected alternatives

1. **Document that applications should avoid kernel RPCs.** Rejected because a
   convention does not make the bad pattern unavailable.
2. **Let `ReadEntity`/`ScanIndex` continue authorizing RiffQL.** Rejected because
   derived authority remains reusable through the raw public surface.
3. **Treat named queries as SDK convenience only.** Rejected because reviewed
   read shapes then cannot be enforced by policy.
4. **Infer relationships from field names.** Rejected because inference is
   ambiguous and contradicts contract-first semantics.
5. **Inject hidden relationship reads.** Rejected because dependencies must
   remain visible in source and explain output.
6. **Use per-step row limits as the sole resource control.** Rejected because
   repeated bounded steps amplify total work.
7. **Remove the kernel protocol immediately.** Rejected because conformance,
   administration, compatibility, and low-level diagnosis still need it.

## Work-package mapping

- WP-280: closed application-query authority and private derived proofs.
- WP-285: whole-query cost vectors and execution fuel.
- WP-290: declared same-partition relationships.
- WP-295: declared same-partition uniqueness.
- WP-300: surface cutover, generated defaults, negative canaries, and final
  safety-by-construction acceptance.
