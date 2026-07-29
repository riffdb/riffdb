# Safety by construction

Status: implemented application-surface rule for WP-280 through WP-300 under
accepted ADR-0055.

RiffDB's application product is not a safer collection of table operations. It
is a closed set of compiled domain operations whose unsafe alternatives are not
available to ordinary application credentials.

## Interaction profiles

| Profile | May invoke | Must not receive |
|---|---|---|
| Stable application | Exact named commands and exact named deployed queries | Ad-hoc RiffQL, raw entity/index operations, field masks, encoded keys |
| Scoped agent/development | Named operations and explicitly granted ad-hoc RiffQL check/explain/execute | Raw entity/index operations unless a separate kernel credential is deliberately used |
| Kernel/administration | Compatible low-level entity/index and administrative operations | No implicit application or agent authority |

Permissions do not flow upward between these profiles. In particular,
`ReadEntity` and `ScanIndex` do not authorize RiffQL, and application-query
permissions do not authorize the corresponding kernel RPCs.

The checked development presets are:

| Preset | Credential contents |
|---|---|
| `ticketdesk-application` (default) | Exact TicketDesk commands and exact query-module-hash/query-name pairs |
| `ticketdesk-agent` | Named operations plus explicit ad-hoc check, explain, and execute; no kernel reads |
| `ticketdesk-kernel` | Raw contract/entity/index diagnosis only; no application commands or RiffQL |

`riffdb dev` issues one selected credential. It has no `full`, `all`, or
combined preset.

## Product rules

1. Application writes invoke only named compiled commands.
2. Stable application reads invoke only exact named deployed queries.
3. Agent ad-hoc RiffQL is a separate, explicit capability.
4. Raw kernel reads require an explicit kernel/admin capability and are absent
   from normal generated clients and MCP catalogs.
5. Relationships and same-partition uniqueness are contract declarations whose
   enforcement is proven by the compiler and authoritative transaction.
6. Every query carries one compiler-proven, policy-authorized whole-request
   cost vector enforced again as execution fuel.
7. Missing fields, rows, relationships, indexes, authority, or budget reject
   the operation; they do not silently weaken the requested semantics.

For compatibility, the current capability's `max_scan_rows` is applied once to
the complete query's scanned-row, point-read, dependent-key, and intermediate-
row totals. It is not reset for each binding. The compiler also hashes bounded
step, projected-value, and encoded-result-byte maxima into the plan. The
executor decrements matching fuel and treats inconsistent backend work reports
as a closed failure. Fuel exhaustion cannot publish a partial result or cursor.

## Required same-partition relationships

A required relationship is declared on the referencing entity using stored
fields and one complete target primary key:

```riff
reference project
  (organization_id, project_id)
  -> Project(organization_id, project_id)
```

This is an integrity declaration, not query shorthand. Compilation rejects the
declaration unless the target list is the complete primary key in canonical
order, source and target types match exactly, every source component is
required, and both entities have the same aggregate-scoped partition identity.
Names, matching field spellings, equal runtime bytes, and a shared tenant UUID
do not establish colocation.

A command that creates a referencing record, or sets any relationship
component, must contain an earlier source-declared `read` of the exact resulting
target key. Target expressions are compared structurally after type resolution.
A partial key, different input, later read, `mutate` binding, or runtime equality
does not count. The read's `else` branch is the declared missing-target business
outcome.

The compiler records this proof as a `RelationshipCheckPlan` naming the
relationship, source binding, and target read. Command explain renders the
link. No hidden read is injected: the ordinary read observation remains in the
dependency set and the commit coordinator rechecks it before authoritative
mutation. Missing evidence, a changed target, or a plan mismatch fails closed
without a commit sequence.

Only required same-partition relationships are supported. Optional,
cross-partition, polymorphic, cascading, deferred, and inferred relationships
are rejected rather than approximated.

## Declared same-partition uniqueness

Scoped uniqueness is declared on an entity and backed by an authoritative
index:

```riff
unique user_email (organization_id, email)
```

The declaration is accepted only when every component is required and
key-compatible and the key begins with the complete aggregate-root route.
RiffDB does not infer uniqueness from names, application checks, a currently
empty scan, or a globally repeated field. Global, collation-dependent,
cross-partition, optional, and undeclared uniqueness are unsupported.

For every create or mutation that can change the value, the compiler derives
the complete resulting tuple from validated command inputs. That tuple becomes
an additional logical conflict capability. If the result depends on an entity
read, omitted assignment, clock, or other runtime value, compilation rejects
the command; the application cannot substitute a check-then-write sequence.

After semantic validation and while the conflict capability is still held, the
commit transaction reads the exact complete unique prefix. Vacant or
same-entity ownership may proceed. Ownership by another entity produces the
durable, retry-replayable `UniqueConflict` execution result before capacity or
commit-sequence assignment. Entity mutation, removal of an old unique entry,
creation of the new entry, outcome, provenance, and commit record remain one
atomic write.

Startup readiness recomputes each declared unique entry from the authoritative
entity post-image and validates both directions in one immutable recovery view.
A missing, duplicate, stale, malformed, or orphan unique-index row is
authoritative corruption. Recovery withholds operational ports and never
silently repairs or chooses a winner.

## Bug classes made unavailable

The stable application profile must prevent:

- generic insert/update/delete and host-language transaction callbacks;
- lost updates and write skew for declared command dependencies and invariants;
- duplicate mutation after uncertain retry;
- partial mutation/outcome/event/provenance/outbox commits;
- raw N+1 and multi-snapshot page assembly;
- unreviewed or string-constructed query execution;
- writes that establish a declared relationship without proving the target;
- races that violate a declared same-partition unique key;
- cross-partition or unindexed query fallback;
- ambiguous result cardinality; and
- cumulative query work above the admitted budget.

## Claim boundary

RiffDB proves declared semantics. It cannot infer a business rule absent from the
contract, infer that several separately named commands were intended as one
business action, or stop a process from calling an external service outside
RiffDB. A workflow that requires atomic database state and an external effect
must commit durable event/outbox intent in one command.

The safety claim therefore applies to supported RiffDB application interactions,
not arbitrary surrounding application behavior. Every committed intermediate
state must still satisfy all declared invariants.

## Required negative acceptance

The application-platform gate includes small intentionally bad programs. A gate
passes only when each program fails before protected output or mutation:

- application credential calls `GetEntity` or `ScanIndex`;
- named-only credential submits ad-hoc RiffQL;
- caller substitutes another module hash, query name, plan, partition, or
  capability revision;
- command creates a dangling declared relationship;
- concurrent commands attempt the same declared unique value;
- repeated query steps amplify a capability's whole-request budget;
- dependent batch exceeds field, row, partition, cursor, or revocation scope;
- TicketDesk attempts a public N+1 or multi-snapshot detail page; and
- generated stable-application code attempts to construct raw query source,
  field masks, encoded keys, or kernel requests.

Positive acceptance must also prove that the same stable application capability
can execute its exact generated commands and named queries through gRPC, CLI
wrappers, and generated MCP tools without gaining any rejected authority.

Run the complete checked negative corpus with:

```bash
./scripts/safety-by-construction-acceptance
```

The corpus registry is
`tests/authorization/fixtures/bad-application-corpus.json`. It maps every
`SAFE-008` case to executable compiler, policy, service, transaction, fuel, or
application-boundary evidence. `riffdb-dev-acceptance` additionally proves
against live `riffdbd` that the default credential can execute the exact
TicketDesk workload but cannot submit ad-hoc RiffQL.

## Migration from pre-cutover development credentials

Capabilities issued before the named-authority cutover are not upgraded or
narrowed in place. Create one replacement credential for the intended profile,
move the caller to it, verify its exact named operations, and revoke the old
credential. Do not reuse an old capability containing `ReadEntity` or
`ScanIndex` as an application credential.

Stable Rust application code should accept `StableApplicationClient`, which
has no entity/index or administrative methods. Kernel tooling deliberately uses
`RiffDbClient` and a separate kernel/admin credential. Generated TypeScript and
MCP artifacts contain exact named operations and immutable module hashes; raw
fixed MCP tools are hidden by current-policy discovery for application and
agent credentials.

The current lower-level JSON capability documents still encode
compiler-derived field visibility and command IDs. They are implementation
fixtures, not the application authoring format. Symbolic role compilation
replaces those documents in WP-310 without changing the authority separation
established here.
