# Safety by construction

Status: normative target for WP-280 through WP-300 under accepted ADR-0055.

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
