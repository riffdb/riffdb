# ADR-0147: Aggregate Byte Budgets for Atomic Collection Commands

- **Status:** Accepted
- **Direction approved:** 2026-08-24 (maintainer, in session)
- **Exact text accepted:** Yes, 2026-08-24 (maintainer, in session)
- **Decision deadline:** Before WP-678 changes contract grammar, command IR,
  plan identity, generated command schemas, or runtime admission
- **Requires:** ADR-0002, ADR-0003, ADR-0011, ADR-0013, ADR-0031, ADR-0055,
  ADR-0059, ADR-0106, ADR-0107, ADR-0117, ADR-0120, ADR-0124, and ADR-0129
- **Amends if accepted:** ADR-0107's static collection-byte proof; it does not
  change the 256-element, 256-mutation-instance, one-partition, or 16 MiB
  complete command-graph ceilings
- **Defines or blocks:** WP-678 through WP-681 and resumption of external
  adapters whose atomic collection count and per-element value bounds cannot
  be multiplied independently
- **Amended by:** ADR-0176 permits a compiler-proved 4 MiB root atomic-command
  input while retaining every individual 1 MiB value and the 16 MiB graph
  ceiling.

Direction, package boundaries, and this exact text were accepted on
2026-08-24.

## Context

ADR-0107 deliberately made a compiler-bounded collection command prove its
worst case by multiplying each element type's maximum canonical size by the
list maximum. That rule is simple and safe, but it treats independent maxima as
if they must occur simultaneously. A record containing one individually
bounded large field therefore cannot have a useful collection cardinality even
when the complete request is independently bounded.

A real external authorization adapter exposes this limitation. Its public
write is one atomic mutation set of up to 100 elements. One optional condition
context may contain at most 524,288 bytes, while the complete submitted request
has a separate bounded encoded size. Multiplying 524,288 by 100 and then
charging the copied authoritative row graph exceeds RiffDB's 16 MiB ceiling,
so the current compiler admits only eight elements. The upstream conformance
suite exercises atomic sets of nine and nineteen ordinary small elements.
Splitting them would change the application's atomicity contract.

The missing capability is generic. Document batches, event envelopes,
configuration updates, metric labels, and authorization tuples commonly have
both a large individual-value ceiling and a smaller aggregate request ceiling.
Raising RiffDB's global command limit, weakening individual validation, or
special-casing an adapter would either increase resource exposure or encode an
external framework into the database.

Two adjacent developer-experience defects were discovered by the same real
loopback, but do not justify weakening query or application boundaries:

1. generated optional cursor parameters are decoded as ordinary values even
   though continuation is carried by the existing protected
   `Options.Cursor` transport field; and
2. `riffdb dev --run` treats a Go module root as the only runnable package,
   excluding library repositories with a dedicated checked runner package.

They are planned separately so neither can broaden or delay the atomic byte-
budget semantics.

## Decision

### 1. Add one explicit aggregate byte constraint to a collection input

The contract language adds an optional aggregate canonical-element-byte bound
to the one list expanded by a `bulk command`. Illustrative syntax is:

```riff
bulk command WriteMutations {
    input request_id: uuid
    input organization_id: uuid
    input mutations: list<TupleMutation, 1..100>
        aggregate_bytes <= 7_000_000
    idempotency_key request_id

    for mutation in mutations {
        create Tuple(organization_id, mutation.tuple_id) as tuple
            else AlreadyExists {}
        set tuple.context = mutation.context
    }

    return Written {}
}
```

`aggregate_bytes` is a positive compiler-owned literal. It bounds the sum of
the complete ADR-0011 canonical value documents for the submitted list
elements, in submitted order. It excludes the enclosing list document's fixed
version/tag/count framing and the other command inputs; the compiler charges
those fixed and independently bounded bytes separately. This definition is
stable across Rust, Go, TypeScript, Python, CLI, MCP, gRPC, and storage
representations and never means JSON, Protobuf, MessagePack, host-object, or
in-memory size.

Every element retains its complete ordinary type validation. A bytes field
declared with maximum 524,288 bytes still rejects a 524,289-byte value even
when aggregate headroom remains. The list retains its declared cardinality.
The aggregate constraint permits different elements to consume different
shares of the fixed total; it is not a way to raise any element, string,
bytes, nesting, record, list, command, mutation, or transport ceiling.

Only the single collection expanded by one `bulk command` may carry this first
constraint. Ordinary commands, nested lists, unexpanded inputs, multiple
budget pools, caller-selected budgets, and runtime budget negotiation remain
unavailable. A collection without the clause retains its exact existing
independent-maximum meaning and least-sufficient format.

### 2. Compile a conservative correlated graph-size proof

The compiler replaces independent multiplication only where the aggregate
constraint supplies a stronger proof. It derives a closed affine upper bound
for the complete canonical command input plus authoritative mutation and event
graph:

```text
fixed_bytes + aggregate_bytes * maximum_copy_coefficient <= 16 MiB
```

`fixed_bytes` includes list framing, scalar inputs, record/field framing,
service-owned values, fixed-size element components, mutation records, events,
and every other existing graph charge. The copy coefficient is the greatest
compiler-proved number of times aggregate-variable element bytes can appear
across the canonical input and complete write/event graph. It is derived from
the finite expression, binding, and instruction plan; application authors and
callers cannot state it.

The proof may be conservative. If an expression, construction, optional flow,
or future language feature cannot be assigned a finite coefficient, the
compiler rejects the aggregate constraint with a source-spanned diagnostic and
retains the old independent-maximum rule. It must never assume values compress,
deduplicate equal payloads, omit fields, or remain referenced rather than
copied. Branch exclusivity may reduce a charge only when the existing command
IR proves the branches mutually exclusive.

The compiler additionally proves the complete worst-case concrete input fits
the public request-envelope ceiling and the existing 16 MiB command-graph
ceiling. It may require an aggregate literal lower than 16 MiB because input
bytes copied into authoritative rows or events are charged again. Global
ceilings are not raised by this ADR.

### 3. Freeze successor grammar and executable IR identities

WP-678 adds least-sufficient successor grammar, executable IR, and bundle
identities. The collection-expansion successor carries the aggregate canonical
element-byte maximum and the compiler-derived maximum copy coefficient. Both
participate in command plan hashing, contract-root hashing, application locks,
generated artifacts, explain output, and compatibility fixtures.

The current topology suggests grammar V16, executable IR V16, and bundle V16.
WP-678 must re-audit `release/version-topology-v1.json` immediately before
implementation. If an intervening accepted change consumes an identity, the
package stops for administrative renumbering; it never reuses or reinterprets
an occupied identity.

V1 through V15 artifacts, hashes, locks, and exact bytes remain readable and
unchanged. A collection without `aggregate_bytes` continues to emit the least-
sufficient existing identity. An old reader rejects the successor before
deployment. The aggregate constraint changes no authoritative stored entity,
command outcome, event, journal, changelog, or commit-record encoding; concrete
accepted commands still use their existing bounded durable envelopes.

### 4. Enforce the same budget before effects on every surface

Generated Rust, Go, TypeScript, and Python methods preflight element count,
individual element types, and the exact canonical aggregate bytes before
opening or using a transport operation. CLI and MCP schemas publish the count,
per-value, and aggregate constraint using one registered RiffDB extension;
their handlers perform the same canonical preflight.

Client preflight is developer feedback, not authority. The first-party Rust
application service decodes and canonicalizes the submitted typed values,
recomputes the aggregate sum with checked arithmetic, and rejects an overflow
before authorization-dependent evaluation, conflict acquisition, staging,
journaling, or mutation. The deterministic runtime receives only a validated
sealed collection input and still validates the concrete expanded graph
against the 16 MiB ceiling before handing it to the commit coordinator.

All failures are typed, bounded, value-free, and distinguish cardinality,
individual-value, aggregate-input, and complete-graph overflow. They may report
the compiled ceiling and safely bounded observed byte count, but never echo an
element, context, secret, partial canonical document, or hidden graph shape.
No surface splits, truncates, retries a subset, or returns partial element
outcomes.

### 5. Prove atomicity with a framework-neutral 100-element corpus

WP-679 uses a neutral `PolicyMutation` fixture with an optional bounded context
field. It proves one through 100 mutations, individual context boundaries,
aggregate exact-bound and plus-one cases, small nine- and nineteen-element
sets, duplicate and business failures, cancellation, idempotent replay,
overlapping commands, crash recovery, provenance, events, and complete-or-
absent visibility through memory and redb.

Rust, Go, TypeScript, Python, CLI, and MCP must produce the same canonical
preflight observation and one real remote 100-element atomic command. Static
guards reject external framework names, schemas, routes, error mappings,
runtime branches, raw transactions, generic mutations, and handwritten
transport encoding. Real framework conformance remains the external adapter
repository's responsibility.

### 6. Correct cursor routing without changing query semantics

WP-680 fixes generated optional cursor parameters as a compatibility
correction under the existing operational-query contract. When a generated
named query exposes a cursor input, its facade routes a non-null typed cursor
to the existing protected `Options.Cursor` continuation field. It must not also
encode the cursor as an ordinary symbolic parameter. Null or absence means no
continuation. A mismatch between generated metadata and operation shape fails
locally before transport.

Generated Rust, Go, TypeScript, and Python clients share fixtures for first
page, continuation, null/absent cursor, malformed cursor, wrong operation,
wrong database/history, expiry, cancellation, and bounded iteration. An
iterator helper may be added only as a finite convenience over this exact
single-page method; it must require an explicit caller-owned maximum page or
item count and preserve typed failures. No client may walk pages implicitly to
implement count, offset, filtering, sorting, or another public semantic.

No query IR, provider, cursor byte, epoch, authorization, freshness, or server
semantics change in WP-680. Existing low-level callers that already use
`Options.Cursor` remain byte-exact.

### 7. Allow an explicit checked Go runner package

WP-681 extends `riffdb dev --run` and its installed equivalent with one
development-only `--go-runner-package` option selecting a repository-relative
Go package directory. The default remains `.` at the module root. The selected
value is canonical, nonempty, slash-normalized, contains no absolute path or
parent traversal, stays beneath the checked application root, and names exactly
one buildable `main` package within the selected module.

The first-party runner invokes `go run` directly with an argument vector and
the existing protected endpoint, credential, and database values. It does not
invoke a shell, accept arbitrary flags, inherit a caller-selected executable,
search outside the repository, or allow the runner to own transport or trust
configuration. The selected path is development process configuration, not
application data authority, and does not enter contract, module, role, plan, or
application-lock identity. The runner prints the repository-relative package
in its bounded startup summary so source and installed invocations are
auditable.

Rust, TypeScript, and Python runner selection is unchanged. Go module-root
applications remain byte- and behavior-compatible. Library-module fixtures
cover a dedicated command package plus traversal, ambiguity, non-main,
symlink-escape, nested-module, missing-tool, interruption, and argument-
injection refusals.

## Options Considered

1. **Raise the global command graph above 16 MiB:** rejected because the real
   requirement is correlated sizing, not larger resource exposure, and every
   durable and operational budget would require requalification.
2. **Lower the individual context maximum:** rejected because it would make a
   valid application value inexpressible and still would not model aggregate
   request limits.
3. **Compile for 100 times every individual maximum:** rejected because it
   admits no useful command under the current graph ceiling.
4. **Validate only the concrete request at runtime:** rejected because the
   compiler would no longer prove a finite worst-case graph or complete
   authority/capacity charge.
5. **Split one collection into several commands:** rejected because it changes
   atomicity, idempotency, provenance, events, and observable intermediate
   state.
6. **Add a framework-specific bulk primitive:** rejected because RiffDB owns a
   generic data capability and external identity/authorization frameworks
   remain adapter concerns.
7. **Add one canonical aggregate constraint plus compiler-derived flow
   coefficients:** proposed because it preserves every global ceiling while
   expressing the actual dependent size relationship.

## Consequences

- Collection cardinality and individual value size no longer have to be
  multiplied as independent simultaneous maxima when a smaller aggregate bound
  is declared.
- Compiler sizing becomes more sophisticated and may conservatively reject a
  graph whose variable-byte flow cannot be proved.
- Canonical encoding is evaluated during generated and service preflight, but
  remains bounded by the declared request ceiling and is paid once per
  invocation, never per authorization check, conflict, retry, or graph row.
- Existing global element, aggregate-instance, graph, request, transport,
  storage, and diagnostic bounds remain unchanged.
- Cursor and Go-runner corrections remove handwritten adapter glue without
  changing database semantics or making runners privileged.

## Compatibility

This ADR changes no bytes or behavior by itself. Its implementation adds
least-sufficient grammar/executable-IR/bundle successors and corresponding
plan, lock, schema, and generated-artifact identities under ADR-0124. Existing
V1 through V15 artifacts remain readable and byte-exact, and contracts that do
not use the clause keep their current writers.

The runtime admission is additive and fail-closed: a successor plan cannot be
installed on an older runtime, and a request above its declared aggregate
bound is rejected before effects. Concrete durable command envelopes retain
their current formats and ceilings. No decoder retirement is authorized.

WP-680 reuses the current cursor transport field and cursor bytes. WP-681
changes no application source or lock format and preserves the default module-
root runner.

## Security

Aggregate sizing is computed from schema-directed typed canonical values before
effectful evaluation. Checked arithmetic, nesting limits, per-value limits, and
the outer request envelope bound apply before allocation proportional to an
untrusted claimed length. Diagnostics are redacted and bounded.

The constraint grants no additional entity, field, partition, command,
mutation, query, secret, or transaction authority. Authorization still derives
from the sealed maximum plan and validates every instantiated row. An
aggregate overflow cannot reveal which element or field crossed the limit.

Cursor routing preserves the existing operation/database/history binding and
never treats a cursor as caller-selected query structure. Go runner selection
is repository-confined, shell-free, lock-bound, and receives only the same
protected values as the current runner.

## Standing Design Tests

- **Interface safety:** callers select only values within one compiled command;
  they cannot choose count, aggregate ceiling, copy coefficient, graph budget,
  transaction, split policy, cursor operation, runner executable, or trust
  configuration. Every layer recomputes or verifies its owned proof and fails
  closed before effects.
- **Scale:** element count, individual values, aggregate canonical bytes,
  variable-byte copies, complete graph, conflict set, allocations, transport,
  retries, diagnostics, cursor pages, and runner paths are explicitly bounded.
  Canonicalization and coefficient validation are paid once per plan or
  invocation, never per row, mutation, authorization atom, retry, or page
  item.

## Testing

- Parser, formatter, source-span, semantic-IR, canonical codec, plan-hash,
  topology, old/current fixture, and least-sufficient writer tests.
- Independent canonical-size evaluator and property tests across element
  counts, variable payload distributions, boundary/plus-one sizes, checked
  arithmetic, optional values, copied fields, events, and rejected flows.
- Runtime, memory/redb, deterministic schedule, cancellation, idempotency,
  journal uncertainty, crash recovery, audit, provenance, event, and atomic
  visibility tests.
- Cross-language generated preflight and real remote atomic collection
  conformance with no framework artifacts.
- Cursor first/next-page parity and repository-confined Go library-runner
  process tests.

## Requirements and Work Packages

- **WP-678:** aggregate collection-budget language, proof, IR successors, and
  compatibility fixtures.
- **WP-679:** service/runtime/generated enforcement and neutral atomic
  collection acceptance.
- **WP-680:** generated cursor-to-options routing correction and bounded helper
  conformance.
- **WP-681:** configurable checked Go development-runner package.
- New aggregate-budget requirement IDs are added to `SPEC.md` only after exact
  acceptance; implementation packages may not weaken existing `BLK-*`,
  `SAFE-*`, `PERF-*`, `API-*`, or `DRV-*` requirements.

## Decision Deadline

The exact text must be accepted before grammar, executable IR, plan hashes,
generated schema extensions or public runner configuration changes. WP-680 may
repair existing cursor routing independently
only if its compatibility audit confirms that no generated public signature or
cursor meaning changes.
