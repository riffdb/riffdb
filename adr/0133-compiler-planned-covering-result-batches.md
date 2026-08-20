# ADR-0133: Compiler-Planned Covering Result Batches and Compact Named-Query Carriage

- **Status:** Proposed
- **Direction approved:** No
- **Exact text accepted:** No
- **Decision deadline:** Before WP-654 changes contract index syntax, query IR,
  durable index production, public Protobuf, or generated query decoding
- **Requires:** ADR-0022, ADR-0023, ADR-0027, ADR-0051, ADR-0052, ADR-0053,
  ADR-0055, ADR-0074, ADR-0111, ADR-0124, ADR-0128, ADR-0130, and ADR-0131
- **Defines or blocks:** WP-654

This record is planning input only until the maintainer accepts its exact text.
It proposes public protocol, compiler identity, and durable index-production
changes that an implementation package cannot authorize by itself.

## Context

WP-653 closed the ordinary compiled `BoardPage50/200/450` path by size. On the
workstation, public p50 grows from 0.529 ms at 50 rows to 3.288 ms at 450 rows.
Storage execution owns about 65% of the marginal time and public response/wire/
client assembly owns about 35%. At 450 rows the storage stages alone total
2.095 ms, including 450 entity-envelope reads and generic named-map shaping.
The last paired cloud receipts put `BoardPage450` at 2.91x safe-application
PostgreSQL on N1 and 3.25x on E2.

An internal candidate revalidated predicates in the row-store adapter,
materialized selected fields once, and bypassed the executor's duplicate
predicate/projection pass. It improved the 450-row p50 only 4.9% and regressed
the 50-row control 5.1%, so WP-653 removed it. A compact wire change alone is
also insufficient: storage execution already exceeds the likely 1.10x public
budget before transport starts. The material candidate must remove both N
entity hydrations and repeated per-row symbolic names/maps.

The durable index schema already carries bounded canonical `covered_values`,
and index replacement already advances the affected epoch when those values
change. Accepted SPEC and ADR-0023 deliberately require every grammar/IR-v1
producer to write the canonical empty record and require a later accepted
language/IR and compatibility decision before nonempty production. This is
that decision; it does not repurpose the field silently.

The current public result repeats entity and field names in every
`ResultRecord`, then the Rust client builds a `BTreeMap<String,
ApplicationValue>` for every row before a generated facade decodes the same
fixed shape. Go, TypeScript, and Python perform equivalent generic shaping.
That representation is useful for ad hoc queries, old clients, CLI, and MCP,
but it is unnecessary for an exact named query whose plan and result schema
are already hash-bound.

## Proposed Decision

### 1. Add an explicit compiler-owned covering-index declaration

The contract language may declare a finite ordered cover on a new index:

```text
index by_project_status (
    organization_id,
    project_id,
    status,
    ticket_id
) cover (
    title,
    reporter_id,
    assignee_id
)
```

Key components are implicitly available to the cover and are not repeated in
the declaration. Cover fields are direct fields of the indexed entity only.
There are no expressions, functions, aliases, relations, aggregates, remote
lookups, dynamic field lists, or caller-provided projections. The compiler
rejects duplicates, fields outside the entity, fields whose maximum canonical
encoding is unknown, secret fields in the alpha profile, and any index whose
maximum entry, write-set, journal-frame, migration, or retained-state charge
would exceed an existing bound.

Cover order is canonical compiler IR and participates in contract bundle,
schema, plan, module, generated-binding, and migration identities. A covering
declaration cannot be added to or changed on an existing index identity.
Additive evolution declares a new index name and migrates/rebuilds it under the
ordinary receipted contract-evolution path.

Applications never request a cover, choose an index, supply a response layout,
or ask for fallback. The query compiler selects the exact declared index only
when its key plus cover contain every field needed for predicates, ordering,
selected output, and the complete compiled row-policy shape.

### 2. Produce covered values atomically from authoritative command facts

For an accepted covering index, the commit coordinator derives one canonical
covered record from the exact transaction-current entity post-image. Index
key, covered record, entity post-image, index epoch, command outcome, events,
provenance, audit, commit record, and changelog-visible mutation remain one
atomic command transition under the existing sole-writer rules. Updating any
covered field replaces the index entry and advances every already-defined
affected epoch even when the index key is unchanged. Deletes remove the index
entry through the accepted tombstone/changelog rules.

Entity state and the commit log remain authoritative. Covered values are
redundant, rebuildable index state and cannot be mutated independently. The
existing bounded `StoredIndexEntryV2` field and codec are used only if the
version-topology audit proves their current semantic identity explicitly
reserved nonempty coverage; otherwise WP-654 introduces the least successor
index-record identity. The decision is made before production writes and is
receipted in the ADR-0124 registry, never inferred after bytes exist.

Startup/recovery structural validation and deterministic reference tests must
prove that every current covering entry is derivable from the current entity
version and that every entity requiring the index has exactly the expected
entry. Validation may be paged and checkpointed at the process-generation
boundary, but a query never pays N entity reads to re-prove already validated
cover bytes. Missing, duplicate, stale, malformed, wrong-lineage, or
non-derivable coverage is authoritative corruption and withholds readiness or
the result. It never triggers entity-hydration fallback.

### 3. Compile one sealed covered-result batch fast path

The query compiler marks an exact named-query step eligible only when all of
these are true:

- it is an ordered bounded index step against the selected covering index;
- the index key plus covered record contain every predicate, order, output,
  and row-policy field;
- no later step depends on an unreturned entity field;
- no aggregate, relation hydration, nearest search, exact-text provider, or
  other engine must observe the generic row representation;
- cardinality, continuation, snapshot, policy, projected-value, and encoded-
  byte fuel remain statically bounded; and
- the complete result layout is pinned by the exact plan and module hashes.

The first-party row-store adapter may then return a move-only internal
`CoveredResultBatch`: one entity identity, one ordered field-layout witness,
bounded positional canonical rows, physical scan/point work, exact index epoch,
and optional continuation. It validates index predicates and row policy in the
same authoritative snapshot before admitting each row. The executor verifies
layout identity, row widths, ordering, bounds, work counts, continuation, and
fuel once, then transfers the positional batch without constructing a
`BTreeMap` per row.

This is a compiler-selected physical implementation of the same named query,
not a public query feature or provider fallback. A plan not marked eligible
uses the existing generic path. A marked plan whose cover proof is unavailable
fails closed; it does not silently perform N entity reads, scan another index,
or return a nearby shape.

### 4. Add additive schema-bound compact public carriage

The application Protobuf adds a compact response arm containing, per named
result field:

- result-field name and cardinality once;
- entity name once;
- the exact ordered selected field names once;
- bounded rows containing exactly one public `Value` per field position; and
- the unchanged query identity, outcome, application head, and continuation.

The request advertises a closed accepted-encoding set. Legacy requests and ad
hoc textual queries default to the existing named-record encoding. New
generated clients advertise compact support for eligible named methods; the
server selects compact only when the exact deployed plan carries the matching
layout witness. The response names the selected encoding and uses exactly one
result arm. Mixed arms, duplicate names, unknown encodings, row-width drift,
identity drift, missing values, excess rows/fields/bytes, or compact output for
an ineligible plan fail closed as an invalid response or internal invariant.

Old clients ignore the additive request/response fields and continue receiving
the legacy arm because they never advertise compact support. New clients accept
both arms so they can call an old server or an old deployed module without
semantic downgrade. Encoding negotiation changes representation only; result,
authorization, snapshot, freshness, cursor, fuel, and error semantics are
identical. It is not an application-visible consistency or safety knob.

The Protobuf descriptor, codec bounds, parent-message encoded-length golden,
strict duplicate-field decoder, session request/response framing, MCP/CLI
conversion, and every public SDK fixture advance in one lockstep compatibility
commit. This ADR explicitly amends the exact transport-shape pins and the
PERF-018 frozen request shape only for this negotiated, byte-equivalent named-
result encoding; all comparator obligations and application semantics remain
unchanged.

### 5. Generated clients decode compact rows directly into typed results

Rust, Go, TypeScript, and Python generators emit one plan-bound positional
decoder per eligible named query. It checks query/module/plan identity,
selected encoding, result-field/cardinality/entity/layout identity, exact row
width, value type, nullability, enum identity, and all collection/byte bounds
before constructing language-idiomatic typed results. It does not first build
generic parameter maps, expose field ordinals, or let application code supply a
decoder or layout.

The generic client API still converts compact rows into its existing named
record model for callers that explicitly use the generic surface. CLI and MCP
continue rendering symbolic names. Their conversion cost is acceptable because
they are not the generated high-volume application path; their output remains
bounded and redacted.

### 6. Authorization and inference safety precede physical shaping

Authentication, capability revision, role/operation authorization,
read-after-commit admission, field visibility, secret-output authority, and
pre/post execution authorization remain at their existing safe points. The
compiler admits a cover only when its complete row-policy inputs are present.
Row policy executes before a row enters the positional batch, using current
principal facts and relationship evidence from the same request. Covered but
unselected policy fields never enter the public layout.

Denied rows affect no returned count, row, continuation identity, public
diagnostic, or application-visible timing class beyond the existing bounded
policy contract. Compact encoding cannot turn absent fields into silent
redaction: an unauthorized selected field rejects the query exactly as today.
Secret fields remain prohibited from alpha covers even when ADR-0128 would
authorize their output, avoiding a new durable secret-copy surface in this
package.

### 7. Mechanics and public gates control activation

Before implementation, WP-654 publishes a closed 50/200/450 prediction from
the WP-653 stage ledger. The candidate must separately attribute:

- entity-envelope hydration avoided by the cover;
- generic executor/map shaping avoided by the internal positional batch;
- service/protobuf/client name and map work avoided by compact carriage; and
- incremental index decode, write amplification, migration, and startup proof
  costs introduced by the cover.

Activation requires paired same-run receipts on the workstation, N1, and E2.
`BoardPage450` must improve at least 40% from the WP-653 path and reach at most
1.10x safe-application PostgreSQL on both cloud profiles. BoardPage50/200,
ordinary reads, every representative unary write, mixed c32 throughput/p95,
seed, startup, contract deployment, backup/restore, and recovery must remain
inside their accepted gates; covering write amplification may not consume the
5.0x seed ceiling. Results and generated values must be byte/semantically exact
across legacy and compact arms in all four languages.

If the combined candidate misses, compact carriage may remain only when it has
an independently measured >=20% customer-path win and no regression; covering
production remains disabled unless it independently clears its declared
read/write/startup gates. No threshold is weakened and no partial implementation
silently changes the default.

## Options Considered

1. **Keep optimizing generic executor maps:** rejected by WP-653's measured
   4.9% result against a 20% predeclared threshold.
2. **Add compact Protobuf only:** rejected as the complete fix because storage
   execution alone already misses the public comparator budget.
3. **Use the current projected provider:** rejected for this page because its
   measured 27--29 ms path is much slower and ADR-0130 forbids silent provider
   substitution.
4. **Cache decoded entities:** rejected because invalidation, policy, and memory
   bounds add risk while every page still performs N lookups and generic
   shaping.
5. **Compiler-planned cover plus positional internal/public batches:** proposed
   because it removes both measured O(rows) costs while keeping the operation,
   layout, bounds, policy, and provider compiler-owned.

## Consequences

- Large bounded application pages can approach one index walk plus value
  carriage rather than N durable entity hydrations and repeated symbolic maps.
- Covered fields increase authoritative write, journal, changelog, backup,
  rebuild, and retained-index bytes; the compiler charges and bounds that cost.
- Cover declarations and compact transport add new compiler, compatibility,
  SDK, and conformance responsibilities.
- Generic/ad hoc query behavior remains available and unchanged.
- Secret covers, relation/aggregate batches, arbitrary projections, public
  ordinals, runtime index choice, and provider fallback remain deferred.

## Compatibility

This proposal changes contract grammar/IR, bundle and plan identities for
contracts that opt into a new covering index; it does not reinterpret existing
indexes or modules. Existing entity and empty-cover index bytes remain readable
and byte-identical. New cover state is created through an additive index and a
receipted rebuild/migration with version-topology registration and retirement
rules.

The public Protobuf change is additive and negotiated. Existing clients and
servers continue using legacy named records. New clients validate and support
both representations. No durable command, entity, event, provenance, outcome,
cursor, or capability identity changes merely because compact carriage is used.

## Security

Only the compiler can select a cover or layout. Capabilities authorize named
operations and fields, never physical encodings. Row and field policy run before
release and are not cached in cover state. All diagnostics and telemetry remain
fixed-cardinality and value-free. Covered fields expand the at-rest copy set, so
alpha excludes secrets and backup/export classifiers must inventory every new
covered field. Malformed or mismatched cover/layout state fails closed without
revealing entity keys, values, field presence, policy facts, or index structure.

## Standing Design Tests

- **Interface safety (AGENTS.md boundary 11):** applications and agents still
  invoke one generated named query with typed bounded parameters. They cannot
  choose an index, cover, field layout, response arm, validation level,
  fallback, snapshot, policy, or hydration strategy. Both encodings prove the
  same exact plan and fail closed on drift; no guarantee can be opted out of.
- **Scale:** cover bytes, index amplification, migration pages, startup proof,
  scan work, rows, columns, values, response bytes, continuation, client decode,
  and diagnostics are compiler bounded. Execution materializes only the
  requested page and uses no database-wide cache, full-population result, or
  co-located-storage assumption beyond the existing single-node row-store POC.

## Testing

- Grammar span snapshots and IR/hash/lock fixtures for valid, duplicate,
  oversized, secret, relationship, expression, and changed-existing-index
  covers.
- Coordinator/reference-model properties for create/update/delete, covered-only
  updates, unchanged covers, epoch advancement, changelog, crash recovery,
  rebuild, backup/restore, and current-entity/cover equivalence.
- Corrupt, stale, missing, duplicate, mixed-version, wrong-lineage, and torn
  cover fixtures proving refusal before release and no entity fallback.
- Executor fuel, continuation, ordering, cardinality, policy, revocation,
  read-after-commit, and exact-layout adversarial tests.
- Strict Protobuf/gRPC/session codec tests and parent-message goldens for legacy,
  compact, malformed, mixed-arm, oversized, old-client/new-server, and
  new-client/old-server cases.
- Rust, Go, TypeScript, and Python generated conformance from one manifest and
  golden result corpus, including nullable and enum values.
- Paired workstation/N1/E2 50/200/450, mixed, unary, seed, startup, deployment,
  recovery, and backup/restore receipts under the predeclared activation gates.

## Requirements and Work Packages

- **Requirements:** `PERF-001`, `PERF-002`, `PERF-008`, `PERF-018`, `STO-002`,
  `API-001`, and `QRY-001` through `QRY-004`
- **Defines or blocks:** `WP-654`
- **Final evidence:** WP-654 and the Agent Application Alpha gate

## Decision Deadline

Exact human acceptance is required before WP-654 edits SPEC, grammar, query or
contract IR, durable index production, public Protobuf, generated clients, or
compatibility fixtures. A direction approval permits only mechanics probes and
fixture planning.
