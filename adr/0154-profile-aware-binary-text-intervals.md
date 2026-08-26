# ADR-0154: Profile-Aware Binary Text Intervals for Bounded Operational Queries

- **Status:** Accepted
- **Direction approved:** 2026-08-26
- **Exact text accepted:** Yes, 2026-08-26
- **Accepted:** 2026-08-26
- **Acceptance reference:** Maintainer exact-text acceptance in the current
  Codex session for upstream commit `03eea78c`
- **Decision deadline:** Before WP-700 admits a range predicate over a
  `binary_utf8_v1` index component
- **Requires:** ADR-0038, ADR-0051, ADR-0053, ADR-0055, ADR-0108, ADR-0111,
  ADR-0124, ADR-0129, ADR-0134, ADR-0145, and ADR-0150
- **Amends if accepted:** ADR-0150's closed operational component-capability
  matrix by making the `binary_utf8_v1` range/complement cell executable under
  the restrictions below
- **Defines or blocks:** WP-700 and WP-701

This record is authoritative for WP-700 and WP-701.

## Context

RiffDB's ordinary operational-query plane already supports one compiler-sealed
physical interval over order-preserving canonical `i64`, `u64`, `timestamp`,
`date`, UUID, and enum index components. It also supports exact equality,
bounded membership, leading-byte prefix selection, and complete bytewise order
over a declared `text_key(field, binary_utf8_v1)` component. Memory and redb
consume the same normalized range schedule, page and scan budget, plus-one
probe, and opaque continuation rules.

ADR-0150 intentionally left the binary-text interval cell unavailable until a
real consumer established its exact semantics. That consumer now exists. A
tuple-oriented authorization store uses raw ULID strings as public changelog
continuation tokens and requires one named query equivalent to:

```riffql
where store_id == $store_id
    && change_id > $from
    && change_id < $horizon
order by change_id asc
```

The continuation token must remain the original ULID string, and changelog
order is its exact UTF-8 byte order. Client-side filtering, cursor walking, or
conversion to a generated RiffDB cursor would change the public protocol and
can produce incorrect page boundaries. A duplicate canonical index cannot
prove the requested order because RiffDB's canonical string key encoding is
length-first. The declared `(store_id, change_id)` binary-text index already
has the correct physical bytes, but the closed capability registry rejects
`<`, `<=`, `>`, and `>=` for that component.

This is not a reason to add an application-specific changelog path or a ULID-
only query primitive. ULID is one useful validation domain, but the missing
database capability is broader: a bounded string field with an already
declared exact binary UTF-8 ordering cannot use the ordinary typed comparison
operators to constrain that same physical order. Adding a ULID scalar now
would also introduce contract syntax, canonical values, wire values, generated
types, persistent encodings, evolution rules, and validation semantics that
the query does not require.

The existing ordinary query IR already carries the comparison operator, typed
string value, selected index fields, component encoding, direction, total
order, row limit, and cursor identity. The existing transient range schedule
already represents inclusive and exclusive endpoints. The narrow missing work
is a compiler capability proof and profile-aware endpoint lowering. If
implementation shows that those existing identities cannot carry or validate
the complete proof before activation, work must stop for ADR-0124
classification rather than adding an implicit runtime convention.

## Proposed Decision

### 1. Admit one profile-aware binary-text interval cell

The ordinary operational component-capability registry will permit
`NotEqual`, `<`, `<=`, `>`, and `>=` on a required bounded string field when,
and only when, all of the following are true:

- the compiler selects a declared `text_key(field, binary_utf8_v1)` component;
- every preceding index component is consumed by an exact compiler-proved
  prefix, including the complete partition route;
- the binary-text component supplies the first remaining declared order term;
- the query declares the complete remaining index order in one direction and
  retains its deterministic unique entity-key tie-breaker;
- the predicate set contains at most one lower and one upper bound, or one
  `NotEqual` complement, on that component;
- every predicate is physically consumed before page, continuation, row-policy
  result admission, and output formation; and
- the existing query page, plus-one probe, scan, hydration, output, parameter,
  range-count, and aggregate encoded-byte ceilings remain satisfied.

The grammar gains no caller-selectable comparison profile, index, collation,
or execution option. Applications continue to declare a finite named RiffQL
query with typed parameters. The compiler alone selects the compatible
declared index or rejects the source with `RDB-QP003` and a source-spanned
remedy.

Canonical string components remain ineligible for range and complement
planning. This decision does not reinterpret their length-first physical
encoding and does not allow a compiler or backend to substitute one encoding
for another.

### 2. Freeze logical and physical comparison semantics

For this cell, string comparison is lexicographic comparison of the canonical
valid UTF-8 byte sequence. No normalization, locale, case folding, numeric
chunking, Unicode collation, regex, wildcard, tokenization, or ULID parsing is
performed. Because UTF-8 preserves Unicode scalar-value order, the behavior is
deterministic across Rust, Go, TypeScript, Python, CLI, MCP, memory, and redb;
generated callers submit and receive ordinary bounded strings.

The compiler records the selected existing `binary_utf8_v1` component in the
complete access shape. Runtime validates that exact component profile once per
plan and request, encodes each submitted endpoint through that profile once,
and constructs the physical half-open range before storage traversal:

- `field > value` begins after the complete encoded value prefix;
- `field >= value` begins at the complete encoded value prefix;
- `field < value` ends before the complete encoded value prefix;
- `field <= value` ends after the complete encoded value prefix; and
- `field != value` forms at most the two disjoint ranges below and above the
  complete encoded value.

The endpoints are confined beneath the preceding exact index prefix. A missing
lower or upper bound means only the start or end of that already partition-
bounded prefix, never an unpartitioned index or database scan. Contradictory
submitted bounds return an exact empty page without storage work.

The logical field value remains the original string. Physical ordered bytes
must not appear in results, diagnostics, cursors, MCP text, generated values,
or application-visible continuation tokens. The database does not validate
that a string is a ULID and does not invent, normalize, or translate an
external continuation token.

### 3. Reuse one bounded range schedule and cursor contract

Profile-aware text intervals use the same immutable physical range schedule
and memory/redb traversal contract as canonical intervals. Forward traversal
sorts ranges and rows in ascending physical order. Reverse traversal reverses
both range order and traversal within each range. One page limit, one plus-one
probe, one scan/fuel budget, one hydration budget, and one output budget apply
to the entire schedule, including a two-range `NotEqual` complement.

The ordinary opaque RiffDB cursor continues to bind the complete predicate
selection, endpoint values, selected index identity and encoding, direction,
module/plan identity, policy context, snapshot, and last emitted physical key.
Changing either endpoint, order, profile, role-relevant policy identity, or
query module invalidates continuation. A named query may also return the
unchanged logical string field for an external protocol that uses that value
as its own next request parameter; doing so does not convert the external value
into a RiffDB cursor or weaken snapshot semantics within one invocation.

No post-page logical filter may repair an incorrectly lowered range. Storage
adapters consume only the sealed physical endpoints and cannot choose a text
profile, comparator, inclusive/exclusive rule, merge strategy, or fallback.

### 4. Preserve authority, policy, and bounded work

Authorization continues to include the entity, predicate field, selected
index, returned fields, row ceiling, and complete named operation. Row policy
is applied through the existing partition-aligned or bounded admitted-row
mechanism before any unauthorized row contributes to results or work-visible
behavior. This decision does not permit a cross-partition range or infer
partition authority from the text interval.

Profile validation, endpoint encoding, range construction, overlap checking,
and compatibility proof are paid once per plan and request. They may not repeat
per scanned row, returned row, page item, storage operation, policy predicate,
or generated-language layer. Work remains proportional to the compiler-bounded
page and scan ceilings, not the population behind an unbounded text range.

An endpoint exceeding its declared bounded string size fails before storage.
Diagnostics may name only the safe field/index symbol, operator class, and
bounded actual/maximum numbers. They contain no submitted endpoint, row value,
cursor contents, policy exclusion, index bytes, or inferred token structure.

### 5. Avoid a speculative scalar and broader search semantics

This decision does not add a `ulid` contract scalar. A future real consumer may
justify a ULID domain type if it needs compiler-owned construction, validation,
canonical wire representation, arithmetic, or evolution semantics beyond an
ordered bounded string. That proposal must define its complete durable and
generated-surface consequences independently.

This decision also does not add locale collation, natural sort, case-insensitive
range, suffix/substring range, regex, full-text relevance, projection-provider
bridges, multiple interval dimensions, Cartesian prefix products,
intersections, skip scans, adaptive planning, or residual filters. Those remain
unavailable unless a later real-consumer decision supplies semantics and
bounds.

### 6. Use existing identities only if they are sufficient

The intended implementation changes no grammar token, query predicate tag,
query-plan layout, index-key encoding, cursor envelope, storage record, wire
message, or generated method signature. Previously compiled modules contain no
executable binary-text interval because the compiler rejected that shape. A
newly compiled plan already identifies its comparison operators and selected
ordered-byte key schema, so its ordinary module and lock hashes change through
the existing canonical compilation path.

Activation and execution must validate that the runtime supports this exact
profile/role combination before storage access. An older or incomplete runtime
must refuse the plan with typed refresh/upgrade guidance; it may not decode the
shape and fail only after returning a partial page. If the current application
or query-module compatibility identity cannot enforce that gate, WP-700 stops
and returns for a least-sufficient versioned successor under ADR-0124. No
existing tag may be reinterpreted merely to avoid a version change.

### 7. Keep external acceptance value-free

Acceptance includes a framework-neutral changelog-shaped corpus and one value-
free external capability receipt proving that a consumer can compile and run
a strict lower/upper binary-text interval in bytewise ascending order using its
own returned logical token. External schema names, routes, adapter source,
generated profiles, authorization-model semantics, and test-suite fixtures
remain in their owning repository.

The receipt may report bounded row counts, operation counts, and timings. It
must not claim general ULID support, PostgreSQL parity, or external framework
completion.

## Options Considered

1. **Add a generic ULID scalar:** rejected for this blocker because it would
   create a new durable and generated type family while the consumer requires
   only exact preservation and bytewise ordering of a bounded string.
2. **Translate the external token into a RiffDB cursor:** rejected because the
   external protocol requires the raw token and because cursor walking changes
   its interval and pagination semantics.
3. **Use a canonical string index:** rejected because its length-first physical
   order does not prove lexicographic UTF-8 intervals.
4. **Filter or sort after reading a page:** rejected because it produces
   incorrect page boundaries and work accounting and violates the safe public
   query boundary.
5. **Duplicate indexes for selection and ordering:** rejected because the one
   binary-text component already has the required exact physical order and
   duplicate maintenance consumes bounded atomic-write index budgets.
6. **Complete the existing binary-text interval cell:** proposed because it
   reuses a real provider, one closed compiler capability, and the existing
   bounded physical range and cursor machinery.

## Consequences

- One declared binary-text index can support equality, bounded membership,
  prefix, strict/inclusive intervals, complement, and compatible bytewise order.
- Raw ordered string tokens can remain application protocol values without
  client page walking or a database-owned token type.
- The compiler, executor, memory backend, and redb backend gain another matrix
  cell and must share exact boundary truth tables.
- Canonical string ranges and broader collations remain unavailable.
- A later ULID scalar remains possible without changing these generic text-
  interval semantics.

## Compatibility

This Proposed ADR changes no current bytes or behavior. After acceptance,
existing contracts, indexes, query modules, locks, cursors, and storage remain
readable and byte-exact. A source query previously rejected for a binary-text
range may compile to a new ordinary plan and therefore receives new normal
module, plan, role, lock, and generated-artifact identities.

No persistent index rebuild is required for an existing
`binary_utf8_v1` component because its bytes already carry the selected order.
Any implementation need for a new query-IR, application-lock, cursor, storage,
or index-key version is a stop condition requiring an ADR-0124 classification
and exact human acceptance before merge.

## Security

Callers provide only bounded typed endpoint values to a compiler-generated
named query. They cannot choose the field, operator family, index, profile,
collation, direction, inclusivity rule, partition, snapshot, work budget,
policy mode, or fallback. Authorization and row policy remain mandatory before
result contribution, and unsupported shapes fail before storage.

Errors, explain output, logs, metrics, and external receipts remain value-free.
The interval must not expose unauthorized row existence through counts,
cursors, page length, scan-class selection, or timing-dependent fallback.

## Standing Design Tests

- **Interface safety (AGENTS.md boundary 11):** applications invoke one finite
  named query with typed bounded strings. All physical comparison, index,
  cursor, policy, and budget choices remain compiler/runtime owned, and there
  is no raw predicate, collation, scan, or fallback selector.
- **Scale:** each invocation constructs at most one lower/upper interval or two
  complement ranges beneath one exact partition prefix and traverses them
  under existing finite scan/page/output ceilings. It performs no population
  materialization, page walk, cross-partition scan, or per-row profile proof.

## Testing

- Compiler source-span tests for lower-only, upper-only, closed/open bounded,
  contradictory, and `NotEqual` binary-text predicates with compatible and
  incompatible indexes and orders.
- A frozen boundary table for empty strings, ASCII, multibyte UTF-8, embedded
  zero bytes where the bounded string model permits them, maximum-length
  values, and length-divergent values such as `doc-3` and `doc6`.
- Memory/redb forward and reverse parity across strict and inclusive bounds,
  two-range complement continuation, plus-one probes, page boundaries, and
  snapshot-bound cursor rejection after predicate or module changes.
- Property tests comparing physical traversal with the authoritative logical
  UTF-8 byte comparator over bounded generated strings.
- Authorization, row-policy, cross-partition refusal, endpoint-size, scan-
  budget, cancellation, redaction, and typed-unavailable tests.
- Compatibility checks proving unchanged old artifacts and activation-time
  refusal on runtimes lacking the matrix cell.
- Generated Rust, Go, TypeScript, Python, CLI, MCP, local-driver, and remote-
  gRPC conformance plus a value-free external receipt.
- Architecture checks proving one registry, one endpoint-lowering owner, one
  range schedule, no ULID/framework branch, no residual filtering, and no per-
  row validation.

## Requirements and Work Packages

- **Future requirements after exact acceptance:** `OQ-056` through `OQ-061`
- **Compiler, lowering, storage, and compatibility:** WP-700
- **Generated surfaces, documentation, and external acceptance:** WP-701

## Decision Deadline

Exact human acceptance is required before WP-700 makes the binary-text
range/complement capability executable. Any new scalar, grammar operator,
comparison profile selectable by a caller, persistent index encoding, query-IR
or cursor format, multi-dimensional interval, residual filter, or weakening of
partition/policy/work bounds requires separate exact review.
