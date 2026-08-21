# ADR-0135: Packed Compiler-Bound Named-Result Carriage

- **Status:** Proposed
- **Direction approved:** Not yet approved
- **Exact text accepted:** Not yet accepted
- **Decision deadline:** Before a production named-query response emits a
  packed result arm or a generated client advertises it
- **Requires:** ADR-0040, ADR-0055, ADR-0056, ADR-0074, ADR-0123, ADR-0124,
  ADR-0127, ADR-0130, ADR-0131, and ADR-0133
- **Defines or blocks:** the packed named-result implementation package and
  WP-623 qualification

## Context

ADR-0133 and WP-654 removed N entity hydration and generic server-side result
maps from eligible covered named queries. They retained `COMPACT_V1`, which
names the result and columns once but still creates one Protobuf `Value` object
per cell and one `CompactResultRow` per row. A 450-row, six-cell page therefore
crosses the public boundary as 2,700 nested generic messages and is decoded
cell by cell before generated result construction.

Current-HEAD cloud evidence in
`docs/performance/wp-623-current-head-diagnostic.md` measures the covered query
executor at about 1.3 ms while public `BoardPage450` remains about 4.75 ms.
The already shipped projected-query `PACKED` arm carries the same 450-by-six
canonical cell shape as packed columns and saves 2.296 ms on N1 and 1.962 ms
on E2 versus its row arm. Applying that observed saving to the compiled page
predicts approximately 2.46 ms and 2.81 ms. This is close enough to the paired
1.10x safe-PostgreSQL ceilings to justify a reject-first implementation, while
being materially larger than any remaining storage or service micro-stage.

The projected packed codec is already first-party Rust, bounded, strict,
fuzzed, and deployed. A second cell format would add compatibility and attack
surface without evidence. The named-query extension must reuse the exact
canonical-cell encoding and `PackedColumn` rules rather than invent a faster
but semantically nearby representation.

## Proposed Decision

### 1. Add one negotiated `PACKED_V1` named-result arm

`NamedResultEncoding` gains one additive `PACKED_V1` value.
`ExecuteQueryResponse` gains one optional packed result field containing:

- result-field name, cardinality, and entity name once;
- the exact ordered selected field names once;
- the bounded row count;
- exactly one `PackedColumn` for each selected field, in compiler-fixed order;
  and
- no row messages and no generic public `Value` messages.

Each column is the existing concatenation of canonical cell encodings plus
`row_count + 1` checked `u32` offsets. The canonical value codec, tag meanings,
numeric and decimal representation, string/bytes bounds, UUID form, enum
identity, null semantics, and strict trailing-byte rejection are unchanged.
This ADR allocates no new canonical value or durable format.

An eligible generated client advertises the closed ordered set
`LEGACY_RECORDS, COMPACT_V1, PACKED_V1`. The server chooses the strongest arm
it can prove for the exact deployed plan. Old clients advertise neither newer
arm and retain legacy results. Existing compact clients continue accepting
`COMPACT_V1`; a server never sends `PACKED_V1` unless the request advertised
it. Unknown, duplicate, unordered, or impossible accepted sets fail closed.

The response names exactly one selected encoding and populates exactly its
matching result arm. Mixed arms, absent selected arms, legacy fields beside a
compact/packed arm, unknown encoding, layout drift, width drift, offset drift,
or trailing cell bytes are invalid responses. Parent-message encoded-length
bounds and strict duplicate-field preflight advance in the same lockstep
commit as the schema.

### 2. Packing remains compiler-selected physical execution

Only a named query already carrying ADR-0133's exact covered-result layout
witness may use `PACKED_V1`. The request cannot provide a layout, field ordinal,
cell type, packing profile, index, provider, or fallback. The service result is
unchanged; the gRPC adapter packs the move-only canonical rows after all
execution and authorization checks.

Packing changes representation only. Query identity, module and plan hashes,
outcome, application head, continuation, snapshot, freshness, row order,
cardinality, nullability, policy, fuel, and public errors are identical to
`COMPACT_V1`. Because the exact field layout is already plan-bound, this
extension does not rotate query IR, query module, plan, contract bundle, lock,
or durable storage identities. The public schema and generated binding
fixtures rotate under ADR-0124's ordinary additive protocol ceremony.

An eligible plan whose canonical batch cannot be packed fails closed. It does
not fall back after execution to another index, provider, hydration path,
query, field set, or semantic result. Negotiation may select an older
byte-equivalent arm before execution only when both endpoints and the deployed
plan lawfully support it.

### 3. Generated clients decode columns directly into typed rows

Rust, Go, TypeScript, and Python generators emit one plan-bound packed decoder
per eligible named query. It validates before release:

- exact contract/module/query/plan identity;
- selected encoding and exclusive response arm;
- result name, cardinality, entity, ordered field names, and column count;
- row count, offset count, monotonic offsets, final offset, column bytes, and
  total response charge;
- exactly one canonical value per cell slice with no trailing bytes;
- the generated field's type, enum identity, nullability, and collection
  bounds; and
- exact row count and continuation rules.

The decoder constructs the generated row in row order directly from column
slices. It does not first construct `Value`, `ApplicationValue`, a symbolic
map, or an application-visible ordinal table. A failure releases no partial
typed result.

The generic Rust client may convert a valid packed arm to its existing named
record model for callers of the generic API. CLI and MCP may do the same for
bounded symbolic rendering. That compatibility conversion is not used by the
generated high-volume path and cannot change its safety checks.

### 4. Authorization and redaction precede packing

Authentication, current capability revision, operation and field authority,
row-policy evaluation, read-after-commit admission, secret-output authority,
and post-execution authorization remain at their existing per-operation safe
points. Packing sees only rows already authorized for release. It never stores
an allow decision, policy fact, credential, hidden row, or secret field outside
the ordinary response buffer.

Denied or revoked data cannot affect public row count, offsets, column count,
continuation, diagnostics, or encoding selection beyond the existing query
contract. Malformed packed data produces the same redacted typed invalid-
response/internal outcome as malformed compact data; cell contents never enter
logs, telemetry, errors, or evidence.

### 5. Bounds and cancellation are unchanged and explicit

The compiler's page, field, cell, and maximum encoded-result charges bound the
complete packed allocation before work. The adapter uses checked cumulative
lengths, at most one output buffer and one offset vector per compiler-fixed
column, and the existing response-byte ceiling. Generated decoders reject
before proportional allocation whenever the envelope, row count, column
count, offsets, or bytes exceed the exact operation schema.

Packing is request-local and cancellation-safe. No buffer, decoded value, or
layout proof survives the operation. It introduces no cache, process-global
state, cross-request dictionary, unbounded compression, deferred work, or
data-dependent telemetry cardinality.

### 6. Activation is conjunctive and reject-first

The implementation package first ports the existing projected packed codec
without changing the selected production encoding, proves byte/semantic
equivalence in all four generated languages, and measures paired
`BoardPage50/200/450` controls on workstation, N1, and E2.

Production selection requires all of the following:

- `BoardPage450` reaches at most 1.10x same-run safe-application PostgreSQL p50
  on both cloud profiles;
- `BoardPage450` improves at least 30% versus same-run `COMPACT_V1` on every
  profile;
- 50- and 200-row pages, point/list/detail reads, every representative unary
  write, c32 mixed throughput and p95, seed, startup, recovery, backup/restore,
  generated size, and memory remain inside their existing gates, with no
  otherwise-applicable metric regressing more than 5%;
- legacy, compact, and packed arms return identical typed values, outcome,
  order, head, cursor, and authorization behavior in Rust, Go, TypeScript, and
  Python; and
- strict malformed, mixed-arm, cancellation, truncation, over-bound, and
  version-skew tests pass through unary and accepted session envelopes.

A miss leaves `PACKED_V1` unselected and removes any production default change.
The mechanics or codec may remain only as non-default compatibility code when
it independently passes all semantic tests and introduces no measurable
regression. No performance threshold may be met by omitting a check, field,
row, outcome, policy decision, or generated construction.

## Options Considered

1. **Continue optimizing covered storage execution:** rejected for this defect.
   Current execution is about 1.3 ms and no longer owns the required 1.7--2.0
   ms cloud reduction.
2. **Compress the Protobuf response:** rejected. Compression is data-dependent,
   adds CPU and amplification limits, and does not remove 2,700 message/value
   constructions.
3. **Invent a typed per-query Protobuf message:** rejected. It would generate
   transport service schemas per application and multiply public descriptors;
   the existing canonical cell codec already supplies strict type carriage.
4. **Reuse projected `PackedColumn` for compiler-bound named results:**
   proposed because its mechanics already show the required gain and its
   codec/bounds are production-proven.

## Consequences

- Large bounded generated pages can cross the wire as a few contiguous column
  buffers rather than thousands of nested generic messages.
- Public Protobuf, strict preflight, generated clients, driver host, CLI/MCP
  compatibility conversion, session envelopes, and golden fixtures gain one
  additive negotiated arm.
- `COMPACT_V1` and legacy records remain supported and byte-compatible.
- The result remains materialized as one bounded page; this is not streaming,
  compression, columnar query execution, or a public physical-layout choice.

## Compatibility

The change is additive to the public application protocol and generated SDK
runtime. Old clients never advertise `PACKED_V1`; old servers ignore its future
enum value only when it is not sent to them. New clients accept legacy and
compact responses from old servers. New servers select only an advertised arm.

No contract, query language, query IR, module, plan, cursor, capability,
command, entity, event, journal, changelog, backup, export, or storage format is
reinterpreted. Protocol descriptors, maximum-field pins, strict preflight,
fixtures, and public response charges advance atomically.

## Security

Packing has no authority. It operates after ordinary authorization over the
same bounded canonical values and exposes no caller-selected schema or codec.
Every structural inconsistency fails closed before typed release. Values and
offset-derived contents are redacted from errors and telemetry. Allocation and
decode work are charged before use, and an attacker cannot request columns,
rows, dictionaries, decompression, or fallback behavior outside the compiled
operation.

## Standing Design Tests

- **Interface safety:** applications still invoke only a finite generated
  named query with typed bounded parameters. They cannot select packing,
  layout, ordinals, field types, index/provider, validation, policy, snapshot,
  freshness, or fallback. Every arm proves the same exact operation identity
  and returns no partial result on drift.
- **Scale:** row, column, offset, cell, buffer, encoded-byte, allocation,
  decode, generated-object, wait, and diagnostic work are compiler/protocol
  bounded per request. No history-, database-, tenant-, or lifetime-sized
  state is introduced.

## Testing

- Strict Protobuf and canonical-cell boundary corpus: zero/maximum rows,
  zero/maximum-width cells, nulls, every value kind, malformed offsets,
  truncated/extra cells, duplicate fields, mixed arms, and over-budget parent
  messages.
- Cross-language golden corpus proving legacy/compact/packed typed equality for
  every generated result shape and optional/enum/decimal/UUID edge.
- Authorization, row/field policy, secret-output, revocation, read-after-
  commit, cursor, cancellation, and malformed-server adversarial tests.
- Unary and bounded-session protocol parity plus old-client/new-server and
  new-client/old-server compatibility.
- Paired workstation, N1, and E2 50/200/450 mechanics and the complete WP-623
  no-regression matrix before selection.

## Requirements and Work Packages

- **Requirements:** API-001, QRY-001, QRY-002, QRY-003, QRY-004, QRY-006,
  QRY-007, QRY-008, QRY-009, PERF-002, PERF-008, and PERF-018.
- **Implementation:** a new work package registered after exact acceptance.
- **Final evidence:** WP-623 and WP-579.

## Decision Deadline

Human exact-text acceptance is required before any production Protobuf,
generated-client, or selected-encoding change. A benchmark-only mechanics port
may be used to falsify the arithmetic, but it cannot be shipped, selected, or
used as release evidence before acceptance.
