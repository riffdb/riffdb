# ADR-0006: Versioned Protobuf and Durable Envelope

- **Status:** Accepted
- **Direction approved:** 2026-07-12
- **Exact text accepted:** 2026-07-12
- **Decision deadline:** Before WP-020 schemas or fixtures merge

The human maintainer accepted this exact text on 2026-07-12.

## Context

Before the 2026-07-12 governance reconciliation, public gRPC sketches disagreed,
and `google.protobuf.Struct` cannot preserve all RiffDB integer and decimal values
exactly. Protobuf definitions are also needed before later semantic owners know
their complete records, so a one-time freeze in WP-020 would force guesses or
out-of-scope changes.

## Decision

SPEC Section 11 is the canonical v0.1 gRPC service inventory and naming surface:
its five services and descriptive RPC names are preserved, and Appendix B must be
reconciled to it. Public dynamic data uses a tagged `riffdb.v1.Value`, not
`google.protobuf.Struct`. The value supports exact signed and unsigned integers,
canonical fixed-scale decimal and money, UUID, date, timestamp, bounded list, and
record fields in canonical order. Floating-point values are not a business-value
fallback.

One proto owner maintains public and durable schemas. WP-020 freezes generation,
package/version rules, common exact values, error/envelope conventions, public
service names, bounds, descriptors, and compatibility fixtures. Semantic records
for contracts, catalog state, entities, commits, capabilities, outbox, projections,
and operational state are added through small proto-owner interface PRs only after
their Rust semantic owner stabilizes. No package duplicates a wire type to avoid
coordination.

The public package is `riffdb.v1`; the durable package is
`riffdb.storage.v1`. WP-020 freezes the five services and the authoritative 16
RPC names and streaming shapes in SPEC Section 11.2. `Value`, public error,
Execute, and stored-envelope messages are fully defined now. Every other named
RPC request/response is an empty phase-zero name-reservation shell with no
speculative field or reserved range. These shells are explicitly unsupported
and are not a supported client baseline. Their semantic owners add reviewed
fields through later proto-owner interface PRs before the operations are
implemented.

WP-020 produces checked-in Prost message types and Tonic-compatible service
descriptors. `riffdb-proto` contains no Tonic runtime or generated client/server
stub. Tonic generation and implementation remain owned by `riffdb-api-grpc` in
WP-130.

### Execute and exact values

The Section 11.3 field and enum numbers are immutable. Execute request field 3
is `optional uint64 expected_contract_version`; absence explicitly selects the
active contract and presence requires an exact match. Bearer credentials are
gRPC authorization metadata. The server constructs trusted principal and
capability context after authentication; v1 Execute contains no client-provided
actor context or provenance claims.

Decimal wire coefficients are 1 through 16 bytes of minimal big-endian two's
complement; zero is the single byte `0x00`. A removable leading `0x00` or `0xff`
sign-extension byte is rejected. Structural scale is at most 38. Conversion to
a canonical decimal or money value requires the compiled precision and scale.
Record names resolve through compiled schema to stable field IDs; an ID and name
supplied together must agree. Duplicate resolved fields reject and canonical
output is increasing by field ID. Enum names are display-only and, when present,
must match the compiled schema. Context-free helpers perform structural
validation only and never claim schema-canonical conversion.

Canonical documents, strings, and byte values retain the 1 MiB limits from
ADR-0011. Lists and records retain 65,535-entry and depth-32 limits. Protocol
names and durable record-type names are at most 256 bytes.

### Public errors

`PublicErrorKind` has zero `UNSPECIFIED`, followed by the eight Rust kinds in
their declared order at values 1 through 8. `RecoveryAction` has zero
`UNSPECIFIED`, followed by `CORRECT_REQUEST`, `RETRY`,
`RESOLVE_WITH_SAME_IDEMPOTENCY_KEY`, `OBTAIN_PERMISSION`, `REFRESH_CONTRACT`,
and `CONTACT_OPERATOR` at values 1 through 6. `ValidationCode` has zero
`UNSPECIFIED`, followed by the eight Rust validation codes in their declared
order at values 1 through 8.

`PublicError` fields are kind 1, stable code 2, static safe message 3, recovery
action 4, oneof validation details 5 or contract-mismatch details 6, and optional
incident UUID bytes 7. A validation issue has code 1 and repeated structured path
segments 2; a path segment oneof uses field ID 1 or zero-based list index 2.
Contract-mismatch details carry active contract version at field 1. Validation
has one through 16 issues and at most 16 path segments. Kind, code, safe message,
recovery, and details must exactly agree with `riffdb-errors`; arbitrary messages
and internal sources are forbidden.

Projection `WaitTimedOut` and `Degraded` states remain typed projection query
results. Authentication framing may return gRPC `UNAUTHENTICATED` before an
API-neutral `PublicError` exists. WP-130 maps validation to `INVALID_ARGUMENT`,
idempotency reuse to `ALREADY_EXISTS`, authorization denial to
`PERMISSION_DENIED`, concurrency deadline to `DEADLINE_EXCEEDED`, contract
mismatch to `FAILED_PRECONDITION`, storage unavailability to `UNAVAILABLE`,
outcome uncertainty to `UNKNOWN`, and internal defect to `INTERNAL`.

### Durable envelope

Durable payloads use explicit record/envelope versions and integrity metadata.
Field numbers are never reused, removed fields are reserved, decoders reject
unsupported versions safely, and the unknown-field retention policy is explicit
per persisted boundary. Pure-Rust deterministic generation and checked-in golden
descriptors/records are required.

`StoredEnvelope` preserves SPEC Section 10.3 fields 1 through 5 exactly.
Storage-format version 1 is the only accepted initial version. `record_type` is
the at-most-256-byte ASCII fully qualified Protobuf payload message name from a
closed registry, spelled without a leading dot, for example
`riffdb.storage.v1.MessageName`. `payload_crc32c` is CRC-32C/Castagnoli over the
exact payload bytes only. `schema_hash` is exactly the 32-byte ADR-0011
schema-domain digest.
The schema-domain payload is:

```text
u16_be record_type_length
+ record_type ASCII bytes
+ u64_be descriptor_set_length
+ canonical descriptor-set bytes
```

The canonical descriptor set is the source-info-stripped transitive descriptor
closure for that payload type. File names are the exact normalized,
forward-slash-separated `FileDescriptorProto.name` values and files are sorted
by those names. The absolute payload and encoded-envelope ceiling is 16 MiB;
semantic owners may set lower per-record limits.

There is no universal payload field named `record_format_version`, preserving
the SPEC `CommitRecord` field layout. The tuple of versioned package, storage
format version, fully qualified record type, and schema hash is the explicit
record version. Any incompatible payload revision uses a new registered type or
package and a migration. The registry tuple also supplies the semantic decoder
and record-specific bound.

Envelope bounds, checksum, registered type, schema hash, and version are checked
before semantic payload decoding. Unknown or ambiguous tuples return a typed
incompatibility; WP-070 turns that into controlled startup refusal. The outer
envelope and supported durable payload must decode and deterministically
re-encode to identical bytes. This rejects unknown fields, duplicate singular
fields, noncanonical ordering, and other alternate encodings at the durable
boundary.

Public message unknown fields are ignored and never relayed; no retention promise
is made. Durable unknown fields are never silently dropped: older software
refuses unregistered schema hashes/versions, and untouched raw payload bytes are
not decoded and rewritten. Only an explicit restartable, idempotent migration may
rewrite a historical payload. Business unknown entity fields are explicit
`ValueField` values retained by stable field ID, not unknown Protobuf wire fields.

### Generation and compatibility

Generation uses locked Protox and Prost libraries through a Cargo example invoked
by `scripts/generate-proto`; there is no repository `build.rs`, external or
vendored `protoc`, network access, or fourth shipped binary. Sorted sources
produce checked-in Rust messages, source-info-stripped descriptors, and fixtures.
`--check` regenerates into a temporary directory and byte-compares every artifact.

Public compatible changes append optional fields, messages, RPCs, or enum values
without changing prior semantics. Changing meaning, defaults, presence,
validation, status mapping, or streaming shape is semantic-breaking once an
operation is supported. Intentionally incompatible released public surfaces use
a new package major. Phase-zero empty shells are exempt because they are marked
unsupported.

Every durable schema change creates a new schema hash. Field/tag reuse is
forbidden and removed names/numbers are reserved. Readers accept only explicitly
registered historical hashes. A migration may register old and new hashes, then
retire the old hash only after restartable rewrite and verification.

Initial envelope compatibility uses an explicitly test-only
`CompatibilityProbe` schema outside the production record registry. WP-020 does
not invent entity, commit, pending, capability, outbox, or projection payloads.

### Reviewed dependencies

The reviewed WP-020 runtime baseline is Prost 0.14.4 with default features
disabled and `derive,std`, plus `crc` 3.4.0 with default features disabled using
`CRC_32_ISCSI`. Generator/test dependencies are `prost-types` 0.14.4,
`prost-build` 0.14.4 with only `format`, Protox 0.9.1 without its binary feature,
and the already approved Proptest 1.11.0 configuration. The checksum golden is
ASCII `123456789` to `0xe3069283`.

There is no native C/C++ or Cargo `links` crate in this baseline. Prost and Bytes
contain upstream unsafe optimizations; generator-only dependencies include
upstream unsafe in their parsing, temporary-file, OS, and formatting graph. The
human maintainer approved those transitive unsafe surfaces on 2026-07-12. All
first-party crates remain `#![forbid(unsafe_code)]`. `libfuzzer-sys` is deferred
because it introduces native LLVM/libFuzzer code and an additional NCSA license
decision; malformed-input Proptest coverage is required in WP-020 meanwhile.

## Options Considered

1. **Section 11 plus custom exact `Value` and phased ownership:** Approved model.
2. **Appendix B services:** Omits or renames parts of the canonical surface.
3. **`google.protobuf.Struct`:** Loses exact large-integer/decimal semantics.
4. **Freeze all future records in WP-020:** Forces premature durable design.
5. **Package-local proto ownership:** Risks duplicate and incompatible wire types.
6. **Tonic stubs in `riffdb-proto`:** Violates the Prost-only type boundary and
   preempts WP-130 transport ownership.
7. **External or vendored protoc:** Adds native/toolchain supply surface despite
   the approved pure-Rust Protox path.

## Consequences

- WP-020 needs a reviewed initial inventory but does not invent later record fields.
- Public wire errors must preserve the approved kind/detail invariants and bounds.
- Phase-zero RPC shells reserve names without claiming an implemented API.
- Later schema changes require coordinated interface PRs and compatibility review.
- Adapters convert between proto values and `riffdb-types`; proto types do not
  become API-neutral service DTOs.
- Decode/re-encode behavior must be tested anywhere unknown-field preservation is
  promised.
- The binary carriage of `PublicError` on non-OK gRPC responses remains a WP-130
  decision and requires human review before client/server conformance freezes.
- WP-020 provides policy and refusal evidence for `STO-022`; restartable,
  idempotent migration evidence belongs to the first production durable-record
  migration and final recovery testing.

## Compatibility

Package names, service/RPC/message names, streaming shapes, supported-operation
field numbers, enum numbers, descriptor sets, exact value semantics, error
mappings, envelope framing/checksum/hash rules, and durable golden bytes are
explicit compatibility boundaries.

## Security

All decoders enforce size, nesting, collection, and diagnostic bounds. Public
errors contain safe fields only. Malformed, unknown, or oversized values fail
closed without panics or resource amplification.

## Testing

Golden baseline descriptors and records, reserved-field checks, cross-language
exact-value vectors, deterministic regeneration, malformed/proptest decoder
coverage, nesting/size limits, unknown-version/type/hash tests, checksum
corruption, and public error redaction. Actual libFuzzer targets remain required
before POC exit after their dependency/license review.

## Requirements and Work Packages

- **Requirements:** `API-001`, `VAL-001` through `VAL-003`, `STO-002`, `STO-020`
  through `STO-022`, `ID-004`, `ENT-002`, `ENT-003`
- **Defines or blocks:** `WP-020`; later interface PRs for `WP-050`, `WP-070`,
  `WP-100`, `WP-110`, `WP-130`, `WP-160`, `WP-170`
- **Final evidence:** `WP-190`, `WP-200`

## Decision Deadline

The initial decision deadline was satisfied by exact-text acceptance on
2026-07-12. Later records require review under this policy, not a new ownership
model.
