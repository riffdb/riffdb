# ADR-0006: Versioned Protobuf and Durable Envelope

- **Status:** Accepted
- **Direction approved:** 2026-07-12
- **Exact text accepted:** 2026-07-12, amended 2026-07-12, 2026-07-13,
  2026-07-20, and 2026-07-22
- **Maintainer-accepted gRPC error-carriage clarification:** 2026-07-20, carry
  exact bounded `riffdb.v1.PublicError` bytes directly in
  `grpc-status-details-bin`, derive the status from `ErrorClass`, expose only the
  static safe message, and fail closed on absent or inconsistent detail
- **Amended by:** ADR-0004, ADR-0007, ADR-0009, ADR-0012, ADR-0017, ADR-0039,
  and ADR-0040
  for the formal post-semantic schema phases and reviewed additive public symbols
- **Decision deadline:** Before WP-020 schemas or fixtures merge

The human maintainer accepted this exact text, including the dependency
disclosure amendment, on 2026-07-12. The human maintainer accepted the companion
schema-ownership and additive-symbol amendments below on 2026-07-13.
The human maintainer accepted the exact gRPC public-error carriage clarification
below on 2026-07-20.

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

For every API-neutral `PublicError`, WP-130 encodes the exact checked
`riffdb.v1.PublicError` message, bounded by `MAX_PUBLIC_ERROR_BYTES` (16 KiB), and
passes those bytes directly to Tonic `Status::with_details`. Those bytes are the
entire `grpc-status-details-bin` value: RiffDB does not wrap them in
`google.rpc.Status`, `Any`, or a second custom envelope. The canonical gRPC status
is derived only from the decoded `ErrorClass`: `InvalidArgument` maps to
`INVALID_ARGUMENT`, `Conflict` to `ALREADY_EXISTS`, `PermissionDenied` to
`PERMISSION_DENIED`, `DeadlineExceeded` to `DEADLINE_EXCEEDED`,
`FailedPrecondition` to `FAILED_PRECONDITION`, `Unavailable` to `UNAVAILABLE`,
`Uncertain` to `UNKNOWN`, and `Internal` to `INTERNAL`. `grpc-message` is exactly
the public error's registry-owned static safe message; it never carries an
internal source, arbitrary diagnostic, incident narrative, or serialized error.

The Rust transport client accepts a non-OK API-neutral response as a
`PublicError` only when details are present, at most 16 KiB, structurally and
semantically valid under the checked `riffdb.v1.PublicError` decoder, and
consistent with both the canonical gRPC status and static safe message. Missing,
malformed, oversized, unknown, or status/message-inconsistent details fail closed
as a typed protocol failure; the client does not reconstruct an error or retry
instruction from `grpc-message` alone. The pre-authentication
`UNAUTHENTICATED` framing exception remains a distinct closed transport failure
before an API-neutral `PublicError` exists and carries no fabricated public-error
detail.

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

There is no native C/C++, native linker invocation, or native-code build script
in this baseline. The generator-only `prettyplease` dependency is pure Rust but
declares a metadata-only `build.rs` and a synthetic Cargo
`links = "prettyplease02"` key; it emits Rust compiler configuration and its own
version metadata, not a native library or linker directive. The human maintainer
reviewed and approved this exception on 2026-07-12.

Prost and Bytes contain upstream unsafe optimizations; generator-only
dependencies include upstream unsafe in their parsing, temporary-file, OS, and
formatting graph. The human maintainer approved those transitive unsafe surfaces
on 2026-07-12. All first-party crates remain `#![forbid(unsafe_code)]`.
`libfuzzer-sys` is deferred because it introduces native LLVM/libFuzzer code and
an additional NCSA license decision; malformed-input Proptest coverage is
required in WP-020 meanwhile.

### 2026-07-13 companion amendments

WP-020 remains the completed phase-zero owner of package/version rules, common
exact values/errors, five services and 16 RPC names, unsupported shells,
generation, descriptors, fixtures, and `StoredEnvelope`. It is not reopened with
records or public fields that depend on later semantic packages.

Formal WP-065 depends on WP-020 and WP-060 and is the sole package that adds the
reviewed `riffdb.storage.v1` semantic-record messages required by accepted
ADR-0004, ADR-0007, ADR-0009, ADR-0012, and ADR-0017. It owns descriptors,
schema hashes, golden bytes, bounds, historical registrations, wire-structural
validation, and checked semantic mappings in the narrowly scoped storage-owned
`proto_codec`. `riffdb-proto` never depends on `riffdb-storage-api`; WP-070 may
persist no record before its WP-065 schema/mapping is reviewed.

The focused WP-010 follow-up owns one narrow exception to the phase sequence: it
adds the exact ADR-0012 `CommandExecutionFailed` kind, detail field, closed code,
domain mapper, preflight, descriptor, and golden fixtures already specified
below. This keeps the closed domain and wire error registries exhaustive and the
workspace compilable. It may add no request, result, service, RPC, capability,
projection, or durable-record field.

Formal WP-127 depends on WP-020 and WP-120 and completes every remaining public
`riffdb.v1` messages only after API-neutral service DTOs stabilize. It owns
descriptors, schema hashes, goldens, bounds, and wire-structural validation, but
does not depend on `riffdb-service` and implements no semantic conversion.
WP-130 depends on WP-127 and owns total service-to-wire conversion. These phases
preserve the one proto owner without a back-edge or speculative field numbers.

The supported Execute completion enum appends `EXECUTED_READ_ONLY = 3`.
Only for that status, zero `commit_sequence` and empty provenance URI/durability
mode are documented wire sentinels that adapters convert to semantic absence.
Committed/replayed results still require their nonzero sequence and complete
terminal fields; every inconsistent combination rejects.

The focused WP-010 follow-up additively appends ADR-0012's
`PublicErrorKind::CommandExecutionFailed` at wire
value `9`, stable code `command_execution_failed`, static message
`command execution failed`, failed-precondition class, and existing
`CONTACT_OPERATOR` recovery. Required detail field
`PublicError.execution_failure = 8` contains
`CommandExecutionFailureDetails { code = 1 }`, whose closed enum is unspecified
`0`, arithmetic fault `1`, and resource limit `2`; zero is invalid when decoding
the required semantic detail. No existing kind, detail field, or code is
renumbered.

ADR-0009 appends the closed `CreateCapabilityRequest` mode values unspecified
`0`, normal `1`, and bootstrap `2` and the closed service result variants; raw
bootstrap token bytes remain gRPC binary metadata, never a request field.
WP-127 preserves that compatible error slice and assigns and freezes all
remaining reviewed public fields before WP-130.
ADR-0017 durable projection records likewise belong to WP-065 and public
projection query/status/frontier/lifecycle/page shapes to WP-127.

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
8. **Wrap the public error in `google.rpc.Status`/`Any`:** Rejected because it
   adds a second type registry and envelope without adding RiffDB semantics.
9. **Use only gRPC status and message:** Rejected because it discards structured
   recovery/detail fields and would make an untrusted text field semantic.
10. **Carry a second custom error envelope:** Rejected because the versioned,
    bounded `riffdb.v1.PublicError` is already the complete public boundary.

## Consequences

- WP-020 needs a reviewed initial inventory but does not invent later record fields.
- Public wire errors must preserve the approved kind/detail invariants and bounds.
- Phase-zero RPC shells reserve names without claiming an implemented API.
- Later schema changes require coordinated interface PRs and compatibility review.
- Adapters convert between proto values and `riffdb-types`; proto types do not
  become API-neutral service DTOs.
- Decode/re-encode behavior must be tested anywhere unknown-field preservation is
  promised.
- WP-130 owns the exact `Status::with_details` server conversion and inverse
  client validation; no transport package may introduce another public-error
  envelope or infer semantics from `grpc-message` alone.
- WP-020 provides policy and refusal evidence for `STO-022`; restartable,
  idempotent migration evidence belongs to the first production durable-record
  migration and final recovery testing.

## Compatibility

Package names, service/RPC/message names, streaming shapes, supported-operation
field numbers, enum numbers, descriptor sets, exact value semantics, error
mappings, envelope framing/checksum/hash rules, and durable golden bytes are
explicit compatibility boundaries.

For supported gRPC operations, direct `riffdb.v1.PublicError` detail bytes, the
16 KiB ceiling, `ErrorClass`-to-status mapping, and exact static safe
`grpc-message` are also compatibility boundaries. Adding a wrapper is a protocol
change, not an implementation detail.

## Security

All decoders enforce size, nesting, collection, and diagnostic bounds. Public
errors contain safe fields only. Malformed, unknown, or oversized values fail
closed without panics or resource amplification.
Server and client tests must also prove that arbitrary internal text cannot enter
`grpc-message` or details, and that invalid details cannot induce a retry or a
typed `PublicError` on the client.

## Testing

Golden baseline descriptors and records, reserved-field checks, cross-language
exact-value vectors, deterministic regeneration, malformed/proptest decoder
coverage, nesting/size limits, unknown-version/type/hash tests, checksum
corruption, and public error redaction. Actual libFuzzer targets remain required
before POC exit after their dependency/license review.

WP-130 conformance fixtures additionally cover every `ErrorClass` mapping, exact
detail bytes, the 16 KiB boundary, static safe messages, and missing, malformed,
oversized, unknown, status-inconsistent, and message-inconsistent details. They
also prove no `google.rpc.Status`, `Any`, or second custom envelope is emitted or
accepted.

## Requirements and Work Packages

- **Requirements:** `API-001`, `VAL-001` through `VAL-003`, `STO-002`, `STO-020`
  through `STO-022`, `ID-004`, `ENT-002`, `ENT-003`
- **Defines or blocks:** `WP-020`; focused `WP-010` execution-failure error-schema
  follow-up; formal durable-schema `WP-065`; formal public-schema `WP-127`;
  consumers `WP-050`, `WP-070`, `WP-100`, `WP-110`, `WP-130`, `WP-160`, and
  `WP-170`
- **Final evidence:** `WP-190`, `WP-200`

## 2026-07-22 additive registry amendments

ADR-0039 adds only `proto/riffdb/storage/v1/index_v2.proto`, importing the
unchanged `application.proto` and defining only `StoredIndexEntryV2` fields 1
through 4. The nine previously accepted durable source files and all 26
previously accepted tuple fixtures remain byte-identical. The durable source
inventory is now ten files. During migration, the readable registry is exactly
27 tuples; the writable role registry is exactly 26 roles, consisting of the 25
unchanged non-index roles plus V2. V1 remains readable but has no current encoder
or normal-write role.

ADR-0040 makes an additive public-v1 change from five services and 16 RPCs to
the same five services and exactly 22 RPCs. It adds `GetContractVersion`,
`DiscoverCommandTools`, and `DiscoverResources` to `ContractService`,
`GetProjectionStatus` to `QueryService`, `TraceProvenance` to `CommitService`,
and `ListPendingOutboxDeliveries` to `AdminService`; `CommandService` remains
unchanged. It creates `proto/riffdb/v1/discovery.proto`, adds optional
`ExecuteCommandResponse.outcome_uri = 9`, and adds optional
`GetOutcomeRequest.outcome_uri = 5` with the accepted exclusive legacy-selector
rule. Existing fields, methods, durable schemas, and fixtures remain unchanged.
WP-137 is the sole owner of these generated public additions; its separately
named exact operation-schema byte checkpoint remains mandatory.

The detailed message, tag, presence, import, and validation registries in
ADR-0039 and ADR-0040 are authoritative. This section does not reopen the
compatibility policy for any other source or symbol.

## Decision Deadline

The initial decision deadline was satisfied by exact-text acceptance on
2026-07-12. Later records require review under this policy, not a new ownership
model. The gRPC carriage decision deadline was satisfied by maintainer acceptance
on 2026-07-20 before WP-130 conversion and conformance implementation.
