# ADR-0011: Canonical Values, Fixed-Scale Decimals, Keys, and Hashing

- **Status:** Accepted
- **Direction approved:** 2026-07-12
- **Exact text accepted:** 2026-07-12, amended 2026-07-12, 2026-07-13, and 2026-07-14
- **Amended by:** ADR-0014 for projection/root plan hashes, ADR-0016 for
  partition/index keys and partition hashing, ADR-0009 for capability-token
  keyed hashing, ADR-0005 for the versioned idempotency key, ADR-0017 for
  projection keys and apply hashing, and ADR-0018 for UUIDv7 assembly and
  production-source ownership; ADR-0051 adds the typed
  `riffdb.query-plan/v1` unkeyed domain; ADR-0091 adds canonical value tag
  `0x0e` (vector) per the amendment section at the end of this document
- **Decision deadline:** Before WP-010 semantic types or fixtures merge

The human maintainer accepted this exact text, including the foundational
identifier and key-envelope amendment, on 2026-07-12, and accepted the companion
registry and UUIDv7 boundary additions below on 2026-07-13.
The maintainer accepted the exact durable-event hash preimage below on
2026-07-14.

## Context

Values cross source, IR, runtime, storage, hashing, Protobuf, JSON Schema, gRPC,
MCP, and SDK boundaries. Rust layout, map iteration, floating point, locale, or
implicit Unicode normalization would make hashes and durable keys platform- or
process-dependent.

## Decision

`riffdb-types` owns one closed canonical value algebra for the POC. Business
decimal types use precision `P` in `1..=38`, scale `S` in `0..=P`, and a signed
`i128` coefficient whose absolute value is less than `10^P`. Construction and
arithmetic are checked. Scale changes are explicit and lossless or fail; there is
no floating point or negative-zero representation. Money uses the same decimal
rules plus exactly three uppercase ASCII currency characters. Integer signedness
and width, UUID, date, timestamp, Boolean, bounded text/bytes, bounded list, and
record types remain distinct.

### Canonical value encoding v1

Every encoded value begins with format byte `0x01` and one type tag. The initial
tag registry is immutable:

| Tag | Value |
|---|---|
| `0x00` | null |
| `0x01` | Boolean |
| `0x02` | signed 64-bit integer |
| `0x03` | unsigned 64-bit integer |
| `0x04` | decimal |
| `0x05` | money |
| `0x06` | UTF-8 string |
| `0x07` | bytes |
| `0x08` | timestamp |
| `0x09` | date |
| `0x0a` | UUID |
| `0x0b` | enum |
| `0x0c` | list |
| `0x0d` | record |

Fixed-width integers use big-endian bytes; signed value payloads use two's
complement. Boolean payload is exactly `0x00` or `0x01`. Decimal payload is
precision `u8`, scale `u8`, then the coefficient as 16-byte big-endian two's
complement. Money payload is three currency bytes followed by the decimal
payload. String and byte payloads use a `u32` big-endian byte length. Timestamp
payload is signed `i64` Unix seconds plus `u32` nanoseconds in
`0..=999_999_999`; date is signed `i32` days since the Unix epoch; UUID is 16
network-order bytes. Enum payload is stable type ID then stable variant ID, both
`u32`; display names are not canonical bytes. List payload is `u32` count then
each canonical value. Record payload is `u32` field count followed by stable
`FieldId` and canonical value pairs in strictly increasing field-ID order.
Duplicate record fields and noncanonical ordering are rejected.

All lengths, counts, and nesting are validated before allocation against the
compiled type and process hard limits. A v1 canonical document, individual
string, or individual byte value is at most 1 MiB; a list or record has at most
65,535 entries; nesting depth is at most 32. Optional absence is the null value
and is legal only under an optional type. Maps, sets, and unbounded collections
are not v1 transactional value variants. Text is exact UTF-8 with grammar- or
field-specific validation. No locale rule or implicit Unicode normalization
changes identity.

### Foundational identifiers

Compiler-assigned aggregate and invariant IDs are distinct `u32` newtypes.
Index epochs and the administration sequence are distinct `u64` newtypes and
must never be substituted for an application `CommitSequence`.

`DatabaseId`, `CapabilityId`, and `ProvenanceId` are UUIDv7 values in 16-byte
network order. `EventId` is the deterministic pair of the event's
`CommitSequence` and zero-based `u32` event ordinal; its canonical bytes are the
sequence as `u64` big endian followed by the ordinal as `u32` big endian.

ADR-0018 leaves that representation unchanged and assigns `riffdb-types` only
pure RFC 9562 UUIDv7 value semantics: checked assembly from an explicit 48-bit
Unix-millisecond field and explicit ten-byte random source input, with six high
source bits masked so 74 random bits enter the UUID, plus checked network-order
byte validation and access. The value layer sets and checks the UUIDv7 version
and RFC variant bits. It owns no ambient clock, entropy provider, persistent
generator state, monotonicity policy, collision registry, or production
identifier source. UUID byte ordering provides only the UUIDv7 layout's coarse
source-time ordering; it is not logical time, commit order, causality,
uniqueness evidence, or an authorization/audit timestamp.

Identifier generation is orchestration metadata outside deterministic command
execution. Under ADR-0018, production source implementations are limited to
`riffdb-auth` for bootstrap `CapabilityId` generation and its separately
approved token entropy; `riffdb-client-rust` for outer `RequestId` and client-
convenience `CapabilityId`/`AgentSessionId` generation; and `riffdb-server` for
new-database `DatabaseId` candidates, injected `ProvenanceIdSource`, hosted-MCP
`RequestId`, injected `IncidentIdSource`, and existing cursor IDs. Consumer-owned
source ports remain with their semantic consumers. A source clock, entropy
payload, provider, or ordering state for a `DatabaseId`, `RequestId`,
`CapabilityId`, or `ProvenanceId` never enters `riffdb-runtime` and must never be
repurposed as command randomness. ADR-0012's already validated `RequestId`
context value remains opaque; its UUID fields have no command-time, entropy, or
ordering semantics.

Contract lineage is the exact, nonempty UTF-8 contract name bounded to 256 bytes.
An environment is a nonempty ASCII slug bounded to 64 bytes and containing only
ASCII letters, digits, `.`, `_`, or `-`. A POC tenant scope is either the global
scope or one exact, nonempty UTF-8 tenant identifier bounded to 256 bytes. These
values are not implicitly normalized. Tenant identifiers and scopes use
redacted default diagnostics.

### Canonical durable keys

Purpose-specific durable key encoders prepend their namespace and format version.
Unsigned ordered components use fixed-width big-endian bytes. Signed ordered
components flip the sign bit before big-endian encoding so lexicographic and
numeric order agree. Variable byte components use a `u32` length followed by
exact bytes. A v1 durable key is at most 4 KiB. Each key schema fixes its
component order; Rust memory layout, serde defaults, insertion order, and
randomized hashing never determine bytes.

The v1 typed-key envelope registry is immutable:

| Key | Prefix | Required identity | Remaining components |
|---|---|---|---|
| Entity | `0x45 0x01` | `EntityTypeId` as `u32` big endian | Compiled primary-key schema |
| Conflict | `0x43 0x01` | `AggregateTypeId` as `u32` big endian | Compiled conflict-key schema |

Typed builders require the identity when they are created. Reconstruction from
raw bytes validates the purpose byte, version byte, required identity, minimum
envelope length, and 4 KiB bound before constructing the type. Validation of the
remaining component count and types requires the exact compiler-produced key
schema and is performed by that semantic decoder; a key envelope alone must not
claim that schema validation occurred.

### Hash framing and domains

The v1 unkeyed frame is ASCII `RIFFDB-HASH`, byte `0x00`, scheme byte `0x01`,
domain length as `u16` big endian, the ASCII domain, payload length as `u64` big
endian, and the payload. Its digest is SHA-256. The v1 keyed frame replaces the
prefix with ASCII `RIFFDB-HMAC`; its digest is HMAC-SHA-256 under the selected
versioned key. These are content and lookup digests, not signatures.

The initial central domain registry is:

| Domain | Mode |
|---|---|
| `riffdb.canonical-value/v1` | SHA-256 |
| `riffdb.source/v1` | SHA-256 |
| `riffdb.contract-bundle/v1` | SHA-256 |
| `riffdb.plan/v1` | SHA-256 |
| `riffdb.command-input/v1` | SHA-256 |
| `riffdb.event/v1` | SHA-256 |
| `riffdb.entity-key/v1` | SHA-256 |
| `riffdb.conflict-key/v1` | SHA-256 |
| `riffdb.schema/v1` | SHA-256 |
| `riffdb.idempotency-key/v1` | HMAC-SHA-256 |

New domains may be added only with one owning semantic boundary and collision
tests. Existing labels, framing, tags, or meanings cannot be reused. Secret and
capability lookup digests require their own accepted ADR before adding another
keyed domain. The custom Protobuf `riffdb.v1.Value` is an exact checked mapping
of this algebra; adapters must supply the compiled decimal type where a wire
value does not carry precision.

### Exact durable-event hash preimage

`EventHash` uses the existing v1 unkeyed frame and SHA-256 domain
`riffdb.event/v1`. The payload supplied to that domain frame is exactly:

```text
EventId canonical bytes
|| EventTypeId:u32_be
|| payload_length:u32_be
|| canonical Value::Record payload bytes
```

`EventId` is exactly 12 bytes: its nonzero `CommitSequence` as `u64` big endian
followed by the zero-based event ordinal as `u32` big endian. `EventTypeId` is
its nonzero numeric value as four big-endian bytes. `payload_length` is the exact
number of bytes in the following complete canonical value and must fit `u32`;
the accepted one-event value bound is tighter. The payload is required to be a
canonical `Value::Record`, including the `0x01` value-format byte, the `0x0d`
record tag, field count, and strictly ordered field/value encodings. It is not a
bare record body, source object, JSON, or deterministic-Protobuf encoding.

The unchanged outer frame is therefore:

```text
"RIFFDB-HASH" || 0x00 || 0x01
|| domain_length:u16_be || "riffdb.event/v1"
|| preimage_length:u64_be || preimage
```

No component may hash payload-only content, omit the event identity or type,
change a length width or byte order, or add the commit/bundle/provenance data to
this domain. `riffdb-types` owns the canonical component encodings and the
generic `riffdb.event/v1` domain-frame hashing primitive; it does not assemble an
event-specific preimage. `riffdb-storage-api` solely owns post-assignment
event-specific preimage construction, hash derivation, and semantic durable-event
constructor validation when building the exact record graph. WP-065 freezes the
exact wire mapping, codec, complete framed goldens, and boundary proofs. WP-070
recomputes the hash during durable decode, structural integrity, and reciprocal
commit/event/outbox validation. A mismatch is
corruption or an invariant violation according to whether it was read from
durable state or proposed before staging; it is never repaired.

### 2026-07-13 companion registry additions

ADR-0005 extends the typed key-envelope registry with idempotency identity
prefix `0x59 0x01`. Its exact payload is the accepted ADR-0005 identity tuple:
16-byte `DatabaseId`; length-framed environment; global/tenant tag and optional
length-framed tenant ID; length-framed `ActorId`; length-framed contract lineage;
`CommandId`; digest scheme; `DigestKeyId`; and 32 digest bytes. Its complete
accepted maximum is 908 bytes, and its checked builder still enforces this ADR's
4,096-byte complete-key ceiling before allocation. Prefix `0x59` is ASCII `Y`
and does not collide with another accepted purpose. ADR-0005 alone owns the
component meanings, order, bounds, and exact decode checks.

ADR-0009 extends the keyed registry with `CapabilityTokenDigest`, domain
`riffdb.capability-token/v1`, HMAC-SHA-256 under the unchanged keyed frame. Its
payload is exactly the decoded 32 raw token bytes; the canonical base64url text,
database, environment, audience, and policy values are not appended. The typed
digest carries scheme byte `0x01`, one nonzero `DigestKeyId` as `u32` big endian,
and exactly 32 HMAC bytes. It is not convertible to the idempotency digest or an
untyped byte array.

ADR-0017 extends the typed key-envelope registry with projection apply marker
`0x41 0x01`, projection frontier/control `0x46 0x01`, and projection group state
`0x47 0x01`. Their exact identity, generation, sequence, and length-framed
canonical group-component payloads are defined only by ADR-0017 and remain under
the 4 KiB key bound. They do not reuse the ADR-0016 ordered component codec.

ADR-0017 also extends the unkeyed registry with typed
`ProjectionApplyHash`, domain `riffdb.projection-apply/v1`, using the unchanged
SHA-256 frame over the exact checked storage-owned canonical apply request. The
hash is idempotency evidence, not a signature or authorization proof. Existing
prefixes, domains, frames, and meanings remain unchanged; the central collision
fixtures include every added entry.

The registry tables above remain the accepted initial baseline as amended by
these explicit additions. They must not be read as excluding the later accepted
domains or key envelopes named here and in ADR-0014/ADR-0016.

## Options Considered

1. **Checked i128 fixed-scale plus custom canonical bytes:** Approved POC choice.
2. **Arbitrary precision decimal:** More range but greater dependency and encoding
   surface than the POC requires.
3. **IEEE floating point:** Cannot provide exact decimal business semantics.
4. **Deterministic Protobuf as the only canonical encoding:** Couples domain
   identity to wire-generation behavior and does not alone define key ordering.
5. **Implicit NFC normalization:** Changes caller bytes and requires a broader
   Unicode/version policy.

## Consequences

- Arithmetic overflow, scale mismatch, and precision loss are typed failures.
- Conversion adapters must prove exact round trips or reject values.
- Changing any tag, ordering, bound, hash algorithm, or text rule is a versioned
  compatibility change.
- Changing any `EventHash` preimage component, width, order, canonical payload
  encoding, domain, or frame is a durable compatibility change.
- The Rust SHA-256/HMAC provider requires separate critical-dependency review and
  may not change canonical bytes or framing.
- The reviewed POC baseline is `sha2` 0.11.0 and `hmac` 0.13.0 with default
  features disabled. Both are Rust implementations with no native C dependency.
  `sha2` contains target-specific unsafe CPU intrinsics guarded by feature
  detection; the human maintainer approved that transitive unsafe surface on
  2026-07-12. All first-party crates remain `#![forbid(unsafe_code)]`.

## Compatibility

Identifier representations, value variants and tags, numeric bounds,
decimal/money encoding, text policy, field order, key envelopes and domains,
canonical bytes, hash algorithm/version, the exact durable-event hash preimage,
and Protobuf mapping are public and durable compatibility boundaries.

## Security

All lengths/nesting are bounded before allocation. Secret values are redacted
before diagnostics. Untrusted canonical bytes are decoded without panics.
Cryptographic hashes are content identifiers, not authorization or signatures.

## Testing

Cross-platform golden byte/hash/key vectors; decimal boundary, arithmetic, and
ordering properties; canonical collection permutation tests; exact Proto/JSON
round trips; malformed decoder fuzzing; maximum depth/length tests; and a central
domain-tag collision registry check. The central purpose registry includes the
ADR-0005 `0x59 0x01` idempotency key and checks its global, maximum 908-byte
tenant, malformed, and over-4,096-byte pre-allocation fixtures through WP-060.
Event-hash fixtures include the complete framed preimage, empty/nonempty record
payloads, field-order rejection, event ID/type/ordinal changes, payload-length
boundaries, cross-domain inequality, codec round trips, and durable mismatch
rejection through WP-060/WP-065/WP-070. WP-100 and WP-190 prove the assigned
sequence/ordinal graph and crash-recovery validation end to end.

## Requirements and Work Packages

- **Requirements:** `ID-001` through `ID-005`, `VAL-001` through `VAL-003`,
  `ENT-002`, `STO-002`, `TXN-042`
- **Defines or blocks:** `WP-010`, `WP-020`, `WP-040`, `WP-060`, formal
  durable-schema `WP-065`, `WP-070`, `WP-090`, `WP-100`, and formal
  public-schema `WP-127`
- **Final evidence:** `WP-190`, `WP-200`

## Decision Deadline

Exact acceptance, including the algorithm/domain registry and decimal bounds, is
required before WP-010 publishes types or golden fixtures.

## Amendment: canonical value tag `0x0e` (vector), recorded 2026-08-11

ADR-0091 (native vector search projections, as amended) introduced
`CanonicalValue::Vector` with canonical tag `0x0e`. The encoding landed in
WP-591 (05eab14) without the amendment this registry requires; this section
records it explicitly so the registry and the code agree. The WP-596
maintainer ruling recorded 2026-08-11 ratifies this amendment as written.

| Tag | Value |
|---|---|
| `0x0e` | vector |

Vector payload is a `u32` big-endian dimension in `1..=4096`, followed by
exactly `dimension` IEEE 754 binary32 components, each as 4 big-endian bytes.
This is the sanctioned narrow exception to the no-floating-point rule for
canonical values: vector components are opaque embedding coordinates, never
business decimals.

Canonical form is enforced at every construction path (constructor and
decoder): components MUST be finite (NaN and infinities are rejected as typed
errors) and negative zero is canonicalized to positive zero before any
equality, ordering, hashing, or encoding observes the value. Consequently
equal vectors have equal canonical bytes and equal durable digests, and NaN
payload bit patterns never reach durable bytes. The golden fixtures in
`crates/riffdb-types/tests/canonical_golden.rs` pin the `0x0e` row alongside
the initial registry.
