# ADR-0011: Canonical Values, Fixed-Scale Decimals, Keys, and Hashing

- **Status:** Accepted
- **Direction approved:** 2026-07-12
- **Exact text accepted:** 2026-07-12
- **Decision deadline:** Before WP-010 semantic types or fixtures merge

The human maintainer accepted this exact text on 2026-07-12.

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

Purpose-specific durable key encoders prepend their namespace and format version.
Unsigned ordered components use fixed-width big-endian bytes. Signed ordered
components flip the sign bit before big-endian encoding so lexicographic and
numeric order agree. Variable byte components use a `u32` length followed by
exact bytes. A v1 durable key is at most 4 KiB. Each key schema fixes its
component order; Rust memory layout, serde defaults, insertion order, and
randomized hashing never determine bytes.

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
- The Rust SHA-256/HMAC provider requires separate critical-dependency review and
  may not change canonical bytes or framing.

## Compatibility

Value variants and tags, numeric bounds, decimal/money encoding, text policy,
field order, key domains, canonical bytes, hash algorithm/version, and Protobuf
mapping are public and durable compatibility boundaries.

## Security

All lengths/nesting are bounded before allocation. Secret values are redacted
before diagnostics. Untrusted canonical bytes are decoded without panics.
Cryptographic hashes are content identifiers, not authorization or signatures.

## Testing

Cross-platform golden byte/hash/key vectors; decimal boundary, arithmetic, and
ordering properties; canonical collection permutation tests; exact Proto/JSON
round trips; malformed decoder fuzzing; maximum depth/length tests; and a central
domain-tag collision registry check.

## Requirements and Work Packages

- **Requirements:** `ID-001` through `ID-005`, `VAL-001` through `VAL-003`,
  `ENT-002`, `STO-002`, `TXN-042`
- **Defines or blocks:** `WP-010`, `WP-020`, `WP-040`, `WP-060`, `WP-090`, `WP-100`
- **Final evidence:** `WP-190`, `WP-200`

## Decision Deadline

Exact acceptance, including the algorithm/domain registry and decimal bounds, is
required before WP-010 publishes types or golden fixtures.
