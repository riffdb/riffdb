# ADR-0016: Canonical Key Components and Partition Identity

- **Status:** Accepted
- **Direction approved:** 2026-07-13
- **Exact text accepted:** 2026-07-13
- **Amends:** ADR-0011 typed-key envelope and hash-domain registries
- **Decision deadline:** Before WP-040 key schemas or plan fixtures merge

## Context

ADR-0011 fixes entity and conflict key envelopes but deliberately delegates the
remaining component schema to the compiler. Grammar version 1 permits every
transactional value type in a field or expression, while entity keys, aggregate
partition expressions, and conflict-key tuples require a smaller, exact set of
canonical component encodings. Without that registry, equal logical keys could
compile to different bytes, and WP-040 could not freeze key layouts or prove
cross-partition rejection.

The specification also illustrates a transaction partition as `Vec<u8>`, even
though component boundaries require domain newtypes and authorization,
provenance, tracing, and later routing all depend on an unambiguous logical
partition identity.

The human maintainer accepted this exact text on 2026-07-13.

## Proposed Decision

### Typed partition and index identity

`riffdb-types` owns bounded `PartitionKey` and `IndexEntryKey` newtypes. A
partition is an opaque canonical logical key distinct from `EntityKey`,
`ConflictKey`, and `TenantScope`; an index entry is a complete durable lookup
key, not an entity identity. The v1 typed-key envelope registry from ADR-0011 is
extended with:

| Key | Prefix | Required identity | Remaining components |
|---|---|---|---|
| Partition | `0x50 0x01` | `AggregateTypeId` as `u32` big endian | Exactly one compiled partition component |
| Index entry | `0x49 0x01` | `IndexId` as `u32` big endian | Compiled index components, `u32` entity-key length, complete `EntityKey` bytes |

The new purpose bytes are ASCII `P` for partition and ASCII `I` for index; the
second byte is key format version 1. Complete keys remain subject to ADR-0011's
4 KiB durable-key bound and checked envelope reconstruction. `PartitionKey`
replaces illustrative raw partition byte vectors at Rust component boundaries.
Tenant scope and logical partition identity remain separate inputs to
authorization and idempotency identity. A partition is aggregate-scoped: equal
component bytes under two different `AggregateTypeId` values are different
logical partitions.

The ADR-0011 unkeyed hash-domain registry is also extended with
`riffdb.partition-key/v1`, owned by `PartitionKeyHash`. It uses the unchanged
ADR-0011 SHA-256 frame over the complete canonical `PartitionKey` bytes. Raw
partition keys are redacted by default; tracing, metrics, and provenance fields
that do not require reversible identity use the typed hash.

### Closed v1 component registry

`riffdb-contract-ir` owns a checked `KeySchema` that records the purpose,
required stable owner ID, ordered component types, declared bounds, and key
codec version. Component bytes do not carry redundant type tags: the typed
envelope identifies the purpose and owner, and checked decoding requires the
exact validated compiler-produced schema as required by ADR-0011.

The immutable v1 component registry is:

| Grammar-v1 scalar | Component payload |
|---|---|
| `bool` | one byte, exactly `0x00` or `0x01` |
| `i64` | big endian after flipping the high sign bit |
| `u64` | unsigned big endian |
| `timestamp` | sign-bit-flipped big-endian Unix seconds, then big-endian `u32` nanoseconds |
| `date` | sign-bit-flipped big-endian `i32` days since the Unix epoch |
| `uuid` | 16 network-order bytes |
| declared enum | `EnumVariantId` as `u32` big endian |
| `string<N>` | `u32` big-endian UTF-8 byte length, then exact UTF-8 bytes |
| `bytes<N>` | `u32` big-endian byte length, then exact bytes |

The enum's `EnumTypeId` is part of the compiled component schema and is not
repeated in each key. Strings receive no case folding, Unicode normalization, or
locale processing. Variable-length components are length delimited even when
they are last. Timestamp nanoseconds must be in `0..=999_999_999`; Boolean and
all other value constraints are checked before encoding.

`decimal<P,S>`, `money<CURRENCY>`, `optional<T>`, `list<T,N>`, and record values
are not legal v1 entity-key, partition-key, or conflict-key components. The
compiler rejects them with stable diagnostics. There are no implicit casts,
stringification, hashing of oversized components, null sentinels, or alternate
encodings. Supporting another component type is a key-format compatibility
decision, not a compiler convenience.

### Schema construction and validation

Entity primary-key components occur in declared key-field order. An aggregate
partition schema has exactly the one scalar type produced by its
`partition_by` expression. Aggregate conflict components occur in declared
`conflict_key` expression order. Aggregate expressions are type-checked in the
root-entity field context; command plans contain typed derivations that use
validated inputs and deterministic constants only before any entity read.

Grammar v1 has no explicit child-to-root join mapping, so v1 aggregate ownership
uses one closed convention. Every aggregate has exactly one root. Every entity
used by a compiled command binding belongs to exactly one aggregate; an entity
cannot belong to multiple aggregates or be both one aggregate's root and child.
An entity outside an aggregate remains available to generic bounded read APIs but
cannot be bound by a grammar-v1 command. Every child's primary key must begin
with the root's complete primary-key schema in the same field-name, field-type,
and field-order sequence; its remaining key fields are child-local. Partition
and conflict expressions may reference only root primary-key fields and
deterministic constants. A command binding instantiates those references from
the corresponding root-key prefix of its validated entity-key expressions. A
child that does not have the exact prefix, an aggregate expression that reads a
non-key field, a bound entity without one owner, or ambiguous ownership fails
compilation.

Every command has at least one entity binding, and all of its read, mutate, and
create bindings belong to one `AggregateTypeId`. Their instantiated partition
derivations must be structurally identical after name resolution and lowering.
A mutating command has at least one mutate/create binding; `set` and `emit`
without a mutable binding are rejected, so there is no event-only command with
an unowned partition. Each mutable binding instantiates the one aggregate's
conflict derivation from the same validated binding-key inputs. A violation fails
`DSL-012` at compilation. Runtime value comparison is not used to rescue a
command whose single-partition locality cannot be proved statically.

At compilation, the maximum encoded size is computed with checked arithmetic
from the six-byte envelope and every fixed or declared variable component bound.
Any schema whose maximum can exceed 4,096 bytes is rejected even if one observed
value would fit. Runtime builders enforce the same complete-key bound. Empty
entity or conflict component lists are invalid under grammar v1, and a partition
schema never has zero or multiple components.

Checked decoding validates the envelope purpose/version/owner, exact component
count, fixed sizes, variable lengths, UTF-8, Boolean and timestamp ranges,
declared string/byte bounds, and an enum variant present in the exact validated
`KeySchema` and contract bundle used for decoding. It does not consult the
currently active catalog when decoding a historical key. Full input consumption
and the complete 4 KiB bound are mandatory. Validation returns no partially
validated key. Index-entry validation additionally checks the explicit entity-key
length, owning entity type/schema, nested entity envelope, and absence of bytes
after that entity key.
The byte order is the canonical total order for capability acquisition and
storage lookup; application code must not substitute display strings or
`CanonicalValue` documents as keys.

`riffdb-types` owns purpose-specific builders and opaque key/hash newtypes.
`riffdb-contract-ir` owns semantic key schemas and the checked schema-directed
encoder/decoder over validated `CanonicalValue`s. The compiler constructs
schemas; runtime, storage, conflict, authorization, and API crates consume only
validated keys or schema-directed derivation plans.

An entity's declared local index records its `IndexId`, owning `EntityTypeId`,
and selected `FieldId` components in source order and reuses this closed scalar
component registry and payload codec. Index declarations select fields rather
than arbitrary expressions. Deferred component types fail compilation, and an
exact-prefix query boundary may end only after a complete component.

A complete `IndexEntryKey` is exactly prefix `0x49 0x01`, `IndexId` as `u32`
big endian, every selected component payload, the complete entity-key byte length
as `u32` big endian, and the complete canonical `EntityKey` bytes. The entity key
must have the index owner's `EntityTypeId` and pass that exact entity key schema.
The compiler uses checked arithmetic to require:

```text
6 + maximum_index_component_payload
  + 4 + maximum_complete_entity_key <= 4,096
```

An index declaration that cannot satisfy the complete durable-key bound fails
WP-040 compilation; a later storage package cannot repair it by truncating or
hashing values. A scan prefix is the six-byte index envelope followed by zero or
more complete leading component payloads. It is a validated transient range
boundary, not a complete `IndexEntryKey`; partial components and prefixes that
include only part of a variable-length payload are invalid. This layout preserves
exact-component prefix scans while making the following entity-key boundary
unambiguous from the compiled schema and explicit length.

Projection result storage is not assigned an envelope by this ADR. WP-040
records typed projection grouping expressions in canonical plan IR; WP-170 must
accept a separate projection-key durable-format decision before persisting
derived keys. It must not reuse an entity, partition, or conflict envelope.

## Options Considered

1. **Closed scalar registry plus typed partition envelope:** Proposed. It freezes
   equality and ordering while keeping the POC codec small.
2. **Canonical-value documents as components:** Rejected. Per-value format/type
   tags create a different key format and do not match the accepted key builder.
3. **Hash every component:** Rejected. It loses useful ordering, introduces
   collision semantics, and prevents exact schema-directed reconstruction.
4. **Permit decimal and money immediately:** Deferred. An order-preserving
   signed-`i128` representation is feasible, but the POC scenario does not need
   these key types and accepting it would enlarge a durable boundary.
5. **Keep partition identity as raw bytes:** Rejected. It permits accidental
   substitution with unrelated byte strings at authorization and commit
   boundaries.

## Consequences

- A focused WP-010 interface follow-up adds `PartitionKey`,
  `PartitionKeyBuilder`, `PartitionKeyHash`, `IndexEntryKey`,
  `IndexEntryKeyBuilder`, their envelope constants, the partition typed hash
  function, builder methods needed by the closed registry, and
  golden/cross-domain fixtures.
- WP-040 can publish exact key schemas and deterministic derivation plans without
  owning foundational byte containers or hash primitives.
- Contracts using a deferred key component type fail compilation rather than
  receiving an unstable encoding.
- Adding or changing a component, envelope, order, normalization rule, or bound
  requires an accepted versioned compatibility decision.

## Compatibility and Security

The partition/index prefixes, partition hash label, component registry,
component and composite order, bounds, and validation rules are public/durable
compatibility boundaries. Lengths and maximum encoded sizes are checked before
allocation. Diagnostics, debug output, tracing, and metrics redact raw logical
keys. Hashes are content identifiers, not authorization proofs.

## Testing

- Golden entity, partition, and conflict bytes at every scalar boundary.
- Lexicographic-order properties for Boolean, signed/unsigned integers, date,
  timestamp, UUID, and enum variant IDs.
- Round trips through every exact compiled schema, including multiple
  variable-length components.
- Index-entry vectors and boundary tests that include the complete entity key and
  reject a composite maximum of 4,097 bytes.
- Stable compiler diagnostics for every deferred type and a maximum-size schema.
- Malformed decoding properties for wrong purpose/version/owner, truncation,
  invalid lengths, UTF-8, Boolean, timestamp, enum, trailing bytes, and key size.
- Hash-domain uniqueness and identical-payload cross-domain inequality.
- Canonical LegalSpend vectors for organization partition and annual-budget
  conflict identity.

## Requirements and Work Packages

- **Requirements:** `ID-003`, `ID-004`, `DSL-003`, `DSL-004`, `DSL-012`,
  `TXN-001`, `TXN-022`, `STO-010`
- **Defines or blocks:** WP-010 interface follow-up; `WP-040`; `WP-060`; `WP-090`
- **Final evidence:** `WP-140`, `WP-190`, `WP-200`

## Decision Deadline

The exact text must be accepted before `PartitionKey`, `IndexEntryKey`, the new
envelopes/hash domain, compiled key schemas, or key-schema fixtures merge.
ADR-0013 must cite this decision before WP-040 implementation starts.
