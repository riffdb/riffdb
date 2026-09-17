# ADR-0021: Service-Audit Target Registry and Canonical Ordering

- **Status:** Accepted
- **Direction approved:** 2026-07-13
- **Exact text accepted:** 2026-07-13
- **Accepted:** 2026-07-13
- **Requires:** ADR-0004, ADR-0007, ADR-0011, and ADR-0013
- **Amends:** ADR-0007 by freezing the exact `ServiceAuditTargetV1` variants,
  semantic tags, lineage scoping, canonical list order, and service construction
  rule
- **Decision deadline:** Before amended WP-010 closes

The human maintainer accepted the original exact nine-variant registry and
request-target construction rule on 2026-07-13. This record does not permit a
new storage or transport path.

The maintainer accepted the additive WP-417 amendment on 2026-08-02. It adds
one exact reactive event-consumer target without changing the bytes or meaning
of tags `0x01` through `0x09`. The maintainer separately approved the additive
durable-generation decision below on 2026-08-02 after the frozen V1 schema
guard rejected an in-place oneof change.

## Context

Accepted ADR-0007 requires every durable service-audit record to carry a
canonical list of at most 16 safe target references. It limits those references
to stable contract lineage or semantic IDs, contract version, commit sequence,
provenance ID, and capability ID, but does not enumerate the variants, scope
lineage-local numeric IDs, assign stable semantic tags, or define canonical list
ordering.

Leaving that vocabulary implicit would let `riffdb-service`, storage, the
durable codec, and transports create different target unions. In particular, a
bare `CommandId`, `EntityTypeId`, `ProjectionId`, or `IndexId` is ambiguous
across contract lineages. Including result objects, business keys, hashes, or
every returned row would also turn a bounded safe audit index into a data leak
or an unbounded result log.

## Decision

### Accepted amendment: registration audit authority

The exact "Accepted amendment: registration audit authority" text in SPEC §13.5
is incorporated here in full and qualifies the original decision below.
Maintainer, in session, 2026-09-16: "Approve exact text", referring to
`docs/architecture/WP-748-REGISTRATION-AUDIT-REVIEW.md`. WP-748 owns implementation,
compatibility fixtures and proof before activation.

The maintainer accepted the exact tag correction in session 2026-09-16:
"Approve exact correction", referring to
`docs/architecture/WP-748-ADMINISTRATION-TAG-REVIEW.md`. The incorporated SPEC
text assigns replication administration tag 75/revision 1 and preserves export
page tag 74/revision 1; all other accepted requirements and existing bytes remain
unchanged.

### Closed v1 registry

`riffdb-types` owns this exact value registry. Tag zero and every unlisted tag
are invalid:

| Tag | Variant | Payload |
|---:|---|---|
| `0x01` | `ContractLineage` | `ContractLineage` |
| `0x02` | `ContractVersion` | `ContractLineage`, nonzero `ContractVersion` |
| `0x03` | `EntityType` | `ContractLineage`, nonzero `EntityTypeId` |
| `0x04` | `Command` | `ContractLineage`, nonzero `CommandId` |
| `0x05` | `Projection` | `ContractLineage`, nonzero `ProjectionId` |
| `0x06` | `Index` | `ContractLineage`, nonzero `IndexId` |
| `0x07` | `Commit` | nonzero `CommitSequence` |
| `0x08` | `Provenance` | checked UUIDv7 `ProvenanceId` |
| `0x09` | `Capability` | checked UUIDv7 `CapabilityId` |
| `0x0a` | `EventConsumer` | `ContractLineage`, `ReactiveModuleHash`, `ReactiveOperationName`, `EventConsumerIdentityHash` |

Every contract semantic target repeats its `ContractLineage`. Numeric identity
is never interpreted outside that scope. `ContractVersion` likewise includes
lineage; a version number alone is not an audit target.

The POC target registry deliberately excludes `FieldId`, `OutcomeId`,
`EnumVariantId`, `EventTypeId`, `EventId`, `EnumTypeId`, `AggregateTypeId`,
`InvariantId`, database/request/session IDs, administration sequence, projection
generation/frontier, bundle/plan/content hashes, entity/index/partition keys,
idempotency identity, cursor, actor/tenant/environment values, and every raw or
free-form value. None is an independently addressed POC service object that
requires another target variant. `AdministrationSequence` occurs only in
`ServiceAuditLinkV1::ControlPlane`, not as a target.

Adding, removing, retagging, or changing the scope or payload of a variant is a
shared semantic and durable compatibility change requiring a later accepted
ADR. Rust enum declaration order, derived `Ord`, display text, and future
Protobuf layout are not the registry.

### Canonical target key and list

Each target has one canonical comparison key:

```text
target-key = tag:u8 || payload

lineage = lineage_byte_length:u32_be || exact_lineage_utf8
ContractLineage = lineage
ContractVersion = lineage || version:u64_be
EntityType      = lineage || entity_type_id:u32_be
Command         = lineage || command_id:u32_be
Projection      = lineage || projection_id:u32_be
Index           = lineage || index_id:u32_be
Commit          = commit_sequence:u64_be
Provenance      = provenance_uuidv7:16_network_order_bytes
Capability      = capability_uuidv7:16_network_order_bytes
EventConsumer   = lineage || reactive_module_hash:32 || operation_name_byte_length:u32_be || exact_operation_name_utf8 || consumer_identity_hash:32
```

The length is the exact UTF-8 byte length after `ContractLineage` validation;
there is no normalization or alternate text encoding. Every numeric component
is already nonzero through its checked type. UUID bytes retain ADR-0018's
checked network-order representation.

`ServiceAuditTargetsV1` contains zero through 16 targets. Its checked
constructor computes the complete keys, sorts lexicographically by those keys,
rejects an exact duplicate, rejects a seventeenth target, and exposes only the
checked canonical slice. It does not rely on hash-map iteration or caller order.
The same variant and local ID in distinct lineages are distinct targets. The
empty list is canonical.

WP-065 may choose reviewed Protobuf oneof field numbers that differ from these
semantic tags, but the conversion must be total and preserve this registry and
canonical order. Durable decoding rejects zero/unknown target kinds, malformed
payloads, duplicates, noncanonical order, and more than 16 entries rather than
sorting untrusted stored bytes into apparent validity.

### Service construction rule

`riffdb-service` is the sole production owner that resolves one invocation into
`ServiceAuditTargetsV1`. A constructor validates shape and order only; it grants
no authorization. Storage accepts the already checked list for its specialized
audit transition and never derives targets. gRPC, MCP, CLI, and SDK adapters
cannot add, remove, or reinterpret targets.

A target identifies a bounded protected object independently selected or
addressed by the request and known before the invocation's `started` append.
The service applies these rules exhaustively:

- validate contract, get active contract, health, statistics, commit scan or
  subscription, outbox listing, and discovery use no target;
- deploy and get one contract version use `ContractVersion`;
- explain and execute use the exact `ContractVersion` plus `Command`;
- resolve outcome uses the request-selected `Command`; a version learned from
  the stored result is not retroactively added;
- get entity uses the exact `ContractVersion` plus `EntityType`;
- scan index uses the exact `ContractVersion` plus `Index`;
- projection query and status use the exact resolved `ContractVersion` plus
  `Projection`;
- get one commit uses `Commit`; provenance trace uses whichever one exact
  `Commit` or `Provenance` selector the request carries; and
- bootstrap/create/revoke capability uses the capability being created or
  revoked as `Capability`; and
- consume, acknowledge, negative-acknowledge, seek, retire, and consumer status
  use the exact `EventConsumer` selected by the request.

If a future accepted POC request shape independently selects a contract lineage
without a version, it uses `ContractLineage`; the current singleton get-active
operation has no selector and remains empty.

The authorizing capability, principal, result link, traversed children, returned
rows/fields/events, and objects merely discovered during execution are never
added as targets. A broad operation never emits one target per result. Start and
terminal records for one invocation use the same canonical target list.
Authoritative result identity belongs only in the independent closed
`ServiceAuditLinkV1`; link commit/provenance/administration values are not copied
into targets unless the original request separately selected that object.

Target resolution remains inside ADR-0007's classified audit scope. Failure to
resolve an intrinsically required target follows the accepted standalone/start
failure lifecycle and does not weaken audit or authorization behavior.

## Ownership

| Boundary | Owner |
|---|---|
| Value enum, semantic tags, canonical key, bounded canonical collection | `riffdb-types`, WP-010 |
| Request-to-target mapping after exact operation/plan classification | `riffdb-service`, WP-120 |
| Checked semantic record field and atomic append input | `riffdb-storage-api`, WP-060 |
| Durable Protobuf representation and total conversion | `riffdb-proto` plus storage-owned codec, WP-065 |
| Coordinator audit executor preserving the checked list | `riffdb-commit`, WP-100 |
| Persistence, integrity, and recovery validation | `riffdb-storage-redb`, WP-070 |

No adapter owns a target registry or durable encoding. MCP remains a
policy-filtered transport over the shared service and has no direct audit or
storage path.

## Alternatives Considered

1. **Nine high-level stable target variants:** accepted; covers every POC
   operation without business values or result expansion.
2. **Every stable compiler ID:** rejected; owner-scoped IDs are ambiguous and
   most are never independently addressed by a service operation.
3. **Free-form URI or string targets:** rejected; unbounded, injectable, and
   transport-dependent.
4. **Use result links as targets:** rejected; addressed objects and known
   authoritative results have different lifecycle semantics.
5. **Let the durable codec sort or repair lists:** rejected; persisted malformed
   state must fail integrity rather than become valid after decoding.
6. **Allow each adapter to describe targets:** rejected; it creates bypass and
   parity failures at a security boundary.

## Compatibility and Testing

WP-010 freezes:

- exact tag and canonical-key goldens for all nine variants;
- zero and adjacent unknown-tag rejection;
- same local semantic ID in different lineages remaining distinct;
- permutation-independent construction;
- empty and exactly-16 acceptance plus duplicate and 17th-item rejection;
- lineage case/byte preservation and numeric/UUID byte order; and
- redacted diagnostics with no raw target payload in error text.

WP-060 runs the memory conformance suite with checked target lists. WP-065
freezes reviewed durable field numbers, mapping, malformed vectors, and
decode/re-encode order. WP-070 checks stored canonical order and fails startup
integrity on malformed, duplicate, unknown, or over-limit targets. WP-100 proves
the executor preserves lists without derivation. WP-120 exhaustively maps all 22
operations, proves start/terminal target equality, and proves result and
authorizing IDs are not copied. WP-190 covers crash/reopen audit integrity;
WP-200 proves transport parity and safe exposure.

`ServiceAuditRecordV1` and `ServiceAuditTargetV1` remain byte-for-byte and
descriptor-for-descriptor frozen with the original nine target fields. WP-417
adds `ServiceAuditRecordV2` in its own source file; its target oneof repeats the
nine frozen field numbers and assigns field 10 to `EventConsumer`. Current
writes use V2. Durable reads accept both generations and reconstruct the same
checked semantic `StoredServiceAuditRecordV1`; V1 is never rewritten in place or
silently interpreted as carrying the new target. Schema hashes, record bounds,
mixed-generation reads, and V2 current-write selection are frozen by generated
fixtures and codec tests.

## Requirements and Work Packages

- **Requirements:** `STO-002`, `MCP-046`
- **Defines or blocks:** amended `WP-010`; `WP-060`; `WP-065`; `WP-070`;
  `WP-100`; `WP-120`
- **Final evidence:** `WP-190`, `WP-200`
