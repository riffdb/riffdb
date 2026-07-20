# ADR-0022: Durable Semantic Protobuf Schema Version 1

- **Status:** Accepted
- **Direction approved:** 2026-07-20
- **Exact text accepted:** 2026-07-20
- **Accepted:** 2026-07-20
- **Requires:** ADR-0004, ADR-0005, ADR-0006, ADR-0007, ADR-0009,
  ADR-0010, ADR-0011, ADR-0012, ADR-0013, ADR-0014, ADR-0016,
  ADR-0017, ADR-0018, ADR-0019, and ADR-0021
- **Clarifies:** The exact WP-065 durable payload schema without changing the
  accepted table, key, envelope, or semantic-storage boundaries
- **Decision deadline:** Before WP-065 adds any semantic-record `.proto` source,
  generated descriptor, schema hash, golden payload, or storage codec

The human maintainer accepted this exact durable semantic-record schema on
2026-07-20. Its package, modules, registered payloads, fields, numbers, enum and
oneof tags, presence rules, canonical-wire rules, and validation boundaries are
now authoritative for WP-065 and its durable consumers.

## Context

The accepted architecture fixes the `riffdb.storage.v1` package, the
`StoredEnvelope` policy, eight new semantic-record source modules, exactly 26
registered payload messages, and storage-owned semantic DTOs. It intentionally
leaves the payload fields and numbers to WP-065, after WP-060 has made the
semantic records concrete.

Letting implementation choose those fields piecemeal would make field presence,
enum values, oneof tags, canonical ordering, and nested-versus-registered status
accidental durable API. It would also risk importing the public value schema,
duplicating IDs already present in canonical keys, or encoding the terminal
idempotency result as a pointer rather than the complete stored outcome.

This proposal maps the storage records that exist at the WP-060 boundary. It
does not add a semantic field merely because Protobuf can represent one. Every
field below has a lossless source or destination in `riffdb-storage-api` or in a
foundational `riffdb-types` value used by that crate.

## Proposed Decision

### Package, source modules, and imports

All definitions use `syntax = "proto3"` and package
`riffdb.storage.v1`. The existing
`proto/riffdb/storage/v1/envelope.proto` remains unchanged. WP-065 adds exactly
these eight sources:

| Source | Imports | Registered payloads owned by the source |
|---|---|---|
| `riffdb/storage/v1/common.proto` | none | none; closed shared helpers only |
| `riffdb/storage/v1/metadata.proto` | `riffdb/storage/v1/common.proto` | `StoredStorageFormatVersionV1`, `StoredDatabaseIdentityV1`, `StoredApplicationSequenceAllocatorV1`, `StoredAdministrationSequenceAllocatorV1` |
| `riffdb/storage/v1/application.proto` | `riffdb/storage/v1/common.proto` | `StoredEntityRecordV1`, `StoredIndexEntryV1`, `StoredIndexEpochV1`, `StoredPendingAdmissionV1`, `StoredExecutionFailedV1`, `StoredOutcomeV1`, `StoredDurableEventV1`, `StoredProvenanceRecordV1`, `StoredCommitRecordV1` |
| `riffdb/storage/v1/catalog.proto` | `riffdb/storage/v1/common.proto` | `StoredContractBundleV1`, `ActiveCatalogPointerV1`, `StoredCatalogAdministrationV1` |
| `riffdb/storage/v1/capability.proto` | `riffdb/storage/v1/common.proto` | `CapabilityRecordV1`, `CapabilityTokenLookupV1`, `CapabilityBootstrapMarkerV1`, `CapabilityAdministrationAuditV1` |
| `riffdb/storage/v1/audit.proto` | `riffdb/storage/v1/common.proto` | `ServiceAuditRecordV1` |
| `riffdb/storage/v1/outbox.proto` | `riffdb/storage/v1/application.proto`, `riffdb/storage/v1/common.proto` | `StoredOutboxIntentV1`, `StoredOutboxStatusV1` |
| `riffdb/storage/v1/projection.proto` | `riffdb/storage/v1/common.proto` | `StoredProjectionStateV1`, `StoredProjectionApplyV1`, `StoredProjectionControlV1` |

No durable source imports `riffdb/v1/value.proto`, another public
`riffdb.v1` source, `google/protobuf/timestamp.proto`, or
`google/protobuf/empty.proto`. Canonical business records are carried as exact
ADR-0011 canonical-record bytes. This keeps decimal precision and stable field
IDs self-contained and prevents a public-schema change from changing a durable
payload's transitive descriptor closure.

`common.proto` owns `TimestampV1`; durable records never use the public
`riffdb.v1.Timestamp`. `UnitV1` is a payload-graph helper used only to give
zero-payload oneof variants explicit presence. Neither helper is a registered
envelope payload.

### Presence, scalar, and canonical-wire policy

The following rules apply to every definition below:

1. Every semantic optional scalar, enum, string, or bytes field uses the
   Proto3 `optional` keyword. Absence and a present default value are therefore
   distinguishable. A present zero enum is still rejected.
2. Singular message fields already have explicit presence in Proto3 and do not
   use the `optional` keyword. Record-specific validation decides whether that
   message presence is required or optional.
3. Oneof presence is explicit. Exactly one member is required for every closed
   semantic sum type below. No oneof accepts the unset case.
4. Nonzero Rust `u32`, `u64`, `NonZeroU16`, `NonZeroU32`, and `NonZeroU64`
   values use `uint32` or `uint64` and reject zero. Signed timestamps use
   `sint64`; nanoseconds use `uint32` and must be less than 1,000,000,000.
5. UUIDv7 identifiers use `bytes`, exactly 16 network-order bytes, and must pass
   the RFC 9562 version-7 and variant checks. Typed hashes use `bytes`, exactly
   32 bytes. `EventIdV1` is its nonzero `uint64` commit sequence and zero-based
   `uint32` ordinal, not an untyped UUID or string.
6. Payload-carried entity, index, partition, and range-prefix keys use their
   exact bounded canonical bytes. A semantic decoder reconstructs the typed key
   and verifies its purpose/version envelope and full consumption. Projection
   payloads instead repeat the exact accepted identity/generation/group or
   sequence components needed to reconstruct and compare their external table
   key, as specified below; they do not carry a second complete key encoding.
7. The canonical-record byte fields are exactly
   `DeclaredOutcomeV1.canonical_value`,
   `StoredEntityRecordV1.canonical_fields`,
   `StoredIndexEntryV1.canonical_covered_values`,
   `StoredDurableEventV1.canonical_payload`, and
   `StoredProjectionStateV1.canonical_measures`. Each is the complete output of
   `riffdb_types::encode_canonical_record`, including canonical-value version
   byte `0x01`, record tag `0x0d`, field count, and strictly increasing stable
   field IDs. Each is at most 1 MiB, must decode as a record, and must re-encode
   to identical bytes. Fields named `canonical_input_hash` are typed hashes;
   `canonical_index_range_prefix` is a canonical key prefix;
   `canonical_bundle` is the opaque compiled-bundle encoding; and each
   `canonical_group_values` element is one canonical scalar-value document.
8. Repeated semantic sets are already canonical before encoding. Decoders
   reject duplicates, decreasing order, wrong owner, or over-limit counts; they
   never sort or repair persisted input.
9. Enum value zero is always `*_UNSPECIFIED` and invalid in a durable semantic
   value. Unknown enum values fail semantic decoding.
10. The ADR-0006 envelope decoder checks version, registered FQN, schema hash,
    checksum, payload and envelope bounds before payload decoding. The payload
    and envelope then decode and deterministically re-encode byte-for-byte.
    Alternate field order, duplicate singular fields, duplicate oneof members,
    nonminimal varints, split packed fields, unknown fields, and trailing bytes
    are noncanonical.
11. No definition contains a speculative `reserved` statement. A future
    removal reserves its accepted name and number in the new schema decision at
    the time of removal.

The complete semantic-optional singular-message list is
`StoredCatalogAdministrationV1.previous_active`,
`CapabilityAdministrationAuditV1.initiator`,
`ServiceAuditRecordV1.principal`, `OutboxRetryMetadataV1.next_attempt_at`, and
`StoredProjectionControlV1.published`, `candidate`, and `failure`. Those fields
use ordinary singular-message declarations and message presence, never the
`optional` keyword. Every other singular message is required unless it is a
oneof member. The complete semantic-optional scalar/enum/string/bytes set is
declared with `optional` in the source blocks below.

Text identity is never normalized during decode. Every contract lineage is
nonempty UTF-8 of at most 256 bytes; every actor/principal ID and tenant ID is
nonempty UTF-8 of at most 256 bytes. Every environment is nonempty ASCII of at
most 64 bytes and contains only alphanumeric bytes, `.`, `_`, or `-`. Approval
IDs are nonempty visible ASCII of at most 256 bytes. Module-specific text, key,
collection, and aggregate limits are stated beside the owning message. Decode
invokes the same foundational newtype constructors used by live admission and
rejects rather than normalizes a value that does not satisfy them.

### `common.proto`

The complete shared helper module is proposed as:

```protobuf
syntax = "proto3";

package riffdb.storage.v1;

message UnitV1 {}

message TimestampV1 {
  sint64 seconds = 1;
  uint32 nanos = 2;
}

enum ActorKindV1 {
  ACTOR_KIND_UNSPECIFIED = 0;
  ACTOR_KIND_HUMAN = 1;
  ACTOR_KIND_AGENT = 2;
  ACTOR_KIND_SERVICE = 3;
}

message TenantScopeV1 {
  oneof scope {
    UnitV1 global = 1;
    string tenant_id = 2;
  }
}

message AdmittedActorContextV1 {
  string principal_id = 1;
  ActorKindV1 actor_kind = 2;
  TenantScopeV1 tenant_scope = 3;
  optional bytes agent_session_id = 4;
}

message AuditPrincipalV1 {
  string principal_id = 1;
  ActorKindV1 actor_kind = 2;
  bytes capability_id = 3;
  uint64 capability_revision = 4;
}

message EventIdV1 {
  uint64 commit_sequence = 1;
  uint32 event_ordinal = 2;
}
```

`TenantScopeV1.scope` uses exactly `global = 1` and `tenant_id = 2`.
`principal_id` is nonempty UTF-8 and at most 256 bytes. `tenant_id` is nonempty
UTF-8 and at most 256 bytes. `agent_session_id` and `capability_id`, when
present, are checked UUIDv7 values. `capability_revision` is nonzero.

### `metadata.proto`

```protobuf
syntax = "proto3";

package riffdb.storage.v1;

import "riffdb/storage/v1/common.proto";

message StoredStorageFormatVersionV1 {
  uint32 storage_format_version = 1;
}

message StoredDatabaseIdentityV1 {
  bytes database_id = 1;
}

message StoredApplicationSequenceAllocatorV1 {
  oneof state {
    uint64 next_commit_sequence = 1;
    UnitV1 exhausted = 2;
  }
}

message StoredAdministrationSequenceAllocatorV1 {
  oneof state {
    uint64 next_administration_sequence = 1;
    UnitV1 exhausted = 2;
  }
}
```

Both allocator oneofs use exactly `next = 1` semantics and `exhausted = 2`.
The next values are nonzero. `StoredStorageFormatVersionV1` is exactly 1 and
must equal the outer envelope's storage-format version.
`StoredDatabaseIdentityV1.database_id` is one checked UUIDv7. These four
messages correspond to the exact `meta` keys `format_version`, `database_id`,
`next_application_sequence`, and `next_administration_sequence`; there is no
combined retained-metadata envelope.

### `application.proto`

```protobuf
syntax = "proto3";

package riffdb.storage.v1;

import "riffdb/storage/v1/common.proto";

enum ExecutionFailureCodeV1 {
  EXECUTION_FAILURE_CODE_UNSPECIFIED = 0;
  EXECUTION_FAILURE_CODE_ARITHMETIC_FAULT = 1;
  EXECUTION_FAILURE_CODE_RESOURCE_LIMIT = 2;
}

enum DurabilityModeV1 {
  DURABILITY_MODE_UNSPECIFIED = 0;
  DURABILITY_MODE_SYNC = 1;
  DURABILITY_MODE_GROUP = 2;
  DURABILITY_MODE_MEMORY = 3;
}

message ExecutablePlanRefV1 {
  string contract_lineage = 1;
  uint64 contract_version = 2;
  bytes contract_bundle_hash = 3;
  uint32 command_id = 4;
  bytes command_plan_hash = 5;
}

message DurableKeySchemaBindingV1 {
  string contract_lineage = 1;
  uint64 contract_version = 2;
  bytes contract_bundle_hash = 3;
}

message IdempotencyKeyDigestV1 {
  uint32 digest_scheme = 1;
  uint32 digest_key_id = 2;
  bytes digest = 3;
}

message IdempotencyIdentityV1 {
  bytes database_id = 1;
  string environment = 2;
  TenantScopeV1 tenant_scope = 3;
  string principal_id = 4;
  string contract_lineage = 5;
  uint32 command_id = 6;
  IdempotencyKeyDigestV1 caller_key_digest = 7;
}

message StoredAdmittedProvenanceClaimsV1 {
  optional string source_repository = 1;
  optional string source_commit = 2;
  optional string reason = 3;
  optional string approval_id = 4;
}

message EntityTargetV1 {
  uint32 entity_type_id = 1;
  bytes entity_key = 2;
}

message ExpectedEntityStateV1 {
  oneof state {
    UnitV1 absent = 1;
    uint64 present_entity_version = 2;
  }
}

message IndexEpochPositionV1 {
  oneof position {
    UnitV1 before_first = 1;
    uint64 epoch = 2;
  }
}

message EntityReadDependencyV1 {
  EntityTargetV1 target = 1;
  ExpectedEntityStateV1 expected = 2;
}

message IndexRangeReadDependencyV1 {
  bytes canonical_index_range_prefix = 1;
  IndexEpochPositionV1 expected = 2;
}

message StoredReadDependencyV1 {
  oneof dependency {
    EntityReadDependencyV1 entity_observation = 1;
    IndexRangeReadDependencyV1 index_range_epoch = 2;
  }
}

message StoredReadDependenciesV1 {
  repeated StoredReadDependencyV1 dependencies = 1;
}

message DeclaredOutcomeV1 {
  uint32 outcome_id = 1;
  bytes canonical_value = 2;
}

message StoredEntityRecordV1 {
  EntityTargetV1 target = 1;
  uint64 entity_version = 2;
  uint64 written_by_contract = 3;
  DurableKeySchemaBindingV1 schema_binding = 4;
  bytes canonical_fields = 5;
}

message StoredIndexEntryV1 {
  bytes index_entry_key = 1;
  DurableKeySchemaBindingV1 schema_binding = 2;
  bytes canonical_covered_values = 3;
}

message StoredIndexEpochV1 {
  bytes canonical_index_range_prefix = 1;
  DurableKeySchemaBindingV1 schema_binding = 2;
  uint64 epoch = 3;
}

message StoredPendingAdmissionV1 {
  IdempotencyIdentityV1 identity = 1;
  bytes canonical_input_hash = 2;
  bytes admission_request_id = 3;
  ExecutablePlanRefV1 plan = 4;
  TimestampV1 logical_time = 5;
  AdmittedActorContextV1 actor = 6;
  bytes partition_key = 7;
  StoredAdmittedProvenanceClaimsV1 provenance_claims = 8;
}

message StoredExecutionFailedV1 {
  StoredPendingAdmissionV1 pending = 1;
  ExecutionFailureCodeV1 code = 2;
}

message StoredOutcomeV1 {
  IdempotencyIdentityV1 identity = 1;
  uint64 commit_sequence = 2;
  bytes admission_request_id = 3;
  ExecutablePlanRefV1 plan = 4;
  bytes canonical_input_hash = 5;
  AdmittedActorContextV1 actor = 6;
  TimestampV1 logical_time = 7;
  bytes partition_hash = 8;
  repeated bytes conflict_hashes = 9;
  DeclaredOutcomeV1 declared_outcome = 10;
  StoredAdmittedProvenanceClaimsV1 admitted_claims = 11;
  bytes provenance_id = 12;
  DurabilityModeV1 durability_mode = 13;
}

message StoredDurableEventV1 {
  EventIdV1 event_id = 1;
  uint32 event_type_id = 2;
  bytes canonical_payload = 3;
  bytes event_hash = 4;
}

message AffectedEntityV1 {
  EntityTargetV1 target = 1;
  uint64 entity_version = 2;
}

message StoredProvenanceRecordV1 {
  bytes provenance_id = 1;
  uint64 commit_sequence = 2;
  IdempotencyIdentityV1 identity = 3;
  bytes admission_request_id = 4;
  ExecutablePlanRefV1 plan = 5;
  bytes canonical_input_hash = 6;
  AdmittedActorContextV1 actor = 7;
  TimestampV1 logical_time = 8;
  bytes partition_hash = 9;
  repeated bytes conflict_hashes = 10;
  uint32 outcome_id = 11;
  repeated AffectedEntityV1 affected_entities = 12;
  repeated EventIdV1 event_ids = 13;
  StoredAdmittedProvenanceClaimsV1 admitted_claims = 14;
}

message CommittedEntityMutationV1 {
  ExpectedEntityStateV1 expected = 1;
  StoredEntityRecordV1 post_image = 2;
}

message StoredCommitRecordV1 {
  uint64 commit_sequence = 1;
  bytes admission_request_id = 2;
  ExecutablePlanRefV1 plan = 3;
  bytes canonical_input_hash = 4;
  AdmittedActorContextV1 actor = 5;
  TimestampV1 logical_time = 6;
  bytes partition_hash = 7;
  repeated bytes conflict_hashes = 8;
  StoredReadDependenciesV1 read_dependencies = 9;
  repeated CommittedEntityMutationV1 mutations = 10;
  repeated StoredDurableEventV1 events = 11;
  DeclaredOutcomeV1 declared_outcome = 12;
  bytes provenance_id = 13;
  repeated EventIdV1 outbox_event_ids = 14;
  DurabilityModeV1 durability_mode = 15;
}
```

The oneof tags are immutable: expected entity state is `absent = 1` or
`present_entity_version = 2`; epoch position is `before_first = 1` or
`epoch = 2`; and stored dependency is `entity_observation = 1` or
`index_range_epoch = 2`.

`IdempotencyKeyDigestV1.digest_scheme` is exactly 1,
`digest_key_id` is nonzero, and `digest` is exactly 32 bytes. The complete
identity must reconstruct the exact ADR-0005 canonical key and match the table
key. Environment, tenant, principal, lineage, command, plan, actor, and
provenance relationships are checked by the semantic constructors.

`EntityTargetV1.entity_type_id` is required because the semantic DTO retains it;
it must be nonzero and equal the owner embedded in `entity_key`.
`StoredIndexEntryV1.index_entry_key` already contains its nonzero `IndexId`.
`canonical_index_range_prefix` likewise contains the `IndexId` in its six-byte
minimum envelope. No application message adds a redundant `index_id` field.
The codec derives the semantic `IndexId` from those canonical bytes and then
constructs the storage DTO, rejecting a malformed prefix before allocation.

`StoredReadDependenciesV1.dependencies`, `conflict_hashes`, mutations, affected
entities, and event IDs retain the exact storage-API canonical order and limits.
Every conflict/partition/input/bundle/plan/event hash has its typed 32-byte
width. Every request and provenance ID is checked UUIDv7. Entity versions,
contract versions, command/outcome/event type IDs, epochs, and commit sequences
are nonzero.

Plan references always contain all five accepted fields. Partition keys are at
most 4 KiB and use the `0x50 0x01` purpose/version envelope. Read dependencies,
mutations, affected entities, and events are each bounded to 4,096 entries;
conflict hashes are bounded to `MAX_COMMIT_CONFLICT_HASHES` (2,046) and strictly
increasing. Pending non-runtime semantic content remains within 64 KiB and a
complete commit record remains within 15 MiB.

In admitted provenance claims, source repository and source commit are either
both present or both absent. Present repository is nonempty UTF-8 and at most
512 bytes; present commit is nonempty visible ASCII and at most 128 bytes;
present reason is nonempty UTF-8 and at most 1,024 bytes; and present approval ID
is nonempty visible ASCII and at most 256 bytes. An all-absent claims value is
valid, but its containing singular message is still required so canonical
encoding has only one representation.

`StoredDurableEventV1.canonical_payload` is a canonical record. Its `event_hash`
must equal the exact ADR-0011 `riffdb.event/v1` hash over
`EventId[12] || EventTypeId:u32_be || payload_length:u32_be || payload`.
Nested event values in a commit and outbox intent must be byte-for-byte equal to
the separately registered event row.

`StoredOutcomeV1` is the complete terminal idempotency-table value and the
persisted result. It is not a pointer. A terminal commit deletes the pending row
and installs exactly one `StoredOutcomeV1` envelope atomically; it writes no
pending tombstone, separate outcome-result row, or second terminal envelope.
`StoredExecutionFailedV1` is the disjoint non-commit terminal value and contains
the complete frozen pending admission plus code.

`DurabilityModeV1` has exact numeric values Sync `1`, Group `2`, and Memory `3`.
The semantic codec and compatibility fixtures retain and round-trip Memory so
the in-memory reference implementation remains representable. A production
durable engine must reject a proposed record containing Memory before staging;
production startup treats a persisted Memory command record as corruption. Sync
and Group are the only values a production durable engine can persist, but the
POC production server still exposes only Sync. Group remains disabled unless
the separate scheduling, fairness, latency, and crash-evidence review required
by SPEC Section 10.5 is completed.

### `catalog.proto`

```protobuf
syntax = "proto3";

package riffdb.storage.v1;

import "riffdb/storage/v1/common.proto";

message StoredContractBundleV1 {
  string contract_lineage = 1;
  uint64 contract_version = 2;
  bytes contract_bundle_hash = 3;
  bytes canonical_bundle = 4;
}

message ActiveCatalogPointerV1 {
  string contract_lineage = 1;
  uint64 contract_version = 2;
  bytes contract_bundle_hash = 3;
}

message StoredCatalogAdministrationV1 {
  uint64 administration_sequence = 1;
  bytes request_id = 2;
  TimestampV1 timestamp = 3;
  AuditPrincipalV1 principal = 4;
  ActiveCatalogPointerV1 previous_active = 5;
  ActiveCatalogPointerV1 activated = 6;
  optional string approval_id = 7;
}
```

`previous_active` is a singular message with presence and is the only optional
pointer; it does not use the `optional` keyword. `activated` and `principal` are
required. Bundle bytes are nonempty and at most 15 MiB. Lineage is nonempty and
at most 256 UTF-8 bytes, versions and administration sequence are nonzero, and
hashes are 32 bytes. The bundle key/payload identity, active-pointer row, bundle
hash, and previous/activated transition are checked for reciprocity. The codec
preserves opaque canonical bundle bytes; the catalog-owned historical validator
performs IR-aware bundle and plan validation.

### `capability.proto`

```protobuf
syntax = "proto3";

package riffdb.storage.v1;

import "riffdb/storage/v1/common.proto";

enum CapabilityPermissionKindV1 {
  CAPABILITY_PERMISSION_KIND_UNSPECIFIED = 0;
  CAPABILITY_PERMISSION_KIND_VALIDATE_CONTRACT = 1;
  CAPABILITY_PERMISSION_KIND_READ_CONTRACT = 2;
  CAPABILITY_PERMISSION_KIND_EXPLAIN_COMMAND = 3;
  CAPABILITY_PERMISSION_KIND_DEPLOY_CONTRACT = 4;
  CAPABILITY_PERMISSION_KIND_INVOKE_COMMAND = 5;
  CAPABILITY_PERMISSION_KIND_READ_ENTITY = 6;
  CAPABILITY_PERMISSION_KIND_SCAN_INDEX = 7;
  CAPABILITY_PERMISSION_KIND_QUERY_PROJECTION = 8;
  CAPABILITY_PERMISSION_KIND_READ_PROJECTION_STATUS = 9;
  CAPABILITY_PERMISSION_KIND_READ_COMMIT = 10;
  CAPABILITY_PERMISSION_KIND_SCAN_COMMITS = 11;
  CAPABILITY_PERMISSION_KIND_SUBSCRIBE_COMMITS = 12;
  CAPABILITY_PERMISSION_KIND_READ_PROVENANCE = 13;
  CAPABILITY_PERMISSION_KIND_INSPECT_OUTBOX = 14;
  CAPABILITY_PERMISSION_KIND_READ_HEALTH = 15;
  CAPABILITY_PERMISSION_KIND_READ_STATISTICS = 16;
  CAPABILITY_PERMISSION_KIND_CREATE_CAPABILITY = 17;
  CAPABILITY_PERMISSION_KIND_REVOKE_CAPABILITY = 18;
  CAPABILITY_PERMISSION_KIND_ADMINISTER_CAPABILITIES = 19;
}

enum RevocationReasonCodeV1 {
  REVOCATION_REASON_CODE_UNSPECIFIED = 0;
  REVOCATION_REASON_CODE_REQUESTED = 1;
  REVOCATION_REASON_CODE_REPLACED = 2;
  REVOCATION_REASON_CODE_SUSPECTED_COMPROMISE = 3;
  REVOCATION_REASON_CODE_POLICY_CHANGE = 4;
}

enum CapabilityAdministrationOperationV1 {
  CAPABILITY_ADMINISTRATION_OPERATION_UNSPECIFIED = 0;
  CAPABILITY_ADMINISTRATION_OPERATION_BOOTSTRAP = 1;
  CAPABILITY_ADMINISTRATION_OPERATION_CREATE = 2;
  CAPABILITY_ADMINISTRATION_OPERATION_REVOKE = 3;
}

message CapabilityTokenDigestV1 {
  uint32 digest_scheme = 1;
  uint32 digest_key_id = 2;
  bytes digest = 3;
}

message CapabilityPermissionV1 {
  CapabilityPermissionKindV1 kind = 1;
  optional string contract_lineage = 2;
  optional uint32 stable_id = 3;
}

message CapabilityPermissionsV1 {
  repeated CapabilityPermissionV1 values = 1;
}

message ScopedPartitionV1 {
  string contract_lineage = 1;
  bytes partition_key = 2;
}

message ExplicitPartitionScopeV1 {
  repeated ScopedPartitionV1 partitions = 1;
}

message PartitionScopeV1 {
  oneof scope {
    UnitV1 all = 1;
    ExplicitPartitionScopeV1 explicit = 2;
  }
}

message EntityFieldVisibilityV1 {
  string contract_lineage = 1;
  uint32 entity_type_id = 2;
  repeated uint32 field_ids = 3 [packed = true];
}

message CapabilityGrantV1 {
  TenantScopeV1 tenant_scope = 1;
  PartitionScopeV1 partition_scope = 2;
  CapabilityPermissionsV1 permissions = 3;
  repeated EntityFieldVisibilityV1 field_visibility = 4;
  uint32 max_scan_rows = 5;
  repeated CapabilityPermissionKindV1 approval_required = 6 [packed = true];
}

message RevokedCapabilityV1 {
  TimestampV1 revoked_at = 1;
  uint64 administration_sequence = 2;
  RevocationReasonCodeV1 reason = 3;
}

message CapabilityLifecycleV1 {
  oneof state {
    UnitV1 active = 1;
    RevokedCapabilityV1 revoked = 2;
  }
}

message CapabilityRecordV1 {
  bytes capability_id = 1;
  uint64 revision = 2;
  CapabilityTokenDigestV1 token_digest = 3;
  bytes database_id = 4;
  string environment = 5;
  string principal_id = 6;
  ActorKindV1 actor_kind = 7;
  repeated string audiences = 8;
  TimestampV1 issued_at = 9;
  TimestampV1 expires_at = 10;
  uint64 creation_sequence = 11;
  bytes creation_request_id = 12;
  CapabilityGrantV1 grant = 13;
  CapabilityLifecycleV1 lifecycle = 14;
}

message CapabilityTokenLookupV1 {
  bytes capability_id = 1;
}

message CapabilityBootstrapMarkerV1 {
  bytes database_id = 1;
  bytes capability_id = 2;
  uint64 administration_sequence = 3;
}

message CapabilityAdministrationAuditV1 {
  uint64 administration_sequence = 1;
  bytes request_id = 2;
  CapabilityAdministrationOperationV1 operation = 3;
  TimestampV1 timestamp = 4;
  AuditPrincipalV1 initiator = 5;
  bytes target_capability_id = 6;
  uint64 resulting_revision = 7;
  optional string approval_id = 8;
  optional RevocationReasonCodeV1 revocation_reason = 9;
}
```

`CapabilityPermissionV1` is one normalized message, not seven competing wire
shapes. The exact valid field combinations are:

| `kind` | Required parameter fields | Meaning of `stable_id` |
|---|---|---|
| ExplainCommand, InvokeCommand | `contract_lineage`, `stable_id` | nonzero `CommandId` |
| ReadEntity | `contract_lineage`, `stable_id` | nonzero `EntityTypeId` |
| ScanIndex | `contract_lineage`, `stable_id` | nonzero `IndexId` |
| QueryProjection, ReadProjectionStatus | `contract_lineage`, `stable_id` | nonzero `ProjectionId` |
| Every other listed kind | neither optional field | not present |

Every present `stable_id` is nonzero and every present lineage is valid. The
permission kind supplies the stable ID's semantic newtype; no wire-side generic
ID enters the semantic API. This normalized shape maps one-to-one to
`CapabilityPermissionV1` after shape validation and keeps the stable permission
kind as the canonical ordering prefix. `CapabilityPermissionsV1.values` is the
only repeated permission field and retains the checked canonical set order. The
wrapper message is required even when its `values` set is canonically empty.

`PartitionScopeV1.scope` uses exactly `all = 1`, `explicit = 2`; capability
lifecycle uses exactly `active = 1`, `revoked = 2`. Explicit partitions are
nonempty, at most 1,024, canonically ordered, and duplicate-free. Permissions
are at most 8,192 and canonically ordered. Field visibility has at most 65,535
entries and fields in total. Every visibility entry has a nonempty, strictly
increasing set of nonzero field IDs; entries themselves are strictly ordered.
`max_scan_rows` is in `1..=500`. `approval_required` is duplicate-free
numeric order and at most 19 entries.

Capability audiences contain 1 through 8 nonempty visible-ASCII values of at
most 512 bytes in strict order. Capability payload semantic content is at most
1 MiB. Digest scheme is 1, digest key ID is nonzero, and digest is 32 bytes.
Capability/database/request IDs are checked UUIDv7; revision and sequence fields
are nonzero. Active records have revision 1. Revoked records have revision 2,
carry a nonzero revoking administration sequence greater than the creation
sequence, and carry exactly one closed reason. Expiry is an exact whole-second
interval after issue with equal nanoseconds and a duration in
`1..=2_592_000`; no requested-duration field is persisted redundantly.

`CapabilityAdministrationAuditV1.initiator` uses singular-message presence:
absence is valid only for Bootstrap. Bootstrap and Create require revision 1
and no revocation reason; Bootstrap has no initiator, Create has one. Revoke
requires an initiator, revision 2, and a present nonzero
`revocation_reason`. These conditions are rejected rather than normalized.

### `audit.proto`

```protobuf
syntax = "proto3";

package riffdb.storage.v1;

import "riffdb/storage/v1/common.proto";

enum ServiceOperationV1 {
  SERVICE_OPERATION_UNSPECIFIED = 0;
  SERVICE_OPERATION_VALIDATE_CONTRACT = 1;
  SERVICE_OPERATION_EXPLAIN_COMMAND = 2;
  SERVICE_OPERATION_DEPLOY_CONTRACT = 3;
  SERVICE_OPERATION_GET_ACTIVE_CONTRACT = 4;
  SERVICE_OPERATION_GET_CONTRACT_VERSION = 5;
  SERVICE_OPERATION_EXECUTE_COMMAND = 6;
  SERVICE_OPERATION_RESOLVE_COMMAND_OUTCOME = 7;
  SERVICE_OPERATION_GET_ENTITY = 8;
  SERVICE_OPERATION_SCAN_INDEX = 9;
  SERVICE_OPERATION_QUERY_PROJECTION = 10;
  SERVICE_OPERATION_GET_PROJECTION_STATUS = 11;
  SERVICE_OPERATION_GET_COMMIT = 12;
  SERVICE_OPERATION_SCAN_COMMITS = 13;
  SERVICE_OPERATION_SUBSCRIBE_TO_COMMITS = 14;
  SERVICE_OPERATION_TRACE_PROVENANCE = 15;
  SERVICE_OPERATION_GET_HEALTH = 16;
  SERVICE_OPERATION_GET_STATISTICS = 17;
  SERVICE_OPERATION_CREATE_CAPABILITY = 18;
  SERVICE_OPERATION_REVOKE_CAPABILITY = 19;
  SERVICE_OPERATION_LIST_PENDING_OUTBOX_DELIVERIES = 20;
  SERVICE_OPERATION_DISCOVER_COMMAND_TOOLS = 21;
  SERVICE_OPERATION_DISCOVER_RESOURCES = 22;
}

enum ServiceAuditPhaseV1 {
  SERVICE_AUDIT_PHASE_UNSPECIFIED = 0;
  SERVICE_AUDIT_PHASE_STARTED = 1;
  SERVICE_AUDIT_PHASE_SUCCEEDED = 2;
  SERVICE_AUDIT_PHASE_DENIED = 3;
  SERVICE_AUDIT_PHASE_CANCELLED = 4;
  SERVICE_AUDIT_PHASE_FAILED = 5;
  SERVICE_AUDIT_PHASE_OUTCOME_UNCERTAIN = 6;
}

enum ServiceIngressKindV1 {
  SERVICE_INGRESS_KIND_UNSPECIFIED = 0;
  SERVICE_INGRESS_KIND_GRPC = 1;
  SERVICE_INGRESS_KIND_MCP_HTTP = 2;
  SERVICE_INGRESS_KIND_IN_PROCESS_TEST_COMPARISON = 3;
}

message ContractVersionAuditTargetV1 {
  string contract_lineage = 1;
  uint64 contract_version = 2;
}

message EntityTypeAuditTargetV1 {
  string contract_lineage = 1;
  uint32 entity_type_id = 2;
}

message CommandAuditTargetV1 {
  string contract_lineage = 1;
  uint32 command_id = 2;
}

message ProjectionAuditTargetV1 {
  string contract_lineage = 1;
  uint32 projection_id = 2;
}

message IndexAuditTargetV1 {
  string contract_lineage = 1;
  uint32 index_id = 2;
}

message ServiceAuditTargetV1 {
  oneof target {
    string contract_lineage = 1;
    ContractVersionAuditTargetV1 contract_version = 2;
    EntityTypeAuditTargetV1 entity_type = 3;
    CommandAuditTargetV1 command = 4;
    ProjectionAuditTargetV1 projection = 5;
    IndexAuditTargetV1 index = 6;
    uint64 commit_sequence = 7;
    bytes provenance_id = 8;
    bytes capability_id = 9;
  }
}

message CommandServiceAuditLinkV1 {
  uint64 commit_sequence = 1;
  bytes provenance_id = 2;
}

message ControlPlaneServiceAuditLinkV1 {
  uint64 administration_sequence = 1;
}

message ServiceAuditLinkV1 {
  oneof link {
    UnitV1 none = 1;
    CommandServiceAuditLinkV1 command = 2;
    ControlPlaneServiceAuditLinkV1 control_plane = 3;
  }
}

message ServiceAuditRecordV1 {
  uint64 administration_sequence = 1;
  bytes request_id = 2;
  TimestampV1 timestamp = 3;
  ServiceOperationV1 operation = 4;
  ServiceAuditPhaseV1 phase = 5;
  AuditPrincipalV1 principal = 6;
  ServiceIngressKindV1 ingress = 7;
  repeated ServiceAuditTargetV1 targets = 8;
  optional string approval_id = 9;
  ServiceAuditLinkV1 link = 10;
}
```

Every `ServiceAuditTargetV1` oneof field number is exactly its accepted
ADR-0021 semantic tag: ContractLineage `1`, ContractVersion `2`, EntityType `3`,
Command `4`, Projection `5`, Index `6`, Commit `7`, Provenance `8`, and
Capability `9`. No separate target-kind enum can drift from those tags.
Lineage-local numeric targets always repeat lineage. Targets contain no business
key, result payload, hash, display URI, or transport text.

The result-link oneof uses exactly `none = 1`, `command = 2`, and
`control_plane = 3`. A command link is all-or-nothing. Target lists contain zero
through 16 entries in exact ADR-0021 canonical-key order and reject duplicates.
The complete semantic record is at most 64 KiB.

`principal` is a singular message with presence and is absent only for the
checked capability-bootstrap Started/Succeeded cases. `link` is required even
for `none`. The operation/phase/principal/link combinations must satisfy the
closed storage-API lifecycle table; malformed combinations are never repaired.

### `outbox.proto`

```protobuf
syntax = "proto3";

package riffdb.storage.v1;

import "riffdb/storage/v1/application.proto";
import "riffdb/storage/v1/common.proto";

message StoredOutboxIntentV1 {
  StoredDurableEventV1 event = 1;
}

message OutboxRetryMetadataV1 {
  uint32 attempts = 1;
  TimestampV1 last_attempt_at = 2;
  TimestampV1 next_attempt_at = 3;
  string destination_id = 4;
  optional string last_safe_error = 5;
}

message OutboxDeliveringV1 {
  uint32 attempt = 1;
  string destination_id = 2;
  TimestampV1 started_at = 3;
  TimestampV1 lease_deadline = 4;
}

message OutboxDeliveredV1 {
  uint32 attempts = 1;
  string destination_id = 2;
  TimestampV1 delivered_at = 3;
}

message OutboxDeadLetterV1 {
  uint32 attempts = 1;
  string destination_id = 2;
  TimestampV1 failed_at = 3;
  optional string last_safe_error = 4;
}

message StoredOutboxStatusV1 {
  EventIdV1 event_id = 1;
  oneof state {
    OutboxRetryMetadataV1 pending = 2;
    OutboxDeliveringV1 delivering = 3;
    OutboxDeliveredV1 delivered = 4;
    OutboxDeadLetterV1 dead_letter = 5;
  }
}
```

The status oneof fields are exactly `pending = 2`, `delivering = 3`,
`delivered = 4`, and `dead_letter = 5`; field 1 is the event identity.
`next_attempt_at` is an optional singular message and therefore uses message
presence without the `optional` keyword. Pending attempts, Delivering attempt,
and Delivered attempts are nonzero. DeadLetter attempts may be zero because the
semantic type permits a policy terminal decision before an attempt begins.

Destination IDs are nonempty visible ASCII and at most 512 bytes. Safe errors
are nonempty UTF-8 and at most 1,024 bytes when present. Every status row's event
ID must match its table key and an authoritative event/intent/commit tuple.

Canonical never-attempted Pending is the absence of a status row. WP-065 adds no
initial-status record, sentinel, or envelope. A present Pending contains retry
metadata and therefore has at least one attempt.

### `projection.proto`

```protobuf
syntax = "proto3";

package riffdb.storage.v1;

import "riffdb/storage/v1/common.proto";

enum ProjectionLifecycleV1 {
  PROJECTION_LIFECYCLE_UNSPECIFIED = 0;
  PROJECTION_LIFECYCLE_BUILDING = 1;
  PROJECTION_LIFECYCLE_CATCHING_UP = 2;
  PROJECTION_LIFECYCLE_READY = 3;
  PROJECTION_LIFECYCLE_DEGRADED = 4;
  PROJECTION_LIFECYCLE_REBUILDING = 5;
  PROJECTION_LIFECYCLE_INVALID = 6;
}

enum PublishedApplyModeV1 {
  PUBLISHED_APPLY_MODE_UNSPECIFIED = 0;
  PUBLISHED_APPLY_MODE_ENABLED = 1;
  PUBLISHED_APPLY_MODE_SUSPENDED = 2;
}

enum ProjectionFailureCodeV1 {
  PROJECTION_FAILURE_CODE_UNSPECIFIED = 0;
  PROJECTION_FAILURE_CODE_ARITHMETIC_OVERFLOW = 1;
  PROJECTION_FAILURE_CODE_MALFORMED_DURABLE_EVENT = 2;
  PROJECTION_FAILURE_CODE_MISSING_COMMIT = 3;
  PROJECTION_FAILURE_CODE_PLAN_OR_SCHEMA_UNAVAILABLE = 4;
  PROJECTION_FAILURE_CODE_PROJECTION_STATE_INTEGRITY = 5;
  PROJECTION_FAILURE_CODE_HARD_LIMIT_EXCEEDED = 6;
}

message ProjectionIdentityV1 {
  string contract_lineage = 1;
  uint32 projection_id = 2;
  bytes projection_plan_hash = 3;
}

message FrontierPositionV1 {
  oneof position {
    UnitV1 before_first = 1;
    uint64 applied_through = 2;
  }
}

message ProjectionGenerationPositionV1 {
  uint64 generation = 1;
  FrontierPositionV1 frontier = 2;
}

message ProjectionFailureV1 {
  uint64 generation = 1;
  ProjectionFailureCodeV1 code = 2;
  optional uint64 at_sequence = 3;
}

message StoredProjectionStateV1 {
  ProjectionIdentityV1 identity = 1;
  uint64 generation = 2;
  repeated bytes canonical_group_values = 3;
  bytes canonical_measures = 4;
  uint64 last_changed_sequence = 5;
}

message StoredProjectionApplyV1 {
  ProjectionIdentityV1 identity = 1;
  uint64 generation = 2;
  uint64 commit_sequence = 3;
  bytes projection_apply_hash = 4;
}

message StoredProjectionControlV1 {
  ProjectionIdentityV1 identity = 1;
  uint64 highest_allocated_generation = 2;
  ProjectionGenerationPositionV1 published = 3;
  ProjectionGenerationPositionV1 candidate = 4;
  optional PublishedApplyModeV1 published_apply_mode = 5;
  ProjectionLifecycleV1 lifecycle = 6;
  ProjectionFailureV1 failure = 7;
}
```

Frontier position uses exactly `before_first = 1` and `applied_through = 2`.
`published`, `candidate`, and `failure` are optional singular messages and use
message presence without the `optional` keyword. `published_apply_mode` and
`ProjectionFailureV1.at_sequence` use Proto3 `optional` because they are an
optional enum and scalar respectively.

Projection IDs, generations, and sequences are nonzero; plan/apply hashes are
32 bytes. Each `canonical_group_values` entry is one complete ADR-0011 canonical
scalar value document. The list has at most 1,024 entries in declared group
order and excludes null, list, record, and every schema-incompatible scalar.
Identity, generation, and group values must reconstruct the complete table
`ProjectionGroupKey` exactly. Identity, generation, and commit sequence in an
apply payload must reconstruct the complete table `ProjectionApplyKey` exactly.
This repetition is intentional: ADR-0017 requires the state payload to repeat
identity, generation, and group values and the apply payload to repeat identity,
generation, and sequence so the payload and external table key reciprocate. The
payload does not embed the complete key bytes in addition to those components.
`canonical_measures` is a canonical record, and the complete projection-state
semantic value is no larger than the accepted 1 MiB bound. The record must
validate under the exact checked projection schema. `projection_apply_hash`
must match the exact storage-owned canonical apply request at marker creation.
A stored marker alone does not contain the
apply request and cannot recompute that hash; startup validates its width,
canonical marker key, retained-generation/frontier reciprocity, and any
available typed apply evidence without inventing a request.

The control decoder invokes `StoredProjectionControlV1::new` so the lifecycle,
highest generation, published/candidate positions, apply mode, frontier, and
failure shape remain exhaustive. It never repairs a frontier or changes a
generation.

### Closed registered payload inventory

`StoredEnvelope.record_type` is exactly one FQN in this table, without a leading
dot. The order is the accepted compatibility-registry order, not an alternate
numeric type registry.

| # | Source | Exact `record_type` FQN | Semantic value |
|---:|---|---|---|
| 1 | `metadata.proto` | `riffdb.storage.v1.StoredStorageFormatVersionV1` | `StorageFormatVersion` |
| 2 | `metadata.proto` | `riffdb.storage.v1.StoredDatabaseIdentityV1` | `DatabaseId` |
| 3 | `metadata.proto` | `riffdb.storage.v1.StoredApplicationSequenceAllocatorV1` | `ApplicationSequenceAllocator` |
| 4 | `metadata.proto` | `riffdb.storage.v1.StoredAdministrationSequenceAllocatorV1` | `AdministrationSequenceAllocator` |
| 5 | `catalog.proto` | `riffdb.storage.v1.StoredContractBundleV1` | `StoredContractBundleV1` |
| 6 | `catalog.proto` | `riffdb.storage.v1.ActiveCatalogPointerV1` | `ActiveCatalogPointerV1` |
| 7 | `catalog.proto` | `riffdb.storage.v1.StoredCatalogAdministrationV1` | `StoredCatalogAdministrationV1` |
| 8 | `application.proto` | `riffdb.storage.v1.StoredEntityRecordV1` | `StoredEntityRecordV1` |
| 9 | `application.proto` | `riffdb.storage.v1.StoredIndexEntryV1` | `StoredIndexEntryV1` |
| 10 | `application.proto` | `riffdb.storage.v1.StoredIndexEpochV1` | `StoredIndexEpochV1` |
| 11 | `application.proto` | `riffdb.storage.v1.StoredPendingAdmissionV1` | `StoredPendingAdmissionV1` |
| 12 | `application.proto` | `riffdb.storage.v1.StoredExecutionFailedV1` | `StoredExecutionFailedV1` |
| 13 | `application.proto` | `riffdb.storage.v1.StoredOutcomeV1` | `StoredOutcomeV1` |
| 14 | `application.proto` | `riffdb.storage.v1.StoredDurableEventV1` | `StoredDurableEventV1` |
| 15 | `outbox.proto` | `riffdb.storage.v1.StoredOutboxIntentV1` | `StoredOutboxIntentV1` |
| 16 | `application.proto` | `riffdb.storage.v1.StoredProvenanceRecordV1` | `StoredProvenanceRecordV1` |
| 17 | `application.proto` | `riffdb.storage.v1.StoredCommitRecordV1` | `StoredCommitRecordV1` |
| 18 | `capability.proto` | `riffdb.storage.v1.CapabilityRecordV1` | `StoredCapabilityRecordV1` |
| 19 | `capability.proto` | `riffdb.storage.v1.CapabilityTokenLookupV1` | `CapabilityTokenLookupV1` |
| 20 | `capability.proto` | `riffdb.storage.v1.CapabilityBootstrapMarkerV1` | `CapabilityBootstrapMarkerV1` |
| 21 | `capability.proto` | `riffdb.storage.v1.CapabilityAdministrationAuditV1` | `StoredCapabilityAdministrationV1` |
| 22 | `audit.proto` | `riffdb.storage.v1.ServiceAuditRecordV1` | `StoredServiceAuditRecordV1` |
| 23 | `outbox.proto` | `riffdb.storage.v1.StoredOutboxStatusV1` | `StoredOutboxStatusV1` |
| 24 | `projection.proto` | `riffdb.storage.v1.StoredProjectionStateV1` | `StoredProjectionStateV1` |
| 25 | `projection.proto` | `riffdb.storage.v1.StoredProjectionApplyV1` | `StoredProjectionApplyV1` |
| 26 | `projection.proto` | `riffdb.storage.v1.StoredProjectionControlV1` | `StoredProjectionControlV1` |

Every other message in this ADR is a closed, unregistered payload-graph helper
and is not a valid `record_type`. In particular, there is no registered
`StoredReadDependenciesV1`, `CapabilityGrantV1`, `RetainedMetadataV1`, combined
administration-audit union, atomic command-record-set, outcome pointer, pending
tombstone, key codec, node record, shutdown record, integrity-history record, or
future-use placeholder.

### Lossless semantic mapping and validation

The storage-owned `proto_codec` is the only semantic conversion layer. It
depends one way on `riffdb-proto`; `riffdb-proto` never imports or depends on
`riffdb-storage-api`. Encoding accepts checked semantic DTOs and cannot invent a
field. Decoding validates wire structure, reconstructs foundational newtypes,
then invokes the current semantic constructors.

The field inventory is lossless, but the current semantic API does not yet
expose every checked reconstruction operation needed by a total decoder. This
ADR does not authorize the codec to bypass private fields, fabricate unrelated
records, or duplicate constructor validation. The four concrete gaps are listed
under Unresolved Review Questions and must be resolved by a reviewed WP-060
follow-up or a narrow allowed-path amendment before the corresponding WP-065
decoder can merge.

The mapping is lossless for these reasons:

| Semantic family | Lossless representation and constructor check |
|---|---|
| Metadata | Exact supported format, UUIDv7 database ID, and allocator `Next(nonzero) | Exhausted` oneofs; no zero sentinel is converted into a newtype. |
| Catalog | Exact lineage/version/hash/opaque canonical bytes and exact optional prior pointer; `StoredCatalogAdministrationV1` preserves principal, request, timestamp, approval, and transition-current pointers. |
| Entity/index/epoch | Complete canonical key bytes, exact schema binding, version/epoch, and canonical record bytes; explicit entity owner must match its key, while index owners are derived from keys with no redundant field. |
| Pending/failure | Every field of `StoredPendingAdmissionV1` is present; `StoredExecutionFailedV1` nests that exact value and one closed failure code. |
| Outcome | Every field of the full `StoredOutcomeV1` is present, including admitted claims, provenance, conflicts, and durability. Replay-only metadata is absent because it is not durable. |
| Event/outbox intent | Exact typed event ID, type ID, canonical payload, and event hash; outbox intent nests the same event message without a divergent copy shape. |
| Provenance | Every immutable identity/context/claim, affected entity/version, and ordered event link is represented. |
| Commit | Every stored read dependency, expected prior state, complete entity post-image, event, outcome, provenance/outbox link, and durability value is represented. |
| Capability | All record, digest, grant, lifecycle, bootstrap, lookup, and administration fields are represented; normalized permission presence reconstructs exactly one Rust enum variant. |
| Service audit | All 22 operations, six phases, three ingresses, nine target variants, optional principal/approval, and closed result link are represented with exact tags. |
| Outbox status | Every present state and retry/lease/completion/dead-letter field is represented; semantic initial absence remains physical absence. |
| Projection | Exact identity/generation/group components reconstruct the state key; identity/generation/sequence reconstruct the apply key; hashes and measures are exact, and control-message presence preserves every optional position/mode/failure. |

For types whose public Rust constructor needs contextual evidence, decoding is
explicitly contextual rather than lossy. Projection state decoding requires the
matching `CheckedProjectionSchema`; historical persisted-key semantic validation
remains catalog-owned. Structural storage decoding first preserves the exact
bounded bytes and never fabricates IR validation.

### Cross-record and table validation

Payload validation is necessary but not sufficient. WP-070 startup and the
typed atomic transitions additionally enforce:

- every identity repeated in a payload equals its physical table key;
- metadata contains exactly the accepted retained categories and no deferred
  category;
- application commit keys and embedded sequences are contiguous from one and
  the application allocator is their checked successor or exact `Exhausted`;
  independently, the shared audit-table union of catalog, capability, and
  service records is contiguous from one and the administration allocator is
  its checked successor or exact `Exhausted`;
- every repeated database identity agrees with permanent metadata; every
  persisted idempotency digest uses scheme 1 and a readable configured key ID;
  every capability digest uses scheme 1, with a readable configured key ID
  additionally required for each active, unexpired capability;
- catalog bundle keys, bundle payloads, active pointer, and administration
  records reciprocate;
- a pending identity has exactly one pending row and no terminal row;
- a committed identity has no pending row and exactly one full StoredOutcome;
- every StoredOutcome has exactly one commit at the same sequence, every commit
  has one outcome, shared immutable fields agree, and linked provenance repeats
  the identity;
- every committed mutation's expected state appears in the commit's canonical
  read dependencies, an expected-absent mutation writes version one, and an
  expected-present mutation writes the checked successor version;
- commit events use the commit sequence and contiguous zero-based ordinals, and
  the commit's outbox event-ID list equals its event-ID list in the same order;
- a StoredExecutionFailed has no application sequence, commit, event, outbox,
  outcome, or command-provenance member;
- standalone event, nested commit event, and nested outbox-intent event are
  equal and their hash recomputes;
- capability record, token lookup, bootstrap marker, and administration audit
  cross-links agree without exposing token material;
- every present outbox status names an authoritative event/intent/commit tuple;
- every projection table key is exactly reconstructed by its payload components;
  for each currently retained published or candidate generation,
  `BeforeFirst` has no marker and `AppliedThrough(N)` has exactly markers
  `1..=N` and none above `N`; and
- retired projection rows and markers still decode canonically and never exceed
  `highest_allocated_generation`, but require no control frontier and are never
  accepted as an apply target, exactly as specified by ADR-0017.

Missing, duplicate, orphaned, unequal, noncanonical, unknown, or still-pending
edges fail closed. The codec and startup validator do not repair, synthesize,
sort, or delete authoritative records.

## Options Considered

1. **Self-contained storage package with canonical-record bytes:** Proposed.
   It is lossless against the current DTOs, keeps durable schema hashes isolated
   from public API changes, and lets the canonical value codec remain the one
   business-value identity.
2. **Import `riffdb.v1.Value` and `Timestamp`:** Rejected. Public decimal values
   omit semantic precision without a compiled schema, and unrelated public
   descriptor changes would enter durable schema-hash closures.
3. **Store only canonical payload blobs for every record:** Rejected. It would
   create a second unreviewed record codec and hide fields needed for bounded
   structural validation and compatibility review.
4. **Encode every Rust enum as a free integer or string:** Rejected. It loses the
   closed registry and allows default/unknown values to enter durable state.
5. **Give every capability permission a different oneof message:** Rejected.
   The normalized kind-plus-present-parameter shape maps exhaustively, keeps
   canonical ordering explicit, and avoids seven parallel parameter containers.
6. **Persist an outcome pointer plus a separate result row:** Rejected by the
   accepted single-terminal-row clarification.
7. **Reserve future record types or fields now:** Rejected by ADR-0019 and the
   no-speculation rule.

## Consequences

- WP-065 has one reviewable field/tag registry before generating durable bytes.
- Durable schema hashes cover a small self-contained import graph.
- Exact canonical record bytes are nested inside deterministic Protobuf rather
  than translating business values twice.
- Semantic decoders remain more strict than generic Protobuf decoders and must
  report typed incompatibility/corruption without leaking payloads.
- Adding or changing a field, enum value, oneof tag, presence rule, module,
  registered FQN, canonical ordering rule, or validation meaning is a durable
  compatibility decision.
- The proposal adds no dependency, storage engine, public API, command-language
  feature, migration, or post-POC record.

## Compatibility

Until accepted and implemented, this ADR creates no persisted compatibility
baseline. After acceptance, all source paths, package/message names, import
closures, field names and numbers, enum symbols and numbers, oneof members and
numbers, presence rules, scalar types, record-type FQNs, canonical ordering, and
semantic validation rules in this record are immutable storage-format
boundaries.

An incompatible change requires a newly registered payload/package or explicit
restartable migration under ADR-0006. An additive field still changes the
schema hash and is not readable by an older registry unless that exact new hash
is reviewed and registered. Field numbers and enum/oneof tags are never reused.

## Security

Raw idempotency keys, capability tokens, digest keys, internal errors, policy
sources, and unredacted diagnostics never enter these messages. Only typed
digests and safe bounded claims are durable. Actor, tenant, capability, target,
and provenance fields retain their accepted redaction classifications when an
error is reported.

All sizes, counts, depths, key envelopes, optional combinations, and aggregate
bounds are checked before allocation where knowable. Checksums and hashes are
integrity evidence, not authentication. Unknown schema hashes, enum values,
oneof shapes, and record types fail closed.

## Testing

Acceptance requires WP-065 to freeze at least:

- source and descriptor goldens for all nine storage source files;
- a generated inventory proving exactly the 26 registered FQNs and no extra or
  deferred payload;
- every field number/type/presence rule, every enum symbol/value, and every
  oneof member/tag in this ADR;
- semantic DTO-to-Proto-to-bytes-to-Proto-to-DTO round trips for every payload
  and every closed variant, including Memory durability in codec-only fixtures;
- zero, unknown, missing, duplicate-singular, multiple-oneof, nonminimal-varint,
  wrong-wire-type, unknown-field, noncanonical-order, over-limit, and trailing
  byte rejection;
- first, maximum, and Exhausted allocator fixtures;
- exact `UnitV1` presence fixtures for every zero-payload oneof member;
- UUIDv7, hash width, timestamp nanosecond, text alphabet, optional-presence,
  canonical record, and canonical key negative fixtures;
- all 19 permission kinds, the exact normalized
  `kind/contract_lineage/stable_id` presence matrix, and the required
  `CapabilityPermissionsV1 { values = 1 }` wrapper including its empty set;
- all nine service targets with oneof numbers equal to semantic tags, canonical
  order, duplicate, zero, unknown, wrong-lineage, empty, 16, and 17 cases;
- full StoredOutcome terminal row and no-pointer/no-tombstone/no-second-record
  fixtures plus malformed outcome/commit/provenance reciprocity vectors;
- exact event preimage/hash and standalone/commit/outbox three-copy fixtures;
- absent initial outbox status and every present status variant;
- every projection lifecycle/control shape, key identity, generation, frontier,
  failure, state, apply hash, and schema-context case;
- per-record canonical `StoredEnvelope` upper-bound proofs, exact actual encoded
  charges, equal-bound success, and one-byte-over rejection;
- deterministic regeneration, current semantic descriptor/payload goldens, and
  the existing compatibility-probe/registry fixtures; because no predecessor
  semantic schema exists, WP-065 must not fabricate an "old" semantic payload;
  and
- parser/decoder fuzz corpus registration for envelope and all semantic modules.

WP-070 adds production Memory-durability rejection, table-key equality,
cross-record startup reciprocity, corruption, crash, and reopen evidence. Later
process tests prove uncertain-response recovery, outbox survival, projection
recovery, and exact-startup refusal end to end.

## Requirements and Work Packages

- **Requirements:** `STO-002`, `STO-011`, `STO-012`, `STO-020`, `STO-021`,
  `STO-022`, `VAL-003`, `TXN-031`, `EFF-003`, `MCP-046`
- **Defines or blocks:** `WP-065`; durable persistence in `WP-070`; consumers in
  `WP-100`, `WP-110`, `WP-130`, `WP-160`, and `WP-170`
- **Final evidence:** `WP-190`, `WP-200`

## Unresolved Review Questions

No semantic DTO field is known to be missing from the proposed wire inventory.
Four current reconstruction APIs are missing, however:

1. `AffectedEntityV1` exposes only `from_record`; a decoder has the exact
   durable `(EntityTarget, EntityVersion)` but cannot construct the value
   without fabricating an entity post-image.
2. `StoredCatalogAdministrationV1` exposes only `from_committed_intent`; the
   intent requires a complete bundle that is intentionally absent from the
   durable administration record.
3. `StoredServiceAuditRecordV1` has no checked `from_stored_parts`. Rebuilding
   normal records through append intents is indirect, and principal-less
   bootstrap Started/Succeeded records require lifecycle-specific constructors
   and external context.
4. `StoredProjectionStateV1::new` requires `CheckedProjectionSchema`. A total
   semantic decode can be contextual, but WP-070's IR-opaque structural startup
   pass also needs an explicitly owned structural wire view or checked
   stored-parts boundary; it cannot silently claim historical schema validation.

WP-065's current allowed paths do not include `records.rs`, `catalog.rs`,
`audit.rs`, or `projection.rs`. Before implementation, the maintainer must choose
either a prerequisite WP-060 interface follow-up or a narrowly reviewed WP-065
allowed-path amendment for checked durable reconstruction APIs. The minimum
shape is an `AffectedEntityV1` stored-parts constructor, catalog- and
service-audit stored-parts constructors, and an explicit projection structural
decode contract. This ADR does not choose the work-package governance change.

Human review must also confirm the newly proposed field numbers, outbox-state
oneof tags, and normalized capability-permission representation before they
become durable. Those numbers are proposed here; they are not implied by the
accepted Rust DTOs themselves.

The proposal deliberately does not decide a first future migration, old-schema
retirement window, or later projection/outbox compaction format. Those features
are not needed to encode the POC records and must not add reservations here.

## Decision Deadline

Exact-text acceptance is required before WP-065 creates any of the eight
semantic `.proto` sources or generated artifacts. Acceptance does not authorize
WP-070 to begin before WP-065's generated schema, codec, bounds, goldens, and
acceptance commands pass.
