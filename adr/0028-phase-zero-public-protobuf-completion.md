# ADR-0028: Phase-Zero Public Protobuf Completion

- **Status:** Accepted
- **Direction approved:** 2026-07-21
- **Exact text accepted:** 2026-07-21
- **Requires:** ADR-0006, ADR-0007, ADR-0009, ADR-0010, ADR-0011,
  ADR-0012, ADR-0013, ADR-0017, ADR-0018, and ADR-0027
- **Clarifies:** The complete `riffdb.v1` public message inventory, field
  numbers, closed results, presence rules, and structural validation owned by
  WP-127
- **Decision deadline:** Before WP-127 changes any phase-zero public message,
  generated descriptor, schema hash, or compatibility fixture

The human maintainer accepted this exact record on 2026-07-21. It authorizes
WP-127 implementation against the public schema defined below.

## Context

ADR-0006 froze the `riffdb.v1` package, the exact `Value`, `PublicError`, and
initial `Execute` messages, and five services with exactly 16 RPCs. Every other
request and response is an explicitly unsupported empty name-reservation shell.
ADR-0007 and WP-120 now provide the API-neutral request and result DTOs needed
to complete those shells without making transport types semantic.

WP-127 is the one public-Protobuf interface package between those semantic DTOs
and WP-130's Tonic adapters. If WP-127 chooses field numbers, absence semantics,
duration units, result envelopes, projection states, or capability shapes only
while implementing validators, those choices become accidental public API.
This proposal records the complete choice before production code is changed.

The protocol must preserve the accepted shared-service boundary. In particular,
MCP remains a policy-filtered transport over the same application service and
does not receive another RPC, a direct storage path, or a more privileged
message. Public messages do not reuse `riffdb.storage.v1` messages: the public
views are authorized and redacted service results, while durable messages are
authoritative storage records with a different compatibility boundary.

## Proposed Decision

### Package, source files, and ownership

Every source below uses `syntax = "proto3"` and package `riffdb.v1`.
`value.proto` and `error.proto` remain unchanged. `command.proto` retains its
existing supported fields and adds only the accepted read-only completion value
and `GetOutcome` messages. WP-127 adds `common.proto`, `contract.proto`,
`query.proto`, `projection.proto`, `commit.proto`, and `admin.proto`.

The unsupported empty shells move from `services.proto` to their owning domain
source. `services.proto` then contains only the same five services and 16 RPCs,
with the same names and the same sole server-streaming shape. This relocation
changes descriptor source-file identity for unsupported shells, so it is part
of this review. It does not move or rename a supported `Value`, `PublicError`,
or `Execute` message.

`riffdb-proto` owns these sources, checked-in Prost messages, descriptor sets,
schema hashes, golden bytes, structural preflight, and context-free wire
validation. It must not depend on `riffdb-service` or implement a service DTO
conversion. WP-130 owns Tonic client/server generation, authentication metadata,
request-context construction, all service-to-wire conversions, status mapping,
and the Rust transport client.

The public sources never import `riffdb.storage.v1`. Generated MCP JSON schemas
remain compiler/IR artifacts consumed through the shared service. The
projection-status support messages below allow later MCP conversion without
adding a public gRPC RPC.

### General presence and encoding rules

The following rules apply to every new definition below:

1. A semantic optional scalar, enum, string, or bytes field uses Proto3
   `optional`. A present zero or empty value may still be invalid.
2. Singular messages already have presence. They are required unless they are
   a oneof member or are listed as semantically optional below.
3. Exactly one known member is required for every closed oneof. An unset oneof,
   a zero enum where a semantic enum is required, and an unknown enum value are
   rejected.
4. The semantically optional singular messages are
   `SemanticDiagnostic.related_span` and `ProjectionStatus.published`,
   `candidate`, and `failure`. Their ordinary message presence represents
   absence; they do not use the `optional` keyword.
5. All other singular messages, including selections, pages, fences, records,
   schemas, timestamps, grants, and build information, must be present.
6. Public unknown Protobuf fields follow ADR-0006: they are ignored and never
   relayed, and no retention promise is made. Duplicate known singular or
   oneof members, malformed wire types, excessive nesting or allocation claims,
   and incomplete required shapes reject during preflight or validation.
7. Requests are bounded to 1,048,576 structurally decoded bytes. Unary results
   and each visible stream item are bounded to 4,194,304 bytes under ADR-0027.
   Validation uses checked arithmetic and bounded recursion before allocating
   attacker-controlled lengths or counts.
8. Strings are exact UTF-8 and are not normalized. Protocol/source names and
   contract lineages are nonempty and at most 256 bytes unless a more specific
   bound is stated. Stable `u32` IDs, contract/entity versions, epochs,
   generations, and application or administration sequences reject zero.
9. Typed hashes are exactly 32 bytes. Canonical entity, index-entry, and
   partition keys are nonempty, at most 4,096 bytes, decode fully through their
   semantic key codec, and agree with every separately carried owner ID.
10. Public business values use the unchanged exact `Value` family. New fields
    known to be records use `ValueRecord`. Existing `Execute.input` and
    `ExecuteCommandResponse.outcome` remain `Value` for compatibility; WP-130
    checks the record shape required by the API-neutral command DTO and active
    compiled schema.

### `common.proto`

The complete shared public helper source is proposed as:

```protobuf
syntax = "proto3";

package riffdb.v1;

import "riffdb/v1/value.proto";

message Unit {}

message ExactContractSelection {
  string contract_lineage = 1;
  uint64 contract_version = 2;
}

message ContractSelection {
  oneof selection {
    Unit active = 1;
    ExactContractSelection exact = 2;
  }
}

message PageRequest {
  optional uint32 limit = 1;
  optional bytes cursor = 2;
}

message FieldSelection {
  repeated uint32 field_ids = 1;
}

message FrontierPosition {
  oneof position {
    Unit before_first = 1;
    uint64 applied_through = 2;
  }
}

enum ActorKind {
  ACTOR_KIND_UNSPECIFIED = 0;
  ACTOR_KIND_HUMAN = 1;
  ACTOR_KIND_AGENT = 2;
  ACTOR_KIND_SERVICE = 3;
}

message TenantScope {
  oneof scope {
    Unit global = 1;
    string tenant_id = 2;
  }
}

message AdmittedActor {
  string principal_id = 1;
  ActorKind actor_kind = 2;
  TenantScope tenant_scope = 3;
  optional bytes agent_session_id = 4;
}

enum ExecutionClass {
  EXECUTION_CLASS_UNSPECIFIED = 0;
  EXECUTION_CLASS_READ_ONLY = 1;
  EXECUTION_CLASS_IDEMPOTENT_MUTATION = 2;
}

enum CommandDurability {
  COMMAND_DURABILITY_UNSPECIFIED = 0;
  COMMAND_DURABILITY_SYNCHRONOUS = 1;
  COMMAND_DURABILITY_GROUP = 2;
}

message DeclaredOutcome {
  uint32 outcome_id = 1;
  string outcome_name = 2;
  ValueRecord value = 3;
}
```

`ContractSelection.active` is explicit; an unset selection never means active.
An exact selection carries a nonempty lineage and nonzero version.

`PageRequest.limit` is optional at the reusable message boundary so a future
operation whose accepted schema permits omission can apply the specified
default of 50. `ScanIndex`, `QueryProjection`, and `ScanCommits` require the
field to be present and in `1..=500`. The cursor is absent for an initial page.
A present cursor is exactly 16 opaque, process-local random bytes and is not a
UUID. An empty present cursor rejects. `FieldSelection.field_ids` contains at
most 1,024 nonzero IDs in strictly increasing order without duplicates; an
empty, present `FieldSelection` is meaningful.

`FrontierPosition.before_first` is not sequence zero. `applied_through` is
nonzero. No decoder constructs a `CommitSequence(0)`.

Tenant IDs and principal IDs are nonempty and at most 256 bytes. A present
`agent_session_id` is an exact UUIDv7. `DeclaredOutcome` has a nonzero stable
outcome ID, its exact compiler-owned source name, and a canonical record with
unique increasing field IDs.

`CommandDurability` describes API-neutral production results and therefore has
only synchronous and group modes. It does not alter the immutable string field
in `ExecuteCommandResponse`, discussed below.

### `contract.proto`

The complete contract API source is proposed as:

```protobuf
syntax = "proto3";

package riffdb.v1;

import "riffdb/v1/common.proto";

message ContractDescriptor {
  string contract_lineage = 1;
  uint64 contract_version = 2;
  bytes bundle_hash = 3;
  bytes source_hash = 4;
  bytes plan_root_hash = 5;
}

message SourceSpan {
  uint32 start = 1;
  uint32 end = 2;
}

message SyntaxDiagnostic {
  string code = 1;
  string summary = 2;
  optional string help = 3;
  SourceSpan span = 4;
  repeated string expected = 5;
}

message SemanticDiagnostic {
  string code = 1;
  string summary = 2;
  optional string help = 3;
  SourceSpan primary_span = 4;
  SourceSpan related_span = 5;
}

message SyntaxDiagnosticList {
  repeated SyntaxDiagnostic diagnostics = 1;
}

message SemanticDiagnosticList {
  repeated SemanticDiagnostic diagnostics = 1;
}

message CompilationDiagnostics {
  oneof diagnostics {
    SyntaxDiagnosticList syntax = 1;
    SemanticDiagnosticList semantic = 2;
  }
}

message SchemaArtifactKey {
  oneof artifact {
    uint32 entity_id = 1;
    uint32 event_type_id = 2;
    uint32 command_input_id = 3;
    uint32 command_outcome_union_id = 4;
    uint32 projection_result_id = 5;
  }
}

message GeneratedSchemaArtifact {
  SchemaArtifactKey key = 1;
  string dialect = 2;
  bytes schema_hash = 3;
  string canonical_json = 4;
}

message BindingFieldRef {
  uint32 binding_id = 1;
  uint32 field_id = 2;
}

message CommandExplain {
  uint32 command_id = 1;
  ExecutionClass execution_class = 2;
  uint32 partition_component_count = 3;
  uint32 conflict_key_count = 4;
  repeated uint32 binding_ids = 5;
  repeated BindingFieldRef read_fields = 6;
  repeated BindingFieldRef write_fields = 7;
  repeated uint32 invariant_ids = 8;
  repeated uint32 event_type_ids = 9;
  repeated uint32 outcome_ids = 10;
  string rendered_text = 11;
}

message ValidateContractRequest {
  bytes request_id = 1;
  string source = 2;
}

message ValidateContractResponse {
  oneof result {
    Unit valid = 1;
    CompilationDiagnostics invalid = 2;
  }
}

message ExplainCommandRequest {
  bytes request_id = 1;
  ContractSelection contract = 2;
  string command_name = 3;
}

message ExplainedCommand {
  ContractDescriptor contract = 1;
  uint32 command_id = 2;
  bytes plan_hash = 3;
  CommandExplain explanation = 4;
  GeneratedSchemaArtifact input_schema = 5;
  GeneratedSchemaArtifact outcome_schema = 6;
}

message ExplainCommandResponse {
  oneof result {
    Unit not_found = 1;
    ExplainedCommand found = 2;
  }
}

message DeployContractRequest {
  bytes request_id = 1;
  string source = 2;
  optional uint64 expected_active_version = 3;
}

message ExpectedActiveVersionMismatch {
  optional uint64 actual_active_version = 1;
}

message DeployContractResponse {
  oneof result {
    ContractDescriptor activated = 1;
    ContractDescriptor already_active = 2;
    ExpectedActiveVersionMismatch expected_active_version_mismatch = 3;
    Unit bundle_conflict = 4;
  }
}

message GetActiveContractRequest {
  bytes request_id = 1;
}

message GetActiveContractResponse {
  oneof result {
    Unit absent = 1;
    ContractDescriptor present = 2;
  }
}
```

All three descriptor hashes and every plan or schema hash are exactly 32 bytes.
Source spans are half-open UTF-8 byte offsets with `start <= end` and must lie in
the submitted source. A diagnostic list is nonempty and contains at most 32
items. Syntax `expected` contains at most 16 compiler-owned static token names.
Diagnostic code, summary, and optional help text must exactly match the closed
compiler registry for the selected syntax or semantic kind; no caller or
internal error text is copied into these fields.

`GeneratedSchemaArtifact.dialect` is exactly
`https://json-schema.org/draft/2020-12/schema`. Its canonical JSON is at most
1 MiB, its hash is exactly 32 bytes, and its key is present with a nonzero stable
owner ID. In an `ExplainedCommand`, the outer command ID, explanation command
ID, both schema keys, selected contract, and plan hash must agree.
`CommandExplain` carries only bounded stable-ID summaries and deterministic,
value-free compiler text. Its collections retain compiler order and reject
duplicate identities where the owning IR forbids them.

Contract source may be empty because invalid source is an ordinary typed
validation result; the complete request remains within 1 MiB. Deploy's absent
`expected_active_version` means the caller expects no active contract. It does
not disable the catalog compare-and-set check. A mismatch's absent
`actual_active_version` means the transaction-current catalog had no active
version.

### `command.proto`

The complete command source is proposed as:

```protobuf
syntax = "proto3";

package riffdb.v1;

import "riffdb/v1/common.proto";
import "riffdb/v1/value.proto";

message ExecuteCommandRequest {
  bytes request_id = 1;
  string command_name = 2;
  optional uint64 expected_contract_version = 3;
  Value input = 4;
}

message ExecuteCommandResponse {
  enum CompletionStatus {
    COMPLETION_STATUS_UNSPECIFIED = 0;
    COMMITTED = 1;
    REPLAYED = 2;
    EXECUTED_READ_ONLY = 3;
  }

  CompletionStatus status = 1;
  uint64 commit_sequence = 2;
  uint64 contract_version = 3;
  bytes plan_hash = 4;
  string outcome_type = 5;
  Value outcome = 6;
  string provenance_uri = 7;
  string durability_mode = 8;
}

message GetOutcomeRequest {
  bytes request_id = 1;
  string contract_lineage = 2;
  string command_name = 3;
  string idempotency_key = 4;
}

message GetOutcomeResponse {
  oneof result {
    Unit not_found = 1;
    ExecuteCommandResponse found = 2;
  }
}
```

The existing Execute names, field numbers, types, and the meanings of statuses
1 and 2 are immutable under ADR-0006. `EXECUTED_READ_ONLY = 3` is additive. A
read-only response requires the exact wire sentinels `commit_sequence = 0`, an
empty `provenance_uri`, and an empty `durability_mode`. A committed or replayed
response requires a nonzero sequence, a canonical provenance locator, and one
of the existing accepted strings `sync`, `group`, or `memory`. Preserving
`memory` in the structural decoder is compatibility for the supported
phase-zero message; WP-130 production conversion can emit only `sync` or
`group`, and P1 server composition explicitly selects `sync`.

Every success status requires a nonzero contract version, a 32-byte plan hash,
a nonempty bounded outcome name, and a present structurally valid `Value`.
Status zero, unknown status, and every status/field inconsistency reject.

`GetOutcome` reuses `ExecuteCommandResponse` rather than creating a second
journaled command-result envelope. Its `found` branch must have status
`REPLAYED`; `COMMITTED` and `EXECUTED_READ_ONLY` reject. The request lineage and
command name are nonempty and at most 256 bytes. The idempotency key is nonempty
and at most 128 bytes. A retry uses a fresh outer request ID while retaining the
same command input idempotency key.

### `query.proto`

The complete authoritative entity and index-query source is proposed as:

```protobuf
syntax = "proto3";

package riffdb.v1;

import "riffdb/v1/common.proto";
import "riffdb/v1/value.proto";

message GetEntityRequest {
  bytes request_id = 1;
  ContractSelection contract = 2;
  uint32 entity_type_id = 3;
  bytes entity_key = 4;
  FieldSelection fields = 5;
}

message Entity {
  bytes entity_key = 1;
  uint64 entity_version = 2;
  uint64 written_by_contract_version = 3;
  ValueRecord fields = 4;
}

message GetEntityResponse {
  oneof result {
    Unit not_found = 1;
    Entity found = 2;
  }
}

message ScanIndexRequest {
  bytes request_id = 1;
  ContractSelection contract = 2;
  uint32 index_id = 3;
  repeated Value leading_components = 4;
  FieldSelection fields = 5;
  PageRequest page = 6;
}

message IndexRow {
  bytes index_entry_key = 1;
  ValueRecord values = 2;
}

message IndexScanFence {
  uint64 index_epoch = 1;
}

message IndexPage {
  repeated IndexRow items = 1;
  optional bytes next_cursor = 2;
  IndexScanFence observed_fence = 3;
}

message ScanIndexResponse {
  IndexPage page = 1;
}
```

Entity type, index, field, version, and epoch IDs are nonzero. Entity and index
keys fully decode through their canonical key codecs and agree with the selected
type or index. Returned fields are policy-filtered canonical records. Index
leading components contain at most 1,024 structurally valid `Value` messages;
WP-130 resolves them against the selected schema before service invocation.

A page contains no more than the effective limit, retains the lower port's
canonical order, and cannot carry `next_cursor` when empty. A present next
cursor is exactly 16 opaque bytes. The index fence has a nonzero epoch and is
required even for an empty page.

### `projection.proto`

The complete projection query and status source is proposed as:

```protobuf
syntax = "proto3";

package riffdb.v1;

import "riffdb/v1/common.proto";
import "riffdb/v1/value.proto";

enum ProjectionLifecycle {
  PROJECTION_LIFECYCLE_UNSPECIFIED = 0;
  PROJECTION_LIFECYCLE_BUILDING = 1;
  PROJECTION_LIFECYCLE_CATCHING_UP = 2;
  PROJECTION_LIFECYCLE_READY = 3;
  PROJECTION_LIFECYCLE_DEGRADED = 4;
  PROJECTION_LIFECYCLE_REBUILDING = 5;
  PROJECTION_LIFECYCLE_INVALID = 6;
}

enum PublishedApplyMode {
  PUBLISHED_APPLY_MODE_UNSPECIFIED = 0;
  PUBLISHED_APPLY_MODE_ENABLED = 1;
  PUBLISHED_APPLY_MODE_SUSPENDED = 2;
}

enum ProjectionFailureCode {
  PROJECTION_FAILURE_CODE_UNSPECIFIED = 0;
  PROJECTION_FAILURE_CODE_ARITHMETIC_OVERFLOW = 1;
  PROJECTION_FAILURE_CODE_MALFORMED_DURABLE_EVENT = 2;
  PROJECTION_FAILURE_CODE_MISSING_COMMIT = 3;
  PROJECTION_FAILURE_CODE_PLAN_OR_SCHEMA_UNAVAILABLE = 4;
  PROJECTION_FAILURE_CODE_PROJECTION_STATE_INTEGRITY = 5;
  PROJECTION_FAILURE_CODE_HARD_LIMIT_EXCEEDED = 6;
}

message ProjectionIdentity {
  string contract_lineage = 1;
  uint32 projection_id = 2;
  bytes projection_plan_hash = 3;
}

message ProjectionGenerationFrontier {
  uint64 generation = 1;
  FrontierPosition frontier = 2;
}

message ProjectionFailure {
  uint64 generation = 1;
  ProjectionFailureCode code = 2;
  optional uint64 at_sequence = 3;
}

message ProjectionUnavailableReason {
  oneof reason {
    Unit building = 1;
    Unit rebuilding = 2;
    ProjectionFailureCode failure = 3;
  }
}

message ProjectionPageFence {
  ProjectionIdentity identity = 1;
  uint64 generation = 2;
  FrontierPosition frontier = 3;
}

message ProjectionRow {
  repeated Value group = 1;
  ValueRecord values = 2;
}

message ProjectionPage {
  repeated ProjectionRow items = 1;
  optional bytes next_cursor = 2;
  ProjectionPageFence observed_fence = 3;
}

message QueryProjectionReady {
  ProjectionPage data = 1;
  FrontierPosition frontier = 2;
}

message QueryProjectionWaitTimedOut {
  uint64 required_sequence = 1;
  FrontierPosition current = 2;
}

message QueryProjectionDegraded {
  FrontierPosition current = 1;
  ProjectionUnavailableReason reason = 2;
}

message QueryProjectionInvalid {
  ProjectionFailureCode reason = 1;
}

message QueryProjectionRequest {
  bytes request_id = 1;
  ContractSelection contract = 2;
  uint32 projection_id = 3;
  repeated Value leading_components = 4;
  optional uint64 required_sequence = 5;
  uint64 wait_nanos = 6;
  PageRequest page = 7;
}

message QueryProjectionResponse {
  oneof result {
    QueryProjectionReady ready = 1;
    QueryProjectionWaitTimedOut wait_timed_out = 2;
    QueryProjectionDegraded degraded = 3;
    QueryProjectionInvalid invalid = 4;
  }
}

message ProjectionStatus {
  ProjectionIdentity identity = 1;
  ProjectionLifecycle lifecycle = 2;
  ProjectionGenerationFrontier published = 3;
  ProjectionGenerationFrontier candidate = 4;
  optional PublishedApplyMode published_apply_mode = 5;
  ProjectionFailure failure = 6;
  FrontierPosition authoritative_head = 7;
}

message GetProjectionStatusRequest {
  bytes request_id = 1;
  ContractSelection contract = 2;
  uint32 projection_id = 3;
}

message GetProjectionStatusResponse {
  oneof result {
    Unit not_found = 1;
    ProjectionStatus found = 2;
  }
}
```

The lifecycle values deliberately preserve the accepted registry in which
`DEGRADED = 4` and `REBUILDING = 5`; Rust declaration order is not a wire
registry. Projection IDs and generations are nonzero, plan hashes are exactly
32 bytes, and `at_sequence`, when present, is nonzero.

`ProjectionUnavailableReason.failure` must be a nonzero known failure code.
`building`, `rebuilding`, and `failure` are the exact three unavailable tags.
The four `QueryProjectionResponse` branches are exhaustive. A ready result's
frontier must exactly equal `data.observed_fence.frontier`; its fence identity
and generation must match the rows and cursor registry. A timed-out result has
a nonzero required sequence. Degraded returns no rows. Invalid exposes only a
closed failure code.

`leading_components` and every row `group` contain at most 1,024 values. The
page contains at most the effective limit and cannot have a cursor when empty.
The service and WP-130 validate schema types, complete group shape, canonical
order, and policy redaction.

`wait_nanos` is a total nanosecond count so the bounded Rust `Duration` maps
without losing subsecond precision. It is at most 30,000,000,000. It must be
zero when `required_sequence` is absent; a present required sequence is
nonzero.

`ProjectionStatus` preserves the service's closed cross-field shapes. The
uninitialized status is `BUILDING` with no published pointer, candidate,
published apply mode, or failure. Initialized states validate the complete
ADR-0017 matrix:

| Lifecycle | Published | Candidate | Apply mode | Failure |
|---|---|---|---|---|
| Uninitialized `BUILDING` | absent | absent | absent | absent |
| Initialized `BUILDING` | absent | present at `BEFORE_FIRST` | absent | absent |
| `CATCHING_UP` | absent | present | absent | absent |
| `READY` | present | absent | `ENABLED` | absent |
| `REBUILDING` | present below candidate | present | enabled or suspended | absent |
| `DEGRADED` | retained failed-state shape | retained failed-state shape | absent without published; otherwise retained, with failed published suspended | present and naming one retained pointer |
| `INVALID` | exact degraded shape | exact degraded shape | exact degraded mode | present and naming one retained pointer |

Pointer generations are nonzero and distinct, no pointer frontier is ahead of
the authoritative head, and published mode is present exactly when published
is present. A failure identifies a retained generation; when `at_sequence` is
present it is the exact successor of that generation's frontier. The checked
API-neutral DTO also proves that an initialized candidate is the highest
allocated generation. That highest value is intentionally not exposed here, so
a context-free public decoder validates only relations represented on the wire;
WP-130 may construct `ProjectionStatus` only from the already checked service
DTO.

`GetProjectionStatusRequest` and `GetProjectionStatusResponse` are support
messages for the API-neutral status operation and later MCP mapping. This ADR
does not add a `GetProjectionStatus` RPC.

### `commit.proto`

The complete commit-query and subscription source is proposed as:

```protobuf
syntax = "proto3";

package riffdb.v1;

import "riffdb/v1/common.proto";
import "riffdb/v1/value.proto";

message EventId {
  uint64 commit_sequence = 1;
  uint32 event_ordinal = 2;
}

message DurableEvent {
  EventId event_id = 1;
  uint32 event_type_id = 2;
  ValueRecord payload = 3;
}

message AffectedEntity {
  bytes entity_key = 1;
  uint64 entity_version = 2;
}

message Commit {
  uint64 commit_sequence = 1;
  bytes admission_request_id = 2;
  string contract_lineage = 3;
  uint64 contract_version = 4;
  uint32 command_id = 5;
  bytes plan_hash = 6;
  bytes canonical_input_hash = 7;
  AdmittedActor actor = 8;
  Timestamp logical_time = 9;
  bytes partition_hash = 10;
  repeated bytes conflict_hashes = 11;
  repeated AffectedEntity affected_entities = 12;
  repeated DurableEvent events = 13;
  DeclaredOutcome outcome = 14;
  string provenance_uri = 15;
  CommandDurability durability = 16;
}

message GetCommitRequest {
  bytes request_id = 1;
  uint64 commit_sequence = 2;
}

message GetCommitResponse {
  oneof result {
    Unit not_found = 1;
    Commit found = 2;
  }
}

message CommitPage {
  repeated Commit items = 1;
  optional bytes next_cursor = 2;
  FrontierPosition observed_fence = 3;
}

message ScanCommitsRequest {
  bytes request_id = 1;
  PageRequest page = 2;
}

message ScanCommitsResponse {
  CommitPage page = 1;
}

enum CommitSubscriptionEndReason {
  COMMIT_SUBSCRIPTION_END_REASON_UNSPECIFIED = 0;
  COMMIT_SUBSCRIPTION_END_REASON_LIFETIME_ELAPSED = 1;
  COMMIT_SUBSCRIPTION_END_REASON_LAGGED = 2;
  COMMIT_SUBSCRIPTION_END_REASON_SCAN_GAP = 3;
  COMMIT_SUBSCRIPTION_END_REASON_POLICY_DENIED = 4;
  COMMIT_SUBSCRIPTION_END_REASON_CANCELLED = 5;
  COMMIT_SUBSCRIPTION_END_REASON_DEADLINE_EXCEEDED = 6;
  COMMIT_SUBSCRIPTION_END_REASON_SERVICE_SHUTDOWN = 7;
  COMMIT_SUBSCRIPTION_END_REASON_UNAVAILABLE = 8;
}

message CommitSubscriptionTerminal {
  CommitSubscriptionEndReason reason = 1;
  FrontierPosition resume_after = 2;
}

message SubscribeCommitsRequest {
  bytes request_id = 1;
  optional uint64 after_sequence = 2;
  uint64 maximum_lifetime_nanos = 3;
}

message CommitNotification {
  oneof notification {
    Commit commit = 1;
    CommitSubscriptionTerminal terminal = 2;
  }
}
```

`EventId` is the semantic pair of nonzero commit sequence and zero-based event
ordinal, not UUID bytes. A durable event's ID sequence equals its containing
commit, and its ordinal equals its position in the commit's event list. Event
type and entity versions are nonzero. Event payloads and outcomes are canonical
policy-filtered records.

`Commit.admission_request_id` and a present actor agent-session ID are exact
UUIDv7 values. The plan, canonical-input, partition, and every conflict hash are
exactly 32 bytes. `provenance_uri` is the canonical ADR-0024 lowercase UUIDv7
resource locator. Logical time is a canonical public `Timestamp` with nanos
less than 1,000,000,000. Durability is synchronous or group.

Conflict hashes, affected entities, and durable events each contain at most
4,096 items, not 1,024. Their order and uniqueness follow the API-neutral
commit view; adapters do not sort or reconstruct protected items. The result
must remain within ADR-0027's response budget even when a durable record could
legally be larger.

Commit pages are contiguous in increasing sequence order, no item exceeds the
required frozen `observed_fence`, and a continuation page retains the first
page's exact fence. An empty page cannot carry a cursor. `after_sequence`, when
present, is nonzero.

`maximum_lifetime_nanos` is a total nanosecond count in
`1..=900,000,000,000`. Each stream item is independently response-bounded. A
terminal notification carries exactly one known reason and the last safely
delivered frontier; it does not reveal an unauthorized next commit.

### `admin.proto`

The complete health, statistics, and capability-administration source is
proposed as:

```protobuf
syntax = "proto3";

package riffdb.v1;

import "riffdb/v1/common.proto";
import "riffdb/v1/value.proto";

message HealthRequest {
  optional bytes request_id = 1;
}

enum PreBootstrapLifecycle {
  PRE_BOOTSTRAP_LIFECYCLE_UNSPECIFIED = 0;
  PRE_BOOTSTRAP_LIFECYCLE_INITIALIZING_VALIDATION = 1;
  PRE_BOOTSTRAP_LIFECYCLE_INITIALIZING_BOOTSTRAP = 2;
}

message PreBootstrapHealth {
  PreBootstrapLifecycle lifecycle = 1;
  bool liveness = 2;
  bool readiness = 3;
}

enum HealthStatus {
  HEALTH_STATUS_UNSPECIFIED = 0;
  HEALTH_STATUS_READY = 1;
  HEALTH_STATUS_NOT_READY = 2;
  HEALTH_STATUS_DEGRADED = 3;
}

enum HealthComponentKind {
  HEALTH_COMPONENT_KIND_UNSPECIFIED = 0;
  HEALTH_COMPONENT_KIND_AUTHORITATIVE_STORAGE = 1;
  HEALTH_COMPONENT_KIND_CATALOG = 2;
  HEALTH_COMPONENT_KIND_COMMIT_COORDINATOR = 3;
  HEALTH_COMPONENT_KIND_PROJECTION = 4;
  HEALTH_COMPONENT_KIND_OUTBOX = 5;
}

enum HealthComponentStatus {
  HEALTH_COMPONENT_STATUS_UNSPECIFIED = 0;
  HEALTH_COMPONENT_STATUS_HEALTHY = 1;
  HEALTH_COMPONENT_STATUS_DEGRADED = 2;
  HEALTH_COMPONENT_STATUS_UNAVAILABLE = 3;
}

message HealthComponent {
  HealthComponentKind component = 1;
  HealthComponentStatus status = 2;
}

message BuildInfo {
  string semantic_version = 1;
  string git_revision = 2;
  string rust_version = 3;
  repeated string enabled_features = 4;
  uint32 storage_format_version = 5;
  uint32 contract_ir_version = 6;
  string mcp_protocol_baseline = 7;
}

message AuthenticatedHealth {
  HealthStatus status = 1;
  optional uint64 active_contract_version = 2;
  optional uint64 last_commit_sequence = 3;
  repeated HealthComponent components = 4;
  Timestamp started_at = 5;
  BuildInfo build = 6;
}

message HealthResponse {
  oneof result {
    PreBootstrapHealth pre_bootstrap = 1;
    AuthenticatedHealth authenticated = 2;
  }
}

message StatsRequest {
  bytes request_id = 1;
}

message StatsResponse {
  uint32 active_cursors = 1;
  uint32 active_commit_subscribers = 2;
  optional uint64 last_commit_sequence = 3;
  optional uint64 pending_outbox_deliveries = 4;
  optional uint32 known_projections = 5;
}

enum CapabilityCreateMode {
  CAPABILITY_CREATE_MODE_UNSPECIFIED = 0;
  CAPABILITY_CREATE_MODE_NORMAL = 1;
  CAPABILITY_CREATE_MODE_BOOTSTRAP = 2;
}

enum CapabilityPermissionKind {
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

message LineageScopedStableId {
  string contract_lineage = 1;
  uint32 stable_id = 2;
}

message CapabilityPermission {
  oneof permission {
    Unit validate_contract = 1;
    Unit read_contract = 2;
    LineageScopedStableId explain_command = 3;
    Unit deploy_contract = 4;
    LineageScopedStableId invoke_command = 5;
    LineageScopedStableId read_entity = 6;
    LineageScopedStableId scan_index = 7;
    LineageScopedStableId query_projection = 8;
    LineageScopedStableId read_projection_status = 9;
    Unit read_commit = 10;
    Unit scan_commits = 11;
    Unit subscribe_commits = 12;
    Unit read_provenance = 13;
    Unit inspect_outbox = 14;
    Unit read_health = 15;
    Unit read_statistics = 16;
    Unit create_capability = 17;
    Unit revoke_capability = 18;
    Unit administer_capabilities = 19;
  }
}

message ScopedPartition {
  string contract_lineage = 1;
  bytes partition_key = 2;
}

message ExplicitPartitionScope {
  repeated ScopedPartition partitions = 1;
}

message PartitionScope {
  oneof scope {
    Unit all = 1;
    ExplicitPartitionScope explicit = 2;
  }
}

message EntityFieldVisibility {
  string contract_lineage = 1;
  uint32 entity_type_id = 2;
  repeated uint32 field_ids = 3;
}

message CapabilityGrant {
  TenantScope tenant_scope = 1;
  PartitionScope partition_scope = 2;
  repeated CapabilityPermission permissions = 3;
  repeated EntityFieldVisibility field_visibility = 4;
  uint32 max_scan_rows = 5;
  repeated CapabilityPermissionKind approval_required = 6;
}

message CreateCapabilityRequest {
  bytes request_id = 1;
  CapabilityCreateMode mode = 2;
  bytes capability_id = 3;
  string principal_id = 4;
  ActorKind actor_kind = 5;
  uint32 requested_lifetime_seconds = 6;
  repeated string audiences = 7;
  CapabilityGrant grant = 8;
}

message CapabilityIdentity {
  bytes capability_id = 1;
  uint64 revision = 2;
}

message CapabilityTransition {
  CapabilityIdentity identity = 1;
  uint64 administration_sequence = 2;
}

message NormalCapabilityCreated {
  CapabilityTransition transition = 1;
  string token = 2;
}

message NormalCreateCapabilityResult {
  oneof result {
    NormalCapabilityCreated created = 1;
    CapabilityIdentity already_created_token_unavailable = 2;
    Unit capability_id_conflict = 3;
  }
}

message BootstrapCreateCapabilityResult {
  oneof result {
    CapabilityTransition created = 1;
    CapabilityTransition replayed = 2;
    Unit bootstrap_conflict = 3;
  }
}

message CreateCapabilityResponse {
  oneof result {
    NormalCreateCapabilityResult normal = 1;
    BootstrapCreateCapabilityResult bootstrap = 2;
  }
}

enum RevocationReason {
  REVOCATION_REASON_UNSPECIFIED = 0;
  REVOCATION_REASON_REQUESTED = 1;
  REVOCATION_REASON_REPLACED = 2;
  REVOCATION_REASON_SUSPECTED_COMPROMISE = 3;
  REVOCATION_REASON_POLICY_CHANGE = 4;
}

message RevokeCapabilityRequest {
  bytes request_id = 1;
  bytes capability_id = 2;
  RevocationReason reason = 3;
}

message RevokeCapabilityResponse {
  oneof result {
    CapabilityTransition revoked = 1;
    CapabilityTransition already_revoked = 2;
    Unit capability_not_found = 3;
  }
}
```

`HealthResponse` is the one unchanged Health RPC's closed discrimination
boundary. `PreBootstrapHealth` has exactly lifecycle, liveness, and readiness;
it cannot co-occur with active-contract detail, commit position, component
inventory, storage identity, policy fact, build/start-time detail, extensible
text, or a map. Its lifecycle is one of the two initialization values and
readiness is false. It is available only through the server's private
pre-marker admission context and performs no policy or storage access and no
durable audit.

`HealthRequest.request_id` is the sole optional outer request ID. Absence is
legal only for the unauthenticated loopback pre-marker route. A valid UUIDv7 is
required for authenticated Health. Request-field absence alone never grants
pre-bootstrap authority: listener, authentication metadata, durable marker
state, lifecycle router, and the private service context all must agree. After
the marker, principal-less Health admission is closed.

Authenticated health has a known nonzero status, optional nonzero active
contract and commit positions, unique components in component-kind order, a
canonical start timestamp, and complete build information. The closed component
kinds are storage, catalog, commit coordinator, projection, and outbox. Build
strings are nonempty ASCII of at most 128 bytes; enabled features contain at
most 64 sorted unique entries; format versions are nonzero. Statistics retain
their fixed five fields. Active cursors are at most 4,096. Active subscribers
fit the service's `u16` bound and are at most 128 even though the wire scalar is
`uint32`.

The permission oneof tags and `CapabilityPermissionKind` values are the same
accepted registry `1..=19`. Unparameterized permissions carry `Unit`.
Explain/invoke-command, read-entity, scan-index, query-projection, and
read-projection-status carry a nonempty lineage plus the appropriate nonzero
stable ID. This closed oneof makes illegal kind/parameter combinations
unrepresentable; fixtures must prove the oneof and enum registries stay aligned.

Capability grants obey these exact bounds and canonical forms:

- tenant and partition scopes are present and closed;
- an explicit partition list is nonempty, canonical, unique, and has at most
  1,024 lineage-scoped complete partition keys;
- permissions are canonical, unique, and contain at most 8,192 atoms;
- each field-visibility entry has a nonempty field set, entries are canonical
  and unique, and both entry count and total field count are at most 65,535;
- `max_scan_rows` is in `1..=500`;
- `approval_required` is sorted, unique, known, and contains at most 19 kinds;
- the complete semantic grant payload is at most 1 MiB.

`CreateCapabilityRequest` contains untrusted caller selections only. Database
ID and environment come from trusted server scope and are not public fields.
The request contains no token, digest, issued or expiry timestamp, revision, or
administration sequence. Principal IDs are nonempty and at most 256 bytes.
Audiences are configured, sorted, unique, nonempty strings of at most 512 bytes;
the list contains `1..=8`. Lifetime is explicitly seconds and lies in
`1..=2,592,000`. Capability ID plus the normalized trusted database/environment,
principal, actor kind, lifetime, audiences, and grant is create replay identity;
the fresh outer request ID is not.

Create mode zero and unknown values reject. Normal create uses ordinary bearer
authentication. Bootstrap is loopback gRPC only, rejects ordinary
authorization metadata, and receives exactly one sensitive
`riffdb-bootstrap-token-bin` metadata value containing the retained 43
canonical ASCII token bytes. No request message contains those bytes.
Bootstrap additionally requires a human target and a grant containing
`ADMINISTER_CAPABILITIES`; no wire mode bypasses those service checks.

Normal create's `created` branch is the only response that contains a token.
Its wire type is `string`, and it is exactly 43 visible ASCII unpadded base64url
characters. `already_created_token_unavailable` returns only capability identity
and revision. Bootstrap created/replayed results never contain or reissue the
token. Request mode and response branch must agree.

Capability IDs are UUIDv7; revisions and administration sequences are nonzero.
Revoke reasons are closed. `capability_id_conflict`, `bootstrap_conflict`, and
`capability_not_found` carry no hidden detail.

### `services.proto`

The complete service source remains exactly five services and 16 RPCs:

```protobuf
syntax = "proto3";

package riffdb.v1;

import "riffdb/v1/admin.proto";
import "riffdb/v1/command.proto";
import "riffdb/v1/commit.proto";
import "riffdb/v1/contract.proto";
import "riffdb/v1/projection.proto";
import "riffdb/v1/query.proto";

service ContractService {
  rpc ValidateContract(ValidateContractRequest) returns (ValidateContractResponse);
  rpc ExplainCommand(ExplainCommandRequest) returns (ExplainCommandResponse);
  rpc DeployContract(DeployContractRequest) returns (DeployContractResponse);
  rpc GetActiveContract(GetActiveContractRequest) returns (GetActiveContractResponse);
}

service CommandService {
  rpc Execute(ExecuteCommandRequest) returns (ExecuteCommandResponse);
  rpc GetOutcome(GetOutcomeRequest) returns (GetOutcomeResponse);
}

service QueryService {
  rpc GetEntity(GetEntityRequest) returns (GetEntityResponse);
  rpc ScanIndex(ScanIndexRequest) returns (ScanIndexResponse);
  rpc QueryProjection(QueryProjectionRequest) returns (QueryProjectionResponse);
}

service CommitService {
  rpc GetCommit(GetCommitRequest) returns (GetCommitResponse);
  rpc ScanCommits(ScanCommitsRequest) returns (ScanCommitsResponse);
  rpc SubscribeCommits(SubscribeCommitsRequest) returns (stream CommitNotification);
}

service AdminService {
  rpc Health(HealthRequest) returns (HealthResponse);
  rpc Stats(StatsRequest) returns (StatsResponse);
  rpc CreateCapability(CreateCapabilityRequest) returns (CreateCapabilityResponse);
  rpc RevokeCapability(RevokeCapabilityRequest) returns (RevokeCapabilityResponse);
}
```

There is no `GetContractVersion`, `GetProjectionStatus`, provenance, outbox,
discovery, or MCP-only gRPC method. Support messages do not imply an RPC.

### Request identities, durations, keys, and tokens

Every authenticated request has `bytes request_id = 1`. It is required and is
exactly 16 network-order UUIDv7 bytes. `HealthRequest` is the sole optional-ID
exception described above. WP-130 validates missing, wrong-length,
wrong-version, and wrong-variant inputs without substitution or repair. A retry
uses a fresh request ID.

The same UUIDv7 rule applies to capability IDs, admission request IDs, present
agent-session IDs, and the existing optional public incident ID. Provenance
resource locators embed the exact canonical lowercase hyphenated UUIDv7.
`Value.uuid_value` remains a business UUID value under ADR-0006 and is not
silently reclassified as a RiffDB system-generated UUIDv7. Cursor tokens are
exactly 16 opaque random bytes and are deliberately not UUIDs. Event IDs are
the sequence/ordinal message above. These rules validate supplied identifiers;
they add no wire-level generation policy or ordering semantics.

Projection wait and commit-subscription lifetime use explicit total nanoseconds
in `uint64`, which maps every accepted bounded Rust `Duration` exactly.
Capability lifetime uses explicit whole seconds in `uint32`, matching its
accepted replay identity. No duration uses an undocumented unit or caller clock.

Canonical entity, index, and partition keys remain opaque bytes at the public
wire boundary but must decode through their typed owner codec. Prefix and group
values use the exact `Value` family because they are selected and validated
against a checked schema. No hash is accepted as authorization evidence in
place of an exact partition key.

The canonical capability token is textual at its normal-create response
boundary and binary gRPC metadata only at bootstrap ingress. It is never a
request-body field, public error detail, log value, metric label, trace field,
or durable response-recovery value.

### Public errors and oversize responses

Business and closed control-plane results are response data in the oneofs above.
API-neutral failures are not added to these response oneofs. WP-130 carries the
existing bounded `riffdb.v1.PublicError` bytes directly in
`grpc-status-details-bin` under ADR-0006, with no `google.rpc.Status`, `Any`, or
second custom envelope.

ADR-0027 `ResponseTooLarge` maps to gRPC `RESOURCE_EXHAUSTED` with static text
and no `PublicError`. WP-127 proves that each supported encoded response is no
larger than the service's conservative response charge. It never truncates a
commit, event, entity, projection row, diagnostic, capability result, or stream
item to fit.

### Invocation claims

The immutable Execute request explicitly has no client-provided actor context
or provenance claims. No accepted source defines public Protobuf field names for
source-repository, source-commit, reason, approval-reference, or agent-session
invocation claims. WP-127 therefore does not invent them in any request.

WP-130 constructs the POC's bounded empty/discarded untrusted-claims value after
authentication. A future public claim carrier or metadata registry requires a
separate reviewed additive protocol decision. It may not change trusted actor
construction, capability authentication, current authorization, redaction, or
provenance admission.

## Options Considered

1. **Domain source files, closed oneofs, and exact units as specified above:**
   proposed; this mirrors the stabilized service DTOs and makes invalid result
   combinations visible to structural validation.
2. **Keep every completed message in `services.proto`:** rejected; it preserves
   unsupported shell source identity but leaves one monolithic ownership file
   and obscures the public semantic modules. The relocation is reviewed before
   any shell becomes supported.
3. **Use enum status plus many optional fields for every response:** rejected;
   it admits incoherent result combinations and repeats the compatibility
   constraint imposed only by the already-frozen Execute envelope.
4. **Duplicate a second journaled result for `GetOutcome`:** rejected;
   `ExecuteCommandResponse` already represents the exact public terminal fields,
   and the wrapper can require `REPLAYED` without a competing shape.
5. **Use `google.protobuf.Duration`, `Timestamp`, `Struct`, or `Empty`:**
   rejected; explicit bounded units, the accepted public timestamp/value family,
   and local `Unit` keep validation and descriptor closure controlled.
6. **Reuse durable storage messages:** rejected; public policy-filtered views and
   authoritative durable records have different ownership and compatibility
   contracts.
7. **Add RPCs for projection status or other service DTO operations:** rejected;
   the accepted POC surface is exactly 16 RPCs. Later MCP support uses the
   shared API-neutral operation, not a privileged storage path.
8. **Carry bootstrap token bytes in `CreateCapabilityRequest`:** rejected; it
   violates ADR-0009's narrow metadata handoff and risks secret propagation.
9. **Infer active selection, before-first frontier, health mode, or capability
   mode from absence/defaults:** rejected; closed oneofs and explicit mode values
   fail closed.

## Consequences

- Every WP-130-supported RPC has a complete bounded message before its adapter
  is implemented.
- Two implementation agents can work on transport and later MCP mapping against
  one frozen semantic field inventory without duplicating public types.
- Result oneofs, explicit frontier variants, and explicit duration units reduce
  invalid cross-field states; validators still must enforce semantic relations.
- `services.proto` becomes stable service inventory rather than a message
  monolith, at the cost of a reviewed descriptor source-file relocation for the
  unsupported shells.
- GetOutcome shares the immutable Execute response, so changes to terminal
  public fields have one compatibility boundary.
- Projection status has reusable public messages but no gRPC RPC in the POC.
- First-party clients must send explicit scan limits and empty-but-present field
  selections where appropriate.
- Complete commit views can exceed the response budget even while valid
  durably; ADR-0027's closed oversize behavior remains explicit.
- Adding a public field, enum value, oneof branch, duration interpretation, or
  claim carrier after acceptance requires compatibility review and regenerated
  fixtures.

## Compatibility

This proposal does not renumber or reinterpret any supported `Value`,
`PublicError`, or Execute field. `EXECUTED_READ_ONLY = 3` is the accepted
additive completion. GetOutcome and all formerly empty shells acquire their
first supported fields before WP-130 exposes them. Empty phase-zero shells were
explicitly not a supported client baseline under ADR-0006.

The existing `PUBLIC_ERROR_KIND_COMMAND_EXECUTION_FAILED = 9`, required
`PublicError.execution_failure = 8`, and arithmetic-fault/resource-limit detail
values remain unchanged and are included in every regenerated descriptor and
compatibility fixture.

The source-file relocation changes descriptor organization and generated source
information, so WP-127 replaces the pre-completion descriptor/schema fixtures
in one reviewed change. The package, message names used by RPCs, service names,
RPC names, and streaming shape remain unchanged. Removed fields in future
versions must reserve their accepted name and number; additive fields must
define presence, bounds, response-charge impact, and unknown-field behavior.

There is no contract grammar, contract IR, plan hash, canonical input hash,
idempotency identity, storage key, durable message, commit ordering, atomicity,
projection persistence, or MCP naming change. No database migration is needed.

## Security

Every request value remains untrusted until WP-130 authenticates metadata,
performs bounded structural conversion, and the shared service authorizes the
exact operation and target. Request bodies never supply trusted actor context,
database/environment scope, authorization decisions, storage capabilities, or
logical time.

The pre-bootstrap Health branch is structurally unable to contain authenticated
detail and is admitted only by private lifecycle authority. Bootstrap is the
sole principal-less mutation, remains loopback-only, and keeps raw token material
out of request and response messages except the one normal-create token release.
Capability results and errors reveal no conflicting stored record.

Pages, commits, entity fields, events, outcomes, projection rows, health detail,
and schemas are produced only after service authorization and redaction. MCP and
gRPC consume the same results; neither adapter may decode a cursor, read storage,
or bypass policy to fill a public message.

All token material is redacted before logs, traces, metrics, diagnostics, panic
messages, and public errors. Malformed identifiers, keys, enums, oneofs, sizes,
and cross-field shapes fail closed with bounded static errors. Public diagnostic
text is compiler-registry owned, and public failure detail remains the accepted
safe error registry.

## Testing

WP-127 freezes and checks:

- a source-info-stripped descriptor inventory and schema hash for every public
  message, enum, oneof, service, RPC, field number, type, and presence label;
- golden bytes for every closed oneof/result branch, enum value, absent-versus-
  present optional scalar, empty Unit branch, page/fence shape, and Execute
  status combination;
- language-neutral client request/response fixtures for all 16 RPCs, including
  every nontrivial result branch and the sole server-streaming notification;
- generated artifact reproducibility through `scripts/generate-proto --check`;
- UUIDv7 golden vectors plus wrong length, version, and variant for each public
  shared-identifier field class, without applying UUIDv7 rules to cursors or
  business UUID values;
- hash, cursor, key, name, source, diagnostic, schema, field-selection,
  projection-component, capability-grant, and collection boundary fixtures;
- exact 1,024 projection component and 4,096 commit collection boundaries;
- zero, maximum, and one-over duration fixtures for 30-second waits,
  900-second subscriptions, and 2,592,000-second capability lifetimes;
- canonical diagnostic registry and span fixtures for syntax and semantic
  failures;
- page item limits, empty-page cursor rejection, and index/commit/projection
  fence consistency;
- all four projection query variants, every lifecycle/apply/failure/unavailable
  tag, uninitialized status, and the complete lifecycle cross-field matrix;
- Health's two exclusive variants, exact three-field pre-bootstrap descriptor,
  optional request-ID routing shape, and rejection of mixed detail;
- all normal/bootstrap create and revoke results, mode/result mismatch, grant
  canonicality, token response text, and proof that no request has a token field;
- parser/preflight fuzzing for malformed tags, lengths, recursion, duplicates,
  unknown enum values, unset oneofs, and oversized repeated fields;
- actual encoded length no greater than ADR-0027 service charge for every
  checked response-charge fixture, including exact-ceiling, one-over, and an
  indivisible oversized commit or stream item.

WP-130 adds total service-to-Protobuf conversion tests, direct PublicError
status-detail conformance, authentication metadata tests, request-ID
non-substitution, capability bootstrap metadata tests, projection stub mapping,
stream termination, client decode failures, and child-process restart evidence.
Architecture tests prove the adapter depends on the shared service rather than
policy, commit coordination, or storage implementations.

## Requirements and Work Packages

- **Requirements:** `API-001`, `VAL-003`, `POC-006`
- **Defines or blocks:** `WP-127` and `WP-130`
- **Consumed later by:** `WP-140`, `WP-150`, `WP-190`, and `WP-200`
- **Final evidence:** `WP-200`

## Decision Deadline

The exact text must be accepted before WP-127 changes a public source, generated
descriptor, schema hash, or compatibility fixture. WP-127 stops after public
messages and structural validation. WP-130 may begin service conversion only
after WP-127 is merged and must not invent, renumber, or reinterpret a public
field while implementing Tonic adapters.
