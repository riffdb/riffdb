# ADR-0040: Public gRPC MCP Parity Bridge

- **Status:** Proposed
- **Direction approved:** No
- **Exact text accepted:** No
- **Requires:** ADR-0005, ADR-0006, ADR-0007, ADR-0009, ADR-0013, ADR-0018,
  ADR-0020, ADR-0024, ADR-0026, ADR-0027, ADR-0028, ADR-0037, Proposed
  ADR-0008
- **Paired proposal:** ADR-0041 defines the CLI-consumed WP-137 client surface;
  WP-137 requires both records, but ADR-0041 does not become a prerequisite for
  deciding this public bridge
- **Would amend:** ADR-0006 and ADR-0028's exact 16-RPC inventory; the
  public/service/client portions of ADR-0007 and ADR-0026 for conditional
  discovery and outcome-locator resolution; ADR-0009's exact credential-
  delivery owner rows; and ADR-0037's exact public-client dependency allowlist
- **Decision deadline:** Before WP-137 edits a public `.proto`, descriptor,
  generated fixture, gRPC service, or public SDK retry surface

This proposal is not authoritative. The current 16-RPC inventory remains in
force until a human accepts an exact amendment and the authoritative SPEC and
work-package registry are reconciled in the same governance change.

## Context

The API-neutral service has 22 closed operations across six object-safe traits.
The accepted public protocol has five services and 16 RPCs. The six operations
without an RPC are:

- `GetContractVersion`
- `GetProjectionStatus`
- `TraceProvenance`
- `ListPendingOutboxDeliveries`
- `DiscoverCommandTools`
- `DiscoverResources`

ADR-0006 and ADR-0028 deliberately froze the smaller inventory and explicitly
said those RPCs do not exist. Proposed ADR-0008 and SPEC Section 12.2 also require
production MCP stdio to be a normal public gRPC client. With only 16 RPCs, stdio
cannot implement the required projection status, provenance, outbox, historical
contract-resource, or policy-filtered discovery behavior without an in-process
service shortcut or a transport mismatch. Those alternatives violate the shared
service boundary.

WP-150 exposes a second public-client gap. Its bootstrap and normal capability-
create uncertainty flows need operation-specific retry/disposition support, but
the Rust client's retry classification is private and only Execute currently has
a public retry helper. WP-150's allowed paths exclude the client crate, so that
support must be supplied upstream rather than reimplemented in the CLI.

## Proposed Decision

### Formal WP-137 bridge

Add a formal **WP-137: Public gRPC MCP parity bridge** after WP-130 and before
both WP-140 and WP-150. It depends on WP-130; WP-140 and WP-150 each depend on
WP-137 without removing any existing dependency. WP-137 is a P1 gate member,
alongside WP-130 and WP-150; it is not retroactively part of P0. Its narrow
purpose is to expose the six existing API-neutral operations through the
existing public protocol and to publish exact operation-specific SDK helpers
needed by stdio and WP-150.

WP-137 may edit only the reviewed public protocol, proto owner, gRPC adapter,
Rust client, generated compatibility fixtures/tests, generation scripts, and
the exact service/server files enumerated below. Those narrow files implement
only conditional discovery, the command-outcome discovery identity, and the
ADR-0008 checked outcome-locator flow. WP-137 may not change storage, policy
rules, runtime, commit ordering, required authorization safe points, redaction,
business semantics, contract language, or durable formats.

### Exact service placement

The package remains `riffdb.v1`; no `v2` package or sixth gRPC service is added.
The five service names remain unchanged and gain exactly these unary RPCs:

```protobuf
service ContractService {
  // Existing four RPCs remain in their current order.
  rpc GetContractVersion(GetContractVersionRequest)
      returns (GetContractVersionResponse);
  rpc DiscoverCommandTools(DiscoverCommandToolsRequest)
      returns (DiscoverCommandToolsResponse);
  rpc DiscoverResources(DiscoverResourcesRequest)
      returns (DiscoverResourcesResponse);
}

service QueryService {
  // Existing three RPCs remain in their current order.
  rpc GetProjectionStatus(GetProjectionStatusRequest)
      returns (GetProjectionStatusResponse);
}

service CommitService {
  // Existing three RPCs remain in their current order.
  rpc TraceProvenance(TraceProvenanceRequest)
      returns (TraceProvenanceResponse);
}

service AdminService {
  // Existing four RPCs remain in their current order.
  rpc ListPendingOutboxDeliveries(ListPendingOutboxDeliveriesRequest)
      returns (ListPendingOutboxDeliveriesResponse);
}
```

`CommandService` is unchanged. The resulting inventory is exactly five services
and 22 RPCs, with `SubscribeCommits` still the sole server-streaming RPC. These
methods invoke the identically named existing service operations; there is no
generic resource read, arbitrary payload, MCP-only semantic operation, raw
storage access, or alternate health/bootstrap method.

### Exact additive public source and tag registry

Acceptance of this record accepts the following complete additive source and
field registry. WP-137 must implement it byte-for-byte; it may not defer tag,
presence, name, oneof, or RPC-placement choices to an implementation PR.

`proto/riffdb/v1/contract.proto` appends exactly:

```protobuf
message GetContractVersionRequest {
  bytes request_id = 1;
  string contract_lineage = 2;
  uint64 contract_version = 3;
}

message GetContractVersionResponse {
  oneof result {
    Unit not_found = 1;
    ContractDescriptor found = 2;
  }
}
```

The request lineage is a checked nonempty `ContractLineage`, the version is
nonzero, and `found` must equal both requested lineage and version. The result
requires exactly one known branch.

`proto/riffdb/v1/projection.proto` is byte-for-byte unchanged. Its already
accepted support messages retain these exact tags and presence:

```protobuf
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

`proto/riffdb/v1/commit.proto` appends exactly:

```protobuf
message ProvenanceSelection {
  oneof selection {
    uint64 commit_sequence = 1;
    bytes provenance_id = 2;
  }
}

message ProvenanceClaims {
  optional string source_repository = 1;
  optional string source_commit = 2;
  optional string reason = 3;
  optional string approval_id = 4;
}

message Provenance {
  bytes provenance_id = 1;
  uint64 commit_sequence = 2;
  bytes admission_request_id = 3;
  string contract_lineage = 4;
  uint64 contract_version = 5;
  uint32 command_id = 6;
  bytes plan_hash = 7;
  AdmittedActor actor = 8;
  Timestamp logical_time = 9;
  uint32 outcome_id = 10;
  repeated AffectedEntity affected_entities = 11;
  repeated EventId event_ids = 12;
  ProvenanceClaims claims = 13;
}

message TraceProvenanceRequest {
  bytes request_id = 1;
  ProvenanceSelection selector = 2;
}

message TraceProvenanceResponse {
  oneof result {
    Unit not_found = 1;
    Provenance found = 2;
  }
}
```

The selector and response result each require exactly one known oneof branch.
Commit sequences, contract versions, command IDs, outcome IDs, and entity
versions are nonzero. `provenance_id` is exactly 16-byte UUIDv7 data;
`admission_request_id` is exactly 16-byte UUIDv7 data; and `plan_hash` is exactly
32 bytes. `claims` is a required message even when all four approved claims are
absent. Claim strings retain their type-owned bounds and validation: repository
and reason are nonempty bounded UTF-8, while source commit and approval ID are
nonempty bounded visible ASCII. Affected entities and event links each contain
at most 4,096 entries and retain service order. Conversion never reconstructs,
sorts, or expands a policy-filtered provenance view.
For `found`, a commit selector must equal `Provenance.commit_sequence` and a
provenance selector must equal `Provenance.provenance_id`; both identities are
still returned so the relation is independently checkable.

`proto/riffdb/v1/admin.proto` adds
`import "riffdb/v1/commit.proto";` after its existing imports and appends
exactly:

```protobuf
enum OutboxDeliveryState {
  OUTBOX_DELIVERY_STATE_UNSPECIFIED = 0;
  OUTBOX_DELIVERY_STATE_PENDING = 1;
  OUTBOX_DELIVERY_STATE_RETRY_SCHEDULED = 2;
  OUTBOX_DELIVERY_STATE_DELIVERING = 3;
  OUTBOX_DELIVERY_STATE_DEAD_LETTER = 4;
}

message OutboxDeliverySummary {
  EventId event_id = 1;
  OutboxDeliveryState state = 2;
  uint32 attempts = 3;
  Timestamp next_attempt_at = 4;
}

message OutboxDeliveryPage {
  repeated OutboxDeliverySummary items = 1;
  optional bytes next_cursor = 2;
}

message ListPendingOutboxDeliveriesRequest {
  bytes request_id = 1;
  PageRequest page = 2;
}

message ListPendingOutboxDeliveriesResponse {
  OutboxDeliveryPage page = 1;
}
```

`next_attempt_at` uses ordinary message presence and is absent when the service
DTO contains no timestamp; it is not a scalar sentinel. State zero and unknown
states reject. The outbox page deliberately has no consistency-fence field:
the API-neutral type is `Page<OutboxDeliverySummary, ()>`. Items retain strict
increasing `EventId` order, are bounded by the effective nonzero page limit, and
an empty page cannot carry `next_cursor`. The cursor is exactly 16 opaque bytes.
No event payload, connector identity, error text, endpoint, credential, or
partition key is added.
The request `page` and response `page` messages are required; their absence is
not a default-page or empty-page spelling.

WP-137 creates `proto/riffdb/v1/discovery.proto` with exactly this complete
source:

```protobuf
syntax = "proto3";

package riffdb.v1;

import "riffdb/v1/common.proto";
import "riffdb/v1/contract.proto";

enum FixedToolKind {
  FIXED_TOOL_KIND_UNSPECIFIED = 0;
  FIXED_TOOL_KIND_VALIDATE_CONTRACT = 1;
  FIXED_TOOL_KIND_GET_ACTIVE_CONTRACT = 2;
  FIXED_TOOL_KIND_EXPLAIN_COMMAND = 3;
  FIXED_TOOL_KIND_DEPLOY_CONTRACT = 4;
  FIXED_TOOL_KIND_RESOLVE_COMMAND_OUTCOME = 5;
  FIXED_TOOL_KIND_GET_ENTITY = 6;
  FIXED_TOOL_KIND_SCAN_INDEX = 7;
  FIXED_TOOL_KIND_GET_COMMIT = 8;
  FIXED_TOOL_KIND_SCAN_COMMITS = 9;
  FIXED_TOOL_KIND_TRACE_PROVENANCE = 10;
  FIXED_TOOL_KIND_QUERY_PROJECTION = 11;
  FIXED_TOOL_KIND_GET_PROJECTION_STATUS = 12;
  FIXED_TOOL_KIND_LIST_PENDING_OUTBOX_DELIVERIES = 13;
  FIXED_TOOL_KIND_GET_HEALTH = 14;
}

message CommandToolDescriptor {
  string tool_name = 1;
  string source_command = 2;
  string contract_lineage = 3;
  uint64 contract_version = 4;
  uint32 command_id = 5;
  GeneratedSchemaArtifact input_schema = 6;
  GeneratedSchemaArtifact outcome_schema = 7;
}

message CommandToolDiscoveryItem {
  oneof item {
    FixedToolKind fixed_tool = 1;
    CommandToolDescriptor command_tool = 2;
  }
}

message OperationSchemaArtifact {
  string schema_id = 1;
  string dialect = 2;
  bytes schema_hash = 3;
  string canonical_json = 4;
}

message OperationSchemaCatalog {
  OperationSchemaArtifact command_operation_envelope = 1;
  OperationSchemaArtifact command_get_outcome_result = 2;
}

message ActiveDiscoveryCatalogFence {
  string contract_lineage = 1;
  uint64 contract_version = 2;
  bytes bundle_hash = 3;
}

message DiscoveryCatalogFence {
  oneof state {
    Unit no_active_contract = 1;
    ActiveDiscoveryCatalogFence active_contract = 2;
  }
}

message CommandToolDiscoveryPage {
  repeated CommandToolDiscoveryItem items = 1;
  optional bytes next_cursor = 2;
  DiscoveryCatalogFence observed_fence = 3;
  OperationSchemaCatalog operation_schemas = 4;
}

message DiscoverCommandToolsRequest {
  bytes request_id = 1;
  PageRequest page = 2;
  DiscoveryCatalogFence prior_fence = 3;
}

message DiscoverCommandToolsResponse {
  oneof result {
    DiscoveryCatalogFence catalog_unchanged = 1;
    CommandToolDiscoveryPage page = 2;
  }
}

message ContractVersionResource {
  string contract_lineage = 1;
  uint64 contract_version = 2;
}

message EntitySchemaResource {
  string contract_lineage = 1;
  uint32 entity_type_id = 2;
  GeneratedSchemaArtifact schema = 3;
}

message CommandResource {
  string contract_lineage = 1;
  uint32 command_id = 2;
  uint64 contract_version = 3;
  string source_command = 4;
}

message CommandOutcomeResource {
  string contract_lineage = 1;
  uint32 command_id = 2;
  string tool_name = 3;
}

message CommitResource {
  oneof target {
    Unit class_template = 1;
    uint64 commit_sequence = 2;
  }
}

message ProvenanceResource {
  oneof target {
    Unit class_template = 1;
    bytes provenance_id = 2;
  }
}

message ProjectionStatusResource {
  string contract_lineage = 1;
  uint32 projection_id = 2;
}

message ResourceDescriptor {
  oneof resource {
    Unit active_contract = 1;
    ContractVersionResource contract_version = 2;
    EntitySchemaResource entity_schema = 3;
    CommandResource command_plan = 4;
    CommandResource command_documentation = 5;
    CommandOutcomeResource command_outcome = 6;
    CommitResource commit = 7;
    ProvenanceResource provenance = 8;
    ProjectionStatusResource projection_status = 9;
    Unit server_health = 10;
  }
}

message ResourceDiscoveryPage {
  repeated ResourceDescriptor items = 1;
  optional bytes next_cursor = 2;
  DiscoveryCatalogFence observed_fence = 3;
}

message DiscoverResourcesRequest {
  bytes request_id = 1;
  PageRequest page = 2;
  DiscoveryCatalogFence prior_fence = 3;
}

message DiscoverResourcesResponse {
  oneof result {
    DiscoveryCatalogFence catalog_unchanged = 1;
    ResourceDiscoveryPage page = 2;
  }
}
```

`FixedToolKind` tags `1..=14` are the exact existing presentation order, not a
permission or service-operation registry. A fixed discovery item requires a
known nonzero enum. A command item carries the compiler-owned ADR-0020 tool name
verbatim, the exact source command, and input/outcome artifacts whose keys are
respectively `CommandInput(command_id)` and
`CommandOutcomeUnion(command_id)`. Versions and stable IDs are nonzero and the
bundle hash is exactly 32 bytes.

The shared service owns the two immutable exact schema sources:

```text
crates/riffdb-service/schema/riffdb.command-operation-envelope-v1.schema.json
crates/riffdb-service/schema/riffdb.command-get-outcome-result-v1.schema.json
```

Their exact `schema_id` values are, respectively,
`riffdb.command-operation-envelope/v1` and
`riffdb.command-get-outcome-result/v1`. `dialect` is exactly
`https://json-schema.org/draft/2020-12/schema`; `canonical_json` is the exact
bounded canonical UTF-8 source; and `schema_hash` is exactly 32 bytes and must
equal the accepted `SchemaHash` of those bytes. Both artifact messages and the
catalog message are required. Every `CommandToolDiscoveryPage`, including an
empty page and every continuation, carries the same complete catalog in tag 4.
`catalog_unchanged` has no page and therefore carries no schema catalog.

The corresponding API-neutral `OperationSchemaArtifact` and
`OperationSchemaCatalog` have private fields and checked constructors. A
`DiscoverCommandToolsResult::Page` carries both its command-tool page and the
complete catalog; `CatalogUnchanged` carries only the equal discovery fence.
HTTP consumes this service DTO directly and stdio receives its identical public
conversion. WP-140 owns one common MCP schema composer in `riffdb-api-mcp` and
both transports call it; neither transport copies, reconstructs, or embeds a
second schema source.

The schema hashes are deliberately not part of `DiscoveryCatalogFence`. These
v1 sources are immutable: any semantic schema revision requires a new versioned
schema ID and separately reviewed field/catalog evolution. MCP sessions do not
retain this catalog across an incompatible server transition or restart.

Every discovery page requires one known `DiscoveryCatalogFence.state` branch,
including empty pages. `no_active_contract` is an explicit `Unit`, not absent
fence data. An active fence has exact lineage/version/bundle identity. A present
cursor is exactly 16 opaque bytes, the page is bounded by the effective limit,
and an empty page cannot carry a cursor. Tool items retain fixed-then-command
canonical order. Resource items retain their service-owned canonical identity
order and are unique.

`prior_fence` uses ordinary message presence and is legal only when the page
request has no cursor. The request `page` message is always required. Each
discovery response requires exactly one known result
branch. When a supplied checked prior fence equals the transaction-current
catalog fence, the service still performs the accepted invocation lifecycle:
current initial discovery authorization, any required durable `started` append,
bounded permit acquisition, fresh exact-facts authorization at the synchronous
acceptance safe point, and complete response validation and bounds. When a
`started` record was required, the one matching terminal `succeeded` record is
appended before release; an unaudited standard read does not fabricate either
record. Only after that fresh authorization may the service return
`catalog_unchanged` carrying that exact fence without
materializing candidates, items, visibility masks, a page, or a cursor. When the
fence differs, the service returns the ordinary first `page` under the current
fence. A response `catalog_unchanged` must equal the request's present
`prior_fence`; it is forbidden when the request omitted that field. A `page` is
forbidden to repeat the supplied stale fence as its observed fence.
Continuation requests omit `prior_fence` and remain governed by their
server-side cursor fence.

`catalog_unchanged` proves only equality of public catalog identity. It does not
prove that the caller previously observed the fence, that two discovery
operations produced the same view, or that a policy-visible inventory is equal,
and it grants no invocation or read authority. The service retains no session
observation state. A fabricated, cross-operation, or different-credential prior
fence may therefore receive this branch but gains no inventory or authority
from it; ordinary gRPC callers must not infer more.

Only an MCP transport may retain its already materialized visible fingerprints
across `catalog_unchanged`, and only under all POC invariants together: stdio
loads one redacted `BearerCredential` once and never reloads or switches it;
HTTP binds the exact `CapabilityId` to the session; `CapabilityGrantV1` is
immutable after creation; revocation or expiry makes fresh authentication fail
and terminates the session; and the accepted visibility policy is static for
that session's lifetime. The adapter must itself know that the prior fence came
from the same operation's previously completed observation. A visibility-policy
implementation change terminates affected sessions. Mutable grants or mutable
policy are forbidden from reusing this optimization until an accepted design
adds an authorization/visibility epoch to the public fence. Every
authentication, reauthorization, audience, or transport failure discards the
pending refresh and retained inference rather than emitting from it.

This conditional read is the public support for MCP list-change/resource-update
watchers. WP-140 may issue it at its accepted five-second watcher interval for
the bounded 900-second session lifetime. It is not one operation's progress
poll loop, does not inherit the 32-observation/30-second progress-poll ceiling,
and never keeps a service invocation, cursor, storage transaction, or catalog
proof alive between calls.

The corresponding API-neutral shapes are exact: each discovery request owns
`PageRequest` plus `Option<DiscoveryCatalogFence>`. Resource discovery becomes
the closed `CatalogUnchanged(DiscoveryCatalogFence) |
Page(Page<ResourceDescriptor, DiscoveryCatalogFence>)` enum. Command discovery
becomes `CatalogUnchanged(DiscoveryCatalogFence) | Page { page:
Page<CommandToolDiscoveryItem, DiscoveryCatalogFence>, operation_schemas:
OperationSchemaCatalog }`. Their fields remain private and constructors enforce
the cursor/prior-fence exclusion and required schema catalog. This is
conditional evaluation of the same two operations, not two new service-
operation or audit tags.

Every `ResourceDescriptor.resource`, `CommitResource.target`, and
`ProvenanceResource.target` requires exactly one known branch. The oneof itself
is the resource-kind registry; no parallel enum is added. Exact commit and
provenance targets remain representable because the closed API-neutral DTO
supports them, while current discovery normally emits their `class_template`
branches. Entity schema artifacts must be keyed to their exact entity type.
Both command-plan and command-documentation descriptors are self-contained. The
service proves lineage, exact contract version, stable command ID, and exact
source command from one active-bundle snapshot before constructing either
descriptor. Their API-neutral constructors are exactly
`ResourceDescriptor::command_plan(lineage, contract_version, command_id,
source_command)` and
`ResourceDescriptor::command_documentation(lineage, contract_version,
command_id, source_command)`, with private fields and a checked `SourceName`;
independent strings are not accepted. This lets either MCP transport resolve
the stable-ID-only URI through exact-version ExplainCommand without an
adapter-side command-tool join or then-current active-version substitution.

The command-outcome descriptor is deliberately distinct from the other command
targets: the service proves lineage, stable command ID, and compiler-owned tool
name from one active-bundle snapshot before construction so either MCP transport
can format the ADR-0008 template without an adapter-side discovery join. It is
discoverability metadata only and cannot mint or resolve an actual outcome
locator; minting remains tied to a resolved durable historical identity and
terminal result. The API-neutral constructor becomes exactly
`ResourceDescriptor::command_outcome(lineage, command_id, tool_name)` with a
private `McpCommandToolNameV1`; an independent string is not accepted.
Descriptors otherwise carry semantic identity only;
preformatted MCP URI text is forbidden and remains solely ADR-0008's
presentation responsibility.

`proto/riffdb/v1/services.proto` adds the discovery import and appends methods
to each existing service in exactly this descriptor order:

```protobuf
import "riffdb/v1/discovery.proto";

service ContractService {
  // Existing four methods remain first and unchanged.
  rpc GetContractVersion(GetContractVersionRequest)
      returns (GetContractVersionResponse);
  rpc DiscoverCommandTools(DiscoverCommandToolsRequest)
      returns (DiscoverCommandToolsResponse);
  rpc DiscoverResources(DiscoverResourcesRequest)
      returns (DiscoverResourcesResponse);
}

service QueryService {
  // Existing three methods remain first and unchanged.
  rpc GetProjectionStatus(GetProjectionStatusRequest)
      returns (GetProjectionStatusResponse);
}

service CommitService {
  // Existing three methods remain first and unchanged.
  rpc TraceProvenance(TraceProvenanceRequest)
      returns (TraceProvenanceResponse);
}

service AdminService {
  // Existing four methods remain first and unchanged.
  rpc ListPendingOutboxDeliveries(ListPendingOutboxDeliveriesRequest)
      returns (ListPendingOutboxDeliveriesResponse);
}
```

`CommandService` is unchanged. Every new request has required
`bytes request_id = 1`; existing Health remains the sole optional-ID exception.
Existing identifier, name, hash, cursor, page, request, response, collection,
and semantic presence bounds from ADR-0027/ADR-0028 apply. Result and selector
oneofs require exactly one known branch. Public unknown fields remain ignored
and never relayed. WP-137 owns structural/wire validation and exchange
validation; its gRPC conversion remains the sole total service-to-wire owner.

### Outcome-resource companion fields

Acceptance of this record together with ADR-0008 freezes two additive changes
without adding another RPC:

1. `ExecuteCommandResponse` gains
   `optional string outcome_uri = 9`, present only for committed/replayed durable
   command results after authorized locator minting and absent for read-only
   results.
2. `GetOutcomeRequest` gains `optional string outcome_uri = 5`. When present,
   legacy `contract_lineage = 2`, `command_name = 3`, and
   `idempotency_key = 4` are required to be empty; when absent, their existing
   nonempty raw-key semantics are unchanged. An older server therefore rejects
   rather than misinterprets a locator request.

The additive response field preserves ordinary public compatibility. Generic
`PublicMessage` and public-client validation accept an absent `outcome_uri` on a
`COMMITTED` or `REPLAYED` response from a pre-WP-137 server. When the field is
present, they require its canonical public shape and its consistency with the
completion status; `EXECUTED_READ_ONLY` always requires absence. This
legacy-optional rule is not permission for a WP-137 server to omit the field.
WP-137 server conversion requires a canonical locator bound to the exact
terminal identity on every `COMMITTED` and `REPLAYED` result and fails closed
before release if the service result lacks one. A found raw-key GetOutcome
result from a WP-137 server carries the locator minted from the matched durable
digest evidence. A found locator GetOutcome result carries a byte-for-byte equal
canonical URI to the request. Operation-specific exchange validation retains
the existing rule that every GetOutcome `found` response has `REPLAYED` status
and validates the locator relations whenever the optional field is present.

WP-140 may not turn legacy absence into an MCP fallback. Stdio completes an
initial full `DiscoverCommandTools` and `DiscoverResources` pass before
returning a policy-filtered tool/resource inventory or accepting a command-tool
invocation. The first successful response from either WP-137-added RPC
establishes the new-surface probe; both inventories must still complete before
use. After that probe, stdio requires `outcome_uri` on every durable
Execute/GetOutcome result and treats absence as an upstream protocol-conformance
failure. HTTP is composed with the same WP-137 server in process and applies the
same strict rule. Neither adapter reconstructs a locator from other response
fields or falls back to a raw idempotency key.

The API-neutral `ResolveCommandOutcomeRequest` gains a private-field checked
selector with exactly two variants:

```text
RawKey { lineage, source_command, idempotency_key }
Locator(OutcomeResourceLocator)
```

`OutcomeResourceLocator` is service-owned and accepts only ADR-0008's one canonical
URI containing owner principal, lineage, stable command ID, compiler-owned tool
name, digest scheme, digest key ID, and digest bytes. The gRPC and MCP adapters
can request checked parsing and can pass the resulting value to the service, but
cannot construct a value from independent fields, extract a raw idempotency key,
mint a locator, compute an HMAC, or form a storage key. The same
`ResolveCommandOutcome` service-operation and audit tag, ADR-0026 initial
existence-blind authorization, durable-fact lookup, terminal authorization, and
the accepted single terminal audit-record lifecycle apply to both selectors.

The RawKey branch retains the accepted active-catalog lineage/source-command
resolution before initial authorization. The Locator branch instead uses its
already checked lineage/stable command ID for that existence-blind initial
authorization, requires its owner principal to equal the authenticated
principal, and does not substitute the active contract. Only after a durable
match does it load the exact version/bundle/plan named by returned authoritative
facts and check the compiler-owned tool name before terminal authorization.

The authoritative outcome port retains its one
`reserve_read_outcome`/`read_outcome` operation and its existing
`AuthoritativeOutcomeRequest -> Option<AuthoritativeOutcomeSnapshot>` signature.
`AuthoritativeOutcomeRequest` becomes a fields-private checked wrapper over
exactly two service-owned selector variants:

```text
RawKeyOutcomeLookup {
  lineage, command_id, principal_id, tenant_scope, idempotency_key
}
DigestedOutcomeLookup {
  lineage, command_id, principal_id, tenant_scope,
  digest_scheme, digest_key_id, digest
}
```

`OutcomeLocatorDigestEvidence` is a fields-private service DTO containing the
checked exact-v1 digest scheme, nonzero `DigestKeyId`, and 32 digest bytes. It has
no raw-key, principal, tenant, lineage, command, database, or environment
accessor and is redacted under `Debug`. Every `AuthoritativeOutcomeFacts` embeds
exactly one such evidence value next to its existing durable identity facts.
The adapter may construct it only after proving that the matched durable
`IdempotencyIdentity` has the same database and environment as trusted process
composition and the same tenant, principal, lineage, and command components as
the lower request. This placement makes the
evidence inseparable from every returned Pending, Journaled, or ExecutionFailed
snapshot and requires no source change to `crates/riffdb-service/src/ports.rs`.

The raw-key path continues to calculate the bounded current-plus-readable HMAC
candidates and call `AdmissionRepository::lookup_admission`. On a single match,
the read adapter copies the digest evidence from the matched durable state's
complete `IdempotencyIdentity` into `AuthoritativeOutcomeFacts`; absence returns
no snapshot, and multiple matches remain integrity-fatal. The service uses the
embedded evidence to mint the canonical locator only after terminal disclosure
authorization. Execute obtains the same evidence directly from the committed or
replayed `StoredOutcomeV1` identity. A journaled service result therefore carries
one checked locator; a read-only result never does.

The digested path accepts no raw key. `riffdb-server` validates the exact v1
scheme/key-ID pair against the value-only
`ReadableIdempotencyDigestInventory` retained from startup configuration,
joins trusted process `DatabaseId` and `Environment` plus the service-resolved
tenant/principal/lineage/command components, constructs one complete
`IdempotencyIdentity`, wraps exactly that identity as the one bounded lookup
candidate, and calls the same `AdmissionRepository::lookup_admission` point
lookup. It then requires the stored identity to equal the reconstructed identity
before embedding evidence in `AuthoritativeOutcomeFacts` and returning the
snapshot. The service loads the returned state's exact historical plan and
requires its lineage, stable command ID, and
compiler-owned tool name to equal the locator before terminal authorization or
release; the URI does not cause active-catalog substitution. After successful
initial authorization, a locator-principal mismatch, unreadable key ID, absent
identity, or locator lineage/command/tool mismatch returns the same `NotFound`
result without indicating which check failed. Malformed locator syntax fails
public validation before lookup. Malformed trusted inventory, multiple matches,
stored-key/embedded-identity mismatch, missing historical state for an existing
admission, or impossible stored shape is integrity, not a scan or fallback. No
adapter receives key material, performs HMAC, scans storage, searches by digest
alone, or bypasses either authorization phase.

The value-only readable inventory is cloned into
`ServerAuthoritativeReadPort` by production composition. The narrowly approved
implementation paths are
`crates/riffdb-service/src/dto.rs`,
`crates/riffdb-service/src/command_operations.rs`,
`crates/riffdb-service/src/response.rs`,
`crates/riffdb-server/src/read_adapters.rs`, and
`crates/riffdb-server/src/process_graph.rs`, plus their existing colocated/unit
and integration tests. `crates/riffdb-service/src/ports.rs` is intentionally not
an allowed path: the DTO extension preserves its existing method signature. Any
need to alter a storage key, repository trait,
durable record, HMAC framing, digest provider, policy rule, or operation/audit
tag is a stop-and-review conflict, not implied scope.

The new Execute field is operation-envelope metadata outside the existing
declared `outcome` `Value`. WP-140 mechanically composes the service-owned
`riffdb.command-operation-envelope/v1` artifact with each bundle's exact
compiler-owned command input and outcome-union artifacts. The fixed GetOutcome
tool uses the separate invocation-independent
`riffdb.command-get-outcome-result/v1` artifact. `outcome_uri` is therefore
schema-valid structured content and may additionally appear as a standard
resource-link content item.

Neither operation artifact is added to a compiled bundle. WP-137 makes no
compiler, IR, bundle-schema-artifact, bundle-encoding, bundle-hash, plan-hash,
or discovery-fence hash change. A parallel per-command operation schema, a
compiler-owned copy, a transport-owned copy, or independent HTTP/stdio
composition is forbidden.

### Public Rust client surface

The Rust client adds the exact unary methods `get_contract_version`,
`discover_command_tools`, `discover_resources`, `get_projection_status`,
`trace_provenance`, and `list_pending_outbox_deliveries`. Each takes its exact
`riffdb_proto::v1` request plus `&CallMetadata`, returns its exact checked
`riffdb_proto::v1` response or `ClientError`, runs outbound and inbound
`PublicMessage` validation, and runs operation-specific exchange validation
where a request constrains response identity, page, or fence. MCP stdio imports
only these public methods and the existing public client types.

Because `ClientError::Public(PublicError)` is already part of the SDK signature,
WP-137 makes the checked owner nameable without a second direct dependency by
adding exactly these re-exports at the SDK root:

```rust
pub use riffdb_errors::{
    ErrorClass, PublicError, PublicErrorDetails, PublicErrorKind, RecoveryAction,
    ValidationCode, ValidationIssue, ValidationIssues, ValidationPath,
    ValidationPathSegment,
};
```

This is a re-export of the single ADR-0037 owner, not an SDK error copy or wire
view. `ClientError::public_error()` continues to return `Option<&PublicError>`;
no peer text, Tonic status, retry boolean, or lossy SDK-local classification is
made public as authority.

WP-137 also owns the one presentation-only protected normal-credential loader
consumed by MCP stdio and WP-150. It creates
`crates/riffdb-client-rust/src/credential_file.rs` and re-exports exactly:

```rust
pub fn load_protected_bearer_credential(
    path: &std::path::Path,
) -> Result<BearerCredential, BearerCredentialFileError>;
```

`BearerCredentialFileError` is the closed, public, copyable, redaction-safe enum
with exactly `UnsupportedPlatform`, `ProtectedFileRejected`, and
`InvalidPresentation`. The loader returns the existing redacted
`BearerCredential`, exposes no credential text/bytes or auth-owned type, and
implements Proposed ADR-0041's exact bounded Linux protected-file procedure,
non-Linux rejection, presentation-only validation, and temporary-buffer
zeroization. Authentication and canonical token decoding remain server-owned.
`BearerCredential` gains exactly this sole non-exposing comparison:

```rust
#[must_use]
pub fn has_same_presentation(&self, other: &BearerCredential) -> bool;
```

It compares the complete checked authorization presentation for exact equality,
returns only a Boolean, and makes no constant-time authentication claim. There
is no raw accessor, file-reader callback, decoded token, path-bearing error, or
second loader.

Acceptance applies Proposed ADR-0041's exact companion dependency amendments.
ADR-0009's complete direct first-party owner set becomes `riffdb-auth` and
`riffdb-cli` for exact `base64 = 0.22.1`, and `riffdb-auth`,
`riffdb-client-rust`, and `riffdb-cli` for exact `zeroize = 1.8.1`; their
accepted default-feature and feature rows do not otherwise change. ADR-0037's
exact `riffdb-client-rust` allowlist gains only
`zeroize = { version = "=1.8.1", default-features = false, features =
["alloc"] }`. The public client gains no direct `base64` or `riffdb-auth`
dependency, and WP-137 adds no `riffdb-auth` path.

The three and only three automatic retry helpers are:

```rust
pub async fn execute_with_retry(
    &mut self,
    command: &IdempotentCommand,
    attempt_budget: AttemptBudget,
    metadata: &CallMetadata,
) -> Result<v1::ExecuteCommandResponse, ClientError>;

pub async fn create_capability_with_retry(
    &mut self,
    create: &NormalCapabilityCreateTemplate,
    attempt_budget: AttemptBudget,
    metadata: &CallMetadata,
) -> Result<v1::CreateCapabilityResponse, ClientError>;

pub async fn create_bootstrap_capability_with_retry(
    &mut self,
    create: &BootstrapCapabilityCreateTemplate,
    attempt_budget: AttemptBudget,
    metadata: &BootstrapCallMetadata,
) -> Result<v1::CreateCapabilityResponse, ClientError>;
```

The existing Execute signature is unchanged. The two new template types have
private fields, redacted `Debug`, no credential field, and a checked constructor
from `v1::CreateCapabilityRequest`. A template requires an empty `request_id`,
the exact Normal or Bootstrap mode named by its Rust type, and every other field
to pass a proto-owned template preflight applying every existing
CreateCapability rule except the deliberately absent request ID. The closed
local error is exact:

```rust
pub enum CapabilityCreateTemplateError {
    NonemptyRequestId,
    WrongMode,
    InvalidBody,
}
```

The constructor never repairs an input. This creates no second capability
request schema.

Before every submission, including the first, each helper obtains a fresh
UUIDv7 `RequestId`, clones the retained semantic body, and changes only
`request_id`. Normal retry retains the same `CapabilityId` and normalized body.
Bootstrap retry additionally reapplies the same referenced
`BootstrapCallMetadata`, whose already persisted token remains owned and
redacted by the existing credential type; the request body never contains it.
The returned response is the existing checked create response. In particular,
`already_created_token_unavailable` remains an `Ok` terminal result after an
uncertain normal-create success; the client never fabricates, caches, or
reissues a token.

The helpers reuse the existing checked retry state only after their template
establishes a stable replay identity. They may resubmit on a checked public
`RecoveryAction::Retry` or `ResolveWithSameIdempotencyKey`, or a locally proven
`DetailsFreeStatus::TransportUnavailable`; they do not infer safety from status
text. Once any submission carries uncertainty, exhaustion, a later
non-resubmittable error, or request-ID-source failure returns the existing
`ClientError::OutcomeUnknown(OutcomeUnknown)`. Its documentation is broadened
from command-only wording to an idempotent operation whose durable result still
requires resolution; its type and variant remain source compatible. Exhaustion
without prior uncertainty returns the final checked error.

There is no generic retry callback, closure-taking retry method, public
status-only retry predicate, arbitrary-RPC wrapper, default attempt budget,
sleep, jitter, or automatic retry for deploy, revoke, administrative reads,
streams, or non-idempotent operations. WP-150 consumes these exact helpers and
does not duplicate transport classification.

### WP-137 ownership and acceptance

The proposed hard dependency is `WP-130`. Required ADRs are ADR-0005, ADR-0006,
ADR-0007, ADR-0009, ADR-0013, ADR-0018, ADR-0020, ADR-0024, ADR-0026, ADR-0027,
ADR-0028, ADR-0037, and accepted exact text for ADR-0008, ADR-0040, and ADR-0041.
WP-137 is added to the P1 gate. Both WP-140 and WP-150 gain a hard dependency on
WP-137. The exact allowed paths are:

```text
proto/riffdb/v1/**
crates/riffdb-proto/**
crates/riffdb-api-grpc/**
crates/riffdb-client-rust/**
crates/riffdb-service/schema/riffdb.command-operation-envelope-v1.schema.json
crates/riffdb-service/schema/riffdb.command-get-outcome-result-v1.schema.json
crates/riffdb-service/src/dto.rs
crates/riffdb-service/src/command_operations.rs
crates/riffdb-service/src/query_discovery_operations.rs
crates/riffdb-service/src/response.rs
crates/riffdb-server/src/read_adapters.rs
crates/riffdb-server/src/process_graph.rs
tests/grpc/**
tests/service/**
fixtures/proto/**
fuzz/Cargo.toml
fuzz/Cargo.lock
fuzz/fuzz_targets/proto_public.rs
scripts/generate-proto
Cargo.lock
```

`work_packages.yaml` must enumerate those six exact service/server source files
and two exact service schema files rather than granting crate-wide exceptions.
Colocated unit tests in those same source files and the two exact integration-
test directories above are allowed. Policy, storage, idempotency, commit,
compiler, IR, auth, and MCP crate paths remain forbidden. The required WP-137
implementation-path evidence must name exactly `Cargo.lock`,
`crates/riffdb-client-rust/Cargo.toml`,
`crates/riffdb-client-rust/src/credential_file.rs`,
`crates/riffdb-client-rust/src/lib.rs`,
`crates/riffdb-client-rust/src/metadata.rs`, and
`crates/riffdb-client-rust/tests/credential_file.rs`; no `riffdb-auth` path is
added.

`fixtures/proto/operation-schema-catalog-v1.txt` freezes the two schema IDs in
field order, canonical byte lengths, and exact hashes. `generate-proto --check`
recomputes those values from the two service-owned sources and verifies the
checked-in fixture. This fixture ownership does not authorize a compiler or
bundle edit.

The package acceptance commands are proposed as:

```text
cargo test -p riffdb-proto -p riffdb-api-grpc -p riffdb-client-rust
cargo test -p riffdb-api-grpc --features server,client --test grpc_end_to_end
cargo test -p riffdb-service -p riffdb-server
./scripts/generate-proto --check
cargo +nightly-2026-07-12 fuzz run proto_public -- -max_len=4194304 -max_total_time=30
cargo fmt --all -- --check
cargo clippy -p riffdb-proto -p riffdb-api-grpc -p riffdb-client-rust \
  -p riffdb-service -p riffdb-server --all-targets --all-features -- -D warnings
cargo doc -p riffdb-proto -p riffdb-api-grpc -p riffdb-client-rust -p riffdb-service -p riffdb-server --no-deps
```

The exit gate is exact descriptor and wire reproducibility, total conversion of
the six existing operations, public-client round trips for all six, raw-key and
locator outcome parity through one point-lookup path, operation-specific retry
evidence, and architecture proof that stdio needs no private/server dependency.

### Layering and authentication

All six gRPC methods use the existing interceptor, request bounds, checked
credential handoff, RequestContext construction, exact PublicError details,
deadline/cancellation handling, service response accounting, and total mapping.
They call only the application-service trait. The gRPC adapter does not import
policy, catalog, runtime, commit, storage API, or a concrete storage engine.

Discovery is policy-filtered by the service but does not grant invocation.
Provenance, outbox, projection, contract, and resource results have already had
current obligations applied before conversion. Stdio repeats authorization by
calling the target RPC for every actual invocation/resource read; it does not
treat a discovery response as a capability.

## Options Considered

1. **Add exactly six RPCs to the existing five services:** proposed; it exposes
   the already closed service inventory and lets stdio remain a normal client.
2. **Add a sixth DiscoveryService:** rejected; it changes the accepted five-
   service organization without necessity.
3. **Run stdio in process against `riffdb-service`:** rejected; it violates the
   production transport boundary and would hide public-protocol gaps.
4. **Have stdio call HTTP MCP or have HTTP call gRPC in process:** rejected;
   either stacks transports or creates asymmetric authentication/lifecycle paths.
5. **Omit the six operations from stdio:** rejected; it violates MCP-010 parity
   and leaves required fixed tools/resources unreachable.
6. **Add a generic resource/read RPC:** rejected; it broadens semantics and
   weakens typed operation-specific authorization.
7. **Let WP-150 copy private retry classification:** rejected; retry identity and
   uncertainty behavior belong to the public client and would drift by caller.

## Consequences

- The public service inventory changes additively from 16 to 22 RPCs; existing
  method names, fields, and streaming shapes remain intact.
- WP-140 can implement stdio entirely through public gRPC with parity to HTTP.
- WP-150 receives exact capability-create uncertainty helpers without widening
  its allowed paths or implementing transport policy.
- Public descriptors, client code, fixtures, and compatibility hashes change and
  must be regenerated atomically.
- Six additional request/response surfaces and the two exact outcome-locator
  fields become public compatibility commitments.
- The service owns two immutable operation-schema sources, and command-tool
  discovery carries their exact versioned IDs, bytes, and hashes.
- The public client gains one protected presentation-only credential loader and
  one non-exposing presentation comparison shared by stdio and WP-150.

## Compatibility

This proposal explicitly amends, rather than silently reinterprets, ADR-0006
and ADR-0028's five-service/16-RPC freeze and their statements that the six RPCs
do not exist. The package remains `riffdb.v1`, so the additions follow its
additive compatibility policy. Existing messages and fields are not renumbered
or reinterpreted. Removed future fields reserve both number and name.

Acceptance requires a same-change reconciliation of SPEC Sections 11.1 through
11.3, 12.2, 12.5 through 12.6, 19.4, and 22.1; `work_packages.yaml` must add
WP-137, its dependency edges, allowed paths, requirements, ADRs, deliverables,
acceptance commands, and gate placement. WP-140 must depend on WP-137. WP-150
must also depend on WP-137 and consume its public helpers rather than gaining
client-internal paths. P1 must add WP-137 to its required set.

The same reconciliation expressly amends ADR-0009's two dependency-owner rows
and credential-delivery wording and ADR-0037's client allowlist exactly as
listed above, and records ADR-0005, ADR-0009, ADR-0018, ADR-0020, ADR-0037,
ADR-0040, and ADR-0041 in WP-137's required ADRs. No other dependency owner,
feature, auth exception, or client-to-auth edge is implied.

No durable Protobuf message, storage envelope/key, contract grammar/IR, plan
hash, canonical input hash, idempotency identity, commit sequence, atomicity, or
projection storage changes. The outcome locator presents an existing
digest identity but does not alter its durable encoding.

## Security

The bridge exposes only service-filtered results. Every method authenticates,
authorizes, audits, reauthorizes where required, applies obligations, and maps
only safe public errors through existing code paths. Discovery is not cached as
positive authority. No new principal-less operation is added and bootstrap is
still never exposed through MCP.

Provenance claims and entity schemas are bounded and already redacted. Outbox
summaries contain no payload. Cursor bytes, outcome locators, principal IDs, raw
keys, tokens, claims, and internal failures remain out of telemetry. Malformed,
unknown, oversized, stale, status-inconsistent, and authorization-inconsistent
wire values fail closed.

## Testing

WP-137 must provide:

- source-info-stripped descriptor and schema-hash fixtures proving exactly five
  services, 22 RPCs, six new unary shapes, and one total streaming RPC;
- language-neutral request/response goldens for every new result branch, enum,
  optional field, selector, resource kind, catalog fence, and page boundary;
- compatibility checks proving every pre-WP-137 supported descriptor symbol,
  field number/type/presence, enum value, RPC name, and streaming shape remains;
- deterministic generation checks and a clean `scripts/generate-proto --check`;
- malformed/unknown/duplicate/oversized wire and preflight fuzz cases;
- total service-to-wire conversion tests for all six operations, including every
  projection lifecycle, provenance selector, outbox state, discovery kind,
  empty/no-active catalog, operation-schema catalog, cursor, redaction, and
  response-budget boundary;
- command-plan/documentation resource fixtures proving lineage/version/command/
  source binding comes from one catalog fence, and command-outcome resource
  fixtures proving lineage/command/tool-name binding comes from one fence, with
  no adapter-side tool/resource join;
- conditional-discovery goldens for request field 3 and response branches 1/2,
  prior-fence-plus-cursor rejection, exact `catalog_unchanged` equality, stale-
  to-first-page transition, continuation omission, and active/absent/active
  changes;
- deterministic adapter/service schedules proving each `CatalogUnchanged`
  observation repeats transport authentication, initial authorization, required
  audit-obligation handling, bounded permit, fresh exact-facts authorization,
  and response bounds; an audited schedule proves one `started`/terminal
  `succeeded` pair and an unaudited schedule proves neither record is fabricated,
  while item materialization and cursor-registry counts remain zero, including
  180 independent observations representing one maximum-lifetime MCP session;
- gRPC authentication, exact PublicError, deadline, cancellation, stale-policy,
  and cursor invalidation conformance;
- client/server round trips proving stdio-required operations need no internal
  crate or service shortcut;
- `fixtures/proto/operation-schema-catalog-v1.txt` and mechanical per-command
  composition fixtures proving exact schema IDs/order/length/hash, required
  catalog presence on every page, strict absence on `catalog_unchanged`, and
  locator schema validity without changing a compiler artifact or bundle hash;
- protected-credential loader and `has_same_presentation` tests required by
  ADR-0041, plus architecture tests proving no public-client `riffdb-auth` or
  `base64` dependency and only the exact reviewed `zeroize` edge;
- operation-specific execute/bootstrap/normal-create retry schedules, fresh
  RequestId call counts, retained identity/body/token rules, uncertain response,
  token-unavailable result, and budget exhaustion; and
- architecture checks forbidding authority-bearing dependencies and generic
  arbitrary-RPC retry APIs.

Fixtures additionally cover the exact outcome field tags, raw-versus-locator
branch exclusivity, 50-character digest component, complete canonical locator,
minted-result presence rules, old-client ignore behavior, ordinary new-client
acceptance of legacy server omission, WP-137 server mandatory emission, MCP
strict failure after its new-RPC probe, raw-key matched-digest evidence,
evidence-in-facts construction for every snapshot variant, unchanged
`ports.rs`, unreadable inventory IDs,
principal/tenant/lineage/command/tool/digest mismatch, absence without scanning,
stored-identity equality, and both ADR-0026 authorization phases.

WP-140 adds MCP transport parity and protocol fixtures. WP-150 adds CLI-level
capability uncertainty evidence. WP-200 supplies final cross-transport proof.

## Requirements and Work Packages

- **Requirements:** `API-001`, `ID-005`, `MCP-001`, `MCP-010`, `MCP-011`, `MCP-030`
  through `MCP-033`, `MCP-040`, `MCP-041`, `MCP-043`, `MCP-045`, `MCP-046`,
  `MCP-047`, `MCP-048`, `POC-001`, `POC-004`, `POC-007`, and `POC-008`
- **Defines:** proposed P1 `WP-137`, depending on `WP-130`
- **Blocks:** `WP-140` and `WP-150`
- **Final evidence:** `WP-200`

## Decision Deadline

The exact six-RPC inventory, field/tag registry, service-owned operation-schema
catalog, outcome-locator companion, protected-credential API, and operation-
specific SDK retry surface must be accepted before WP-137 changes any public
source or generated artifact. WP-140 cannot implement production stdio until
WP-137 merges. Acceptance must include authoritative SPEC/work-package,
ADR-0006/ADR-0028, ADR-0009/ADR-0037, and ADR-0041 reconciliation; merging this
Proposed file alone changes no authority.
