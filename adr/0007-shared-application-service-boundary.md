# ADR-0007: Shared Application-Service Boundary

- **Status:** Accepted
- **Amended by:** ADR-0021 for the exact service-audit target registry,
  lineage scoping, canonical order, and per-operation construction rule
- **Direction approved:** 2026-07-12
- **Exact text accepted:** Yes
- **Accepted:** 2026-07-13
- **Requires:** ADR-0004, ADR-0009, ADR-0012, and ADR-0017 accepted before or
  in the same governance commit
- **Amends:** SPEC Sections 4.1, 5.2, 9.1, 9.2, 9.5, 12.11,
  13.1, 13.4, 17.3, 19.4, 19.5, 22.1, and 22.2; work-package dependencies,
  required ADRs, allowed paths, gates, and deliverables; ADR-0004's
  administration transition; and ADR-0012's admitted context
- **Clarifies:** SPEC Sections 4.1, 5.2, 9.1, 11, 12, 13, 15.3, and 19.4
- **Decision deadline:** Before WP-100 publishes executor ports or WP-120 publishes service traits

The human maintainer accepted this exact shared-service record on 2026-07-13 as
part of the atomic semantic-interface governance batch.

## Context

RiffDB has one semantic operation path. gRPC, MCP HTTP, the MCP stdio bridge,
the CLI, the Rust SDK, and the comparison application must not independently
reimplement plan selection, authorization, idempotency, command execution,
redaction, or consistency behavior. A transport that calls storage, catalog,
runtime, policy, or the commit coordinator around the shared service would be a
privileged alternate path even when it happens to return the same result in one
test.

This record resolves three ownership gaps before WP-100 and WP-120 proceed:

1. The SPEC Section 4.1 component table describes the application service as
   owning idempotency lookup, lock acquisition, runtime invocation, and commit
   submission, while the WP-100 objective assigns admission, locking,
   evaluation, revalidation, and commit orchestration to `riffdb-commit`. This
   record establishes that the service owns the API-level workflow and delegates
   the complete post-authorization command lifecycle to a lower executor owned
   by WP-100. The reconciled SPEC and manifest use that boundary.
2. WP-120 precedes WP-160, WP-170, and WP-180 but must expose outbox,
   projection, statistics, and health operations. Depending directly on those
   later crates would create a cycle; inventing their core records in WP-120
   would steal semantic ownership.
3. MCP-046 requires every mutating MCP invocation and administrative read to be
   recorded in the audit stream. WP-110 mentions audit hooks and the storage
   specification has an audit table, but the authoritative manifest does not
   assign the API-neutral emission rule, coordinator append port, complete
   durable attempt record, or schema sequencing to executable packages. Accepted
   ADR-0004 supplies the compatible lower append transition; this record and the
   reconciled manifest assign the remaining owners together. A service-only
   in-memory hook cannot satisfy the requirement.

The accepted ADR-0006 also fixes five public gRPC services and 16 RPC names but
leaves most request and response messages as unsupported phase-zero shells.
Service DTOs must stabilize before the proto owner fills those shells; generated
Protobuf types cannot become the service's semantic model.

## Decision

### Layering and acyclic dependency direction

`riffdb-service` owns API-neutral orchestration. It depends downward on
foundational types and errors, contract IR/compiler/catalog readers,
authentication and policy entry points, the WP-100 executor ports, and bounded
consumer-owned semantic read ports. It never owns a storage engine, storage
transaction, conflict lease, idempotency record, raw capability record, or
protocol object.

The production dependency direction is:

```text
types / errors / contract IR
             |
auth -> policy
             |
catalog / semantic readers / commit executor
             |
        riffdb-service
          /         \
   gRPC adapter   MCP HTTP adapter
        |
 CLI / SDK / production MCP stdio bridge
```

The diagram shows call direction, not permission for every lower crate to depend
on every peer. In particular:

- `riffdb-commit` does not depend on `riffdb-service`, Protobuf, Tonic, rmcp, or
  an API adapter.
- `riffdb-service` may consume only narrow read/executor traits. It does not
  receive a general `StorageEngine`, `CoordinatorWriteTransaction`, redb handle,
  or arbitrary callback.
- `riffdb-projection`, `riffdb-outbox`, and `riffdb-observability` retain their
  existing lower-level ownership and do not depend on `riffdb-service` merely to
  implement a service trait. WP-185 supplies mechanical newtype adapters around
  their handles for consumer-owned service ports.
- API adapters may call the credential-authentication entry point and
  `riffdb-service`. They do not call a policy evaluator, catalog, runtime,
  idempotency component, commit executor, projection store, outbox store, or
  storage API directly.
- `riffdb-server` composes implementations and adapters. It adds no policy,
  redaction, command, query, cursor, or audit semantics.
- Production CLI and SDK calls use public gRPC. Production MCP stdio is a gRPC
  client. Only MCP HTTP is in-process in `riffdbd`, and it still calls the same
  application service object.

An in-process service handle is an internal Rust interface and a test/comparison
surface, not a supported embedded-database API. WP-125 may use it. Production
callers outside `riffdbd` use public protocols.

### Authentication and request context

Authentication and authorization are separate operations:

1. A transport adapter enforces envelope and credential-size limits, extracts
   the credential from the protocol-defined location, supplies a trusted
   configured audience and database/environment context, and calls the one
   `riffdb-auth` authentication entry point.
2. Successful authentication returns a privately constructible
   `AuthenticatedPrincipal`. It identifies the current capability/principal
   reference and contains no raw token or digest-key material.
3. The adapter constructs a checked service `RequestContext` and invokes exactly
   one operation-specific application-service method.
4. The service validates the semantic request, derives operation facts, and
   calls the current `riffdb-policy` authorization entry point. Authentication
   never implies authorization.

ADR-0009 bootstrap is the only principal-less exception. The trusted loopback
gRPC adapter may construct a privately checked `BootstrapRequestContext` only
after the auth-owned bootstrap preparation has validated and consumed the raw
credential. It contains the request ID, trusted gRPC ingress, request control,
and checked digest candidates, but no authenticated principal, raw credential,
caller provenance claims, or general authorization decision. Only the bootstrap
mode of `AdministrationApplication::create_capability` accepts it; all other
service methods require the ordinary `RequestContext`. This adds neither a
second RPC nor a direct storage path.

Missing or malformed credentials may fail in the adapter as the generic
unauthenticated transport result allowed by ADR-0006. An authenticated principal
whose operation is denied receives the generic API-neutral authorization error.
No adapter may construct an authenticated principal, an allow decision, or a
validated approval from request fields. Test-only construction lives in
`riffdb-testkit` or a `cfg(test)` provider and cannot be enabled in a production
feature graph.

`riffdb-service` owns `RequestContext`. It contains exactly these semantic
categories:

- One validated UUIDv7 `RequestId`, identifying this transport submission as
  required by ADR-0005.
- One `AuthenticatedPrincipal` from `riffdb-auth`.
- One closed trusted ingress kind: gRPC `0x01`, MCP HTTP `0x02`, or in-process
  test/comparison `0x03`. CLI, SDK, and production MCP stdio arrive as gRPC.
- Optional bounded untrusted actor claims: `AgentSessionId`, source repository,
  source commit, reason, and approval reference.
- One nonserializable `RequestControl` carrying an absolute monotonic deadline
  and cancellation signal.
- Optional bounded trace propagation data used only by trusted telemetry code.

`RequestContext` never contains raw authorization metadata, an MCP request ID,
Tonic extensions, HTTP headers, source addresses, proto messages, storage
handles, a caller-supplied tenant assertion, or a caller-constructed
`ActorContext`. The service derives the trusted `ActorContext` used for
provenance from the authenticated principal plus only those claims that current
policy validates. Unvalidated claims are ignored or rejected according to
policy. They are never persisted in the POC durable service audit; any separate
security telemetry remains bounded, redacted, and non-authoritative.

The v1 claim bounds are 512 UTF-8 bytes for source repository, 128 visible-ASCII
bytes for source commit, 1,024 UTF-8 bytes for reason, and 256 visible-ASCII
bytes for an approval reference. Empty supplied strings, invalid encoding, or
over-limit values reject before policy evaluation. Free-form values are never
metric labels or public error text.

The `RequestControl` deadline is outside deterministic command time. It may
cancel waits and work before the non-retroactive admission boundaries below; it
never supplies `tx.time`, durable timestamps, or command-visible time. Dropping
an adapter future is not itself proof that a submitted authoritative operation
was aborted.

### Closed service operation inventory

`riffdb-service` owns the closed operation inventory and its semantic
classification. `riffdb-types` owns the value-only `ServiceOperationV1` tag type
because policy, service, commit, storage, and durable encoding all use it. The
same typed value is used consistently for policy facts, telemetry, cursor
binding, and security-audit records:

| Tag | Operation | Class |
|---:|---|---|
| `0x01` | Validate contract | Read-only compute |
| `0x02` | Explain command | Read-only compute |
| `0x03` | Deploy contract | Control-plane mutation |
| `0x04` | Get active contract | Metadata read |
| `0x05` | Get contract version | Metadata read |
| `0x06` | Execute command | Command mutation or classified command read |
| `0x07` | Resolve command outcome | Application-data read |
| `0x08` | Get entity | Application-data read |
| `0x09` | Scan index | Application-data scan |
| `0x0a` | Query projection | Derived-data scan/wait |
| `0x0b` | Get projection status | Metadata read |
| `0x0c` | Get commit | Administrative read |
| `0x0d` | Scan commits | Administrative read |
| `0x0e` | Subscribe to commits | Administrative stream |
| `0x0f` | Trace provenance | Administrative read |
| `0x10` | Get health | Metadata read |
| `0x11` | Get statistics | Administrative read |
| `0x12` | Create capability | Control-plane mutation |
| `0x13` | Revoke capability | Control-plane mutation |
| `0x14` | List pending outbox deliveries | Administrative read |
| `0x15` | Discover command tools | Policy-filtered discovery |
| `0x16` | Discover resources | Policy-filtered discovery |

Zero and unknown tags fail closed. Adding an operation requires a service
compatibility review, a policy mapping, an audit classification, bounds, and
adapter parity evidence. Discovery filtering is not an authorization safe point
for later invocation and does not emit one denial event for every omitted item.
These tags identify service operations; they are not capability-permission tags
from ADR-0009 and code never compares the two numeric registries directly.
`riffdb-policy` owns an exhaustive typed mapping from each service operation and
its resolved facts to the required permission atom(s).

The API-neutral Rust surface is split into object-safe, `Send + Sync`
operation-specific traits so adapters can depend on only the coherent surface
they expose:

| Service trait | Operations |
|---|---|
| `ContractApplication` | validate, explain, deploy, get active, get version |
| `CommandApplication` | execute, resolve outcome |
| `QueryApplication` | get entity, scan index, query projection, projection status |
| `CommitApplication` | get commit, scan commits, subscribe, trace provenance |
| `AdministrationApplication` | health, statistics, capability create/revoke, outbox status |
| `DiscoveryApplication` | visible command descriptors and resource descriptors |

An aggregate `ApplicationService` handle may expose those six trait objects, but
it adds no catch-all operation accepting an enum, arbitrary payload, closure, or
storage key. Operation methods accept a `RequestContext` and their one checked
request DTO. There is no generic read, generic mutation, SQL, or raw admin call.

The five public gRPC services map their fixed 16 RPCs to the corresponding
methods above. Get-contract-version, projection-status, provenance, outbox, and
discovery operations are service operations needed by MCP or composition but do
not silently add a v0.1 gRPC RPC. Adding a public RPC remains an ADR-0006
proto-owner compatibility change. Protocol inventories may differ; an operation
exposed by two transports must have identical service semantics.

### DTO and interface ownership

Semantic types have one owner:

| Owner | Types or interfaces |
|---|---|
| `riffdb-types` | Stable IDs, versions, canonical values, timestamps, keys, hashes, bounded provenance-claim components, and shared service-audit tags, targets, and result links |
| `riffdb-errors` | `PublicError`, incident IDs, and safe recovery classifications |
| `riffdb-contract-ir` | Checked bundles, plans, schemas, compatibility reports, and plan identities |
| `riffdb-auth` | Raw-credential wrapper, `AuthenticatedPrincipal`, authentication entry point, and narrow synchronous initial-authentication clock interface |
| `riffdb-policy` | Operation facts, decisions, obligations, validated approval/provenance claims, current-policy authorizer, value-only authorized capability-mutation preparation and `TransactionCurrentCapabilityFacts`, synchronous `AuthorizationClock`, and pure `TransactionCurrentCapabilityVerifier` |
| `riffdb-commit` | Command admission/execution result port, typed control-plane executor, ordered audit executor, and synchronous `AdministrationClock` |
| `riffdb-catalog` | Catalog readers, deployment candidate, and active-version semantics |
| `riffdb-storage-api` | Durable records, lower bounded semantic readers, and the narrow checked durable-record `proto_codec`; never public service DTOs or a port handed directly to the service |
| `riffdb-service` | Request context/control, operation requests/results, pages/cursors, sanitized summaries, discovery descriptors, authoritative/late-subsystem consumer ports, and the six service traits |
| `riffdb-proto` | Public/durable Protobuf messages, envelopes, descriptors, wire-structural validation, and foundational value/error conversions; no storage API dependency or runtime semantics |

Service request and response DTOs use domain types and checked constructors.
They do not derive Prost messages, contain `prost_types`, expose Tonic/rmcp/redb
types, or double as durable records. They are not serialized by core crates.
Adapters perform explicit total conversions and reject values that cannot be
represented exactly.

Every operation returns either its typed service result or a closed
`ServiceFailure`: one public-safe `PublicError`, cancellation before the relevant
admission boundary, or an elapsed request deadline. Declared command outcomes
and projection `Ready`, `WaitTimedOut`, `Degraded`, and `Invalid` states are
data, not `ServiceFailure`. Authentication failure normally precedes the service
call. Internal sources are retained in trusted tracing under an incident ID and
are not stored in a DTO.

The service execute result is a closed enum. `Journaled` contains a committed or
replayed status, original `CommitSequence`, exact contract version and
`PlanHash`, declared outcome identity and canonical value, `ProvenanceId`, and
durability mode. It contains a typed provenance ID rather than a transport URI;
gRPC and MCP render the public resource link. A replay returns the original
terminal fields and allocates no new application sequence. `ReadOnlyExecuted`
contains only the exact contract version/plan hash and declared outcome identity/
canonical value. It has no replay flag, sequence, provenance ID, durability mode,
or durable-recovery promise.

For a new durable admission, provenance ownership is deliberately split by
lifecycle rather than duplicated:

1. `riffdb-types` owns the bounded, value-only source-repository, source-commit,
   reason, approval-reference, and agent-session component types. Construction
   proves syntax and size only; it does not make a caller claim trusted.
2. `riffdb-policy` validates the untrusted request claims for one exact operation
   and returns a privately constructible `AuthorizedProvenanceClaims`. It contains
   only approved source repository, source commit, reason, and validated approval
   identity, never raw credentials, token data, transport metadata, or an agent
   session. A validated agent session is stored once in ADR-0012's
   `AdmittedActorContext` and is not duplicated here.
3. `riffdb-service` may pass that value only with the matching authorized new-
   admission preparation. The service does not persist it, place it in a cursor,
   or replace stored claims during a retry.
4. `riffdb-commit` owns new-versus-existing admission selection. For a new
   admission it converts the authorized value into the storage-owned
   `StoredAdmittedProvenanceClaimsV1`; for a pending or terminal identity it
   ignores replacement claims and uses the exact stored snapshot.
5. `riffdb-storage-api` owns `StoredAdmittedProvenanceClaimsV1` as a nested part
   of the pending/terminal admission record. `riffdb-storage-memory` and
   `riffdb-storage-redb` persist it, and `riffdb-proto` owns its durable encoding.
   It contains exactly optional approved source repository, source commit, reason,
   and validated `ApprovalId` values and never repeats actor or agent-session data.
6. On successful application commit, the coordinator copies the stored snapshot
   into the policy-approved command provenance record in the same atomic record
   set. ADR-0012 `ExecutionFailed` retains it with the terminal non-commit
   admission but creates no command provenance record.

The exact ADR-0012 `AdmittedActorContext` remains separately frozen and is the
only actor value visible to deterministic runtime. Source repository, source
commit, reason, approval, current capability, and retry-session claims never
enter `TransactionContext` or ADR-0004's runtime `EvaluatedCommand`. The
coordinator combines that runtime result with the exact stored admission into the
final self-contained `CommitIntent`. A retry's current principal/capability and
approval are still evaluated for current authorization and recorded in that
retry's service audit, without overwriting either admitted snapshot.

### Command preparation and WP-100 executor boundary

The accepted resolution of the SPEC/WP-100 ownership tension is:

- `riffdb-service` owns request-level orchestration through semantic validation,
  exact plan preparation, policy authorization, audit admission, invocation of
  the executor, obligation application, and final safe result.
- `riffdb-commit` owns the lower command admission and execution port and the
  complete lifecycle after an authorized preparation is accepted: pending
  reservation creation/resolution, idempotency recheck, conflict acquisition,
  bounded snapshot materialization, deterministic runtime evaluation,
  dependency revalidation, sequence assignment, atomic commit, lease release,
  and commit notification.

The executor port is not a `CommitIntent` submission API exposed to adapters.
It accepts a checked, value-only command preparation containing the exact
ADR-0004 `ExecutablePlanRef` (lineage, contract version, bundle hash, command ID,
and command plan hash), schema-normalized canonical input, caller idempotency
identity material, policy-resolved actor and tenant/partition context, request
ID, and bounded request control. It contains no raw token, policy engine,
transport value, storage transaction, or caller-supplied `ActorContext`.

After bounded snapshot materialization, the executor supplies deterministic
runtime only the exact checked plan, snapshot, and ADR-0012 transaction context.
Runtime returns ADR-0004's storage-API-owned `EvaluatedCommand`, containing the
dependencies, mutations, events, and declared outcome but no stored provenance
claims. The coordinator then combines that exact value with the immutable stored
admission envelope and plan-derived partition/conflict evidence to construct the
final self-contained `CommitIntent`. The service cannot construct or submit
either value, and the coordinator cannot edit the evaluated mutations, events,
or outcome while adding admission metadata.

The preparation carries the current submission's `RequestId` for tracing/audit.
Only a newly created admission freezes that value as ADR-0005/ADR-0012
`admission_request_id`; every retry uses a fresh outer request ID and never
overwrites the stored original. The idempotency key and canonical command input,
not the request ID, remain unchanged for mutation recovery.

Preparation uses a bounded inspect/confirm loop, at most three observations:

1. The service asks the idempotency executor for an existing pending or terminal
   plan identity without creating a reservation. Absence selects the requested
   active/explicit plan; presence selects the exact stored historical plan.
2. The service loads that exact checked plan from the catalog, normalizes input
   under its schema as required by ADR-0013, derives partition and operation
   facts, and authorizes with current policy.
3. The service submits the authorized preparation. The coordinator rechecks the
   observed idempotency state. If concurrent state now points at another valid
   historical plan, it returns `PreparationChanged` without evaluation and the
   service repeats from step 1. More than three changes return the existing safe
   retry/contract-mismatch class; the service does not spin.
4. If absent state remains absent, the coordinator durably creates the ADR-0005
   pending reservation. If pending or terminal state matches, it resumes or
   replays only after exact historical input comparison. A different canonical
   input returns the accepted safe idempotency-reuse error.

This split preserves ADR-0013 historical-plan normalization without making
`riffdb-commit` depend on `riffdb-service` and without letting the service create
an idempotency record or acquire a conflict lease itself. The lower executor
request/result types stabilize in a small WP-100 interface PR reviewed with this
ADR before WP-120 implementation.

An audited mutation has two current-policy checks. The first exact-facts check
establishes whether the request may proceed to its durable `started` record. Once
that record is durable, the executor grants a bounded, cancellation-aware queue
permit. The service then reloads the exact plan/facts, performs a fresh
current-policy check, and synchronously accepts the matching preparation through
that permit without an intervening `.await` or fallible audit write. That second
check immediately followed by synchronous acceptance is the non-retroactive
command authorization boundary. Revocation committed before it denies the
request and produces the post-start terminal `denied` phase. Revocation after
acceptance is not retroactive and does not cancel that admitted command; later
requests reauthorize. A preparation changed by a concurrent idempotency
observation must obtain a fresh permit and reauthorize before its next synchronous
acceptance, without appending another `started` record for the same invocation.

Grammar-v1 read-only commands still use the shared service and a bounded executor
read path, but they are always unjournaled in the POC. They create no pending or
terminal command-idempotency record, persisted command outcome, mutation commit
record, provenance record, or application `CommitSequence`. A required service
audit is written only to the separate administration stream and does not journal
the read result. Generic service/public schemas may accept an optional request-
correlation key, but it carries no durable idempotency, replay, or outcome-
recovery promise for a read-only command. A future durable read-only journal
requires an accepted ADR. These operations never gain a direct service-to-
storage mutation path.

### Control-plane executor boundary

Contract deployment, capability creation/revocation, and one-time bootstrap are
typed control-plane operations. The service validates and authorizes the request
then calls the WP-100 control-plane executor. The commit coordinator alone
assigns an `AdministrationSequence` and drives the authoritative transaction.
Catalog, auth, policy, service, gRPC, and MCP code never update authoritative
tables directly.

Deployment carries the exact candidate bundle identity, expected active version,
validated approval when required, request/actor context, and no source parser or
transport object. Capability operations carry the stable typed request defined
by the accepted capability ADR. Transaction-current expected-version,
initiating-capability, lifecycle, and replay checks belong to the coordinator
operation defined by the corresponding catalog/capability decision, not to a
weaker adapter precheck.

Normal capability create/revoke carries a privately constructible policy-owned
`AuthorizedCapabilityMutationPreparation`. After any executor queue wait and
inside the short authoritative transaction, the coordinator samples a fresh
time through the synchronous policy-owned `AuthorizationClock`, reloads the
transaction-current authorizing capability record through its storage handle,
mechanically copies every verifier-relevant checked field into policy-owned
bounded `TransactionCurrentCapabilityFacts`, and calls the pure policy-owned
`TransactionCurrentCapabilityVerifier`. That lowering performs no allow/deny,
expiry, delegation, approval, lifecycle, or permission predicate. The
verifier checks lifecycle, expected revision, expiry, audience, exact operation
permission, delegation subset, and validated approval binding and returns a
closed typed allow or deny. It performs no storage or time I/O, mutation,
sequence assignment, transport dispatch, obligation/redaction work, or general
operation authorization. The coordinator owns the transaction and transition,
consumes the result, and does not duplicate those policy predicates. The same
successfully validated `AuthorizationClock` value is the normal capability
create/revoke transition timestamp: create uses it as `issued_at`, computes the
exact checked `expires_at`, and verifies that exact proposed interval; revoke
uses it as `revoked_at`. The matching capability-administration record uses the
same value. It is not sampled again from `AdministrationClock`. Catalog
activation and principal-less bootstrap instead use the commit-owned
`AdministrationClock` defined below.

Accepted ADR-0009 defines the exact unauthenticated one-time bootstrap exception,
token carriage, and compound bootstrap audit record. This ADR creates no second
bootstrap RPC, direct storage path, or generic administrator bypass.

### Current-policy authorization safe points

The service uses one injected current-policy authorizer from WP-110. Every check
reloads current capability/policy state, takes a fresh value from the synchronous
policy-owned `AuthorizationClock`, and returns a privately constructible
authorized result plus obligations. A positive result is not cached across safe
points. For an audited mutation, the pre-start check and the post-start check
immediately before synchronous executor acceptance are distinct safe points.
`riffdb-server` owns the concrete operating-system provider used in production
composition; policy owns this interface and its time validation. The
deterministic runtime never receives either.

The mandatory safe points are:

| Operation | Required current-policy checks |
|---|---|
| Every unary operation | After bounded semantic validation and target resolution; an intrinsically audited operation checks once before `started` and again immediately before protected read/admission |
| Execute command | After the plan is classified and exact plan/input/partition facts are derived, then again after durable `started` and a bounded queue permit immediately before synchronous executor admission |
| Resolve outcome | Before lookup and again before returning the complete historical outcome |
| Control-plane mutation | Before `started`, then after a bounded permit immediately before synchronous executor submission; capability create/revoke additionally use the policy-owned pure transaction-current verifier inside the coordinator transaction after queue wait |
| Bounded wait | Before registering, after every wake, and immediately before returning data |
| Paginated operation | On the initial request and independently on every cursor use before the page read |
| Commit subscription | Before registration and before every externally visible item |
| Long read-only compute | Before starting and again before returning a sensitive plan/report if policy may have changed |
| MCP discovery | When producing the visible catalog; invocation performs its own independent check |

Outcome recovery uses the authenticated request's current authority. Matching
the same stable principal and tenant is necessary for ADR-0005 identity but is
not sufficient for disclosure: policy must authorize the exact contract
lineage/command, recorded partition, and complete outcome schema. A rotated
capability may resolve an outcome only when those current checks pass.

If policy state changes while an operation waits or streams, the next safe point
denies or terminates before another value becomes visible. Revocation does not
erase an already returned page/item and does not roll back an admitted command.

### Applying obligations and redaction

Policy decisions and raw obligations never cross into a transport adapter. The
service applies every recognized obligation or denies the operation. An unknown,
duplicate, contradictory, or inapplicable obligation fails closed.

Application rules are:

- Tenant and partition constraints are incorporated into the bounded semantic
  read request before storage/projection access and checked again on returned
  identities. They are not applied only after an unrestricted scan.
- A policy row limit lowers the requested page limit. It never raises a caller,
  contract, or process bound.
- Entity, commit, provenance, outbox, and projection fields are filtered into
  public-safe DTOs inside the service before cursor state, audit summaries,
  telemetry, or adapter serialization receives them.
- A command may be invoked or recovered only when policy permits its complete
  declared outcome schema. The POC does not return a schema-invalid partial
  command outcome.
- A projection query similarly returns complete rows conforming to the compiled
  projection-result schema or is denied; WP-120 does not invent a second
  field-level projection schema.
- Validated approval is attached only to the authorized operation/control-plane
  request for which policy validated it. Caller approval text is never treated
  as proof.
- Audit and output-classification obligations are executed before releasing a
  value to an adapter. Audit failure follows the fail-closed rules below.
- Safe error construction and redaction happen before tracing subscribers,
  metrics, gRPC status details, MCP text/content, or CLI rendering.

Adapters may perform lossless protocol conversion, output-size enforcement, and
MCP Markdown sanitization. They may further omit data for protocol limits, but
they may not restore a field, widen a row limit, reinterpret a denial, or turn a
business outcome into an execution error.

### Semantic read and late-subsystem ports

The service consumes only consumer-owned, bounded ports. Catalog reads may come
from `riffdb-catalog` because they return checked catalog semantics rather than a
persistence handle. For all other authoritative or late state,
`riffdb-service` owns four consumer ports:

1. `AuthoritativeReadPort` exposes get-entity, bounded index scan, stored-outcome,
   get/scan commit, provenance, and a bounded commit-notification source. Its
   implementation mechanically calls lower specialized readers from
   `riffdb-storage-api`; the service itself never receives a storage API trait,
   storage engine, table, key/value interface, or transaction. Raw records are
   returned only to service internals and are policy-filtered before emission.
2. `ProjectionQueryPort` accepts a checked lineage/projection identity, bounded
   normalized query, optional required sequence, and deadline. It returns the
   ADR-0010 typed ready/timeout/degraded/invalid semantics plus already bounded
   projection rows and an observed frontier/generation.
3. `OutboxStatusPort` exposes bounded, payload-free delivery-status summaries
   for authorized administrative inspection. It never exposes connector
   credentials, raw event payloads, or a delivery mutation.
4. `OperationalStatusPort` returns bounded component health/statistic signals
   that the service classifies into authoritative readiness versus derived
   degradation.

WP-120 ships deterministic fake/unavailable providers for its own service tests.
It does not claim real projection or outbox behavior. WP-130 tests projection
wire mapping against an injected fake as required by the SPEC. WP-160, WP-170,
and WP-180 retain their core APIs. WP-130/server composition may provide the
first mechanical `AuthoritativeReadPort` adapter needed by public entity/commit
RPCs. WP-185 owns the final projection/outbox/operational newtype adapters. These
adapters may translate checked typed states but contain no query, frontier,
retry, health, cursor, or policy decision.

An absent optional outbox-status provider causes the optional MCP operation to
be absent from discovery. A required projection or authoritative health provider
that is absent returns a typed unavailable/degraded result; it never returns an
empty successful result.

### Pagination and cursor semantics

`riffdb-service` owns `PageRequest`, `Page<T>`, and opaque `CursorToken`. The POC
uses a bounded in-memory server-side cursor registry; cursor contents are never
client-authored, signed claims, authorization proofs, or durable state.

The exact POC rules are:

- A page limit is `1..=500`. Protocols that permit omission use default 50;
  gRPC operations whose schema requires an explicit limit reject omission.
- Policy and operation-specific limits may lower the effective limit.
- A cursor token is 16 unpredictable bytes from an injected OS-random source
  outside deterministic runtime. Public text mappings are owned by the relevant
  proto/MCP interface follow-up; the service never parses display text.
- Registry insertion is atomic and insert-if-absent. A token collision retries
  generation at most three times; three collisions return the same safe
  unavailable/retry result as registry exhaustion and expose no candidate token.
- Cursor registry capacity is 4,096 globally and 64 live cursors per stable
  principal. Exhaustion returns a safe unavailable/retry result; it never evicts
  another request in a way that broadens access.
- Cursor lifetime is at most 300 seconds measured by the service monotonic
  clock. Restart invalidates all cursors safely.
- Stored state includes the stable principal, operation, exact normalized query,
  active/historical contract identity, policy-relevant tenant/partition facts,
  scan position, consistency fence, and expiry. It contains no raw credential,
  capability token, unredacted result row, or caller provenance text.
- A cursor is reusable until expiry so a lost page response can be retried. A
  use by another principal, operation, or query returns the same generic invalid
  cursor result and reveals no stored state.
- Every use reauthorizes current policy. Stored policy facts constrain lookup
  but never substitute for the fresh decision; narrower current policy denies or
  narrows before reading.
- Commit scans freeze an inclusive upper commit sequence on the first page.
  Entity/index scans bind an index/range epoch and fail with cursor invalidation
  if it changes; the service does not hold a storage snapshot between calls.
  Projection scans bind the exact plan/generation and observed frontier required
  by the accepted projection format decision. A port unable to provide the
  required fence fails closed rather than offering inconsistent pagination.
- `Page<T>` contains at most the effective limit, an optional next token, and the
  typed observed fence. Items are policy-filtered before insertion.

ADR-0017's projection storage query returns one atomic published-generation
snapshot and a lower typed continuation position. That continuation is stored
inside this server-side registry entry and is never the public `CursorToken`.
The service does not split ADR-0017's control read from its row scan or
reinterpret `FrontierPosition`; projection service results use its exact
`ProjectionIdentity`, `ProjectionGeneration`, and `FrontierPosition` types.

`riffdb-service` owns a narrow `CursorTokenGenerator` consumer port and a
deterministic fake restricted to tests. Production cursor generation is wired by
WP-185 server composition by direct use of `getrandom` 0.3.4 with default
features disabled and no optional features. The provider fills exactly 16 bytes
per attempt and exposes only the injected `CursorTokenGenerator` interface to
the service. It is independent of capability-token entropy and deterministic
command runtime.

`riffdb-service` separately owns a synchronous `CursorMonotonicClock` consumer
port that returns an opaque process-relative checked tick used only for cursor
expiry. WP-185 supplies a server provider backed by one private
`std::time::Instant` origin; tests inject explicit ticks and advance without
sleeps. Tick regression or checked-add/elapsed overflow fails closed as cursor
unavailable. This port is neither serialized nor exposed to runtime and is
distinct from authentication, authorization, admission, and administration wall
clocks. Restart still invalidates the entire registry.

This exact dependency is already present in the locked graph through Proptest.
Its reviewed implementation uses target-gated unsafe/operating-system calls and
its `build.rs` performs target configuration detection only; its licenses are
MIT OR Apache-2.0. Current dependency-policy checks pass. This acceptance
authorizes `riffdb-server` to make it a direct dependency and adds `Cargo.lock`
to WP-185 allowed paths even if the already locked version leaves the file
unchanged. A version, feature, target-support, transitive-dependency, build-script,
unsafe-surface, or license change requires a new dependency/security review.
Changing to self-contained signed cursors requires a separate keyed-domain,
custody, rotation, and compatibility decision.

### Wait and stream semantics

No service wait holds a redb transaction, materialized storage snapshot,
conflict lease, synchronous mutex guard, or command-runtime frame across
`.await`.

Projection and other read-after-sequence waits have a caller deadline, a
configuration maximum, and a POC hard maximum of 30 seconds. A requested value
above the hard maximum is a validation error rather than a silent extension.
Wait registration and cancellation are bounded. Every wake checks cancellation,
deadline, current policy, exact projection identity/generation, and persisted
frontier before returning. Spurious or stale notifications cannot produce a
ready result.

Commit subscriptions have these POC bounds:

- At most 128 live subscribers per server.
- A 256-item bounded buffer per subscriber.
- Catch-up scans of at most 500 commits per batch.
- A maximum stream lifetime of 900 seconds, after which the client reconnects
  from the last delivered sequence.
- One current-policy check and redaction pass before each externally visible
  notification.

The service never silently drops a commit. Buffer overflow, a scan gap, policy
change, cancellation, deadline, or service shutdown ends the stream with a typed
terminal condition. A lagged subscriber receives the last safely delivered
sequence as a resume point but no unauthorized next record. The gRPC proto-owner
follow-up must define lossless mapping for every stream item and terminal shape
before `SubscribeCommits` is marked supported.

MCP progress is adapter presentation over service/executor progress events. It
is monotonic and bounded, contains no unredacted values, and stops after the
service operation terminates. Progress cannot change cancellation or commit
semantics.

### Durable security and administration audit

`riffdb-service` owns the checked pre-sequence `ServiceAuditInput` and the
decision to emit security-audit attempts. WP-100 owns an
`AdministrationAuditExecutor` that appends one typed record through the commit
coordinator and allocates the shared `AdministrationSequence`. `riffdb-commit`
also owns the synchronous `AdministrationClock`; neither `ServiceAuditInput` nor
a storage request accepts a caller-selected timestamp. The executor is not a
general logging or write API. `riffdb-storage-api` owns the specialized atomic
append transition and durable semantic `StoredServiceAuditRecordV1`;
`riffdb-proto` owns its later durable encoding.

`AdministrationClock` returns a validated canonical `Timestamp`. Its values are
not required to be monotonic and never establish record order;
`AdministrationSequence` is the sole administration order. The coordinator
samples it exactly once for each standalone service-audit record, normal
`started` record, normal terminal record, and catalog transition. A new-bootstrap
compound transition uses one sample for its principal-less `started` record and
capability-administration record; exact bootstrap replay uses one sample for its
new `started` record. The bootstrap invocation's separate terminal record uses a
new sample. Normal capability create/revoke is the sole exception: the
transaction-current `AuthorizationClock` value already used by the verifier is
also the transition and capability-administration timestamp, and is not sampled
again from `AdministrationClock`.

Failure to obtain or validate an administration-clock value is an audit outage.
The service records a redacted incident, marks authoritative readiness unhealthy,
does not recursively try to audit that outage, and admits no protected work or
releases no protected output. It uses the same caller-visible mapping as an
unavailable required append at that lifecycle position under the matrix below.
If this happens while recording a policy denial, the caller still receives only
`AuthorizationDenied`.

The durable semantic `StoredServiceAuditRecordV1` contains, in order:

1. Coordinator-assigned nonzero `AdministrationSequence`.
2. Request ID.
3. Coordinator-observed timestamp.
4. `ServiceOperationV1` tag.
5. `ServiceAuditPhaseV1`: started `0x01`, succeeded `0x02`, denied `0x03`,
   cancelled without a known authoritative result or released protected output
   `0x04`, failed without a known authoritative application or control-plane
   result `0x05`, or outcome uncertain `0x06`.
6. Optional authenticated principal, actor kind, capability ID, and capability
   revision; absent only for ADR-0009's checked bootstrap invocation.
7. Trusted `ServiceIngressKindV1`.
8. A canonical list of at most 16 safe `ServiceAuditTargetV1` references: stable
   contract lineage/semantic IDs, contract version, commit sequence, provenance
   ID, or capability ID only.
9. Optional validated approval ID.
10. Exactly one closed `ServiceAuditLinkV1`:

    ```text
    None
    Command {
      commit_sequence: CommitSequence,
      provenance_id: ProvenanceId,
    }
    ControlPlane {
      administration_sequence: AdministrationSequence,
    }
    ```

Targets and result links are independent: targets identify the bounded protected
objects addressed by the invocation, while a link identifies an authoritative
result already known for that invocation. A command link is all-or-nothing and
cannot contain only a commit sequence or only provenance. A control-plane link
cannot be represented as a command target.

The record contains no raw credential, digest, idempotency key, entity/index/
partition key, cursor, source text, command input, output payload, reason, source
repository/commit, free-form error, network address, or transport debug value.
Target references have one closed typed encoding in the proto-owner record;
unknown target or phase tags reject.

Accepted ADR-0021 freezes the nine exact target variants/tags, requires lineage
on every contract-scoped ID, defines the canonical comparison key and
duplicate/order rules, and assigns exhaustive request-to-target construction to
`riffdb-service`. It excludes result-link values and returned/traversed objects
unless the original request independently selected them. WP-065 retains sole
ownership of reviewed Protobuf field numbers and total durable mapping.

Unknown authoritative commit status is `outcome uncertain`, never `cancelled`.
A resumable pending command reservation may remain after a proven pre-result
cancellation; phase `0x04` does not assert that no admission record exists.

For this POC, the audit boundary in `MCP-046` begins only after successful
`RequestContext` construction. A command enters the intrinsic mutation scope
only after the checked executable plan has been resolved and classified as a
mutation. A closed control-plane mutation or administrative read/stream is
intrinsically in scope once its closed operation is classified; bounded target
resolution then occurs inside that scope. An allowed standard read enters scope
only when its policy decision includes the
durable-audit obligation; an ordinary allowed standard read without that
obligation remains unaudited. Independently of those allow-path rules, every
explicit policy `Deny` after `RequestContext` construction is audited: it is a
standalone `denied` record if no `started` record exists and the invocation's
terminal `denied` record otherwise.

Malformed envelopes; missing, malformed, or unresolvable credentials; and an
unknown or missing Execute target before a checked plan can be classified remain
bounded transport-security telemetry. They do not consume an administration
sequence or create a fabricated principal-less record. Policy-filtered discovery
omissions also do not create one record per hidden item, although invoking a
known stale or hidden operation and receiving an explicit policy denial does.
Likewise, a standard read's semantic or authorization-clock failure before an
allow decision establishes its audit obligation remains bounded redacted
telemetry.
ADR-0009's successful new bootstrap and exact replay are the only durable
principal-less invocations. Failed, malformed, or mismatched principal-less
attempts remain bounded transport-security telemetry.

The ordinary audited invocation state machine is:

1. Resolve the exact bounded operation, targets, and policy facts, then perform
   the initial current-policy check. A semantic/input/internal failure or
   `AuthorizationClock` failure after the operation enters intrinsic audit scope
   but before `started` appends one standalone `failed` record. Pre-start
   cancellation or deadline appends one standalone `cancelled` record. An
   initial policy denial appends one standalone `denied` record for every known
   operation, including an otherwise ordinary standard read.
2. After the initial `Allow`, an intrinsically audited operation, or an allowed
   standard read carrying the audit obligation, durably appends exactly one
   `started` record before protected work is admitted.
3. After `started`, obtain a bounded, cancellation-aware permit for the exact
   executor or query path. Reload the exact plan and transaction-current policy
   facts, obtain a fresh `AuthorizationClock` value, and evaluate policy again.
   Immediately after this fresh `Allow`, synchronously accept the matching work
   through the held permit, with no intervening `.await` or fallible audit write.
   A changed command preparation releases the permit and repeats this exact-facts
   check/admission loop without appending another `started` record. A post-start
   denial, failure, or cancellation appends the corresponding terminal record
   and submits no protected work.
4. Once accepted, mutation authorization is non-retroactive for that admitted
   operation. The service awaits the authoritative result independently of the
   adapter future. It completely applies policy obligations, redaction, field
   filtering, classification, and all output/count/byte bounds to the safe result
   before constructing a terminal `succeeded` record. It appends the terminal
   record before releasing any result or protected output.
5. A known committed command result, including replay and a declared no-op
   outcome, uses `ServiceAuditLinkV1::Command` with both original commit sequence
   and provenance ID. A known control-plane result uses
   `ServiceAuditLinkV1::ControlPlane`. ADR-0012 durable `ExecutionFailed` uses
   phase `failed`, link `None`, and no application sequence; it remains distinct
   from a declared application outcome. A pre-commit cancellation or failure
   with no authoritative result uses `cancelled` or `failed` and link `None`.
6. Every invocation with a durable `started` record has exactly one terminal
   phase selected by this state machine, and the service attempts exactly one
   append for that phase when it learns the terminal class. At most one terminal
   record may become durable; an append failure, unknown append status, or process
   crash may leave none visible and recovery never synthesizes it. Every retry uses its fresh current request ID for its own
   start/terminal pair even when a linked command provenance retains the original
   admission request ID. Recovery never appends a terminal record to an earlier
   crashed invocation and never fabricates one from later observations.

Administrative reads and audit-obligated standard reads use the same initial
check, `started`, bounded permit, fresh exact-facts check, and synchronous
admission sequence. Their complete filtered and bounded unary result must exist
before `succeeded` is appended and is released only after that append is known
durable. For a stream, the audited invocation ends at establishment: the service
constructs a bounded policy-filtered stream handle, appends `succeeded`, and only
then releases the handle. Later per-item policy checks, filtering, and bounded
delivery are continuation behavior, not new audit invocations and not additional
terminal phases. A later denial or failure terminates the stream safely and emits
bounded security telemetry; it cannot append a second terminal phase to the
establishment invocation.

Bootstrap does not perform a standalone append before its first authoritative
transition because that would make the emptiness predicate false. A successful
new-bootstrap compound transition validates emptiness, then atomically writes a
principal-less `started` record followed by the capability-administration record,
marker, capability, and digest lookup. The started record links the allocated
control-plane administration sequence. Exact replay appends a new
principal-less `started` record linked to the original transition and does not
mutate the capability or marker. Either result receives a separate terminal
`succeeded` record before release. A malformed credential, invalid request,
failed emptiness check, or marker/request/digest mismatch remains telemetry and
allocates no administration sequence.

Required audit append failures map exactly as follows; an original safe
validation, internal, cancellation, or application result never bypasses this
matrix:

1. A proven-abort failure of a required standalone or `started` append before
   business admission returns `StorageUnavailable`, submits no protected work,
   and marks readiness unhealthy.
2. `CommitStatusUnknown` for a standalone or `started` append returns
   `StorageUnavailable`, submits no business work, and fences authoritative
   writes and readiness. It is never reported as command `OutcomeUnknown`.
3. A proven-abort or unknown append while recording a denial still returns only
   `AuthorizationDenied`, releases no permission or protected output, records a
   redacted incident, and marks readiness unhealthy; an unknown append also
   fences authoritative writes.
4. Terminal append failure after a known committed command result or durable
   ADR-0012 `ExecutionFailed` returns command `OutcomeUnknown`, with recovery by
   the same idempotency identity. The known result is withheld.
5. Terminal append failure after a known pre-commit cancellation or failure with
   no durable terminal idempotency state returns `StorageUnavailable`; the
   original result is withheld.
6. Terminal append failure for a read or stream-establishment invocation returns
   `StorageUnavailable` and releases no output or stream handle.
7. Terminal append failure after a committed or uncertain control-plane
   transition returns that operation's typed unavailable or uncertain recovery
   result and releases no success payload.

Audit append never recursively audits itself. A process crash or unknown audit
commit may leave a durable `started` record without a visible terminal record.
Recovery does not synthesize its terminal; the next invocation receives its own
start/terminal pair, while authoritative command or control-plane records decide
whether protected state committed.

If the compound bootstrap transaction returns `CommitStatusUnknown`, the
coordinator fences further authoritative writes and readiness fails closed. The
service appends no inferred terminal record, releases no success, and returns
only the safe uncertain control-plane classification. The operator recovers with
the retained credential; the next healthy invocation deterministically observes
either an empty database or the exact replay state. Failure to append the
post-commit bootstrap `succeeded` record also withholds success and uses that
same recovery path without rolling back the authoritative transition.

The accepted atomic governance reconciliation resolves the former work-package
metadata conflict: WP-060/WP-070 require ADR-0007 and own the semantic/durable
audit paths, WP-065 owns the reviewed schema/mapping phase, WP-100 owns the audit
executor tests, and WP-120 owns service orchestration only. Durable `MCP-046`
evidence still requires those packages and the final protocol/service tests; a
fake audit port alone cannot claim it.

### Cancellation, panic containment, and uncertain outcomes

Service cancellation follows SPEC Section 9.7 exactly:

- Before executor admission, cancellation stops work and releases cursor/wait
  resources after any required standalone or post-`started` terminal audit is
  resolved under the failure matrix above.
- Before a conflict lease grant, the executor removes its waiter.
- After lease grant but before commit submission, cancellation stops evaluation
  and releases the lease; an ADR-0005 pending reservation may remain resumable.
- While queued, the executor may cancel only if it has not synchronously accepted
  the request.
- Once the coordinator starts the authoritative transaction, cancellation and
  adapter disconnect do not roll it back.
- A committed result is never reported as cancelled merely because its response
  receiver disappeared.

The executor owns submitted work independently of the adapter future. Dropping a
one-shot receiver cannot drop a coordinator transaction or leak a conflict
lease. When the service remains connected but cannot distinguish pre-commit
failure from post-commit response loss, it returns the existing
`OutcomeUnknown` public error with same-idempotency-key recovery guidance. It
never retries a mutation with a new key or invents an MCP idempotency key.

`ResolveOutcome` and an equal-input execute retry return the one persisted
outcome only after current authorization and disclosure checks. Cancellation of
an MCP call after submission may produce no protocol response; the durable
outcome remains resolvable.

The service boundary catches panics from compiler, policy, query-port, and
runtime/executor calls that are designated containable, records only a redacted
incident, releases non-durable resources, and returns `InternalDefect` when a
response is still possible. An invariant breach from the commit coordinator or
durable corruption is not converted into ordinary request failure; the server
fails readiness or fails fast as required by SPEC Section 9.8.

### Adapter responsibilities and dependency bans

Transport adapters may:

- Decode and bound protocol envelopes.
- Extract credentials and call the shared authenticator.
- Convert exact values and IDs into checked service DTOs.
- Propagate deadline, cancellation, and trace context.
- Map typed results/errors to the accepted protocol representation.
- Enforce a stricter response-size limit and sanitize MCP text/Markdown.
- Present already filtered discovery descriptors and generated schema artifacts.

They may not:

- Access `riffdb-storage-api`, a storage implementation, catalog state, runtime,
  conflict manager, idempotency state, commit executor, projection state, or
  outbox state directly.
- Evaluate policy, construct an allow decision, apply only a subset of
  obligations, or cache positive authorization.
- Create a command/control-plane intent or allocate a sequence.
- Decode or forge service cursor state.
- Build a command-specific input/output schema independently of the checked
  contract bundle.
- Turn MCP discovery visibility into invocation authority.
- Return raw lower-layer errors, records, secrets, or debug formatting.

Architecture tests inspect Cargo metadata and source imports. `riffdb-api-grpc`
and `riffdb-api-mcp` may depend on the shared authentication interface solely for
credential handoff, plus `riffdb-service` and their protocol/schema conversion
dependencies. A direct dependency on a banned semantic/storage crate is a test
failure even if no known bypass call is currently made.

### Service hard bounds

The POC service hard bounds are:

| Boundary | Limit |
|---|---:|
| Structurally decoded unary request | 1 MiB before tighter schema bounds |
| Service unary response before adapter encoding | 4 MiB |
| Page items | 500 |
| Default page items where omission is legal | 50 |
| Cursor token bytes | exactly 16 |
| Cursor lifetime | 300 seconds |
| Live cursors | 4,096 server / 64 principal |
| Projection/read wait | 30 seconds |
| Commit subscribers | 128 |
| Subscription buffer | 256 items |
| Commit catch-up batch | 500 records |
| Subscription lifetime | 900 seconds |
| Audit target references | 16 |
| Command preparation observations | 3 |
| Cursor token generation attempts | 3 |
| Source repository / commit / reason / approval claim | 512 / 128 / 1,024 / 256 bytes |

Compiled contract bounds, canonical-value bounds, parser diagnostics, storage
scan bounds, policy row limits, and adapter message limits may be lower and
remain authoritative. Counts and sizes are checked before allocation with
checked arithmetic. Configuration may lower a hard bound but cannot raise it in
the POC.

### Proto-owner and generated-schema boundaries

WP-020 remains the completed baseline that froze generation, exact values,
envelopes, five services, 16 RPC names, and phase-zero shells. It is not reopened
with deliverables that depend on its own downstream WP-060 or WP-120 consumers.
After semantic service DTOs stabilize, formal `WP-127` depends on WP-020 and
WP-120 and fills the corresponding ADR-0006 phase-zero public messages. WP-127
merges before WP-130 consumes them; WP-130 gains WP-127 as a hard dependency but
does not gain `proto/**`, `crates/riffdb-proto/**`, or fixture ownership. WP-127
preserves the fixed services, RPC names, Execute fields, streaming shapes, exact
`Value`, and public error mapping. It does not copy Rust service layout or expose
internal obligations, authenticated principals, cursor state, audit records, or
executor tickets.

To map the closed unjournaled read-only result without changing an existing
Execute field number, WP-127 appends `EXECUTED_READ_ONLY = 3` to
`ExecuteCommandResponse.CompletionStatus`. Only for that status,
`commit_sequence` is the wire sentinel zero and `provenance_uri` and
`durability_mode` are empty. Adapters convert those fields to absence and never
construct `CommitSequence(0)`, a provenance ID, or a durability mode. `COMMITTED`
and `REPLAYED` continue to require their original nonzero sequence and complete
terminal fields. Unknown status or any inconsistent field combination rejects.
The appended enum value and sentinel rules are a reviewed additive semantic
change to the supported Execute operation and require descriptor/golden/client
fixtures in WP-127/WP-130.

WP-127 adds and fixtures exact wire shapes for pages/cursors, command results,
projection typed results, health, capability operations, and every commit-stream
terminal variant before those RPCs become supported. Service-to-wire conversion
remains WP-130 adapter code. The binary carriage of `PublicError` on non-OK gRPC
responses remains the explicit ADR-0006 WP-130 human-review item; this ADR does
not choose it silently.

MCP command input and declared-outcome schemas remain the exact compiler-owned
ADR-0013 artifacts. `riffdb-service` owns only the generic operation envelope
and already filtered discovery descriptor. MCP mechanically composes them and
must not maintain parallel command schemas. MCP tool names, resource URI text,
cursor text encoding, and protocol fixtures remain blocked on ADR-0008.

The durable Protobuf payload `riffdb.storage.v1.ServiceAuditRecordV1` encodes
`riffdb-storage-api`'s semantic `StoredServiceAuditRecordV1`. Formal WP-065
depends on WP-020 and WP-060 and adds the durable messages, descriptors, goldens,
wire validation, and checked semantic mapping in
`riffdb-storage-api::proto_codec` only after the Rust semantic owner and
specialized transition are accepted. WP-070 depends on WP-065 and may persist
the record only afterward. `riffdb-proto` never depends on
`riffdb-storage-api`; the storage API's core traits never expose a Prost type. A
public DTO is never persisted merely because it already has similar fields.
This explicit ordering is WP-020 -> WP-060 -> WP-065 -> WP-070, with no hidden
back-edge or reopened completed package.

### Coordination with the accepted ADR batch

This exact record was checked against ADR-0004, ADR-0009, ADR-0012, and ADR-0017.
The following coordination is part of their atomic acceptance batch:

- **ADR-0004:** The exact `ExecutablePlanRef`, owned-snapshot, short-transaction,
  realistic empty/nonempty batch states, sequence, lower reader boundaries,
  `EvaluatedCommand` split, and narrowly typed standalone service-audit append
  are compatible. ADR-0007 gives the service consumer-owned read ports rather
  than an ADR-0004 persistence port. The service loads a historical plan for
  validation and policy; the coordinator independently resolves and rechecks the
  same exact plan for execution, so neither load substitutes the active plan.
  WP-065 owns the reviewed durable schema/mapping bridge before WP-070.
- **ADR-0009:** The authentication/principal boundary, fresh safe-point checks,
  complete-outcome disclosure, and no positive authorization cache align. The
  two ADRs use distinct operation and permission tag registries with an
  exhaustive typed policy mapping. ADR-0009 explicitly defers durable denied/
  administrative-read audit to ADR-0007/ADR-0008, so this record supplies that
  missing owner but still depends on ADR-0009 for exact capability/bootstrap
  semantics.
- **ADR-0012:** The immutable admitted actor, current-policy retry check, fixed
  logical time, and non-commit `ExecutionFailed` state align. ADR-0007 phase
  `0x05` records that terminal non-commit failure. The reconciliation places
  `StoredAdmittedProvenanceClaimsV1` beside, not inside, ADR-0012's deterministic
  runtime context and reflects it in the pending-record/proto follow-up.
  ADR-0012's ninth public error is frozen by the exact WP-010 proto carve-out and
  must be preserved by the later service/protocol mapping.
- **ADR-0017:** Atomic published-generation queries, exact projection identity,
  generation, `FrontierPosition`, and lower continuation state align after the
  cursor clarification above. The projection crate owns query/wait semantics;
  the service owns current authorization, public cursor registry, obligation
  application, and adapter-facing result. Neither layer splits the one-snapshot
  control/row read.

### Exact SPEC reconciliation required at acceptance

The governance commit that accepts this record replaces the two affected SPEC
Section 4.1 rows with the following text:

| Component | Responsibility | Must not own |
|---|---|---|
| Command application service | API-neutral semantic validation, exact plan/input preparation, current authorization, security-audit orchestration, invocation of typed command/control-plane executors and bounded read ports, obligation application, and safe result release | Transport formatting, conflict acquisition, runtime evaluation, storage transactions, sequence assignment, or authoritative writes |
| Commit coordinator | After acceptance of an authorized typed preparation, create or resolve admission, acquire logical capabilities, materialize bounded snapshots, invoke deterministic runtime, revalidate, invoke the narrow policy-owned transaction-current capability verifier where required, assign application or administration sequences, atomically apply authoritative records, and notify bounded consumers | Contract parsing, general service/transport policy decisions, duplicated capability-policy predicates, obligations/result redaction, or external effects |

The same commit replaces the following SPEC Section 5.2 rows exactly. These are
compile-time dependency permissions, not permission to bypass the call boundaries
in this record:

| Crate | Public responsibility | Allowed dependency direction |
|---|---|---|
| `riffdb-proto` | Generated public/durable Protobuf types, envelopes, descriptors, wire validation, and foundational value/error conversion helpers | Types, errors, and Prost only; no storage API, runtime, service, or transport dependency |
| `riffdb-storage-api` | Semantic snapshots, evaluated commands, commit intents, durable DTOs, typed transitions/readers, the narrow durable `proto_codec` mapping bridge, and ADR-0017 projection-schema consumers | Types/errors; proto only through `proto_codec`; contract IR only for immutable `ProjectionGroupSchema`/`BoundProjectionGroupSchema` values in the projection-schema module; no compiler, command-plan interpretation, runtime, commit, service, transport, or concrete-engine dependency |
| `riffdb-runtime` | Deterministic command-plan interpreter that consumes owned snapshots and produces `EvaluatedCommand` without external I/O | IR, invariant engine, and storage semantic value/snapshot types; no provenance claims, admission persistence, storage engine, service, or transport dependency |
| `riffdb-policy` | Deny-by-default authorization, capability scopes, obligations, approvals, provenance validation, value-only authorized capability-mutation preparation and transaction-current facts, synchronous authorization clock, and pure transaction-current capability verification | Auth and types/errors only; no storage API, commit, service, transport, protocol, authoritative write handle, or concrete storage dependency |
| `riffdb-commit` | Admission, capability acquisition, deterministic evaluation orchestration, assembly of the final `CommitIntent`, revalidation, sequencing, authoritative commit, typed control-plane operations, ordered audit execution, and the synchronous `AdministrationClock` | Runtime, conflict, idempotency, catalog, storage API, and policy-owned authorized-preparation/provenance/facts values plus only `TransactionCurrentCapabilityVerifier` and `AuthorizationClock`; no service, transport, protocol, general policy authorizer, obligations/redaction engine, or policy-owned storage reader |
| `riffdb-auth` | Principal authentication, local development capability tokens, expiry, credential resolution, narrow synchronous authentication clock, and `CredentialAuthenticator` entry point | Types, errors, and storage-owned capability readers; no policy, command execution, service, or transport dependency |
| `riffdb-service` | API-neutral command, contract, entity, commit, provenance, projection, discovery, administration, and health services | Foundational types/errors, contract/compiler/catalog semantics, auth and policy entry points, typed commit executors, and consumer-owned bounded read ports; no transport, general storage engine, or concrete storage implementation |
| `riffdb-api-grpc` | Tonic services, authentication interceptors, bounds checks, and wire conversions | Service, the auth-owned `CredentialAuthenticator` interface, proto, and Tonic; no policy, catalog, runtime, commit, storage API, or storage implementation |
| `riffdb-api-mcp` | MCP tool/resource catalogs, schema translation, authorization-aware discovery presentation, Streamable HTTP authentication/handling, and protocol adaptation | Service, the auth-owned `CredentialAuthenticator` interface, `rmcp`, and JSON Schema support; no direct policy, catalog, runtime, commit, storage API, or storage implementation |
| `riffdb-server` | `riffdbd` process composition, configuration, lifecycle, hosted gRPC/HTTP endpoints, and concrete OS-time providers implementing the separate authentication, authorization, and administration clock ports | Service/API crates and concrete auth, policy-clock, executor, storage, outbox, projection, and observability implementations solely for composition; no new policy, command, query, redaction, cursor, or audit semantics |

`CredentialAuthenticator` accepts one size-checked opaque credential plus trusted
configured database, environment, and audience context. It returns only a
privately constructible `AuthenticatedPrincipal` or the generic safe
authentication failure. It exposes no capability reader, policy decision,
obligation, token digest, or storage handle. gRPC and MCP HTTP may call this one
entry point before constructing `RequestContext`; they may not call any other
auth/policy operation. Production MCP stdio remains a gRPC client and does not
authenticate through an in-process shortcut.

The same acceptance commit makes these remaining SPEC edits:

- Section 9.1 assigns transport decode/authentication and `RequestContext`
  construction to the adapter, semantic preparation/current authorization/audit
  admission/result release to the service, and steps after typed preparation
  acceptance to the WP-100 executor described above.
- Sections 4.1, 5.2, 9.1, 9.5, and 17.3 state that deterministic runtime returns
  `EvaluatedCommand` and the coordinator alone combines it with the exact stored
  admission to construct `CommitIntent`; runtime determinism tests compare the
  former and coordinator assembly tests compare the latter.
- Sections 9.2, 9.5, 13.1, and 13.4 use ADR-0012's exact
  `AdmittedActorContext` for runtime and the separate persisted
  `StoredAdmittedProvenanceClaimsV1` lifecycle defined here. They do not place
  source, reason, approval, current capability, or retry claims in deterministic
  runtime context.
- Section 12.11 states the authenticated-valid MCP-046 scope above and retains
  malformed/unauthenticated attempts as bounded transport-security telemetry;
  it does not silently treat unauthenticated traffic as an authoritative audit
  write request.
- Sections 9.1, 11, and 22.2 state that grammar-v1 read-only commands are
  unjournaled and create no pending/terminal command-idempotency record,
  persisted outcome, provenance, or application sequence. Required service audit
  remains a separate administration record. A generic request-correlation key is
  optional transport metadata only and promises no durable replay for this
  operation class. Section 11 appends `EXECUTED_READ_ONLY = 3` to the existing
  Execute completion-status enum and freezes the status-dependent zero/empty
  wire sentinels above; adapters map them to absence, never to semantic zero
  values, and reject every inconsistent status/field combination.
- Section 22.2 also adopts ADR-0004's one-partition cross-domain read rule,
  upfront acquisition of every mutation domain, complete influential dependency
  evidence, and grammar-v1 rejection of write-influencing indexed range reads.
- Section 22.1 marks ADR-0004, ADR-0007, ADR-0009, ADR-0012, and ADR-0017
  Accepted and records their cross-references. No status is changed before every
  change in this section is present in the same commit.

### Exact work-package manifest reconciliation

The following table is the additive delta from the current
`work_packages.yaml`. “None” is deliberate: it means the current field remains
unchanged. The governance commit applies the entire table, including rows whose
change originates in a required ADR, so the accepted ADR set and manifest cannot
describe different implementation orders.

| WP | `depends_on` additions | `required_adrs` additions | `allowed_paths` additions | Deliverable additions |
|---|---|---|---|---|
| WP-010 | None | ADR-0006, ADR-0007, ADR-0009, ADR-0012, ADR-0013, ADR-0017 | Exact ADR-0012 carve-out: `proto/riffdb/v1/error.proto`; generated `riffdb.v1.rs`; command/value/public-error mappers and wire preflight; Proto generator example; schema/wire tests; production descriptor, schema inventory, and wire-vector fixtures | Shared service-operation/audit phase/ingress/target/result-link value vocabulary, including the exact phase-`0x04` meaning and all-or-nothing command link, plus bounded admitted-provenance component types, complete nonzero stable-ID/phase-zero decoder enforcement, the foundational additions required by ADR-0009, ADR-0012, and ADR-0017, and the exact execution-failure wire slice |
| WP-020 | None | None | None | None; the accepted phase-zero Protobuf/envelope baseline remains complete and is not reopened with downstream-dependent deliverables |
| WP-040 | None | ADR-0004, ADR-0017 | None | Exact projection identity, plan/group-key metadata, generated fixtures, and rejection of dynamic/cross-partition mutation acquisition or hidden write-influencing indexed range reads before the storage projection port freezes |
| WP-050 | None | None | `Cargo.lock` | None; generated lockfile authority covers root-workspace catalog/storage/IR path-dependency wiring and does not approve a new third-party dependency |
| WP-060 | WP-040 | ADR-0007, ADR-0012, ADR-0017 | `Cargo.lock` | `StoredServiceAuditRecordV1` with independent targets, exact closed result links, and coordinator-owned timestamps; specialized ordered service-audit append/read transitions; `StoredAdmittedProvenanceClaimsV1` in admission records; exact ADR-0017 projection ports; and memory conformance tests |
| WP-065 (new) | WP-020, WP-060 | ADR-0004, ADR-0005, ADR-0006, ADR-0007, ADR-0009, ADR-0010, ADR-0011, ADR-0012, ADR-0013, ADR-0014, ADR-0016, ADR-0017 | `Cargo.lock`; `proto/**`; `crates/riffdb-proto/**`; `fixtures/proto/**`; `scripts/generate-proto*`; `crates/riffdb-storage-api/Cargo.toml`; `crates/riffdb-storage-api/src/lib.rs`; `crates/riffdb-storage-api/src/proto_codec/**` | Durable semantic-record messages, descriptors, schema hashes, goldens, historical registrations, wire validation, and checked storage DTO mappings; no reverse proto-to-storage dependency |
| WP-070 | WP-065 | ADR-0007, ADR-0012, ADR-0017 | `Cargo.lock`; `tests/service_audit_recovery/**` | Durable service-audit persistence/indexes, admitted-provenance admission fields, exact projection tables, integrity checks, and crash/reopen coverage using WP-065 schemas/mappings |
| WP-080 | None | ADR-0007 | `Cargo.lock` | Runtime returns `EvaluatedCommand`; it receives no stored provenance claims and does not construct the final `CommitIntent` |
| WP-100 | None | None | `Cargo.lock`; `tests/service_audit/**` | Bounded idempotency inspect/confirm plus command, control-plane, and `AdministrationAuditExecutor` ports; commit-owned synchronous `AdministrationClock` and exact sample rules; coordinator assembly of `CommitIntent`; new-admission provenance freezing; stored-claim reuse; bounded-permit/fresh-auth/synchronous-admission behavior and exact start/terminal link/failure semantics plus the compound bootstrap audit transition; complete mechanical storage-record-to-policy-facts lowering and verifier/clock use after capability-mutation queue wait; an unjournaled read-only path with no command admission/sequence; and executor cancellation/uncertainty tests |
| WP-110 | None | ADR-0007 | `Cargo.lock` | Auth-owned `CredentialAuthenticator`; policy-owned synchronous `AuthorizationClock`, value-only `AuthorizedCapabilityMutationPreparation` and `TransactionCurrentCapabilityFacts`, pure `TransactionCurrentCapabilityVerifier`, exhaustive typed service-operation-to-permission mapping, and privately constructible `AuthorizedProvenanceClaims`; verifier, clock-failure, denial, delegation, approval, and redaction tests; ADR-0009 retains separate approval of its capability entropy/HMAC/base64/zeroization graph |
| WP-120 | None | ADR-0004, ADR-0009, ADR-0012, ADR-0017 | `Cargo.lock` | Six API-neutral service traits, checked request context, executor/read consumer ports, initial/start/permit/fresh-auth/synchronous-admission orchestration, exact audit scope and append-failure mappings, fully filtered/bounded pre-success results, stream-establishment audit, current-policy safe points, obligation application, cursors/pages/waits/streams, and deterministic fake cursor generator |
| WP-125 | None | None | None | None; the existing no-storage service adapter and shared oracle are sufficient |
| WP-127 (new) | WP-020, WP-120 | ADR-0006, ADR-0007, ADR-0009, ADR-0010, ADR-0011, ADR-0012, ADR-0013, ADR-0017 | `Cargo.lock`; `proto/**`; `crates/riffdb-proto/**`; `fixtures/proto/**`; `scripts/generate-proto*` | Completed public phase-zero message fields, including `EXECUTED_READ_ONLY = 3` and its exact sentinel rules; descriptors, wire validation, schema hashes, and golden/client fixtures for every WP-130-supported RPC; no service-to-wire adapter code |
| WP-130 | WP-127 | ADR-0009, ADR-0012, ADR-0017 | `Cargo.lock` | gRPC credential handoff through `CredentialAuthenticator`; total service/proto conversion including status-dependent Execute validation and sentinel-to-absence mapping; error/stream/projection mapping; and architecture tests proving no policy or lower semantic bypass |
| WP-135 | None | None | None | None; it remains a public gRPC client of the shared semantics |
| WP-140 | None | ADR-0012, ADR-0017 | None | MCP HTTP credential handoff through `CredentialAuthenticator`, stdio-over-gRPC parity, service-only invocation/discovery, and durable authorization/audit conformance evidence |
| WP-150 | None | ADR-0009 | None | None beyond its existing public-gRPC CLI deliverables; it gains no in-process auth or service bypass |
| WP-160 | None | ADR-0007 | None | Bounded payload-free outbox-status source semantics for the later mechanical service adapter |
| WP-170 | None | ADR-0007, ADR-0017 | None | Bounded projection query/status/wait source using exact projection identity, generation, frontier, and lower continuation semantics |
| WP-180 | None | None | None | None; its existing upstream telemetry/health-hook integration remains sufficient |
| WP-185 | None | ADR-0017 | `Cargo.lock` | Mechanical authoritative/projection/outbox/operational service-port adapters; concrete authentication-, authorization-, and administration-clock providers; and the reviewed direct `getrandom` 0.3.4 cursor generator with default features disabled and no optional features |
| WP-190 | None | ADR-0007, ADR-0017 | None | Process crash/restart evidence for started/terminal service audit, retry/resume audit linkage, admitted-provenance preservation, cursor invalidation, and projection frontier behavior |
| WP-200 | None | ADR-0017 | None | Cross-transport semantic/audit parity and generated-artifact evidence in the final requirement report |

`riffdb-proto` remains the sole crate owner of checked-in `.proto`, generated
wire types, descriptors, and protocol fixtures. WP-020 owns the baseline;
WP-065 and WP-127 are formal proto-owner packages with explicit downstream
dependencies rather than retroactive WP-020 revisions. In particular:

- WP-065 alone may add durable semantic-record schemas and the exact storage API
  mapping bridge. WP-060 cannot guess wire fields and WP-070 cannot define a
  record schema while implementing redb.
- The focused WP-010 exception may add only ADR-0012's already reviewed
  execution-failure kind/detail/code, mapper, preflight, generated artifacts, and
  goldens. WP-127 alone may fill every remaining public phase-zero shell after
  service DTOs stabilize. WP-130 owns total service/wire adapter conversion but
  does not gain proto source or fixture paths.
- WP-060, WP-070, and WP-100 implement their respective semantic, durable, and
  executor pieces inside their declared or explicitly added paths; no package
  edits a neighboring owner merely to avoid an interface PR.
- WP-120 gains `Cargo.lock` only for generated root-workspace path-dependency
  wiring and supplies no production randomness implementation.
- WP-185 gains `Cargo.lock` authority only for the exact reviewed direct
  `getrandom` configuration above. Any graph change outside that approval stops
  for a new review.

The same governance reconciliation grants `Cargo.lock` to every root-workspace
implementation package whose owned crate manifests can change dependency edges,
including the existing WP-050, WP-060, WP-080, WP-090, WP-100, WP-120, and
WP-130 cases. Lockfile path authority is generated-artifact ownership, not
approval of an unreviewed crate, version, feature, build script, native/unsafe
surface, or license.

The same manifest change adds `MCP-046` to WP-060, WP-065, WP-070, WP-100,
WP-120, and WP-190 so each audit layer must map its part to automated evidence;
WP-140 retains final MCP conformance ownership. WP-065 requirements also include
`STO-002`, `STO-020` through `STO-022`, and `VAL-003`; WP-127 requirements include
`API-001`, `VAL-003`, and ADR-0017's projection-fixture `POC-006`. Existing
requirement assignments are not removed.

Add these acceptance commands:

- WP-065: `cargo test -p riffdb-proto -p riffdb-storage-api` and
  `./scripts/generate-proto --check`;
- WP-070:
  `cargo test -p riffdb-storage-redb --test service_audit_recovery` in addition
  to the existing storage recovery matrix;
- WP-100: `cargo test -p riffdb-commit --test service_audit_ordering`;
- WP-120: `cargo test -p riffdb-service --test service_audit_orchestration`;
  and
- WP-127: `cargo test -p riffdb-proto` and
  `./scripts/generate-proto --check`.

Each external root-level test has exactly one owning crate manifest and needs no
root package. WP-070 adds
`[[test]] name = "service_audit_recovery"` in
`crates/riffdb-storage-redb/Cargo.toml` with path
`../../tests/service_audit_recovery/service_audit_recovery.rs`; WP-100 adds
`[[test]] name = "service_audit_ordering"` in
`crates/riffdb-commit/Cargo.toml` with path
`../../tests/service_audit/service_audit_ordering.rs`; and WP-120 adds
`[[test]] name = "service_audit_orchestration"` in
`crates/riffdb-service/Cargo.toml` with path
`../../tests/service/service_audit_orchestration.rs`. Those three crate
manifests are already inside their owning packages' allowed crate paths. The
test files use only the corresponding package's already-declared or reviewed
dev-dependencies; a new dependency still requires ordinary dependency review.

SPEC Sections 19.4/19.5, `work_packages.yaml`, and
`diagrams/work_package_dag.dot` must add both packages and edges. The manifest P1
gate must add WP-065 and WP-127 explicitly while preserving every existing P1
member; their WP-070/WP-130 hard edges remain independently required.

### Atomic governance rule

ADR-0007 must not be accepted alone. One commit must:

1. Apply every SPEC edit above and update the ADR index.
2. Accept ADR-0004 with the specialized service-audit append/read transition and
   durable owner boundary used here.
3. Accept ADR-0009 with the shared authenticator/principal boundary and a
   cross-reference to ADR-0007's denied/read audit ownership.
4. Accept ADR-0012 with `StoredAdmittedProvenanceClaimsV1` beside, never inside,
   deterministic `TransactionContext`, and retain it through terminal non-commit
   admission.
5. Accept ADR-0017 with the exact projection types consumed by the service port
   and cursor registry.
6. Add formal WP-065 and WP-127, their hard edges, explicit P1 membership without
   removing existing members, acceptance commands, and corresponding SPEC/diagram
   nodes; do not reopen WP-020 with hidden downstream prerequisites. Apply the
   remaining complete manifest delta above; the
   ADR-0009 SPEC Sections 10.2/13.2 and Appendix A reconciliation; the ADR-0012
   `LOG-001`, Sections 13.4/17, and
   public-error reconciliation; the ADR-0017 SPEC Sections 10.2/15/19.4/19.5 and
   `diagrams/work_package_dag.dot` reconciliation; and the amendment/cross-
   reference updates to accepted ADR-0005, ADR-0006, ADR-0010, ADR-0011, and
   ADR-0016 required by those records.
7. Set ADR-0007 to `Status: Accepted` and `Exact text accepted: Yes` in the same
   commit as items 1 through 6.

A partial application of this batch is invalid and blocks WP-060 record freeze,
WP-100 executor-port freeze, and WP-120 service-trait freeze. Implementations may
not treat a status-only edit or a subset of the manifest table as authoritative.

## Options Considered

1. **Shared service plus lower executor and consumer-owned late ports:**
   Selected. It preserves one authorization/result path and an acyclic package
   graph.
2. **Service directly owns locks, runtime, and commit mechanics:** Rejected. It
   duplicates WP-100 orchestration and makes storage/transaction concerns leak
   into the public-service package.
3. **Commit executor owns policy and transport request handling:** Rejected. It
   creates a policy/service dependency cycle and mixes API concerns into the
   durable coordinator.
4. **Adapters orchestrate lower crates independently:** Rejected. It creates
   semantic drift and privileged paths.
5. **Projection/outbox crates implement service traits directly:** Rejected as
   the default because WP-120 precedes them and direct mutual ownership invites
   a cycle. Composition newtypes preserve both owners.
6. **Long-lived storage snapshots for pagination:** Rejected. They retain engine
   transactions across requests and make waits/cursors process-resource leaks.
7. **Self-contained signed cursors in this ADR:** Rejected for the POC. They add
   a new keyed format and key-custody decision when bounded server-side state is
   sufficient.
8. **Best-effort in-memory security audit:** Rejected. It cannot satisfy
   MCP-046 or survive a crash.
9. **Return raw policy obligations for adapters to apply:** Rejected. Different
   adapters would become independent security implementations.

## Consequences

- WP-100 publishes a small command/control-plane/audit executor interface before
  WP-120 and remains independent of the service and transports.
- WP-120 owns all operation orchestration, current authorization, obligation
  application, pagination, waits, sanitized DTOs, and audit emission.
- WP-130 and WP-140 are conversion/protocol packages, not alternate
  application services.
- WP-160/WP-170/WP-180 can proceed in parallel after WP-120 by targeting their
  core semantics while WP-185 owns mechanical service-port adapters.
- Server-side cursors are bounded and safely invalidated by restart; they do not
  promise snapshot isolation across a changed scan epoch.
- Required audit introduces an ordered coordinator write before protected
  admission, a bounded permit plus fresh exact-facts authorization immediately
  before synchronous acceptance, and a terminal audit write after the complete
  safe result is filtered and bounded but before release. Bootstrap embeds its
  first started record in the typed authoritative transition so the audit itself
  cannot violate emptiness. Formal WP-065 supplies the earlier durable
  storage/proto mapping.
- Adding a service operation, safe point, obligation interpretation, pagination
  guarantee, or bypass dependency requires architecture/security review.

## Compatibility

The Rust service traits are internal interfaces, but their operation identities,
result semantics, error classes, authorization safe points, obligation behavior,
cursor/page consistency, wait/stream terminal states, cancellation admission
boundaries, audit scope, phase meanings, result-link variants, administration-
clock ownership/sample rules, and append-failure mappings are cross-transport
behavioral compatibility surfaces.

Public compatibility begins when a proto/MCP operation leaves its ADR-0006
phase-zero shell or its ADR-0008 fixture is published. Wire field numbers, cursor
text/bytes, schemas, status mapping, and stream shape then follow their owning
compatibility policy. Service Rust struct layout, async-trait implementation
technique, channel type, fake providers, and composition newtype names are not
public or durable formats.

## Security

Authentication does not imply authorization, discovery does not imply
invocation authority, and no adapter may construct trusted context. Current
policy is checked at every defined safe point. Tenant, partition, field, row,
approval, audit, and output obligations are applied before any adapter sees a
value.

Raw credentials, keys, business values, cursor state, internal records, and
untrusted free-form claims remain outside errors, telemetry, audit summaries,
MCP metadata, and generated descriptions. Cursor tokens are lookup handles, not
authorization proof. Audit outages fail closed for protected reads and new
mutations. Cancellation cannot be represented as a rollback after the
authoritative admission boundary.

The POC remains a local-development service. These interfaces do not claim
remote OAuth/TLS, distributed rate-limit, multi-node cursor, or replicated audit
semantics.

## Testing

WP-120 freezes behavior with one transport-neutral scenario corpus and fake
ports:

- Every operation has allow, deny, unknown-obligation, stale-capability,
  cancellation, deadline, oversized-input, and redaction cases as applicable.
- Call-order assertions prove semantic validation, current authorization, audit,
  executor/read-port call, obligation application, and result release occur in
  the required order.
- Architecture tests reject banned Cargo dependencies/imports and direct
  adapter construction of executor, policy, storage, or authenticated types.
- Command tests cover new admission, pending resume, terminal replay,
  historical-plan normalization, input mismatch, three-observation churn,
  revocation before/after admission, dropped response receiver, fresh outer
  request IDs with stable mutation idempotency identity, and `OutcomeUnknown`
  recovery. Read-only command cases prove there is no pending or
  terminal command-idempotency record, persisted outcome, provenance, or
  application sequence and that any required audit uses only the administration
  stream.
- WP-127/WP-130 Execute fixtures cover journaled commit, replay, and unjournaled
  read-only success; they reject unknown statuses, read-only fields with nonempty
  terminal metadata, journaled statuses with missing terminal metadata, and any
  attempt to construct a semantic zero sequence from a wire sentinel.
- Policy matrices cover tenant/partition pushdown, row-limit lowering, entity/
  commit/provenance/outbox/projection filtering, complete command/projection
  results, approval, and fail-closed unknown obligations.
- Cursor tests cover query/principal binding, epoch/generation change, expiry,
  restart invalidation, capacity, retrying a lost page, policy narrowing, and no
  raw state in tokens or diagnostics.
- Wait tests use explicit hooks and cover cancellation/wake races, stale/spurious
  notifications, exact deadline boundaries, reauthorization, and no resource
  held across `.await`.
- Stream tests cover contiguous catch-up/live handoff, bounded-buffer overflow,
  resume sequence, per-item reauthorization/redaction, lifetime, cancellation,
  shutdown, and no silent drops.
- Audit tests prove initial authorization, durable start, bounded permit, fresh
  exact-facts authorization, and synchronous admission in that order; exactly one
  terminal selection per started invocation; complete filtering/bounding before
  success; establishment-only stream audit; succeeded links for new execution,
  pending resume, and terminal replay; all post-context denials; the exact
  standalone/start/denial/terminal failure matrix; no recursive audit; independent
  targets and all-or-nothing result links; canonical but nonmonotonic timestamps;
  exact administration-clock sample counts and outages; pre/post-crash states;
  and secret/value canaries.
- Bootstrap audit tests prove the atomic consecutive started/transition pair,
  marker linkage, a replay-specific started/succeeded pair without a second
  capability transition, no durable record for malformed or mismatched
  candidates, and fail-closed compound-commit or terminal-audit uncertainty.
- Capability-mutation tests advance the injected clock during queue wait and
  mutate lifecycle, revision, audience, delegation, and approval state before
  the transaction-current check. Architecture tests prove commit imports only
  the narrow policy preparation/facts/verifier/clock surface, facts lowering
  copies every required checked field, and commit contains no duplicate policy
  predicate or general authorizer.
- Panic tests prove contained defects receive one incident and release cursors,
  waiters, and executor receivers while coordinator corruption fails readiness.

WP-130 runs the same canonical service scenarios through gRPC and checks exact
proto/error/stream conversion. WP-140 runs them through MCP HTTP and stdio-over-
gRPC, including stale-name denial, structured result/schema conformance,
progress, cancellation, pagination, administrative audit, and secret canaries.
WP-125 compares the in-process service against the shared workload oracle.
WP-185 uses real projection/outbox/health adapters. WP-200 compares normalized
observable results across every exposed path; protocol-only presentation may
differ, semantic outcomes and authorization may not.

## Requirements and Work Packages

- **Requirements:** `SYS-004`, `API-001`, `ID-005`, `SEC-001` through `SEC-004`,
  `TXN-010`, `TXN-040` through `TXN-044`, `MCP-001`, `MCP-010`, `MCP-011`,
  `MCP-020` through `MCP-024`, `MCP-030`, `MCP-033`, `MCP-041`, `MCP-043`
  through `MCP-046`, `PRJ-001` through `PRJ-004`, `POC-008`
- **Defines or blocks:** foundational/storage follow-ups in `WP-010`, `WP-060`,
  and `WP-070`; formal durable/public proto-owner packages `WP-065` and
  `WP-127`; WP-100 executor result/audit ports;
  `WP-110`; `WP-120`; `WP-125`; `WP-130`; `WP-140`; `WP-150`; source ports
  for `WP-160`, `WP-170`, and `WP-180`; and adapters in `WP-185`
- **Final evidence:** `WP-190`, `WP-200`

## Accepted Decisions

Acceptance of this exact text approved these choices rather than inferring them
from the earlier direction approval:

1. Resolve the SPEC Section 4.1/WP-100 tension by giving WP-100 the complete
   lower command lifecycle while WP-120 owns API orchestration, policy,
   obligations, and safe result release.
2. Require the exact initial-authorization/start/bounded-permit/fresh-
   authorization/synchronous-admission/terminal lifecycle around every audited
   operation, with one terminal phase per started invocation and fully safe output
   before success; audit every explicit post-context policy denial, end stream
   audit at establishment, use the exact append-failure matrix, and retain
   malformed authentication and unknown preclassification Execute targets as
   bounded transport telemetry.
3. Add the specialized service-audit transition to ADR-0004/storage/proto and
   apply the exact full-manifest delta above, including formal WP-065/WP-127 and
   the required storage/audit test paths.
4. Approve the policy/service/commit/storage/proto ownership chain for admitted
   provenance claims, including stored-claim reuse and their exclusion from
   ADR-0012 deterministic runtime context.
5. Resolve SPEC Section 22.2 by keeping grammar-v1 read-only commands
   unjournaled, with no durable command admission/outcome/provenance/application
   sequence and no replay promise for an optional request-correlation key; keep
   required service audit separate in the administration stream, together with
   ADR-0004's one-partition/upfront-capability/indexed-range restrictions. Approve
   the additive `EXECUTED_READ_ONLY = 3` Execute status, its exact zero/empty wire
   sentinels, sentinel-to-absence mapping, and fail-closed rejection rules without
   admitting a semantic zero sequence.
6. Use bounded process-local server-side cursors with 16-byte random handles,
   three-attempt collision handling, five-minute expiry, restart invalidation,
   scan fences, and no signed cursor format; approve direct `getrandom` 0.3.4 in
   `riffdb-server` with default features disabled and no optional features under
   the reviewed dependency constraints above.
7. Accept the 30-second wait, 15-minute stream, cursor/subscriber capacities,
   and remaining service hard bounds in this record.
8. Accept the 22-operation internal inventory, including MCP-only discovery,
   provenance, projection-status, and optional outbox methods without adding
   unreviewed gRPC RPCs.
9. Use consumer-owned authoritative/projection/outbox/operational ports with
   mechanical server composition adapters, rather than direct service access to
   persistence ports or dependency cycles with later crates.
10. Approve the exact SPEC Section 5.2 rows above: gRPC and MCP HTTP may call only
    the auth-owned `CredentialAuthenticator` before the service, and MCP loses its
    direct policy dependency.
11. Approve policy ownership of `AuthorizationClock`, commit ownership of the
    separate `AdministrationClock`, canonical rather than monotonic clock values,
    exact administration timestamp sampling, and reuse of the validated
    transaction-current authorization value for normal capability transition
    timestamps. Approve value-only authorized capability-mutation preparations,
    transaction-current facts, and the pure verifier; permit commit to depend only
    on those narrow policy interfaces while retaining the ban on a general
    authorizer, obligations/redaction, or policy storage-reader access in commit.
12. Approve bootstrap's atomic principal-less started/authoritative pair, exact-
    replay started/terminal pair, and telemetry-only treatment of failed
    principal-less candidates instead of a standalone pre-bootstrap audit write.
13. Accept the atomic governance rule: ADR-0004, ADR-0007, ADR-0009, ADR-0012,
    ADR-0017, every named authoritative amendment, formal WP-065/WP-127, and the
    manifest delta land in one commit; partial application is invalid.

## Decision Deadline

The command/control-plane/audit executor split must be accepted before WP-100
publishes ports. Request context, service traits/DTOs, safe points, obligations,
cursors, waits, streams, and late-subsystem ports must be accepted before
WP-120 merges them. WP-065 must merge before WP-070; WP-127 and its reviewed
public fields must merge before WP-130. Public fields require ADR-0006 review, and
MCP names/URIs/text cursor mappings require ADR-0008 acceptance.

The atomic governance commit satisfies this decision deadline. No package may
depart from the now-frozen semantic/durable interfaces, claim durable MCP-046
evidence before its assigned packages pass, or substitute best-effort telemetry.
