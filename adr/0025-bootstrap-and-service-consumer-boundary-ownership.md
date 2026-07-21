# ADR-0025: Bootstrap and Service Consumer Boundary Ownership

- **Status:** Accepted
- **Direction approved:** 2026-07-21
- **Exact text accepted:** 2026-07-21
- **Accepted:** 2026-07-21
- **Requires:** ADR-0004, ADR-0007, ADR-0009, ADR-0018, ADR-0021, and
  ADR-0023
- **Clarifies:** The WP-110 bootstrap-secret handoff and the WP-120/WP-130
  consumer-port, create-invocation, and production-ingress boundaries
- **Decision deadline:** Before WP-110 extends bootstrap preparation or WP-120
  freezes its service interfaces

The human maintainer accepted these exact narrow ownership and construction
rules on 2026-07-21. This record changes no token format, policy predicate,
service operation, public Protobuf field, durable record, or command semantic.

## Context

The accepted architecture fixes the behavior but leaves five adjacent Rust
interface choices implicit. ADR-0009 says `riffdb-auth` owns checked bounded
`BootstrapDigestCandidates` and consumes the raw bootstrap credential before
the API-neutral service is called. ADR-0007 says the service receives only
checked semantic reads, owns six traits containing exactly 22 operations, and
uses a privately checked `BootstrapRequestContext`. WP-100 additionally needs
the service to recover an absent revoke target that appears between initial
authorization and the transaction-current check.

Those requirements can be implemented in superficially compiling but unsafe
ways: passing a raw token or an untyped digest vector through the service,
returning a storage capability record to the service, representing normal and
bootstrap create with independently selectable mode and context values, or
letting a test ingress constructor enter the production graph. Catalog access
also needs a consumer boundary that does not give the service a storage
repository or create a second interpretation of catalog state.

## Decision

### Auth-owned bootstrap token preparation

`riffdb-auth` owns a distinct `BootstrapDigestCandidates` type. It is bounded,
nonserializable, redacted under formatting, privately constructed, and
move-only: it does not implement `Clone` or `Copy`. Its only production
constructor is the capability digest-key provider operation
`prepare_bootstrap_token(RawCapabilityToken)`.

That operation takes the already validated, zeroizing `RawCapabilityToken` by
value, computes exactly one candidate for every readable capability key in
newest-to-oldest provider order, identifies the current-key candidate, and
drops the raw token before returning. The returned value exposes only read-only
`as_slice()` and `current()` access to checked
`CapabilityTokenDigest` values. It exposes no raw secret, digest-key bytes, key
handle, mutable candidate collection, arbitrary constructor, serialization, or
secret-bearing debug output.

The trusted adapter moves this value through the fields-private checked service
`BootstrapRequestContext` construction entry point. The service may
mechanically lower `as_slice()` and `current()` into WP-100's checked bootstrap
preparation. `riffdb-service` and `riffdb-commit` do not receive the raw token or
depend on digest-key custody, and `riffdb-commit` does not gain a dependency on
`riffdb-auth`.

### Service-owned authoritative revoke-target read

`riffdb-service` owns `AuthoritativeReadPort`. In addition to ADR-0007's
authoritative read operations, it has a bounded
`read_capability_revoke_target(CapabilityId)` operation used only to prepare and
retry `RevokeCapability` authorization.

The result is a service-owned, digest-free, closed absent-or-present durable
observation. A present observation contains only the checked capability ID,
revision, activity, database, environment, principal, actor kind, canonical
audiences, issue and expiry times, and complete grant. An absent observation
contains only the requested capability ID and the trusted database/environment
scope; it invents no revision, activity, principal, audience, validity interval,
or grant. Neither variant contains an invocation `RequestId`, token digest,
token lookup, raw credential, digest-key identity, storage record, repository,
transaction, or mutation handle.

`riffdb-policy` retains sole ownership of `CapabilityRevokeTargetFacts` and
`AbsentCapabilityRevokeTargetFacts`. The service combines its invocation
`RequestId` with the returned observation through those existing checked policy
constructors. The service observation does not implement a policy predicate or
act as an alternate policy-facts type; its fields are the minimal mechanical
read bridge needed to construct the policy-owned value.

WP-130 owns the production mechanical adapter from the lower specialized
capability reader to this snapshot. The adapter performs checked field copying
and absence classification only. It performs no policy predicate, retry,
mutation, or error-redaction decision. WP-120 owns the absent-to-present reload,
reauthorization, fresh executor-permit acquisition, and resubmission behavior
already required by SPEC and WP-120.

### One closed capability-create invocation

`AdministrationApplication` retains one `create_capability` operation and the
closed operation inventory remains exactly 22 methods. Its API-neutral input is
the service-owned closed enum `CreateCapabilityInvocation` with exactly these
variants:

- `Normal`, pairing an ordinary checked `RequestContext` with a normal create
  request; and
- `Bootstrap`, pairing a privately constructed `BootstrapRequestContext` with
  the bootstrap request.

There is no independent mode flag at the service boundary and no constructor
that can pair a normal context with bootstrap semantics or a bootstrap context
with normal semantics. The gRPC adapter maps the accepted public create-mode
enum totality into this closed service enum; zero and unknown public modes still
reject. Adding the enum does not add a service method, RPC, or mutation path.

### Service-owned checked catalog read port

`riffdb-service` owns `CatalogReadPort`. It returns only bounded, checked catalog
semantic values produced by `riffdb-catalog`, including the active/versioned
bundle, executable-plan, lineage, and deployment-preparation views required by
the service operations. It does not return `CatalogRepository`, a storage API
trait, an active-pointer record without its checked catalog interpretation, a
table/key/value interface, transaction, or mutation handle.

WP-130 owns the production mechanical adapter. It calls the catalog-owned
validation and lookup operations against the production repository and maps
their already checked values and closed errors into the service port. The
adapter does not duplicate bundle validation, compatibility classification,
lineage traversal, plan resolution, deployment policy, or coordinator
submission. Catalog deployment remains a typed WP-100 control-plane operation;
this read port grants no write authority.

### Production bootstrap ingress

The lower shared `ServiceIngressKindV1::InProcessTestComparison` value and the
WP-100 checked bootstrap preparation's ability to consume it are retained for
deterministic service, comparison, and coordinator tests. This does not create
a production bootstrap ingress.

The production `riffdb-service::BootstrapRequestContext` is intrinsically gRPC.
Its fields are private, and its public checked construction entry point requires
an auth-owned `BootstrapDigestCandidates`, fixes
`ServiceIngressKindV1::Grpc`, and accepts no caller-selected ingress value. The
trusted WP-130 loopback gRPC bootstrap adapter is its sole production call site,
after applying ADR-0009's listener, metadata, and raw-token preparation checks.
Any in-process test/comparison construction entry point is available only under
`cfg(test)` or from the non-production testkit/comparison graph and cannot be
selected through a Cargo production feature.

WP-130 architecture tests must prove that the production `riffdbd` component
graph has no reference to the test constructor or
`InProcessTestComparison` bootstrap path. MCP, ordinary gRPC, CLI/SDK
convenience, and direct storage remain unable to bootstrap through another
path. The production behavior remains exactly the loopback gRPC-only bootstrap
required by SPEC and ADR-0009.

## Options Considered

1. **Distinct auth-owned move-only bootstrap candidates:** accepted; it makes
   raw-secret consumption and checked bootstrap authority visible in the type
   system without coupling commit to auth.
2. **Reuse an untyped digest vector or pass the raw token onward:** rejected;
   either loses construction authority or crosses the service secret boundary.
3. **Return a storage capability record for revoke preparation:** rejected; it
   leaks token lookup/durable representation and gives the service a lower
   storage vocabulary it does not own.
4. **Independent create mode and context arguments:** rejected; their Cartesian
   product admits invalid normal/bootstrap combinations and invites divergent
   adapter checks.
5. **Direct catalog repository access from the service:** rejected; it violates
   the consumer-owned semantic-port rule and lets service code reinterpret raw
   catalog persistence.
6. **Delete the in-process bootstrap ingress from all lower layers:** rejected;
   deterministic comparison tests need the accepted test ingress, while the
   gRPC-hardcoded service entry point and production graph checks enforce the
   real boundary.

## Consequences

- Raw bootstrap credentials have one short auth-owned lifetime and cannot be
  accidentally retained by service or coordinator DTOs.
- Revoke authorization can reload a newly appeared target without a storage
  bypass or disclosure of token digests.
- Normal and bootstrap creation share one service operation while invalid
  context/mode combinations are unrepresentable.
- Catalog and authoritative read implementations can be replaced by
  deterministic fakes in WP-120 without weakening production semantics.
- WP-130 must provide two small mechanical adapters and production graph checks.
- The lower test ingress remains visible in shared value types; review must
  distinguish that test vocabulary from constructible production authority.

## Compatibility

This decision changes internal Rust source interfaces only. It changes no
public Protobuf tag or field, durable Protobuf envelope or record, storage key,
contract grammar, typed IR, plan hash, canonical input hash, idempotency
identity, commit ordering, capability digest bytes, or token text. Public
`CreateCapabilityRequest` mode values remain unspecified `0`, normal `1`, and
bootstrap `2` with zero and unknown values rejected.

Any future production ingress beyond loopback gRPC, service method split,
serializable bootstrap candidate, digest-bearing service read, or raw catalog
repository exposure requires a new reviewed compatibility and security
decision.

## Security

All five rules are fail-closed boundaries. A raw bootstrap token is consumed in
auth and zeroized on drop. Candidate digests are redacted and grant no authority
outside the private bootstrap context. Revoke snapshots omit all credential and
lookup material and are reauthorized at every accepted safe point. The closed
create invocation prevents context confusion. The catalog and authoritative
read ports confer observation only, never storage or mutation capability.

The in-process bootstrap path is test evidence, not a production transport. The
loopback gRPC adapter remains the sole production call site of the checked
construction entry point, and the shared application service, policy layer,
service audit, and commit coordinator remain mandatory. No MCP or direct-storage
bootstrap exception is introduced.

## Testing

WP-110 tests candidate count/order, exact `current()` selection, private
construction, move-only ownership, redacted formatting, and token cleanup. A
compile-fail or equivalent architecture test proves that candidates are not
cloneable or serializable and that raw token bytes cannot be recovered.

WP-120 architecture and integration tests freeze the six-trait/22-operation
inventory, exhaustively match both `CreateCapabilityInvocation` variants, prove
the service has no storage-engine or raw-token boundary, and exercise
absent-target reload through a digest-free fake snapshot. Catalog-port tests
compare fake responses with catalog-owned checked values and prove storage
errors fail closed rather than becoming empty catalog state.

WP-130 component-graph tests prove that production bootstrap context
construction fixes gRPC ingress, no test constructor or in-process bootstrap
path enters the production feature graph, and neither adapter performs policy
or storage mutation. Its public gRPC test covers normal/bootstrap mode
separation, and its process-level restart test covers real loopback bootstrap,
authenticated deployment, command execution, and reads. WP-200 supplies final
cross-transport non-bypass evidence.

## Requirements and Work Packages

- **Requirements:** `SYS-004`, `API-001`, `ID-005`, `SEC-001`, `SEC-002`,
  `SEC-003`, `SEC-004`, `POC-008`, `POC-009`
- **Defines or blocks:** `WP-110`, `WP-120`, and `WP-130`
- **Final evidence:** `WP-130` for the first runnable production graph and
  `WP-200` for final cross-transport evidence

## Decision Deadline

This exact text is accepted before WP-110 publishes the bootstrap preparation
and before WP-120 publishes its consumer ports, contexts, and 22-operation
service surface. WP-130 must freeze the production-only construction and
mechanical-adapter evidence before claiming the first runnable P1 gate.
