# ADR-0044: Hosted MCP Context and Dependency Completion

- **Status:** Accepted
- **Direction approved:** 2026-07-23
- **Exact text accepted:** 2026-07-23
- **Accepted:** 2026-07-23
- **Acceptance reference:** Human maintainer confirmation in the current Codex
  session on 2026-07-23
- **Requires:** ADR-0007, ADR-0008, ADR-0018, ADR-0037, ADR-0040, and ADR-0043
- **Amends:** SPEC Sections 5.2, 5.3, 5.4, 9.1, 12.2, and 13.1; the ADR
  register; and WP-140's required ADRs, allowed paths, dependency evidence,
  deliverables, acceptance commands, and exit gate
- **Clarifies:** Service-owned hosted-MCP request-context construction,
  foundational MCP dependency ownership, and the mandatory rmcp telemetry
  suppression boundary
- **Decision deadline:** Before the WP-140 dependency graph or hosted HTTP
  implementation merges

The human maintainer explicitly confirmed this exact corrective direction in
the current Codex session on 2026-07-23. This record supplies interfaces that
the accepted hosted MCP design already requires but the current service and
manifest surfaces cannot express. It adds no authentication or authorization
decision, request-context claim carrier, public or durable field, command
semantic, storage access, or MCP capability.

## Context

Hosted Streamable HTTP must authenticate through
`CredentialAuthenticator` and then call the same API-neutral service used by
gRPC. The current general `RequestContext::new` requires policy-owned
`UntrustedInvocationClaims`. The only closed transport constructor fixes gRPC
ingress. Letting `riffdb-api-mcp` import policy only to create an empty claim set
would violate the accepted adapter boundary, while labelling hosted calls as
gRPC would corrupt trusted ingress and audit data.

Two related dependency omissions also prevent the accepted adapter design:

- ADR-0018 makes `riffdb-api-mcp` the consumer-side owner of the hosted
  `RequestIdSource`, which must return the canonical checked
  `riffdb_types::RequestId`.
- HTTP service failures and stdio public-client failures contain the same
  `riffdb_errors::PublicError`; a single common MCP renderer must consume that
  owner rather than duplicate error classification and safe detail mapping.

Finally, ADR-0008 requires both MCP binaries to suppress all pinned-SDK tracing
events whose target is `rmcp` or begins `rmcp::`. The stdio manifest currently
cannot install that filter.

## Decision

### Service owns hosted context construction

`riffdb-service` adds exactly this closed constructor:

```rust
pub const fn from_authenticated_mcp_http(
    request_id: RequestId,
    principal: AuthenticatedPrincipal,
    control: RequestControl,
    trace: Option<TraceContext>,
) -> Self
```

It delegates to the existing general constructor with:

- `ServiceIngressKindV1::McpHttp`;
- `UntrustedInvocationClaims::new(None, None, None, None, None)`;
- the supplied checked `RequestId`, privately constructed
  `AuthenticatedPrincipal`, process-local `RequestControl`, and optional
  already-trusted `TraceContext`.

The constructor has no ingress parameter, claims parameter, raw byte/header
parameter, credential parameter, MCP identifier, or public field. It does not
authenticate, authorize, generate an identifier, read a clock, create audit
authority, or apply obligations.

WP-140 initially passes no trace context unless an already-trusted
server-composition carrier exists. MCP JSON-RPC IDs, session IDs, progress
tokens, credentials, tool arguments, and transport headers are not trace
authority and must not be converted into `TraceContext`.

The accepted MCP schemas have no request-context provenance-claim carrier.
MCP session identity must never be promoted to `AgentSessionId`. Adding a
separately supplied agent session or any other invocation claim requires a
reviewed carrier and a new accepted decision. Empty claims are therefore the
only WP-140 construction.

### Foundational dependencies are direct and unconditional

`riffdb-api-mcp` adds these exact direct rows:

```toml
riffdb-errors = { version = "0.1.0", path = "../riffdb-errors", default-features = false }
riffdb-types = { version = "0.1.0", path = "../riffdb-types", default-features = false }
tracing = { version = "=0.1.44", default-features = false, features = ["std"] }
```

`riffdb-types` supplies checked identifiers and structural values. It grants no
entropy source, clock, authentication, policy, storage, sequence assignment, or
service authority. The hosted `RequestIdSource` returns a checked `RequestId`;
source failure stops before context construction, service invocation, or
durable administration audit. MCP protocol and session identifiers are never
substitutes.

`riffdb-errors` supplies the one public-safe failure vocabulary shared by
service and public client. `riffdb-api-mcp` owns one conversion from a borrowed
`PublicError` into its bounded presentation failure. Transport-specific closed
failures remain backend inputs to local closed cases. MCP never constructs a
`PublicError` from peer text and exposes no reverse conversion.

These rows are unconditional because ADR-0008's exact crate feature lists add
only transport-specific edges. Ungated common code may consume foundation
types, public-safe errors, and tracing metadata, but still has no auth, service,
server, policy, runtime, commit, catalog, storage API, or storage implementation
edge.

### Telemetry suppression is common and non-reloadable

`riffdb-api-mcp` owns one ungated target predicate:

```text
target != "rmcp" && !target.starts_with("rmcp::")
```

It also owns the closed, bounded, redaction-safe MCP transport events. The
predicate rejects exact `rmcp` and every `rmcp::` descendant; similar unrelated
targets such as `rmcp2` are not classified as SDK targets.

`riffdb-mcp-stdio` adds:

```toml
tracing-subscriber = { version = "=0.3.23", default-features = false, features = ["fmt"] }
```

It installs the common predicate as the final outer global filter, outside all
user-controlled directives and every reload handle. A permissive user filter
cannot re-enable a rejected event. Subscriber installation failure is fatal
before MCP service starts. Formatting explicitly writes to stderr and disables
ANSI; stdout remains protocol-only. No `env-filter`, `ansi`, `json`,
`tracing-log`, or default feature is enabled by this row.

WP-185 must reuse the same predicate when it composes hosted MCP into
`riffdbd` after WP-180. WP-140 owns the predicate, stdio installation, hosted
registration hook, and harness evidence; it does not edit the server binary.

### Runtime and scheduling features

`riffdb-mcp-stdio` uses Tokio's current-thread runtime with exactly `macros` and
`rt` as direct features. It does not require `rt-multi-thread`.

`riffdb-api-mcp` retains Tokio's `time` feature only for the production
implementation behind the injected adapter-local monotonic scheduler. Session
expiry, polling, rate, wait, and notification decisions consume injected,
testable state. Tokio time is not a semantic wall clock, deterministic command
input, logical time, identifier source, or process-global authority.

### WP-140 corrective scope

WP-140 may additionally edit only:

```text
crates/riffdb-service/src/context.rs
crates/riffdb-service/tests/architecture.rs
```

The service manifest, policy crate, general service orchestration, public
protocol, and durable formats remain outside this correction. WP-140 acceptance
must compile and test both MCP transport features and the service constructor;
default-empty feature tests alone are insufficient.

## Consequences

- Hosted MCP can construct a correctly branded service context without
  importing policy or fabricating claims.
- The durable administration audit receives truthful `McpHttp` ingress.
- Checked request identity and public-safe failure mapping retain one canonical
  owner each.
- Common rendering can remain byte-identical across HTTP service DTOs and stdio
  public-gRPC DTOs without duplicating an error registry.
- SDK request, result, credential, and session canaries cannot escape through
  pinned rmcp tracing when the required process filter is installed.
- The production stdio graph remains a public client with no auth, service,
  policy, server, runtime, commit, catalog, or storage dependency.

## Rejected Alternatives

1. **Add `riffdb-policy` to MCP:** rejected because an adapter must not own or
   construct policy vocabulary.
2. **Use the gRPC constructor for hosted HTTP:** rejected because it falsifies
   trusted ingress and audit data.
3. **Accept arbitrary claims or infer an agent session from MCP state:**
   rejected because no accepted carrier exists and MCP identifiers are
   transport-only.
4. **Return raw UUID bytes from `RequestIdSource`:** rejected because it
   duplicates canonical UUIDv7 validation.
5. **Hide `RequestId` behind a service re-export:** rejected because a direct
   foundation edge names the actual owner and creates no authority cycle.
6. **Map `PublicError` independently in both adapters:** rejected because it
   creates two compatibility and redaction mappings.
7. **Place the rmcp filter inside a user/reloadable layer:** rejected because it
   could be removed or overridden.
8. **Use Tokio's multithread runtime for stdio by default:** rejected because
   the bridge needs no multithread scheduling authority.

## Compatibility

The service constructor and Cargo edges are additive internal Rust interfaces.
They change no MCP JSON-RPC shape, HTTP field grammar, Protobuf field, gRPC
method, storage key, durable envelope, contract source, IR, hash, schema
artifact, locator, cursor, or accepted fixture byte.

Changing ingress, accepting nonempty claims, deriving trace context from
untrusted MCP data, moving the request source, adding an authority dependency,
duplicating public-error mapping, or weakening the telemetry filter requires
renewed human review.

## Security

Authentication remains auth-owned and authorization remains service/policy
owned. The new constructor can join checked inputs but cannot create any of
them. Empty claims prevent accidental trust elevation. Foundation dependencies
carry values and safe errors only. The hard tracing filter is defense in depth
against a reviewed SDK that may log complete protocol values; all first-party
events remain bounded and redaction-safe.

## Testing

WP-140 must prove:

- the hosted constructor fixes `McpHttp` ingress and all five claims to absent;
- the constructor exposes no ingress, claims, raw bytes, header, credential, or
  MCP identifier parameter;
- only a typed optional `TraceContext` can carry trusted trace propagation;
- hosted code uses the closed constructor and has no policy dependency;
- request-source failure occurs before context construction and audit;
- MCP protocol/session/progress identifiers never become RiffDB request or
  agent-session identity;
- one common `PublicError` conversion serves both backends and trusts no peer
  text;
- exact target-boundary cases and permissive user-filter tests keep every
  `rmcp`/`rmcp::*` canary out while retaining a safe RiffDB event;
- stdio stdout contains only MCP protocol frames and diagnostics use stderr;
- subscriber collision or install failure stops before service; and
- feature-tree tests retain the accepted authority boundaries.

WP-185 repeats the hard-filter and hosted-context evidence in the composed
`riffdbd` process. WP-200 supplies final cross-transport authorization, audit,
and recovery evidence.

## Requirements and Work Packages

- **Requirements:** `API-001`, `MCP-011`, `MCP-041`, `MCP-043`, `MCP-044`,
  `MCP-046`, and `POC-008`
- **Defines or blocks:** `WP-140` and hosted composition in `WP-185`
- **Final evidence:** `WP-140`, `WP-185`, and `WP-200`

## Decision Deadline

This exact record was accepted before the corrective constructor or dependency
graph merged. Acceptance does not pre-accept a generated `Cargo.lock`; the
resolved direct and transitive graph retains ADR-0008's separate exact-byte
human review requirement.
