# ADR-0043: Auth-Owned Retained MCP Credential

- **Status:** Accepted
- **Direction approved:** 2026-07-23
- **Exact text accepted:** 2026-07-23
- **Accepted:** 2026-07-23
- **Acceptance reference:** Human maintainer confirmation in the current Codex
  session on 2026-07-23
- **Requires:** ADR-0007, ADR-0008, ADR-0009, and ADR-0040
- **Amends:** SPEC Sections 5.2, 5.3, and 12.2; the ADR register; and
  WP-140's required ADRs, exact allowed paths, deliverables, acceptance
  evidence, and exit gate
- **Clarifies:** ADR-0007's adapter-to-authentication boundary, ADR-0008's
  Streamable HTTP credential handoff, and ADR-0009's raw-token ownership and
  zeroization rules
- **Decision deadline:** Before WP-140 implements or merges the Streamable HTTP
  credential handoff

The human maintainer explicitly confirmed acceptance of this decision in the
current Codex session on 2026-07-23. This record adds one bounded, auth-owned
temporary credential container needed by WP-140. It does not add an
authentication decision, token format, credential source, session credential
cache, dependency owner, public protocol field, durable field, or policy path.

## Context

ADR-0007 requires every transport adapter to pass an extracted credential to
the one auth-owned `CredentialAuthenticator`. ADR-0008 additionally requires
the Streamable HTTP adapter to remove the authorization header before rmcp
dispatch and forbids the adapter from decoding a bearer token or caching an
authentication decision. ADR-0009 gives `riffdb-auth` sole ownership of the
core raw-token and secret-buffer boundary.

The existing `OpaqueCredential<'a>` is a borrowed view. WP-140's HTTP adapter
needs a small owned value that can preserve the already bearer-stripped bytes
across its adapter handoff and then recreate only that borrowed view for the
synchronous authenticator call. Letting MCP own a general byte container or
token-shaped type would duplicate the secret boundary and could accidentally
grow syntax, decoding, logging, or authentication behavior in the transport.
Making the helper validate the exact token grammar would also turn its
construction result into a second authentication decision.

## Decision

### Auth owns the retained opaque value

`riffdb-auth` owns one public, fields-private
`RetainedOpaqueCredential`. Its implementation lives in
`crates/riffdb-auth/src/authenticator.rs` beside `OpaqueCredential` and
`CredentialAuthenticator`.

The value has these exact properties:

- it is neither `Clone` nor serializable and exposes no serde implementation;
- its credential storage is the existing auth-owned
  `zeroize::Zeroizing<[u8; 43]>`, plus only private length metadata needed to
  identify the initialized prefix;
- `Debug` and `Display` are redacted and never include length, input bytes,
  token syntax, a digest, or authentication details;
- construction accepts any already bearer-stripped byte slice whose length is
  at most 43 bytes, copies it into the zeroizing buffer, and records its exact
  length;
- construction rejects only a length greater than 43 and exposes no input
  bytes in that bounded failure;
- construction performs no empty check, exact-length check, alphabet check,
  base64 operation, token parsing, digest computation, capability lookup,
  clock read, authentication, authorization, or telemetry; and
- its sole public data accessor is
  `borrow(&self) -> OpaqueCredential<'_>`, which borrows exactly the recorded
  prefix. It exposes no byte slice, mutable view, owned extraction, iterator,
  formatter, serialization, or conversion into token text.

Empty or malformed input within the bound therefore remains opaque and
construction supplies no judgment about it. If such a value reaches
authentication, only the unchanged `CredentialAuthenticator` evaluates it.
The helper is not evidence that a credential is present, syntactically valid,
authenticated, authorized, or bound to a session.

The 43-byte capacity is the existing canonical capability-token text bound. It
does not change the protocol rule that a normal Streamable HTTP Authorization
value contains case-sensitive `Bearer ` followed by exactly 43 canonical token
bytes. Transport framing may reject an absent, malformed, or over-bound value
before authentication under ADR-0008's existing rules. Those framing failures
and the authenticator's short, malformed, unknown, expired, revoked, or
mismatched failures retain the existing generic unauthenticated public
behavior.

### MCP receives retention, not authority

The WP-140 `streamable-http` implementation in `riffdb-api-mcp` may construct
`RetainedOpaqueCredential` only from the bearer-stripped credential extracted
under ADR-0008's ordered HTTP checks. It may call `borrow()` only to pass the
result to the injected `CredentialAuthenticator`.

The adapter still:

- calls the same `CredentialAuthenticator` used by gRPC;
- receives only the authenticator's privately constructed
  `AuthenticatedPrincipal` or closed authentication failure;
- constructs no actor, capability fact, policy decision, obligation, or
  service authorization result;
- performs no token decode, digest, capability read, policy call, or auth
  cache;
- removes the Authorization header before rmcp dispatch;
- stores only ADR-0008's authenticated `CapabilityId` and clock binding in an
  active session, never the retained credential or its borrowed view; and
- moves exactly one retained value into a live SSE response handler when that
  request establishes a stream, uses only `borrow()` for fresh authentication
  before each emitted notification, and releases it when the handler closes;
  every non-streaming request releases its retained value after its
  authentication handoff, without logging, metrics, audit, provenance, MCP
  content, public error, or session serialization.

An SSE response handler is not MCP session state. It owns no cached principal,
policy decision, obligation, or authorization result. Reauthentication failure
closes or suppresses output under ADR-0008's existing fail-closed rules; it
never falls back to the initialization principal or capability binding.

`OpaqueCredential`, `RetainedOpaqueCredential`, and
`CredentialAuthenticator` remain auth-owned. MCP receives no constructor for
`AuthenticatedPrincipal`, no access to raw capability-token types, and no
alternative authentication entry point. Production stdio remains a public
gRPC client and does not gain an auth dependency.

### Dependency and package ownership

This decision adds no Cargo dependency and changes no reviewed direct-owner
set. `riffdb-auth` already directly owns exact `zeroize = 1.8.1` with default
features disabled and `alloc` enabled. The direct `zeroize` owners remain
exactly `riffdb-auth`, `riffdb-client-rust`, and `riffdb-cli`.

WP-140 alone receives permission to edit these two additional exact paths:

```text
crates/riffdb-auth/src/authenticator.rs
crates/riffdb-auth/tests/architecture.rs
```

The source path owns the helper and its inline semantic tests. The architecture
test path freezes source ownership, non-clone/non-serialization constraints,
the existing zeroize owner set, and the absence of a new MCP authentication
authority. WP-140 receives no permission to edit another auth source, the auth
manifest, a digest provider, raw-token implementation, protected-file loader,
public protocol schema, service authorization path, policy crate, or storage
crate.

## Consequences

- Streamable HTTP can retain one bounded credential presentation without
  creating an MCP-owned secret type.
- Each live SSE response handler can perform the required fresh authentication
  for every notification without placing credential bytes or an authentication
  result in reusable session state.
- Malformed input cannot distinguish a helper-validation branch from the
  auth-owned generic authentication path, except for the already permitted
  transport size bound.
- The helper is deliberately less ergonomic than a general secret container:
  it cannot be cloned, serialized, exposed as bytes, or converted into token
  text.
- The 43-byte array is always zeroized on drop, including unused capacity.
  This makes no new claim about copies already made by HTTP/framework values,
  operating-system buffers, allocators, or the caller-provided slice.
- The internal cross-crate Rust interface grows by one narrowly scoped type.
  No public wire, durable, IR, hash, schema, MCP registry, or CLI v1 bytes
  change.

## Rejected Alternatives

1. **Retain `Vec<u8>` or `String` in `riffdb-api-mcp`:** rejected because it
   duplicates the auth-owned raw credential and cleanup boundary.
2. **Make the retained helper parse the canonical token:** rejected because it
   creates a second syntax/authentication decision and can become an oracle.
3. **Expose bytes from the retained helper:** rejected because MCP needs only a
   borrowed `OpaqueCredential` for the authenticator call.
4. **Store the credential in MCP session state:** rejected because sessions
   bind to the authenticated capability identity and must reauthenticate each
   request, not retain a bearer credential or an auth decision.
5. **Add a new secret-container dependency:** rejected because the accepted
   auth-owned `zeroize` edge already supplies the exact bounded cleanup
   mechanism.

## Compatibility

This is an additive internal Rust adapter interface. It changes no MCP JSON-RPC
shape, HTTP header grammar, Protobuf message, gRPC behavior, durable record,
storage key, contract source, IR, schema artifact, locator, cursor, hash, or
checkpoint fixture. Changing its 43-byte capacity, exposing raw data, adding
clone/serialization, moving ownership out of auth, or permitting a second
authentication decision requires renewed human security and compatibility
review.

## Security

The retained value grants possession of temporary credential bytes but no
authority result. Authentication stays fail closed and existence blind through
`CredentialAuthenticator`; authorization and obligation application stay in
the shared service and policy path. Secret bytes must be removed before all
telemetry and externally visible content. Zeroization is defense in depth for
the auth-owned buffer and is not represented as complete memory erasure.

## Testing

WP-140 tests must prove:

- every input length from zero through 43 is copied without syntax validation
  and `borrow()` yields the exact prefix to a test authenticator;
- a 44-byte input is rejected without retaining or formatting input bytes;
- mutation or release of the caller's source buffer does not change the
  retained copy;
- `Debug` and `Display` redact canary credentials;
- source and dependency guards keep the type in
  `riffdb-auth/src/authenticator.rs`, forbid clone/serde/raw accessors, and keep
  the exact existing zeroize owner set;
- HTTP passes only `borrow()` to `CredentialAuthenticator`, performs no local
  decode or auth decision, gives exactly one retained value to each live SSE
  response handler, freshly authenticates every emitted notification, and never
  stores a retained credential in session state; and
- malformed, unknown, expired, revoked, and session-mismatched requests retain
  ADR-0008 and ADR-0009's generic public behavior and redaction canaries.

The existing MCP conformance, architecture, dependency-policy, and secret-canary
tests remain required.

## Requirements and Work Packages

- **Requirements:** `MCP-011`, `MCP-043`, and `MCP-048`
- **Defines or blocks:** `WP-140`
- **Final evidence:** `WP-140`, `WP-185`, and `WP-200`

## Decision Deadline

This exact record must be accepted before WP-140 implements or merges the
Streamable HTTP credential handoff. The auth helper and its architecture guards
must merge as part of WP-140 before the adapter consumes it. This acceptance
does not pre-accept any new dependency or resolved lock graph; those retain
their separate human dependency-review requirement.
