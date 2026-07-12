# ADR-0009: Opaque Server-Side POC Capabilities

- **Status:** Proposed
- **Direction approved:** 2026-07-12
- **Exact text accepted:** No
- **Decision deadline:** Persistence fields before WP-070; full record before WP-110

The human architecture review approved this direction. This record remains
Proposed until its exact text is reviewed and accepted.

## Context

The POC needs revocable, inspectable authorization without treating client claims
as provenance. Token generation, storage, scope, audience, rotation, and
revocation are a cryptographic and durable security boundary.

## Proposed Decision

Capabilities are opaque 32-byte values generated with the operating system CSPRNG
outside deterministic command execution and encoded base64url without padding.
The raw token is returned once and never persisted, logged, traced, measured, or
placed in provenance.

The server stores only
`HMAC-SHA-256(server_secret[key_id, version], raw_token)` with an explicit key
ID/version. Capability records bind a stable capability ID to
environment, database/server identity, explicit allowed audiences, stable
principal, tenant scope, permissions, obligations, issuance/expiry, revocation,
and administrative audit metadata. Authentication selects only configured
canonical audiences; HTTP never derives audience from `Host`.

Capabilities are reloaded and reauthorized on every request so revocation applies
to the next request. Stdio receives a gRPC-scoped token. Creation and revocation
are typed coordinator control-plane operations, not direct storage writes. Client
supplied provenance and capability metadata are untrusted inputs.

## Options Considered

1. **Opaque random token plus HMAC digest:** Approved POC choice with simple
   revocation and no client claims.
2. **Plain cryptographic hash:** Exposes low-entropy or stolen token verification
   to offline guessing if storage leaks.
3. **Self-contained signed token:** Adds claim/version/key-distribution surface and
   weaker immediate revocation.
4. **Persist raw token:** Unacceptable secret exposure.

## Consequences

- Server secret management, rotation, and multi-key verification need explicit
  configuration and tests.
- Lost tokens cannot be recovered; replacement requires a new capability.
- Capability lookup requires HMAC calculation before record access.
- Remote OAuth/TLS identity remains post-POC.

## Compatibility

Token text format, digest algorithm/key version, record encoding, audience
canonicalization, and revocation semantics are durable/security boundaries.

## Security

Use a reviewed pure-Rust cryptographic implementation subject to human approval
under `AGENTS.md`. Compare digests in constant time. Zeroize temporary raw-token
buffers where the selected safe library supports it. Deny unknown key versions,
audiences, expired/revoked records, and environment mismatch without revealing
which check failed.

## Testing

Entropy-source injection tests, format vectors, deterministic HMAC vectors,
constant-time API usage review, raw-token secret canaries, environment/database/
audience/tenant matrices, rotation/revocation tests, crash tests for control-plane
updates, and architecture checks preventing token access in runtime/MCP storage.

## Requirements and Work Packages

- **Requirements:** `ID-005`, `SEC-001` through `SEC-004`, `MCP-043`, `MCP-048`
- **Defines or blocks:** neutral persistence in `WP-060`/`WP-070`; `WP-110`,
  `WP-140`, `WP-185`
- **Final evidence:** `WP-200`

## Decision Deadline

Accept record fields and key-version layout before WP-070 freezes storage. Human
approval of the cryptographic dependency and exact ADR text is required before
WP-110 implements token creation or verification.
