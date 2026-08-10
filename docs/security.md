# Security Posture and Threat Model

RiffDB's alpha target is a controlled Linux deployment. Application gRPC can
use verified direct TLS or a protected Unix socket; hosted MCP remains local.
The system is not hardened for an untrusted internet-facing or shared
multi-tenant service.

## Trust Boundaries

- `riffdbd` owns authoritative storage, key providers, authentication,
  authorization, command execution, sequencing, and atomic persistence.
- `riffdb`, the Rust SDK, hosted MCP, and `riffdb-mcp` are public clients. None
  may open redb or construct a privileged service.
- MCP tool discovery and invocation are policy filtered. Maintenance is not an
  MCP operation.
- Projection and outbox workers consume committed authoritative state. Their
  failure may degrade service but cannot roll back a commit.
- The deterministic runtime receives checked inputs and reserved logical time;
  it has no ambient I/O, clock, or randomness.

## Primary Threats

| Threat | POC control | Residual limit |
|---|---|---|
| Unauthorized mutation | Opaque capabilities, current-state policy, shared service path | No production identity provider |
| MCP tool overexposure | Authorization-filtered catalogs and stale-name denial | Hosted transport remains local only |
| Credential disclosure | Exact protected-file rules, zeroizing custody, redacted errors | Local root can read process/state |
| Retry duplication | Canonical input hash and principal/command/key identity | Restore can erase the suffix containing an idempotency record |
| Torn authoritative state | One commit coordinator and atomic storage transaction | Process/storage defects remain possible |
| Malformed input exhaustion | Bounded parsing, scans, waits, schemas, and diagnostics | POC limits are not production SLOs |
| Logging leakage | Closed telemetry vocabularies before subscriber layers | Operator-added subscribers require review |
| Corrupt storage | Complete startup structural/catalog validation, fail closed | No privileged repair tool |
| Backup path traversal | Checked name joined only beneath server-owned root | Backups are not encrypted |
| Destructive restore abuse | Exact confirmation plus current and staged authorization | History-incarnation fence (ADR-0072); residual risk is non-participating clients |

## Deployment Rules

- Keep cleartext application gRPC and hosted MCP on literal loopback. Remote
  application gRPC must use the `direct_tls` profile; same-host/sidecar clients
  may use a protected `local_socket` profile.
- A TCP or HTTP/2 proxy may forward verified TLS, but forwarded identity,
  tenant, database, principal, and authorization assertions are ignored. The
  normal RiffDB bearer credential and database selector remain mandatory.
- Keep the database file, backup root, capability digest-key file, and
  idempotency digest-key file lexically disjoint. The packaged layout does so.
- Make the `riffdb` service account the only writer to the database directory.
  Never run two `riffdbd` processes against one database, and never grant the
  CLI, MCP bridge, backup utilities, or another service direct write access.
- Own secret files by the exact service effective user at mode `0600`.
- Protect TLS private keys with no group or other permission bits. Replace the
  certificate and key atomically; an invalid replacement retains the last
  valid identity for new handshakes.
- Give the optional MCP bridge a narrowly scoped normal capability. Never use
  the bootstrap document as its long-lived credential.
- Treat membership in the `riffdb-mcp` socket group as access to the bridge
  capability.
- Protect backup artifacts and receipts even though they contain no digest-key
  documents.
- Inspect only structured, redaction-safe output. Do not add raw business
  values, tokens, filesystem paths, or arbitrary error strings to telemetry.

The systemd units remove ambient capabilities, deny privilege escalation,
restrict writable paths, devices, namespaces, kernel interfaces, address
families, and non-loopback IP traffic. The bridge units have no access to the
RiffDB state directory. Hardening is defense in depth; application-enforced
loopback and authorization rules remain authoritative.

## Incident Handling

For suspected bearer compromise, revoke the capability through the public
service and replace the credential file. Replacing a file alone does not revoke
the durable capability.

For digest-key compromise, stop admission and obtain architecture review before
rotation. Capability and idempotency keys are distinct typed namespaces and
must not reuse material. Preserve required readable keys during rotation.

For storage or receipt corruption, stop. RiffDB intentionally fails readiness
closed and has no online repair path. Restore only from a fully validated
offline backup and account for the destructive rewind limitations.
