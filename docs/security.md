# Security Posture and Threat Model

RiffDB's POC target is a trusted Linux host used for local development and
architecture validation. It is not hardened for an untrusted network or
multi-tenant production deployment.

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
| Destructive restore abuse | Exact confirmation plus current and staged authorization | No history-incarnation fence |

## Deployment Rules

- Keep gRPC and hosted MCP on literal loopback addresses.
- Do not place a reverse proxy, port forward, or public tunnel in front of the
  POC endpoints.
- Keep the database file, backup root, capability digest-key file, and
  idempotency digest-key file lexically disjoint. The packaged layout does so.
- Make the `riffdb` service account the only writer to the database directory.
  Never run two `riffdbd` processes against one database, and never grant the
  CLI, MCP bridge, backup utilities, or another service direct write access.
- Own secret files by the exact service effective user at mode `0600`.
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
