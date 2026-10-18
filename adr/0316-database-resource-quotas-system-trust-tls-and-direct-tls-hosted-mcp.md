---
adr: "0316"
title: Database Resource Quotas, System-Trust TLS, and Direct-TLS Hosted MCP
status: accepted
tier: guarantee
date: 2026-10-18
accepted: 2026-10-18
acceptance: 'user, in session, 2026-10-18: "i accept"'
requires: [ADR-0028, ADR-0035, ADR-0036, ADR-0102, ADR-0103, ADR-0271]
amends: [ADR-0028, ADR-0102, ADR-0103]
supersedes: []
requirements: [NET-001, NET-002, NET-005, MCP-001, MCP-048]
packages: [WP-876]
obligations:
  - id: OBL-0316-1
    package: WP-876
    summary: Per-database storage quotas, token-bucket rate limits, and worker caps enforce isolation while preserving reads, uncertainty recovery, and exports
    proof: governor_enforces_token_buckets_and_preserves_reads_and_exports_at_storage_ceiling
  - id: OBL-0316-2
    package: WP-876
    summary: Remote HTTPS client profiles derive server identity from the canonical endpoint and support system CA bundle trust roots alongside explicit PEM trust roots
    proof: remote_https_profile_derives_server_name_and_supports_system_and_explicit_trust_roots
  - id: OBL-0316-3
    package: WP-876
    summary: Hosted MCP supports a direct-TLS listener with rustls 1.3, exact https://<host>:<port>/v1/mcp audience derivation, and bounded connection admission
    proof: direct_tls_hosted_mcp_listener_enforces_https_audience_and_origin_validation
review_triggers:
  - Remote HTTPS connections permit insecure certificate-verification bypass, plaintext HTTP over non-loopback addresses, or unvalidated SNI hostnames.
  - Storage quota enforcement blocks read queries, idempotent command-outcome recovery, capability revocation, or application export.
  - Hosted MCP direct-TLS listener bypasses capability authentication, audience binding, or Origin validation.
---
# ADR-0316: Database Resource Quotas, System-Trust TLS, and Direct-TLS Hosted MCP

## Context

Multi-tenant developer-tier cloud hosting (`[databases.<alias>]` up to 64 databases per `riffdbd` process under `ADR-0271`) and remote developer connectivity expose three surface gaps:

1. **Per-database resource isolation**: While `riffdbd` bounds process-wide listeners and per-database retention (`ADR-0182`), operators cannot configure per-database on-disk storage byte ceilings, per-database read/write token-bucket rate limits, or per-database blocking worker thread and coordinator capacity bounds (`[databases.<alias>.limits]` / `[maintenance.limits]`).
2. **Zero-config public CA TLS ergonomics**: `ADR-0102` requires every `https://` client connection in `riffdb-config`, `riffdb-client-rust`, `riffdb-driver-host`, `riffdb-client-python-native`, and `riffdb-cli` to supply both an explicit `--tls-trust-root <path>` (`0600` regular file) and an explicit `--tls-server-name <name>`, even when connecting to a cloud endpoint with a publicly trusted certificate where the canonical HTTPS endpoint already carries the exact DNS host or IP literal.
3. **Remote Hosted MCP ingress**: `ADR-0028` and `ADR-0103` added `DirectTls` to the gRPC application listener, but Hosted MCP (`crates/riffdb-api-mcp`, `crates/riffdb-server`) still only supports loopback HTTP (`mcp_listen`), preventing remote MCP clients from connecting directly over TLS 1.3 with an `https://<host>:<port>/v1/mcp` audience.

## Decision

1. **Per-Database Resource Quotas & Rate Governors (`crates/riffdb-service`, `crates/riffdb-server`)**:
   - Add `DatabaseResourceAdmissionPort` to `riffdb-service` and `DatabaseLimitsConfig` / `ServerDatabaseResourceGovernor` to `riffdb-server` (`[maintenance.limits]` for single-database configs, `[databases.<alias>.limits]` for named multi-database configs):
     - `max_storage_bytes` (`65,536..=1_099_511_627_776`): sums `.redb`, `.redb.journal`, sealed `.redb.journal.ext.*`, `.redb.format`, `.redb.derived`, `projections_root`, and `outbox_root`. When reached, storage-growing mutations fail closed with `PublicErrorKind::Overloaded` (`RESOURCE_EXHAUSTED`), while reads, `ResolveCommandOutcome` (uncertainty recovery), `RevokeCapability`, and `StartApplicationExport` / `GetApplicationExportPage` / `GetApplicationExport` / `CancelApplicationExport` remain admitted (`AGENTS.md` Rules 11 & 12).
     - `max_write_rps` / `write_burst` and `max_read_rps` / `read_burst` (`1..=1_000_000`): exact integer nanosecond token-bucket rate governors per database; `GetHealth` and `GetStatistics` remain exempt.
     - `max_blocking_operations` (`1..=64`), `blocking_worker_threads` (`1..=max_blocking_operations`), and `max_coordinator_capacity` (`1..=4,096`): bound per-database blocking threads and commit coordinator queues.

2. **System CA Bundle Trust Roots & Endpoint-Derived Server Identity (`crates/riffdb-config`, `crates/riffdb-client-rust`, `crates/riffdb-driver-host`, `crates/riffdb-client-python-native`, `crates/riffdb-cli`)**:
   - Introduce `ClientTrustRootConfig` (`SystemCaBundle` vs `ExplicitPemFile(ProtectedFilePath)`) in `riffdb-config` and `TlsTrustRootSource` (`SystemCaBundle` vs `PemFile(PathBuf)`) in `riffdb-client-rust`.
   - When `tls_server_name` is omitted on a remote `https://` endpoint, derive `ServerIdentity` directly from `CanonicalHttpsEndpoint::host()`. When `tls_trust_root` is omitted on a remote `https://` endpoint, default to `SystemCaBundle`, loading the platform system CA bundle (`/etc/ssl/certs/ca-certificates.crt`, `/etc/pki/tls/certs/ca-bundle.crt`, `/etc/ssl/cert.pem`, `/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem`) up to `1 MiB` and `512` anchors validated through `rustls-webpki`. Explicit `--tls-trust-root` and `--tls-server-name` remain supported and take precedence.

3. **Direct-TLS Hosted MCP Listener (`crates/riffdb-api-mcp`, `crates/riffdb-server`)**:
   - Extend `[server.mcp_listener]` in `riffdb-server` with `mode = "loopback_cleartext"` and `mode = "direct_tls"` (`listen`, `public_endpoint`, `certificate_chain`, `private_key`, optional `bounds`), while preserving legacy `mcp_listen` / `--mcp-listen` / `RIFFDB_MCP_LISTEN` as loopback cleartext.
   - In `mode = "direct_tls"`, bind a rustls TLS 1.3 listener (`ALPN = ["http/1.1"]`, bounded connections and handshake timeout from `ListenerBounds`), derive the required MCP capability audience from `public_endpoint` as `https://<host>:<port>/v1/mcp`, and enforce the same `Host` / `:authority`, `Origin`, capability authentication, and non-bypass service execution as loopback MCP.

## Standing design tests

- **Interface safety:** Certificate verification is never disabled (`SystemCaBundle` or `ExplicitPemFile` is mandatory on every `https://` connection); storage quotas never block read queries, uncertainty recovery (`ResolveCommandOutcome`), capability revocation, or application export (`StartApplicationExport` / `GetApplicationExportPage`).
- **Scale:** Per-database blocking worker threads (`1..=64`), rate buckets (`1..=1_000_000` RPS), storage byte quotas (`64 KiB..=1 TiB`), system CA bundle size (`<= 1 MiB`, `<= 512` certs), and direct-TLS MCP connections (`ListenerBounds`) are strictly bounded.

## Checks

- `cargo test -p riffdb-server governor_enforces_token_buckets_and_preserves_reads_and_exports_at_storage_ceiling`
- `cargo test -p riffdb-client-rust remote_https_profile_derives_server_name_and_supports_system_and_explicit_trust_roots`
- `cargo test -p riffdb-api-mcp direct_tls_hosted_mcp_listener_enforces_https_audience_and_origin_validation`
