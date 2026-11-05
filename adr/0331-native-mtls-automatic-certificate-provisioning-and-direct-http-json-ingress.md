---
adr: "0331"
title: Native mTLS, Automatic Certificate Provisioning, and Direct HTTP/JSON Application Ingress
status: accepted
tier: guarantee
date: 2026-11-05
accepted: 2026-11-05
acceptance: "User approved native mTLS, automatic certificate issuance, and direct HTTP/JSON application ingress plan"
requires: ["0007", "0009", "0102", "0103", "0105", "0316"]
amends: ["0102", "0103", "0105", "0316"]
supersedes: []
requirements: ["API-001", "NET-001", "NET-002", "NET-003", "NET-004", "NET-006", "NET-007", "NET-008", "SEC-001", "OUT-001"]
packages: ["WP-886", "WP-887", "WP-890"]
obligations:
  - id: OBL-0331-1
    package: WP-886
    description: "DirectTlsListenerConfig and TlsClientConfig support native mutual TLS (client_ca_certificate_path on server listeners via WebPkiClientVerifier, ClientTlsIdentity on clients, and atomic hot reload) while preserving NET-004 bearer capability authentication"
    proof: mtls_listener_verifies_client_certificate_and_requires_bearer_capability
  - id: OBL-0331-2
    package: WP-886
    description: "Deterministic ECDSA P-256 X.509 v3 certificate provisioning (auto_issue on direct_tls listeners and riffdb tls issue CLI) mints CA, server SAN-bound leaf, and client leaf bundles with mode-0600 private keys that pass rustls-webpki verification"
    proof: auto_issued_x509_bundle_passes_direct_tls_and_mtls_verification
review_triggers:
  - "Client certificate verification bypasses or substitutes for opaque RiffDB bearer capability authentication (NET-004)."
  - "Automatic certificate issuance writes private keys with permissions broader than 0600 or omits Subject Alternative Name (SAN) binding for the canonical endpoint."
  - "Direct HTTP/JSON ingress bypasses ApplicationService, CredentialAuthenticator, idempotency enforcement, or ListenerBounds."
---
# ADR-0331: Native mTLS, Automatic Certificate Provisioning, and Direct HTTP/JSON Application Ingress

## Context

Under `ADR-0102`, `ADR-0103`, `ADR-0105`, and `ADR-0316`, `riffdbd` supports server-authenticated TLS 1.3 (`direct_tls`) on the gRPC application listener and the hosted MCP HTTP listener, while client connections validate server certificates against an explicit PEM trust root or the system CA bundle. However, three transport and ingress gaps remained documented in `docs/known-limitations.md`:

1. **Native Mutual TLS (mTLS):** `DirectTlsListenerConfig` configured `rustls::ServerConfig::with_no_client_auth()`, and `TlsClientConfig` had no client certificate/key identity fields, requiring an external proxy when operators needed transport-layer mutual TLS.
2. **Automatic Certificate Issuance:** Setting up `direct_tls` or mTLS required external OpenSSL/cfssl tooling to mint CA, server SAN-bound leaf, and client leaf certificates with `0600` private keys.
3. **Direct HTTP/JSON Application Ingress:** Application clients outside gRPC and MCP had no direct HTTP/JSON endpoint on `riffdbd` to execute compiled commands, run named RiffQL queries, or recover outcomes by idempotency key.

## Decision

1. **Native Mutual TLS (`WP-886`):**
   - `ServerTlsFiles` and `DirectTlsListenerConfig` in `crates/riffdb-config/src/remote.rs` gain an optional `client_ca_certificate_path: Option<ProtectedFilePath>` (`client_ca_certificate` in TOML / `--tls-client-ca-cert` on CLI).
   - When `client_ca_certificate_path` is configured, `load_tls_snapshot` and `load_http1_tls_acceptor` in `crates/riffdb-server/src/daemon.rs` parse the PEM trust bundle (up to `MAX_TLS_PEM_FILE_BYTES`, at most 64 CA certificates), construct a `RootCertStore`, and configure `WebPkiClientVerifier::builder(Arc::new(root_store)).build()` on `rustls::ServerConfig`. Unauthenticated or untrusted client handshakes fail closed during the TLS 1.3 handshake.
   - `ReloadableTlsIdentity` tracks the `(certificate_chain, private_key, client_ca_certificate)` snapshot triple and publishes updated mTLS verifiers atomically for new handshakes (`NET-007`).
   - `TlsClientConfig` in `crates/riffdb-config` and `RiffDbClient` in `crates/riffdb-client-rust` gain optional `ClientTlsIdentity` (`certificate_chain: ProtectedFilePath`, `private_key: ProtectedFilePath`), loaded via the protected-file custody validator (`0600` key mode) and attached via `tonic::transport::Identity::from_pem`. `riffdb-cli` exposes `--tls-client-cert` and `--tls-client-key`.
   - Per `NET-004`, client TLS certificates authenticate the transport peer only and NEVER assert a RiffDB principal, role, database, or permission; every protected operation still requires a valid opaque RiffDB bearer capability.

2. **Automatic X.509 v3 Certificate Issuance and Rotation (`WP-886`):**
   - Implement a deterministic first-party ECDSA P-256 (`ring::signature::ECDSA_P256_SHA256_ASN1_SIGNING`) PKCS#8 and X.509 v3 DER/PEM certificate generator in `crates/riffdb-config` (reusing `ring` already in `Cargo.lock` with zero new dependencies).
   - Issued certificates use X.509 v3 with explicit BasicConstraints, KeyUsage, ExtendedKeyUsage (`serverAuth` / `clientAuth`), SubjectKeyIdentifier, AuthorityKeyIdentifier, validity bounds, and Subject Alternative Name (`dNSName` or `iPAddress` matching `CanonicalHttpsEndpoint::host()`).
   - `DirectTlsListenerConfig` supports `auto_issue = true` (with `ca_certificate_path` and `ca_private_key_path`). Before binding or reloading, if the server certificate/key (or CA certificate/key) is missing or within its renewal window, `riffdbd` atomically writes private keys with mode `0600` and certificate chains with mode `0644` and validates the bundle via `verify_certificate_identity`.
   - `riffdb-cli` adds `riffdb tls issue` (`--out-dir`, `--endpoint`, `--validity-days`, `--issue-client`) to generate complete server + mTLS client bundles offline.

3. **Direct HTTP/JSON Application Ingress (`WP-887`):**
   - `riffdb-server` adds an optional `[server.http_listener]` (`--http-listen` / `RIFFDB_HTTP_LISTEN` for loopback cleartext, or `mode = "direct_tls"` with optional mTLS and `auto_issue`).
   - The HTTP/JSON listener enters the exact same `ApplicationService`, `CredentialAuthenticator` (`Authorization: Bearer <token>`), `RequestIdSource`, and `ListenerBounds` admission pipeline as gRPC and MCP (`API-001`, Non-Negotiable Boundary #3):
     - `GET /v1/health`: unauthenticated liveness probe (`{"status":"alive"}`, `NET-008`), or authenticated database readiness when `Authorization: Bearer <token>` and `?database=<alias>` are supplied.
     - `POST /v1/databases/{database}/commands/{command}`: executes a compiled command by name with JSON arguments (`{"arguments": {...}, "expected_active_version": ...}`) and required `Idempotency-Key` header (or `"idempotency_key"` body field), returning the canonical JSON outcome envelope (`status`, `commit_sequence`, `replayed`, `output`, `events`).
     - `POST /v1/databases/{database}/queries/{module}/{query}`: executes a compiled named RiffQL query with JSON parameters, freshness selection (`latest`, `at_least`, `bounded_staleness`), and optional pagination cursor.
     - `GET /v1/databases/{database}/outcomes/{idempotency_key}`: resolves a persisted command outcome by idempotency key (`OUT-001`).

## Standing design tests

- **Interface safety:** mTLS never bypasses opaque RiffDB capability authentication (`NET-004`); HTTP/JSON command execution requires an explicit `Idempotency-Key` and executes through `ApplicationService`; auto-issued private keys are created with mode `0600`.
- **Scale:** Request bodies on HTTP/JSON ingress are bounded by `ListenerBounds::max_request_body_bytes` (`<= 4 MiB`), concurrent connections and in-flight requests are bounded by `ListenerBounds`, and CA trust bundles remain bounded (`<= 1 MiB`, `<= 64` certs).

## Checks

- `cargo test -p riffdb-config -p riffdb-client-rust -p riffdb-server -p riffdb-cli`
- `./scripts/handbook check`
