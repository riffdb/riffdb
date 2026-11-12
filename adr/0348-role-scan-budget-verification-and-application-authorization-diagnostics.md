---
adr: "0348"
title: Compile-time role scan-budget verification, primary-key set access bounds, and environment-default authorization diagnostics
status: accepted
tier: guarantee
date: 2026-11-12
accepted: 2026-11-12
acceptance: 'maintainer, in session, 2026-11-12: "yes, commit and push, no release yet"'
requires: ["0006", "0051", "0052", "0074", "0156", "0199", "0200", "0305", "0344"]
amends: ["0006", "0052", "0156", "0199", "0200", "0305"]
supersedes: []
requirements: ["SEC-001", "SEC-002", "AAA-003", "AAA-004", "AAA-005", "QRY-004", "DRV-001", "DRV-005", "DX-003", "DX-006", "HBK-003"]
packages: ["WP-927"]
obligations:
  - id: OBL-0348-1
    package: WP-927
    description: "Compiled application roles derive max_scan_rows across both index scan limits and primary-key in $set point-access maximum_rows, and compile-time role verification rejects explicit max_scan_rows smaller than any granted query access bound"
    proof: role_scan_budget_covers_primary_key_in_set_point_access_maximum_rows
  - id: OBL-0348-2
    package: WP-927
    description: "CurrentAuthorizer::application_query_exceeds_scan_budget checks per-access maximum_rows <= grant.max_scan_rows alongside aggregate query cost so point-set queries exceeding max_scan_rows classify as scan_budget_exceeded rather than missing_permission"
    proof: point_set_query_exceeding_grant_scan_rows_reports_scan_budget_exceeded
  - id: OBL-0348-3
    package: WP-927
    description: "RDB-AUTH-0214 authorization denial classifications are carried on the dedicated authorization_diagnostic field across ApplicationError, protobuf, riffdb-driver-host, CLI, MCP, and SDKs while preserving symbol_path strictly for application symbols (AAA-004)"
    proof: authorization_diagnostic_uses_dedicated_field_and_preserves_symbol_path
  - id: OBL-0348-4
    package: WP-927
    description: "authorization_diagnostics defaults to true outside prod/production and false in prod/production, and enforces CLI -> environment -> databases.<alias> TOML -> server TOML -> environment default precedence"
    proof: authorization_diagnostics_defaults_on_in_development_and_off_in_production
review_triggers:
  - "authorization_diagnostic leaks raw capability tokens, internal policy state, or tenant/entity existence beyond the closed AuthorizationDiagnosticCode set."
  - "AuthorizationDiagnosticCode is written into ApplicationErrorContext.symbol_path instead of the dedicated authorization_diagnostic field."
  - "Compiled application role lowering permits a granted query whose access maximum_rows exceeds the role's effective max_scan_rows."
---
# ADR-0348: Compile-time role scan-budget verification, primary-key set access bounds, and environment-default authorization diagnostics

## Context

Building and deploying multi-service applications and adapters (OpenFGA, Better Auth, MLflow, and custom applications) surfaced four related authorization, role-compilation, and driver-diagnostic gaps:

1. **Primary-key `in $set` queries denied at runtime despite role grant (`RDB-AUTH-0214`):**
   - When a RiffQL query uses `where pk_field in $set` on an entity's primary key (`Set<..., max = N>`), the query compiler lowers a `PointAccessRequirement` (`index_id: None`, `maximum_rows: N`).
   - `default_max_scan_rows` in `crates/riffdb-query-module/src/application_role.rs` previously inspected only `scan_requirements` (`index_id: Some(...)`) and ignored `point_access_requirements` (`index_id: None`), defaulting a role with only primary-key lookups to `max_scan_rows = 1` unless an explicit `max_scan_rows` was larger.
   - At runtime, `crates/riffdb-policy/src/decision.rs` enforces `access.maximum_rows() <= grant.max_scan_rows()` for every access requirement, so any primary-key `in $set` query with `max > 1` (or `max > explicit max_scan_rows`) compiled cleanly into the role lockfile and deployed, then failed at runtime with `RDB-AUTH-0214`.
2. **Undifferentiated `RDB-AUTH-0214` denials during development:**
   - Every capability denial collapsed into `RDB-AUTH-0214: current capability does not permit the requested operation` with no distinction between an expired/revoked credential, a database/environment/audience mismatch, a missing command/query permission, a partition/tenant scope mismatch, or a `max_scan_rows` scan-budget shortfall.
   - Furthermore, `CurrentAuthorizer::application_query_exceeds_scan_budget` checked aggregate query cost against `grant_covers_application_query_budget` without checking per-access `access.maximum_rows() <= grant.max_scan_rows()`, causing point-set queries that exceeded `max_scan_rows` to classify as `missing_permission` instead of `scan_budget_exceeded`.
3. **Preserving `symbol_path` semantics (`AAA-004`):**
   - `ApplicationErrorContext.symbol_path` is reserved for caller-visible application symbols (`AAA-004`). Overloading `symbol_path` to carry denial reason codes would change the meaning of an existing protocol field and clobber or fabricate symbol paths.
4. **Opaque driver-session errors and container UID / remote-identity ergonomics:**
   - The Go SDK collapsed `riffdb-driverd` error frames into `"RiffDB driver session failed"`, `riffdb-driverd` logged nothing at `WARN` when rejecting peer credentials or handshakes, and `riffdb-driverd` accepted only `peer.uid == host_uid` on Unix sockets even when an operator explicitly runs a non-root application container (such as distroless `65532`) against a shared volume socket.

## Decision

1. **Compile-time role `max_scan_rows` derivation and verification (`crates/riffdb-query-module`):**
   - `required_query_max_scan_rows` and `default_max_scan_rows` in `crates/riffdb-query-module/src/application_role.rs` compute the maximum `maximum_rows` across both `scan_requirements` (index scans) and `point_access_requirements` (primary-key `Get` and `in $set` point-set lookups) for every query granted to the role.
   - If a role declares an explicit `max_scan_rows` smaller than the `maximum_rows` of any query granted to that role, role compilation fails closed with `ApplicationRoleModuleError::MaxScanRowsBelowGrantedQuery { role, query, configured, required }` before a lockfile or bundle can be produced.
2. **Per-access row ceiling in `grant_covers_application_query_budget` (`crates/riffdb-policy`):**
   - `grant_covers_application_query_budget` in `crates/riffdb-policy/src/authorizer.rs` verifies `access.maximum_rows() <= grant.max_scan_rows()` for every `ApplicationQueryAccessRequirement` in `target.accesses()` in addition to the aggregate `QueryCostVectorV1` budget, matching the per-access row gate in `decision.rs` so `CurrentAuthorizer::application_query_exceeds_scan_budget` classifies primary-key point-set budget failures as `scan_budget_exceeded`.
3. **Dedicated `authorization_diagnostic` field and environment-aware default (`crates/riffdb-errors`, `crates/riffdb-proto`, `crates/riffdb-service`, `crates/riffdb-server`, `crates/riffdb-driver-host`, `crates/riffdb-cli`, `crates/riffdb-api-mcp`, `clients/`):**
   - Define closed enum `AuthorizationDiagnosticCode` in `crates/riffdb-errors`: `MissingPermission`, `ScanBudgetExceeded`, `TenantScopeMismatch`, `PartitionScopeMismatch`, `FieldVisibilityDenied`, `RowPolicyDenied`, `ApprovalRequired`, `DelegationExceedsAuthority`, and `InactiveOrStaleCapability`.
   - Carry `Option<AuthorizationDiagnosticCode>` on a dedicated `authorization_diagnostic` field on `ApplicationErrorContext`, `riffdb.app.v1.ApplicationError` (`optional string authorization_diagnostic = 14`), `riffdb-driver-host` `DriverResponse::Error` and `DriverBatchOutcome::Error`, CLI JSON output, MCP error presentation, and Go/TypeScript/Python SDK error details. `symbol_path` remains strictly reserved for application symbols (`AAA-004`).
   - `authorization_diagnostics` is configurable via `--authorization-diagnostics <true|false>`, `RIFFDB_AUTHORIZATION_DIAGNOSTICS`, `server.authorization_diagnostics`, and `databases.<alias>.authorization_diagnostics` with strict precedence `CLI -> environment -> databases.<alias> TOML -> server TOML -> environment default`. When unset, it defaults to `true` outside `prod`/`production` and `false` in `prod`/`production`.
4. **Driver-session error propagation, optional `remote_identity_hash`, and explicit `allowed_peer_uids` (`crates/riffdb-driver-host`, `clients/go/runtime`):**
   - Go SDK `DriverError` formats the full `[CODE] category: message` (and `authorization_diagnostic` when present), and omits `remote_identity_hash` from the handshake when `RIFFDB_REMOTE_HASH` is unset while `riffdb-driverd` continues verifying the remote server identity.
   - `riffdb-driverd` logs rejected socket connections, handshakes, and capability denials at `WARN` and supports optional `allowed_peer_uids = [<uid>, ...]` (up to 16 explicit non-zero UIDs) on Unix socket listeners.

## Options considered

- **Encoding `AuthorizationDiagnosticCode` inside `ApplicationErrorContext.symbol_path`:** Rejected because `symbol_path` is normatively defined by `AAA-004` as an application symbol path; placing denial reason codes in `symbol_path` fabricates symbol paths and conflicts with real symbol paths.
- **Always enabling or always disabling `authorization_diagnostics` without configuration:** Rejected; defaulting to `true` in non-production environments and `false` in `prod`/`production` with an explicit override gives immediate actionable feedback during local development and testing while preserving strict existence-blind redaction in production by default.

## Consequences

- Primary-key `in $set` queries automatically size the role's default `max_scan_rows` (or fail closed at compile time if an explicit `max_scan_rows` is too small).
- When `authorization_diagnostics` is enabled, `RDB-AUTH-0214` responses include `authorization_diagnostic` across gRPC, `riffdb-driverd`, CLI, MCP, and all SDKs without altering `symbol_path`.

## Standing design tests

- **Interface safety:** `symbol_path` never contains diagnostic reason strings; `AuthorizationDiagnosticCode` is a closed 9-variant enum validated at every wire boundary (`riffdb-proto` and `riffdb-driver-host`); production environments (`prod`, `production`) default `authorization_diagnostics` to `false`.
- **Scale:** `AuthorizationDiagnosticCode` adds at most one bounded static ASCII token (`<= 32` bytes) to `ApplicationError`.

## Checks

- Proof tests `role_scan_budget_covers_primary_key_in_set_point_access_maximum_rows`, `point_set_query_exceeding_grant_scan_rows_reports_scan_budget_exceeded`, `authorization_diagnostic_uses_dedicated_field_and_preserves_symbol_path`, and `authorization_diagnostics_defaults_on_in_development_and_off_in_production`.
- `./scripts/check-adr-obligations`, `./scripts/check-acceptance-commits`, `./scripts/check-generated`, and `./scripts/handbook check`.
