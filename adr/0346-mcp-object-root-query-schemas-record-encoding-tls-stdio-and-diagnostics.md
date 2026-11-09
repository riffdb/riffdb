---
adr: "0346"
title: MCP object-root query schemas, hosted record encoding parity, verified-TLS stdio bridge, tool descriptions, and structured error diagnostics
status: accepted
tier: guarantee
date: 2026-11-08
accepted: 2026-11-08
acceptance: 'maintainer, in session, 2026-11-08: "yes"'
requires: ["0008", "0040", "0047", "0064", "0194", "0198", "0316", "0343"]
amends: ["0008", "0040", "0047", "0194", "0198", "0316"]
supersedes: []
requirements: ["MCP-001", "MCP-010", "MCP-020", "MCP-021", "MCP-023", "MCP-026", "MCP-040", "MCP-043", "MCP-047", "MCP-048", "HBK-003", "HBK-006"]
packages: ["WP-925"]
obligations:
  - id: OBL-0346-1
    package: WP-925
    description: "Every generated MCP query, command, saga, and vector tool schema emits top-level type: object with accurate scalar property types, and both HostedServiceMcpBackend and PublicGrpcMcpBackend encode and validate populated query records across all scalar types"
    proof: mcp_query_and_command_tools_emit_object_root_schemas_and_validate_records
  - id: OBL-0346-2
    package: WP-925
    description: "riffdb-mcp stdio bridge supports verified-TLS https:// endpoints with optional trust-root and server-name configuration emitted by application deploy, and reports actionable closed startup diagnostics as well as --help and --version"
    proof: mcp_stdio_bridge_connects_over_verified_tls_and_reports_actionable_startup_errors
  - id: OBL-0346-3
    package: WP-925
    description: "Dynamic MCP command and query tools publish deterministic titles and descriptions, initialize negotiates both 2025-11-25 and 2025-06-18 protocol offers, and internal/schema MCP failures attach redaction-safe structured error.data"
    proof: mcp_dynamic_tools_populate_descriptions_echo_supported_protocol_versions_and_include_error_data
review_triggers:
  - "Any MCP tool inputSchema or outputSchema is emitted without a top-level type: object member."
  - "HostedServiceMcpBackend and PublicGrpcMcpBackend diverge in how named-query parameters or result records are encoded or validated against outputSchema."
  - "riffdb-mcp connects to an https:// endpoint without certificate verification or exposes raw credentials or internal error sources in startup or JSON-RPC diagnostics."
---
# ADR-0346: MCP object-root query schemas, hosted record encoding parity, verified-TLS stdio bridge, tool descriptions, and structured error diagnostics

## Context

End-to-end integration testing of RiffDB 0.2.0's hosted Streamable HTTP (`/mcp`) and local stdio (`riffdb-mcp`) Model Context Protocol surfaces against a strict spec-compliant MCP client (Kiro CLI with `mcp-remote`) exposed 11 integration defects and usability gaps:

1. **Unpaginated named-query `outputSchema` lacked top-level `"type": "object"`:** In `crates/riffdb-query-module/src/generation.rs`, `generate_all_query_tools` emitted `{"$schema": ..., "oneOf": [...]}` without `"type": "object"` whenever `pagination.is_none()` (and similarly in `generate_mcp_commands`, saga tools, and vector staleness/model-version schemas). Strict MCP clients require `outputSchema.type == "object"` and rejected the entire `tools/list` response.
2. **Hosted `/mcp` queries returning populated records failed with `-32603 "MCP request failed"`:**
   - `mcp_type_schema` in `crates/riffdb-query-module/src/generation.rs` matched capitalized `"Bool"`, `"I64"`, `"U64"`, `"Bytes"` instead of the lowercase `"bool"`, `"i64"`, `"u64"`, `"bytes<N>"` type names produced by `riffdb-query-ir`, causing boolean, integer, and bytes record fields to be typed as `"string"` in `outputSchema`.
   - `HostedServiceMcpBackend::named_query_value` in `service_backend.rs.in` delegated scalar values to `canonical_natural_value`, which encoded `i64`/`u64` as strings and `timestamp`/`date`/`decimal`/`money` as nested JSON objects instead of the natural JSON scalars emitted by `PublicGrpcMcpBackend::application_result_value` and expected by `outputSchema`. Consequently, `BoundedStructuredContent::validate` failed on every non-empty query record over `/mcp`.
   - `PublicGrpcMcpBackend::application_value` checked `as_i64()` before `as_u64()`, turning positive integer query parameters into `ApplicationValue::I64`, which `symbolic_query.rs` rejected for `Limit` / `BoundedLimit` parameters.
3. **`riffdb-mcp` stdio bridge rejected `https://` endpoints:** `riffdb-mcp` accepted only loopback `http://` endpoints even though `riffdb application deploy` against a direct-TLS server generated `mcp.toml` with `endpoint = "https://127.0.0.1:17443"`.
4. **Opaque `-32603` errors and `riffdb-mcp` startup messages:** `-32603 "MCP request failed"` carried `error.data = None` and logged nothing on the server; `riffdb-mcp` printed only `riffdb-mcp configuration was rejected` without naming the setting or reason, and rejected `--help` and `--version`.
5. **No CI schema/record conformance gate across both MCP backends:** Existing tests validated empty/not-found query responses or synthetic schemas rather than executing populated record queries and commands through both `HostedServiceMcpBackend` and `PublicGrpcMcpBackend` under `BoundedStructuredContent::validate`.
6. **Undocumented MCP audience (`<public_endpoint>/mcp`), client configuration (`mcp.json`), and role scoping:** Deployment workflows did not provision or explain the separate `/mcp` capability audience, lacked working client configuration examples (`mcp-remote` + `NODE_EXTRA_CA_CERTS`, direct HTTP headers, `riffdb-mcp`), left dynamic tool `title`/`description` empty (`None`), and returned `2025-11-25` when a client offered `2025-06-18`.

## Decision

1. **Object-root MCP tool schemas and accurate scalar type mapping (`crates/riffdb-query-module`, `crates/riffdb-api-mcp`):**
   - Every generated MCP tool `inputSchema` and `outputSchema` (including unpaginated named queries, paginated named queries, vector staleness/model-version tools, command tools, and saga tools) MUST include top-level `"type": "object"` alongside its `properties` or `oneOf` branches.
   - `McpDynamicToolDefinition::from_discovered_query` enforces that the compiled `result_schema` root has `"type": "object"` before exposing the tool.
   - `mcp_type_schema` matches both lowercase IR scalar names (`"bool"`, `"i64"`, `"u64"`, `"bytes"`, `"bytes<N>"`) and capitalized aliases, mapping `"bool"` to `{"type": "boolean"}`, `"i64"`/`"u64"` to `{"type": "integer"}`, and `"bytes"`/`"bytes<N>"` to `{"contentEncoding": "base64", "type": "string"}`.
2. **Natural scalar parity between `HostedServiceMcpBackend` and `PublicGrpcMcpBackend` (`crates/riffdb-api-mcp`, `crates/riffdb-mcp-stdio`, `crates/riffdb-service`):**
   - `HostedServiceMcpBackend::named_query_value` (`service_backend.rs.in`) encodes `Bool` as JSON boolean, `I64`/`U64` as JSON integers, `Bytes` as standard base64 string, `Date` as `days.to_string()`, `Timestamp` as `format!("{seconds}.{nanos:09}")`, `Decimal` as `format!("{coefficient}e-{scale}")`, and `Money` as `format!("{currency}:{coefficient}e-{scale}")`, matching `PublicGrpcMcpBackend::application_result_value` and `mcp_type_schema` byte-for-byte.
   - `PublicGrpcMcpBackend::application_value` checks `as_u64()` before `as_i64()` (matching `HostedServiceMcpBackend::natural_parameter`), and `symbolic_query.rs` accepts non-negative `SubmittedValue::I64` for `Limit`/`BoundedLimit` and coerces string `Timestamp`, `Date`, and base64 `Bytes` parameters in `coerce_natural_query_value`.
3. **Verified-TLS `https://` support and CLI ergonomics in `riffdb-mcp` and `application deploy` (`crates/riffdb-mcp-stdio`, `crates/riffdb-cli`):**
   - `riffdb-mcp` accepts both loopback `http://` endpoints and verified-TLS `https://<host>:<port>` endpoints, with optional `tls_trust_root` (`--tls-trust-root`, `RIFFDB_MCP_TLS_TRUST_ROOT`, `mcp.tls_trust_root`) and `tls_server_name` (`--tls-server-name`, `RIFFDB_MCP_TLS_SERVER_NAME`, `mcp.tls_server_name`). When `https://` is configured, `riffdb-mcp` connects via `RiffDbClient::connect_verified_tls` using the configured PEM trust root (or system CA bundle when omitted) and verified server identity (`tls_server_name` or the endpoint host).
   - `riffdb application deploy` writes `tls_trust_root` and `tls_server_name` into generated `mcp.toml` when deploying over TLS, and supports `--mcp-audience <URI>` (or prints the exact `role bind --audience <endpoint>/mcp` command and `mcp-hosted.json` template) so operators can immediately connect both `riffdb-mcp` and hosted HTTP MCP clients.
   - `riffdb-mcp` handles `--help`/`-h` and `--version`/`-V` cleanly on stdout (exit code `0`) and formats specific closed `StdioConfigError` reasons on stderr when configuration is rejected.
4. **Deterministic dynamic tool descriptions, `2025-06-18` + `2025-11-25` protocol negotiation, and structured error diagnostics (`crates/riffdb-api-mcp`, `docs/mcp/`):**
   - `McpDynamicToolDefinition::from_discovered_command` and `from_discovered_query` populate deterministic, bounded (`<= 256` bytes) `title` and `description` strings derived from the compiled tool name, parameter/input property names, and declared outcome branches (`ADR-0198` parity).
   - `riffdb-api-mcp` accepts both `"2025-11-25"` and `"2025-06-18"` protocol offers during `initialize` and in `MCP-Protocol-Version` headers, echoing `"2025-06-18"` when requested while falling back to `"2025-11-25"` for unknown offers.
   - When an MCP tool call or request fails internally (`output_schema_validation`, `backend_response`, `session_capacity`), `riffdb-api-mcp` logs a bounded `tracing::warn!` event and attaches a closed, redaction-safe `error.data` object (`{"schema": "riffdb.mcp.internal-error/v1", "code": "...", "phase": "...", "retryable": ...}`).

## Options considered

- **Wrapping unpaginated query results in a synthetic outer object instead of adding `"type": "object"` to the `oneOf` root:** Rejected because every `oneOf` branch is already a closed JSON object (`{"status": "Found", ...}` / `{"status": "NotFound"}`), so adding `"type": "object"` at the root satisfies JSON Schema Draft 2020-12 and strict MCP validators without breaking existing result payloads.
- **Keeping `riffdb-mcp` cleartext-only:** Rejected because production and Docker Compose deployments use direct TLS (`https://`), and `riffdb-client-rust` already implements verified TLS (`ADR-0316`).

## Consequences

- Spec-compliant MCP clients (Kiro CLI, Claude Code, Cursor, MCP Inspector) validate `tools/list` and execute record-returning named queries and commands over both `/mcp` and `riffdb-mcp`.
- Generated `generated/mcp/tools.json` artifacts include top-level `"type": "object"`, accurate scalar types (`boolean`, `integer`, base64 `string`), and non-empty `title` and `description` fields.

## Standing design tests

- **Interface safety:** `BoundedStructuredContent::validate` remains fail-closed on every tool response; `error.data` uses closed static phase/code identifiers and never leaks record payloads, credentials, or internal stack traces; `riffdb-mcp` TLS always verifies server certificates and never enables insecure skip-verify.
- **Scale:** Generated tool `title` (`<= 128` bytes) and `description` (`<= 256` bytes) remain strictly within the `tools/list` framing budget (`524,288` bytes).

## Checks

- Proof tests `mcp_query_and_command_tools_emit_object_root_schemas_and_validate_records`, `mcp_stdio_bridge_connects_over_verified_tls_and_reports_actionable_startup_errors`, and `mcp_dynamic_tools_populate_descriptions_echo_supported_protocol_versions_and_include_error_data`.
- `./scripts/check-adr-form`, `./scripts/check-adr-obligations`, `./scripts/check-generated`, `./scripts/handbook check`, and `./scripts/acceptance --wp WP-925`.
