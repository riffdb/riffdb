---
adr: "0344"
title: Multi-application driver multiplexing, one-line SDK connections, JSON-native SDK types, and bulk loop revision fences
status: accepted
tier: surface
date: 2026-11-08
accepted: 2026-11-08
acceptance: 'maintainer, in session, 2026-11-08: "yes, sounds good, proceed to build"'
requires: ["0121", "0124", "0137", "0303", "0304", "0340"]
amends: ["0137", "0303", "0304", "0340"]
supersedes: []
requirements: ["DRV-001", "DRV-002", "DRV-003", "DRV-005", "DRV-006", "DRV-007", "DRV-008", "DX-004", "DX-016"]
packages: ["WP-923"]
obligations:
  - id: OBL-0344-1
    package: WP-923
    description: "riffdb-driver-host supports multi-application [[applications]] multiplexing on one Unix socket and fills optional lock/remote hashes during local session handshake while enforcing exact contract bundle and role verification"
    proof: multi_application_driver_host_multiplexes_databases_and_roles_on_one_socket
  - id: OBL-0344-2
    package: WP-923
    description: "Generated TypeScript and Go clients provide one-line connect/connectFromEnv constructors and JSON-serializable Timestamp, integer, revision, and decimal representations"
    proof: generated_typescript_and_go_clients_connect_and_serialize_json_ergonomically
  - id: OBL-0344-3
    package: WP-923
    description: "Contract compiler recognizes revision inequality guards with early outcome return inside bounded for-loop bodies as valid RDB-C030 revision fences"
    proof: bulk_loop_revision_inequality_guard_satisfies_rdb_c030
review_triggers:
  - "Multi-application driver multiplexing allows a session bound to (database, role) to execute operations or use credentials belonging to a different (database, role)."
  - "TypeScript or Go JSON serialization or numeric input coercion silently truncates precision via IEEE-754 floats for out-of-range 64-bit integers or business decimals."
  - "Loop-body revision fence recognition admits unguarded mutations that occur before the revision check or on a different entity binding."
---
# ADR-0344: Multi-application driver multiplexing, one-line SDK connections, JSON-native SDK types, and bulk loop revision fences

## Context

End-to-end application rehearsals combining a Next.js TypeScript app, `@riffdb/better-auth`, `@riffdb/openfga` (Go), and `riffdb-mlflow` (Python) exposed five developer-experience gaps:

1. **10-value driver handshake ceremony:** Connecting from TypeScript (`DriverApplicationTransport.connect`) or Go (`riffdb.Connect`) required passing 9 identity hashes (`applicationManifestHash`, `operationCatalogHash`, `contractLineage`, `contractVersion`, `contractBundleHash`, `database`, `role`, `roleDefinitionHash`, `remoteIdentityHash`) alongside `socketPath`, even though `riffdb-driverd` already loaded the verified lock, catalog, and remote TLS config, and the generated client already knew its compiled contract lineage, version, bundle hash, and role definitions.
2. **One `riffdb-driverd` process per `(database, role)` and `SUN_LEN` socket limits:** `DriverDocument` only accepted one `[application]` table, forcing a stack with three databases (`todos`, `better-auth`, `openfga`) to run three separate `riffdb-driverd` processes, sockets, and configs, and long checkout paths easily exceeded the 103/107-byte Unix domain socket path limit.
3. **Asymmetric deployment models across Python, TypeScript, and Go:** Python (`riffdb_application`) connects directly using environment variables (`RIFFDB_ENDPOINT`, `RIFFDB_CREDENTIAL_FILE`), whereas TypeScript and Go connect through a `riffdb-driverd` socket without a shared `connectFromEnv` abstraction or socket transport option in Python.
4. **Generated types fight `JSON.stringify` and web routes:** TypeScript `u64`/`i64`/`revision` values decode as `bigint` (which throws `TypeError` in `JSON.stringify`) and `timestamp` decodes as `{ seconds, nanos }` instead of an ISO-8601 string, while inputs reject safe JS `number`, decimal string, or `Date` values. Go's `riffdb.Instant`, `ExactDecimal`, and `ExactMoney` similarly lack `json.Marshaler`/`json.Unmarshaler` and `time.Time` helpers.
5. **Bulk loop revision fences (`RDB-C030`) and `riffdb application lock` flag friction:** Inside `for item in items` command loops, an explicit `if t.revision != item.expected_revision { return Conflict {} }` guard before `overwrite`/`set` was not recognized by `RDB-C030` fence analysis, and `riffdb application lock` failed when invoked without `--write` or `--check`.

## Decision

1. **Multi-application `[[applications]]` multiplexing and streamlined local handshake in `riffdb-driverd` (`crates/riffdb-driver-host`, `crates/riffdb-cli`):**
   - `DriverDocument` accepts `[[applications]]` (alongside legacy single `[application]`), where each entry binds `(database, role, lock_file, manifest_file, operation_catalog_file)` and an optional per-target `credential_file` (falling back to `[remote].credential_file`).
   - One `riffdb-driverd` instance multiplexes all configured `(database, role)` targets over a single Unix domain socket and shared TLS connection pool, isolating each session to its handshake-selected `(database, role)` catalog and live-reloaded credential.
   - During the local Unix socket `Handshake`, the client must supply `contract_lineage`, `contract_version`, `contract_bundle_hash`, and `role` (plus `database` when multiple databases share the same `(contract_bundle_hash, role)`). If `application_manifest_hash`, `operation_catalog_hash`, `role_definition_hash`, or `remote_identity_hash` are empty strings, `DriverHost` resolves them from its verified target entry and returns the full `HandshakeAck`; if provided, `DriverHost` verifies exact equality.
   - `riffdb application deploy --driver-config-output <PATH>` automatically merges/updates `[[applications]]` entries when `<PATH>` already targets the same `[remote]`, auto-materializes the runtime operation catalog when absent, and defaults `--driver-socket` to a short path (`<= 100` bytes) when omitted.
2. **One-line `connect` / `connectFromEnv` across TypeScript, Go, and Python (`crates/riffdb-query-module`, `clients/`):**
   - Generated TypeScript (`<Module>Client.connect` / `<Module>Client.connectFromEnv`) and Go (`Connect<Module>Client` / `Connect<Module>ClientFromEnv` / `Connect<Role>Client`) clients embed their compiled contract identity and role definitions, requiring only `socketPath` (or `RIFFDB_DRIVER_SOCKET`).
   - When `RIFFDB_DRIVER_SOCKET` is unset and `RIFFDB_DRIVER_CONFIG` (or `RIFFDB_ENDPOINT` + `RIFFDB_CREDENTIAL_FILE`) is set, TypeScript and Go `connectFromEnv` can auto-spawn or reuse a background `riffdb-driverd` on a short runtime socket. Conversely, Python's `riffdb_application` gains `DriverSocketApplicationTransport` / `RIFFDB_DRIVER_SOCKET` support so all three languages support both shared driver sockets and environment-driven connections.
3. **JSON-native and ergonomic SDK value types (`clients/typescript/runtime`, `clients/go/runtime`, `crates/riffdb-query-module`):**
   - TypeScript decodes timestamps as `RiffTimestamp` (`seconds`, `nanos`, `toISOString()`, `toDate()`, `toJSON(): string`), accepts `RiffTimestamp | Date | string | { seconds, nanos }` on timestamp inputs, accepts `bigint | number | string` (enforcing `Number.isSafeInteger` for `number` and exact integer bounds) on `u64`/`i64`/`revision` inputs, and attaches a non-enumerable `toJSON()` on decoded query rows/outcomes (`revision` -> decimal string, `RiffTimestamp` -> ISO-8601 string, `ExactDecimalValue` -> decimal string, `u64`/`i64` -> safe integer `number` or decimal string above $2^{53}-1$).
   - Go adds `Time()`, `InstantFromTime()`, `String()`, `MarshalJSON()`, and `UnmarshalJSON()` on `riffdb.Instant`, and `String()`, `MarshalJSON()`, and `UnmarshalJSON()` on `ExactDecimal` and `ExactMoney`.
4. **Loop-body revision fence recognition (`RDB-C030`) and `riffdb application lock` default (`crates/riffdb-contract-compiler`, `crates/riffdb-cli`):**
   - `RDB-C030` control-flow guard analysis propagates `if <binding>.revision != <expr> { return ... }` (and `==` else-return) guards inside `for` loop bodies to subsequent mutations of `<binding>` in the same loop iteration.
   - `riffdb application lock` defaults to `--write` when neither `--write` nor `--check` is passed.

## Standing design tests

- **Interface safety:** Session routing in `riffdb-driverd` remains fail-closed on `(database, role, contract_lineage, contract_version, contract_bundle_hash)` and never shares credentials across distinct `(database, role)` targets; numeric and decimal JSON/input conversions reject non-integer floats and preserve exact >53-bit integer and decimal representations as strings.
- **Scale:** Multi-application multiplexing bounds configured `[[applications]]` targets to `<= 64` per `riffdb-driverd` process and shares one bounded HTTP/2 connection pool per remote endpoint.

## Checks

- Unit and integration tests in `crates/riffdb-driver-host`, `crates/riffdb-query-module`, `crates/riffdb-contract-compiler`, `crates/riffdb-cli`, `clients/typescript/runtime`, `clients/go/runtime`, and `clients/python/runtime`.
- `./scripts/check-adr-form`, `./scripts/check-adr-obligations`, `./scripts/generate-cli-reference --check`, and `./scripts/handbook check`.
