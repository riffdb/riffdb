---
adr: "0319"
title: Online Dynamic Multi-Database Provisioning and High-Density Hosting
status: accepted
tier: surface
date: 2026-10-18
accepted: 2026-10-18
acceptance: "User selected Option B (ADR-0319 online dynamic multi-database provisioning and high-density hosting)"
requires: ["0156", "0188", "0292", "0316", "0317", "0318"]
amends: ["0292"]
supersedes: []
requirements: ["NET-005", "REC-001", "REC-002", "REC-004", "PERF-007", "PERF-019"]
packages: ["WP-879"]
obligations:
  - id: OBL-0319-1
    package: WP-879
    description: "Reloading a multi-database configuration dynamically provisions newly added [databases.<alias>] sections onto GrpcDatabaseRoutes and HostedMcp and cleanly deprovisions removed aliases (releasing their redb lock and committing CleanCloseState::Clean) without interrupting active peer databases"
    proof: online_multi_database_reload_provisions_and_deprovisions_aliases_without_interrupting_peers
  - id: OBL-0319-2
    package: WP-879
    description: "Reloading an invalid, empty, path-colliding, or process-global-mutating configuration fails closed, logs a structured reload-rejected event, and retains the active multi-database inventory unchanged"
    proof: invalid_or_incompatible_multi_database_reload_fails_closed_and_retains_active_inventory
review_triggers:
  - "Online deprovisioning removes an alias without completing complete_graceful_close or drops the last remaining database in the process."
  - "Online provisioning admits traffic to a newly added alias before startup validation and disjoint-path checks succeed, or mutates an existing alias's immutable storage paths in place."
---
# ADR-0319: Online Dynamic Multi-Database Provisioning and High-Density Hosting

## Context

`ADR-0292` introduced multi-database hosting (`[databases.<alias>]`) with a static `MAX_DATABASES_PER_PROCESS = 32` ceiling and a startup-fixed route map in `GrpcDatabaseRoutes` and `HostedMcp`. With per-database resource budgets (`ADR-0316`), compact 8 MiB journal extents (`ADR-0317`), and scale-to-zero idle hibernation (`ADR-0318`), a single `riffdbd` process can now host hundreds of compact or hibernated developer and preview databases, but adding or removing a database still requires restarting `riffdbd` and briefly interrupting every peer database in the process.

## Decision

1. **High-Density Multi-Database Ceiling (`crates/riffdb-types`, `crates/riffdb-server`):**
   - Raise `MAX_DATABASES_PER_PROCESS` (`crates/riffdb-types/src/database.rs`) from `32` to `512` (amending `ADR-0292`'s initial `32`-database bound).
   - Raise `MAX_CONFIG_DOCUMENT_BYTES` (`crates/riffdb-server/src/config.rs`) from `65_536` (`64 KiB`) to `524_288` (`512 KiB`) so a 512-database TOML configuration document parses within bounded memory.

2. **Dynamic Route Table Mutation (`crates/riffdb-api-grpc`, `crates/riffdb-server`):**
   - `GrpcDatabaseRoutes` (`crates/riffdb-api-grpc/src/server.rs`) and `HostedMcp` (`crates/riffdb-server/src/hosted_mcp.rs`) expose bounded `insert`, `remove`, and `contains` operations over their `RwLock`-backed route maps.
   - `insert` rejects duplicate aliases and enforces `MAX_DATABASES_PER_PROCESS`. `remove` rejects unknown aliases and refuses to remove the last remaining database route (`>= 1` active route invariant).

3. **Live Multi-Database Configuration Reconciliation (`crates/riffdb-server`):**
   - `ServerConfig` retains the resolved `--config <path>` (or `RIFFDB_CONFIG`) path when loaded from disk. In multi-database mode, `riffdbd` monitors the configuration file's `(dev, ino, mtime, len)` identity (matching `ReloadableTlsIdentity`) and invokes `reconcile_multi_database_inventory` on change.
   - Reconciliation parses and validates the candidate configuration (including `validate_disjoint_paths` across all configured databases), verifies that process-global settings (`mode`, `application_listener`, `audience`, `mcp_listener`, `mcp_origins`, `capability_key_path`, `idempotency_key_path`, `redb_commit_profile`) and retained databases' immutable storage identity (`database_path`, `backup_root`, `projections_root`, `environment`) have not changed, and fails closed on any error by logging `riffdbd config reload rejected` while keeping the current inventory running.
   - **Added aliases:** Opened via `build_single_database_graph`, wrapped in `HibernatableDatabaseRoute` when `idle_hibernate_seconds` is set, registered with supervisor monitors, and published into `GrpcDatabaseRoutes` and `HostedMcp`.
   - **Removed aliases:** Atomically unpublished from `GrpcDatabaseRoutes` and `HostedMcp`, drained and closed via `MultiDatabaseGraph::shutdown` (`complete_graceful_close`), releasing their `redb` file lock and worker threads while peer databases continue serving uninterrupted.

## Standing design tests

- **Interface safety:** Added databases undergo full startup validation, disjoint-path verification, and capability/idempotency isolation before route publication; removed databases drain in-flight work and commit `CleanCloseState::Clean` before releasing storage locks; invalid reloads fail closed without disturbing live databases.
- **Scale:** Raises per-process hosting capacity 16x (`32 -> 512` databases) with `O(delta)` online provisioning and deprovisioning and zero downtime for peer databases.

## Checks

- `online_multi_database_reload_provisions_and_deprovisions_aliases_without_interrupting_peers` (`OBL-0319-1`).
- `invalid_or_incompatible_multi_database_reload_fails_closed_and_retains_active_inventory` (`OBL-0319-2`).
- `./scripts/acceptance --wp WP-879`.
