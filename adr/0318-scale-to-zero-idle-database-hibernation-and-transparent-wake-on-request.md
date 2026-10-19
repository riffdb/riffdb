---
adr: "0318"
title: Scale-to-Zero Idle Database Hibernation and Transparent Wake-on-Request
status: accepted
tier: surface
date: 2026-10-18
accepted: 2026-10-18
acceptance: "User selected Option A (ADR-0318 scale-to-zero idle database hibernation and wake-on-request)"
requires: ["0095", "0156", "0188", "0316", "0317"]
amends: []
supersedes: []
requirements: ["NET-005", "REC-001", "REC-002", "REC-004", "PERF-007", "PERF-019"]
packages: ["WP-878"]
obligations:
  - id: OBL-0318-1
    package: WP-878
    description: "An idle primary database configured with idle_hibernate_seconds gracefully closes its RunningProductionGraph after the idle window, releases its redb lock and worker threads, and transparently wakes on the next gRPC or Hosted MCP request without returning an error"
    proof: idle_database_hibernates_releases_lock_and_wakes_transparently_on_request
  - id: OBL-0318-2
    package: WP-878
    description: "Concurrent requests arriving for a hibernated database coalesce onto a single-flight wake transition and process shutdown leaves hibernated databases clean without reopening them"
    proof: concurrent_wake_requests_coalesce_single_flight_and_shutdown_skips_hibernated_database
review_triggers:
  - "Hibernating a database bypasses complete_graceful_close or drops uncheckpointed writes, active subscriptions, or pending outbox deliveries."
  - "Waking a hibernated database bypasses startup validation, capability authentication, or per-database resource limits."
---
# ADR-0318: Scale-to-Zero Idle Database Hibernation and Transparent Wake-on-Request

## Context

`ADR-0317` reduced the initial on-disk journal footprint of a compact database from `640 MiB` to `8 MiB`, enabling hundreds of developer and preview databases to reside on a single commodity NVMe volume. However, `riffdbd` currently keeps every configured `RunningProductionGraph` open in memory for the lifetime of the server process (`crates/riffdb-server/src/daemon.rs`), retaining `redb` page caches, `TransientIndexes`, file descriptors, and per-database worker threads even when a tenant database receives no traffic for hours.

To host high-density multi-tenant developer databases with working-set memory and thread count proportional only to actively used databases, `riffdbd` needs an opt-in idle hibernation lifecycle that cleanly closes idle databases using the existing graceful-close protocol (`ADR-0156`, `ADR-0188`) and transparently wakes them on the next gRPC or Hosted MCP request.

## Decision

1. **Configurable `idle_hibernate_seconds` and Safe Idle Hibernation (`crates/riffdb-server`):**
   - `[maintenance.limits]` and `[databases.<alias>.limits]` accept an optional `idle_hibernate_seconds = <u64>` (`1..=86_400`, default `None` / disabled). Unknown, zero, or out-of-range values fail closed with `ServerConfigError::InvalidLimitsConfiguration`.
   - When `idle_hibernate_seconds` is configured on a primary database, `riffdbd` installs a `HibernatableDatabaseRoute` (`crates/riffdb-server/src/hibernation.rs`) implementing both `GrpcLifecycleRoute` and `CredentialAuthenticator`. Every gRPC lifecycle admission and Hosted MCP request records monotonic activity on the route.
   - Once `idle_hibernate_seconds` elapses without activity and the database has zero active service jobs (`spawner.active_jobs() == 0`), zero live commit subscribers (`notifications.active_subscriber_count() == 0`), zero pending outbox deliveries (`pending_outbox_delivery_count() == Ok(0)`), and ordinary maintenance readiness (`maintenance_lifecycle.ordinary_admission_available()`), the supervisor transitions the route to `Hibernated`, drains and closes its `RunningProductionGraph` via `complete_graceful_close()`, and drops the graph so `0` `redb` caches, worker threads, or database file locks remain held.

2. **Single-Flight Transparent Wake-on-Request (`crates/riffdb-server`):**
   - When a gRPC or Hosted MCP request arrives for a `Hibernated` database, `HibernatableDatabaseRoute` runs a single-flight wake transition that reopens the database via `open_redb_startup_with_extent_profile` (observing the `CleanCloseState::Clean` receipt written at hibernation), builds a fresh `RunningProductionGraph` and `ProductionLifecycleRoute`, installs the new route and `CredentialAuthenticator`, and registers the awakened generation with the supervisor.
   - Concurrent requests arriving while a wake is in progress wait on the same single-flight wake completion and execute against the awakened generation without returning a transient `Unavailable` or `404` error.
   - Process shutdown stops active generations while leaving already-hibernated databases in their verified `CleanCloseState::Clean` on-disk state without reopening them.

## Standing design tests

- **Interface safety:** Applications, CLI users, and MCP agents observe identical authentication, authorization, durability, and fresh-read semantics across hibernation and wake transitions; no unsafe opt-out or client retry loop is required.
- **Scale:** A hibernated database holds `0` `redb` caches, `0` worker threads, and `0` open database file descriptors while idle, and single-flight wake prevents thundering-herd reopens under concurrent arrival.

## Checks

- `idle_database_hibernates_releases_lock_and_wakes_transparently_on_request` (`OBL-0318-1`).
- `concurrent_wake_requests_coalesce_single_flight_and_shutdown_skips_hibernated_database` (`OBL-0318-2`).
- `./scripts/acceptance --wp WP-878`.
