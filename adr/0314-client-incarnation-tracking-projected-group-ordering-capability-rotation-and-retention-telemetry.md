---
adr: "0314"
title: Client Incarnation Tracking, Projected Group Ordering, Capability Rotation, and Retention Telemetry
status: accepted
tier: guarantee
date: 2026-10-16
accepted: 2026-10-16
acceptance: 'user, in session, 2026-10-16: "Yes" (approve WP-869..WP-872 to close known-limitations product gaps)'
requires: [ADR-0052, ADR-0072, ADR-0086, ADR-0087, ADR-0092, ADR-0182, ADR-0306, ADR-0309, ADR-0311, ADR-0312, ADR-0313]
amends: []
supersedes: []
requirements: [REC-002, STO-001, STO-030, STO-031, PRJ-002, PRJ-005, OQ-045, SEC-001, SEC-002, SEC-003, ID-005, PERF-001, END-007, END-008, DX-002, DX-014]
packages: [WP-869, WP-870, WP-871, WP-872]
obligations: []
review_triggers:
  - Client or MCP history-incarnation tracking overrides an explicit caller-supplied observed_history_incarnation or weakens RDB-HISTORY-0101 validation.
  - Projected group-by ordering accepts sort fields outside the declared group keys or exceeds max_group_cardinality during group accumulation.
  - Capability rotation or bootstrap renewal exposes raw bearer tokens in logs/inventory files or deletes authoritative database/key state.
  - Online retention telemetry exposes raw tenant, principal, entity, or request identifiers or alters public gRPC Health membership.
---
# ADR-0314: Client Incarnation Tracking, Projected Group Ordering, Capability Rotation, and Retention Telemetry

## Context

Four actionable product gaps remain in `docs/known-limitations.md`:

1. **Unwired client and MCP `observed_history_incarnation` tracking (`docs/known-limitations.md:64`)**:
   `RiffDbClient` (`crates/riffdb-client-rust`) defines `apply_observed_history_incarnation_get`, `apply_observed_history_incarnation_scan`, `apply_observed_history_incarnation_subscribe`, `remember_history_incarnation_from_get`, and `remember_history_incarnation_from_scan`, yet `get_commit`, `scan_commits`, `subscribe_commits`, `replay_events`, and `tail_events` never invoke them automatically. Likewise, `McpStdioBackend` (`crates/riffdb-mcp-stdio/src/backend.rs`) hardcodes `observed_history_incarnation: None` on `riffdb://commits/<seq>` resource reads and does not remember the session's observed `history_incarnation` across commit/event tool calls.
2. **Ad-hoc projected `group by` ordering, top-`N` `--limit`, and stale aggregate limitations prose (`docs/known-limitations.md:47`)**:
   Named RiffQL aggregates already support `having`, `order by`, top-`N` `take`, UTC temporal buckets, quantiles, `stddev`, `collect`, and per-parent rollups under `ADR-0309`, and `riffdb query projected` already accepts repeatable multi-column `--group-by`. However, on the ad-hoc projected path, `execute_group_by` in `crates/riffdb-columnar/src/query.rs` ignores `request.order` and `request.limit`, and `crates/riffdb-service/src/projected_query.rs` clamps `max_group_cardinality` to `limit` when `order` is requested instead of sorting accumulated groups and truncating to `limit`.
3. **No operator capability rotation CLI or convenience bootstrap credential renewal (`docs/known-limitations.md:22-23`)**:
   `riffdb capability` provides `bootstrap`, `create`, `revoke`, `list`, and `show`, but rotating an expiring capability requires manually copying its grants and role bindings into a new request, minting a replacement, rebinding roles, replacing the credential file, and revoking the superseded capability. In addition, `scripts/release-source-bootstrap` has no `--renew` flag to rotate expiring convenience credentials and rejects post-deployment health checks when an application contract is already active (`status == "healthy"`).
4. **Online retention state and active binding fence invisible until process shutdown (`docs/known-limitations.md:69-77`)**:
   `RetentionLaneStatus` (`crates/riffdb-server/src/retention_worker.rs`) tracks the live online retention state (`holding`, `idle`, `advancing`, `fenced`, `refused`, `cancelled`), active binding fence (`undelivered_outbox`, `projection_durable_frontier`, `operator_hold`, etc.), steps completed, and sequences pruned, but emits them only once at process exit in `shutdown_census.rs`, while `riffdb retention status` requires a closed database.

## Decision

1. **Automatic Client & MCP `observed_history_incarnation` Tracking (`WP-869`)**:
   - In `RiffDbClient` (`fixtures/compatibility/operation-adapters-v1/riffdb-client-rust/client.rs.in` and `crates/riffdb-client-rust/src/generated/client.rs`), automatically apply `self.observed_history_incarnation` when the caller leaves `observed_history_incarnation` unset (`None` or `0`) in `get_commit`, `scan_commits`, `subscribe_commits`, `replay_events`, and `tail_events`, and automatically remember positive `history_incarnation` values returned by `GetCommitResponse`, `ScanCommitsResponse`, `ReplayEventsResponse`, and `TailEventsResponse`.
   - In `McpStdioBackend` (`crates/riffdb-mcp-stdio/src/backend.rs`), track the session's observed `history_incarnation` (`AtomicU64`) and seed `RiffDbClient::set_observed_history_incarnation` across `McpResourceLocator::Commit`, `McpFixedToolRequest::GetCommit`, and `McpFixedToolRequest::ScanCommits`.

2. **Ad-Hoc Projected Group-By Ordering, Top-N Limit, and Aggregate Limitations Reconciliation (`WP-870`)**:
   - In `execute_group_by` (`crates/riffdb-columnar/src/query.rs`) and `crates/riffdb-service/src/projected_query.rs`, validate that any `request.order` fields on a grouped query belong to `group.keys`, sort materialized groups by `request.order` with canonical ascending `compare_values` tie-breaking across remaining group keys, and when `request.order` is non-empty, fold up to `budget.max_group_cardinality` and truncate the sorted groups to `request.limit` (bounded by the authorized row limit).
   - Reconcile `docs/known-limitations.md` and `docs/reference/CLI.md` to reflect `ADR-0309`'s RiffQL aggregate surface and the ad-hoc projected CLI's multi-column `--group-by`, `--order`, and `--limit` support.

3. **Operator Capability Rotation (`riffdb capability rotate`) & Bootstrap Renewal (`WP-871`)**:
   - Add `riffdb capability rotate <CAPABILITY_UUIDV7> --credential-output <PATH> [--capability-id <NEW_UUIDV7>] [--ttl-ms <MS>] [--revoke-old] [--inventory-file <PATH>] [--database-path <PATH>]` (`CommandIdentity::CapabilityRotate`) in `crates/riffdb-cli`, reading the source capability's non-secret metadata from the local inventory ledger or quiescent database, creating a replacement capability with identical principal and scope/command/query/view/stream grants, re-applying its role bindings, atomically writing the replacement credential file, optionally revoking the old capability (`RevocationReason::Replaced`), and updating the inventory ledger.
   - Add `--renew` to `scripts/release-source-bootstrap` and `xtask` (`cargo riffdb bootstrap (--user|--system) --renew`) to rotate existing convenience `operator` and `mcp` credentials via `riffdb capability rotate --revoke-old`, and allow `release-source-bootstrap`'s health verification to accept deployed databases (`status == "healthy"` or `"degraded"`) alongside pre-deployment `"not_ready"`.

4. **Online Retention Telemetry & Fence Observability (`WP-872`)**:
   - Add redaction-safe atomic online retention telemetry (`online_retention_state`, `online_retention_fence`, `online_retention_steps_total`, `online_retention_sequences_total`, `online_retention_watermark_sequence`) to `Observability` (`crates/riffdb-observability`), updated by `retention_worker.rs` (`crates/riffdb-server`).
   - Expose these gauges/counters in `encode_prometheus_v1`, `encode_otel_json_v1`, `encode_flight_record_ndjson_v1`, and `riffdb telemetry summarize`, and update `docs/known-limitations.md` and `docs/configuration.md`.

## Options and consequences

- Preserving explicit per-request `observed_history_incarnation` precedence while defaulting unset fields from the client's remembered incarnation protects stateful clients and MCP sessions across destructive restores without breaking stateless callers.
- Keeping online retention telemetry in `Observability` (`/metrics`, `/v1/metrics`, and the NDJSON flight recorder) honors `ADR-0182` section 6 (no public gRPC `Health` member or control RPC) while giving operators live visibility into active retention fences.

## Standing design tests

- **Interface safety:** Explicit caller-supplied `observed_history_incarnation` always takes precedence over client-remembered state. Grouped projected `order` rejects non-group-key fields. Capability rotation never logs or persists raw bearer tokens in inventory files. Online retention telemetry emits only closed enum codes/labels and numeric counters (`ID-005`).
- **Scale:** Grouped projected queries remain bounded by `max_group_cardinality` (`<= 10,000`) and `MAX_AGGREGATE_STATE_BYTES_V1` (`1 MiB`). Retention telemetry updates use $O(1)$ atomic stores.

## Checks

- `client_and_mcp_track_observed_history_incarnation_across_commit_and_event_reads` in `crates/riffdb-client-rust` and `crates/riffdb-mcp-stdio`.
- `projected_group_by_honors_key_order_and_top_n_limit` in `crates/riffdb-columnar` and `crates/riffdb-service`.
- `capability_rotate_cli_mints_replacement_rebinds_roles_and_revokes_superseded` in `crates/riffdb-cli` and `./scripts/release-source-bootstrap-smoke`.
- `online_retention_telemetry_exposes_state_fence_and_counters_in_prometheus_otel_and_flight_recorder` in `crates/riffdb-observability` and `crates/riffdb-server`.
