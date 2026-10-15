---
adr: "0310"
title: Audited failover orchestration, standby client routing, and Helm follower deployment
status: accepted
tier: surface
date: 2026-10-15
accepted: 2026-10-15
acceptance: "maintainer, in session, 2026-10-15 (lets tackle the replication improvements)"
requires: [ADR-0072, ADR-0093, ADR-0100, ADR-0178, ADR-0271]
amends: []
supersedes: []
requirements: [REP-002, REP-004, REP-005, REP-006, DRV-001, DRV-005, DX-014]
packages: [WP-859, WP-860, WP-861]
obligations: []
review_triggers:
  - "Failover orchestration bypasses primary fencing proof, authority checks, or lineage/incarnation validation."
  - "Client standby routing silently downgrades history incarnation, leadership epoch, or idempotency guarantees."
  - "Helm follower rendering embeds secret tokens in ConfigMaps instead of referencing Kubernetes Secrets."
---
# ADR-0310: Audited failover orchestration, standby client routing, and Helm follower deployment

## Context

`ADR-0093` and `ADR-0178` established the asynchronous changelog-shipping replication tier (`REP-002` through `REP-009`), including follower mode, capability-guarded replication streams, primary fencing (`fence_replication_primary`), incarnation-fenced follower promotion (`promote_follower`), and follower read policies (`Available`, `Bounded`, `Causal`). However, three operational and client-ergonomic gaps remain in the surface layer:

1. **Multi-step manual CLI cutover and health inspection**: Operators must inspect replication lag by digging through `riffdb server health` JSON, and executing a planned failover requires manually minting two UUIDv7 operation IDs and running `riffdb follower fence-primary` against the primary followed by `riffdb follower promote` against the follower with six repeated lineage flags.
2. **Static single-endpoint application clients and driver hosts**: `StableApplicationClient` and `DriverPool` (`riffdb-driver-host`) bind to a single static endpoint. When a primary is fenced (`RDB-REP-0102`) or promoted, applications must be reconfigured and restarted, and read-only queries (`Available`, `Bounded`, `Causal`) cannot be automatically offloaded to follower replicas.
3. **Helm chart primary-only configuration**: `release/helm/riffdb` renders only primary-mode `riffdbd.toml` files and provides no values structure for deploying a standby follower pod wired to a primary's replication source.

## Decision

1. **CLI Replication Status & One-Command Audited Failover (`WP-859`)**:
   - Add `riffdb follower status` (`CommandIdentity::FollowerStatus`), which queries `Health` on the target node, extracts the `replication` component and `ReplicationStatistics` (`role`, `history_incarnation`, `source_frontier`, `applied_frontier`, `acknowledged_frontier`, `registered_followers`, `application_lag_sequences`, `administration_lag_sequences`), and renders a dedicated replication summary.
   - Add `riffdb follower failover` (`CommandIdentity::FollowerFailover`), which accepts the follower target selection (`--database-id`, `--history-incarnation`, `--leadership-epoch`, `--hold-id`, `--registration-generation`), `--primary-config <PATH>` or `--primary-endpoint <URI>`, and optional `--fence-operation-id` / `--operation-id` (auto-minting UUIDv7s when omitted). It executes `fence_replication_primary` against the primary, verifies the fence receipt, and executes `promote_follower` against the follower, returning one unified cutover receipt (`fence_status`, `promotion_status`, `final_application_sequence`, `applied_application_sequence`, `application_rpo`, `history_incarnation`, `leadership_epoch`). If either step is uncertain, the error output preserves both operation IDs for exact idempotent retry.
2. **Standby-Aware Client Failover & Read-Replica Routing (`WP-860`)**:
   - Extend `StableApplicationClient` and `DriverPool` / `DriverRuntime` (`crates/riffdb-driver-host`) with optional bounded `standby_endpoints` (up to 4 endpoints) and `read_replica_endpoints` (up to 4 endpoints).
   - When a command or write operation encounters a fenced-primary/follower-mode refusal or transport unavailability and standby endpoints are configured, the client probes standby channels, verifies active contract identity (`verify_active_contract`), promotes the verified standby channel to active primary, and retries idempotent operations without weakening incarnation or outcome guarantees.
   - When `read_replica_endpoints` are configured, eligible read queries are routed to healthy replica channels first and fall back to the primary channel on replica unavailability or follower-lag refusal.
3. **Helm Chart Follower Deployment (`WP-861`)**:
   - Extend `release/helm/riffdb/values.yaml` and `templates/configmap.yaml` with `server.mode` (`"primary"` default or `"follower"`) and `server.replicationSource` (`endpoint`, `serverName`, `tlsTrustRootFile`, `credentialSecretKey`, `database`, `databaseId`, `historyIncarnation`, `leadershipEpoch`, `holdId`), rendering `[server.replication_source]` in `riffdbd.toml` with credentials mounted from Kubernetes Secrets.

## Standing design tests

- **Interface safety:** Failover remains an explicit audited administrative operation requiring primary fence proof (`REP-005`); no unattended consensus election is introduced. Client failover verifies exact contract identity before cutover and preserves idempotency and monotonic incarnation fencing.
- **Scale:** Standby and read-replica endpoint lists are statically bounded (`<= 4` endpoints each), and `riffdb follower failover` performs exactly two bounded unary administrative RPCs (`fence_replication_primary` and `promote_follower`).

## Checks

- `follower_status_and_failover_cli_orchestrate_fenced_cutover_and_preserve_uncertainty_ids` in `crates/riffdb-cli`.
- `driver_pool_and_application_client_failover_to_standby_and_route_replica_reads` in `crates/riffdb-driver-host` and `crates/riffdb-client-rust`.
- `./scripts/check-helm-render` and `./scripts/handbook check`.
