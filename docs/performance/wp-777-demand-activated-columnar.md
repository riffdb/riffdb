# WP-777 demand-activated columnar runtime

WP-777 keeps every admitted columnar source cold until a semantic projected
query demands it. Core startup retains only bounded checked registrations and
one server-owned worker. It does not open or decode an artifact, create a
snapshot, scan authoritative population, catch up, publish, notify, or
checkpoint merely because a source is configured.

## First-demand behavior

The first query atomically moves one source from `Cold` to `Activating`, wakes
the existing columnar worker, and immediately returns the public typed
`Building` result with no rows. Concurrent first queries coalesce: no caller
owns a task or retained request, and cancellation cannot cancel the shared
activation. The worker rereads the current vector selection where applicable,
opens exactly that generation, validates its complete bounded artifact and
durable frontier, and installs one immutable view. Ordinary apply,
publication, checkpoint, freshness, retention, policy, cursor, and
notification semantics resume only after that installation.

Failure is closed and rowless. Corrupt, partial, mismatched, stale, excessive,
unknown, incompatible, or unavailable selected state does not fall back to a
different generation, layout, source, partition set, frontier, V1 encoding, or
authoritative scan. The source remains failed for the process generation, so
requests cannot create an unbounded reopen loop. The `projection` health
component is degraded while a source is cold, activating, or failed; it becomes
healthy only when all admitted sources are active (or when none are admitted).

No application, agent, SDK, transport, operator, MCP, CLI, or configuration
surface can choose activation timing, prewarm a source, select eager mode,
select artifact bytes, skip validation, control the worker count, or alter the
bounds.

## Lifecycle and resource evidence

Startup and shutdown census lines report bounded `columnar_cold_sources`,
`columnar_activations`, and `columnar_population_passes` counters. A configured
no-demand lifecycle must report admitted cold sources with zero activations and
zero population passes. A clean close remains eligible because the columnar
plane is derived and the worker is joined before storage closes; shutdown does
not activate a cold source.

Demand activation does not relax the post-demand PERF-007 and PERF-008 gates.
After activation, the production graph still owns one authoritative writer,
uses least-authority reader handles and immutable shared plans/views, preserves
fresh authorization and response-release checks, and retains all existing
bounded concurrency, heap, latency, throughput, and shutdown acceptance gates.
The exact WP-705 lifecycle checkpoints provide release evidence separately;
this page makes no performance claim until those fixed receipts pass.

The semantic proofs are
`configured_columnar_artifacts_remain_cold_through_readiness_and_clean_close`,
`first_columnar_demand_coalesces_one_activation_and_returns_building_without_rows`,
`columnar_activation_installs_only_the_validated_selected_immutable_view`,
`failed_columnar_activation_never_serves_rows_or_falls_back`, and
`columnar_activation_shutdown_abandons_without_stop_caused_publication`.
