---
adr: "0306"
title: Prometheus OTel Metrics Exposition And Soak Flight Recorder
status: accepted
tier: surface
date: 2026-10-15
accepted: '2026-10-15'
acceptance: 'maintainer, in session, 2026-10-15: "yes", in response to the ADR-0306 and WP-845..WP-847 proposal'
requires: [ADR-0049, ADR-0086, ADR-0111, ADR-0184, ADR-0250]
amends: [ADR-0049, ADR-0184]
supersedes: []
requirements: [ID-005, PERF-001, END-007, END-008, DX-014]
packages: [WP-845, WP-846, WP-847]
obligations: []
review_triggers:
  - Metrics exposition or flight-recorder output emits raw tenant identifiers, entity keys, request identifiers, capability tokens, or unbounded label cardinalities.
  - Loopback metrics listener permits non-loopback socket binding or mutating operations.
  - riffdb-observability adds non-leaf crate dependencies beyond riffdb-types, riffdb-errors, and tracing.
---
# ADR-0306: Prometheus OTel Metrics Exposition And Soak Flight Recorder

## Context

`riffdb-observability` already collects a comprehensive, closed, redaction-safe in-process telemetry inventory across all 40 required metric families, 5 command pipeline stages, 15 read pipeline stages, 5 write service stages, preparation-pool and reorder-buffer depth histograms, completion-lane counters, and authoritative health/readiness state. However, `ADR-0049` and `ADR-0184` originally left metrics strictly in-process with no exposition surface:

1. **Operator and founding-engineer visibility gap:** External OpenTelemetry collectors, Prometheus scrapers, and local debugging workflows cannot inspect live `riffdbd` counters, gauges, or stage latency histograms without attaching a debugger or reading internal benchmark harnesses.
2. **24/72-hour endurance and soak observability gap (`END-007`, `END-008`):** Long-running TicketDesk lifecycle and soak runs (`examples/ticketdesk`) verify correctness and coarse phase summaries, but when a latency spike, queue buildup, or storage growth inflection occurs hours into a 24-hour or 72-hour run, operators need periodic time-series snapshots of stage latency percentiles (`p50`, `p95`, `p99`), writer busy/idle ratios, queue-delay EWMA, and database/journal byte high-water marks on disk.

## Decision

1. **Pure-Rust Prometheus/OpenMetrics text and OTLP JSON encoders in `riffdb-observability` (`WP-845`):**
   - `riffdb-observability` remains a strict leaf crate depending only on `riffdb-types`, `riffdb-errors`, and `tracing`.
   - `MetricRegistry` and `Observability` expose deterministic, allocation-bounded `encode_prometheus_v1(&self) -> String`, `encode_otel_json_v1(&self, timestamp_unix_nanos: u64) -> String`, and `encode_flight_record_ndjson_v1(&self, timestamp_unix_nanos: u64, storage_bytes: u64, journal_bytes: u64) -> String` methods implemented with `std::fmt::Write`.
   - Exposition covers the 40 required metric families, closed pipeline stage histograms (`command_stage`, `read_stage`, `write_service_stage`, `preparation_pool_depth`, `reorder_buffer_occupancy`, `command_group_residence`), completion-lane counters, and health status gauge in canonical sorted order.
   - All labels remain restricted to closed compile-time enums and bounded numeric identifiers (`ContractVersion`, `CommandId`, `ProjectionId`, `HotKeyBucket`); raw tenant, principal, request, entity, and secret values are structurally unrepresentable (`ID-005`).
2. **Optional loopback metrics HTTP listener and periodic NDJSON flight recorder in `riffdbd` (`WP-846`):**
   - `riffdbd` (`crates/riffdb-server`) accepts optional `--metrics-listen <LOOPBACK_IP_LITERAL:NONZERO_PORT>` (`RIFFDB_METRICS_LISTEN` / `server.metrics_listen` in TOML). Like `--mcp-listen`, non-loopback addresses and zero ports fail closed at configuration parse time.
   - When configured, `riffdbd` serves read-only HTTP/1 `GET /metrics` (`text/plain; version=0.0.4; charset=utf-8` Prometheus exposition) and `GET /v1/metrics` (`application/json` OTLP JSON `ExportMetricsServiceRequest` envelope) using its existing `axum` dependency. All other paths and methods fail closed.
   - `riffdbd` also accepts optional `--telemetry-flight-recorder <PATH>` (`RIFFDB_TELEMETRY_FLIGHT_RECORDER` / `server.telemetry_flight_recorder`) and `--telemetry-flight-interval-ms <MS>` (`RIFFDB_TELEMETRY_FLIGHT_INTERVAL_MS` / `server.telemetry_flight_interval_ms`, bounded to `100..=3_600_000`, default `10_000`) to periodically append one bounded NDJSON snapshot line per interval during server execution and on graceful drain.
3. **CLI flight-recorder summary (`riffdb telemetry summarize`) and 24/72-hour rehearsal integration (`WP-847`):**
   - `riffdb-cli` adds `riffdb telemetry summarize <PATH>` (supporting human-readable and `--output json` modes) to aggregate a flight-recorder NDJSON log into sample counts, window throughput, stage latency percentile high-water marks (`p50`, `p95`, `p99`), writer busy/idle saturation, storage/journal byte growth, and health transitions.
   - `examples/ticketdesk` (`lifecycle` binary) wires optional periodic flight-recorder capture into its rehearsal artifact directory so 24/72-hour soak runs record continuous telemetry time-series alongside the final rehearsal report.

## Consequences

- Operators and OpenTelemetry/Prometheus agents can scrape `riffdbd` over loopback (`GET /metrics` or `GET /v1/metrics`) without sidecars or unsafe remote exposure, and 24/72-hour endurance runs produce continuous, inspectable time-series flight logs.

## Standing design tests

- **Interface safety:** Metrics exposition and flight records emit only closed-vocabulary labels and numeric IDs from `riffdb-observability`, never raw business keys, tenant IDs, or secrets (`ID-005`). `--metrics-listen` binds exclusively to literal IPv4/IPv6 loopback addresses and exposes only read-only `GET /metrics` and `GET /v1/metrics`.
- **Scale:** `MAX_COMMAND_METRIC_SERIES` remains capped at `1024` with explicit overflow drop counters; each flight-recorder NDJSON snapshot is single-line and bounded, and `riffdb-observability` adds zero third-party dependencies.

## Checks

- Unit and integration tests for Prometheus text and OTLP JSON exposition, redaction canary exclusion, loopback-only `--metrics-listen` enforcement, periodic NDJSON flight-recorder emission, and `riffdb telemetry summarize`.
