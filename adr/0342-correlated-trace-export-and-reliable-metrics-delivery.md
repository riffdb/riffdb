---
adr: "0342"
title: Correlated Trace Export And Reliable Metrics Delivery
status: accepted
tier: surface
date: 2026-11-08
accepted: '2026-11-08'
acceptance: 'maintainer, in session, 2026-11-08: "i accept, build", accepting the exact ADR-0341 through ADR-0343 text reserved at 5e54542e4'
requires: [ADR-0049, ADR-0306, ADR-0332, ADR-0341, ADR-0180]
amends: [ADR-0049, ADR-0306, ADR-0332]
supersedes: []
requirements: [ID-005, PERF-001, NET-002, NET-003, END-007, END-008]
packages: [WP-916, WP-917]
obligations: []
review_triggers:
  - Remote telemetry can disable TLS verification, follow redirects, use remote cleartext, or load credentials from arbitrary headers/environment detectors.
  - Trace context or sampling can influence authorization, idempotency, routing, or durable identities.
  - Export changes required metric semantics, numeric identity cardinality, semantic readiness, or authoritative commit behavior.
  - A network/export SDK enters a semantic owner or riffdb-observability gains a non-leaf dependency.
---
# ADR-0342: Correlated Trace Export And Reliable Metrics Delivery

## Context

Closed tracing hooks and in-process metrics already exist, but external export
currently covers metrics only. Operators cannot correlate operational logs with
exported request spans, distinguish collector rejection from delivery, or prove
signal isolation under backpressure. All three signals need bounded, independently
observable delivery while retaining the existing metric registry and TLS profile.

## Decision

1. Compose optional log/trace exporters in `riffdb-server`; semantic crates and
   `riffdb-observability` keep their existing dependency boundaries and typed
   hooks. Use official Rust OpenTelemetry APIs/SDK/OTLP encoders and the tracing
   bridges rather than a second bespoke span/log protocol. Admit exact versions
   `opentelemetry`, `opentelemetry_sdk`, `opentelemetry-otlp`, and
   `opentelemetry-appender-tracing` 0.33.1; `tracing-opentelemetry` 0.34.0;
   defaults disabled, enabling only logs/trace, SDK batch support, and OTLP
   HTTP/JSON encoding needed for the two signals. Use a bounded custom HTTP
   client adapter over the accepted server TLS transport; no reqwest/default
   native TLS, compression, gRPC exporter, auto-detectors or internal-logs feature.
   Verify exact feature names, compatible graph, MSRV, licenses and advisories
   before dependency admission; feature expansion needs review.
2. Add non-reloadable `[server.telemetry.logs]` and `[server.telemetry.traces]`
   tables with `endpoint`, optional protected `bearer_token_file` and
   `ca_certificate_path`, `queue_capacity` (64..=4096, default 2048),
   `batch_size` (1..=512, default 128 and no greater than queue capacity),
   `export_interval_ms` (100..=60000, default 1000), and `timeout_ms`
   (100..=10000, default 5000). Traces additionally select
   `sample_per_million` (0..=1000000, default 100000). Each signal is disabled
   when its table is absent. Existing metric push flags/environment/TOML remain
   compatible. New tables use file configuration only; do not silently consume
   SDK `OTEL_*` environment configuration, resources or endpoints.
3. Require canonical verified HTTPS or literal-loopback HTTP endpoints using
   NET-002/NET-003 and existing protected secret/CA custody. Bound URL, headers,
   request/response sizes, connection and whole-export time. Reject userinfo,
   fragments, redirects, remote cleartext and disabled verification before I/O.
   Each signal uses its explicit full endpoint; never rewrite a metrics URL to
   guess a logs/traces destination. Reuse the accepted crypto provider.
4. Server-owned resource attributes are fixed `service.name=riffdbd`, build
   version, and a fresh opaque process instance ID. Add only checked database
   aliases in their established bounded scope. Do not auto-detect hostnames,
   addresses, process arguments, environment, principal, or tenant metadata.
   OTLP logs derive only from ADR-0341 checked events; safe request spans keep
   the complete SPEC 16.4 field inventory where available, static span names,
   and bounded numeric/status attributes. Never reconstruct spans from sampled
   metric snapshots or infer successful outcomes from missing observations.
5. Trace one API-neutral request across current gRPC, hosted/stdio MCP bridge,
   CLI and SDK request paths, keeping operation/request/incident correlation
   distinct from trace/span identity. Bound live spans to 4096 per process,
   attributes/events/links to 32/32/1 per span, and individual strings to 256
   bytes. Local sampling is process-owned and cannot affect mandatory metrics
   or operational warnings. Export transport failure cannot change service
   behavior. A bounded trace is observability evidence, not a durable audit.
6. At ingress, optionally accept only syntactically valid bounded W3C
   `traceparent`; ignore invalid/duplicate context and omit `baggage` and
   `tracestate`. Remote IDs and the sampled bit confer no authority and cannot
   exceed local sampling/resource bounds. Correlate local outgoing client calls
   through public transport metadata without adding durable/protobuf identity.
   Preserve MCP hard filters; protocol buffers/stdout never carry log records.
7. Keep ADR-0306's frozen metric names, types, bucket boundaries, label bounds,
   Prometheus route and OTLP JSON shape. Fix exporter success accounting to parse
   complete bounded HTTP and OTLP responses, including partial rejection, 429,
   retryable 5xx, malformed/truncated/oversized bodies and TLS failure. No attempt
   counts as accepted merely because a write or HTTP 2xx occurred. Add monotonic
   self-observability by closed signal/sink/reason: accepted, failed, rejected,
   dropped, queue depth, export duration and last successful export age. No
   endpoint, exception string, trace ID, tenant or request becomes a label.
8. Each signal has its own bounded worker/queue; one in-flight batch per signal,
   at most 1 MiB per request and 64 KiB per response. Metrics coalesce to one
   latest cumulative snapshot rather than queueing every interval. Logs/traces
   use at most three bounded attempts per batch within the configured export
   deadline, with capped backoff and bounded Retry-After; count final loss.
   Never replay cumulative metric samples as deltas. A broken exporter cannot
   recurse through its own log pipeline, starve healthy signals or block RPCs.
9. Sampling, overflow, collector outage and process crash may lose diagnostic
   telemetry. Required semantic observations and existing readiness decisions
   retain their accepted rules. On shutdown, stop admission, finish request
   observations, and drain all diagnostic sinks under ADR-0341's one shared
   at-most-five-second budget inside the existing process deadline. Expose
   dropped/unflushed records; do not promise durable logs or exactly-once export.

## Standing design tests

- **Interface safety:** no SDK setting, trace context, resource attribute or
  exporter failure can bypass redaction, authentication, authorization, TLS,
  durability, provenance or idempotency. No application data leaves typed hooks.
- **Scale:** all span inventories, records, attributes, queues, batches, responses,
  retries and waits are bounded independently of database population; metrics
  retain fixed label vocabularies and snapshot coalescing.

## Checks

- Real collector decoding of all three signals and trace/log correlation, plus
  golden metric compatibility, label-cardinality and secret-canary tests.
- Controlled-clock/scheduler tests for sampling, 429/Retry-After, partial success,
  malformed responses, saturation, failed DNS/TLS, outage/recovery and shutdown.
- Concurrent-database/process tests proving signal isolation and stable outcomes
  under stalled exporters; package acceptance, dependency policy and full CI.
