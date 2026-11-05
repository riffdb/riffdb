---
adr: "0332"
title: External OTLP Metrics Push and Outbox Webhook/Broker Push Delivery
status: accepted
tier: guarantee
date: 2026-11-05
accepted: 2026-11-05
acceptance: "User approved external OTLP metrics push and outbox webhook/broker delivery plan"
requires: ["0049", "0102", "0180", "0306", "0316"]
amends: ["0049", "0306"]
supersedes: []
requirements: ["EFF-001", "EFF-002", "EFF-003", "ID-005", "NET-002", "NET-003", "PERF-001", "DX-014"]
packages: ["WP-888", "WP-889", "WP-890"]
obligations:
  - id: OBL-0332-1
    package: WP-888
    description: "RunningTelemetryExporter and ServerConfig push redaction-safe OTLP/HTTP JSON metrics snapshots periodically and on graceful shutdown drain to verified HTTPS or loopback HTTP collector endpoints while rejecting remote cleartext HTTP"
    proof: telemetry_exporter_pushes_otlp_json_periodically_and_on_shutdown
  - id: OBL-0332-2
    package: WP-889
    description: "ConfiguredOutbox and VerifiedWebhookConnector deliver committed durable events to verified HTTPS or loopback HTTP webhook/broker endpoints with EventId idempotency headers, optional bearer authentication, and deterministic 2xx/retryable/4xx dead-letter classification"
    proof: webhook_outbox_connector_delivers_idempotent_events_and_classifies_retry_and_dead_letter
  - id: OBL-0332-3
    package: WP-890
    description: "Handbook pages (docs/known-limitations.md, docs/configuration.md, and docs/security.md) remove the two resolved Platform and Deployment bullets and document mTLS, auto_issue, HTTP/JSON ingress, OTLP push, and http_webhook outbox delivery"
    proof: scripts/handbook
review_triggers:
  - "OTLP push or outbox webhook delivery permits remote cleartext HTTP over non-loopback addresses or disables TLS certificate verification."
  - "crates/riffdb-outbox adds direct network/HTTP client crate dependencies instead of keeping socket/TLS transport in riffdb-server."
  - "Outbox webhook delivery executes inside the authoritative command commit transaction or omits the stable 12-byte EventId deduplication header."
---
# ADR-0332: External OTLP Metrics Push and Outbox Webhook/Broker Push Delivery

## Context

Under `ADR-0306` and `WP-160`/`WP-185`, `riffdbd` exposed Prometheus and OTLP JSON metrics over a loopback scrape listener (`GET /metrics`, `GET /v1/metrics`) and a local NDJSON flight recorder, while the outbox worker (`crates/riffdb-outbox` and `crates/riffdb-server/src/outbox_worker.rs`) supported only `kind = "durable_local"` filesystem archives in production configuration (with `ConnectorDestinationKind::HttpWebhook` wired only to `DisabledHttpWebhookConnector`). This left a deployment limitation documented in `docs/known-limitations.md`: metrics and outbox delivery could not push directly to external OpenTelemetry collectors or HTTP-accessible message brokers/webhook consumers.

## Decision

1. **OTLP/HTTP Metrics Push Exporter (`WP-888`):**
   - `ServerConfig` (`crates/riffdb-server/src/config.rs`) and `RunningTelemetryExporter` (`crates/riffdb-server/src/telemetry_exporter.rs`) gain optional OTLP/HTTP push configuration (`telemetry_otlp_endpoint` / `--telemetry-otlp-endpoint` / `RIFFDB_TELEMETRY_OTLP_ENDPOINT`, `telemetry_push_interval_ms` bounded to `100..=3_600_000` with default `10_000`, optional `telemetry_bearer_token_file: ProtectedFilePath`, and optional `telemetry_ca_certificate_path: ProtectedFilePath`).
   - Endpoint validation requires either a verified `https://` endpoint (validated via `SystemCaBundle` or explicit `telemetry_ca_certificate_path` using `tokio-rustls` + `rustls-webpki`, `NET-002` / `NET-003`) or a literal loopback `http://127.0.0.1:<port>/...` / `http://[::1]:<port>/...` collector agent endpoint. Non-loopback cleartext `http://` fails closed at configuration load time.
   - On each push interval and once during graceful shutdown drain, `RunningTelemetryExporter` encodes `observability.encode_otel_json_v1(timestamp_unix_nanos)` and sends a bounded `POST` (`Content-Type: application/json`) to the configured endpoint with a bounded timeout (`<= 10s`), preserving `ID-005` label safety. Push failures increment an internal telemetry export counter and log a scrubbed warning without halting authoritative database execution.

2. **Outbox HTTP/HTTPS Webhook and Broker Push Connector (`WP-889`):**
   - `crates/riffdb-outbox` remains free of network/TLS crate dependencies (`ADR-0180`); the concrete HTTP/HTTPS push transport (`VerifiedWebhookConnector`) lives in `crates/riffdb-server/src/outbox_worker.rs` and implements `riffdb_outbox::OutboxConnector` with `ConnectorDeclaration::new(ConnectorDestinationKind::HttpWebhook, ConnectorIdempotency::EventId)`.
   - `ConfiguredOutbox` in `crates/riffdb-server/src/config_outbox.rs` (`[maintenance.outbox]` and `[databases.<alias>.outbox]`) accepts `kind = "http_webhook"` alongside `kind = "durable_local"`, with fields:
     - `destination_id` (`OutboxDestinationIdV1`)
     - `endpoint` (verified `https://<host>:<port>/<path>` or loopback `http://127.0.0.1:<port>/<path>` / `http://[::1]:<port>/<path>` for local broker bridges)
     - `ca_certificate_path: Option<ProtectedFilePath>` (defaults to `SystemCaBundle` for `https://`)
     - `bearer_token_path: Option<ProtectedFilePath>` (mode `0600` secret file loaded via protected-file custody)
     - `timeout_seconds`, `max_attempts`, `retry_delays_seconds`, `poll_interval_ms`, and `batch_limit`.
   - For each claimed `DeliveryAttempt` (always outside the authoritative command transaction, `EFF-001` / `EFF-002`), `VerifiedWebhookConnector` sends an HTTP/1.1 `POST` within `attempt.timeout_seconds()` carrying:
     - `Content-Type: application/json`
     - `Idempotency-Key: <hex-encoded-12-byte-EventId>` (`EFF-003`)
     - `X-RiffDB-Event-Id`, `X-RiffDB-Event-Type-Id`, `X-RiffDB-Event-Hash`, `X-RiffDB-Delivery-Attempt`, and `X-RiffDB-Destination-Id`
     - Optional `Authorization: Bearer <token>`
     - A deterministic JSON envelope containing `event_id`, `destination_id`, `attempt`, `commit_sequence`, `event_type_id`, `event_version`, `event_hash`, and canonical base64 + decoded JSON fields of the committed event record.
   - Response classification:
     - HTTP `200..=299`: `ConnectorDisposition::Accepted` (marks outbox intent `Delivered`).
     - HTTP `408`, `429`, `500..=599`, or socket/TLS/timeout error: `ConnectorDisposition::Retryable(OutboxSafeErrorV1)` (schedules bounded backoff retry or dead-letters once `max_attempts` is exhausted).
     - All other HTTP `400..=499`: `ConnectorDisposition::PermanentFailure(OutboxSafeErrorV1)` (transitions immediately to `DeadLetter` with scrubbed status code).

3. **Handbook and Known Limitations Update (`WP-890`):**
   - Remove the two resolved bullets from `Platform and Deployment` in `docs/known-limitations.md` and document mTLS, `auto_issue` / `riffdb tls issue`, `[server.http_listener]`, OTLP push, and `kind = "http_webhook"` outbox configuration across `docs/configuration.md` and `docs/security.md`.

## Standing design tests

- **Interface safety:** Outbox webhook delivery never runs inside the commit transaction, always carries `Idempotency-Key` (`EventId`), refuses remote cleartext HTTP, and scrubs error messages via `OutboxSafeErrorV1` before persistence or logging.
- **Scale:** Webhook response bodies read by `VerifiedWebhookConnector` and `RunningTelemetryExporter` are capped at `64 KiB`, timeouts are bounded by `timeout_seconds` (`1..=300`), and `riffdb-outbox` adds zero network dependencies.

## Checks

- `cargo test -p riffdb-outbox -p riffdb-server`
- `./scripts/handbook check`
