---
adr: "0341"
title: Production Structured Logging And Bounded Operator Sinks
status: accepted
tier: surface
date: 2026-11-08
accepted: '2026-11-08'
acceptance: 'maintainer, in session, 2026-11-08: "i accept, build", accepting the exact ADR-0341 through ADR-0343 text reserved at 5e54542e4'
requires: [ADR-0049, ADR-0044, ADR-0266, ADR-0306, ADR-0180]
amends: [ADR-0049, ADR-0266]
supersedes: []
requirements: [ID-005, DX-003, END-007, END-008]
packages: [WP-914, WP-915]
obligations: []
review_triggers:
  - A filter, sink, dependency subscriber, or environment variable can expose unvalidated text or bypass redaction before subscribers.
  - A logging sink performs blocking I/O in command evaluation or blocks authoritative admission, execution, or commit on output delivery.
  - Provider accounting samples or telemetry loss become invisible to endurance qualification.
  - A new dependency changes the accepted TLS or cryptographic provider, or requires first-party unsafe code.
---
# ADR-0341: Production Structured Logging And Bounded Operator Sinks

## Context

Server operational messages use direct stderr writes alongside closed semantic tracing
hooks. Operators lack consistent event codes, JSON output, severity filtering, file
retention, and sink-loss visibility. Multi-database workers repeat process-wide
provider samples, obscuring configuration-reload failures. Cloud and standalone
installations need one documented operational log boundary without weakening
ADR-0049's redaction-before-subscriber rule or endurance evidence.

## Decision

1. Install one process-owned `tracing-subscriber` pipeline in `riffdbd` before
   database/service construction. Semantic owners keep their existing injected
   closed hooks; deterministic evaluation gains no I/O, clock, randomness, or
   process-global state. `riffdb-observability` retains its existing leaf edges.
2. Admit exact server-only dependencies `tracing = 0.1.44` (`std`),
   `tracing-subscriber = 0.3.23` (`std`, `registry`, `fmt`, `json`), and
   `tracing-appender = 0.2.5` (no optional features), all with defaults disabled.
   Reuse the workspace's exact serde/JSON and directory-custody dependencies.
   Record resolved features, transitive graph, licenses, advisories, and MSRV in
   implementation evidence; new crypto/native providers require human review.
3. Emit operational events through a closed typed event registry, with a stable
   event code, fixed message, severity, component, and bounded typed attributes.
   No raw error/source/debug string, value, credential, endpoint, filesystem path,
   hostname, or business identifier reaches a subscriber. Existing permitted
   correlation IDs/hashes follow SPEC 16.4 and never become metric labels.
   Preserve the MCP stdio hard filter and protocol-only stdout boundary.
4. Freeze `riffdb.log/v1` JSON Lines: schema, UTC timestamp, severity, event code,
   component, fixed message, build version, process instance ID, and a closed
   event-specific attribute object. Optional authorized database aliases and
   applicable request/incident/trace/span correlation have explicit presence
   rules. All records are valid single-line JSON without ANSI escapes, at most
   16 KiB. Human formatting derives from the same checked event. Panic output
   and pre-subscriber startup failure retain bounded redacted emergency handling.
5. Add non-reloadable `[server.logging]` settings with CLI and `RIFFDB_LOG_*`
   equivalents under normal flag/environment/file precedence: `format` is
   `text|json` (default text); `level` is `error|warn|info|debug|trace` (default
   info); `stderr` defaults true and cannot be disabled without another enabled
   log sink. At most 32 overrides select closed first-party
   components. Dependency targets remain excluded even at trace; ambient
   `RUST_LOG` cannot widen the hard filter. Unknown fields/values fail closed.
   Container and Helm configurations explicitly select JSON. No application
   principal can select sinks, filters, or resources.
6. Optionally enable one rotating file sink using an absolute protected
   `directory`, fixed server-owned basename, `max_file_bytes` in
   1 MiB..=256 MiB (default 32 MiB), and `max_files` in 2..=32 (default 8).
   Reject symlinks, unsafe ownership/modes, and overlap with protected database,
   backup, projection, secret, and outbox paths. Create directories/files with
   0700/0600 custody; rotation and retention operate only on owned log files
   through directory-anchored operations. Enforce byte and file-count bounds,
   including restart and failed rotation; never scan arbitrary directories.
7. Producers enqueue already-bounded records without blocking on sink I/O.
   Each enabled sink has at most 4,096 queued records including a 64-record
   high-severity reserve. Count every queue rejection/write failure by closed
   sink/severity/reason; expose depth and loss without recursive logging.
   All sinks remain best effort, including error logs: no durability or audit
   guarantee is inferred. A full disk or slow collector cannot change a command
   outcome or readiness decision. Existing semantic defect handling remains.
   Startup reports unusable configured sinks; runtime failures have rate-limited
   safe reporting through healthy sinks. Shutdown drains under one at-most-five
   second budget contained within the existing process shutdown deadline.
8. Migrate server lifecycle, readiness, reload, worker and exporter messages to
   typed events. Reload refusal names the safe reason, applicable alias/setting
   role, retained-active-state disposition, and restart guidance. Deduplicate
   unchanged reload refusals and report recovery. Provider pool accounting is
   sampled once per process at startup, once per 60 seconds, and at final stop;
   normal console visibility is debug. Preserve the fixed numeric sample and
   explicit enablement/collection required by endurance qualification, including
   missing/stale samples and unavailable accounting. Never sample away warnings,
   reload refusal, readiness transitions, or accounting failure.
9. External log destinations use stderr/file collectors or ADR-0342's optional
   OTLP log export. Publish tested systemd, container and Kubernetes collection
   recipes; do not embed vendor credentials or vendor-specific clients.

## Standing design tests

- **Interface safety:** operators configure observability only; application
  authority, TLS, typed outcomes, durability, idempotency and redaction cannot be
  disabled. Permissive sibling subscribers still cannot observe secret canaries.
- **Scale:** record size, component vocabulary, queues, file inventory, retention,
  rate limits, shutdown time and provider sampling are bounded independently of
  entity population. No logging task owns authoritative storage or audit writes.

## Checks

- Closed-schema snapshots, semantic severity/filter assertions, sibling-subscriber
  secret canaries, and process-level early-startup/shutdown/reload tests.
- Deterministic queue saturation, blocking-writer, disk-full, rotation, symlink,
  restart retention and repeated multi-database provider-sample tests.
- Existing endurance provider-budget parsers and sample-coverage checks.
- Package acceptance, generated-artifact and handbook checks; full merge CI.
