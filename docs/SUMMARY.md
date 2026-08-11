# Summary

- [RiffDB Handbook](README.md)

# Start Here

- [What RiffDB Is](getting-started/WHAT-IS-RIFFDB.md)
- [Why RiffDB Exists](VISION.md)
- [Installation](installation.md)
- [Your First Application](getting-started/FIRST-APPLICATION.md)
- [Agent Application Quickstart](getting-started/AGENT-APPLICATION-QUICKSTART.md)
- [Build a Symbolic Application](getting-started/SYMBOLIC-APPLICATIONS.md)
- [Inspect an Application](getting-started/INSPECTION.md)

# Tutorials

- [TicketDesk End to End](tutorials/TICKETDESK.md)
- [PostgreSQL Safety Comparison](tutorials/POSTGRESQL-COMPARISON.md)
- [TicketDesk Acceptance Evidence](getting-started/TICKETDESK-ACCEPTANCE.md)

# Core Concepts

- [Contract-First State](concepts/CONTRACT-FIRST.md)
- [Command Lifecycle](concepts/COMMAND-LIFECYCLE.md)
- [Consistency and Recovery](concepts/CONSISTENCY.md)
- [Safety by Construction](safety-by-construction.md)
- [Compiled Row Policies](security/ROW-POLICIES.md)

# Build Applications

- [Application Source and Exact Lock](getting-started/APPLICATION-MANIFEST.md)
- [Contract Migrations](contracts/MIGRATIONS.md)
- [Workflow Transitions and Fenced Leases](contracts/WORKFLOWS.md)
- [Bounded Workflow Schedulers](contracts/SCHEDULERS.md)
- [Domain Events](contracts/DOMAIN-EVENTS.md)
- [Reactive Modules](reactive/MODULES.md)
- [Reactive Application Clients](reactive/CLIENTS.md)
- [Live Named Queries](reactive/LIVE-QUERIES.md)
- [Contextual Agent Subscriptions](reactive/CONTEXTUAL-SUBSCRIPTIONS.md)
- [Safe Application Profiles](getting-started/SAFE-APPLICATION-PROFILES.md)
- [Authoring Diagnostics](getting-started/AUTHORING-DIAGNOSTICS.md)
- [Application Errors](getting-started/APPLICATION-ERRORS.md)
- [Resumable Command Batches](getting-started/COMMAND-BATCHES.md)
- [Rust Applications](sdks/RUST.md)
- [Driver Host](sdks/DRIVER-HOST.md)
- [Go Applications](sdks/GO.md)
- [TypeScript Applications](getting-started/TYPESCRIPT-APPLICATIONS.md)
- [Python Applications](python-driver.md)
- [MCP for Agents](mcp/agent-cookbook.md)

# Contract Language

- [Contract Language Overview](contracts/README.md)
- [Authoring Reference](contracts/AUTHORING.md)
- [Language Reference](contracts/LANGUAGE.md)
- [Bounded Collection Commands](contracts/BOUNDED-COMMANDS.md)
- [Command and Invariant Cookbook](contracts/COMMAND-INVARIANT-COOKBOOK.md)
- [Unsafe Patterns and Corrections](contracts/examples/NEGATIVE-EXAMPLES.md)

# RiffQL

- [RiffQL Language](riffql/LANGUAGE.md)
- [Planning and Bounds](riffql/PLANNING.md)
- [Immutable Query Modules](riffql/MODULES.md)
- [Query IR Reference](riffql/IR.md)

# Operate RiffDB

- [Configuration](configuration.md)
- [Remote and Local Application Ingress](operations/REMOTE-INGRESS.md)
- [Multiple Databases](operations/MULTIPLE-DATABASES.md)
- [Backup and Restore](backup-restore.md)
- [Contract Migration Acceptance](operations/CONTRACT-MIGRATION-ACCEPTANCE.md)
- [Application Installation Campaigns](operations/APPLICATION-INSTALLATION-CAMPAIGNS.md)
- [TicketDesk Reactive Acceptance](operations/TICKETDESK-REACTIVE-ACCEPTANCE.md)
- [Upgrade, Reset, and Removal](upgrade-removal.md)
- [Troubleshooting](operations/TROUBLESHOOTING.md)
- [Security Posture](security.md)
- [Known Limitations](known-limitations.md)
- [Compatibility](compatibility.md)

# Architecture

- [System Overview](architecture/OVERVIEW.md)
- [Command Execution Path](architecture/COMMAND-PATH.md)
- [Composite Read Views](architecture/COMPOSITE-READ-VIEWS.md)
- [Deployable Application Alpha Plan](architecture/DEPLOYABLE-APPLICATION-ALPHA.md)
  - [Alpha Architecture Freeze](architecture/DEPLOYABLE-APPLICATION-ALPHA-FREEZE.md)
- [Vectors Pillar Recon Map](architecture/vectors-recon-map.md)
- [Vector Projection Obligations](architecture/vectors-projection-obligations.md)

# Performance

- [Benchmark Integrity](performance/benchmark-integrity.md)
- [App-baseline Alpha Evidence](performance/app-baseline-alpha.md)
- [WP-449 Writer Evidence](performance/wp-449-writer-evidence.md)
- [WP-451 Commutative Child Appends](performance/wp-451-commutative-child-appends.md)
- [WP-452 Write Amplification](performance/wp-452-write-amplification.md)
- [WP-457 Writer Feeding](performance/wp-457-writer-feeding.md)
- [WP-458 Shared Command Intent](performance/wp-458-shared-command-intent.md)
- [WP-459 Consumed Staged Graphs](performance/wp-459-consumed-staged-graphs.md)
- [WP-460 Allocation-free Record Validation](performance/wp-460-allocation-free-validation.md)
- [WP-461 Single-owner Staged Event Collection](performance/wp-461-single-owner-events.md)
- [WP-462 Allocation-free Wire Reservation](performance/wp-462-allocation-free-wire-reservation.md)
- [WP-463 Allocation-free Read Dependencies](performance/wp-463-allocation-free-read-dependencies.md)
- [WP-464 Bounded Batch Ingress](performance/wp-464-bounded-batch-ingress.md)
- [WP-466 Bounded Durability Epochs](performance/wp-466-durability-epochs.md)
- [WP-467 Last-Durable Frontier](performance/wp-467-last-durable-frontier.md)
- [WP-468 Revisited Writer Feeding](performance/wp-468-revisited-writer-feeding.md)
- [WP-469 Fenced Single-Subgroup Commit](performance/wp-469-fenced-single-subgroup.md)
- [WP-470 Batched Command-Audit Table Access](performance/wp-470-batched-command-audit-tables.md)
- [WP-471 Revision-Aware Commit Materialization](performance/wp-471-revision-aware-commit.md)
- [WP-472 Single-Pass Commit Materialization](performance/wp-472-single-pass-commit.md)
- [WP-473 Sealed Command-Audit Link Evidence](performance/wp-473-sealed-command-audit-links.md)
- [WP-474 Sealed Entity Post-Image Materialization](performance/wp-474-sealed-entity-postimage.md)
- [WP-475 Retained Canonical Permission Lookup](performance/wp-475-retained-permission-keys.md)
- [WP-476 Constant-Time Durable Shape Dispatch](performance/wp-476-durable-shape-dispatch.md)
- [WP-478 Pipelined Durability Journal](performance/wp-478-pipelined-durability-journal.md)
- [WP-479 Segmented Command Authority](performance/wp-479-segmented-command-authority.md)
- [WP-480 Preallocated Recyclable Journal](performance/wp-480-journal-mechanics.md)
- [WP-481 Constant-time Query Frontiers](performance/wp-481-constant-time-query-frontiers.md)
- [WP-482 Streaming Command Segments](performance/wp-482-streaming-command-segments.md)
- [WP-483 Streaming Command Capsules](performance/wp-483-streaming-command-capsules.md)
- [WP-484 Proven Command-segment Framing](performance/wp-484-proven-command-segment-framing.md)
- [WP-485 State-bearing Segment Mechanics](performance/wp-485-state-bearing-segment-mechanics.md)
- [WP-486 Journal-authoritative State Overlay](performance/wp-486-journal-state-overlay.md)
- [WP-413 Migration Evolution Evidence](performance/wp-413-migration-evolution.md)

# Reference

- [CLI Reference](reference/CLI.md)
- [Values and Identifiers](reference/VALUES.md)
- [Errors and Outcomes](reference/ERRORS.md)
- [Rust API](reference/RUST-API.md)
- [Release Verification](release.md)
