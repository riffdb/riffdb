# Summary

- [RiffDB Handbook](README.md)

# Start Here

- [What RiffDB Is](getting-started/WHAT-IS-RIFFDB.md)
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

# Build Applications

- [Application Source and Exact Lock](getting-started/APPLICATION-MANIFEST.md)
- [Contract Migrations](contracts/MIGRATIONS.md)
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
- [TypeScript Applications](getting-started/TYPESCRIPT-APPLICATIONS.md)
- [Python Applications](python-driver.md)
- [MCP for Agents](mcp/agent-cookbook.md)

# Contract Language

- [Contract Language Overview](contracts/README.md)
- [Authoring Reference](contracts/AUTHORING.md)
- [Language Reference](contracts/LANGUAGE.md)
- [Command and Invariant Cookbook](contracts/COMMAND-INVARIANT-COOKBOOK.md)
- [Unsafe Patterns and Corrections](contracts/examples/NEGATIVE-EXAMPLES.md)

# RiffQL

- [RiffQL Language](riffql/LANGUAGE.md)
- [Planning and Bounds](riffql/PLANNING.md)
- [Immutable Query Modules](riffql/MODULES.md)
- [Query IR Reference](riffql/IR.md)

# Operate RiffDB

- [Configuration](configuration.md)
- [Multiple Databases](operations/MULTIPLE-DATABASES.md)
- [Backup and Restore](backup-restore.md)
- [Contract Migration Acceptance](operations/CONTRACT-MIGRATION-ACCEPTANCE.md)
- [TicketDesk Reactive Acceptance](operations/TICKETDESK-REACTIVE-ACCEPTANCE.md)
- [Upgrade, Reset, and Removal](upgrade-removal.md)
- [Troubleshooting](operations/TROUBLESHOOTING.md)
- [Security Posture](security.md)
- [Known Limitations](known-limitations.md)
- [Compatibility](compatibility.md)

# Architecture

- [System Overview](architecture/OVERVIEW.md)
- [Command Execution Path](architecture/COMMAND-PATH.md)

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
- [WP-413 Migration Evolution Evidence](performance/wp-413-migration-evolution.md)

# Reference

- [CLI Reference](reference/CLI.md)
- [Values and Identifiers](reference/VALUES.md)
- [Errors and Outcomes](reference/ERRORS.md)
- [Rust API](reference/RUST-API.md)
- [Release Verification](release.md)
