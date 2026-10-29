# Summary

- [RiffDB Handbook](README.md)

# Start Here

- [What RiffDB Is](getting-started/WHAT-IS-RIFFDB.md)
- [Why RiffDB Exists](VISION.md)
- [Installation](installation.md)
- [Driver and CLI Packages](getting-started/PACKAGE-INSTALLATION.md)
- [Your First Application](getting-started/FIRST-APPLICATION.md)
- [Database-Shaped Project Workflow](getting-started/DATABASE-WORKFLOW.md)
- [Agent Database Quickstart](getting-started/AGENT-DATABASE-QUICKSTART.md)
- [Agent Application Quickstart](getting-started/AGENT-APPLICATION-QUICKSTART.md)
- [Build a Symbolic Application](getting-started/SYMBOLIC-APPLICATIONS.md)
- [Inspect an Application](getting-started/INSPECTION.md)
- [Vector Search](getting-started/VECTOR-SEARCH.md)

# Tutorials

- [TicketDesk End to End](tutorials/TICKETDESK.md)
- [PostgreSQL Safety Comparison](tutorials/POSTGRESQL-COMPARISON.md)

# Core Concepts

- [Contract-First State](concepts/CONTRACT-FIRST.md)
- [Command Lifecycle](concepts/COMMAND-LIFECYCLE.md)
- [Consistency and Recovery](concepts/CONSISTENCY.md)
- [Safety by Construction](safety-by-construction.md)
- [Compiled Row Policies](security/ROW-POLICIES.md)
- [Secret Field Classification](security/SECRET-FIELDS.md)

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
- [Compiled Framework Profiles](integrations/COMPILED-FRAMEWORK-PROFILES.md)
- [Adapter Evidence Classes](integrations/ADAPTER-EVIDENCE.md)
- [Authoring Diagnostics](getting-started/AUTHORING-DIAGNOSTICS.md)
- [Contract and RiffQL Editor Tooling](getting-started/EDITOR-TOOLING.md)
- [Application Errors](getting-started/APPLICATION-ERRORS.md)
- [Resumable Command Batches](getting-started/COMMAND-BATCHES.md)
- [Generated Operational Queries](getting-started/OPERATIONAL-QUERIES.md)
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
- [Bounded Delete Commands](contracts/BOUNDED-COMMANDS.md)
- [Atomic Indexed Selection](contracts/ATOMIC-INDEXED-SELECTION.md)
- [Command and Invariant Cookbook](contracts/COMMAND-INVARIANT-COOKBOOK.md)
- [Unsafe Patterns and Corrections](contracts/examples/NEGATIVE-EXAMPLES.md)

# RiffQL

- [RiffQL Language](riffql/LANGUAGE.md)
- [Bounded Partition-Set Queries](riffql/PARTITION-SET-QUERIES.md)
- [Long-Value Pattern Search](riffql/LONG-PATTERN.md)
- [Tokenized Text Search](riffql/TOKENIZED-TEXT.md)
- [Exact Predicate and Order Families](riffql/EXACT-PREDICATES.md)
- [Planning and Bounds](riffql/PLANNING.md)
- [Immutable Query Modules](riffql/MODULES.md)
- [Query IR Reference](riffql/IR.md)

# Operate RiffDB

- [Configuration](configuration.md)
- [Durable Local Outbox Archive](operations/LOCAL-OUTBOX-ARCHIVE.md)
- [Helm Operations](operations/HELM-OPERATIONS.md)
- [Remote and Local Application Ingress](operations/REMOTE-INGRESS.md)
- [Follower Registration and Retirement](operations/FOLLOWER-ADMINISTRATION.md)
- [Multiple Databases](operations/MULTIPLE-DATABASES.md)
- [Startup Integrity and Clean Restarts](operations/STARTUP-INTEGRITY.md)
- [Backup and Restore](backup-restore.md)
- [Symbolic Application Export](operations/APPLICATION-EXPORT.md)
- [Application Installation Campaigns](operations/APPLICATION-INSTALLATION-CAMPAIGNS.md)
- [Upgrade, Reset, and Removal](upgrade-removal.md)
- [Troubleshooting](operations/TROUBLESHOOTING.md)
- [Security Posture](security.md)
- [Known Limitations](known-limitations.md)
- [Compatibility](compatibility.md)
- [Versioning and Retirement](versioning-and-retirement.md)

# Architecture

- [System Overview](architecture/OVERVIEW.md)
- [Command Execution Path](architecture/COMMAND-PATH.md)
- [Composite Read Views](architecture/COMPOSITE-READ-VIEWS.md)
- [Schema-Bound Columnar Control](architecture/COLUMNAR-CONTROL.md)

# Reference

- [CLI Reference](reference/CLI.md)
- [Named-result Carriage](reference/NAMED-RESULT-CARRIAGE.md)
- [Values and Identifiers](reference/VALUES.md)
- [Errors and Outcomes](reference/ERRORS.md)
- [Rust API](reference/RUST-API.md)
- [Release Verification](release.md)
