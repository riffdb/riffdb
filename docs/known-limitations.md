# Known Limitations

This page lists current engineering and operational limitations in the `0.1.x`
release series that may be expanded in future versions. Intentional
architectural boundaries—such as compiled commands instead of ad-hoc SQL,
partition-scoped transactions, and capability-based data authorization—are
covered in [What RiffDB Is](getting-started/WHAT-IS-RIFFDB.md),
[Safety by Construction](safety-by-construction.md), and the
[Command Invariant Cookbook](contracts/COMMAND-INVARIANT-COOKBOOK.md#patterns-intentionally-unavailable).

## Platform and Deployment

- Linux is currently the only supported server deployment platform for `riffdbd` and protected key-file loading.
- Native mTLS, automatic certificate issuance, and direct HTTP/JSON ingress are not yet built into `riffdbd` (application gRPC supports loopback, direct TLS, and Unix domain sockets).
- Metrics (`GET /metrics`) and outbox delivery (`[maintenance.outbox]`) currently write only to loopback HTTP or local filesystem archives rather than pushing directly to external telemetry collectors or message brokers.

## Replication, Backup, and Recovery

- Follower replication is asynchronous on a single-primary topology with explicit operator-fenced promotion, without consensus-based quorum commits or automatic failover; see [Follower Administration](operations/FOLLOWER-ADMINISTRATION.md).
- Full physical backup, restore publication, and `riffdb storage scrub` require taking the database process offline, whereas incremental archive backup runs online; see [Backup and Restore](backup-restore.md).
- Restarting after an abrupt process termination can require storage-engine repair proportional to database file size while snapshot locators rebuild in the background; see [Startup Integrity](operations/STARTUP-INTEGRITY.md).

## Contracts and Writes

- Multi-step durable workflows and sagas spanning multiple independent commands or partitions are not yet built into the runtime.
- Claiming an arbitrary free row from an unowned pool in a single round trip still serializes on the aggregate conflict key; see [Atomic Indexed Selection](contracts/ATOMIC-INDEXED-SELECTION.md).

## Queries, Search, and Projections

- A single query call returns at most one bounded page (up to its compiled row limit, capped at 65,534 rows or 4 MiB) rather than a streaming multi-page cursor.
- First-party HNSW vector search currently scales up to 65,536 rows per partition (`ann_threshold`), uses count-based staleness thresholds, and cannot be combined with multi-source joins or hybrid text search in one query.
- Tokenized text search (`riff_bm25_v1`) and pattern search (`long_pattern_v1`, up to 8,000 bytes) do not yet support regular expressions, fuzzy matching, language stemming, or highlighted snippets.

## Retention

- Demand-activated columnar projections and unconfigured outbox workers (`[maintenance.outbox]`) hold online history retention at their last durable frontier until queried, archived, or explicitly detached with `riffdb retention projection-detach`; see [Configuration](configuration.md#online-retention).
