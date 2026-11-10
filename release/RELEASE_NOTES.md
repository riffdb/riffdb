# RiffDB v0.2.2

## Developer Experience, Driver Reconnection, Python 3.11+ Wheels, and RiffQL Primary-Key Set Plans (`0.2.2`)

`v0.2.2` is a patch release in the `0.2.x` series (durable epoch 1, writer 5)
that improves developer onboarding, transport resilience, Python compatibility,
and RiffQL query planning and set chunking:

- **Primary-key membership without requiring a secondary index:** RiffQL `many`
  queries whose complete primary key is constrained (`==` on partition/prefix
  key fields and `in $ids` on a primary-key field with ascending `order by`)
  now compile to bounded `QueryAccessKind::Point` plans within a single snapshot
  when no declared secondary index matches, while continuing to prefer declared
  covering indexes when present.
- **Explicit `Set<T, MAX>` bounds (`1..=65,535`) and `<query>All` chunking
  guarantees:** Generated `<query>All` / `<query>_all` / `<Query>All` SDK
  helpers across TypeScript, Python, and Go enforce upfront total item bounds
  (`items.length <= maxPages * MAX_ITEMS`) before issuing any RPC and
  canonically deduplicate and sort input sets in the query's `order by`
  direction (`asc`/`desc` and canonical vs `binary_utf8_v1` string collation).
- **Minimal-identity `riffdb-driverd` handshakes, automatic reconnect, and
  unified SDK ergonomics:** `riffdb-driverd` resolves single-application and
  single-role defaults from its configuration when optional identity hashes are
  omitted (`RIFFDB_DRIVER_SOCKET` + `RIFFDB_CREDENTIAL_FILE`); TypeScript and Go
  driver runtimes automatically reconnect on socket EOF/reset after a
  `riffdb-driverd` restart; Python adds `connect_from_env()`; and TypeScript
  generated clients accept both transport classes and factories while unifying
  `DriverApplicationTransport` and `DriverGeneratedApplicationTransport`.
- **CPython 3.11+ (`cp311-abi3`) wheels:** Prebuilt Python wheels now target the
  stable `cp311-abi3` ABI (`requires-python = ">=3.11"`), supporting CPython
  3.11, 3.12, 3.13, and 3.14 across `x86_64` and `aarch64` Linux (`manylinux`)
  and `arm64` macOS.
- **Contract bulk loop field access & operator CLI defaults:** Contract
  `for item in input.items` loops support direct field projection (`item.field`)
  inside command expressions; `riffdb tls issue` adds `--san-dns` and
  `--san-ip`; `riffdb capability bootstrap --operator` automatically includes
  configured server gRPC/MCP audiences; `riffdb application deploy` defaults
  role credential lifetime to 30 days; `durable_local` outbox defaults its
  directory to `<data_dir>/outbox` with `RDB-LOG-0305` diagnostics; and
  multi-database `riffdbd` servers wait for every configured database to
  complete recovery before serving readiness.

In addition, `v0.2.2` retains all `v0.2.1` operational hardening, announcement
readiness, and performance capabilities:

- **Pre-decode transport admission and authentication:** Both HTTP (`/mcp`,
  health/metrics) and gRPC (including 8 MiB command and 32 MiB administration
  migration services) enforce TLS handshake concurrency limits, authenticate
  bearer credentials, and bound concurrent inbound work before allocating or
  decoding request bodies.
- **Storage-verified saga step delegation and backup integrity:** Multi-step
  saga execution verifies retained step authority against authoritative storage
  records (`SagaStepAuthority`) and re-validates policy on every step; backup
  artifact validation always computes a full SHA-256 content digest rather than
  consulting a sampled-edge cache.
- **Bounded operator sockets, webhook deadlines, and protected CLI writes:**
  `OperatorDriverSocket` serves connections in bounded concurrent tasks that
  propagate shutdown; outbox HTTP/HTTPS webhook delivery enforces a single
  overall deadline across DNS resolution, TCP connect, TLS handshake, request
  write, and response reading; and CLI capability inventory persistence refuses
  symlinks and writes through mode-`0600` temporary files.
- **Watch, search, and columnar performance:** Idle watch stream reuse across
  Rust, Python, and TypeScript clients; incremental tokenized-text index
  checkpoint byte accounting; zero-copy ANN cache lookups; $O(N + k \log k)$
  exact nearest-neighbor selection; $O(N \log N)$ live-query key validation;
  chunked TypeScript frame buffering; and bounded columnar partition merges.

External ecosystem qualification covers one server/four databases with Better Auth,
OpenFGA, MLflow, and the synthetic todo application, including retained-state
restart and representative failure injection. Adapter candidate profile
migrations are explicit. The main repository runs product/SDK conformance and
contains no direct adapter-package imports or framework rehearsal dependency.

This is the `v0.2.2` release of RiffDB. It
provides contract-first command execution, shared public transports, durable
outcomes, provenance, outbox intent, projection frontiers, native MCP, offline
maintenance, replicated availability, and crash recovery backed by the passing
18-phase release gate receipt at
`release/evidence/deployable-application-alpha-v1.json`.

## Product Binaries

- `riffdbd`
- `riffdb`
- `riffdb-driverd`
- `riffdb-mcp`

Comparison, fixture, conformance, and safety runners are not shipped products.

## Compatibility

Public protocol, durable records, storage, contract IR, generated schema, MCP,
CLI output, backup manifest, and maintenance receipt formats are versioned.
This artifact carries `release/durable-format-manifest-v1.json` plus the exact
compatibility fixture inventory and release-pair table used to generate it.
The current identity is durable epoch 1, writer 5 (`0.2.2`). Downgrade is unsupported. See
`docs/compatibility.md`.

The manifest distinguishes the redb layout and each maintenance, migration,
and format-upgrade receipt family rather than treating coincident numeric
versions as interchangeable. Every supported edge names both source and target
release labels.

Daemon startup now compares the retained format marker before redb open and
prints `RDB-FORMAT-0101` with the sole safe action on mismatch. Operators can
run the same source-free check with `riffdb storage preflight`. The declared
same-epoch writer transitions to writer 5 are available only through the offline,
verified-backup-bound, restartable `riffdb storage upgrade` command. New
physical backups carry the exact format marker and compatible restore range;
legacy range-less backups require their source release.

Capability persistence preserves the original migration-only V2 schema and
uses V3+ for installation and operational authority. The reader contains one narrow recovery
case for the historical build that emitted the additive installation
field under V2's compact identity; new writes never reproduce that collision.

## Offline Contract Migration

Offline migration supports exact direct-parent migration through additive, structural,
and key/ownership gates. The operator workflow is available only through the
shared gRPC administration service, Rust SDK, and CLI with dedicated
`MigrateContract` authority. It retains an immutable pre-migration backup,
publishes a fully validated same-filesystem stage atomically, fences predecessor
writes, and recovers or rolls back before readiness. MCP and application
drivers intentionally expose no migration operation.

## Reactive Applications

RiffDB supports compiler-proved partitioned domain-event streams, bounded durable
consumers, live named queries, and contextual agent subscriptions. The
TicketDesk Application Source V4 example provides queue and detail watches
through an application-owned authenticated SSE relay and crash-safe generated
reaction helpers across Rust, TypeScript, Python, and MCP. Reactive delivery is
at least once and partition ordered; it is not raw CDC or exactly-once external
effect delivery.

## Replicated Availability and Archive Backup

`riffdbd --mode follower` provides authoritative-changelog shipping
(`riffdb.changelog-frame/v3`), gap-free resume, follower reads under the
existing `Causal`, `Bounded`, and `Available` freshness vocabulary, audited
follower registration and retirement with sequence hold budgets, quorum commit
durability and automatic failover (`ADR-0337`), explicit incarnation-fenced
primary promotion (`riffdb follower fence-primary` and `riffdb follower promote`),
and changelog-derived incremental archive backup and sequence-targeted restore
(`riffdb storage restore-archive`). Cascading followers and multi-primary writes
remain outside the 0.2.x scope.

## Operational Search and Multi-Step Sagas

RiffQL named queries support typo-tolerant fuzzy BM25 (`bm25_fuzzy`), bounded
highlighted snippet extraction (`snippet`), English Porter stemming, and
reciprocal rank fusion (`hybrid_rrf`) across generated Rust, Go, TypeScript,
Python, and MCP surfaces (`ADR-0338`). Multi-step durable saga coordination
persists atomically through authoritative tables and V3 changelog records so
online backups and replicated followers retain complete saga state.

## License

RiffDB is offered under either the MIT License or the Apache License, Version
2.0, at your option. The release bundle includes `COPYRIGHT`, `LICENSE-MIT`,
and `LICENSE-APACHE`.

Copyright © 2026 Kevin O'Shea and O'Shea & Sons, LLC.

## Destructive Restore Warning

Restore preserves `DatabaseId`, advances the durable `history_incarnation`, and
may rewind both authoritative sequence spaces to the backup frontier. Clients
that send their observed incarnation receive a typed mismatch instead of
silently binding a destroyed-suffix observation to new history. Clients that
do not participate remain unvalidated; discard every post-frontier locator,
cursor, session, sequence expectation, authorization observation, and
idempotency assumption after destructive restore.

## Evidence Qualification

- The product server uses redb. The isolated Fjall comparison fails the
  unchanged semantic conformance profile, so its report contains no eligible
  performance samples and this release makes no redb-versus-Fjall speed claim.
- Published redb, canonical PostgreSQL-versus-public-RiffDB, and semantic
  workload results are revision-specific. They carry their methodology, raw
  samples, storage medium, durability mode, and checked report hashes.
- Deliberately unsafe PostgreSQL counterexamples are correctness evidence only
  and are excluded from every performance result.
- Crash verification inventories 38 crash boundaries, executes the 18 required
  process-matrix cases, consumes named owner-package evidence for the
  remainder, and records zero production synchronization gaps.
- Replication verification records the receipted six-criterion replication and
  archive-restore acceptance campaign under
  `release/evidence/replication/replication-acceptance-v1.json` and the
  `PERF-008` emitter stability disclosure under
  `release/evidence/replication/perf-008-emitter-disclosure.md`.
- The bundle includes an installed-binary demo. It is distinct from the full
  source-checkout acceptance run and uses no Cargo or test harness.
- Release sign-off remains unsigned until the acceptance commands pass at the
  release revision and a maintainer records the architecture-review decision.

See `docs/architecture/DESIGN-DECISIONS.md` and `release/evidence/`.
