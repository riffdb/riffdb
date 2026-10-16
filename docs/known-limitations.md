# Known Limitations

These limits are part of the POC release posture, not hidden roadmap promises.

## Deployment and Operations

- RiffDB is a pre-release proof of concept, and its dual license does not change its security, compatibility, or support limits.
- Linux is the supported deployment platform; protected credential and key-file loading is unavailable on other operating systems.
- The source convenience installer targets Linux systemd, requires a valid private `XDG_RUNTIME_DIR` for user-scope unit verification, and does not configure login sessions or lingering.
- Automated source-install acceptance exercises user scope only, while system-scope installation requires first-install acceptance on a disposable or staging host.
- Application gRPC supports literal-loopback cleartext, verified direct TLS with static certificate and key paths, and a protected Unix socket, while hosted MCP remains loopback-only and OAuth, mTLS, automatic certificate issuance, and untrusted internet-facing operation are unsupported.
- Browser code must use an application-owned backend and never receive a RiffDB credential.
- `riffdbd` exposes only the documented POC configuration fields, with a loopback-only metrics listener and no storage-engine selector, general tuning surface, or secret values in TOML.
- After generic bootstrap, systemd `active` means the process started while authenticated Health remains `not_ready` until the first application contract is deployed.
- The production server uses redb only, and the isolated Fjall comparison workspace is never linked into or shipped as a server binary.
- Benchmark reports are revision-specific evidence rather than a standing performance promise, with no absolute TPS gate and no valid claim without published hardware, filesystem, durability mode, workload, revision, and raw output.
- The sustainable TicketDesk rehearsal remains blocked by continuing-load source-receipt reclamation and final-window tail latency; see the [live observations](architecture/WP-825-LIVE-OBSERVATIONS.md).
- Ordinary redb reads validate RiffDB envelopes, CRCs, schemas, and keys without verifying every engine page checksum, whereas offline physical verification requires extra disk space for a private copy and refuses repair or unrecovered journal suffixes; see [startup integrity](operations/STARTUP-INTEGRITY.md).
- The shared crash-recovery matrix covers the enumerated POC recovery boundaries rather than providing a general disaster-recovery guarantee.
- The POC authorization model uses opaque local capability tokens rather than a production identity provider or OAuth authorization server.
- Generic bootstrap deploys no application and grants its MCP developer no wildcard application-data authority until an administrator binds a compiled role or lineage-scoped capability.
- Convenience bootstrap capabilities expire after at most 30 days with no automatic renewal.
- Re-running the source installer updates the three binaries while preserving existing keys, configuration, and unit files, and does not perform configuration migration, key rotation, uninstall, or format upgrades.
- Individual binary replacement is atomic, but replacing all three binaries is not transactional across an interrupted install.
- Optional Codex MCP registration provides no atomic create-if-absent operation and must not run concurrently with another writer to the same user's Codex MCP configuration.
- Metrics are exposed only over an optional literal-loopback HTTP listener (`GET /metrics` and `GET /v1/metrics`) or periodic local NDJSON flight-recorder files (`riffdb telemetry summarize <PATH>`), with no non-loopback network metrics listener or push exporter.
- Neither a clean restart nor a bounded dirty restart performs a full population scrub, and `riffdb storage scrub` takes the database offline for the entire pass with no partial scope, resume, or repair option.
- Restart after an abrupt stop pays a storage-engine repair proportional to the database file size, and the first write after any restart rebuilds the in-memory command indexes once before admitting writes.
- Outbox delivery supports only the explicitly configured POC connector behavior.
- The optional systemd MCP socket shares one capability among members of its Unix socket group, so operators must spawn one bridge per client when principal identities must differ.
- Release verification validates systemd unit syntax and hardening directives without starting the units inside the host systemd manager.
- A POC-exit decision requires all POC-001 through POC-010 evidence together with a human architecture review.

## Contracts and Writes

- There is no general SQL surface, arbitrary transaction callback, analytical join engine, distributed transaction, automatic failover, or consensus protocol.
- An ordinary command may atomically delete and return the transaction-current preimage of at most one complete-key, partition-local `no_inbound` entity, whereas multi-row deletes must use the separately compiler-bounded collection deletion forms.
- Reactive applications are partition-local and bounded, with no raw CDC, cross-partition ordering, wall-clock event retention, exactly-once external effects, event-sourced reconstruction, webhooks, or in-process agent inference.
- Protected event replay and reaction validation acquire the storage mutation fence to obtain a transaction-current capability, row, and relationship view, and protected workers use bounded consumer long-poll rather than the global reactive wakeup.
- Contract migration administration is available through the operator service, Rust client, and CLI, and is intentionally absent from MCP, TypeScript, Python, and generated application clients; Gate C rejects key swaps and predecessor-occupied rekey chains.
- The PostgreSQL counterexamples in the repository demonstrate only that unsafe write patterns remain expressible in a general SQL surface and are excluded from RiffDB's compiled command surface.

## Queries and Projections

- Runtime-selectable page sizes use the query-only `Limit<MAX>` refinement up to 65,534 rows and do not refine offsets, byte budgets, contract integers, aggregate work, or projection candidate budgets.
- A single RiffDB query invocation returns at most its compiled row limit (and never more than 65,534 rows or 4 MiB of encoded results), with no hidden multi-page response or streaming query protocol.
- Named bounded RiffQL aggregates support exact counts, sum, mean, min, max, Boolean any/all, and bounded `group by` (automatically bounded on `enum`/`bool` keys or via `group by ... take <N>`), while the separate ad-hoc projected CLI supports only `count`, integer-only `sum`, `min`, and `max` with bounded `group_by` in encoded-key byte order.
- Candidate algebra supports one compiler-sealed non-output binding across at most eight same-partition declared-index sources, one-level bounded one-to-many expansion, and at most 32 compiler-expanded root orders, without depth-two expansion, cross-partition joins, or caller-supplied sort expressions.
- `long_pattern_v1` provides exact binary or Unicode-fold equality, literal affix/substring, and LIKE/ILIKE matching for values up to 8,000 bytes as a rebuildable candidate provider, without general regular expressions or direct unanchored negation.
- Ordinary row-store indexes execute exact equality, one bounded membership or range/prefix branch, and the complete declared order, while canonical string ranges, multiple branching dimensions, residual post-page filters, and runtime join optimization remain compile-time refusals.
- The external tuple-capability fixture is a narrow source-compilation receipt for bytewise equality, membership, and order rather than a full external framework-conformance claim.
- Named vector search selects exact scan at or below the declared per-organization `ann_threshold` (up to 500 rows when undeclared or 65,536 rows when declared) and first-party in-memory HNSW on the primary above that threshold when a warm graph is available, refusing larger organizations with `ScanBudgetExceeded`.
- Vector staleness uses v1 count semantics (`stale_count > threshold`) rather than duration-based thresholds.
- Production `nearest()` requires one explicit compiler-owned projected source with `available`, causal, or duration-bounded freshness and refuses multi-source vector queries, vector joins, or row-store fallbacks.
- Columnar artifacts are demand-activated on their first semantic query, returning `Building` while activation is running and remaining unavailable for the process generation if activation fails.
- Projection state is derived and rebuildable, so projections may report degraded or building status while authoritative commits remain available.
- Tokenized-text search supports conjunction, capped disjunction, phrase, and bounded ordered proximity with fixed `riff_bm25_v1` ranking across eight retained provider epochs, without wildcards, regular expressions, fuzzy matching, stemming, snippets, caller boosts, or hybrid vector/text queries.

## Replication, Backup, and Restore

- Asynchronous follower replication, follower reads, operator registration and retirement, and explicit incarnation-fenced primary promotion operate on a single-primary topology without quorum durability, automatic failover, cascading followers, or multi-primary writes; see [follower administration](operations/FOLLOWER-ADMINISTRATION.md).
- Restore publication requires the database directory to be private to the sole configured service identity, and shared write access or concurrent server processes are unsupported.
- Full backup and restore publication are offline maintenance operations, while changelog-derived archive backup and restore operate at commit-sequence granularity without wall-clock point-in-time recovery or built-in archive encryption; see [backup and restore](backup-restore.md).
- Destructive restore preserves `DatabaseId` while rewinding sequence allocators and bumping the durable `history_incarnation` fence (`RDB-HISTORY-0101`), leaving clients that omit `observed_history_incarnation` unvalidated.
- Locators, cursors, sessions, read-after-sequence expectations, authorization observations, and idempotency assumptions from a destroyed history suffix are invalid after a restore.

## Retention

Online retention (see [Configuration](configuration.md#online-retention)) never
passes a fence, and in this POC three fences bind more than an operator may
expect:

- Because this POC composes no production outbox delivery destination, an emitted event is never marked delivered automatically and holds retention from the first undelivered event onward.
- A declared columnar projection that has not been queried in the current process holds retention at its last populated frontier (or at the beginning of history if never populated) until a query activates it.
- The first online retention step invalidates the validated-prefix startup checkpoint so the next restart performs complete startup validation proportional to the live population; see [Startup integrity](operations/STARTUP-INTEGRITY.md).
- Online logical pruning is paced to about five percent of writer time in journaled steps of at most 256 sequences and can fall behind under sustained write saturation; see the [retention design review](architecture/WP-815-DESIGN-REVIEW.md).

## Storage reclamation

- Sealed journal extents (`<database>.riffjournal.sealed.<generation>`) retain command and receipt history referenced by compact stubs in the database file and must never be deleted by hand.
- A sealed journal extent is retired only after logical pruning and source-history reclamation release every command and receipt reference with no active reader or scrub holding it.

## What a command cannot express

The compiler proves bounded structural properties of application writes; see the
[write cookbook](contracts/COMMAND-INVARIANT-COOKBOOK.md#declare-the-evidence-for-each-write).
The command language has the following structural limits:

- Commands read entities by complete key or through one [atomic indexed first-row selection](contracts/ATOMIC-INDEXED-SELECTION.md) whose equality prefix proves the partition and every conflict key before the read.
- An invariant may span entities inside one aggregate, and a command may mutate multiple aggregates that share a partition route, but no invariant may relate two distinct aggregates.
- A command cannot read a derived projection because write decisions must never depend on an asynchronous projection watermark.
- Allocating an unspecified free row from an unowned pool in a single round trip without root contention is not expressible inside one command.
- General multi-step durable workflows and sagas outside a single compiled command are not implemented in this POC.
