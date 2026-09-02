# Known Limitations

These limits are part of the POC release posture, not hidden roadmap promises.

- RiffDB is not production ready. Its dual license does not change the POC
  security, compatibility, or support limits.
- Linux is the POC deployment gate. Protected credential and key-file loading
  is unavailable on other platforms.
- The source convenience installer targets Linux systemd. Every user-scope
  install requires a valid private `XDG_RUNTIME_DIR` for unit verification.
  Starting or restarting additionally requires an available per-user manager;
  `--no-start` avoids that live-manager requirement. The installer does not
  configure login sessions or lingering.
- Automated source-install acceptance exercises user scope; separate release
  checks validate the shared systemd assets. Automation does not execute the
  source installer's real account, `/etc`, or `/usr/local` flow. System scope
  requires first-install acceptance on its disposable or staging host.
- Application gRPC supports literal-loopback cleartext, verified direct TLS,
  and a protected Unix socket. Hosted MCP remains loopback-only. Direct TLS has
  static certificate/key paths and intentionally exposes no mTLS, native-root,
  trust-all, cipher-suite, protocol-version, or provider knobs. OAuth,
  automatic certificate issuance, and untrusted internet-facing operation are
  unsupported.
- The Linux-only `riffdb-driverd` application host and its protected Unix
  protocol are available. Generated Go and long-lived server-side TypeScript
  bindings for that protocol are separate alpha work; browser code must still
  use an application-owned backend and never receive a RiffDB credential.
- `riffdbd` exposes only the documented POC configuration fields. There is no
  network metrics listener, storage-engine selector, general tuning surface,
  or secret value in TOML.
- systemd `active` means the process started. After generic bootstrap,
  authenticated Health is intentionally `not_ready` with no active contract;
  require `ready` or `degraded` only after deploying the first application
  contract.
- The production server uses redb only. The isolated Fjall adapter fails the
  unchanged storage semantic conformance profile, so Fjall performance samples
  are ineligible and the POC makes no redb-versus-Fjall speed claim.
- Benchmark reports are revision-specific evidence, not a standing performance
  promise. Release verification rejects absent, mismatched, or
  correctness-unqualified redb, budget-comparison, and semantic-workload
  reports.
- The checked WP-190 inventory contains 38 boundaries. Eighteen require
  process-matrix execution; the remainder are satisfied by named owner-package
  evidence. The synchronized report records no production gap, but this is
  crash evidence for the POC matrix rather than a general disaster-recovery
  guarantee.
- Named bounded RiffQL aggregates support exact row/present/distinct counts,
  exact sum and mean state, min/max, and Boolean any/all. The separate
  projected ad-hoc CLI remains deliberately narrow: `count`, `sum`, `min`,
  and `max` with bounded `group_by` are available; `sum` accepts integer
  columns only and rejects decimal or money columns with a typed
  `type_mismatch`. One ungrouped request computes exactly one function — ask
  for several by grouping. Scan and grouping budgets are server-side with no
  request vocabulary and no explain surface; a grouped query returns at most
  the row limit it declared, and an exceeded budget is a typed rejection rather
  than a partial result. Groups arrive in the server's deterministic
  encoded-key byte order, which is not collation order; sort client-side for
  presentation.
- There is no general SQL surface, arbitrary transaction callback, analytical
  join engine, distributed transaction, replication, failover, or consensus.
- Compiler-sealed command decisions support bounded branch-local entity field
  assignments, embeddings, creates, and durable events. Workflow transition
  and lease instructions retain their existing dedicated command forms and are
  rejected inside a decision arm; no-effect never runs either form. Decisions
  are not general branching, per-item partial outcomes, callbacks, or dynamic
  transaction programs.
- Runtime-selectable page sizes use the query-only `Limit<MAX>` refinement,
  where `MAX` is at most 65,534. Bounded limits do not
  refine offsets, byte budgets, contract integers, full-population aggregate
  work, or projection candidate work; those retain their separately compiled
  bounds.
- One RiffDB invocation still returns at most its compiled maximum and never
  more than 65,534 rows. The independent 4 MiB encoded-result and transport
  ceilings often make the practical maximum smaller for wide rows, as can
  provider, policy, cost, hydration, or role bounds. RiffDB has no hidden
  multi-page response or streaming query protocol.
- Candidate algebra supports one compiler-sealed non-output binding, at most
  eight same-partition declared-index sources, and only single-source
  deduplication, intersection, union, or authorized root-universe difference.
  It is complete-before-order and all-or-refusal, but it is not a general join,
  correlated subquery language, recursive expression, cross-partition plan, or
  caller-supplied set. Ordinary exact-index and declared `long_pattern_v1`
  sources are available and may participate in one common fenced result set;
  this does not make every other provider kind an interchangeable candidate
  source.
  One-level bounded one-to-many expansion compiles to additive language, plan,
  and module identities and executes in memory and redb with complete-before-
  release, policy-before-observation semantics. Depth-two, cross-partition,
  independent target pagination, and general joins remain unavailable.
  A query may expose at most 32 compiler-expanded root orders through one
  complete contract enum. Callers cannot submit arbitrary sort structure, and
  changing the enum choice starts a different cursor family.
- `long_pattern_v1` provides exact binary or Unicode-fold equality, literal
  prefix/suffix/substring, and LIKE/ILIKE matching for source values through
  8,000 bytes. It is a compiler-declared, rebuildable candidate provider, not a
  general regular-expression or ad hoc search endpoint. A direct negated
  provider source is unavailable: negation must be compiled as candidate
  difference from an authorized positive root universe. Provider generations
  are finite, and a continuation whose authoritative snapshot or provider
  generation is no longer servable fails typed instead of switching epochs.
- An ordinary command may atomically delete and return the transaction-current preimage of exactly
  one complete-key, partition-local `no_inbound` entity. Multiple ordinary deletes, inbound
  `restrict` or `cascade`, set-null, orphaning, cross-partition deletion, and physical erasure are
  unavailable on that unary path; use the separately compiler-bounded collection deletion forms
  where supported. Delete authority never implies secret-output authority.
- Ordinary row-store indexes currently execute exact equality, one bounded
  membership or presence/text-prefix branch, and the complete declared order.
  `binary_utf8_v1` membership and strict/inclusive interval or two-range `!=`
  complement are bytewise and cursor-safe when that component supplies the
  first remaining order term. Order-preserving canonical numeric, time, UUID,
  and enum components support the same bounded interval shape. Canonical
  string ranges, multiple branching dimensions, overlapping unions,
  residual post-page filters, index intersections, caller-selected plans, and
  general joins are unavailable. Relationship composition is limited to
  compiler-declared same-partition complete-key points, singular-key-driven
  separately bounded index reads, and one ordered dependent complete-key batch
  with zero-or-one target per distinct driver key, plus one compiler-bounded
  same-partition one-to-many expansion nested under its bounded driver.
  One root-local `exists` or `not exists` predicate may lower through a declared
  same-partition junction index to the existing candidate algebra. General
  semijoins, dependent or correlated existence, cross-partition joins,
  Cartesian products, recursion, and runtime join optimization are unavailable.
  Recompile and redeploy a refused named query rather than emulating it in
  application code.
- The WP-690 external receipt is intentionally a narrow source-compilation
  acceptance for shared bytewise equality, membership, and order. It is not a
  full external application or framework-conformance result. Canonical-string
  range sources remain correctly refused because length-prefixed canonical
  order cannot prove lexical range semantics; recompile against an explicitly
  declared `binary_utf8_v1` component rather than applying client filtering or
  comparator substitution.
- Vector search's exact and declared approximate tiers are application-reachable
  through the same named RiffQL operation. The server chooses exact search at
  or below the contract's per-organization `ann_threshold` and first-party HNSW
  strictly above it; callers cannot choose a tier or weaken the declared recall
  target. The approximate graph is rebuilt ephemerally for each bounded query,
  so this POC does not claim persistent-graph latency for large partitions. The
  current production candidate ceiling is 500 rows, which also means a declared
  threshold of 500 or more remains exact under the POC ceiling. The native and typed Protobuf value branches, gRPC and
  hosted MCP conversions, and CLI numeric component arrays now preserve finite
  binary32 vectors without bytes punning. Contract IR V15 now seals one exact
  model identity/current version and bounded replay ceilings per production
  vector field. A compiler-sealed `embed` command now atomically persists the
  entity vector and authoritative model/version/write-sequence evidence;
  generic `set` cannot bypass that evidence. Generated Rust, Go, TypeScript,
  and Python models expose exact-dimension vector values and contract-sealed
  model constructors. Generated clients also expose the bounded symbolic
  staleness/model inspection operation.
- Vector staleness now has v1 count semantics: a declared positive stale-entity
  threshold breaches only when `stale_count > threshold`; duration-based
  semantics are future work. Authoritative embedding and source-field write
  evidence now persists atomically, and paginated staleness/model-version DTOs
  exist and the public symbolic operation enumerates them under an exact role
  grant, bounded page, current policy, and snapshot-bound cursor. The dedicated
  `vector_staleness` health component is maintained from authoritative
  observations rather than being overloaded onto `projection`; missing or
  invalid observer state remains unavailable.
- Production `nearest()` requires one explicit compiler-owned projected source
  and `available`, causal, or duration-bounded freshness. The exact adapter
  admits current-policy rows before ranking and enforces the shared 500-row
  ceiling. Vector projection generations rebuild from a stable authoritative
  snapshot plus retained tail and publish only after an exact checkpoint.
  Compiler-owned replay age/byte/backlog ceilings detach over-budget
  generations from retention; building or rebuilding is a typed refusal, and
  restart never overclaims the persisted frontier. Multi-source vector
  queries, vector joins/aggregates, and
  production ANN routing remain deferred and never fall back to row-store
  scans or mixed snapshots.
- Reactive applications are partition-local and bounded. P8 does not provide
  raw CDC, global or cross-partition order, physical time-based event
  retention, exactly-once external effects, event-sourced reconstruction,
  persisted hydration, direct browser credentials, webhooks, connectors,
  arbitrary callbacks, or in-process agent inference.
- Protected event replay and reaction validation currently enter the redb
  mutation fence to obtain one transaction-current capability/row/relationship
  view, even though replay aborts without mutation. This is a correctness-first
  alpha path and can contend with the writer under heavy protected replay. The
  singleton global reactive wakeup is unavailable to protected roles because
  it cannot suppress hidden-commit timing without a subscription identity;
  protected workers use bounded consumer long-poll instead.
- Contract migration source, canonical artifacts, Application Source V3, Lock
  V4, read-only planning, dedicated authorization, public check/apply/status,
  Gate-A through Gate-C memory semantics, redb staged execution, crash recovery,
  automatic rollback, populated public workflow, direct-parent compatibility
  matrix, and bounded migration baseline are implemented. Gate C deliberately
  rejects key swaps and predecessor-occupied rekey chains.
  Migration administration is intentionally absent from MCP,
  TypeScript, Python, and generated application clients.
- The POC authorization model uses opaque local capability tokens, not a
  production identity provider or OAuth authorization server.
- Capability administration has create and revoke operations but no
  operator-facing inventory command. Retain each issued capability ID in a
  private operational inventory; deleting a bearer file is not revocation.
- Generic bootstrap deploys no application. Its MCP developer can validate and
  deploy contracts but has no wildcard application-data authority. After
  deployment, an administrator must bind a compiled application role or issue
  an exact lineage-scoped capability before application commands or data are
  available through that identity.
- Convenience bootstrap capabilities expire after at most 30 days. There is
  no automatic renewal; a replacement must be created through the public
  service while an administrative capability remains valid.
- Re-running the source installer updates the three binaries but deliberately
  preserves existing keys, server configuration, unit files, and system
  sysusers/tmpfiles definitions. It is not a general configuration migration,
  key-rotation, uninstall, or compatibility-aware upgrade mechanism.
- Individual binary replacement is atomic, but replacement of all three
  binaries is not transactional. Rerun an interrupted install before starting
  or restarting the service.
- Optional Codex MCP registration checks for an existing `riffdb` entry before
  calling the replacement-capable Codex command, but Codex provides no atomic
  create-if-absent operation. Do not run it concurrently with another writer
  to the same user's Codex MCP configuration.
- Metrics are in-process; there is no network metrics exporter.
- A graceful clean restart proves bounded continuity but is not a full
  population scrub. Latent corruption outside the bound roots may fail closed
  when an affected row is first accessed. Dirty restart still performs complete
  startup validation, but the separately specified authorized offline scrub is
  not yet exposed as a public POC operation.
- Outbox delivery has only the explicitly configured POC connector behavior.
- Projection state is rebuildable and can be degraded while authoritative
  commits remain available.
- Tokenized-text declarations, durable provider-state V1, named boolean search,
  and fixed `riff_bm25_v1` ranking are implemented. The closed boolean shapes
  are conjunction, capped disjunction, phrase, and bounded ordered proximity.
  There is no wildcard, regular expression, fuzzy match, stemming, highlighting,
  snippet, caller boost, score output, hybrid vector/text query, or scan fallback.
  The production provider retains eight epochs per registered query shape, so a
  ranked cursor may return the typed snapshot-retired outcome after sustained
  writes even if the cursor token itself has not expired.
- Restore publication assumes the database directory is private to the sole
  configured service identity: the current operator for user scope or `riffdb`
  for system scope. An uncooperative process with equal write authority can
  invalidate filesystem assumptions, so shared write access and concurrent
  servers are unsupported.
- Backup and restore are offline. There is no online, incremental, encrypted,
  remote-object, or point-in-time backup.
- Restore preserves `DatabaseId` while rewinding history. Destroyed application
  and administration sequence suffixes can still be reused. ADR-0072 adds a
  durable `history_incarnation` fence so participating clients can detect the
  rewind via optional `observed_history_incarnation` validation
  (`RDB-HISTORY-0101`). Non-participating clients remain unvalidated.
- Locators, cursors, sessions, read-after-sequence expectations, authorization
  observations, and idempotency assumptions from a destroyed suffix are
  invalid after restore.
- The optional systemd MCP socket shares one capability among members of its
  Unix socket group. Spawn one bridge per client when identities must differ.
- Release verification checks unit syntax and hardening directives, runs
  `riffdbd` with closed stdin through SIGTERM drain, and drives authenticated
  newline-delimited MCP over the bridge's socket-style stdin/stdout. It does
  not start the units inside the host systemd manager, so deployment must still
  verify that the target manager applies the declared sandbox and socket
  ownership.
- The PostgreSQL counterexamples prove only that unsafe patterns remain
  expressible in a general SQL/host-code surface and are absent from RiffDB's
  supported application mutation surface. They are not performance cases.
- POC performance has no absolute TPS gate. No benchmark claim is valid until
  the exact hardware, filesystem, durability mode, workload, revision, and raw
  output are published.
- A POC-exit decision still requires all POC-001 through POC-010 evidence plus
  a human architecture review: proceed to alpha, revise and repeat, or stop.
