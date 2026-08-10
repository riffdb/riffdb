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
- Projected ad-hoc aggregates are deliberately narrow. `count`, `sum`, `min`,
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
- Reactive applications are partition-local and bounded. P8 does not provide
  raw CDC, global or cross-partition order, physical time-based event
  retention, exactly-once external effects, event-sourced reconstruction,
  persisted hydration, direct browser credentials, webhooks, connectors,
  arbitrary callbacks, or in-process agent inference.
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
- Outbox delivery has only the explicitly configured POC connector behavior.
- Projection state is rebuildable and can be degraded while authoritative
  commits remain available.
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
