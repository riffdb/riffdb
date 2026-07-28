# Known Limitations

These limits are part of the POC release posture, not hidden roadmap promises.

- RiffDB is not production ready. Its dual license does not change the POC
  security, compatibility, or support limits.
- Linux is the POC deployment gate. Protected credential and key-file loading
  is unavailable on other platforms.
- gRPC and hosted MCP are loopback-only, cleartext, and have no TLS, proxy,
  OAuth, or remote-bind support.
- `riffdbd` exposes only the documented POC configuration fields. There is no
  network metrics listener, storage-engine selector, general tuning surface,
  or secret value in TOML.
- systemd `active` means the process started; use RiffDB Health for database
  readiness after bootstrap.
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
- There is no general SQL surface, arbitrary transaction callback, analytical
  join engine, distributed transaction, replication, failover, or consensus.
- The POC authorization model uses opaque local capability tokens, not a
  production identity provider or OAuth authorization server.
- Metrics are in-process; there is no network metrics exporter.
- Outbox delivery has only the explicitly configured POC connector behavior.
- Projection state is rebuildable and can be degraded while authoritative
  commits remain available.
- Restore publication assumes the database directory is private to the
  `riffdb` service user. An uncooperative process with equal write authority can
  invalidate filesystem assumptions, so shared write access and concurrent
  servers are unsupported.
- Backup and restore are offline. There is no online, incremental, encrypted,
  remote-object, or point-in-time backup.
- Restore preserves `DatabaseId` while rewinding history. There is no
  incarnation/history epoch, so destroyed application and administration
  sequence suffixes can be reused.
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
