# Configuration Reference

Application gRPC has a closed listener profile: development-only cleartext on
a literal loopback address, verified direct TLS on TCP, or a protected Unix
socket. There is no insecure remote mode. Hosted MCP remains loopback-only.
See [Remote and Local Application Ingress](operations/REMOTE-INGRESS.md).

## Project `riffdb.toml`

The database-shaped `init`, `push`, `generate`, `status`, `diff`, and `migrate`
verbs use a closed project document. They select `riffdb.toml` in the current
directory by default; explicit `--config` and then `RIFFDB_CONFIG` take
precedence. Other CLI commands retain the existing no-discovery behavior.

```toml
[client]
endpoint = "http://127.0.0.1:7443"
database = "default"

[project]
schema = "riffdb.application.json"
generators = ["rust"]
```

The schema path must be bounded, workspace-relative, and free of `.` or `..`
components. `generators` contains one through four unique values from `rust`,
`go`, `typescript`, and `python`. Unknown keys, duplicate targets, symlinked
configuration files, absolute or escaping schema paths, and empty target lists
reject without fallback. See the [Database-Shaped Project
Workflow](getting-started/DATABASE-WORKFLOW.md) for command semantics.

## `riffdbd`

Every server field resolves independently in this order:

1. an explicit CLI value;
2. its `RIFFDB_*` environment value;
3. an explicitly selected TOML document; then
4. the built-in safe local default.

A present invalid higher-precedence value rejects instead of falling through.
The TOML path is selected by `--config`, then `RIFFDB_CONFIG`, then absence.
There is no implicit configuration-file discovery.

The legacy database, environment, and backup settings below define one database
with alias `default`. A process may instead define 1 through 32
`[databases.<alias>]` tables. Mixing the legacy and named forms rejects.

| Setting | CLI | Environment | TOML | Default | Restart | Impact |
|---|---|---|---|---|---|---|
| Database | `--database` | `RIFFDB_DATABASE` | `server.database` | `data/riffdb.redb` | Yes | Correctness, storage compatibility |
| Application listener | `--listen` (legacy loopback only) | `RIFFDB_LISTEN` (legacy loopback only) | `server.grpc_listen` or `server.application_listener` | `127.0.0.1:7443` loopback cleartext | Yes | Availability, security |
| Environment | `--environment` | `RIFFDB_ENVIRONMENT` | `server.environment` | `local` | Yes | Capability authentication compatibility |
| gRPC audience | `--audience` | `RIFFDB_AUDIENCE` | `server.audience` | `riffdb-grpc-loopback` | Yes | Capability authentication compatibility |
| Hosted MCP listen | `--mcp-listen` | `RIFFDB_MCP_LISTEN` | `server.mcp_listen` | absent | Yes | Availability, security |
| Hosted MCP origins | repeatable `--mcp-origin` | `RIFFDB_MCP_ORIGINS` | `server.mcp_origins` | empty | Yes | Browser-origin security |
| Backup root | `--backup-root` | `RIFFDB_BACKUP_ROOT` | `maintenance.backup_root` | `$CWD/backups` | Yes | Correctness, recovery, security |
| Capability key path | `--capability-keys` | `RIFFDB_CAPABILITY_KEYS` | `server.capability_keys` | `config/capability.keys` | Yes | Authentication and key compatibility |
| Idempotency key path | `--idempotency-keys` | `RIFFDB_IDEMPOTENCY_KEYS` | `server.idempotency_keys` | `config/idempotency.keys` | Yes | Same-key recovery compatibility |

The only TOML tables and keys are:

```toml
[server]
database = "/var/lib/riffdb/data/riffdb.redb"
grpc_listen = "127.0.0.1:7443"
environment = "local"
audience = "riffdb-grpc-loopback"
capability_keys = "/etc/riffdb/capability.keys"
idempotency_keys = "/etc/riffdb/idempotency.keys"
# mcp_listen = "127.0.0.1:7444"
# mcp_origins = ["http://127.0.0.1:3000"]

[maintenance]
backup_root = "/var/lib/riffdb/backups"
```

### Named archive directories (WP-749 integration)

An operator may bind up to 16 archive names per database in TOML. The legacy
single-database form uses `[[maintenance.archives]]`; the named form uses
`[[databases.<alias>.archives]]`. Each entry requires all three fields:

```toml
[[maintenance.archives]]
name = "daily"
path = "/var/lib/riffdb/archives/daily"
encryption = "operator_managed"
```

Names are checked identifiers and must be unique within their database. Paths
must be bounded absolute directories and disjoint from every database, backup,
projection, archive and other reserved process path. Invalid or overlapping
entries reject configuration. `encryption` is either `unencrypted` or
`operator_managed`; the latter declares external operator-provided encryption,
and does not ask RiffDB to encrypt files. Archive paths never come from restore
request input.

These bindings currently support the internal archive maintenance driver.
Automatic archive production and the public archive restore command remain
under implementation; configuring an entry does not start an archive worker.
See [Backup and Restore](backup-restore.md).

The legacy `server.grpc_listen` form always means literal-loopback cleartext.
For an explicit closed listener profile, omit `grpc_listen` and select exactly
one tagged table:

```toml
[server.application_listener]
mode = "loopback_cleartext"
listen = "127.0.0.1:7443"
```

```toml
[server.application_listener]
mode = "direct_tls"
listen = "0.0.0.0:7443"
public_endpoint = "https://riffdb.internal.example:7443"
certificate_chain = "/etc/riffdb/tls/server-chain.pem"
private_key = "/etc/riffdb/tls/server-key.pem"

[server.application_listener.bounds]
max_connections = 1024
max_streams_per_connection = 128
handshake_timeout_seconds = 10
idle_timeout_seconds = 300
keepalive_interval_seconds = 30
drain_timeout_seconds = 30
```

```toml
[server.application_listener]
mode = "local_socket"
path = "/run/riffdb/application.sock"
access = "owner_only" # or "owner_and_group"
```

The explicit listener table cannot be mixed with `grpc_listen`, `--listen`, or
`RIFFDB_LISTEN`. TLS has no cleartext fallback, native-root mode, trust-all
mode, mutual-TLS selector, cipher-suite selector, or provider selector. The
certificate must contain the DNS/IP identity in `public_endpoint`; validation
happens before bind. Certificate/key replacements are adopted atomically for
new handshakes only after the complete pair validates.

The equivalent multi-database form is:

```toml
[server]
grpc_listen = "127.0.0.1:7443"
audience = "riffdb-grpc-loopback"
capability_keys = "/etc/riffdb/capability.keys"
idempotency_keys = "/etc/riffdb/idempotency.keys"

[databases.ea]
path = "/var/lib/riffdb/data/ea.redb"
backup_root = "/var/lib/riffdb/backups/ea"
environment = "local"

[databases.budget_demo]
path = "/var/lib/riffdb/data/budget-demo.redb"
backup_root = "/var/lib/riffdb/backups/budget-demo"
environment = "local"
```

Aliases match `[a-z][a-z0-9_-]{0,63}`, sort by canonical bytes, and are unique.
All database, backup, projection, source/receiver artifact, and key paths across
the process must be pairwise lexically disjoint. The process opens every database and builds every graph
before publishing readiness. One structural startup failure stops the process.

Backup roots must be siblings. This is valid:

```toml
[databases.default]
path = "/var/lib/riffdb/data/riffdb.redb"
backup_root = "/var/lib/riffdb/backups/default"
environment = "local"

[databases.ea]
path = "/var/lib/riffdb/data/ea.redb"
backup_root = "/var/lib/riffdb/backups/ea"
environment = "local"
```

This is invalid because one configured root contains another:

```toml
[databases.default]
path = "/var/lib/riffdb/data/riffdb.redb"
backup_root = "/var/lib/riffdb/backups"
environment = "local"

[databases.ea]
path = "/var/lib/riffdb/data/ea.redb"
backup_root = "/var/lib/riffdb/backups/ea"
environment = "local"
```

Startup reports the two conflicting logical roles and aliases, but never
echoes configured path values.

The complete document is UTF-8 and at most 65,536 bytes. Unknown tables or
keys, duplicates, wrong types, duplicate scalar flags, missing flag values, and
empty selected values reject. Paths are nonempty, at most 4,096 platform bytes,
and lexically normalized. The database and key paths may be relative to the
server working directory; the backup root must be absolute.

The database path, backup root, projection root, source and receiver artifact
roots, capability key path, and idempotency key path must be lexically disjoint. A file
is not accepted when another configured path is equal to it, contains it, or is
contained by it.

Each database reserves a private replication artifact root by appending
`.riffreplication` to its path (for example, `data/main.redb.riffreplication`).
The derived path also has the 4,096-byte ceiling. This directory holds at most
four source bootstrap artifacts and an exclusion lock; it cannot overlap any
other configured database or root. Source composition creates it with private
permissions and refuses symlink or identity substitution. See
[bounded bootstrap transfer](architecture/CHANGELOG-V3.md#bounded-bootstrap-transfer-wp-746)
for hold retention and cleanup behavior.

Each database also reserves `.riffreceiver` beside its database path, with the
same path-length and disjointness checks. Its fixed receiver inventory is
separate from source artifacts so promotion cannot mix the two layouts. The
managed receiver holds an exclusive inventory lock and creates private transfer
and candidate directories. Initial creation uses temporary names until the first
durable progress checkpoint exists; crash recovery discards only those known
unpublished files and resumes published progress. After authenticated attachment
succeeds, the receiver removes candidate scratch links and then transfer evidence,
while retaining the live follower's engine lock. Interrupted cleanup resumes
against the validated live follower. See [follower mode](#follower-mode) for daemon
configuration and lifecycle behavior.

The packaged systemd unit fixes the authoritative state root at
`/var/lib/riffdb/data`, the backup root at `/var/lib/riffdb/backups`, and the
configuration root at `/etc/riffdb`. The latter contains the root-owned
`riffdbd.toml` path-only configuration and the two protected digest-key
documents. It contains no raw digest key in TOML.

Optional hosted MCP settings:

| Option | Value | Restart | Impact |
|---|---|---|---|
| `--mcp-listen` | Literal loopback address with nonzero port | Yes | Enables hosted Streamable HTTP MCP |
| `--mcp-origin` | Repeatable checked loopback origin | Yes | Browser-origin authorization |

`--mcp-origin` without `--mcp-listen` rejects. With no allowed origins, only
requests without an `Origin` header are accepted. The MCP protected-resource
audience is derived exactly as `http://<listen-authority>/mcp`; it cannot be
configured and must differ from the gRPC audience.

`RIFFDB_MCP_ORIGINS` is one nonempty comma-separated list with no trimming or
normalization. TOML uses a string array and CLI uses one flag per origin.
Whichever source wins supplies the whole list. At most 16 unique origins and
8,192 aggregate bytes are accepted; each origin is at most 512 visible ASCII
bytes and must pass the hosted transport's exact loopback-origin checks.

Other `RIFFDB_*` variables are not server settings. There is no metrics
listener, storage-engine selector, group-commit tuning, transaction callback,
or nondeterministic scheduler setting in the POC configuration surface.

## Digest-Key Documents

Capability keys:

```text
riffdb-capability-digest-keys-v1
1:<exactly 64 lowercase hexadecimal characters>
```

Idempotency keys:

```text
riffdb-idempotency-digest-keys-v1
1:<exactly 64 lowercase hexadecimal characters>
```

Documents are at most 1,024 bytes, end with LF, contain one through eight
entries, use unique nonzero canonical decimal IDs, and contain no duplicate
material. The first listed entry is the write key; later entries remain
readable for rotation. The two namespaces must never reuse material.

On Linux, each secret source must be a regular non-symlink final component
owned by the process effective user with no group or other mode bits. Metadata
is checked before and after a no-follow open.

## `riffdb` Client

Configuration is resolved independently for each field, highest precedence
first:

| Field | Flag | Environment | TOML | Default |
|---|---|---|---|---|
| Endpoint | `--endpoint` | `RIFFDB_ENDPOINT` | `client.endpoint` | `http://127.0.0.1:7443` |
| Database | `--database` | `RIFFDB_DATABASE` | `client.database` | `default` |
| Output | `--output` | `RIFFDB_OUTPUT` | `client.output` | `human` |
| Attempts | `--max-attempts` | `RIFFDB_MAX_ATTEMPTS` | `client.max_attempts` | `3` |
| Credential file | `--credential-file` | `RIFFDB_CREDENTIAL_FILE` | `client.credential_file` | absent |
| TLS trust root | none | `RIFFDB_TLS_TRUST_ROOT` | `client.tls_trust_root` | absent |
| TLS server name | none | `RIFFDB_TLS_SERVER_NAME` | `client.tls_server_name` | absent |

The TOML path is selected only by `--config`, then `RIFFDB_CONFIG`, then
absence. There is no directory discovery. A present invalid higher-precedence
value rejects instead of falling through.

The endpoint is either exact lowercase
`http://<literal-loopback-IP>:<port-1..65535>` or canonical
`https://<DNS-or-IP>:<port-1..65535>`. HTTPS requires both an absolute,
normalized trust-root path and an exact peer DNS name or IP that matches the
endpoint authority. Supplying either TLS field for cleartext, omitting either
field for HTTPS, or requesting a trust-all/native-root/downgrade mode rejects.
Generated application client configuration retains these two non-secret TLS
selectors. Unknown, duplicate, or wrong-type input rejects. Output is `human`
or `json`. Attempts is canonical decimal `1..=10`.

Exactly one normal credential source may resolve:

- the protected credential file selected above; or
- `RIFFDB_CAPABILITY_TOKEN`, an exact 43-byte bearer presentation.

Supplying both rejects. Prefer the protected file. Machine output is one compact
JSON object plus LF and no other stdout bytes.

## `riffdb-mcp` Stdio Bridge

The bridge accepts only:

```text
--config PATH
--endpoint LOOPBACK_HTTP_ENDPOINT
--database DATABASE
```

Configuration path precedence is `--config`, `RIFFDB_MCP_CONFIG`, then absent.
Endpoint precedence is `--endpoint`, `RIFFDB_MCP_ENDPOINT`, `[mcp].endpoint`,
then `http://127.0.0.1:7443`. Database precedence is `--database`,
`RIFFDB_MCP_DATABASE`, `[mcp].database`, then `default`.

Its TOML contains only:

```toml
[mcp]
endpoint = "http://127.0.0.1:7443"
database = "default"
credential_file = "/var/lib/riffdb-mcp/credential"
expected_audience = "riffdb-grpc-loopback"
```

Credential selection is exactly one of `RIFFDB_MCP_CAPABILITY_TOKEN`,
`RIFFDB_MCP_CREDENTIAL_FILE`, or `[mcp].credential_file`. The environment file
path takes precedence over the TOML path, while simultaneous raw token and file
selection rejects. The credential is loaded once and moved into the ordinary
public gRPC client. `expected_audience`, when present, is bounded target-identity
evidence written by application provisioning; it grants no authority and has
no flag or environment override.

The bridge accepts a database alias, not a path. It does not accept a storage
option, environment, audience authority override, policy, or server-side
authority.

## Follower mode

`--mode follower` (or `RIFFDB_MODE=follower`, or `[server] mode = "follower"`)
selects the separate follower startup path before primary initialization or
maintenance. The default remains `primary`. Each configured database must have
one complete `replication_source` block; primary mode rejects such blocks.
The mode follows the usual CLI, environment, then file precedence. Source
identity and trust settings are file-only and cannot be overridden individually.

For the legacy single-database form, use `[server.replication_source]`. For a
named database, use `[databases.<alias>.replication_source]`. Required fields are:

| Field | Meaning |
| --- | --- |
| `endpoint` | Canonical HTTPS primary endpoint |
| `trust_root` | Absolute path to the primary's CA certificate file |
| `server_name` | Exact verified server identity matching the endpoint |
| `credential_file` | Absolute path to a protected replication capability token file |
| `database` | Database alias at the source |
| `database_id` | Expected UUIDv7 database identity, with hyphens |
| `history_incarnation` | Expected nonzero history incarnation |
| `leadership_epoch` | Expected nonzero leadership epoch |
| `hold_id` | Stable nonzero 16-byte source hold ID, as 32 lowercase hex digits |

Credential files use the existing protected-file loader. TLS always verifies the
configured CA and server name. Trust and credential paths cannot overlap database,
backup, projection, replication scratch or digest-key paths. All source peers are
checked before local construction starts. Receiver scratch is the reserved
`<database-path>.riffreceiver` sibling directory.

Startup bootstraps an absent follower or fully validates and reopens an existing
one. A retained complete transfer permits the existing candidate-publication
recovery path to check its exact fence and destination before replacement.
Tail reception uses the supervised worker's bounded retry and shutdown rules.

After validation, the daemon activates a writer-free application service for each
configured alias, binds configured hosted MCP routes, and emits the ordinary
application-ready receipt. Standard input accepts `shutdown`; process signals
also stop the daemon. Shutdown closes transport admission, stops replication and
drains accepted service reads before releasing the remaining snapshot pins.

Authentication and current policy observe the latest completed follower prefix.
A capability or contract installed later on the source becomes visible through
replication; followers expose no local capability-bootstrap convenience path.
Commands, administration, migration, maintenance and export return the typed
follower-mode refusal. Policy-required durable read audit also refuses. See
[the follower audit boundary](architecture/CHANGELOG-V3.md) for Health, Statistics
and denied-read telemetry.

The daemon process tests compare every authoritative namespace under the
app-baseline workload and resume across repeated source and follower crashes.
WP-746 and [WP-747](architecture/WP-747-VERIFICATION.md) pass their full CI
gates, including follower freshness, provider equality and sequence-lag reporting.

Follower named queries can use the existing exact-text, exact-predicate,
tokenized-text and long-pattern providers. Their local checkpoints live under
the configured `projections_root` and remain rebuildable. Background workers
read only completed follower snapshots; the applied application frontier is
their local head for minimum-commit and AdmissionHead checks. A provider that
has not caught up returns the existing typed freshness or availability refusal.
Configured scalar and vector columnar sources stay cold until demand. One owned
worker builds independently validated disposable V2 views under
`projections_root/follower-columnar`; no source artifact is required and no local
authoritative control is written. Cold or activating sources return the existing
typed Building/unavailable outcome and keep columnar health degraded. Failed
materialization remains closed until owned recovery or restart; a request cannot
clear failure. Restart rebuilds these disposable views from the completed prefix.

Projected reads retain [Causal, Bounded and Available](concepts/CONSISTENCY.md)
semantics using the applied local head. Replication source-head observations only
report lag. Foreign database/history tokens refuse before waiting, and opaque
cursors are local to one live process. See
[sequence-lag health and statistics](operations/REMOTE-INGRESS.md) for both-node reporting.
