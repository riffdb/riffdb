# Configuration Reference

Application gRPC has a closed listener profile: development-only cleartext on
a literal loopback address, verified direct TLS on TCP, or a protected Unix
socket. There is no insecure remote mode. Hosted MCP remains loopback-only.
See [Remote and Local Application Ingress](operations/REMOTE-INGRESS.md).

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
All database, backup, and key paths across the process must be pairwise
lexically disjoint. The process opens every database and builds every graph
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

The database path, backup root, capability key path, and idempotency key path
must be lexically disjoint. A file is not accepted when another configured path
is equal to it, contains it, or is contained by it.

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

The TOML path is selected only by `--config`, then `RIFFDB_CONFIG`, then
absence. There is no directory discovery. A present invalid higher-precedence
value rejects instead of falling through.

The CLI's current `[client]` endpoint remains exact lowercase
`http://<literal-loopback-IP>:<port-1..65535>`; remote verified-TLS application
connections use the Rust client configuration documented in
[Remote and Local Application Ingress](operations/REMOTE-INGRESS.md). Unknown,
duplicate, or wrong-type input rejects. Output is `human` or `json`. Attempts
is canonical decimal `1..=10`.

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
