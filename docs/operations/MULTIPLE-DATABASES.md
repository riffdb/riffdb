# Multiple Databases

One `riffdbd` process can host up to 512 independently durable databases. Each
configured alias has its own redb file, catalog, credentials, commit sequence,
projections, outbox, health state, and recovery boundary.

## Selection occurs before authentication

Every public connection selects a canonical database alias before its
credential is authenticated. A credential is bound to one database audience
and cannot authorize another alias. The server does not fall back to a default
database when an explicit selection is unknown or malformed.

Public paths select the alias as follows:

| Path | Selection |
|---|---|
| CLI and generated clients | Client configuration `database` value |
| MCP stdio | `database` in the MCP configuration file |
| Hosted MCP | `riffdb-database` request header |
| gRPC | Checked database-routing metadata from the client facade |

## Add or remove a database

When `riffdbd` is started with a configuration file that declares `[databases.<alias>]`
tables, the daemon monitors that configuration file for modifications and reconciles the
configured database inventory online without restarting the process or interrupting
active peer aliases:

- **Online provisioning:** Adding a new `[databases.<alias>]` entry opens or initializes
  its `path`, starts its per-database workers (and idle-hibernation supervisor
  when `limits.idle_hibernate_seconds` is configured), and registers the alias on the gRPC and
  hosted MCP routing tables.
- **Online deprovisioning:** Removing an existing `[databases.<alias>]` entry removes
  the alias from gRPC and hosted MCP routing, drains its per-database workers, writes
  `CleanCloseState::Clean`, and releases its `redb` file lock while leaving its durable
  file on disk.
- **Fail-closed validation:** If a modified configuration file is malformed, exceeds
  512 databases (`512 KiB` configuration document bound), mutates an existing
  `[databases.<alias>]` entry (`retained_database_changed` — such as changing its
  `path`, `projections_root`, or `[databases.<alias>.outbox]` after the alias is
  opened), or mutates process-wide listener, TLS, authentication, or resource
  settings, the reload is rejected with a warning diagnostic (`config reload rejected`)
  and the previously active database inventory remains unchanged. To apply
  configuration changes to an already-open database alias, restart `riffdbd`
  (for example, `docker compose restart riffdbd`).

For installer-managed single-user deployments, you can also use the database management
workflow described in [Installation](../installation.md).

Do not copy a credential from one alias into another configuration. Create or
bind the application role against the target database so the credential,
audience, active contract, and named operations agree.

## Multi-Database `riffdbd.toml` and Docker Compose Bootstrap

When using `[databases.<alias>]` tables, `[server]` defines only process-wide
settings (`audience`, `capability_keys`, `idempotency_keys`, and listeners).
**Do not** set `database` or `environment` under `[server]` when `[databases.<alias>]`
tables are present — each `[databases.<alias>]` entry owns its own `path`,
`backup_root`, `projections_root`, `environment`, and `[databases.<alias>.outbox]`
(always set `projections_root` to a writable directory inside container volumes, and
configure `[databases.<alias>.outbox]` when deployed contracts emit durable outbox
events):

```toml
[server]
audience = "my-cluster-audience"
capability_keys = "/run/secrets/capability.keys"
idempotency_keys = "/run/secrets/idempotency.keys"

[server.application_listener]
mode = "direct_tls"
listen = "0.0.0.0:7443"
public_endpoint = "https://riffdbd:7443"
certificate_chain = "/run/secrets/server.pem"
private_key = "/run/secrets/server.key"

[databases.openfga]
path = "/var/lib/riffdb/data/openfga.redb"
backup_root = "/var/lib/riffdb/backups/openfga"
projections_root = "/var/lib/riffdb/data/projections/openfga"
environment = "playground"

[databases.openfga.outbox]
kind = "durable_local"
directory = "/var/lib/riffdb/data/outbox/openfga"
quota_encoded_bytes = 268435456

[databases.better-auth]
path = "/var/lib/riffdb/data/better-auth.redb"
backup_root = "/var/lib/riffdb/backups/better-auth"
projections_root = "/var/lib/riffdb/data/projections/better-auth"
environment = "playground"

[databases.better-auth.outbox]
kind = "durable_local"
directory = "/var/lib/riffdb/data/outbox/better-auth"
quota_encoded_bytes = 268435456
```

When running in Docker Compose with a published host port (for example
`127.0.0.1:17443:7443`) and a certificate issued with
`SAN = DNS:riffdbd, DNS:localhost, IP:127.0.0.1`:

1. **Container `healthcheck` (unauthenticated process liveness):**
   Omit `--database` and omit `--tls-server-name` (or set it to `127.0.0.1` to
   match `--endpoint https://127.0.0.1:7443`):
   ```yaml
   healthcheck:
     test: ["CMD", "riffdb", "--endpoint", "https://127.0.0.1:7443", "--tls-trust-root", "/run/secrets/ca.pem", "server", "health"]
     interval: 2s
     timeout: 3s
     retries: 30
   ```
2. **Per-database `capability bootstrap --operator` (executed inside the container):**
   Bootstrap is a one-shot local-authority operation per database and must
   connect to loopback inside the container (`https://127.0.0.1:7443`). Do **not** set
   `tls_server_name = "riffdbd"` when `endpoint` is `https://127.0.0.1:7443`
   (mismatched endpoint/TLS identities fail closed with `configuration_invalid`);
   omit `tls_server_name` so `127.0.0.1` is verified against the certificate's
   `IP:127.0.0.1` SAN.
   `capability bootstrap --operator` automatically includes the server's
   configured `[server] audience` alongside `riffdb-cli` and `riffdb-mcp` (or you
   can pass repeatable `--audience <AUDIENCE>` explicitly). You can bootstrap all
   configured databases in one loop:
   ```bash
   for db in openfga better-auth; do
     docker compose exec -T riffdbd \
       riffdb \
         --endpoint https://127.0.0.1:7443 \
         --tls-trust-root /run/secrets/ca.pem \
         --database "$db" \
         capability bootstrap \
         --operator \
         --audience playground-grpc \
         --lifetime-seconds 2592000 \
         --bearer-output "/var/lib/riffdb/bootstrap/operator-${db}.credential"
   done
   ```
3. **Deploying applications, credential lifetimes, and rotation:**
   Both `capability bootstrap --operator` and `application deploy --provision-role`
   default `--lifetime-seconds` to `2592000` (30 days, the maximum capability
   lifetime). Before a role credential expires, rotate it in place without
   re-bootstrapping the database:
   ```bash
   riffdb --database openfga application deploy \
     --provision-role OpenFgaAdapter \
     --replace-role-credential \
     --lifetime-seconds 2592000 \
     --credential-output /absolute/path/to/creds/role-openfga.credential
   ```
4. **Resetting a database after a failed bootstrap attempt:**
   Bootstrap is strictly one-shot per database. Each `<db>.redb` file has
   durable companion files in the same directory (`<db>.redb.journal*`,
   `<db>.redb.format`, `<db>.redb.derived`, `<db>.redb.riffoutbox`) plus its
   `projections_root` directory. Deleting only `<db>.redb` leaves companion
   files from the previous `DatabaseId` behind and causes startup recovery or
   `credential_invalid` failures. To reset one database cleanly while `riffdbd`
   is stopped, remove all of its companion paths together:
   ```bash
   rm -rf /var/lib/riffdb/data/openfga.redb* \
          /var/lib/riffdb/data/outbox/openfga \
          /var/lib/riffdb/data/projections/openfga
   ```
   When redeploying an application against a recreated database from the same
   working tree, either pass `--replace-role-credential` or remove the stale
   local deployment state under `<app>/.riffdb/deployments/`.
5. **Host-side client configuration and Rootless Podman permissions:**
   Use an **absolute path** for `credential_file` and `tls_trust_root`, ensure
   credential and key files are mode `0600` (regular files, never symlinks), and
   omit `tls_server_name` (or set `tls_server_name = "127.0.0.1"` to match
   `endpoint = "https://127.0.0.1:17443"`):
   ```toml
   [client]
   endpoint = "https://127.0.0.1:17443"
   database = "openfga"
   output = "json"
   max_attempts = 3
   credential_file = "/absolute/path/to/creds/operator-openfga.credential"
   tls_trust_root = "/absolute/path/to/secrets/ca.pem"
   ```
   *(In-network container peers that connect to `endpoint = "https://riffdbd:7443"`
   verify `DNS:riffdbd` automatically.)*

   > [!TIP]
   > **Rootless Podman UID mapping (`keep-id`):** The release container runs as
   > UID/GID `65532:65532` and requires `0600` secret files owned by its
   > effective UID. Under Rootless Podman, add a Compose override with
   > `userns_mode: "keep-id:uid=65532,gid=65532"` so host-owned `0600` files bind-mounted
   > into the container appear owned by `65532:65532` inside the container.

## MCP per application

Create one MCP configuration per database and role. Multiple entries may point
to the same `riffdb-mcp` binary and `riffdbd` endpoint, but each bridge is bound
to exactly one selected alias and credential. This preserves least authority
without requiring a separate server instance.

Run before registering a bridge:

```bash
riffdb-mcp doctor --config ~/.config/riffdb/mcp-myapp.toml
```

The doctor verifies routing and credential/tool discovery without widening a
narrow application role merely to grant health access.

## Isolation and operations

Backup, restore, reset, and removal operate on one database while the service is
offline. A fault in one database can make that database unavailable without
changing another database's authoritative state. Process-level resource limits
remain shared, so capacity planning must account for the combined configured
set.
