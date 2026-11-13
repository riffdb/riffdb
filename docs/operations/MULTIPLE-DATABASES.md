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
     cred="/var/lib/riffdb/bootstrap/operator-${db}.credential"
     docker compose exec -T riffdbd /bin/sh -eu -c '
       db="$1"
       cred="$2"
       if [ -s "$cred" ]; then
         exit 0
       fi
       out="$(riffdb \
         --output json \
         --endpoint https://127.0.0.1:7443 \
         --tls-trust-root /run/secrets/ca.pem \
         --database "$db" \
         capability bootstrap \
         --operator \
         --audience playground-grpc \
         --lifetime-seconds 2592000 \
         --bearer-output "$cred")"
       case "$out" in
         *\"status\":\"created\"*) ;;
         *)
           rm -f "$cred"
           echo "$out" >&2
           exit 1
           ;;
       esac
     ' sh "$db" "$cred"
   done
   ```
   Or as a one-shot init service in `compose.yaml` that shares `riffdbd`'s network namespace (`network_mode: "service:riffdbd"`) so `127.0.0.1:7443` is local loopback. Preserve any existing credential file and never pass `--force` unconditionally on rerun (an already-bootstrapped database returns `bootstrap_conflict` with exit code `0`, so `--force` would overwrite a working local bearer before the server reports the conflict):
   ```yaml
   services:
     riffdb-bootstrap:
       image: riffdb:latest
       network_mode: "service:riffdbd"
       depends_on:
         riffdbd:
           condition: service_healthy
       volumes:
         - ./secrets:/run/secrets:ro
         - riffdb-bootstrap:/var/lib/riffdb/bootstrap
       entrypoint: ["/bin/sh", "-eu", "-c"]
       command:
         - |
           for db in openfga better-auth; do
             cred="/var/lib/riffdb/bootstrap/operator-$${db}.credential"
             if [ -s "$$cred" ]; then
               continue
             fi
             out="$$(riffdb --output json \
               --endpoint https://127.0.0.1:7443 \
               --tls-trust-root /run/secrets/ca.pem \
               --database "$$db" \
               capability bootstrap --operator \
               --audience playground-grpc \
               --lifetime-seconds 2592000 \
               --bearer-output "$$cred")"
             case "$$out" in
               *\"status\":\"created\"*) ;;
               *)
                 rm -f "$$cred"
                 echo "$$out" >&2
                 exit 1
                 ;;
             esac
           done
   ```
3. **Deploying applications, custom environments, credential lifetimes, and rotation:**
   Both `capability bootstrap --operator` and `application deploy --provision-role`
   default `--lifetime-seconds` to `2592000` (30 days, the maximum capability
   lifetime). Pass `--force` to `capability bootstrap` only on an unbootstrapped
   database when intentionally replacing a leftover local output file from a
   prior interrupted attempt (cleanup of newly created or `--force`-replaced
   local files on connection or RPC failure is automatic, whereas a
   `bootstrap_conflict` response on an already-bootstrapped database returns
   exit status `0`).
   When an application manifest locks roles for multiple environments (such as
   `development`, `staging`, and `production` in `riffdb.application.json` and
   `riffdb.application.lock.json`), pass `--environment <ENV>` to
   `riffdb application deploy` to select the locked role whose declared
   `environment` matches `<ENV>` (or to verify that `--provision-role <ROLE>`
   is locked for `<ENV>`). Adding a new environment requires declaring a role
   for that environment in `riffdb.application.json` and running
   `riffdb application lock --write`:
   ```bash
   riffdb --database openfga application deploy \
     --environment staging \
     --replace-role-credential \
     --lifetime-seconds 2592000 \
     --credential-output /absolute/path/to/creds/role-openfga.credential
   ```
   During `--replace-role-credential`, the CLI proves the successor credential
   against the server and atomically replaces the watched `driverd.toml`
   credential path (`--credential-output` when supplied, or
   `<deployment-root>/driver-role.credential` by default) before revoking the
   predecessor capability, allowing a running `riffdb-driverd` host to reload
   the rotated credential without a restart.
4. **Running all four framework adapters through one `riffdb-driverd` host:**
   Every language runtime (Go, TypeScript, and Python) can connect through one
   shared `riffdb-driverd` Unix socket (`RIFFDB_DRIVER_SOCKET`). `riffdb application deploy --provision-role <ROLE>`
   (or `--environment <ENV>`) emits `operation-catalog.json`, `driverd.toml`, and `driver.env` under
   `.riffdb/deployments/<database>/<role>/` for Go, TypeScript, and Python
   applications alike:
   1. Provision each adapter database (`openfga`, `better-auth`, `mlflow`, `langgraph`) and merge their targets into a shared `driverd.toml` with `--driver-config-output` (note that the application manifest path is a positional argument, and `--deployment-dir` takes the base directory whose parent already exists—the CLI creates `<deployment-dir>/<database>` automatically):
      ```bash
      riffdb --database openfga application deploy /path/to/riffdb-openfga/riffdb.application.json --environment staging --deployment-dir /run/user/1000/riffdb/deployments --driver-config-output /run/user/1000/riffdb/driverd.toml --driver-socket /run/user/1000/riffdb/driver.sock
      riffdb --database better-auth application deploy /path/to/riffdb-better-auth/profiles/organization-security/riffdb.application.json --environment staging --deployment-dir /run/user/1000/riffdb/deployments --driver-config-output /run/user/1000/riffdb/driverd.toml --driver-socket /run/user/1000/riffdb/driver.sock
      riffdb --database mlflow application deploy /path/to/riffdb-mlflow/riffdb.application.json --environment staging --deployment-dir /run/user/1000/riffdb/deployments --driver-config-output /run/user/1000/riffdb/driverd.toml --driver-socket /run/user/1000/riffdb/driver.sock
      riffdb --database langgraph application deploy /path/to/riffdb-langgraph/riffdb.application.json --environment staging --deployment-dir /run/user/1000/riffdb/deployments --driver-config-output /run/user/1000/riffdb/driverd.toml --driver-socket /run/user/1000/riffdb/driver.sock
      ```
   2. Or combine their `[[applications]]` blocks into one `0600` `driverd.toml` listening on
      one Unix socket (for example `/run/user/1000/riffdb/driver.sock` in a `0700` directory)
      and start `riffdb-driverd /run/user/1000/riffdb/driverd.toml`. Each adapter's handshake
      automatically selects its matching `[[applications]]` entry by contract lineage and version:
      ```bash
      export RIFFDB_DRIVER_SOCKET=/run/user/1000/riffdb/driver.sock
      ```
      *(Python adapters continue to accept direct TLS via `RIFFDB_ENDPOINT` /
      `RIFFDB_CREDENTIAL_FILE` / `RIFFDB_TLS_TRUST_ROOT` as well, or Unix socket
      mode via `RIFFDB_DRIVER_SOCKET`.)*
   3. **Container UID (`SO_PEERCRED`) and remote identity hash when sharing a socket across containers:**
      - `riffdb-driverd` enforces Linux `SO_PEERCRED` on every accepted Unix socket connection: the client process must run as the **same effective UID** as the owner of the socket directory. If a container image defaults to `nonroot` (`65532`) while `riffdb-driverd` runs as your mapped host UID (`1000` with `userns_mode: "keep-id"`), set `user: "1000:100"` and `userns_mode: "keep-id"` on the client container as well.
      - `RIFFDB_REMOTE_HASH` in the generated `driver.env` is `sha256("<endpoint>|<tls_server_name>")` for the endpoint used during `riffdb application deploy`. If `riffdb application deploy` ran on the host against `https://127.0.0.1:17443` (`tls_server_name = "127.0.0.1"`) while `driverd.toml` inside a container network connects to `https://riffdbd:7443` (`tls_server_name = "riffdbd"`), either **omit `RIFFDB_REMOTE_HASH`** (`OPENFGA_RIFFDB_REMOTE_IDENTITY_HASH`) so `riffdb-driverd` resolves and returns its verified remote identity hash on handshake, or compute `printf 'https://riffdbd:7443|riffdbd' | sha256sum`.
6. **Resetting a database after a failed bootstrap attempt:**
   Bootstrap is strictly one-shot per database. Each `<db>.redb` file has
   durable companion files in the same directory (`<db>.redb.journal*`,
   `<db>.redb.format`, `<db>.redb.derived`, `<db>.redb.riffoutbox`) plus its
   `projections_root` directory. Deleting only `<db>.redb` (or deleting files
   while `riffdbd` is still running and holding open file descriptors) leaves
   companion files from the previous `DatabaseId` behind and causes startup
   recovery or `credential_invalid` failures. To reset one database cleanly
   while `riffdbd` is stopped, remove all of its companion paths together:
   ```bash
   rm -rf /var/lib/riffdb/data/openfga.redb* \
          /var/lib/riffdb/data/outbox/openfga \
          /var/lib/riffdb/data/projections/openfga
   ```
   When redeploying an application against a recreated database from the same
   working tree, either pass `--replace-role-credential` or remove the stale
   local deployment state under `<app>/.riffdb/deployments/` (or `--deployment-dir`).
7. **Host-side client configuration, Rootless Podman permissions, and host-loopback services:**
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
   > **Rootless Podman UID mapping (`keep-id`) and host-loopback services:**
   > - The release `riffdbd` container runs as UID/GID `65532:65532` and requires `0600` secret files owned by its effective UID. Under Rootless Podman, add `userns_mode: "keep-id:uid=65532,gid=65532"` so host-owned `0600` files bind-mounted into `riffdbd` appear owned by `65532:65532` inside the container.
   > - Rootless bridge networks (`host.containers.internal`) cannot reach host services that bind strictly to `127.0.0.1` (such as default local Ollama or loopback-published ports). For containers that must reach host-loopback endpoints, either run the container with `network_mode: host` (binding its own listener to `127.0.0.1:<port>`) or connect through a shared Unix socket volume (`riffdb-driverd`).

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
