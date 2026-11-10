# Troubleshooting

Start with the process boundary, selected database, credential audience, and
active application identity. Do not work around a failure by widening a role or
editing a redb file.

## Service does not start

```bash
systemctl --user status riffdbd.service
journalctl --user -u riffdbd.service --since today
```

Common causes are a missing protected digest-key file, incorrect permissions,
duplicate database aliases, a redb path that is not writable by the service
user, or startup integrity validation rejecting incomplete durable evidence.
Correct configuration or restore from a verified backup; do not delete records
inside the database.

Startup automatically selects verified clean validation, bounded dirty recovery
or complete validation from its checked evidence. An abrupt stop can also require
storage-engine repair proportional to file size. After bounded readiness,
locator-dependent operations can return typed unavailable while the admitted
background directory construction finishes; bounded metadata reads remain
available. Do not remove or modify private metadata to select a different path.
See [Startup Integrity and Clean Restarts](STARTUP-INTEGRITY.md).

If stderr reports `RDB-FORMAT-0101`, keep the service stopped and inspect the
same immutable decision explicitly:

```bash
riffdb storage preflight --database-path /absolute/path/to/riffdb.redb
```

Follow only the returned action. A declared same-epoch transition uses
`riffdb storage upgrade` with the exact verified source backup; an unsupported
epoch requires the retained source binary and the documented symbolic
export/reimport ceremony. Never remove the marker or upgrade receipt, copy a
marker from another database, or look for a force/reset flag.

If a restart reports corruption only when a particular record is used, that is
the documented accessed-row integrity boundary, not evidence that validation was
disabled. Keep the service stopped if the error affects required authority or
active application state, retain the incident ID, and restore from a verified
backup when directed. To check the whole database, run `riffdb storage scrub`
and observe the printed operation with `riffdb backup operation`; it takes the
database offline for the pass. Do not edit lifecycle metadata or invoke
internal storage APIs as a substitute.

## Health reports `degraded` with a failed storage scrub

The background scrub found a row that fails validation. The `storage_scrub`
member is `failed` with an incident ID, and `authoritative_storage` is
degraded. Service continues, and any request that touches the damaged row fails
closed. Retain the incident ID, avoid the affected data where possible, and
restore from a verified backup when directed. A restart does not clear the
defect: the next background scrub starts at `pending` and finds it again. On a
V2 database a failure in retained history withdraws authoritative readiness
instead; the server then reports `not_ready`. See [Startup
Integrity](STARTUP-INTEGRITY.md#background-and-offline-scrubs).

## CLI reports `configuration_invalid: client configuration is invalid`

`riffdb` validates all client flags, environment variables, and `--config`
TOML fields before opening a network connection. Check these three rules:

1. **`--tls-trust-root` (and `client.tls_trust_root`, `--tls-client-cert`, `--tls-client-key`) must be an absolute, normalized path.**
   Passing a relative path such as `--tls-trust-root .runtime/secrets/ca.pem` fails closed; use `"$PWD/.runtime/secrets/ca.pem"`.
2. **`--tls-server-name` (`client.tls_server_name`) must match the host in `--endpoint` (`client.endpoint`).**
   RiffDB rejects any configuration where the endpoint authority host and `tls_server_name` differ (for example, `--endpoint https://127.0.0.1:7443 --tls-server-name riffdbd` or `--tls-server-name localhost`). Simply omit `--tls-server-name` / `tls_server_name` so `riffdb` derives the expected identity from the endpoint host automatically (make sure your server certificate includes `IP:127.0.0.1` in `subjectAltName` when connecting to `https://127.0.0.1:<port>`, and `DNS:riffdbd` when container peers connect to `https://riffdbd:<port>`).
3. **Unauthenticated `riffdb server health` must omit `--database` when probing liveness.**
   Without `--credential-file`, `riffdb server health` performs a process-liveness check (`status: "pre_bootstrap"`, `lifecycle: "process_alive"`, exit code `0`). Passing both `--credential-file` and `--database <alias>` performs an authenticated database-readiness check (`status: "ready"`, `readiness: true`).

## `authentication_failed` or `credential_invalid` after bootstrap or database reset

If calls fail with `authentication_failed` or `credential_invalid`, check these
three causes:

1. **Custom `[server] audience` vs. capability audiences:**
   Every capability lists the exact `[server] audience` values it is valid for.
   `riffdb capability bootstrap --operator` automatically includes the server's
   configured `audience` alongside `riffdb-cli` and `riffdb-mcp`, or you can
   pass `--audience <AUDIENCE>` explicitly. If you bootstrap from a hand-written
   JSON request file, ensure its `"audiences"` array contains your `riffdbd.toml`
   `[server] audience` value.
2. **Deleting only `<db>.redb` without removing companion files:**
   Bootstrap is strictly one-shot per database. Each `<db>.redb` owns sibling
   companion files in the same directory (`<db>.redb.journal*`,
   `<db>.redb.format`, `<db>.redb.derived`, `<db>.redb.riffoutbox`) plus its
   `projections_root` directory. Deleting only `<db>.redb` leaves companion
   files from the previous `DatabaseId` behind and causes startup recovery or
   `credential_invalid` failures. While `riffdbd` is stopped, remove
   `<db>.redb*`, its outbox directory, and its projections directory together.
   When redeploying an application against a recreated database from the same
   working tree, pass `--replace-role-credential` (or remove stale local state
   under `<app>/.riffdb/deployments/`).
3. **Expired role or operator credential:**
   Both `capability bootstrap --operator` and `application deploy --provision-role`
   default `--lifetime-seconds` to `2592000` (30 days, the maximum capability
   lifetime; earlier releases defaulted `application deploy` to `28800` / 8
   hours). To rotate an expiring or expired application role credential in
   place without re-bootstrapping the database, rerun `application deploy` with
   `--replace-role-credential`:
   ```bash
   riffdb --database <alias> application deploy \
     --provision-role <RoleName> \
     --replace-role-credential \
     --lifetime-seconds 2592000 \
     --credential-output /absolute/path/to/role.credential
   ```

## CLI connects to the wrong database

Inspect the client configuration and its `database` alias. Authenticated health
and active-contract responses include the selected alias and audience. An
unknown alias or a credential issued for another database fails closed.

See [Configuration](../configuration.md) and [Multiple
Databases](MULTIPLE-DATABASES.md).

## `application deploy` reports `database_readiness_unavailable` or `config reload rejected`

The CLI preserves `database_readiness_unavailable` and now lists every blocking
component/status reported by that same authenticated health response. Select
`--output json-v2` for structured sibling `diagnostics`; `--output json` retains
the v1 response. For example, projection `degraded` and outbox `unavailable`
appear together, with supported checks for both. Health does not report a root
cause for every component, so a suggested configuration check is not a claim
that a particular callback or destination is missing.

An alias mismatch or absent audience reports only the identity problem, without
revealing the mismatched database's components or identity values. Pre-bootstrap
health and absent/malformed evidence are distinct refusals. An authorization
failure remains the original typed authentication/authorization error. No extra
health RPC, role-binding workaround, or relaxed readiness gate is introduced.

When deploying to a containerized or multi-database `riffdbd` instance:

1. **Set `projections_root` and `[databases.<alias>.outbox]` per database.**
   In the container image, the working directory `/var/lib/riffdb` is read-only unless mounted; set `projections_root = "/var/lib/riffdb/data/projections/<alias>"` on each `[databases.<alias>]` block so projection workers can persist state inside the writable data volume. If the contract emits outbox events, configure `[databases.<alias>.outbox]` (`kind = "durable_local"`, `directory = "/var/lib/riffdb/data/outbox/<alias>"`, `quota_encoded_bytes = 268435456`) so the outbox subsystem is healthy.
2. **Restart `riffdbd` after changing an existing database's configuration.**
   Online config reload only admits adding or removing `[databases.<alias>]` entries. Editing an already-open database's settings logs `config reload rejected` (`kind=retained_database_changed`), and `docker compose up -d` does not restart a running container when only a bind-mounted config file changes. Run `docker compose restart riffdbd` after modifying an existing `[databases.<alias>]` block.
3. **Ensure the operator credential includes `read_health` (`Health`).**
   `riffdb application deploy --provision-role <Role>` queries authenticated database health to verify the active contract and authentication audience before binding the application role.

## MCP starts but tools are missing

```bash
riffdb-mcp doctor --config ~/.config/riffdb/mcp-myapp.toml
```

Then check:

1. The MCP host restarted or reloaded after configuration changed.
2. Tool names contain only lowercase letters, digits, and underscores.
3. The bridge selects the intended database alias.
4. The credential is the bound application role, not a bootstrap credential.
5. The active contract and query module match the generated application lock.

`tools/list` is capability-filtered. A missing operation can be absent, stale,
or unauthorized; the server deliberately does not disclose which to an
untrusted caller.

## Application check reports stale artifacts

Run the explicit sequence from the application repository:

```bash
riffdb application check
riffdb application lock --write
riffdb application generate --locked
riffdb application lock --check
```

Review the lock and generated diff. Do not edit generated files. A successor
must compile against the exact active parent; all local role and query preflight
must succeed before deployment performs its first remote mutation.

## A command response was interrupted

Do not invent a new idempotency key. Retry the identical command with its
original key or use the client facade's outcome resolution. A changed input
under the same key is rejected; a new key denotes a new logical command.

## Contract deployment fails

Read the structured source-spanned diagnostic. Additive evolution is bounded:
renames, removals, type changes, and key/ownership changes require a checked
`.riffm` contract migration (or the documented local reset path for disposable
development data). Back up the database before reset and stop the service
throughout the operation.

## Projection is behind

Inspect health and the reported projection frontier. A read-after-commit query
may wait only to its bounded deadline. Projection state is rebuildable; never
patch it to make a frontier appear current.

## Report a defect

Capture the RiffDB version, public error code, incident ID, database alias,
operation name, relevant configuration with secrets removed, and a minimal
reproduction. Never attach credential files, digest keys, raw protected logs,
or production database files.

## Find a configuration reload failure in server logs

Use `RDB-LOG-0101` (`configuration`) to find rejected inventory reloads. Its
`cause` identifies the refusal, `retained_previous` confirms that the active
configuration remains in use, and `fix` names the supported repair. A
`retained_database_changed` or `process_global_changed` refusal requires a
restart to apply the change. Repeated unchanged rejected files are deduplicated;
a successful reconciliation emits `RDB-LOG-0102`.

When the `outbox` health component reports `degraded`, check `RDB-LOG-0305`
(`outbox`, `"durable local outbox archive unavailable"`) in `riffdbd` logs. If
`directory` does not exist yet on startup, `riffdbd` creates it automatically
with mode `0700`. If the directory was pre-created on the host with broader
permissions (such as `0755`) or a mismatched owner UID (for example under
rootless Podman without `--userns=keep-id:uid=65532,gid=65532`), `RDB-LOG-0305`
reports `cause = "unsafe_directory_custody"` and
`fix = "require_owned_0700_directory_and_0600_archive_files"`. Fix the directory
mode (`chmod 0700 <dir>`) and ownership (`chown 65532:65532 <dir>` or use
`keep-id` user namespace mapping), or delete the empty pre-created directory and
let `riffdbd` create it with `0700`.

Use `[server.logging] format = "json"` for structured collection. Regular
provider accounting is debug-level; explicitly set the `projection` component
to `debug` when collecting endurance evidence. Accounting unavailability stays
visible as a warning. Raising verbosity cannot enable sensitive dependency logs.
See [server operational logging](../configuration.md#server-operational-logging).
