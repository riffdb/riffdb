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

## CLI connects to the wrong database

Inspect the client configuration and its `database` alias. Authenticated health
and active-contract responses include the selected alias and audience. An
unknown alias or a credential issued for another database fails closed.

See [Configuration](../configuration.md) and [Multiple
Databases](MULTIPLE-DATABASES.md).

## `application deploy` reports `database_readiness_unavailable` or `config reload rejected`

When deploying to a containerized or multi-database `riffdbd` instance:

1. **Set `projections_root` and `[databases.<alias>.outbox]` per database.**
   In the container image, the working directory `/var/lib/riffdb` is read-only unless mounted; set `projections_root = "/var/lib/riffdb/data/projections/<alias>"` on each `[databases.<alias>]` block so projection workers can persist state inside the writable data volume. If the contract emits outbox events, configure `[databases.<alias>.outbox]` (`kind = "durable_local"`, `directory = "/var/lib/riffdb/data/outbox/<alias>"`) so the outbox subsystem is healthy.
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

Use `[server.logging] format = "json"` for structured collection. Regular
provider accounting is debug-level; explicitly set the `projection` component
to `debug` when collecting endurance evidence. Accounting unavailability stays
visible as a warning. Raising verbosity cannot enable sensitive dependency logs.
See [server operational logging](../configuration.md#server-operational-logging).
