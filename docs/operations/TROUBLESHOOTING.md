# Troubleshooting

Start with the process boundary, selected database, credential audience, and
active application identity. Do not work around a failure by widening a role or
editing a redb file.

## Service does not start

```bash
systemctl --user status riffdb.service
journalctl --user -u riffdb.service --since today
```

Common causes are a missing protected digest-key file, incorrect permissions,
duplicate database aliases, a redb path that is not writable by the service
user, or startup integrity validation rejecting incomplete durable evidence.
Correct configuration or restore from a verified backup; do not delete records
inside the database.

## CLI connects to the wrong database

Inspect the client configuration and its `database` alias. Authenticated health
and active-contract responses include the selected alias and audience. An
unknown alias or a credential issued for another database fails closed.

See [Configuration](../configuration.md) and [Multiple
Databases](MULTIPLE-DATABASES.md).

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
renames, removals, type changes, and other incompatible edits require the
documented pre-alpha reset path for disposable data. Back up the database before
reset and stop the service throughout the operation.

## Projection is behind

Inspect health and the reported projection frontier. A read-after-commit query
may wait only to its bounded deadline. Projection state is rebuildable; never
patch it to make a frontier appear current.

## Report a defect

Capture the RiffDB version, public error code, incident ID, database alias,
operation name, relevant configuration with secrets removed, and a minimal
reproduction. Never attach credential files, digest keys, raw protected logs,
or production database files.
