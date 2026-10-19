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
  its `storage_path`, starts its per-database workers (and idle-hibernation supervisor
  when `idle_unload_after` is configured), and registers the alias on the gRPC and
  hosted MCP routing tables.
- **Online deprovisioning:** Removing an existing `[databases.<alias>]` entry removes
  the alias from gRPC and hosted MCP routing, drains its per-database workers, writes
  `CleanCloseState::Clean`, and releases its `redb` file lock while leaving its durable
  file on disk.
- **Fail-closed validation:** If a modified configuration file is malformed, exceeds
  512 databases (`512 KiB` configuration document bound), rebind an active alias's
  `storage_path`, or mutates process-wide listener, TLS, authentication, or resource
  settings, the reload is rejected with a warning diagnostic and the previously active
  database inventory remains unchanged.

For installer-managed single-user deployments, you can also use the database management
workflow described in [Installation](../installation.md).

Do not copy a credential from one alias into another configuration. Create or
bind the application role against the target database so the credential,
audience, active contract, and named operations agree.

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
