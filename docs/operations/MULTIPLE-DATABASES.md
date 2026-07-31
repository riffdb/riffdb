# Multiple Databases

One `riffdbd` process can host up to 32 independently durable databases. Each
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

## Add a database

Use the installer-owned database management workflow described in
[Installation](../installation.md). Adding a database is an offline,
configuration-validated operation: stop the user service, update the
configuration and protected key material through the installer command, then
restart and verify health.

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
