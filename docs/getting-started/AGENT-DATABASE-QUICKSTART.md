# Agent Database Quickstart

This page is generated from the same bounded source as the repository skill,
managed AGENTS section, and `llms.txt` distillation.

## Install the project rails

From a project containing `riffdb.toml`:

```bash
riffdb agent init
```

The command installs `.agents/skills/riffdb/SKILL.md`, appends one managed
section to root `AGENTS.md`, and merges `mcpServers.riffdb` into root
`.mcp.json`. It retains unrelated configuration, is byte-idempotent, and fails
before writing when a managed target conflicts. It never embeds a credential.

## Safe working loop

1. Edit the symbolic schema and bounded named queries.
2. Run riffdb push and review an exact proposed lock identity when one is reported.
3. Run riffdb generate to materialize the selected typed SDKs from that exact lock.
4. Call only generated operations and use explicit freshness or staleness surfaces.
5. Run riffdb status or riffdb diff to compare local and installed identities.

## Core rules

- Treat riffdb.toml and symbolic RiffDB source as the author-owned database interface.
- Use generated application operations; never hand-author protocol identities, storage access, or transaction machinery.
- Resolve uncertain mutations with the same idempotency key before taking another action.
- Use MCP discovery and riffdb://application/guide for the deployed, authorization-filtered application surface.

The application-specific MCP guide is `riffdb://application/guide`. Its content
is generated at read time from the current, authorization-filtered deployed
catalog, so it does not teach hidden operations or stale contract identities.

## POC limitations

The POC requires an already running RiffDB service and an ordinary authorized capability credential supplied through protected environment or configuration. Agent initialization never creates or embeds a credential.
