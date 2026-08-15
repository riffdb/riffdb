---
name: riffdb
description: Safely author and operate the RiffDB project in this repository.
---

# RiffDB project agent guide

## Safety rails

- Treat riffdb.toml and symbolic RiffDB source as the author-owned database interface.
- Use generated application operations; never hand-author protocol identities, storage access, or transaction machinery.
- Resolve uncertain mutations with the same idempotency key before taking another action.
- Use MCP discovery and riffdb://application/guide for the deployed, authorization-filtered application surface.

## Working loop

1. Edit the symbolic schema and bounded named queries.
2. Run riffdb push and review an exact proposed lock identity when one is reported.
3. Run riffdb generate to materialize the selected typed SDKs from that exact lock.
4. Call only generated operations and use explicit freshness or staleness surfaces.
5. Run riffdb status or riffdb diff to compare local and installed identities.

## Commands

```text
riffdb init <application> --generator <language>
riffdb agent init
riffdb push
riffdb generate
riffdb status
riffdb diff
```

The POC requires an already running RiffDB service and an ordinary authorized capability credential supplied through protected environment or configuration. Agent initialization never creates or embeds a credential.
