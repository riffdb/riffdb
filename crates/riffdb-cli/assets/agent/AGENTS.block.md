<!-- riffdb-agent:start -->
## RiffDB project workflow

- Treat riffdb.toml and symbolic RiffDB source as the author-owned database interface.
- Use generated application operations; never hand-author protocol identities, storage access, or transaction machinery.
- Resolve uncertain mutations with the same idempotency key before taking another action.
- Use MCP discovery and riffdb://application/guide for the deployed, authorization-filtered application surface.

1. Edit the symbolic schema and bounded named queries.
2. Run riffdb push and review an exact proposed lock identity when one is reported.
3. Run riffdb generate to materialize the selected typed SDKs from that exact lock.
4. Call only generated operations and use explicit freshness or staleness surfaces.
5. Run riffdb status or riffdb diff to compare local and installed identities.

Rerun `riffdb agent init` to verify these exact generated rails.
<!-- riffdb-agent:end -->
