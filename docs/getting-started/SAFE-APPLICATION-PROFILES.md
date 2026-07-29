# Safe application profiles

RiffDB deliberately does not have one development credential that can do
everything. Choose the smallest interaction profile:

```bash
# Default: exact generated commands and named queries only
cargo run -p riffdb-cli -- dev --role ticketdesk-application --seed

# Agent runtime: a separate exact named-operation role
cargo run -p riffdb-cli -- dev --role ticketdesk-agent --seed

# Low-level diagnosis: raw entity/index access, no application execution
cargo run -p riffdb-cli -- dev --role ticketdesk-kernel
```

Each invocation issues one protected credential for only the selected profile.
The kernel profile cannot seed or run the application acceptance workload. The
stable profile cannot check, explain, or execute ad-hoc source and cannot call
raw entity/index operations. The agent profile is also compiled from its
symbolic manifest allowlist; it does not gain ad-hoc RiffQL merely because its
principal is an agent.

## Why the split exists

An exact named query permission binds contract lineage, immutable query-module
hash, and query name. The compiler-derived plan, partition, fields, indexes,
result shape, and whole-request cost remain private execution requirements.
They cannot be reused to construct `GetEntity` or `ScanIndex`.

A kernel permission is the opposite: it authorizes one compatible low-level
operation but does not authorize RiffQL. This preserves the kernel protocol for
conformance and diagnosis without making it the application programming model.

MCP discovery is filtered from the same current capability. Application and
agent catalogs omit raw entity/index and ad-hoc-source tools. Kernel catalogs
do not silently acquire named commands or queries.

## Moving an existing development deployment

There is no automatic widening or reinterpretation:

1. Deploy the current contract and query module.
2. Issue a replacement application, agent, or kernel credential.
3. Verify the exact named application workload with the new credential.
4. Revoke the old combined credential.
5. Keep kernel credentials out of application configuration.

Do not add `ReadEntity` or `ScanIndex` to make a named query pass. A named-query
denial means the exact module/query authority, field visibility, partition
scope, current capability revision, or whole-query budget is wrong.

## Evidence and limits

The original independent TicketDesk build rated the storage-shaped application
path 4/10 and identified numeric IDs, manual masks, N+1 reads, and multi-snapshot
page composition. Its follow-up build stayed on symbolic commands and named
RiffQL; the one junction-to-entity collection gap it found was closed by the
bounded dependent-key batch work in WP-275. The checked TicketDesk application
now uses one request per page and an application-only client facade.

This is not a claim that RiffDB infers undeclared business rules or controls
external side effects. It proves supported, declared RiffDB semantics. Use
durable events/outbox intent when an external effect must be coupled to a
command, and declare every relationship or uniqueness rule that RiffDB must
enforce.

Run:

```bash
./scripts/safety-by-construction-acceptance
./scripts/riffdb-dev-acceptance
./scripts/check-ticketdesk-symbolic-boundary
```
