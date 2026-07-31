# ADR-0063: Bounded Multiple Databases Per Server Process

- **Status:** Accepted
- **Date:** 2026-07-30
- **Decision owners:** RiffDB maintainers
- **Related requirements:** `SYS-001`, `API-001`, `SEC-001`, `SEC-002`,
  `SEC-003`, `REC-003`, `MCP-010`, `MCP-011`
- **Related work packages:** `WP-380`, `WP-382`, `WP-383`, `WP-384`, `WP-385`
- **Amends:** ADR-0001, ADR-0004, ADR-0007, ADR-0008, ADR-0009,
  ADR-0024, ADR-0029, ADR-0040, ADR-0041, ADR-0044, ADR-0049, ADR-0050

## Context

The current standalone server composes one durable database, lifecycle,
application service, and public transport graph. Operators should be able to
host unrelated local applications in one `riffdbd` process without sharing
authority, catalog state, commit sequences, maintenance state, or storage.

Capability credentials are database-bound. Selecting a database by searching
every configured credential store for a presented bearer would leak identity,
make routing depend on secrets, and weaken fail-closed authorization.

The human maintainer explicitly approved the exact bounded multi-database
direction in the current Codex session on 2026-07-30.

## Decision

### Database identity and configuration

`riffdb-types` owns `DatabaseAlias`, an operator-visible routing identifier
whose canonical grammar is `[a-z][a-z0-9_-]{0,63}`. Alias comparison is exact.
There are at most 32 configured databases and aliases are processed in
ascending byte order.

The TOML form is:

```toml
[server]
grpc_listen = "127.0.0.1:7447"

[databases.ea]
path = "/home/user/.local/share/riffdb/ea.redb"
backup_root = "/home/user/.local/share/riffdb/backups/ea"
environment = "local"
```

Process-wide listener, audience, digest-key, hosted-MCP, and redb commit-profile
settings remain under `[server]`. Each database owns its path, backup root, and
environment. All configured database paths, backup roots, and protected key
paths must be pairwise lexically disjoint.

The existing single-database flags, environment variables, `[server].database`,
`[server].environment`, and `[maintenance].backup_root` form remains supported
as one implicit alias named `default`. The legacy form and `[databases]` must not
be mixed. Runtime create, drop, attach, and detach are not part of this
decision.

### Routing before authentication

Every public request is bound to exactly one configured alias before
credential lookup, authorization, lifecycle routing, or application-service
entry:

- gRPC carries exact ASCII metadata `riffdb-database`;
- hosted MCP carries exact ASCII HTTP header `riffdb-database`;
- stdio MCP configuration contains a fixed `database` alias; and
- CLI and SDK clients are constructed or configured with a fixed alias and add
  it to every request.

When exactly one database is configured, an absent selector resolves to that
database for compatibility. When multiple databases are configured, an absent,
duplicate, malformed, or unknown selector fails before credential lookup with
bounded, existence-blind public behavior. A credential from another database
produces the same generic authentication failure as any other invalid
credential. The implementation never scans databases by credential.

### Isolation and ownership

One process owns a bounded registry of independent per-database production
graphs. Each graph has its own:

- durable redb file and `DatabaseId`;
- lifecycle and server generation;
- active catalog and contract lineage;
- capability and revocation state;
- application and administration sequences;
- idempotency records;
- conflict manager and commit coordinator;
- entity state, commit log, events, provenance, outbox, projections, frontiers,
  and subscriptions; and
- maintenance state, backup root, recovery, and readiness.

There is no cross-database command, read, transaction, capability, query,
projection, outbox, maintenance operation, or locator resolution. The commit
coordinator remains the sole sequence owner inside each graph. One database's
maintenance state does not drain another database.

Process startup opens and structurally validates every configured database
before serving. Failure of any configured database blocks process readiness and
public serving. Shutdown stops admission and drains every graph. Aggregate
process resources and all per-database resources remain bounded.

### Public and durable compatibility

No durable record, storage key, canonical value, contract IR, bundle, plan hash,
or `DatabaseId` encoding changes. Existing single-database files open unchanged.
Every accepted `riffdb://` v1 locator retains its exact bytes and is interpreted
only within the already-selected database request or MCP session context.
Aliases are transport/configuration routing data and are not injected into
durable identities, idempotency identity, hashes, provenance, or locators.

The `riffdb-database` selector is a public protocol boundary. Its grammar,
missing-selector compatibility, duplication rejection, and error behavior are
covered by fixtures and conformance tests.

## Consequences

- One daemon and one systemd unit can host multiple isolated applications.
- Database selection is explicit and auditable without becoming authorization.
- A malformed configured database prevents partial process startup.
- Per-database graph composition is heavier than a shared coordinator but keeps
  semantic ownership simple and prevents accidental cross-database state.
- Dynamic database administration and cross-database operations remain
  deferred.

## Rejected Alternatives

- **Search credential stores for a matching bearer:** leaks and conflates
  routing with authentication.
- **One shared catalog, coordinator, or sequence:** violates database isolation
  and changes accepted sequence semantics.
- **Put aliases into v1 locators or durable keys:** creates an unnecessary
  incompatible durable/public change.
- **Permit partial startup:** makes process readiness ambiguous and can silently
  omit an operator-configured database.
- **Require one service instance per application forever:** creates avoidable
  operational overhead and does not prove bounded multi-database hosting.

## Testing

- Canonical `DatabaseAlias` grammar, bounds, ordering, and serde fixtures.
- Legacy single-database configuration compatibility and mixed-form rejection.
- Pairwise path-overlap and 33-database rejection.
- Missing, duplicate, malformed, unknown, and cross-database-credential
  selector conformance for gRPC and hosted MCP.
- Stdio, CLI, and SDK selector propagation tests.
- Two active databases with different contracts, credentials, sequences,
  idempotency keys, data, projections, maintenance state, and restarts.
- Crash and restart evidence showing that committing or recovering one database
  cannot mutate the other.
- Exact compatibility fixtures for all existing durable bytes and v1 locators.

## Requirements and Work Packages

- **Requirements:** `MDB-001` through `MDB-010`
- **Defines or blocks:** `WP-380`, `WP-382`, `WP-383`, `WP-384`
- **Final evidence:** `WP-385`
