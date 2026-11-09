# RiffDB

**Documentation:** Start with the [RiffDB Handbook](docs/README.md) for the
application tutorial, language guides, MCP cookbook, operations, architecture,
and reference material. The repository builds the same handbook locally with
`./scripts/handbook build` and publishes it through GitHub Pages from `main`.

## Build an application

Start from a new directory (or an existing project) with `riffdb init --starter`:

```bash
mkdir task-tracker && cd task-tracker
riffdb init task-tracker --generator typescript --starter
riffdb generate
riffdb dev
```

Or scaffold a complete standalone repository with runnable starter code and seed
data:

```bash
riffdb new order-desk --language typescript
cd order-desk
riffdb dev --seed --run
```

This compiles your `.riff` contract and `.riffq` queries, locks their exact
cryptographic identities, generates typed Rust, TypeScript, Python, Go, and MCP
bindings offline, and boots an isolated ephemeral `riffdbd` server without
exposing numeric schema IDs, field masks, encoded keys, protobuf records, or
kernel RPCs to handwritten application code. Follow the step-by-step
[Your First Application tutorial](docs/getting-started/FIRST-APPLICATION.md),
[Database-Shaped Project Workflow](docs/getting-started/DATABASE-WORKFLOW.md),
[Agent Application Quickstart](docs/getting-started/AGENT-APPLICATION-QUICKSTART.md),
and [Contract Reference](docs/contracts/README.md). Python applications use the
[Rust-backed Python driver](docs/python-driver.md).

RiffDB is a standalone database written in Rust for contract-first operational
state. Applications mutate state through compiled, typed commands. The database
owns invariant evaluation, conflict domains, retry identity, durable outcomes,
events, provenance, outbox intent, and projection frontiers.

## The Race RiffDB Eliminates: Competing Reservations & Idempotent Retries

In a typical application, reserving inventory or spending from a shared budget
requires coordinating transaction isolation, `SELECT ... FOR UPDATE` lock order,
idempotency keys, and outbox rows in application code. In RiffDB, you declare
the conflict boundary and business rule once in your contract:

```riff
command ReserveBudget {
  input request_id: string<128>
  input org_id: uuid
  input envelope_id: uuid
  input amount: u64
  idempotency_key request_id

  mutate BudgetEnvelope(org_id, envelope_id) as env else EnvelopeMissing {}
  require SufficientFunds: env.remaining >= amount else InsufficientFunds {
    remaining: env.remaining
  }

  set env.remaining = env.remaining - amount checked_by SufficientFunds
  return Reserved { remaining: env.remaining }
}
```

When two callers race for the last `100` units on `(org_id, envelope_id)` and
one caller retries after a dropped connection, the engine enforces both
serialization and idempotent replay at the storage boundary:

```rust
// Caller A and Caller B race concurrently for the last 100 units:
let (a, b) = tokio::join!(
    client_a.reserve_budget(ReserveBudgetInput {
        request_id: "req-a-001".into(),
        org_id: org_id.clone(),
        envelope_id: envelope_id.clone(),
        amount: 100,
    }),
    client_b.reserve_budget(ReserveBudgetInput {
        request_id: "req-b-001".into(),
        org_id: org_id.clone(),
        envelope_id: envelope_id.clone(),
        amount: 100,
    }),
);
// Exactly one succeeds: Reserved { remaining: 0 } (commit_sequence: 42, replayed: false)
// The other gets a typed refusal: InsufficientFunds { remaining: 0 }

// Caller A retries after a network timeout with the same request_id:
let retry = client_a
    .reserve_budget(ReserveBudgetInput {
        request_id: "req-a-001".into(),
        org_id,
        envelope_id,
        amount: 100,
    })
    .await?;
assert!(retry.replayed);
assert_eq!(retry.commit_sequence, a.unwrap().commit_sequence); // Same commit 42, zero double-spend
```

To run the bundled competing-budget and crash-recovery demonstration from a
binary release bundle (`./demo --assert`) or from a source checkout:

```bash
./scripts/demo --assert
```

RiffDB uses Semantic Versioning (`0.2.1` current release). Default local
development is safe by construction: public endpoints bind loopback by default
(or verified direct TLS / protected Unix sockets when configured), and the
authorization model uses server-side capability records. Release verification
requires all three commands below to pass from a clean checkout:

```bash
./scripts/ci-all
./scripts/demo --assert
./scripts/release-poc --verify
```

The checked-in evidence manifest is a candidate map, not a signed result.
`scripts/demo --assert` writes the revision-specific result to
`target/wp200/demo-report.json` only after every selected assertion succeeds.

## Symbolic application quick start

Application and agent development is RiffQL-first. Start a disposable,
authorized TicketDesk environment with its contract, named query module,
development role, and 276-row command seed:

```bash
cargo run -p riffdb-cli -- dev --seed
```

Every list or detail page is one symbolic query request; mutations remain one
compiled command invocation. The kernel gRPC API remains supported, but normal
application code does not construct IDs, masks, keys, or protobuf field maps.
See [Symbolic applications](docs/getting-started/SYMBOLIC-APPLICATIONS.md) and
the [safe application profiles](docs/getting-started/SAFE-APPLICATION-PROFILES.md),
and the [TicketDesk acceptance report](docs/getting-started/TICKETDESK-ACCEPTANCE.md).

## Why RiffDB

RiffDB narrows the application mutation surface. First-party gRPC, MCP, CLI,
and Rust SDK calls all enter the same API-neutral application service,
authorization layer, deterministic command runtime, conflict manager, and
commit coordinator. The commit coordinator is the only component that assigns
commit sequences or atomically persists authoritative mutations, outcomes,
events, provenance, and commit records.

The budget comparison keeps a correct PostgreSQL implementation beside RiffDB
and also demonstrates four deliberately unsafe PostgreSQL variants. The
qualification matters: a pattern described as impossible in RiffDB is absent
from RiffDB's **supported application mutation surface**. It is not a claim
about malicious administrators, filesystem compromise, defects, or features
outside the documented scope. Unsafe PostgreSQL variants are correctness
evidence only and are excluded from every performance result.

See [the comparison workspace](examples/budget-comparison/README.md) and its
checked [safety report](examples/budget-comparison/fixtures/safety/report-v1.json).

## Architecture

```text
riffdb / Rust SDK / riffdb-mcp       hosted MCP
               |                         |
       public gRPC adapter          MCP HTTP adapter
               \                         /
                       authentication
                              |
                API-neutral application service
                              |
                         authorization
                              |
               deterministic runtime + conflict manager
                              |
                       commit coordinator
                              |
                  authoritative redb storage
                              |
                 outbox and projection workers
```

MCP is a policy-filtered transport, never a storage path. Projection state is
derived and rebuildable; entity state and the commit log are authoritative.
The deterministic runtime has no filesystem, network, operating-system clock,
process-global mutation, or untracked randomness.

## Quick Source Install

From the repository root on a Linux systemd host, install a private user
service and then explicitly bootstrap its database:

```bash
cargo riffdb install --user
cargo riffdb bootstrap --user --register-codex
```

Omit `--register-codex` when Codex MCP registration is not wanted. A
machine-wide service uses `--system` for both commands. Run both commands as
the intended operator user: never run Cargo with `sudo`. The system installer
builds as that user and invokes `sudo` itself for privileged destination
inspection, publication, account setup, and systemd operations. Treat the
mutable checkout helper as an interactive administrator tool, not as a command
to allowlist in `sudoers`.

Install and bootstrap are intentionally separate. Bootstrap creates the first
durable administrator, creates a generic MCP developer capability, and writes
private client configuration. It does not deploy an application contract. The
developer can validate and deploy a contract; exact application command and
data access is granted afterward through application roles or scoped
capabilities. Capabilities expire after at most 30 days and are not
automatically renewed. See [Installation](docs/installation.md) for paths,
`--no-start`, systemd user manager requirements, rerun limits, and advanced
bundle/manual procedures.

To install one daemon with multiple isolated databases, name them before the
first service start and bootstrap each alias independently:

```bash
cargo riffdb install --user --database ea --database orders
cargo riffdb bootstrap --user --database ea --register-codex
cargo riffdb bootstrap --user --database orders --register-codex
```

The generated MCP registrations are `riffdb_ea` and `riffdb_orders`; each
credential, client config, contract catalog, sequence, and durable file remains
bound to only its selected database.

## Manual Build

The workspace pins Rust 1.98.1. On Linux (`x86_64` or `aarch64`) or Apple
Silicon macOS (`aarch64-apple-darwin`) with rustup:

```bash
export PATH="${CARGO_HOME:-$HOME/.cargo}/bin:$PATH"
rustup show
cargo build --locked --release \
  -p riffdb-server --bin riffdbd \
  -p riffdb-cli --bin riffdb \
  -p riffdb-mcp-stdio --bin riffdb-mcp
```

Contributors running generated-artifact or handbook checks can install and
verify the exact auxiliary tools with:

```bash
./scripts/developer-tools install
./scripts/developer-tools check
```

The check reports a system `cargo` shadowing the rustup proxy and a Cargo
configuration that names an unavailable optional `rustc-wrapper`. It never
rewrites global Cargo configuration.

RiffDB ships three binaries:

- `riffdbd`: the database server and optional hosted loopback MCP endpoint.
- `riffdb`: the public gRPC administration and application CLI.
- `riffdb-mcp`: a stdio MCP bridge over the ordinary public gRPC client.

Comparison and safety runners under `examples/` are evidence tools, not product
binaries.

## Run

For a disposable source-tree smoke test that uses the production server binary,
closed stdin, and SIGTERM:

```bash
cargo build --locked -p riffdb-server --bin riffdbd
./scripts/release-systemd-smoke --binary target/debug/riffdbd
```

For an installed service, key provisioning, bootstrap, and a first contract
deployment, follow [Installation](docs/installation.md). The exact server,
client, hosted MCP, and stdio bridge settings are in
[Configuration](docs/configuration.md).

One daemon may host up to 32 isolated named databases. Select one on every
public client:

```bash
riffdb --database ea --config "$HOME/.config/riffdb/client.toml" server health
riffdb-mcp --database ea --config "$HOME/.config/riffdb/mcp.toml"
```

For an MCP host using project `.mcp.json`:

```json
{
  "mcpServers": {
    "riffdb_ea": {
      "command": "/home/you/.local/bin/riffdb-mcp",
      "args": [
        "--config",
        "/home/you/.config/riffdb/mcp.toml",
        "--database",
        "ea"
      ]
    }
  }
}
```

Each alias has its own contract catalog, credentials, command history, commit
sequence, projections, outbox, and storage. There are no cross-database
operations.

A verified binary bundle also contains `demo`. From its extracted root,
`./demo --assert` starts a disposable server, writes and queries the bundled
budget example through public binaries, and shuts down with SIGTERM. It uses no
source-tree Cargo command or test harness.

The complete acceptance demonstration is:

```bash
./scripts/demo --assert
```

It requires either `RIFFDB_BUDGET_POSTGRES_URL` pointing at a dedicated
PostgreSQL 18.4 database or Docker for an ephemeral digest-pinned PostgreSQL
container. The script is fail-closed: unavailable live evidence, a skipped
recovery matrix, a mismatched fixture, or an unmet engine-comparison dependency
is an error.

## Operations

For agent-facing value formats, retries, discovery, and command resources, see
the [MCP agent cookbook](docs/mcp/agent-cookbook.md).

- [Installation](docs/installation.md)
- [Production systemd deployment](docs/operations/PRODUCTION-DEPLOYMENT.md)
- [Configuration reference](docs/configuration.md)
- [Offline backup and restore](docs/backup-restore.md)
- [Upgrade and removal](docs/upgrade-removal.md)
- [Security posture and threat model](docs/security.md)
- [Compatibility](docs/compatibility.md)
- [Release verification](docs/release.md)

Destructive restore preserves `DatabaseId` and rotates the durable
`history_incarnation` fence while rewinding sequence allocators. Sequences in
the destroyed suffix can later be reused, and every locator, cursor,
idempotency assumption, and authorization fact from that suffix must be
discarded. Read [Offline Backup and Restore](docs/backup-restore.md) before
operating it.

## License

Copyright © 2026 Kevin O'Shea and O'Shea & Sons, LLC.

RiffDB is an open-source project of
[O'Shea & Sons, LLC](https://osheaandsons.com/).

RiffDB is licensed under either the MIT License or the Apache License, Version
2.0, at your option. See `COPYRIGHT`, `LICENSE-MIT`, and `LICENSE-APACHE`.
