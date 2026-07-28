# RiffDB

RiffDB is a standalone database written in Rust for contract-first operational
state. Applications mutate state through compiled, typed commands. The database
owns invariant evaluation, conflict domains, retry identity, durable outcomes,
events, provenance, outbox intent, and projection frontiers.

This repository is a proof of concept, not a production release. The POC is
local-development safe: public endpoints are loopback-only, transport is
cleartext, and the authorization model uses server-side capability records. A
POC-exit claim is valid only after all three commands below pass from a clean
checkout and a maintainer records the architecture-review decision:

```bash
./scripts/ci-all
./scripts/demo --assert
./scripts/release-poc --verify
```

The checked-in evidence manifest is a candidate map, not a signed result.
`scripts/demo --assert` writes the revision-specific result to
`target/wp200/demo-report.json` only after every selected assertion succeeds.

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
outside the POC. Unsafe PostgreSQL variants are correctness evidence only and
are excluded from every performance result.

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

## Build

The workspace pins Rust 1.97.0. On Linux with rustup:

```bash
rustup show
cargo build --locked --release \
  -p riffdb-server --bin riffdbd \
  -p riffdb-cli --bin riffdb \
  -p riffdb-mcp-stdio --bin riffdb-mcp
```

The POC ships exactly three binaries:

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

- [Installation and systemd](docs/installation.md)
- [Configuration reference](docs/configuration.md)
- [Offline backup and restore](docs/backup-restore.md)
- [Upgrade and removal](docs/upgrade-removal.md)
- [Security posture and threat model](docs/security.md)
- [Compatibility](docs/compatibility.md)
- [Known limitations](docs/known-limitations.md)
- [Release verification](docs/release.md)

Destructive restore has an important POC limitation: it preserves `DatabaseId`
but rewinds history without an incarnation fence. Sequences in the destroyed
suffix can later be reused, and every locator, cursor, idempotency assumption,
and authorization fact from that suffix must be discarded. Read
[Offline Backup and Restore](docs/backup-restore.md) before operating it.

## License

RiffDB is licensed under either the MIT License or the Apache License, Version
2.0, at your option. See `LICENSE-MIT` and `LICENSE-APACHE`.
