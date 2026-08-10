# Your First Application

This walkthrough creates the small generated `Item` application, runs it on a
disposable RiffDB database, and shows where source intent becomes an exact
reviewed application.

## Prerequisites

Install the binaries with the [source checkout convenience
installer](../installation.md#source-checkout-convenience-install), or build the
workspace once:

```bash
cargo build --locked -p riffdb-cli -p riffdb-server
```

## Scaffold

From the directory that should contain the new project:

```bash
riffdb new item-desk
cd item-desk
```

The generated repository separates three things:

| File | Owner | Meaning |
|---|---|---|
| `riffdb.application.json` | Author | Symbolic paths, generation targets, and requested roles |
| `riffdb/contract.riff` and `riffdb/queries/*.riffq` | Author | State, commands, invariants, and reads |
| `riffdb.application.lock.json` and `generated/` | Compiler | Exact bundle, query plans, roles, and generated artifacts |

Inspect the contract before accepting a changed lock. The starter command
creates an item through a declared command; no generated client contains a raw
entity mutation.

## Check, lock, and generate

```bash
riffdb application check
riffdb application lock --write
riffdb application lock --check
riffdb application generate --locked
```

`check` is read-only. `lock --write` is the explicit point at which new
compiler-owned identities are accepted. `generate --locked` reproduces clients
only for the exact lock. Deployment performs all local source, role, query, and
generated-artifact preflight before its first remote mutation.

## Run a disposable server

```bash
riffdb dev --seed --run
```

The command starts an isolated loopback server, bootstraps through the normal
capability path, deploys the exact application, binds the requested role, runs
seed JSONL as ordinary idempotent commands, and starts the generated example.
It removes the disposable database and credentials when the process exits.

Use this mode while authoring. For a persistent user service, follow
[Installation](../installation.md) and deploy against the configured database
alias.

## Make a change

Add a field, command, or bounded query using the [Contract Authoring
Reference](../contracts/AUTHORING.md) and [RiffQL
Language](../riffql/LANGUAGE.md). Then repeat the check and lock sequence. A
compatible successor must increment the contract version and preserves the
exact active parent identity.

Pre-alpha evolution supports the documented additive subset. If a dogfood
schema needs an incompatible change, use the explicit offline reset procedure
in [Upgrade, Reset, and Removal](../upgrade-removal.md); never delete an active
database behind a running server.

## Choose a client

- [Rust](../sdks/RUST.md) uses the stable Rust application facade and generated
  operation module.
- [Go](../sdks/GO.md) uses generated facades over the retained Rust driver host.
- [TypeScript](TYPESCRIPT-APPLICATIONS.md) uses the checked generated package.
- [Python](../python-driver.md) uses the Rust-backed sync or async driver.
- [MCP](../mcp/agent-cookbook.md) exposes only tools authorized for the bound
  role and selected database.

All four preserve the same operation identities, values, closed outcomes,
idempotency behavior, and public errors.
