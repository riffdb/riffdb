# Sealed evaluator environment

This bundle contains public binaries, public documentation, generated-client
runtimes, evaluation briefs, and schemas. It intentionally excludes RiffDB
server/runtime/storage source and all TicketDesk source.

The evaluator sets:

```text
PATH=<bundle>/bin:$PATH
CARGO_HOME=<bundle>/.cargo
RIFFDB_TYPESCRIPT_RUNTIME=<bundle>/public/typescript
RIFFDB_RUST_SDK=<bundle>/public/rust-sdk/crates/riffdb-client-rust
```

`CARGO_HOME` contains only a Cargo configuration: all third-party Rust
dependencies are checksummed under `public/rust-sdk/vendor`, the generated
application client is patched to the bundled public SDK, and network access is
disabled. The bundle self-test creates a fresh application and completes
`riffdb dev --seed --acceptance` through this offline path.

A freshly scaffolded Rust repository starts with the public registry-shaped
`Cargo.lock`. On its first `riffdb dev` run, this predecessor source-SDK bundle
reconciles that lock once, offline, against the exact bundled source patch and
then executes Cargo with `--locked`. Later runs require no dependency-resolution
step. Package-first bundles do not perform this reconciliation: their generated
lock is already package-shaped and remains byte-identical.

For TypeScript, `riffdb new --language typescript` materializes the exact
compiler, Node types, product runtime, lockfile, build scripts, and HTTP starter
from `$RIFFDB_TYPESCRIPT_RUNTIME`; no registry operation is required. The
bundle also carries `riffdb-driverd` and a development-only loopback TLS
identity. `riffdb dev --seed --run` retains the credential and verified remote
transport in that Rust host and gives the TypeScript process only its protected
socket and public exact-handshake identity.

For Go, `riffdb new --language go` materializes the generated application and
the first-party `riffdb.dev/application` runtime as a local module replacement.
It uses only the Go standard library and the bundled Rust driver host, so the
scaffold builds and runs without a module proxy or language-owned TLS stack.
Go 1.24 or newer is required.

For Python, `riffdb new --language python` copies the architecture-matching
`cp313-abi3` wheel from `public/python`, locks its SHA-256 in `uv.lock`, and
creates a source-layout application that completes `uv sync --locked` without
a registry lookup. CPython 3.13 or 3.14 is required.

`bin/riffdb-builder-mcp --workspace <path>` exposes the same local check,
diagnostic, lock, and generation semantics as the CLI plus the bundled public
references. It has no credential and no runtime operation. These are
product-owned public packages and tools; using them is not handwritten
transport glue. Agents must not inspect or receive any other RiffDB source
tree.

For evaluation, handwritten RiffDB glue means application-authored transport
or RPC wrappers, parameter/result maps, wire encoders/decoders, capability or
grant construction, or response decoding that replaces generated operations.
Normal application code—HTTP handlers, domain decisions, generated-client
calls, typed outcome handling, rendering, tests, and value-free identity
evidence—is not glue. A boundary-clean application with none of the former
reports zero even though it necessarily contains ordinary application code.

An evaluator records ordinary value-free chronology with
`evaluation/event-schema.json`. A `first_write` or `first_page_read` event is
not a successful milestone by itself: it must have a matching record under
`evaluation/qualified-event-schema.json` backed by the generated application
lock and the exact identity returned by the runtime. Do not record credentials,
entity values, command inputs, query results, or fixture contents.
