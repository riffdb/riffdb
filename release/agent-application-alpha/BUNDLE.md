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

For TypeScript, `riffdb new --language typescript` materializes the exact
compiler, Node types, product runtime, lockfile, build scripts, and HTTP starter
from `$RIFFDB_TYPESCRIPT_RUNTIME`; no registry operation is required.

`bin/riffdb-builder-mcp --workspace <path>` exposes the same local check,
diagnostic, lock, and generation semantics as the CLI plus the bundled public
references. It has no credential and no runtime operation. These are
product-owned public packages and tools; using them is not handwritten
transport glue. Agents must not inspect or receive any other RiffDB source
tree.

An evaluator records ordinary value-free chronology with
`evaluation/event-schema.json`. A `first_write` or `first_page_read` event is
not a successful milestone by itself: it must have a matching record under
`evaluation/qualified-event-schema.json` backed by the generated application
lock and the exact identity returned by the runtime. Do not record credentials,
entity values, command inputs, query results, or fixture contents.
