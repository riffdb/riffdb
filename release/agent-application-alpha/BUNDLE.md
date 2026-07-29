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

For TypeScript, install `@riffdb/application` from
`$RIFFDB_TYPESCRIPT_RUNTIME`. These are product-owned public packages; using
them is not handwritten transport glue. Agents must not inspect or receive any
other RiffDB source tree.
