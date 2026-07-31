# Rust API

The supported Rust application surface is the `riffdb-client-rust` crate and the
compiler-generated module for an exact application lock.

[Open the generated `riffdb-client-rust` API documentation](../api/rust/riffdb_client_rust/index.html).

The handbook build runs Rustdoc with warnings denied and publishes only this
public client crate. Other workspace crates implement internal compiler,
service, policy, runtime, commit, and storage boundaries; their public Rust
items are not a compatibility promise for application authors.

Start with [Rust Applications](../sdks/RUST.md) before using the type-level
reference.
