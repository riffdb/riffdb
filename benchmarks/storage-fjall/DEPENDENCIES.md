# Fjall Dependency Audit

## Decision Scope

WP-075 pins `fjall = 3.1.8` with `default-features = false` in a nested,
non-production workspace. This audit supports that isolated comparison only.
It does not approve Fjall, its durable format, or its unsafe dependency surface
for production RiffDB.

The selected Fjall release is licensed `MIT OR Apache-2.0`, declares Rust
1.90.0, and is compiled here with the repository Rust 1.97.0 toolchain. Its
default `lz4` feature, optional `lz4_flex` dependency, `metrics`, `bytes_1`, and
internal white-box feature are not enabled.

## Resolved Graph

[`reports/dependency-inventory-v1.tsv`](reports/dependency-inventory-v1.tsv)
freezes the 40-package normal dependency closure selected for
`x86_64-unknown-linux-gnu`. The inventory records versions, licenses, declared
MSRVs, build scripts, proc macros, and a lexical unsafe-keyword line count over
each packaged `src` directory.

The active graph contains no `cc` crate, native library binding, LZ4 crate, or
`android_system_properties`. It does contain Rust build scripts for
`crossbeam-epoch`, `crossbeam-utils`, `getrandom`, `libc`,
`parking_lot_core`, `proc-macro2`, `quote`, and `rustix`, plus the
`enum_dispatch` proc macro.

The lockfile also records target-conditional alternatives such as `errno` and
`windows-sys`; they are not active in the frozen Linux normal graph. A lockfile
entry alone does not mean a crate is linked for the benchmark target.

The checked-in lockfile passes the repository advisory, license, source, and
ban policies. `cargo deny` reports one allowed duplicate-version warning:
`hashbrown` 0.14.5 is selected by `dashmap`, while 0.16.1 is selected by
`quick_cache` through `lsm-tree`. `cargo audit` reports no known vulnerabilities
for the lockfile.

## Unsafe Surface

All first-party Rust in this workspace is compiled with
`#![forbid(unsafe_code)]`. Cargo lint inheritance does not impose that policy
on third-party crates.

Fjall 3.1.8 declares `#![deny(unsafe_code)]` but locally expects two
always-configured unsafe buffer-builder blocks in `meta_keyspace.rs`. A third
unsafe block exists behind its disabled `lz4` feature. Its `lsm-tree` 3.1.8
dependency and several memory, hash-table, synchronization, and platform
dependencies also contain unsafe code. The lexical counts in the inventory are
disclosure signals, not a soundness audit: they include comments, attributes,
generated code, and conditionally compiled source.

This means Fjall does not satisfy RiffDB's first-party safe-Rust rule merely
because the comparison crate does. Any proposal to link Fjall into `riffdbd`
must return to human review with a narrower source audit, Miri/sanitizer
evidence where applicable, maintenance history, advisory status, and a durable
format/recovery evaluation.

## Reproduction

The active package and feature graphs can be reproduced with:

```bash
cargo tree --manifest-path benchmarks/storage-fjall/Cargo.toml \
  --locked -p fjall@3.1.8 -e normal \
  --target x86_64-unknown-linux-gnu --no-dedupe
cargo tree --manifest-path benchmarks/storage-fjall/Cargo.toml \
  --locked -p fjall@3.1.8 -e features \
  --target x86_64-unknown-linux-gnu
```

Dependency policy and advisory checks use the nested lockfile:

```bash
cargo deny --manifest-path benchmarks/storage-fjall/Cargo.toml \
  --workspace --locked --no-default-features \
  --target x86_64-unknown-linux-gnu --exclude-dev \
  check --config deny.toml
cargo audit --file benchmarks/storage-fjall/Cargo.lock
```

When comparing the checked-in inventory, use the normal Linux graph with Fjall
defaults disabled. `deny.toml` intentionally asks root-workspace CI to inspect
all features, so its graph can be broader than this configured comparison.
