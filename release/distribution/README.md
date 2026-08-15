# RiffDB Driver Distribution

This directory defines the pre-1.0 four-ecosystem package release. The
distribution is a delivery mechanism for first-party RiffDB binaries and
runtimes; it does not move transport trust, retries, authorization, command
semantics, or durable-format decisions out of Rust.

## Supported alpha platforms

- `x86_64-unknown-linux-gnu`
- `aarch64-unknown-linux-gnu`

Other platforms fail closed at package resolution. The application daemon and
driver host remain Linux-only for alpha.

## Package identities

| Ecosystem | CLI | Runtime |
|---|---|---|
| npm | `@riffdb/cli` | `@riffdb/client` |
| PyPI | `riffdb-cli` | `riffdb` (exact alias for `riffdb-application`) |
| Go | verified native installer | `riffdb.dev/application` |
| Rust | `riffdb-cli` | `riffdb-client-rust` |

The existing `@riffdb/application`, `riffdb-application`, vendored Go module,
and source-contained Rust SDK remain the explicit sealed/offline compatibility
path. Normal project generation emits language bindings that pair with the
registry packages; it does not copy transport code into the application.

## Build, sign, and verify

Build one platform cell from a clean release revision:

```bash
scripts/build-driver-distributions \
  --output "$HOME/tmp/riffdb-distribution-linux-x64" \
  --binary-dir "$PWD/target/release" \
  --python-wheel "$PWD/dist/python/riffdb_application-0.1.0-cp313-abi3-manylinux_2_28_x86_64.whl" \
  --target x86_64-unknown-linux-gnu
```

The build contains no credential or signing key. A release operator signs the
complete checksum inventory separately:

```bash
scripts/sign-driver-distributions --key /protected/release-ed25519 \
  "$HOME/tmp/riffdb-distribution-linux-x64"
scripts/verify-driver-distributions \
  --allowed-signers /protected/riffdb-allowed-signers \
  --identity riffdb-release \
  "$HOME/tmp/riffdb-distribution-linux-x64"

scripts/driver-package-arrival \
  --distribution "$HOME/tmp/riffdb-distribution-linux-x64" \
  --allowed-signers /protected/riffdb-allowed-signers \
  --identity riffdb-release \
  --receipt "$HOME/tmp/riffdb-package-arrival-linux-x64.json"
```

Publication is forbidden until the signature, artifact manifest, durable-format
digest, package-arrival smoke, and both platform cells verify. Registry tokens
are release-operator inputs and are never written into an artifact or receipt.
The release workflow builds both native architectures and exercises a detached
ephemeral signature for CI; only a separately controlled release ceremony may
use the durable release identity or publish to public registries.

Rust publication follows `metadata/rust-publish-order-v1.txt`. Each `.crate`
contains the complete non-development first-party dependency closure and its
crate-owned compatibility assets. The package-arrival referee compiles the Rust
runtime and installs the CLI exclusively from those archives, installs npm and
Python from their local package mirrors, and resolves Go through the emitted
module proxy. A successful source-tree build is not a substitute.

## Four-step application story

After installing the ecosystem CLI and runtime package:

1. `riffdb init inventory --generator <language>`
2. `riffdb push`
3. `riffdb generate`
4. invoke the generated named operation with the language runtime

`fixtures/driver/quickstart-v1.json` is the canonical machine-readable story;
WP-604 pins its exact step count across all four language cells.
