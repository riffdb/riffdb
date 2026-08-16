# Driver and CLI Packages

RiffDB's alpha distribution tells the same application story in Rust, Go,
TypeScript, and Python. The language packages are delivery vessels around the
first-party Rust driver boundary; they do not reimplement TLS, credentials,
retry, cancellation, public-error validation, or read-after-commit semantics.

## Supported alpha platforms

The signed `0.1.0` package set supports:

- `x86_64-unknown-linux-gnu`
- `aarch64-unknown-linux-gnu`

Package resolution fails on other operating systems and architectures. This is
an explicit alpha limitation, not an invitation to run an unverified binary.

## Install the CLI and runtime

| Language | CLI | Application runtime |
|---|---|---|
| TypeScript | `npm install --save-dev @riffdb/cli@0.1.0` | `npm install @riffdb/client@0.1.0` |
| Python | `pip install riffdb-cli==0.1.0` | `pip install riffdb==0.1.0` |
| Go | signed binary installer | `go get riffdb.dev/application@v0.1.0` |
| Rust | `cargo install riffdb-cli --version 0.1.0` | `cargo add riffdb-client-rust@0.1.0` |

The Python `riffdb` package is an exact install alias for the normative
`riffdb-application==0.1.0` implementation package. Generated Python modules
therefore continue to import `riffdb_application`. Go and TypeScript generated
modules use their published runtimes; Rust uses `riffdb-client-rust`.

The signed native binary installer also places the one architecture-matching
`riffdb-application` wheel under its installation prefix at `public/python`.
Consequently, the installed native CLI can run `riffdb new --language python`
without `RIFFDB_APPLICATION_WHEEL`; the scaffold embeds that already verified
wheel and pins it in `uv.lock`. A source-tree CLI still requires the explicit
environment variable because it has no signed installation prefix.

## Local development registries

Adapter and application packages can be developed in separate repositories
before the RiffDB drivers are publicly released. WP-611 derives one immutable
development publication from the ordinary unsigned WP-601 distribution and
serves it through native loopback registries. This facility contains no
application-specific integration code: a Better Auth adapter, for example,
belongs in its own repository and consumes `@riffdb/client` like any other npm
package.

Build and serve one publication with a positive, never-reused build number:

```bash
mkdir -p "$HOME/tmp/riffdb-development-registry"
./scripts/build-driver-development-publication \
  --distribution /absolute/path/to/wp601-distribution \
  --registry-root "$HOME/tmp/riffdb-development-registry" \
  --build-number 42
./scripts/serve-driver-development-registries \
  --publication "$HOME/tmp/riffdb-development-registry/builds/42" \
  --state "$HOME/tmp/riffdb-development-registry/state-42"
```

The server binds only `127.0.0.1`. It exposes Verdaccio at port 4873 and the
Python Simple, Go proxy, and Cargo sparse views at port 4874. Verdaccio 6.9.2 is
pinned; set `RIFFDB_VERDACCIO_BIN` to an absolute executable installed by your
tooling, or allow the lifecycle command to obtain that exact version through
`npm exec`. Registry state and its local publishing credential remain under the
explicit `--state` directory. No user-global npm, pip, Go, or Cargo
configuration is changed.

Build 42 maps to these exact versions and project-local settings:

| Ecosystem | Version | External-repository configuration |
|---|---|---|
| npm | `0.1.0-dev.42` | `npm install @riffdb/client@0.1.0-dev.42 --registry http://127.0.0.1:4873/` |
| Python | `0.1.0.dev42` | `pip install riffdb==0.1.0.dev42 --index-url http://127.0.0.1:4874/python/simple/` |
| Go | `v0.1.0-dev.42` | `GOPROXY=http://127.0.0.1:4874/go,off go get riffdb.dev/application@v0.1.0-dev.42` |
| Rust | `0.1.0-dev.42` | Add registry `riffdb-dev` with index `sparse+http://127.0.0.1:4874/cargo/index/`, then depend on exact version `=0.1.0-dev.42`. |

Use repository-local configuration or command flags in consumers. Stop the
foreground lifecycle command to close both ports. The publication under
`builds/42` remains immutable and may be served again with the same state;
reusing build 42 for different bytes fails closed. Use a new build number after
any driver change.

Run the complete clean-consumer proof with:

```bash
./scripts/driver-development-registry-acceptance --all-ecosystems
```

The acceptance creates four directories outside the repository, installs or
resolves through the four native registry protocols, imports or compiles each
runtime, checks exact lock metadata, and rejects repository paths. Rust's
third-party dependencies remain explicitly sourced from crates.io in the
sparse metadata; all `riffdb-*` transitive crates remain on `riffdb-dev`.
Development publications are unsigned prereleases and cannot be passed back
into the production distribution or signing workflow.

For TypeScript consumers, a successful named query against a domain-empty
database reports `applicationHead: 0n`. The application frontier is a
non-negative u64; commit sequences, contract versions, and read-after-commit
inputs remain strictly positive. The clean npm-consumer cell exercises this
zero-frontier boundary through the installed package on every development
publication.

## One four-step workflow

With a RiffDB service running and an ordinary application credential available:

```bash
riffdb init inventory --generator typescript
riffdb push
riffdb generate
npm start
```

Replace the generator and final command for the selected language. `init`
creates only bounded RiffDB project files, `push` performs the existing exact
check/lock/install ceremony, and `generate` writes only the selected typed
binding. A changed compiler-owned identity still requires explicit acceptance;
an incompatible successor still routes to `riffdb migrate`.

The exact four-step contract lives in
`fixtures/driver/quickstart-v1.json` and is checked across all four languages.

## Cross-driver qualification

One generated conformance application is locked once and exercised from the
signed runtime packages in all four languages against the same remote TLS
service. Rust and Python carry native plan identities. TypeScript carries both
the plan and retained-driver operation identity. Go carries the exact
manifest, catalog, and operation identity through the retained Rust driver
host. These are different transport representations of the same compiler-owned
contract, not language-specific semantics.

The package matrix pins the bundle, plan, generated-artifact, manifest,
catalog, operation, input-schema, outcome, replay, freshness, and public-error
observations. It also pins the onboarding story at four steps. Run:

```bash
./scripts/check-driver-package-matrix
./scripts/driver-package-conformance-acceptance
```

The second command builds or accepts one signed distribution, installs every
runtime from package artifacts, generates from the installed CLI, and runs the
shared corpus. A source-tree runtime, handwritten wire adapter, numeric
compiler identity, divergent outcome, or extra onboarding step fails.

Sealed package-first evaluation bundles additionally include an exact
TypeScript compiler, Node type declarations, and their closed transitive type
dependencies. The installed `riffdb dev --run` workflow places that tooling on
the TypeScript child build path automatically; applications do not need an
ambient global `tsc` or a hand-authored `typeRoots` override.

The generated Rust `Cargo.lock` is package-shaped rather than workspace-shaped:
with the sealed bundle's `CARGO_HOME`, `cargo check --offline --locked` must not
rewrite it. Do not copy a machine-wide Cargo configuration into the project;
ambient compiler wrappers and linker settings are outside the signed package
environment.

For a local package-first repository that has no running service or ordinary
credential yet, use `riffdb dev --seed --run` (or `riffdb dev --run` without
seed inputs). The four-step `init` / `push` / `generate` / invoke workflow is
the remote-service path: `push` deliberately requires both the service and its
credential and does not bootstrap either one.

For a Go or TypeScript HTTP application, `--run` remains attached while the
application serves. Wait for the application's ready marker, exercise the
page, and then terminate the development process. A healthy serving process is
not expected to exit on its own.

## Verify a standalone binary bundle

Release bundles contain `checksums.sha256` and its detached SSH signature. The
release identity and allowed-signers file must come from a separately trusted
channel:

```bash
cd /absolute/path/to/distribution
ssh-keygen -Y verify \
  -f /protected/riffdb-allowed-signers \
  -I riffdb-release \
  -n riffdb-driver-distribution-v1 \
  -s checksums.sha256.sig < checksums.sha256
sha256sum --strict --check checksums.sha256

/absolute/path/to/distribution/install/riffdb-install \
  --distribution /absolute/path/to/distribution \
  --target x86_64-unknown-linux-gnu \
  --prefix "$HOME/.local"
```

Run both verification commands from the distribution root. They use only the
separately trusted signer file and operating-system tools, so verification does
not bootstrap through executable code from the untrusted bundle. The installer
then rechecks the selected binary against that signed inventory before
atomically publishing it. Registry credentials and signing keys are never
included in a distribution or receipt.

## Sealed and offline installations

Normal package-arrival projects use registry imports. Existing vendored
`@riffdb/application`, `riffdb-application`, `riffdb.dev/application`, and the
source-contained Rust SDK remain supported only for an explicitly sealed or
offline bundle. Offline mode does not weaken application identities or server
authorization.

## Alpha compatibility

These packages carry the durable-format policy described in
[Compatibility](../compatibility.md). Physical downgrade is unsupported. A
future incompatible storage epoch must refuse before mutation and use the
documented symbolic export/reimport path; it must never silently reset data.
