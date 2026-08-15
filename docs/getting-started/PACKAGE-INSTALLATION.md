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
