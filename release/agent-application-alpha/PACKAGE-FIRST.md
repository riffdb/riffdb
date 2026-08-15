# Campaign-03 package-first sealed environment

This bundle is derived from one verified signed RiffDB distribution. The
application repository starts empty. Do not use a repository checkout or a
vendored compatibility runtime.

The evaluator places `<bundle>/bin` on `PATH`. That directory contains the
verified package CLI plus evaluator-owned `riffdbd`, `riffdb-driverd`, MCP, and
development orchestration binaries. The signed distribution and local package
mirrors are under `<bundle>/packages/distribution`.

The four-step project workflow below targets an already running remote RiffDB
service and requires an ordinary application credential:

1. `riffdb init <name> --generator <language>`
2. author the symbolic contract, named queries, and role, then `riffdb push`
3. `riffdb generate`
4. install the selected runtime package and invoke the generated operation

The sealed Campaign-03 workspace intentionally starts with neither a remote
service nor a credential. For that local evaluation, do not probe `riffdb
push`: run `riffdb new <name> --language <language> --directory .`, replace the
sample domain, install the selected runtime, and use `riffdb dev --seed --run`
(or `riffdb dev --run` without seed inputs). Unlike the remote-oriented `init`
shape, `new` supplies the complete current application source, role arrays,
runtime manifest, and generated-client scaffold. The development command
performs the local lock, generation, deployment, scoped role binding, and
application launch against its disposable development service. It is the
package-first local path, not a fallback or rescue.

Application source is closed JSON. Preserve the initialized generation members
and the required role arrays even when they are empty. Source V6 additionally
requires `row_policies: []` on every role. Contract expressions reference enum
values as `EnumName.VariantName`, never as an unqualified variant.

The common authoring shape is deliberately closed:

- command clauses are ordered as inputs/service values, idempotency, all
  `read`/`mutate`/`create` bindings, requirements, effects, then `return`;
- every declared reference written by a create or mutation needs its own
  dominating exact target read, even when another target in the same aggregate
  was already read;
- every bounded `many` query needs a declared index matching its equality
  prefix and ordering; optional index fields use `presence(field)` rather than
  an ordinary key component; and
- multiple RiffQL outcomes are pipe-delimited, for example
  `outcomes Found | NotFound | IntegrityFailure`.

The bundled contract authoring and RiffQL references contain complete checked
examples. Follow a compiler diagnostic at its source span; do not replace a
rejected proof with application-side preflight logic.

Offline runtime installation sources are:

- Go: `GOPROXY=file://<bundle>/packages/distribution/go-proxy GOSUMDB=off`
- Python: `pip --no-index --find-links <bundle>/packages/distribution/pypi`
- Rust: set `CARGO_HOME=<bundle>/.cargo`; the signed `.crate` packages are the
  `vendored-sources` directory. Do not copy an ambient Cargo configuration
  into the application. The generated `Cargo.lock` is already package-shaped
  and must pass `cargo check --offline --locked` unchanged.
- TypeScript: install the exact `riffdb-client-0.1.0.tgz` under
  `<bundle>/packages/distribution/npm`; the evaluator compiler and its exact
  target platform binary are available at `<bundle>/tooling/typescript/bin/tsc`.
  `riffdb dev --run` automatically places this compiler and its closed Node
  type dependency set on the child build path

The complete distribution's signed inventory was verified before the bundle
was sealed. The evaluator receives an intentionally source-pruned runtime
subset, so that subset is not presented as the signed whole. Verify
`<bundle>/checksums.sha256`, then verify the selected files with:

```bash
(cd <bundle>/packages/distribution && \
  sha256sum --strict --check ../runtime-subset-checksums.sha256)
```

`packages/qualification/receipt.json` records the complete signed inventory
digest, signer identity, and runtime-subset inventory digest. `bundle.json`
binds both digests. The original signed whole inventory and signature are
retained under `packages/qualification/` as evidence of pre-pruning
qualification; they do not claim that omitted implementation archives are in
the evaluator subset.

For Go and TypeScript web applications, `riffdb dev --seed --run` owns the
local RiffDB and driver processes and then launches the application. A web
runner is expected to remain alive. Wait for its public ready marker, exercise
the HTTP page, then terminate the development command; remaining alive while
serving is not a runtime failure. Invoke the bundle's `riffdb`/`riffdb-dev`
path so its bundled TypeScript compiler and driver host remain on the child
path. Use an application-owned writable cache when the host package-manager
cache is read-only.

The sealed Cargo home contains only its offline source configuration and
immutable vendored sources. Cargo's mutable global, package, and registry
cache markers are removed after package qualification and are rejected by the
bundle verifier, so rebuilding from the same revision and signed distribution
produces the same outer inventory.

Package installation is setup, not an identity-change ceremony. Count a
ceremony only when RiffDB requires explicit review of changed compiler-owned
application identity. Count a rescue when an operator or evaluator supplies
product guidance or manually repairs product-generated state after the run
starts.
