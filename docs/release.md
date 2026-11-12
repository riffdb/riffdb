# Release Verification

The release verifier assembles evidence; it does not turn a skipped test or missing dependency
into a pass.

Run from a clean checkout:

```bash
./scripts/ci-all
./scripts/demo --assert
./scripts/release-poc --verify
```

Before announcement, run the [core artifact conformance
checks](operations/ANNOUNCEMENT-READINESS.md) against the archive, image, and
signed first-party driver distribution from the same clean revision. Framework
integration tests are owned by their adapter repositories or an external
playground and consume the core behavior contract. Their evidence completes
the five-track rehearsal; the main RiffDB build does not import or test those
adapter packages. Anonymous asset downloads and image pulls remain a separate
gate after publication.

Before changing any release-significant identity, classify it and update the
machine-readable topology described in [Versioning and
Retirement](versioning-and-retirement.md). Release review runs
`./scripts/check-version-topology`; it rejects source-identity drift, incomplete
reader/writer or lifecycle metadata, and disagreement with the durable-format
manifest. A retirement also requires the prior-notice, frozen-fixture,
typed-refusal, operator-action, and permanent-reservation evidence in that
policy. The current baseline retires no decoder.

`scripts/demo --assert` verifies the candidate requirement map and writes a
revision-specific `demo_verified` report to `target/wp200/demo-report.json`
only after its live and process-level suites pass. `POC-010` remains
`requires_release_verification` in that intermediate report. The demo consumes
the performance comparison report with the exact
`supported_application_mutation_surface` qualifier and rejects any attempt to
mark the unsafe variants benchmark eligible.

The same evidence check pins the checked crash-recovery report hash, command list, case
order, coverage counts, and production-gap inventory. Any unresolved
`production-gap` row rejects `POC-009`; a partial executed-case report is never
treated as complete crash coverage.

`scripts/release-poc --verify`:

1. rejects a dirty checkout or missing repository ownership or license notice;
2. reruns `./scripts/demo --assert` to replace all ignored target evidence at
   the exact clean revision, then runs `./scripts/ci-all` and validates checked
   evidence, systemd units, closed-stdin/SIGTERM behavior, authenticated MCP
   bridge framing, generated artifacts, and dependency policy;
3. builds exactly `riffdbd`, `riffdb`, `riffdb-driverd`, and `riffdb-mcp` with the Git revision
   embedded in two independent Cargo target directories and compares every
   binary byte-for-byte, then bootstraps the release server and verifies its
   reported semantic version, Git revision, Rust version, feature set, storage
   format, contract IR version, and MCP protocol baseline;
4. produces deterministic CycloneDX 1.5 SBOMs and dependency trees from the
   product, nested budget-comparison, and isolated Fjall-comparison lockfiles;
5. assembles documentation, the installed-binary demo, the tracked budget
   comparison sources, service units, release notes, known limitations, and
   the final `locally_verified` requirement report with only `POC-010` promoted
   from the demo report;
6. installs the staged bundle into a private root, compares every required
   asset and mode, checks the extracted layout, and runs the installed demo;
7. writes and verifies SHA-256 checksums; and
8. creates the release tarball twice, compares it byte-for-byte, and only then
   atomically publishes the private staging directory as `target/wp200/release`.

No `locally_verified` report is published under `target/wp200/release/` after a
failed run. The private staging directory is removed on normal failure. Outputs
are local artifacts until a maintainer signs the release verification
requirements. The source and bundle are offered under `MIT OR Apache-2.0` and
include the canonical ownership notice and both license texts.

Copyright © 2026 Kevin O'Shea and O'Shea & Sons, LLC.

The systemd check combines `systemd-analyze verify`, exact hardening-directive
checks, direct daemon lifecycle execution, and the bridge's newline-delimited
socket framing. An unprivileged release job does not launch the units in the
host manager; installation verification on the target host remains required
for manager-enforced sandboxing and Unix-socket ownership.

The CycloneDX files conservatively inventory each locked resolved workspace.
The adjacent dependency text files narrow each normal/build graph. The budget
comparison source in the binary bundle retains repository-relative development
dependencies; run it from the matching full source revision rather than
treating it as a standalone installed product. The Fjall workspace remains an
engine-review harness and is not linked into or shipped as a product binary.

## Human Sign-Off

The final review must record:

- source revision and clean-tree proof;
- the three acceptance command results;
- the release gate result report;
- benchmark hardware/methodology and raw artifacts;
- dependency, advisory, license, and SBOM review;
- backup/restore rewind acknowledgment;
- security posture and known limitations; and
- one decision: proceed with the release, revise and repeat a gate, or stop.

## Full Application Release Gate

The final installed gate is intentionally a coordinator, not another semantic
implementation:

```bash
./scripts/deployable-alpha-acceptance \
  --all-domains --all-languages --remote
```

The passing receipt at
`release/evidence/deployable-application-alpha-v1.json` binds the exact Git
revision and command inventory plus SHA-256 references to the durable-format,
comparator performance, bound 72-hour endurance, six-run agent campaign, and every
adapter conformance/export/disaster-recovery artifact used by the run. A
generated artifact that is absent, empty, oversized, or a symlink prevents the
receipt from being published even when every phase exited successfully.

It preflights the entire closed phase inventory before running anything, then
executes application-binding and durable-format checks, retained performance
verification, Compose and Kubernetes deployment checks, four-language driver
and adapter conformance, bulk/query/workflow/row-policy behavior, symbolic
export/reimport, destructive recovery, retained 72-hour endurance evidence,
installed bootstrap, and the sealed independent-agent evaluation. Missing
component scripts or evidence fail before an expensive partial gate.

Each phase result stores only its symbolic name, exit status, elapsed time,
and a SHA-256 of the reviewed argument array; the outer passing receipt also
stores the exact evidence references described above. A failed run remains under
`target/deployable-alpha-gate/`; only a complete pass publishes
`release/evidence/deployable-application-alpha-v1.json`. The coordinator cannot
waive a phase, shorten endurance, substitute a diagnostic benchmark, or print
command arguments into the receipt.

The retained comparator corpus is verified separately with:

```bash
./scripts/check-alpha-performance-evidence --verify
```

That verifier checks both content hashes and the semantic 90-second,
three-repetition, idle-host, safe-app durability, stability, correctness, and
concurrency matrix. It does not rerun the benchmark during routine release
verification.

## Signed Tag Release Automation and Consumer Verification

Tagging a release revision (`v<major>.<minor>.<patch>`) triggers
`.github/workflows/release.yml`. Before building or publishing any artifact, the
`version-gate` job requires a clean checkout and runs:

```bash
./scripts/check-version-topology --release-tag v0.2.4
./scripts/check-durable-format-manifest
./scripts/check-driver-distribution
./scripts/check-driver-conformance-manifests
./scripts/check-generated
```

A tag whose semantic version, transition class (`VER-004`), topology record
(`VER-001`), or durable-format manifest (`AFC-001`) disagrees with the source
tree fails closed before any binary bundle, container image, or driver package
is built or published.

After the version gate passes, the release workflow:

1. executes `./scripts/release-poc --verify` on the clean revision;
2. builds and verifies the `x86_64` and `aarch64` four-ecosystem driver
   distributions (`Rust`, `Go`, `TypeScript`, and `Python`), embedding each
   language's exact `DRV-014` `conformance-manifest-v1.json` in the published
   package archive as well as under `conformance/<language>-v1.json`;
3. builds the container image from the tracked `release/container/Dockerfile`
   using the verified release binaries, runs `./scripts/check-container-startup`,
   and pushes the digest-pinned image (`:v<version>`, `:<version>`, and `:latest`
   when the tag is the highest SemVer release tag) to `ghcr.io`;
4. runs `./scripts/publish-release-artifacts assemble` and
   `./scripts/publish-release-artifacts verify` to bind the release bundle,
   packaged Helm chart (`riffdb-<version>.tgz`), consumer verification receipt
   (`consumer-verification-v1.json`), CycloneDX SBOM (`sbom.cdx.json`),
   durable-format manifest (`durable-format-manifest-v1.json`), version topology
   (`version-topology-v1.json`), container image digest, and four `DRV-014`
   conformance manifests into `release-provenance-v1.json` and `SHA256SUMS`,
   signed with `ssh-keygen -Y sign` under namespace `riffdb-release-v1` and
   attested with SLSA build provenance; and
5. runs `./scripts/publish-release-artifacts publish-registries` to verify
   cross-architecture package determinism and publish the release to all six
   target registries:
   - **GitHub Releases**: immutable binary bundle, driver distribution bundles,
     Helm chart archive, consumer verification receipt, SBOM, provenance,
     `SHA256SUMS` + `.sig`, allowed signers, Go proxy assets, and `DRV-014`
     conformance manifests;
   - **GHCR**: container image tags and OCI Helm chart
     (`oci://ghcr.io/riffdb/riffdb/charts`);
   - **npm**: `@riffdb/cli-linux-x64`, `@riffdb/cli-linux-arm64`, `@riffdb/cli`,
     and `@riffdb/client` (authenticated via `NPM_TOKEN` or GitHub Actions OIDC
     Trusted Publishing with `--provenance`);
   - **PyPI**: `riffdb-application` (`x86_64` and `aarch64` `abi3` wheels plus
     source distribution), `riffdb`, and `riffdb-cli` (authenticated via
     `PYPI_TOKEN` or GitHub Actions OIDC Trusted Publishing);
   - **crates.io**: the topological Rust SDK runtime closure (`riffdb-types`,
     `riffdb-errors`, `riffdb-config`, `riffdb-contract-ir`, `riffdb-proto`,
     `riffdb-client-rust`, and `riffdb-application`); and
   - **Go**: annotated submodule tag `clients/go/runtime/v<version>` plus Go
     module proxy assets (`v<version>.{info,mod,zip}`) for
     `riffdb.dev/application`.

Version bumps across all workspace, Helm, distribution, Python, TypeScript, and
editor manifests are synchronized and checked with:

```bash
./scripts/bump-version --check
./scripts/bump-version --to 0.2.4 --sync
```

The public documentation site (`docs.riffdb.com`, built by
`.github/workflows/handbook.yml`) runs verification builds on `main`, pull
requests, and weekly schedules, and deploys GitHub Pages on `main`, manual
`workflow_dispatch`, and when a release tag (`v<major>.<minor>.<patch>`)
matching the highest SemVer tag is pushed.

A consumer verifies a downloaded release publication using only an
out-of-band trusted `riffdb-release-allowed-signers` file and standard
operating-system tools before extracting or executing any binary:

```bash
ssh-keygen -Y verify \
  -f /protected/riffdb-release-allowed-signers \
  -I riffdb-release \
  -n riffdb-release-v1 \
  -s SHA256SUMS.sig < SHA256SUMS
sha256sum --strict --check SHA256SUMS
```

Once `SHA256SUMS` is verified, `release-provenance-v1.json` pins the exact Git
revision, bundle archive digest, CycloneDX SBOM digest, durable-format manifest
digest (`alpha_epoch`, `writer`, `record_registry_digest`, and
`compatibility_fixture_digest`), container image `@sha256:...` digest, and the
four `DRV-014` driver conformance manifest digests.
