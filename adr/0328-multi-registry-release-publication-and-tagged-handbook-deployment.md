---
adr: "0328"
title: Multi-registry release publication and tagged handbook deployment
status: accepted
tier: surface
date: 2026-11-04
accepted: 2026-11-04
acceptance: "User approved multi-registry release publication, version synchronization tooling, and tagged handbook deployment plan"
requires: ["0124", "0156", "0200"]
amends: []
supersedes: []
requirements: ["AFC-001", "DRV-014", "VER-001", "VER-004"]
packages: ["WP-579"]
obligations:
  - id: OBL-0328-1
    package: WP-579
    description: "scripts/check-version-topology and scripts/bump-version synchronize and check the canonical release version from release/version-topology-v1.json across workspace, Helm, distribution, Python, TypeScript, and editor manifests and verify durable_format.sha256"
    proof: scripts/check-version-topology
  - id: OBL-0328-2
    package: WP-579
    description: "scripts/publish-release-artifacts packages the Helm chart and consumer-verification-v1.json into the signed release publication and publishes verified artifacts across GitHub Releases, GHCR container and OCI Helm chart, npm, PyPI, crates.io, and Go module tags"
    proof: scripts/publish-release-artifacts
review_triggers:
  - "A release or packaging script hardcodes a release version instead of reading release/version-topology-v1.json."
  - "GitHub Pages handbook deployment publishes from an untagged branch or a backport tag older than the highest SemVer release tag."
---
# ADR-0328: Multi-registry release publication and tagged handbook deployment

## Context

`ADR-0124`, `ADR-0156`, and `ADR-0200` defined the release-significant version topology (`release/version-topology-v1.json`), four-language driver distribution bundles (`release/distribution/manifest-v1.json`), and signed release provenance (`scripts/publish-release-artifacts`). However, nine packaging scripts hardcoded `"0.1.0"`, `.github/workflows/release.yml` created the GitHub Release without pushing packages to GHCR OCI Helm charts, npm, PyPI, `crates.io`, or Go module tags, and `.github/workflows/handbook.yml` deployed `docs.riffdb.com` on every push to `main` rather than tracking the latest tagged release.

## Decision

1. **Canonical Version Authority and Atomic Synchronization (`scripts/bump-version`):**
   - `release/version-topology-v1.json` (`release`) is the single source of truth read by `scripts/build-driver-distributions`, `scripts/check-driver-distribution`, `scripts/driver-package-arrival`, `scripts/driver-package-conformance`, `scripts/prepare-rust-package-mirror`, `scripts/write-python-artifact-receipt`, `scripts/check-python-package`, `scripts/test-python-platform-matrix`, and `scripts/check-workspace-policy`.
   - `scripts/bump-version` (`--check`, `--to <X.Y.Z>`, `--self-test`) atomically updates and validates `Cargo.toml`, internal workspace dependency specifiers, `release/version-topology-v1.json`, `release/durable-format-manifest-v1.json`, `release/distribution/manifest-v1.json` (including `durable_format.sha256`), `release/helm/riffdb/Chart.yaml`, and client/editor manifests.
2. **Six-Registry Release Publication (`scripts/publish-release-artifacts` and `.github/workflows/release.yml`):**
   - `scripts/publish-release-artifacts assemble` and `verify` package `release/helm/riffdb` into `riffdb-<version>.tgz` and emit `consumer-verification-v1.json`, binding both into `release-provenance-v1.json` and `SHA256SUMS`.
   - `scripts/publish-release-artifacts publish-registries` verifies cross-architecture determinism for platform-neutral archives across `x86_64` and `aarch64` and publishes to GitHub Releases, GHCR (`:v<version>`, `:<version>`, `:latest`, and `oci://ghcr.io/<repo>/charts`), npm (`@riffdb/cli-linux-x64`, `@riffdb/cli-linux-arm64`, `@riffdb/cli`, `@riffdb/client`), PyPI (`riffdb-application` wheels + sdist, `riffdb`, `riffdb-cli`), `crates.io` (the 7-crate Rust SDK runtime closure in topological order), and Go (`clients/go/runtime/v<version>` submodule tag plus proxy assets).
3. **Tagged Release Handbook Deployment (`.github/workflows/handbook.yml`):**
   - `.github/workflows/handbook.yml` runs `build` verification on `main`, pull requests, and weekly schedules, and deploys GitHub Pages (`docs.riffdb.com`) only on `v<major>.<minor>.<patch>` tag pushes that pass `scripts/check-version-topology --release-tag` and match the highest SemVer tag in the repository.

## Standing design tests

- **Interface safety:** Consumers and operators never receive unverified, unsigned, or version-drifted packages or documentation from an untagged `main` commit.
- **Scale:** Registry publication and version synchronization operate on bounded, deterministic artifact sets and fail closed before external mutation on any digest or version mismatch.

## Checks

- `./scripts/bump-version --check && ./scripts/bump-version --self-test`
- `./scripts/check-version-topology --release-tag v0.1.0`
- `./scripts/check-driver-distribution`
- `./scripts/publish-release-artifacts --self-test`
- `./scripts/handbook check`
