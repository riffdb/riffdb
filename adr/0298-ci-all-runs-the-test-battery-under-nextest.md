---
adr: "0298"
title: CI-All Runs The Test Battery Under Nextest
status: proposed
tier: surface
date: 2026-10-13
accepted: null
acceptance: null
requires: [ADR-0184]
amends: [ADR-0184]
supersedes: []
requirements: [TEST-003]
packages: []
# One entry per deferred obligation. `proof` is a test function name or a
# scripts/<name>; ./scripts/check-adr-obligations requires it to exist as a
# definition once the owning package is complete.
obligations: []
review_triggers:
  - scripts/ci-all runs fewer test targets, tests or doctests than the four SPEC 18.2 partitions plus the doctest run.
  - The ci-all test step stops running each test in its own process.
---
# ADR-0298: CI-All Runs The Test Battery Under Nextest

## Context

ADR-0184 Decision 4 moved SPEC 18.2's required unit and integration job to four cargo-nextest partitions and kept
`cargo test --workspace --all-features` "a valid local command" that "remains in `scripts/ci-all`". It records no reason for
keeping it in `ci-all`. The one capability `cargo test` has that nextest lacks is doctests: the workspace has 40, in 46 doctest
binaries, and nextest does not run them.

`cargo test` runs each test target's tests as threads of one process. Many `riffdb-storage-redb` tests spawn crash children of the
test binary. A spawned child holds a copy of every open descriptor until it executes, so a concurrent test in the same process that
closes and reopens its database or archive in that window finds the file lock still held (`DatabaseAlreadyOpen`, archive
`SinkUnavailable`). On disk it is a rare flake; with test scratch on tmpfs it failed three to five tests on every full
`riffdb-storage-redb` run, and none once the child-spawning tests were excluded (two runs). The partitioned CI jobs, package
acceptance and nextest runs never see it: nextest runs each test in its own process. `ci-all` was therefore the one gate that kept
test scratch on disk, the cause of the disk saturation that prompted RAM scratch, and the one gate without per-test limits.

## Decision

1. `scripts/ci-all` runs the unit and integration battery as one unpartitioned
   `cargo nextest run --locked --workspace --all-features --profile ci`: the same targets, tests, harnessless wrappers, per-test
   limits and setup scripts as the four SPEC 18.2 partitions together, one process per test.
2. It then runs `cargo test --locked --workspace --all-features --doc`, so every doctest still runs in `ci-all`.
3. `ci-all` requires the pinned cargo-nextest version in `scripts/tool-versions`, as it already requires cargo-deny, cargo-audit
   and cargo-machete.
4. `cargo test --workspace --all-features` remains a valid local command. ADR-0184's partitioning, coverage proof and wall-time
   evidence are unchanged.

## Consequences

- `ci-all` test scratch is RAM-backed through the nextest setup script, and in-process lock collisions between tests no longer
  affect the merge gate.
- Per-test nextest limits now apply in `ci-all`; a campaign that outgrows them fails there as it already does in CI.
- Cross-test interference inside one process is no longer exercised by any gate. It is a property of the test harness, not of the
  product, which opens each database from one process.

## Standing design tests

- **Interface safety:** no public surface changes.
- **Scale:** the gate runs the same bounded test set; RAM scratch for the measured battery peaked near 1 GiB.

## Checks

- `scripts/ci-all` runs both commands; `./scripts/check-test-partition-coverage` continues to prove every target is scheduled.
- On the development workstation the nextest battery and the doctest run pass on this branch.
