---
adr: "0350"
title: Public-Boundary Guarantee Proofs Production-Default Harnesses And Cross-Surface Conformance Gates
status: accepted
tier: guarantee
date: 2026-11-13
accepted: 2026-11-13
acceptance: 'maintainer, in session, 2026-11-13: "accepted"'
requires: [ADR-0005, ADR-0006, ADR-0007, ADR-0023, ADR-0051, ADR-0052, ADR-0058, ADR-0063, ADR-0074, ADR-0106, ADR-0181, ADR-0184, ADR-0298, ADR-0343, ADR-0347, ADR-0348, ADR-0349]
amends: [ADR-0181, ADR-0343]
supersedes: []
requirements: [TEST-001, TEST-003, GOV-004, GEN-005, TXN-001, TXN-002, CMP-001, QRY-001, QRY-004, AAA-003, AAA-004, DRV-005, DRV-007, DRV-012, MDB-001, MDB-006, DX-003, DX-004, DX-006, HBK-003]
packages: [WP-929, WP-930, WP-931, WP-932, WP-933, WP-934]
obligations: []
review_triggers:
  - Production crates retain file-wide `#![allow(dead_code)]` or bare `#[allow(dead_code)]` outside generated application bindings.
  - Service or daemon integration harnesses default command durability to `CoordinatorDurability::Sync` while `riffdbd` standard profile defaults to `CoordinatorDurability::Group`.
  - A `surface` or `guarantee` obligation for user-visible runtime behavior is discharged solely by calling an internal crate helper without a public-boundary integration or script proof.
  - Cross-language driver conformance omits error-diagnostic decoding, invariant rejection, cursor continuation, set chunking, reconnect, or `OutcomeUnknown` parity across supported surfaces.
  - Scoped acceptance on `surface` or `guarantee` changes omits reverse-dependent workspace crates unless explicitly opted out for inner-loop iteration.
---
# ADR-0350: Public-Boundary Guarantee Proofs Production-Default Harnesses And Cross-Surface Conformance Gates

## Context

The `0.2.1`–`0.2.4` series uncovered application-visible defects—duplicate idempotent commands stopping `CoordinatorDurability::Group` databases, primary-key `in $set` queries compiling into under-budgeted roles, post-mutation `invariant` rejections surfacing as `deadline_exceeded`, repeated or failed `capability bootstrap` runs leaving stale or overwritten credentials, wide-row sibling cursor budget drift, SDK error-decoder mismatches, and invalid handbook snippets—despite green unit, storage, crash, and CI suites.

Repository audit traced those escapes to six structural gaps: (1) 21 production `.rs` files carry file-wide `#![allow(dead_code)]` alongside ~196 item-level suppressions and unreferenced historical scripts, masking dead or unwired logic; (2) `ServiceHarness` defaults to `CoordinatorDurability::Sync` whereas `riffdbd` defaults to `CoordinatorDurability::Group`; (3) `check-adr-obligations` accepts internal `crates/*/src/**` unit helpers as sole proof of public guarantees; (4) shared remote driver conformance checks only four happy/key-reuse operations and `ci-all`/`acceptance` omit Go and TypeScript runtime unit suites; (5) `scripts/downstream-adapter-check` skips root-level adapter contracts and never compiles `.riffq` queries or role scan budgets, while handbook checks leave non-Rust SDK call sites and multi-database bootstrap loops unexecuted; and (6) scoped `scripts/acceptance` omits reverse-dependent crates by default.

## Decision

1. **Dead-code elimination and lint-suppression guard (`scripts/check-dead-code-allowances`):**
   - Remove file-wide `#![allow(dead_code)]` and bare `#[allow(dead_code)]` across `crates/*/src/**` (outside generated client bindings), deleting dead branches, wiring and testing any uncovered live invariants, gating test-only compatibility helpers behind `#[cfg(test)]` or `test-fixtures`, and using `#[expect(dead_code, reason = "...")]` for justified RAII guards so `rustc` rejects stale suppressions.
   - Retire unreferenced non-generator scripts in `scripts/` that are unnamed by `SPEC.md`, `AGENTS.md`, accepted ADRs, CI workflows, or active work packages, and enforce the lint policy in `scripts/check-workspace-policy`.
2. **Production-default harness durability and deferred-pipeline schedule exploration:**
   - Default `ServiceHarness` in `tests/service/support/mod.rs` to `CoordinatorDurability::Group` (matching `RedbCommitProfile::Standard`), retaining explicit `CoordinatorDurability::Sync` builders for `Hardened`-profile tests and parameterizing saga worker suites across both modes without sleep synchronization.
   - Add deterministic multi-epoch state-transition exploration in `crates/riffdb-commit/tests/deferred_pipeline_sequences.rs` exercising intra-group duplicates, cross-epoch in-flight duplicates, post-flush replays, key-reuse mismatches, conflict retries, and `CommitCheckRejected` against `redb` and a reference oracle.
3. **Mandatory public-guarantee end-to-end suite (`tests/public_guarantees/`):**
   - Add a `cargo-nextest` integration target on `riffdb-testkit-server` exercising real `riffdbd` listeners, `redb` (`Group` and `Sync`), and public transports across five guarantees: concurrent duplicate idempotency across unpublished group epochs; `.riff` post-mutation `invariant` rollback and attempt exhaustion mapping to `RDB-INPUT-0101` (`correct_request`) with non-terminal idempotency state; compile-to-runtime agreement across PK `Get`, PK `in $set`, covering scans, EAV `candidates`, role `max_scan_rows` (`RDB-APP-0205`), and `authorization_diagnostic` (`AAA-004`); wide-field (`<= 1 MiB`) plus sibling-binding cursor pagination within `4 MiB` pages; and post-send disconnect `OutcomeUnknown` classification followed by daemon/driver restart and safe replay.
4. **Seven-surface conformance corpus v2 and SDK unit gate (`scripts/check-sdk-runtimes`):**
   - Extend `scripts/driver-conformance` and `fixtures/driver/conformance-app` across Rust gRPC, Go `driverd`, TypeScript `driverd`, Python gRPC, Python `driverd` socket, MCP, and CLI JSON to verify parity on `RDB-INPUT-0101` invariant rejections, stale-revision conflicts, `RDB-AUTH-0214` `authorization_diagnostic` decoding, `<query>All` PK-set chunking and collation ordering, wide-row cursor continuation, `driverd` restart reconnect, and post-send `OutcomeUnknown`.
   - Wire `scripts/check-sdk-runtimes` (`tsc --noEmit`, `npm test`, `go test` on `clients/go/{runtime,operator}`, and Rust/Python client unit suites) into `governance/tiers.yaml`, `scripts/ci-all`, and `.github/workflows/ci.yml`.
5. **Lifecycle provisioning, full adapter manifest checks, and executable handbook recipes:**
   - Add `crates/riffdb-cli/tests/provisioning_lifecycle.rs` testing failed bootstrap cleanup, initial bootstrap, idempotent rerun without `--force`, pre-validation `--force` safety, `OutcomeUnknown` recovery retention (`RDB-CAP001`), external-cwd deploy, `driver.allowed_peer_uids` preservation across redeploy, live `riffdb-driverd` credential reload, and daemon/driver restart continuity.
   - Fix `scripts/downstream-adapter-check` to discover every contract and `riffdb.application.json` under `fixtures/adapters/**`, verify `.riffq` queries, role scan budgets, and locks, and trigger on RiffQL, query-compiler, query-module, policy, and adapter-fixture changes.
   - Extend `scripts/handbook` and `scripts/check-handbook-recipes` (and `scripts/remote-compose-acceptance`) to typecheck handbook TypeScript, Go, and Python SDK examples against generated clients and execute the documented multi-database bootstrap loop twice against a live server.
6. **Boundary-proof obligations, reverse-dependent acceptance, RED/GREEN bug rules, and multi-DB candidate rehearsal:**
   - For v2 `surface` and `guarantee` ADRs accepted after `2026-11-13`, `scripts/check-adr-obligations` requires each obligation to resolve to a boundary proof (`tests/**`, `crates/*/tests/**`, `clients/**`, or a CI-wired `scripts/**`) or declare `boundary_proof` alongside an internal unit `proof`.
   - Default `scripts/acceptance` to include reverse-dependent workspace crates on `surface` and `guarantee` changes (with `--crate-only` opt-out for local iteration), codify strict RED-then-GREEN public-boundary regression testing for bug fixes in `AGENTS.md`, and run `scripts/rehearse-multi-db-candidate` (one multi-DB `riffdbd`, shared `riffdb-driverd`, concurrent 4-SDK + MCP load, fault injection, and rolling restart) in `.github/workflows/driver-distribution.yml` and `scripts/announcement-readiness`.

## Options considered

1. **Automated git-revert mutation testing in CI:** Rejected because multi-file `.proto`, IR, and lockfile fixes cannot be mechanically inverted per hunk; strict RED/GREEN authoring rules in `AGENTS.md`, `--self-test` fault canaries on check scripts, and mechanical boundary-proof checks in `check-adr-obligations` enforce real fault sensitivity deterministically.
2. **Duplicating full multi-SDK builds in `.github/workflows/ci.yml`:** Rejected; running fast SDK unit checks in `ci.yml` while executing `rehearse-multi-db-candidate` inside `driver-distribution.yml` (which already builds native binaries, wheels, Go modules, and npm packages on every PR) delivers full PR coverage within ~60 incremental seconds.

## Consequences

- Dead code and unverified `#![allow(dead_code)]` suppressions are removed and blocked from re-entering production crates.
- Service harnesses, public-guarantee tests, 7-surface conformance, adapter checks, and handbook recipes prove composed application behavior under production defaults.

## Standing design tests

- **Interface safety:** Public error codes, `symbol_path`, environment-default `authorization_diagnostics` redaction, idempotency non-bypass, and least-privilege role bounds remain unchanged; test hooks never weaken production admission or durability.
- **Scale:** Sequence explorers, wide-row pagination fixtures, and multi-DB rehearsals use explicit bounded iterations, `<= 4 MiB` query pages, and deterministic synchronization without sleeps.

## Checks

- `scripts/check-dead-code-allowances`, `scripts/check-sdk-runtimes`, `scripts/downstream-adapter-check`, `scripts/driver-conformance`, `scripts/handbook`, `scripts/check-handbook-recipes`, `scripts/check-adr-obligations`, and `scripts/rehearse-multi-db-candidate`.
- `deferred_pipeline_sequences`, `public_guarantees`, `provisioning_lifecycle`, and `./scripts/ci-all`.
