# WP-723 downstream adapter protection

Package: WP-723. Tier: internal. Dependency WP-569 is complete in the inherited
main history. This package changes CI and contributor tooling, not database,
protocol, authorization, or language semantics.

## Behavior and decisions

- Publish RiffDB and all three adapters as public repositories under the
  `riffdb` GitHub organization, per the maintainer's 2026-09-17 session request.
  The adapter repositories previously had no remote.
- Keep repository URLs and immutable full commit pins in
  `scripts/downstream-adapters.json`. Fetch each into a disposable checkout;
  use the working-tree Rust CLI to check sources, exact locks, and generated
  artifacts. No adapter program, framework test, or dependency install runs
  in this job. CI uses neither repository secrets nor local sibling paths.
- Run the unconditional **Downstream adapters** job on every pull request,
  push, and manual CI dispatch. Require this stable check name on `main`.
- Retain explicit read-only `--repo-root` and `--repo` options for local
  iteration. Missing adapters now fail even if another adapter passes.
- Put the same orchestration tests and real compiler regression in `ci-all`.

## Evidence

The initial eight-test orchestration suite includes a regression identified
before implementation: with only OpenFGA present, the old checker returned
success while skipping Better Auth and MLflow. It now rejects that state.
The suite also covers propagated compilation failures while continuing to
check other adapters, successful complete coverage, missing manifests,
unknown names, missing arguments, missing declared profiles, and malformed or
mutable pin sets.

The committed Better Auth source at
`9dfd90e591380b21f2918f5b8c50573ca5ff4e62` reproduced:

```text
RDB-QS003 [query_syntax] Limit must declare its maximum
riffdb/queries/admin_users_all_email_asc.riffq:146..151
```

Its nine old declarations are now `Limit<100>`. The initial OpenFGA and MLflow
commits also had stale application locks. The published adapter revisions
include refreshed exact artifacts and the maintainer-requested commits of
pending Better Auth profile materialization and MLflow batch/metric work.

A fresh fetch from all three public GitHub repositories passed application
compilation, lock checks, and generation checks. The automated ADR-0167
reproduction then copied the successful Better Auth tree, restored the bare
`Limit` spelling, ran the same downstream checker, and required both nonzero
status and `RDB-QS003`. A missing executable or unrelated lock failure cannot
satisfy that diagnostic assertion.

Local checks: eight orchestration tests; pinned GitHub check with ADR-0167
reproduction; `bash -n`; `git diff --check`; package allowed-path check;
formatting and handbook check. Package closure records final acceptance.

## Compatibility and follow-ups

The contributor handbook's [downstream pin procedure](../CONTRIBUTING.md#updating-downstream-adapter-pins)
requires intentional language breaks and their adapter pin updates in the
same reviewed RiffDB change, with links to adapter diffs. This is the affected
public handbook page, already reachable through `docs/SUMMARY.md`.

Default downstream checks now fetch GitHub pins and need network access.
Explicit local working-copy checks remain offline after the CLI is built.
The CI gate proves compiler and generated-artifact compatibility, not full
framework behavior or installed-driver release conformance (DRV-014).

MLflow's pending work passed formatting, lint, mypy, all 21 tests, and
application compilation; its upstream conformance target remains unimplemented.
Better Auth's application compiled, but `npm run check` could not complete:
the immutable `@riffdb/*@0.1.0-dev.16` packages point to an unavailable
`127.0.0.1:4873` registry, and matching artifacts were not found locally.
Those dependency pins and tests were not weakened. The adapter commit records
this limitation. Public GitHub source publication is not a package release.

The pending Better Auth materialization fixture independently exposed an
unused cursor on unique `take 1` lookups (`RDB-QM005`). The adapter fix omits
that cursor while retaining it for pagination, regenerates the fixture, and
adds a regression assertion. The profile's 19 queries now compile. Its path
is explicit in the pin manifest, so CI covers both Better Auth applications
and refuses a missing profile.

Initial GitHub publication also exposed a workflow-level `runner.temp`
reference in the existing driver-development workflow, where the runner
context is unavailable. It now lives on the consuming step's environment;
no release test or package behavior changes.

The first hosted Python packaging run exposed independent baseline failures:
[run 35282049177](https://github.com/riffdb/riffdb/actions/runs/35282049177).
The x86_64 source distribution omits `crates/riffdb-operation-registry/Cargo.toml`;
the ARM job cannot write `dist/riffdb-python-artifacts-v1.json` after its container
build. These release-packaging failures are outside WP-723's compiler gate and
remain follow-ups; this report does not claim the entire hosted CI is green.


## Completion audit — 2026-09-17

[Hosted job 105406159789](https://github.com/riffdb/riffdb/actions/runs/35282049162/job/105406159789)
completed successfully at 22:33:51 UTC for PR head
`8700d595debac6d787b5a27e252806900a498b58`. Its retained log reports all eight
orchestration tests passing, all three full commit pins fetched, all four
application directories compiling, and the deliberate bare `Limit` copy failing
with `RDB-QS003` through the same checker. The package closure changes only
this evidence and work-package metadata after that implementation proof.

| Required outcome | Current evidence |
| --- | --- |
| Dependency merged | WP-569's completed closure and implementation are inherited from main. |
| CI can reach pinned sources | All four GitHub repositories are public; the hosted log fetched the exact three revisions in `scripts/downstream-adapters.json`. |
| Every PR checks every adapter | `CI` has an unconditional `pull_request` trigger and an unconditional **Downstream adapters** job, with no path filter or allowed failure. |
| Compiler failures block the RiffDB change | The checker propagates failures; `main` requires this check from GitHub Actions app 15368, with strict freshness and administrator enforcement. The protection API was read back after configuration. |
| ADR-0167 failure is reproduced | The hosted job first passed the real applications, then required a nonzero checker exit and `RDB-QS003` for the restored bare `Limit`. |
| Intentional breaks are reviewed together | `docs/CONTRIBUTING.md` documents adapter migration, full revision updates, diff links, and the required same-change check. |
| Scope and governance | Internal tier; no authoritative Rust, language, protocol, or durable format changed. Package acceptance and unscoped acceptance pass. |

The adapter repositories are clean after their requested commits and publication.
The implementation is ready for review in [PR #1](https://github.com/riffdb/riffdb/pull/1).
The separate packaging failures above must be resolved before claiming a green
full merge/release battery; this closure does not merge the pull request.
