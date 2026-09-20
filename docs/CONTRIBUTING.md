# Contributing and governance

RiffDB's safety lives in machine checks: architecture tests, byte-exact
fixtures, crash matrices, and human review of public and durable surfaces.
Its cost used to live somewhere else — four hand-written copies of the same
intent, in `SPEC.md`, `work_packages.yaml`, an ADR, and an obligations ledger,
reconciled by hand. Process v2, in force from 2026-09-02, keeps every machine
check and every human review trigger and makes the paper trail generated,
tiered, and derived from code: the ceremony a change owes follows the surface
it touches, and each fact is written once, in ADR front matter and
`governance/tiers.yaml`, where the tooling reads it.

## Tiers

`./scripts/classify-change` assigns a tier to every change from the paths it
touches, using the rules in `governance/tiers.yaml`. The first matching rule
wins for one file, and the tier of the change is the maximum over its files.
The commit message names the result in a `Governance-Tier: <tier>` trailer,
which may raise the computed tier but never lower it.

| Tier | When it applies | What it requires | Who reviews |
|---|---|---|---|
| internal | Everything not matched by a higher rule: implementation, tests, benchmarks, tooling, internal crates | Tests and acceptance only. No decision record; the commit message names the tier | The implementing agent |
| surface | Public and generated surfaces: `proto/**`, the syntax, IR, and compiler crates, the gRPC, MCP, and client crates, the CLI surface, `clients/**`, `templates/**`, `fixtures/**`, `release/**`, `SPEC.md`, `adr/**`, and the redb layout, key, codec, and format-upgrade modules | A short-form record (ADR v2, `tier: surface`, body at most 120 lines) and a passing acceptance run | A human reviews the fixture and generated diffs |
| guarantee | The guarantees themselves: the commit, conflict, idempotency, auth, policy, runtime, and invariant crates, the durable redb and storage-api boundaries, the MCP handler and observer, and `AGENTS.md` | A full record (ADR v2, `tier: guarantee`, body at most 200 lines) whose exact text a human accepts before merge, and a passing acceptance run | A human accepts the record text and reviews the change |

A package carries its own `tier`. `./scripts/acceptance --wp WP-NNN` warns when
the package tier and the computed tier disagree and fails when the package tier
is the lower of the two.

## Decision records

A v2 record opens with YAML front matter whose `adr:` key makes it a v2 file.
The front matter is the single source: index rows, package skeletons,
obligation tracking, and requirement links are derived from it, so a fact
belongs there once rather than in prose.

```yaml
---
adr: 0185
title: Short Title In Title Case
status: proposed            # proposed | accepted | rejected | superseded
tier: surface               # surface | guarantee
date: 2026-09-02
accepted: null              # the date a human accepted it
requires: [ADR-0093]        # records this one depends on
amends: []
supersedes: []
requirements: [REP-002, REP-003]
packages: [WP-746, WP-747]
obligations:
  - id: OBL-0185-1
    package: WP-746
    proof: follower_applies_exact_prefix_byte_faithfully
    says: A follower driven by the app-baseline workload holds a byte-faithful
      prefix.
review_triggers:
  - Any change to acknowledgement semantics or the write hot path.
---
```

The body has four sections: `## Context` (at most two paragraphs), `## Decision`
(numbered, one testable statement each), `## Standing design tests` (interface
safety and scale, both answered), and `## Checks` (the fixtures, tests, scripts,
and architecture checks that freeze the decision). A surface-tier body is at
most 120 lines. A guarantee-tier body may add `## Options considered` and
`## Consequences` and is capped at 200 lines. `adr/0000-template.md` is the
form; `./scripts/adr-new` writes it.

Acceptance is a human act, and it must be visible as one. A human sets
`status: accepted`, the `accepted:` date, and, for any record accepted on or
after 2026-09-12, an `acceptance:` reference naming who accepted it and how
(for example `maintainer, in session, 2026-09-14`). The decision is the
maintainer's; the mechanics need not be. An agent never sets `status: accepted`
on its own initiative, but once the maintainer has said a record is accepted
the agent makes the acceptance commit itself, quoting where and when in
`acceptance:`, rather than asking the maintainer to edit files. The commit
that accepts a record touches only `adr/**`, `work_packages.yaml`, and
`SPEC.md`, so acceptance never rides along with code.
`./scripts/check-acceptance-commits` enforces both rules on every commit in the
range and on the working tree; `governance/tiers.yaml` holds the date and the
allowed paths under `acceptance:`. Exact-text acceptance before merge is
required at guarantee tier. Direction approval is given once per program, not
per record.

Standing maintainer directives live under `directives:` in
`governance/tiers.yaml`, and `./scripts/brief` prints them at the top of every
package brief. Add one there rather than in a record when the instruction is
about how to work rather than what to build.

D-001 restricts new functionality records while WP-757, WP-760, and WP-772
remain open. It permits records needed for discovered high or critical issues.
Investigation and repair can proceed under existing accepted decisions;
changing an accepted decision or a SPEC MUST still requires exact-text human
acceptance. D-003 continues to govern implementation choices within those decisions.

`adr/README.md` holds the index between the `<!-- adr-index:start -->` and
`<!-- adr-index:end -->` markers. It is generated by `./scripts/adr-index
--write` and verified by `./scripts/adr-index --check`; do not hand-edit it.

## Work packages

A package still names the scope one agent may change and the gate it must
pass. New packages take this shape:

```yaml
- id: WP-800
  name: Short imperative name
  tier: surface                     # internal | surface | guarantee
  depends_on: [WP-746]
  objective: What this package makes true, in two or three sentences.
  requirements: [REP-002, REP-003]  # SPEC requirement IDs it proves
  required_adrs: [ADR-0185]
  allowed_paths: [crates/riffdb-storage-api/**, tests/**]
  deliverables:
  - One deliverable per line, each observable in the tree.
  exit_gate: The single observable condition that closes the package.
  standing_design_tests:
  - Interface safety: ...
  - Scale: ...
  acceptance_commands:
  - ./scripts/acceptance --wp WP-800
  - ./scripts/ci-all
```

`allowed_paths` names the crates a package may change; it is scope, not an
inventory of every file the change will touch. `./scripts/check-allowed-paths
--wp WP-NNN` always permits the consequence paths listed under
`scope.always_allowed_paths` in `governance/tiers.yaml` (the manifest, SPEC,
`Cargo.lock`, generated code, fixtures, tests, docs, release evidence,
templates, clients, examples, evaluations, scripts) and the package's own
records, so none of those need listing. A touched file outside scope fails the
check only when it sits on a surface- or guarantee-tier path; an internal-tier
file outside scope is reported as a warning. Paths are read at the base commit,
so entering another crate is a separate widening commit that precedes the
package change; it needs no approval, only that commit. `./scripts/wp-new ADR-NNNN` prints one skeleton per
entry in the record's `packages:` list, with the tier taken from the record.

### Decision authority

An accepted record delegates every choice inside its Decision section to the
package that implements it. The implementer decides those choices and does not
ask. The test is: would the change alter the text of an accepted record's
Decision section or a SPEC MUST, or add a durable identity, public surface, or
guarantee that no accepted record names? If no, it is an implementation choice.
If yes, it is an architectural change: propose a record and stop.

Implementation choices are recorded, not approved. A commit that makes a
non-obvious choice lists it under `Decisions:` in the commit message, and the
closure block carries them all:

```yaml
  closure:
    status: complete
    completed_at: "2026-09-14"
    decisions_taken:
    - Activation installs the registry and V3 roots in one Immediate
      transaction; an erased root reopens fail-closed rather than as legacy.
```

The maintainer reviews `decisions_taken` once, when the package closes, which
is the only planned conversation per package. Only three conditions stop work
before then: the change would contradict an accepted decision or a SPEC MUST;
a SPEC 19.6 review trigger fires and no accepted record covers the change; or
two readings of an acceptance criterion would produce materially different
work. Every other question is batched into the closure report.

A closed set fixed by a record (attribution tags, enum variants, namespace
classes, error codes) is almost always incomplete on first contact with the
code. Until the format's first durable write outside the test tree, the
implementing package may complete such a set without asking: it records the
completion as an amendment in the record's own commit, touching only `adr/**`,
in the form `Amendment N (completed by WP-NNN on <date>): <what was added and
why>`, and lists it in `decisions_taken`. Removing or reinterpreting a member,
or completing a set after its first durable write, is a record change and
stops.

A patch-level or security-advisory bump of a dependency that an accepted
record already admits is likewise an implementation choice. Keep the exact
pin, run `cargo deny check` and `cargo audit`, and record the version, the
advisory or reason, and both results in `Decisions:` and `decisions_taken`.
If the pinning record names the exact version, update that one token in the
record's own commit with the same line. A new dependency, a minor or major
change to a pinned critical dependency, a feature-set change, or any bump
that alters a public or durable behavior still stops under AGENTS.md.

## Obligations and requirement tags

An obligation is a promise a record makes that its own change does not yet
keep. It names the owning package and a `proof`: a test function name, or a
`scripts/<name>` script. The proof must exist in code, not in prose.
`./scripts/check-adr-obligations` requires a match for `fn <proof>` in Rust,
`def <proof>` or `test_<proof>` in Python, or the script path, found outside
`adr/`, `docs/`, `work_packages.yaml`, `SPEC.md`, and `governance/`. A missing
proof is reported while the owning package is open and fails once that package
closes.

A requirement is proven by a tag on the test that establishes it: a comment
line `// req: REP-002, REP-003` in Rust, Go, and TypeScript, or
`# req: REP-002` in Python, Ruby, bash, and YAML. The tag sits immediately
above the test function, or at the top of the file to cover every test in it.
Tags are scanned under `crates/**`, `tests/**`, `clients/**`, `examples/**`,
`evaluations/**`, and `scripts/**`. An unknown ID is an error, and every
requirement listed by a package completed on or after the cutover must carry
one. `./scripts/check-requirement-coverage --report` lists what is still
unproven.

## Commands

| Command | Use |
|---|---|
| `./scripts/brief WP-NNN [--adr-lines N]` | Render the package as a prompt: objective, tier, dependencies, allowed paths, deliverables, exit gate, requirement text, ADR decisions, obligations, acceptance plan, review triggers |
| `./scripts/classify-change [--base <ref>] [--range a..b] [--json] [--require <tier>] [<path>...]` | Compute the tier of a change, the file that decided it, and the triggered checks |
| `./scripts/acceptance [--wp WP-NNN] [--base <ref>] [--range a..b] [--dry-run] [--full] [--with-dependents] [--no-tests]` | Run the plan for one change: format, clippy and tests for the touched crates, and every path-triggered check |
| `./scripts/check-allowed-paths --wp WP-NNN` | Prove the change stayed inside the package's `allowed_paths` |
| `./scripts/adr-new --title "..." --tier surface\|guarantee [--requires ADR-..] [--requirements ID..] [--packages WP-..]` | Allocate the next number and write the v2 skeleton |
| `./scripts/wp-new ADR-NNNN` | Print the package skeletons the record's front matter implies |
| `./scripts/adr-index --write` / `--check` | Regenerate or verify the index in `adr/README.md` |
| `./scripts/check-adr-obligations [--report] [--json] [--prune-ledger]` | Check obligation proofs; prune superseded ledger entries |
| `./scripts/check-requirement-coverage [--report] [--json]` | Check that every requirement is claimed by a package or proven by a tag |
| `./scripts/governance-cost [--since <date>] [--range a..b] [--json]` | Measure governance lines against all other changed lines |
| `./scripts/ci-all` | The full battery, required at merge |
| `./scripts/check-three-places --base <ref>` | Reject handwritten adapter edits outside `src/generated/` when Protobuf or the public operation registry changes |
| `./scripts/generate-operation-adapters --check` | Verify generated public adapters and the registry-derived driver drift corpus |
| `./scripts/generate-query-clients --check` | Verify the canonical generation model, checked-in templates, and generated application clients |
| `./scripts/check-test-partition-coverage` | Prove every Cargo test target belongs to exactly one whole-target SHA-256 partition |
| `./scripts/run-test-partition --mode whole-target --partition N` | Run CI partition `N`, from 1 through 4, through its exact nextest binary filterset |

`./scripts/acceptance` runs the path-triggered checks itself.
`./scripts/handbook check` verifies generated references, links, snippets,
and the published Rust API surface. `./scripts/check-container-startup`
starts the release container, because rendering checks alone cannot detect
startup failures.

### External adapter compatibility

RiffDB builds, tests, releases, and merges independently of sample adapter
repositories. Its first-party Rust runtime, language fixtures, SDK conformance,
and package tests remain core responsibilities. A sample adapter's availability
or compatibility cannot block those checks.

Each adapter repository owns a **RiffDB compatibility** workflow. Its
`.github/riffdb-revision` file selects an immutable RiffDB commit for normal
pull-request checks. A scheduled run checks RiffDB `main` and reports drift in
the adapter repository. Manual dispatch can test a proposed RiffDB revision.
These checks compile the adapter's application sources, exact locks, and
existing generated artifacts with the selected Rust CLI; framework-runtime
conformance remains the adapter's separate responsibility.

When upgrading an adapter, select the new RiffDB commit in the adapter
repository, migrate its sources, review regenerated artifacts, and run
`riffdb application check`. Commit the pin and migration together in the
adapter's pull request. Intentional language changes are governed by RiffDB's
own specification, accepted ADRs, and compatibility tests; sample adapters
update on their own schedule.

CI runs the required unit and integration battery as four whole-target
cargo-nextest jobs. The SHA-256 assignment is stable from each canonical
nextest binary ID, so every test within one target stays in the same job; the
coverage check compares Cargo metadata with all four filtersets before those
jobs run. `./scripts/ci-all` deliberately retains the unpartitioned
`cargo test --workspace --all-features` merge check.

## Legacy records and packages

- ADR-0001 through ADR-0177 keep the legacy header, a `# ADR-NNNN: Title` line
  followed by a `- **Status:**` list. They are not rewritten, they are read
  exactly as they were accepted, and the index renders their tier as `legacy`.
- `adr/obligations-outstanding.yaml` is closed to new entries. It is read and
  pruned, never appended; an entry whose record now has front matter is
  superseded and removed by `./scripts/check-adr-obligations --prune-ledger`.
  A new obligation belongs in the record's front matter.
- Packages whose `closure.completed_at` is earlier than 2026-09-02 are exempt
  from the requirement-tag rule. Their requirements stay claimed rather than
  proven, and nothing reopens them.

## Governance cost

`./scripts/governance-cost` measures what the process costs in lines. The
baseline recorded at the top of `governance/tiers.yaml`, for
`--since 2026-06-01`, is 101,477 governance lines against 1,771,277 other lines
over 1,273 commits: a governance share of 5.4% of all changed lines, with 519
of those commits touching a governance path. Re-run it after a program to see
whether the share moved, and treat a rise without a matching rise in reviewed
surface as a process defect rather than as diligence.

## Performance measurements

Read [measurement-host selection](performance/benchmark-host-selection.md) before
choosing a host or reusing a baseline. Hardware SHA-2 is required for future
measurements by maintainer direction; N1 results are historical and testing there
has stopped. Host/profile enforcement is being recorded separately. Coordinate
exclusive use and compare matching toolchains and build profiles. Keep P99 as a
hard selection gate. The [writer-census defects](performance/wp749-writer-census-defects.md)
require independent wall-time reconciliation with an explicit remainder;
`busy + idle` does not cover the writer's full interval.

## Build hygiene

- A session running several package worktrees should export one shared
  `CARGO_TARGET_DIR` and reuse it across that session's worktrees, and never
  share one across concurrent sessions: cargo's target lock serializes builds.
- Dependency debuginfo is disabled workspace-wide
  (`[profile.dev.package."*"] debug = false`) while workspace crates keep line
  tables. Do not re-enable it in a package without maintainer approval.
- Prefer targeted cleanup (`rm -rf <target>/*/incremental`, age-based sweeps)
  over `cargo clean`; a machine-wide `sccache` wrapper caches dependency
  compilation, so cold rebuilds are cheap but still wasteful.

## Handbook maintenance

`docs/SUMMARY.md` is the public handbook inventory. Public behavior must be
documented from the same pull request that changes it.

### Source of truth

| Subject | Authoritative source | Handbook responsibility |
|---|---|---|
| Normative semantics | `SPEC.md` and accepted ADRs | Explain implemented behavior without weakening it |
| Work sequencing | `work_packages.yaml` | Keep historical package status out of primary user guidance |
| CLI | Clap definitions in `riffdb-cli` | Regenerate `docs/reference/CLI.md` |
| Architecture visuals | `diagrams/*.dot` | Regenerate checked SVGs |
| Public Rust API | `riffdb-client-rust` | Build Rustdoc into the site artifact |
| Contract and query syntax | Grammar, compiler, fixtures | Update language pages and runnable examples together |
| Installation and configuration | Installer scripts and config parsers | Keep commands, paths, defaults, and security notes exact |

Do not hand-edit `docs/reference/CLI.md` or `docs/assets/*.svg`. Regenerate with:

```bash
./scripts/generate-cli-reference --write
./scripts/generate-handbook-diagrams --write
```

### Local checks

Install and verify the pinned local tools before running the handbook check:

```bash
export PATH="${CARGO_HOME:-$HOME/.cargo}/bin:$PATH"
./scripts/developer-tools install
./scripts/developer-tools check
```

The check also verifies that `cargo +1.98.1` resolves through rustup. A
configured `rustc-wrapper` such as `sccache` is optional, but if a Cargo config
names one it must be installed or that stale setting must be removed. RiffDB
does not silently change user Cargo configuration.

Then run:

```bash
./scripts/handbook check
```

The check validates source inventory, generated freshness, snippets, internal
links and fragments, external runtime assets, the mdBook build, and public
Rustdoc. Use `./scripts/handbook serve` for a local preview.

### Writing rules

- State the POC boundary when a reader could mistake a feature for production
  readiness.
- Use current public commands and symbolic names; do not teach internal IDs or
  storage access.
- Give images meaningful alternative text and keep tables usable on narrow
  screens.
- Link to one detailed source rather than copying long instructions into
  several pages.
- Mark non-executable illustrative code explicitly. Tested snippets must remain
  deterministic and require no external service unless the page says so.
- Put internal histories and work-package evidence in an excluded directory,
  not in the public learning path.

The PR note names the pages a change updated under `Behavior added or changed`,
or gives a concrete reason the change cannot affect users, operators,
application authors, public interfaces, or compatibility.
