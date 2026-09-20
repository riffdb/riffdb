# AGENTS.md — RiffDB POC

## Mission

Build the standalone Rust proof of concept for **RiffDB** defined by `SPEC.md`. The POC proves contract-first command semantics, durable concurrency safety, idempotent uncertainty recovery, native MCP exposure, provenance, outbox intent, projection watermarks, and crash recovery.

The specification and accepted architecture decision records are authoritative. Issue text and agent prompts may narrow scope but may not weaken normative requirements.

## Non-negotiable architecture boundaries

1. Database semantics, protocols, transport trust decisions, and authoritative
   runtime code are first-party Rust. Target-language generated application
   bindings may own only the language-idiomatic values and typed facade assembly
   permitted by an accepted interface ADR and equivalent semantic tests.
2. Application writes occur only through compiled commands.
3. gRPC, MCP, CLI, and SDK paths use the same API-neutral application service, authorization layer, command runtime, and commit coordinator.
4. The deterministic command runtime performs no network, filesystem, operating-system clock, process-global mutation, or untracked randomness.
5. The commit coordinator is the only component that assigns commit sequences or applies authoritative mutations.
6. Mutations, persisted outcome, durable events, provenance, and commit record are atomic for one command.
7. MCP is not a privileged bypass and does not access storage directly.
8. Projection state is derived and rebuildable. The commit log and entity state are authoritative.
9. First-party crates use `#![forbid(unsafe_code)]` unless an accepted ADR grants a narrow exception.
10. Do not add SQL, Raft, distributed transactions, arbitrary transaction callbacks, or general analytical joins to the POC critical path.
11. Application-facing interfaces are always safe: no public surface may let an
    application developer or agent express an unsafe operation or silently opt
    out of a guarantee (transactions, idempotency, org scoping, typed
    freshness, bounded queries, durability of acknowledged writes). Unsafe
    machinery is permitted only below the public boundary. Every ADR touching
    a public surface must answer the interface-safety design test in the ADR
    template; weakening this boundary requires explicit human acceptance, never
    an implementation-package deviation.
12. Opinions live only in the data layer (`docs/VISION.md`). The capability
    model governs data authority for application principals; RiffDB does not
    implement end-user identity, sessions, sign-in flows, or OAuth, and no
    feature may require an application to adopt RiffDB-owned userland
    machinery. Pillars are independently adoptable, and the changelog/export
    path out of any pillar is a supported surface, never removed to retain
    data.
    For ADR-0207's changelog transition, V1/V2 emitters may become test-only
    compatibility evidence. The V3 changelog substrate and supported application
    export remain intact; application export behavior is unchanged.

## This repository is public

`github.com/riffdb/riffdb` is a public repository. Everything committed here is
world-readable, permanently, including in history after a file is deleted.

**Never commit any of the following, in code, docs, evidence, fixtures, test
data, commit messages, or generated output:**

- IP addresses, hostnames, DNS names or URLs of real machines — bench hosts,
  VPS instances, internal services, anything routable. Record a machine by its
  **specifications only**: `8 vCPU, AMD EPYC 9B14`, never its address.
- Credentials of any kind: API keys, tokens, passwords, private keys,
  certificates, connection strings, cloud service-account files, `.netrc`.
- Personal information: real names beyond repository authorship, email
  addresses, accounts, absolute home directory paths (`/home/<user>/...`).
  Use `~`, a relative path, or a placeholder.
- Build artifacts and compiled output. They embed absolute developer paths and
  bloat history. `.gitignore` covers them; do not override it with `git add -f`.
- Session transcripts, agent logs, or tool output captured verbatim, which
  routinely contain all of the above.

Use documentation ranges when an address is genuinely needed in an example:
`192.0.2.0/24`, `198.51.100.0/24`, `203.0.113.0/24` (RFC 5737), `example.com`.

**Never `git add -A` or `git add .`** in this repository. Multiple agents and
worktrees share this checkout; a blanket add sweeps up other sessions' untracked
files, which has repeatedly captured agent transcripts and machine addresses.
Stage explicit paths, and read `git status --short` before every commit.

Before committing, confirm your diff introduces none of the above. This is not
recoverable by a later deletion: removing a disclosure requires rewriting public
history and rotating whatever leaked.

## Authoritative files

- `SPEC.md`, `work_packages.yaml`, and `governance/tiers.yaml` (the tiers, the
  path rules that assign them, and the path-triggered checks).
- Accepted `adr/*.md`: `status: accepted` in v2 front matter, or
  `- **Status:** Accepted` in a legacy header. A v2 record's front matter owns
  its tier, requirements, packages, obligations, and review triggers.
- `.proto` and grammar sources once merged, and their generated fixtures.

When these conflict, stop and request human review. Do not choose whichever
interpretation is easiest to implement.

## Change protocol

Every change has a tier. `./scripts/classify-change` computes it from the
touched paths against `governance/tiers.yaml` as the maximum over them; the
commit message names it in a `Governance-Tier: <tier>` trailer that may only
raise it.

- **internal**: tests and acceptance only; no decision record.
- **surface**: a short-form decision record (ADR v2, `tier: surface`, body at
  most 120 lines) and human review of the fixture diffs.
- **guarantee**: a full record (`tier: guarantee`, body at most 200 lines) whose
  exact text a human accepts before merge, and human review.

For a work package, run `./scripts/brief WP-NNN`; the brief is the prompt. It
carries the objective, tier, dependencies, allowed paths, deliverables, exit
gate, standing design tests, requirement text, ADR decisions, owned
obligations, the acceptance plan, and the review triggers. Confirm every hard
dependency is merged, add or identify a failing test first, keep changes inside
`allowed_paths` (`./scripts/check-allowed-paths --wp WP-NNN` proves it), and
run `./scripts/acceptance --wp WP-NNN` while you work. Without a package,
internal tier needs no record; above it, `./scripts/adr-new --title "..."
--tier surface|guarantee` writes the v2 skeleton and `./scripts/wp-new
ADR-NNNN` prints the package skeletons it implies.

Tag the test that proves a requirement with a comment line immediately above the
test function, or at the top of the file to cover every test in it:
`// req: REP-002, REP-003` in Rust, Go, and TypeScript, `# req: REP-002` in
Python, Ruby, bash, and YAML. Unknown IDs are errors, and every requirement of a
package completed on or after the cutover MUST be proven by a tag. Each ADR
obligation names a `proof`: a test function or a `scripts/<name>` that MUST
exist in code, not in prose. `./scripts/check-adr-obligations` searches outside
`adr/`, `docs/`, `work_packages.yaml`, `SPEC.md`, and `governance/`, and fails
once the owning package closes without it.

Whatever the tier: never weaken fail-closed behavior to make a test pass, never
duplicate a semantic type in two crates to avoid coordinating an interface, keep
input, output, recursion, collection, scan, wait, and diagnostic sizes bounded,
and preserve redaction before logging, metrics, MCP text, or public errors.

Before completion: `./scripts/acceptance` passes; docs are updated where public
behavior changed; the closure block records `completed_at`; and the PR note
(SPEC 19.7) gives Package (`WP-NNN`, or `none` with the tier), Tier, Behavior
added or changed, Checks run, Compatibility, and Hazards and follow-ups.

## Documentation maintenance protocol

- `docs/SUMMARY.md` defines the public handbook. Any user-visible behavior,
  contract language, protocol, CLI, configuration, installation, operational,
  compatibility, or SDK change MUST update the affected handbook page in the
  same pull request, and new public behavior MUST be reachable from
  `docs/SUMMARY.md`.
- Regenerate generated references and diagrams when their source changes; never
  hand-edit one. Examples MUST use current public interfaces and name POC
  limits, never presenting proposed or deferred behavior as available.
- `./scripts/acceptance` runs the path-triggered checks, among them
  `./scripts/handbook check`, `./scripts/check-container-startup`, and
  `./scripts/downstream-adapter-check`, which `docs/CONTRIBUTING.md` explains.
- The PR note names the updated pages, or a concrete reason the change cannot
  affect users, operators, application authors, interfaces, or compatibility.

## Standard commands

```bash
./scripts/acceptance   # per change: the plan for the touched paths
./scripts/ci-all       # the full battery, required at merge
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo deny check
```

Package-specific Loom, Shuttle, fuzz, recovery, generation, and conformance
commands are defined in `work_packages.yaml` and reported by `./scripts/brief`.

## Rust rules

- Prefer domain-specific newtypes over raw strings and integers.
- No floating-point representation for business decimals.
- Avoid panics on untrusted input and recoverable runtime conditions.
- Public errors must be typed and safe to expose; internal sources remain in tracing with incident IDs.
- Do not hold a synchronous mutex guard across `.await`.
- Cancellation must release all non-durable capabilities.
- Persistent encodings require versioning and compatibility fixtures.
- Repeated map or set serialization must be canonically ordered before hashing or persistence.
- Avoid broad feature sets on dependencies; justify critical dependencies.

## Testing rules

- Tests must assert semantics, not only code paths.
- Every concurrency primitive requires an explored or deterministic schedule test.
- Every durable boundary requires process-level crash coverage where applicable.
- Every compiler diagnostic added or changed requires a source-span snapshot plus semantic assertion.
- Every MCP command tool requires generated input/output schema tests and authorization tests.
- Every mutation path requires provenance and idempotency assertions.
- Do not accept sleeps as synchronization in correctness tests; use explicit hooks, barriers, or test schedulers.

## Stop and request human review when

- Transaction ordering, conflict ownership, read validation, atomicity, or outcome sequencing would change.
- A new language construct weakens static dependency visibility.
- A storage key, Protobuf field, durable envelope, or IR format must change incompatibly.
- MCP tool visibility, mutating-tool behavior, authorization, or redaction would change.
- Unsafe Rust, native code, cryptography, or a critical dependency is proposed.
- A required guarantee appears impossible under an accepted ADR.
- A work package needs paths outside its declared scope.
- A test reveals a conflict between the specification and an accepted ADR.

Do not hide the conflict behind a TODO or broaden the implementation silently.

## Definition of a useful handoff

A completed agent task leaves the next agent with:

- Compiling, documented public interfaces.
- Automated acceptance tests.
- Stable fixtures or generated artifacts.
- No unexplained warnings or ignored failures.
- A concise note describing assumptions, invariants, and remaining hazards.
