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

## Authoritative files

- `SPEC.md`
- `work_packages.yaml`
- `adr/*.md` with status `Accepted`
- `.proto` and contract grammar sources once merged
- Generated compatibility fixtures checked into the repository

When these conflict, stop and request human review. Do not choose whichever interpretation is easiest to implement.

## Work-package protocol

Before editing:

1. Read the assigned `WP-*` entry in `work_packages.yaml`.
2. Read its requirement IDs and ADR dependencies.
3. Confirm all hard dependencies are merged at the revision named in the task.
4. Restate the allowed paths and public interfaces in the PR description.
5. Add or identify a failing test for the behavior being implemented.

During implementation:

- Keep changes inside `allowed_paths` unless an interface change is separately approved.
- Prefer small interface-first commits when downstream packages depend on new types.
- Never weaken fail-closed behavior to make a test pass.
- Never duplicate a semantic type in two crates to avoid coordinating an interface.
- Use bounded input, output, recursion, collection, scan, wait, and diagnostic sizes.
- Preserve redaction before logging, metrics, MCP text, or public errors.

Before completion:

1. Run every `acceptance_commands` entry for the work package.
2. Run formatting and Clippy for changed crates.
3. Regenerate code and fixtures; verify the worktree is clean.
4. Update documentation and ADR status where required.
5. List requirement IDs satisfied by automated tests.
6. Report known limitations and follow-up issues.

## Documentation maintenance protocol

- `docs/SUMMARY.md` defines the public handbook. Any user-visible behavior,
  contract language, public protocol, CLI, configuration, installation,
  operational, compatibility, or SDK change MUST update the affected handbook
  page in the same pull request.
- New public behavior MUST be discoverable from `docs/SUMMARY.md`; do not leave
  public guidance only in an ADR, work-package report, test, example, or source
  comment.
- Generated handbook references and diagrams MUST be regenerated and checked in
  when their authoritative source changes. Do not hand-edit generated pages.
- Examples MUST use current public interfaces and identify POC limitations. Do
  not document proposed or deferred behavior as available.
- Run `./scripts/handbook check` for changes that affect public behavior or
  handbook sources. The check includes the book build, generated-reference
  freshness, links, snippets, and the published Rust API surface.
- Pull requests MUST state their documentation impact. `Not applicable` is
  acceptable only with a concrete reason for changes that cannot affect users,
  operators, application authors, public interfaces, or compatibility.

## Required PR description

```text
Work package:
Requirement IDs:
ADRs consulted:
Upstream revision:
Allowed paths used:
Behavior added or changed:
Compatibility classification:
Security implications:
Tests executed:
Generated artifacts checked:
Documentation impact:
Known limitations:
Follow-up issues:
```

## Standard commands

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo doc --workspace --no-deps
cargo deny check
```

Package-specific Loom, Shuttle, fuzz, recovery, generation, and conformance commands are defined in `work_packages.yaml` and repository scripts.

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
